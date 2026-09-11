package migration

import (
	"context"
	"sync"
	"time"

	"firestore-dr-migration/pkg/logger"
)

// InFlightBackfillBatch tracks a registered batch in-flight across partition workers.
type InFlightBackfillBatch struct {
	SeqNum          uint64
	SucceededDocs   int64
	MaxDocIDPerType map[BSONType]any // Highest _id per BSON type present in this batch
	Acked           bool
}

// BackfillPartitionTracker coordinates thread-safe out-of-order ACK watermarking,
// periodic checkpoint flushes, and Extended JSON disk persistence for a reader partition.
type BackfillPartitionTracker struct {
	mu                 sync.Mutex
	log                *logger.Logger
	checkpoint         *PartitionCheckpoint
	checkpointPath     string
	batches            map[uint64]*InFlightBackfillBatch
	pendingSeqs        []uint64
	nextSeqNum         uint64
	lastCheckpointSeq  uint64
	ackCountSinceSave  int
	saveThreshold      int
	checkpointInterval time.Duration
	lastSaveTime       time.Time
	completed          bool
}

// NewBackfillPartitionTracker creates a new BackfillPartitionTracker.
func NewBackfillPartitionTracker(
	log *logger.Logger,
	checkpoint *PartitionCheckpoint,
	checkpointPath string,
	checkpointInterval time.Duration,
	saveThreshold int,
) *BackfillPartitionTracker {
	if checkpointInterval <= 0 {
		checkpointInterval = 5 * time.Minute
	}
	if saveThreshold <= 0 {
		saveThreshold = 100
	}
	return &BackfillPartitionTracker{
		log:                log,
		checkpoint:         checkpoint,
		checkpointPath:     checkpointPath,
		batches:            make(map[uint64]*InFlightBackfillBatch),
		pendingSeqs:        make([]uint64, 0),
		nextSeqNum:         1,
		saveThreshold:      saveThreshold,
		checkpointInterval: checkpointInterval,
		lastSaveTime:       time.Now(),
	}
}

// Start spawns a background periodic flush goroutine.
func (t *BackfillPartitionTracker) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(t.checkpointInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.mu.Lock()
				if !t.completed && t.ackCountSinceSave > 0 {
					t.saveCheckpointsLocked()
				}
				t.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// RegisterBatch registers a newly read document batch for this partition, returning its unique sequence number.
func (t *BackfillPartitionTracker) RegisterBatch(batch []interface{}) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeqNum
	t.nextSeqNum++

	maxIDs := make(map[BSONType]any)
	for _, doc := range batch {
		if docID := extractDocID(doc); docID != nil {
			bType := GetBSONType(docID)
			existing, exists := maxIDs[bType]
			if !exists {
				maxIDs[bType] = docID
			} else {
				// Only overwrite if the new docID is greater than the current stored max.
				// This ensures correctness regardless of document ordering within the batch.
				if cmpResult, err := CompareBSONValues(docID, existing); err == nil && cmpResult > 0 {
					maxIDs[bType] = docID
				}
			}
		}
	}

	t.batches[seq] = &InFlightBackfillBatch{
		SeqNum:          seq,
		MaxDocIDPerType: maxIDs,
		Acked:           false,
	}
	t.pendingSeqs = append(t.pendingSeqs, seq)
	return seq
}

// AckBatch marks the batch with the given sequence number as successfully written/processed.
func (t *BackfillPartitionTracker) AckBatch(seq uint64, succeededDocs int64) {
	if seq == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if batch, ok := t.batches[seq]; ok {
		if !batch.Acked {
			batch.Acked = true
			batch.SucceededDocs = succeededDocs
			t.ackCountSinceSave++
		}
	}

	if !t.completed && (t.ackCountSinceSave >= t.saveThreshold || time.Since(t.lastSaveTime) >= t.checkpointInterval) {
		t.saveCheckpointsLocked()
	}
}

// MarkCompleted marks the partition backfill as completed, saving the final completed checkpoint to disk.
func (t *BackfillPartitionTracker) MarkCompleted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.completed = true
	t.saveCheckpointsLocked()
}

// Close flushes all contiguous acknowledged batches to disk and logs any unacknowledged gaps.
func (t *BackfillPartitionTracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.completed {
		return
	}
	t.saveCheckpointsLocked()

	if len(t.pendingSeqs) > 0 {
		t.log.Warnf("[BackfillPartitionTracker] Shutdown complete with %d unacknowledged batches (first unacked seq: %d)",
			len(t.pendingSeqs), t.pendingSeqs[0])
	}
}

// GetCheckpoint returns the current in-memory partition checkpoint.
func (t *BackfillPartitionTracker) GetCheckpoint() *PartitionCheckpoint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.checkpoint
}

// saveCheckpointsLocked advances checkpoint progress and flushes to disk.
func (t *BackfillPartitionTracker) saveCheckpointsLocked() {
	var prunedCount int
	var newLastSeq uint64
	var totalSucceeded int64
	newMaxIDs := make(map[BSONType]any)

	for _, seq := range t.pendingSeqs {
		batch := t.batches[seq]
		if batch != nil && batch.Acked {
			for bType, maxID := range batch.MaxDocIDPerType {
				newMaxIDs[bType] = maxID
			}
			totalSucceeded += batch.SucceededDocs
			newLastSeq = seq
			prunedCount++
		} else {
			break
		}
	}

	candidate := cloneCheckpoint(t.checkpoint)
	if candidate != nil {
		if candidate.TypeProgress == nil {
			candidate.TypeProgress = make(map[BSONType]*TypeRangeBoundary)
		}
		for bType, maxID := range newMaxIDs {
			if candidate.TypeProgress[bType] == nil {
				candidate.TypeProgress[bType] = &TypeRangeBoundary{BSONType: bType}
			}
			candidate.TypeProgress[bType].SavedLastID = maxID
		}
		candidate.ApproximateDocsMigrated += totalSucceeded
		candidate.UpdatedAt = time.Now().UTC()
	}

	if t.checkpointPath != "" && candidate != nil {
		if err := SavePartitionCheckpoint(t.checkpointPath, candidate); err != nil {
			t.log.Warnf("[BackfillPartitionTracker] Failed to save checkpoint to %s: %v", t.checkpointPath, err)
			return
		}
		t.log.Debugf("[BackfillPartitionTracker] Saved checkpoint successfully to %s (seq: %d)", t.checkpointPath, newLastSeq)
	}

	t.checkpoint = candidate
	for i := 0; i < prunedCount; i++ {
		delete(t.batches, t.pendingSeqs[i])
	}
	t.pendingSeqs = t.pendingSeqs[prunedCount:]
	if prunedCount > 0 {
		t.lastCheckpointSeq = newLastSeq
	}
	t.ackCountSinceSave = 0
	t.lastSaveTime = time.Now()
}

func cloneCheckpoint(cp *PartitionCheckpoint) *PartitionCheckpoint {
	if cp == nil {
		return nil
	}
	clone := &PartitionCheckpoint{
		Database:                cp.Database,
		Collection:              cp.Collection,
		PartitionIndex:          cp.PartitionIndex,
		TotalSplits:             cp.TotalSplits,
		ApproximateDocsMigrated: cp.ApproximateDocsMigrated,
		UpdatedAt:               cp.UpdatedAt,
		TypeProgress:            make(map[BSONType]*TypeRangeBoundary, len(cp.TypeProgress)),
	}
	for k, v := range cp.TypeProgress {
		if v != nil {
			clone.TypeProgress[k] = &TypeRangeBoundary{
				BSONType:     v.BSONType,
				RangeStartID: v.RangeStartID,
				RangeEndID:   v.RangeEndID,
				SavedLastID:  v.SavedLastID,
			}
		}
	}
	return clone
}
