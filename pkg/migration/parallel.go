package migration

/*
Pipeline Architecture & Thread Lifecycle Map (Who Produces & Consumes):

| Queue Stage     | Added to by (Producers)                                 | Removed from by (Consumers)                               | Role in the Pipeline |
| :-------------- | :------------------------------------------------------ | :-------------------------------------------------------- | :------------------- |
| ingestQueue     | source readers (1 per change stream source)             | partition router (1 central thread)                       | Buffers raw change stream events fetched from TCP sockets, waiting to be sorted and partitioned. |
| batchingQueue   | partition router (1 central thread)                       | transformer and batcher (1 per active worker)             | Buffers partitioned events routed to a specific worker, waiting to be batched and grouped. |
| batchWriteQueue | transformer and batcher (1 per active worker)             | target writers (1 per active worker)                       | Buffers finalized, ready-to-write batch task payloads (OperationGroup), waiting to be committed to target DB. |
*/

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"firestore-dr-migration/pkg/assess"
	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// EventDistributor manages the distribution of change events to workers
type EventDistributor struct {
	workers                   []*Worker
	incrementalWorkerCount    int
	sourceDB                  *db.MongoDB
	targetDB                  *db.MongoDB
	collectionConfigs         map[string]map[string]config.CollectionConfig
	changeStreams             []*mongo.ChangeStream
	ctx                       context.Context
	log                       *logger.Logger
	resumeTokenPath           string
	checkpointInterval        time.Duration
	saveThreshold             int
	incrementalWriteBatchSize int
	forceOrderedOperations    bool
	flushInterval             time.Duration // Flush interval in milliseconds
	dlq                       DLQ           // Dead Letter Queue for failed documents
	retryManager              *RetryManager // Retry manager for transient errors

	// Statistics tracking
	incrementalStatsManager *IncrementalStatsManager // Manager for statistics and replication lag
	cfg          *config.Config
	DryRun       bool // Dry run flag

	partitionTracker *PartitionTracker // Thread-safe ack-based progress checkpoint tracker
}

// NewEventDistributor creates a new event distributor
func NewEventDistributor(ctx context.Context, sourceDB, targetDB *db.MongoDB,
	collectionConfigs map[string]map[string]config.CollectionConfig,
	changeStreams []*mongo.ChangeStream, log *logger.Logger,
	resumeTokenPath string, checkpointInterval time.Duration,
	saveThreshold, incrementalWorkerCount, incrementalWriteBatchSize int,
	forceOrderedOperations bool, flushInterval time.Duration, cfg *config.Config, dlq DLQ, incrementalStatsManager *IncrementalStatsManager) *EventDistributor {

	// Create retry manager from config
	retryMgr := NewRetryManagerFromConfig(cfg, log)

	tracker := NewPartitionTracker(log, resumeTokenPath, checkpointInterval, saveThreshold, len(changeStreams))
	// Start the periodic checkpoint flush loop
	tracker.Start(ctx)

	return &EventDistributor{
		workers:                   make([]*Worker, incrementalWorkerCount),
		incrementalWorkerCount:    incrementalWorkerCount,
		sourceDB:                  sourceDB,
		targetDB:                  targetDB,
		collectionConfigs:         collectionConfigs,
		changeStreams:             changeStreams,
		ctx:                       ctx,
		log:                       log,
		resumeTokenPath:           resumeTokenPath,
		checkpointInterval:        checkpointInterval,
		saveThreshold:             saveThreshold,
		incrementalWriteBatchSize: incrementalWriteBatchSize,
		forceOrderedOperations:    forceOrderedOperations,
		flushInterval:             flushInterval,
		dlq:                       dlq,
		retryManager:              retryMgr,
		cfg:                       cfg,
		incrementalStatsManager:              incrementalStatsManager,
		partitionTracker:          tracker,
	}
}

// hashDocumentID creates a hash of the document ID
func hashDocumentID(docID interface{}) int {
	// Convert docID to a byte representation
	var bytes []byte

	switch id := docID.(type) {
	case string:
		bytes = []byte(id)
	case primitive.ObjectID:
		bytes = id[:]
	case int, int32, int64, float64, float32:
		bytes = []byte(fmt.Sprintf("%v", id))
	case bson.D:
		data, err := bson.Marshal(id)
		if err != nil {
			bytes = []byte(fmt.Sprintf("%v", id))
		} else {
			bytes = data
		}
	case bson.M:
		data, err := bson.Marshal(id)
		if err != nil {
			bytes = []byte(fmt.Sprintf("%v", id))
		} else {
			bytes = data
		}
	default:
		bytes = []byte(fmt.Sprintf("%v", id))
	}

	// Use FNV-1a hash algorithm
	h := fnv.New32a()
	h.Write(bytes)
	return int(h.Sum32())
}

// getWorkerIndex determines which worker should handle a document based on its ID
func (d *EventDistributor) getWorkerIndex(docID interface{}) int {
	hash := hashDocumentID(docID)
	// Ensure positive result
	return ((hash % d.incrementalWorkerCount) + d.incrementalWorkerCount) % d.incrementalWorkerCount
}

// Start begins the event distribution process
// Start begins the event distribution process
func (d *EventDistributor) Start() error {
	d.log.Infof("Starting event distributor with %d workers (GroupOpsByDistinctId: %t, changeStreams: %d)", d.incrementalWorkerCount, d.cfg.GroupOpsByDistinctId, len(d.changeStreams))

	// Concurrency Architecture (Sub-Context Coordination for Fail-Fast Partitioning):
	// We establish a cancelCtx child context linked to a thread-safe terminal error store (termErr).
	// If any sharded partition change stream reader background thread encounters a connection drop, cursor
	// invalidation, or EOF, it triggers cancel() via the setError helper. This fail-fast design immediately
	// signals exit paths across all concurrent readers (unblocking changeStream.Next) and the central distributor
	// consumer routing loop (unblocking select <-cancelCtx.Done()), preventing quiet data loss or silent freezes.
	cancelCtx, cancel := context.WithCancel(d.ctx)
	defer cancel()

	var errOnce sync.Once
	var termErr error
	setError := func(err error) {
		errOnce.Do(func() {
			termErr = err
		})
		cancel()
	}

	// Start the statistics manager periodic reporting
	if d.incrementalStatsManager != nil {
		d.incrementalStatsManager.Start(cancelCtx)
	}

	transformer := NewFieldTransformer(d.cfg.DropEmptyFieldNames, d.cfg.ConvertLongFieldNamesInNestedDocs, d.cfg.RetryConfig.ConvertInvalidIds, d.log)

	// Pre-populate the active failed document IDs from the DLQ file on startup
	var activeFailedIDs map[string]string
	var activeFailedMu sync.RWMutex
	if d.dlq != nil && d.dlq.FilePath() != "" {
		d.log.Info("DLQ resolution ledger: performing pre-scan startup check...")
		var scanErr error
		activeFailedIDs, scanErr = PopulateActiveFailedIDs(d.dlq.FilePath(), d.log)
		if scanErr != nil {
			d.log.Warnf("DLQ resolution ledger: failed to pre-scan DLQ file: %v (continuing without resolution logging)", scanErr)
		} else {
			d.log.Infof("DLQ resolution ledger: loaded %d active failed IDs on startup", len(activeFailedIDs))
			if d.incrementalStatsManager != nil {
				d.incrementalStatsManager.SetActiveFailedIDs(activeFailedIDs, &activeFailedMu)
			}
		}
	}

	// Initialize workers with retry manager and stats manager
	for i := 0; i < d.incrementalWorkerCount; i++ {
		d.workers[i] = NewWorker(i, cancelCtx, d.log, d.targetDB, d.collectionConfigs, d.incrementalWriteBatchSize, d.forceOrderedOperations, d.dlq, d.retryManager, d.incrementalStatsManager, d.cfg.GroupOpsByDistinctId, d.flushInterval, d.cfg.IncrementalIncomingQueueSize, d.cfg.IncrementalProcessingQueueSize, transformer)
		d.workers[i].SetPartitionTracker(d.partitionTracker)
		if activeFailedIDs != nil {
			d.workers[i].SetActiveFailedIDs(activeFailedIDs, &activeFailedMu)
		}
	}

	// Ensure all workers are shut down sequentially and safely upon exit
	defer func() {
		if d.incrementalStatsManager != nil {
			d.incrementalStatsManager.RegisterQueues(nil, nil)
		}
		d.log.Info("Shutting down workers...")
		for _, worker := range d.workers {
			if worker != nil {
				worker.Shutdown()
			}
		}
		if d.partitionTracker != nil {
			d.log.Info("Flushing final resume tokens...")
			d.partitionTracker.Close()
		}
	}()

	// Ingest queue for concurrent reader threads to publish events.
	// Capacity scales with the number of streams to avoid memory lockups and hold multiple batches safely.
	ingestQueue := make(chan QueueEvent, d.cfg.IncrementalIncomingQueueSize*len(d.changeStreams))
	if d.incrementalStatsManager != nil {
		d.incrementalStatsManager.RegisterQueues(d.workers, ingestQueue)
	}
	var readerWg sync.WaitGroup

	// Spawn concurrent background reader threads (Producer routines) for each sharded change stream.
	// This implements Asynchronous Prefetching (overlapping CPU allocation/routing and Network I/O):
	// while the distributor is routing batch A, these background loops are already prefetching batch B
	// over their respective TCP sockets, delivering a massive throughput/WPS scale-out!
	for idx, stream := range d.changeStreams {
		readerWg.Add(1)
		go func(streamIndex int, changeStream *mongo.ChangeStream) {
			defer readerWg.Done()
			// Defense in depth: a panic in the mongo driver (e.g. Next racing a Close)
			// must not take down the whole process. Recover, convert to a terminal
			// error so the distributor unwinds cleanly, and let readerWg.Done() run.
			defer func() {
				if rec := recover(); rec != nil {
					d.log.Errorf("[Reader %d] recovered from panic: %v", streamIndex, rec)
					setError(fmt.Errorf("change stream reader %d panicked: %v", streamIndex, rec))
				}
			}()

			for {
				// Block on network socket until the next batch is fetched/returned by the driver
				startTime := time.Now()
				ok := changeStream.Next(cancelCtx)
				latency := time.Since(startTime)
				readTime := time.Now()

				if !ok {
					// Reader EOF/Error Assessment:
					// Next returning false indicates the stream partition has terminated. We analyze the root cause:
					if err := changeStream.Err(); err != nil {
						if err == context.Canceled {
							// Normal shutdown path coordinated by cancelCtx sub-context
							d.log.Infof("[Reader %d] Change stream interrupted by cancellation", streamIndex)
							return
						}
						// Shard connection drop or database partition error: log and bubble up to pipeline controller
						d.log.Errorf("[Reader %d] Change stream error: %v", streamIndex, err)
						setError(err)
					} else {
						// Unexpected empty return (EOF cursor closure without explicit driver error string):
						// Bubble up custom error so supervisor knows this partition has stopped feeding events.
						d.log.Errorf("[Reader %d] Change stream cursor closed unexpectedly", streamIndex)
						setError(fmt.Errorf("change stream reader %d closed unexpectedly", streamIndex))
					}
					return
				}

				rawEvent := changeStream.Current
				size := len(rawEvent)

				if d.incrementalStatsManager != nil {
					d.incrementalStatsManager.RecordReadMetric(streamIndex, latency, size)
				}

				if d.DryRun {
					if d.incrementalStatsManager != nil {
						eventTime := ExtractEventTimeFromRaw(rawEvent)
						d.incrementalStatsManager.RecordDryRunLag(eventTime, readTime)
					}
					continue
				}

				// Deep copy BSON raw bytes safely for cross-thread channel transfers
				rawCopy := make(bson.Raw, len(rawEvent))
				copy(rawCopy, rawEvent)

				resumeToken := changeStream.ResumeToken()
				eventTime := ExtractEventTimeFromRaw(rawCopy)
				var seqNum uint64
				if d.partitionTracker != nil {
					seqNum = d.partitionTracker.Register(streamIndex, resumeToken, eventTime)
				}

				event := QueueEvent{
					Event:       rawCopy,
					ReadTime:    readTime,
					StreamIndex: streamIndex,
					SeqNum:      seqNum,
				}

				select {
				case ingestQueue <- event:
				case <-cancelCtx.Done():
					if d.partitionTracker != nil {
						d.partitionTracker.Ack(streamIndex, seqNum)
					}
					return
				}
			}
		}(idx, stream)
	}

	// Guarantee every reader goroutine has fully exited before Start returns.
	// The caller closes the change streams via `defer stream.Close(ctx)` the moment
	// Start returns; if a reader were still blocked in changeStream.Next(), that
	// concurrent Close would race the mongo driver and segfault (nil cursor deref).
	// Cancel first to unblock any in-flight Next, then wait for readers to drain.
	// Registered after the reader loop so (LIFO) it runs BEFORE the worker-shutdown
	// and top-level cancel defers — readers are gone before anything else tears down.
	defer func() {
		cancel()
		readerWg.Wait()
	}()

	// Spin up cleanup supervisor to close the queue once all threads have terminated
	go func() {
		readerWg.Wait()
		close(ingestQueue)
	}()

	// Main loop (Consumer routine) to distribute events from the shared pre-fetched queue to workers.
	// Since background threads keep this shared queue populated, the main distributor thread loop
	// experiences near-zero wait I/O delay and can route events lock-freely at top speeds!
	for {
		select {
		case event, ok := <-ingestQueue:
			if !ok {
				// All reader threads completed. If there was a termination error, return it!
				if termErr != nil {
					return termErr
				}
				return nil
			}
			event.DistributorTime = time.Now()

			rawEvent, ok := event.Event.(bson.Raw)
			if !ok {
				if d.partitionTracker != nil {
					d.partitionTracker.Ack(event.StreamIndex, event.SeqNum)
				}
				continue
			}

			// Non-DML / Control Event Graceful Filtering (Prevent Log Pollution):
			// Change streams report DDL/system events (like drops, invalidations, index changes) that do not have
			// a documentKey payload. We identify these via fast BSON type-validated binary lookup.
			// Safety Boundary: If the event is a collection drop ("drop"), we treat it as a terminal failure to prevent silent data loss.
			// For other harmless/unsupported non-DML events, we skip them cleanly with partition progress sequence ACKs.
			opTypeVal, err := rawEvent.LookupErr("operationType")
			if err == nil && opTypeVal.Type == bson.TypeString {
				opType := opTypeVal.StringValue()
				if opType == "drop" {
					var nsCtx string
					if ns := ExtractNamespaceFromRawEvent(rawEvent); ns != "" {
						nsCtx = " " + ns
					}
					return fmt.Errorf("terminal failure: collection drop event detected in partition %d%s", event.StreamIndex, nsCtx)
				}
				if opType != "insert" && opType != "update" && opType != "replace" && opType != "delete" {
					d.log.Warnf("Skipping unsupported change stream event type %q", opType)
					if d.incrementalStatsManager != nil {
						d.incrementalStatsManager.RecordSkippedEvent(opType)
					}
					if d.partitionTracker != nil {
						d.partitionTracker.Ack(event.StreamIndex, event.SeqNum)
					}
					continue
				}
			}

			// Extract documentKey._id via fast binary lookup
			docKeyVal, err := rawEvent.LookupErr("documentKey")
			if err != nil {
				d.log.Errorf("Invalid raw change event: missing documentKey")
				if d.partitionTracker != nil {
					d.partitionTracker.Ack(event.StreamIndex, event.SeqNum)
				}
				continue
			}
			docKeyRaw := docKeyVal.Document()
			docIDVal, err := docKeyRaw.LookupErr("_id")
			if err != nil {
				d.log.Errorf("Invalid raw change event: missing documentKey._id")
				if d.partitionTracker != nil {
					d.partitionTracker.Ack(event.StreamIndex, event.SeqNum)
				}
				continue
			}

			// Determine worker index deterministically by key hashing
			hash := hashBytes(docIDVal.Value)
			workerIndex := ((hash % d.incrementalWorkerCount) + d.incrementalWorkerCount) % d.incrementalWorkerCount

			// Dispatch event to the target worker channel
			event.DistributorPushTime = time.Now()
			pushStart := time.Now()
			select {
			case d.workers[workerIndex].batchingQueue <- event:
				stall := time.Since(pushStart)
				if stall > 1*time.Millisecond && d.incrementalStatsManager != nil {
					d.incrementalStatsManager.RecordBatchingQueueStall(stall)
				}
			case <-cancelCtx.Done():
				if d.partitionTracker != nil {
					d.partitionTracker.Ack(event.StreamIndex, event.SeqNum)
				}
				if termErr != nil {
					return termErr
				}
				return nil
			}

		case <-cancelCtx.Done():
			if termErr != nil {
				return termErr
			}
			return nil
		}
	}
}

// saveResumeToken is left as a legacy stub for single-stream compatibility
func (d *EventDistributor) saveResumeToken(resumeToken bson.Raw) {
	d.savePartitionResumeToken(0, resumeToken)
}

// savePartitionResumeToken saves the resume token for a specific stream partition
func (d *EventDistributor) savePartitionResumeToken(partitionIndex int, resumeToken bson.Raw) {
	var resumeTokenDoc bson.M
	if err := bson.Unmarshal(resumeToken, &resumeTokenDoc); err != nil {
		d.log.Errorf("[Reader %d] Error unmarshaling resume token: %v", partitionIndex, err)
		return
	}

	path := GetPartitionResumeTokenPath(d.resumeTokenPath, partitionIndex, len(d.changeStreams))
	if err := SaveResumeToken(path, resumeTokenDoc); err != nil {
		d.log.Errorf("[Reader %d] Error saving resume token to %s: %v", partitionIndex, path, err)
	} else {
		d.log.Debugf("[Reader %d] Saved resume token successfully to %s", partitionIndex, path)
	}
}

// QueueEvent wraps the raw change stream or oplog event with metadata like read time
type QueueEvent struct {
	Event               interface{} // bson.M or bson.Raw
	ReadTime            time.Time
	DistributorTime     time.Time
	DistributorPushTime time.Time
	StreamIndex         int
	SeqNum              uint64
}

// WriteOperation represents a single write operation
type WriteOperation struct {
	DocumentID          interface{}
	Document            interface{}
	TransformedDocument interface{} // Pre-calculated transformed document for Firestore/Spanner compatibility
	UpdateDescription   interface{} // For modifier updates ($set, $inc, etc.)
	Namespace           string
	OpType              string
	// Stats
	EventTime         time.Time // Time when the change event occurred on the source database (clusterTime or wallTime)
	ReadTime          time.Time // Time when the event was read from the change stream/oplog by the distributor
	WorkerReceiveTime time.Time // Time when the event was received by the worker thread goroutine
	SuccessTime       time.Time // Set when successfully written to target DB
	SuccessAfterRetry bool      // Set to true if the operation succeeded after a retry
	DLQed             bool      // Track if the operation was routed to the DLQ
	Error             error     // Track the exact write failure error
	StreamIndex       int
	SeqNum            uint64
}

// OperationGroup represents a group of operations of the same type and namespace
type OperationGroup struct {
	Namespace  string
	OpType     string
	Operations []WriteOperation
	CreatedAt  time.Time // New field to track when the group was created
}

// Worker processes change events for a subset of documents
type Worker struct {
	id                int
	ctx               context.Context
	log               *workerLogger
	targetDB          *db.MongoDB
	collectionConfigs map[string]map[string]config.CollectionConfig
	incrementalStatsManager      *IncrementalStatsManager

	// Queue of raw change events waiting to be partitioned and batched concurrently
	batchingQueue chan interface{}

	// Current group being built
	currentGroup *OperationGroup

	// Queue of groups waiting to be processed
	batchWriteQueue chan *OperationGroup

	// Maximum group size
	incrementalWriteBatchSize int

	// Force ordered operations for all types
	forceOrderedOperations bool

	// Dead Letter Queue for failed documents
	dlq DLQ

	// Retry manager for transient error handling
	retryManager *RetryManager

	// For shutdown coordination
	wg sync.WaitGroup

	// Mutex to protect concurrent access
	mu sync.RWMutex

	// Flag to indicate if shutdown is in progress
	shutdownInProgress bool

	// Dynamic grouping configurations
	groupOpsByDistinctId bool
	currentGroupIDs      map[int]bool
	flushInterval        time.Duration
	partitionTracker     *PartitionTracker
	transformer          *FieldTransformer

	// Map to track active failed document IDs for Resolution Tombstoning
	activeFailedIDs map[string]string
	activeFailedMu  *sync.RWMutex
}

// SetPartitionTracker sets the PartitionTracker for this worker
func (w *Worker) SetPartitionTracker(tracker *PartitionTracker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.partitionTracker = tracker
}

// SetActiveFailedIDs sets the shared map and lock for tracking active failed document IDs
func (w *Worker) SetActiveFailedIDs(m map[string]string, mu *sync.RWMutex) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.activeFailedIDs = m
	w.activeFailedMu = mu
}

// pushToBatchWriteQueue sends a completed group to the batch write queue while tracking any block/stall durations
func (w *Worker) pushToBatchWriteQueue(group *OperationGroup) {
	start := time.Now()
	w.batchWriteQueue <- group
	stall := time.Since(start)
	if stall > 1*time.Millisecond && w.incrementalStatsManager != nil {
		w.incrementalStatsManager.RecordBatchWriteQueueStall(stall)
	}
}

// flushCurrentGroup moves the current group to the batch write queue if it exists
func (w *Worker) flushCurrentGroup() bool {
	// Must be called with lock held
	if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
		w.log.Debugf("Flushing group: %s.%s with %d operations",
			w.currentGroup.Namespace, w.currentGroup.OpType,
			len(w.currentGroup.Operations))

		w.pushToBatchWriteQueue(w.currentGroup)
		w.currentGroup = nil
		w.currentGroupIDs = make(map[int]bool)
		return true
	}
	return false
}

type statsTrackingDLQ struct {
	underlyingDlq DLQ
	incrementalStatsManager  *IncrementalStatsManager
}

func (s *statsTrackingDLQ) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}, eventTime time.Time) {
	s.underlyingDlq.WriteFailed(sourceDB, sourceCollection, documentID, err, phase, opType, document, eventTime)
	// Incremented in incrementalStatsManager.RecordLags to prevent double counting and maintain context
}

func (s *statsTrackingDLQ) WriteResolved(sourceDB, sourceCollection string, documentID interface{}, phase string, eventTime time.Time) {
	s.underlyingDlq.WriteResolved(sourceDB, sourceCollection, documentID, phase, eventTime)
	s.incrementalStatsManager.RecordDLQResolution(1)
}

func (s *statsTrackingDLQ) Count() int64 {
	return s.underlyingDlq.Count()
}

func (s *statsTrackingDLQ) FilePath() string {
	return s.underlyingDlq.FilePath()
}

func (s *statsTrackingDLQ) Close() {
	s.underlyingDlq.Close()
}

// NewWorker creates a new worker
func NewWorker(id int, ctx context.Context, log *logger.Logger,
	targetDB *db.MongoDB, collectionConfigs map[string]map[string]config.CollectionConfig,
	incrementalWriteBatchSize int, forceOrderedOperations bool, dlq DLQ, retryManager *RetryManager, incrementalStatsManager *IncrementalStatsManager, groupOpsByDistinctId bool, flushInterval time.Duration,
	batchingQueueSize int, batchWriteQueueSize int, transformer *FieldTransformer) *Worker {

	var workerDLQ DLQ = dlq
	if dlq != nil && incrementalStatsManager != nil {
		workerDLQ = &statsTrackingDLQ{
			underlyingDlq: dlq,
			incrementalStatsManager:  incrementalStatsManager,
		}
	}

	w := &Worker{
		id:                        id,
		ctx:                       ctx,
		log:                       &workerLogger{workerID: id, logger: log},
		targetDB:                  targetDB,
		collectionConfigs:         collectionConfigs,
		batchingQueue:             make(chan interface{}, batchingQueueSize),
		batchWriteQueue:           make(chan *OperationGroup, batchWriteQueueSize),
		incrementalWriteBatchSize: incrementalWriteBatchSize,
		forceOrderedOperations:    forceOrderedOperations,
		dlq:                       workerDLQ,
		retryManager:              retryManager,
		incrementalStatsManager:              incrementalStatsManager,
		groupOpsByDistinctId:      groupOpsByDistinctId,
		currentGroupIDs:           make(map[int]bool),
		flushInterval:             flushInterval,
		transformer:               transformer,
	}

	// Increment WaitGroup count for both background goroutines on the parent constructor thread
	w.wg.Add(2)

	// Statically spawn the worker and eventLoop threads at startup
	go w.processBatchWriteQueue()
	go w.eventLoop()

	return w
}

// eventLoop concurrently drains raw change events from the batchingQueue, partitioning and batching them lock-freely
func (w *Worker) eventLoop() {
	defer w.wg.Done()

	// Local worker-level ticker to flush groups that time out
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-w.batchingQueue:
			if !ok {
				// Queue closed, flush any remaining operations
				w.mu.Lock()
				w.flushCurrentGroup()
				close(w.batchWriteQueue) // Signal processBatchWriteQueue to drain and exit
				w.mu.Unlock()
				return
			}
			w.ProcessEvent(event)

		case <-ticker.C:
			w.mu.Lock()
			if w.currentGroup != nil && len(w.currentGroup.Operations) > 0 {
				if time.Since(w.currentGroup.CreatedAt) >= w.flushInterval {
					if w.incrementalStatsManager != nil {
						w.incrementalStatsManager.IncrementTimeoutFlushes()
					}
					w.flushCurrentGroup()
				}
			}
			w.mu.Unlock()

		case <-w.ctx.Done():
			// Context canceled, flush remaining and shut down
			w.mu.Lock()
			w.flushCurrentGroup()
			close(w.batchWriteQueue)
			w.mu.Unlock()
			return
		}
	}
}

// ProcessEvent handles a single change event by decoding raw BSON concurrently in the worker thread
func (w *Worker) ProcessEvent(eventArg interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var event bson.M
	var readTime time.Time
	var streamIndex int
	var seqNum uint64

	switch e := eventArg.(type) {
	case QueueEvent:
		readTime = e.ReadTime
		streamIndex = e.StreamIndex
		seqNum = e.SeqNum
		if w.incrementalStatsManager != nil && !e.DistributorTime.IsZero() && !e.DistributorPushTime.IsZero() {
			ingestQueueDelay := e.DistributorTime.Sub(e.ReadTime)
			batchingQueueDelay := time.Since(e.DistributorPushTime)
			w.incrementalStatsManager.RecordQueueDelays(ingestQueueDelay, batchingQueueDelay)
		}
		switch inner := e.Event.(type) {
		case bson.M:
			event = inner
		case bson.Raw:
			if err := bson.Unmarshal(inner, &event); err != nil {
				w.log.Errorf("Failed to unmarshal raw BSON change event: %v", err)
				if w.partitionTracker != nil {
					w.partitionTracker.Ack(streamIndex, seqNum)
				}
				return
			}
		default:
			w.log.Errorf("Invalid inner event type: %T", e.Event)
			if w.partitionTracker != nil {
				w.partitionTracker.Ack(streamIndex, seqNum)
			}
			return
		}
	case bson.M:
		event = e
		if rt, exists := event["readTime"]; exists {
			if t, ok := rt.(time.Time); ok {
				readTime = t
			}
		}
	case bson.Raw:
		if err := bson.Unmarshal(e, &event); err != nil {
			w.log.Errorf("Failed to unmarshal raw BSON change event: %v", err)
			return
		}
	default:
		w.log.Errorf("Invalid event type in ProcessEvent: %T", eventArg)
		return
	}

	if readTime.IsZero() {
		readTime = time.Now()
	}

	// Extract operation details
	opType, _ := event["operationType"].(string)
	if w.incrementalStatsManager != nil {
		w.incrementalStatsManager.IncrementEventsWorkerReceived(opType)
	}
	ns, _ := event["ns"].(bson.M)
	dbName, _ := ns["db"].(string)
	collName, _ := ns["coll"].(string)
	namespace := fmt.Sprintf("%s.%s", dbName, collName)

	documentKey, _ := event["documentKey"].(bson.M)
	docID := documentKey["_id"]

	if w.transformer != nil && w.transformer.convertInvalidIds && !w.transformer.isValidIDType(docID) {
		originalType := fmt.Sprintf("%T", docID)
		convertedID := serializeIDDeterministically(docID)
		w.log.Infof("[%s] Proactively converting change stream documentKey _id %v (type: %s) to string: %s (Solution 1, 2 & 4)",
			namespace, docID, originalType, convertedID)
		docID = convertedID
		if documentKey != nil {
			documentKey["_id"] = docID
		}
	}

	// Get fullDocument as interface{} to support both bson.M and map[string]interface{}
	// This is needed because legacy oplog replicator returns map[string]interface{}
	fullDocument := event["fullDocument"]

	// Graceful Null Document Update Skipping (Decoupled Logic Fix):
	// In MongoDB change streams, an update event that is immediately followed by a delete can arrive with a nil
	// fullDocument payload. Because we process updates using complete document replacements, we cannot process
	// the operation without a payload. We skip these events gracefully. Crucially: This check is decoupled from the
	// incrementalStatsManager check: previously, a nil incrementalStatsManager bypassed the skip block, causing key transformation failures
	// and unnecessary DLQ errors.
	if opType == "update" && fullDocument == nil {
		if w.incrementalStatsManager != nil {
			w.incrementalStatsManager.IncrementUpdatedThenDeleted(w.id)
			w.incrementalStatsManager.RecordSkippedEvent("update-doc-missing")
		}
		if w.partitionTracker != nil {
			w.partitionTracker.Ack(streamIndex, seqNum)
		}
		return
	}

	// Get updateDescription for modifier updates ($set, $inc, etc.)
	var updateDescription interface{}

	// Extract clusterTime or wallTime for lag tracking
	eventTime := ExtractEventTime(event)

	// Debug log for worker events
	w.log.Debugf("received event: type=%s, namespace=%s, docID=%v, hasFullDoc=%v, hasUpdateDesc=%v",
		opType, namespace, docID, fullDocument != nil, updateDescription != nil)

	// Create write operation
	op := WriteOperation{
		DocumentID:        docID,
		Document:          fullDocument,
		UpdateDescription: updateDescription,
		Namespace:         namespace,
		OpType:            opType,
		EventTime:         eventTime,
		ReadTime:          readTime,
		WorkerReceiveTime: time.Now(),
		StreamIndex:       streamIndex,
		SeqNum:            seqNum,
	}

	docHash := hashDocumentID(docID)

	// Check if we need to create a new group
	var needNewGroup bool
	if w.groupOpsByDistinctId {
		needNewGroup = w.currentGroup == nil ||
			w.currentGroup.Namespace != namespace ||
			len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize ||
			w.currentGroupIDs[docHash]
	} else {
		needNewGroup = w.currentGroup == nil ||
			w.currentGroup.OpType != opType ||
			w.currentGroup.Namespace != namespace ||
			len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize
	}

	if needNewGroup && w.currentGroup != nil {
		if w.incrementalStatsManager != nil {
			// Determine the reason the group had to be flushed
			if w.groupOpsByDistinctId && w.currentGroupIDs[docHash] {
				w.incrementalStatsManager.IncrementGroupFlushReason("collision")
			} else if w.currentGroup.Namespace != namespace {
				w.incrementalStatsManager.IncrementGroupFlushReason("namespace")
			} else if len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize {
				w.incrementalStatsManager.IncrementGroupFlushReason("batchfull")
			} else if w.currentGroup.OpType != opType {
				w.incrementalStatsManager.IncrementGroupFlushReason("optype")
			}
		}

		// Add current group to processing queue
		w.pushToBatchWriteQueue(w.currentGroup)
		w.currentGroup = nil
		w.currentGroupIDs = make(map[int]bool)
	}

	// Create a new group if needed
	if w.currentGroup == nil {
		groupOpType := opType
		if w.groupOpsByDistinctId {
			groupOpType = "mixed"
		}
		w.currentGroup = &OperationGroup{
			Namespace:  namespace,
			OpType:     groupOpType,
			Operations: []WriteOperation{op},
			CreatedAt:  time.Now(), // Set creation timestamp
		}
		w.currentGroupIDs = make(map[int]bool)
		w.currentGroupIDs[docHash] = true
	} else {
		// Add to current group
		w.currentGroup.Operations = append(w.currentGroup.Operations, op)
		w.currentGroupIDs[docHash] = true
	}

	// If current group has reached max size, add it to the queue
	if len(w.currentGroup.Operations) >= w.incrementalWriteBatchSize {
		if w.incrementalStatsManager != nil {
			w.incrementalStatsManager.IncrementGroupFlushReason("batchfull")
		}
		w.pushToBatchWriteQueue(w.currentGroup)
		w.currentGroup = nil
	}
}

// processBatchWriteQueue processes groups in the batch write queue sequentially.
// [Safety Fix 6: Graceful Worker Shutdown Data Loss]
// This uses a direct range loop over batchWriteQueue instead of selecting on w.ctx.Done().
// Selecting on w.ctx.Done() would cause the consumer thread to exit immediately upon cancellation,
// losing any remaining flushed batches. Using range guarantees that all remaining records
// in the queue are fully processed and drained before the goroutine exits on shutdown.
func (w *Worker) processBatchWriteQueue() {
	defer w.wg.Done()

	for group := range w.batchWriteQueue {
		// Process the group
		w.executeBatchWrite(*group)
	}

	if w.isShutdownInProgress() {
		w.log.Debugf("Completed processing all groups during shutdown")
	}
}

// executeBatchWrite processes a single operation group
func (w *Worker) executeBatchWrite(group OperationGroup) {
	if w.partitionTracker != nil {
		defer func() {
			for _, op := range group.Operations {
				w.partitionTracker.Ack(op.StreamIndex, op.SeqNum)
			}
		}()
	}

	w.log.Debugf("Processing group: %s.%s with %d operations",
		group.Namespace, group.OpType, len(group.Operations))

	// Get target collection
	parts := strings.SplitN(group.Namespace, ".", 2)
	if len(parts) != 2 {
		w.log.Errorf("Invalid namespace format: %s", group.Namespace)
		return
	}
	dbName, collName := parts[0], parts[1]

	// Get mapped collection name
	targetCollName := w.getTargetCollectionName(dbName, collName)

	if w.targetDB == nil {
		now := time.Now()
		for i := range group.Operations {
			group.Operations[i].SuccessTime = now
		}
		if w.incrementalStatsManager != nil {
			w.incrementalStatsManager.RecordLags(group.Operations)
		}
		return
	}

	targetCollection := w.targetDB.GetCollection(targetCollName)

	// Use a graceful shutdown context with timeout if the main context was canceled.
	// This ensures the final queues flushed during Ctrl+C / shutdown can actually write to MongoDB.
	writeCtx := w.ctx
	var cancel context.CancelFunc
	if w.ctx.Err() != nil {
		writeCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	} else {
		// Enforce query-level timeout of 90 seconds to prevent workers getting stuck
		// and tearing down TCP connections on driver socket timeout.
		writeCtx, cancel = context.WithTimeout(w.ctx, 90*time.Second)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	// Determine if we should use ordered operations
	useOrdered := group.OpType == "update" || group.OpType == "replace" || w.forceOrderedOperations

	if w.incrementalStatsManager != nil {
		w.incrementalStatsManager.RecordBulkWrite(len(group.Operations), useOrdered)
	}

	// Process based on operation type
	collConfig, exists := w.collectionConfigs[dbName][collName]
	isUpsertInsert := group.OpType == "insert" && exists && collConfig.UpsertMode
	isBulkWrite := group.OpType == "mixed" || group.OpType == "update" || group.OpType == "replace" || group.OpType == "delete" || isUpsertInsert

	if isBulkWrite {
		var models []mongo.WriteModel
		for idx := range group.Operations {
			op := &group.Operations[idx]
			model, err := w.buildWriteModel(op, dbName, collName)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			models = append(models, model)
		}

		if len(models) > 0 {
			startTime := time.Now()
			_, err := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
			if w.incrementalStatsManager != nil {
				w.incrementalStatsManager.RecordLatency(group.OpType, group.Operations, time.Since(startTime), w.id, err == nil)
			}
			w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
				_, retryBulkErr := targetCollection.BulkWrite(writeCtx, models, options.BulkWrite().SetOrdered(useOrdered))
				return retryBulkErr
			})
		}
	} else if group.OpType == "insert" {
		var docs []interface{}
		for idx := range group.Operations {
			op := &group.Operations[idx]
			transformed, err := w.transformer.Transform(op.Document, dbName, collName, op.DocumentID)
			if err != nil {
				w.handleTransformationFailure(op, dbName, collName, err)
				continue
			}
			op.TransformedDocument = transformed
			docs = append(docs, transformed)
		}

		if len(docs) > 0 {
			startTime := time.Now()
			_, err := targetCollection.InsertMany(writeCtx, docs, options.InsertMany().SetOrdered(useOrdered))
			if w.incrementalStatsManager != nil {
				w.incrementalStatsManager.RecordLatency("insert", group.Operations, time.Since(startTime), w.id, err == nil)
			}
			w.handleBulkWriteResult(writeCtx, &group, err, targetCollection, dbName, collName, func() error {
				_, retryInsertErr := targetCollection.InsertMany(writeCtx, docs, options.InsertMany().SetOrdered(useOrdered))
				return retryInsertErr
			})
		}
	}

	// Record replication lag for successfully processed operations
	if w.incrementalStatsManager != nil {
		w.incrementalStatsManager.RecordLags(group.Operations)
	}

	// Scan group operations for successful updates resolving previously failed documents
	if w.activeFailedMu != nil {
		w.activeFailedMu.Lock()
		if w.activeFailedIDs != nil {
			for _, op := range group.Operations {
				if !op.SuccessTime.IsZero() && !op.DLQed {
					key := MakeDLQKey(dbName, collName, op.DocumentID)
					if originalPhase, exists := w.activeFailedIDs[key]; exists {
						if w.dlq != nil {
							w.dlq.WriteResolved(dbName, collName, op.DocumentID, originalPhase, op.EventTime)
						}
						delete(w.activeFailedIDs, key)
					}
				}
			}
		}
		w.activeFailedMu.Unlock()
	}
}

// handleBulkWriteResult processes the outcome of a bulk write operation, setting SuccessTime for succeeded operations and executing fallbacks for failed ones.
func (w *Worker) handleBulkWriteResult(ctx context.Context, group *OperationGroup, err error, targetCollection *mongo.Collection, dbName, collName string, transientRetryFunc func() error) {
	if err == nil {
		now := time.Now()
		for i := range group.Operations {
			group.Operations[i].SuccessTime = now
		}
		w.log.Debugf("[%s.%s] Bulk %s operations processed successfully: %d documents", dbName, collName, group.OpType, len(group.Operations))
		return
	}

	// Use errors.As instead of direct type assertion (err.(mongo.BulkWriteException))
	// because the driver or retry wrapper may wrap the underlying BulkWriteException.
	var bulkWriteException mongo.BulkWriteException
	ok := errors.As(err, &bulkWriteException)
	if ok && ctx.Err() != context.Canceled {
		nonDupCount := 0
		for _, writeErr := range bulkWriteException.WriteErrors {
			if !isDuplicateKeyError(writeErr.Code, writeErr.Message) {
				nonDupCount++
			}
		}

		if nonDupCount > 0 {
			w.log.Errorf("[%s.%s] Bulk %s operations partially failed: %d failed", dbName, collName, group.OpType, len(bulkWriteException.WriteErrors))
		} else {
			w.log.Debugf("[%s.%s] Bulk %s operations partially failed: %d failed (all duplicate key errors)", dbName, collName, group.OpType, len(bulkWriteException.WriteErrors))
		}

		failedIndices := make(map[int]bool)
		for _, writeErr := range bulkWriteException.WriteErrors {
			failedIndices[writeErr.Index] = true
		}
		now := time.Now()
		for i := range group.Operations {
			if !failedIndices[i] {
				group.Operations[i].SuccessTime = now
			}
		}

		// Process individual errors
		for _, writeErr := range bulkWriteException.WriteErrors {
			var failedDocID interface{}
			var op WriteOperation
			if writeErr.Index < len(group.Operations) {
				op = group.Operations[writeErr.Index]
				failedDocID = op.DocumentID
			}

			isDup := isDuplicateKeyError(writeErr.Code, writeErr.Message)
			if isDup {
				if w.incrementalStatsManager != nil {
					w.incrementalStatsManager.IncrementDuplicateKeys(1)
				}
				w.log.Debugf("[%s.%s] Duplicate key occurrence at index %d (opType=%s), _id=%v: %v. Gracefully falling back.", dbName, collName, writeErr.Index, op.OpType, failedDocID, writeErr.Message)
			} else {
				w.log.Errorf("[%s.%s] Write error at index %d (opType=%s), _id=%v: %v", dbName, collName, writeErr.Index, op.OpType, failedDocID, writeErr.Message)
			}

			// Note: We do not classify the error here to decide whether to skip the fallback.
			// Attempting fallback write first is safer: (1) if classification regexes are incomplete,
			// we avoid dropping valid writes; (2) some errors are batch-specific and succeed individually.
			if writeErr.Index < len(group.Operations) {
				w.retryIndividualOperation(ctx, targetCollection, &group.Operations[writeErr.Index], dbName, collName, writeErr.Message)
			}
		}
	} else {
		// Handle non-bulk write errors
		if err == context.Canceled || ctx.Err() == context.Canceled {
			w.log.Debugf("[%s.%s] Bulk %s operations canceled due to context cancellation", dbName, collName, group.OpType)
			return
		}

		w.log.Errorf("[%s.%s] Error performing bulk %s operations: %v", dbName, collName, group.OpType, err)

		// For transient errors, retry the bulk operation with backoff before falling back
		bulkRetrySucceeded := false
		if w.retryManager != nil && transientRetryFunc != nil {
			errType := w.retryManager.ClassifyError(err)
			if errType == ErrorTypeConnection || errType == ErrorTypeContention {
				w.log.Infof("[%s.%s] Transient error detected. Retrying bulk %s operations with backoff...", dbName, collName, group.OpType)
				retryErr := w.retryManager.RetryWithBackoff(ctx, transientRetryFunc)
				if retryErr == nil {
					w.log.Infof("[%s.%s] Bulk %s operations succeeded after retry", dbName, collName, group.OpType)
					bulkRetrySucceeded = true
					now := time.Now()
					for i := range group.Operations {
						group.Operations[i].SuccessTime = now
						group.Operations[i].SuccessAfterRetry = true
					}
				} else {
					w.log.Warnf("[%s.%s] Bulk %s operations still failed after retries: %v. Falling back to individual operations.", dbName, collName, group.OpType, retryErr)
				}
			}
		}

		if !bulkRetrySucceeded {
			// Fall back to individual updates/deletes/replaces
			for i := range group.Operations {
				w.retryIndividualOperation(ctx, targetCollection, &group.Operations[i], dbName, collName, err.Error())
			}
		}
	}
}

// getTargetCollectionName gets the target collection name for a source collection
func (w *Worker) getTargetCollectionName(dbName, collName string) string {
	// Base target name: an explicit mapping wins, else the source name.
	target := collName
	if w.collectionConfigs[dbName] != nil {
		if collConfig, exists := w.collectionConfigs[dbName][collName]; exists {
			target = collConfig.TargetCollection
		}
	}
	// Apply a rename-collection remediation (reserved __x__ → _x_) so writes land
	// in the sanitized collection the assessment simulated. Keyed by the SOURCE
	// name; a no-op when the target is already legal.
	if w.transformer != nil {
		renamed := w.transformer.SanitizeTargetName(dbName, collName, target)
		if renamed != target {
			recordCollectionRename(w.log.logger, dbName, collName, renamed)
		}
		target = renamed
	}
	return target
}

// isShutdownInProgress returns if a shutdown task is currently in progress thread-safely
func (w *Worker) isShutdownInProgress() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.shutdownInProgress
}

// WaitForCompletion waits for all processing to complete
func (w *Worker) WaitForCompletion() {
	w.wg.Wait()
	w.log.Debugf("All operations processed successfully")
}

// Shutdown ensures any pending operations are processed
func (w *Worker) Shutdown() {
	w.mu.Lock()
	w.shutdownInProgress = true
	w.mu.Unlock()

	// Close raw incoming queue to signal eventLoop to drain and close batchWriteQueue
	close(w.batchingQueue)

	// Wait for both eventLoop and processGroups goroutines to complete
	w.WaitForCompletion()
}

// retryIndividualOperation retries a single failed write operation of a batch using fallback execution.
func (w *Worker) retryIndividualOperation(ctx context.Context, targetCollection *mongo.Collection, op *WriteOperation, dbName, collName string, originalErrorMsg string) {
	filter := bson.M{"_id": op.DocumentID}
	if w.incrementalStatsManager != nil {
		w.incrementalStatsManager.IncrementSequentialRetries(op.OpType, 1)
	}

	// Check if this error represents a duplicate key/already exists constraint failure
	isDup := isDuplicateKeyError(0, originalErrorMsg)

	switch op.OpType {
	case "insert":
		var transformed interface{}
		if op.TransformedDocument != nil {
			transformed = op.TransformedDocument
		} else {
			var err error
			transformed, err = w.transformer.Transform(op.Document, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for fallback insert, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
				return
			}
			op.TransformedDocument = transformed
		}

		if isDup {
			// If duplicate key, retry using ReplaceOne with upsert=true to overwrite
			w.log.Debugf("[%s.%s] Fallback upserting duplicate insert document _id=%v", dbName, collName, op.DocumentID)
			err := w.executeWithRetry(ctx, func() error {
				_, replaceErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true))
				return replaceErr
			})
			if err != nil {
				w.log.Debugf("[%s.%s] Fallback upsert failed for insert document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
			} else {
				w.log.Debugf("[%s.%s] Successfully upserted document _id=%v after duplicate key error", dbName, collName, op.DocumentID)
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		} else {
			// For non-duplicate key errors, retry with standard InsertOne
			err := w.executeWithRetry(ctx, func() error {
				_, insertErr := targetCollection.InsertOne(ctx, transformed)
				return insertErr
			})
			if err != nil {
				// If it actually already exists (e.g. concurrent write or socket retry pre-exist), fallback to ReplaceOne upsert
				if isDuplicateKeyError(0, err.Error()) {
					if w.incrementalStatsManager != nil {
						w.incrementalStatsManager.IncrementDuplicateKeys(1)
					}
					w.log.Debugf("[%s.%s] Fallback InsertOne got duplicate key, retrying with upsert overwrite for _id=%v", dbName, collName, op.DocumentID)
					replaceErr := w.executeWithRetry(ctx, func() error {
						_, repErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true))
						return repErr
					})
					if replaceErr != nil {
						w.log.Errorf("[%s.%s] Fallback upsert replace after duplicate insert failed for document _id=%v: %v", dbName, collName, op.DocumentID, replaceErr)
						w.markDLQ(op, dbName, collName, replaceErr)
					} else {
						op.SuccessTime = time.Now()
						op.SuccessAfterRetry = true
					}
				} else {
					w.log.Errorf("[%s.%s] Fallback insert failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
					w.markDLQ(op, dbName, collName, err)
				}
			} else {
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		}

	case "update":
		if op.Document != nil {
			var transformed interface{}
			if op.TransformedDocument != nil {
				transformed = op.TransformedDocument
			} else {
				var err error
				transformed, err = w.transformer.Transform(op.Document, dbName, collName, op.DocumentID)
				if err != nil {
					w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace update, document _id=%v: %v", dbName, collName, op.DocumentID, err)
					w.markDLQ(op, dbName, collName, err)
					return
				}
				op.TransformedDocument = transformed
			}
			err := w.executeWithRetry(ctx, func() error {
				_, replaceErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true))
				return replaceErr
			})
			if err != nil {
				if err == context.Canceled || ctx.Err() == context.Canceled {
					w.log.Debugf("[%s.%s] Fallback replace update for document _id=%v canceled", dbName, collName, op.DocumentID)
					return
				}

				// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
				if isDuplicateKeyError(0, err.Error()) {
					if w.incrementalStatsManager != nil {
						w.incrementalStatsManager.IncrementDuplicateKeys(1)
					}
					w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
					retryErr := w.executeWithRetry(ctx, func() error {
						_, repErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(false))
						return repErr
					})
					if retryErr != nil {
						w.log.Errorf("[%s.%s] Fallback replace update without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
						w.markDLQ(op, dbName, collName, retryErr)
					} else {
						w.log.Debugf("[%s.%s] Successfully completed replace update for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
						op.SuccessTime = time.Now()
						op.SuccessAfterRetry = true
					}
				} else {
					w.log.Errorf("[%s.%s] Fallback replace update failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
					w.markDLQ(op, dbName, collName, err)
				}
			} else {
				op.SuccessTime = time.Now()
				op.SuccessAfterRetry = true
			}
		} else {
			w.log.Errorf("[%s.%s] Fallback update failed: document payload is nil for _id=%v", dbName, collName, op.DocumentID)
		}

	case "replace":
		var transformed interface{}
		if op.TransformedDocument != nil {
			transformed = op.TransformedDocument
		} else {
			var err error
			transformed, err = w.transformer.Transform(op.Document, dbName, collName, op.DocumentID)
			if err != nil {
				w.log.Errorf("[%s.%s] Field name transformation failed for fallback replace, document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
				return
			}
			op.TransformedDocument = transformed
		}
		err := w.executeWithRetry(ctx, func() error {
			_, replaceErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(true))
			return replaceErr
		})
		if err != nil {
			if err == context.Canceled || ctx.Err() == context.Canceled {
				w.log.Debugf("[%s.%s] Fallback replace for document _id=%v canceled due to context cancellation", dbName, collName, op.DocumentID)
				return
			}

			// If it failed because the document already exists (concurrent upsert collision), retry with upsert=false
			if isDuplicateKeyError(0, err.Error()) {
				if w.incrementalStatsManager != nil {
					w.incrementalStatsManager.IncrementDuplicateKeys(1)
				}
				w.log.Debugf("[%s.%s] Concurrent upsert collision detected for replace document _id=%v, retrying without upsert", dbName, collName, op.DocumentID)
				retryErr := w.executeWithRetry(ctx, func() error {
					_, repErr := targetCollection.ReplaceOne(ctx, filter, transformed, options.Replace().SetUpsert(false))
					return repErr
				})
				if retryErr != nil {
					w.log.Errorf("[%s.%s] Fallback replace without upsert failed for document _id=%v: %v", dbName, collName, op.DocumentID, retryErr)
					w.markDLQ(op, dbName, collName, retryErr)
				} else {
					w.log.Debugf("[%s.%s] Successfully completed replace for document _id=%v after concurrent upsert resolution", dbName, collName, op.DocumentID)
					op.SuccessTime = time.Now()
					op.SuccessAfterRetry = true
				}
			} else {
				w.log.Errorf("[%s.%s] Fallback replace failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
				w.markDLQ(op, dbName, collName, err)
			}
		} else {
			op.SuccessTime = time.Now()
			op.SuccessAfterRetry = true
		}

	case "delete":
		err := w.executeWithRetry(ctx, func() error {
			_, deleteErr := targetCollection.DeleteOne(ctx, filter)
			return deleteErr
		})
		if err != nil {
			w.log.Errorf("[%s.%s] Fallback delete failed for document _id=%v: %v", dbName, collName, op.DocumentID, err)
			w.markDLQ(op, dbName, collName, err)
		} else {
			op.SuccessTime = time.Now()
			op.SuccessAfterRetry = true
		}
	}
}

func (w *Worker) markDLQ(op *WriteOperation, dbName, collName string, err error) {
	// A delete whose _id cannot be represented in Firestore (e.g. a string _id over
	// 1500 bytes, or a reserved __x__ name) targets a document that could never have
	// been written to Firestore in the first place — its own insert would have failed
	// identically. The delete is therefore a vacuous no-op that will fail against the
	// target forever, pinning a permanent DLQ warning that no source fix can clear.
	// Treat it as resolved instead: mark the op successful so the group's resolution
	// pass writes a tombstone for any prior failure of this same _id.
	if op.OpType == "delete" && idUnrepresentableInFirestore(op.DocumentID) {
		w.log.Infof("[%s.%s] Delete of Firestore-unrepresentable _id=%v cannot apply to target (doc could never exist there); recording as resolved instead of DLQ", dbName, collName, op.DocumentID)
		op.SuccessTime = time.Now()
		return
	}
	if w.dlq != nil {
		w.dlq.WriteFailed(dbName, collName, op.DocumentID, err, "incremental", op.OpType, op.Document, op.EventTime)
	}
	op.DLQed = true
	op.Error = err

	// Track the failed document ID for Resolution Tombstoning
	if w.activeFailedMu != nil {
		w.activeFailedMu.Lock()
		if w.activeFailedIDs != nil {
			key := MakeDLQKey(dbName, collName, op.DocumentID)
			w.activeFailedIDs[key] = "incremental"
		}
		w.activeFailedMu.Unlock()
	}
}

// idUnrepresentableInFirestore reports whether an _id value can never be stored in
// Firestore under any transform — i.e. it trips a blocking (not auto-fixable) ID
// rule such as id-length (>1500 bytes) or id-reserved (__x__). Reuses the same
// assessment rules the pre-migration checker uses, so the two can never drift.
func idUnrepresentableInFirestore(id interface{}) bool {
	for _, f := range assess.CheckID(id) {
		if f.Severity == assess.SeverityBlock {
			return true
		}
	}
	return false
}

func (w *Worker) handleTransformationFailure(op *WriteOperation, dbName, collName string, err error) {
	w.log.Errorf("[%s.%s] Field name transformation failed for %s, document _id=%v: %v", dbName, collName, op.OpType, op.DocumentID, err)
	w.markDLQ(op, dbName, collName, err)
}

// isDuplicateKeyError checks if a write error code or message represents a duplicate key/document already exists constraint failure.
func isDuplicateKeyError(code int, msg string) bool {
	return code == 11000 ||
		strings.Contains(msg, "Document already exists") ||
		strings.Contains(msg, "E11000")
}

// buildWriteModel builds a single mongo.WriteModel from a WriteOperation, transforming Firestore-incompatible keys.
func (w *Worker) buildWriteModel(op *WriteOperation, dbName, collName string) (mongo.WriteModel, error) {
	if op.OpType == "delete" {
		return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": op.DocumentID}), nil
	}

	if op.OpType == "insert" || op.OpType == "update" || op.OpType == "replace" {
		if (op.OpType == "update" || op.OpType == "replace") && op.Document == nil {
			return nil, fmt.Errorf("document payload is nil for _id=%v", op.DocumentID)
		}

		transformed, err := w.transformer.Transform(op.Document, dbName, collName, op.DocumentID)
		if err != nil {
			return nil, err
		}
		op.TransformedDocument = transformed

		collConfig, exists := w.collectionConfigs[dbName][collName]
		useUpsert := op.OpType == "update" || op.OpType == "replace" || (exists && collConfig.UpsertMode)

		if useUpsert {
			return mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": op.DocumentID}).
				SetReplacement(transformed).
				SetUpsert(true), nil
		}

		return mongo.NewInsertOneModel().SetDocument(transformed), nil
	}

	return nil, fmt.Errorf("unsupported operation type: %s", op.OpType)
}

type workerLogger struct {
	workerID int
	logger   *logger.Logger
}

func (wl *workerLogger) Debug(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Debug(newArgs...)
}

func (wl *workerLogger) Debugf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Debugf("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Info(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Info(newArgs...)
}

func (wl *workerLogger) Infof(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Infof("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Warn(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Warn(newArgs...)
}

func (wl *workerLogger) Warnf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Warnf("Worker %d: "+format, newArgs...)
}

func (wl *workerLogger) Error(args ...interface{}) {
	newArgs := append([]interface{}{fmt.Sprintf("Worker %d: ", wl.workerID)}, args...)
	wl.logger.Error(newArgs...)
}

func (wl *workerLogger) Errorf(format string, args ...interface{}) {
	newArgs := make([]interface{}, 1+len(args))
	newArgs[0] = wl.workerID
	copy(newArgs[1:], args)
	wl.logger.Errorf("Worker %d: "+format, newArgs...)
}

const fnvOffset32 = 2166136261
const fnvPrime32 = 16777619

func hashBytes(data []byte) int {
	hash := uint32(fnvOffset32)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= fnvPrime32
	}
	return int(hash)
}

// StartPeriodicFlushLoop starts a goroutine that periodically checks all workers and flushes timed out groups
func StartPeriodicFlushLoop(ctx context.Context, workers []*Worker, flushInterval time.Duration, log *logger.Logger) {
	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				for _, worker := range workers {
					if worker == nil {
						continue
					}
					worker.mu.Lock()
					if worker.currentGroup != nil && len(worker.currentGroup.Operations) > 0 {
						if time.Since(worker.currentGroup.CreatedAt) >= flushInterval {
							log.Debugf("Flushing group in worker %d due to timeout: %s.%s with %d operations",
								worker.id, worker.currentGroup.Namespace, worker.currentGroup.OpType,
								len(worker.currentGroup.Operations))
							worker.flushCurrentGroup()
						}
					}
					worker.mu.Unlock()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// InFlightEvent represents a change stream event that is currently in-flight/processing
type InFlightEvent struct {
	SeqNum      uint64
	ResumeToken bson.Raw
	Acked       bool
	EventTime   time.Time
}

// PartitionTracker coordinates safe, backlogged-aware checkpointing of multi-partition change streams
type PartitionTracker struct {
	mu                 sync.Mutex
	log                *logger.Logger
	resumeTokenPath    string
	unackedEvents      map[int]map[uint64]*InFlightEvent
	pendingSeqs        map[int][]uint64
	nextSeqNum         map[int]uint64
	lastCheckpointSeq  map[int]uint64
	ackCountSinceSave  int
	saveThreshold      int
	checkpointInterval time.Duration
	lastSaveTime       time.Time
	totalPartitions    int
}

// NewPartitionTracker creates a new PartitionTracker
func NewPartitionTracker(log *logger.Logger, resumeTokenPath string, checkpointInterval time.Duration, saveThreshold int, totalPartitions int) *PartitionTracker {
	if checkpointInterval <= 0 {
		checkpointInterval = time.Minute
	}
	if saveThreshold <= 0 {
		saveThreshold = 1000
	}
	return &PartitionTracker{
		log:                log,
		resumeTokenPath:    resumeTokenPath,
		unackedEvents:      make(map[int]map[uint64]*InFlightEvent),
		pendingSeqs:        make(map[int][]uint64),
		nextSeqNum:         make(map[int]uint64),
		lastCheckpointSeq:  make(map[int]uint64),
		saveThreshold:      saveThreshold,
		checkpointInterval: checkpointInterval,
		lastSaveTime:       time.Now(),
		totalPartitions:    totalPartitions,
	}
}

// Start spawns a background periodic flush goroutine for PartitionTracker checkpoints
func (t *PartitionTracker) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(t.checkpointInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				t.mu.Lock()
				if t.ackCountSinceSave > 0 || time.Since(t.lastSaveTime) >= t.checkpointInterval {
					t.saveCheckpointsLocked()
				}
				t.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Register registers a newly read event for the given partition/streamIndex, returns its unique sequence number
func (t *PartitionTracker) Register(streamIndex int, resumeToken bson.Raw, eventTime time.Time) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	seq := t.nextSeqNum[streamIndex]
	if seq == 0 {
		seq = 1
	}
	t.nextSeqNum[streamIndex] = seq + 1

	event := &InFlightEvent{
		SeqNum:      seq,
		ResumeToken: resumeToken,
		Acked:       false,
		EventTime:   eventTime,
	}

	if t.unackedEvents[streamIndex] == nil {
		t.unackedEvents[streamIndex] = make(map[uint64]*InFlightEvent)
	}
	t.unackedEvents[streamIndex][seq] = event
	t.pendingSeqs[streamIndex] = append(t.pendingSeqs[streamIndex], seq)

	return seq
}

// Ack marks the given sequence number in the partition/streamIndex as successfully processed/completed
func (t *PartitionTracker) Ack(streamIndex int, seq uint64) {
	if seq == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if events, ok := t.unackedEvents[streamIndex]; ok {
		if event, ok := events[seq]; ok {
			if !event.Acked {
				event.Acked = true
				t.ackCountSinceSave++
			}
		}
	}

	if t.ackCountSinceSave >= t.saveThreshold || time.Since(t.lastSaveTime) >= t.checkpointInterval {
		t.saveCheckpointsLocked()
	}
}

// Close flushes final checkpoints and logs outstanding in-flight warnings
func (t *PartitionTracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.saveCheckpointsLocked()

	for streamIndex, seqs := range t.pendingSeqs {
		if len(seqs) > 0 {
			t.log.Warnf("[PartitionTracker] Shutdown complete with %d unacknowledged events in partition %d (first unacked seq: %d)", 
				len(seqs), streamIndex, seqs[0])
		}
	}
}

// saveCheckpointsLocked evaluates the consecutive ACK boundary and advances checkpoints on disk
func (t *PartitionTracker) saveCheckpointsLocked() {
	t.lastSaveTime = time.Now()
	t.ackCountSinceSave = 0

	for streamIndex, seqs := range t.pendingSeqs {
		var prunedCount int
		var highestSafeToken bson.Raw
		var highestSafeTime time.Time
		var highestSafeSeq uint64

		for _, seq := range seqs {
			event := t.unackedEvents[streamIndex][seq]
			if event != nil && event.Acked {
				highestSafeToken = event.ResumeToken
				highestSafeTime = event.EventTime
				highestSafeSeq = seq
				delete(t.unackedEvents[streamIndex], seq)
				prunedCount++
			} else {
				break
			}
		}

		if prunedCount > 0 {
			t.pendingSeqs[streamIndex] = t.pendingSeqs[streamIndex][prunedCount:]
		}

		if highestSafeSeq > 0 && highestSafeSeq > t.lastCheckpointSeq[streamIndex] {
			t.lastCheckpointSeq[streamIndex] = highestSafeSeq
			t.savePartitionResumeToken(streamIndex, highestSafeToken, highestSafeTime)
		}
	}
}

// savePartitionResumeToken persists the partition-specific resume token with rotation backups
func (t *PartitionTracker) savePartitionResumeToken(partitionIndex int, resumeToken bson.Raw, eventTime time.Time) {
	var resumeTokenDoc bson.M
	if err := bson.Unmarshal(resumeToken, &resumeTokenDoc); err != nil {
		t.log.Errorf("[PartitionTracker %d] Error unmarshaling resume token: %v", partitionIndex, err)
		return
	}

	path := GetPartitionResumeTokenPath(t.resumeTokenPath, partitionIndex, t.totalPartitions)
	if err := SaveResumeToken(path, resumeTokenDoc, eventTime); err != nil {
		t.log.Errorf("[PartitionTracker %d] Error saving resume token to %s: %v", partitionIndex, path, err)
	} else {
		t.log.Debugf("[PartitionTracker %d] Saved resume token successfully to %s (seq: %d)", partitionIndex, path, t.lastCheckpointSeq[partitionIndex])
	}
}

func (w *Worker) executeWithRetry(ctx context.Context, op func() error) error {
	err := op()
	if err != nil && w.retryManager != nil && err != context.Canceled && ctx.Err() != context.Canceled {
		errType := w.retryManager.ClassifyError(err)
		if errType == ErrorTypeConnection || errType == ErrorTypeContention {
			w.log.Infof("Transient error during fallback execution. Retrying with backoff...")
			err = w.retryManager.RetryWithBackoff(ctx, op)
		}
	}
	return err
}

