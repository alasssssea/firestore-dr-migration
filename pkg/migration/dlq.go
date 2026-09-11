package migration

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// DLQVersion represents the current DLQ record schema & reprocessing strategy version
const DLQVersion = "v1"

// DLQRecord represents a single failed document record in the Dead Letter Queue
type DLQRecord struct {
	SourceDB         string      `bson:"sourceDB" json:"sourceDB"`
	SourceCollection string      `bson:"sourceCollection" json:"sourceCollection"`
	DocumentID       interface{} `bson:"documentID" json:"documentID"`
	ResolvedID       interface{} `bson:"resolvedID,omitempty" json:"resolvedID,omitempty"`
	Error            string      `bson:"error,omitempty" json:"error,omitempty"`
	Phase            string      `bson:"phase" json:"phase"`
	OpType           string      `bson:"opType,omitempty" json:"opType,omitempty"`
	Timestamp        string      `bson:"timestamp" json:"timestamp"`
	EventTime        string      `bson:"eventTime,omitempty" json:"eventTime,omitempty"` // Time when the change event occurred (incremental phase only)
	Document         interface{} `bson:"document,omitempty" json:"document,omitempty"`
}

// DLQWriter provides thread-safe writing of failed documents to a JSONL file
type DLQWriter struct {
	filePath string
	file     *os.File
	mu       sync.Mutex
	count    int64 // atomic counter for total records written
	log      *logger.Logger
}

// NewDLQWriter creates a new DLQ writer that appends to the given file path.
// The file is created if it doesn't exist, or appended to if it does.
func NewDLQWriter(filePath string, log *logger.Logger) (*DLQWriter, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file %s: %w", filePath, err)
	}

	// If the file is newly created (size is 0), write the DLQ version header as the first line.
	fileInfo, err := file.Stat()
	if err == nil && fileInfo.Size() == 0 {
		header := map[string]string{"dlqVersion": DLQVersion}
		data, marshalErr := bson.MarshalExtJSON(header, false, false)
		if marshalErr == nil {
			data = append(data, '\n')
			_, _ = file.Write(data)
		}
	}

	log.Infof("DLQ file opened: %s", filePath)

	return &DLQWriter{
		filePath: filePath,
		file:     file,
		log:      log,
	}, nil
}

// WriteFailed appends a failed document record to the DLQ file.
// This method is safe to call from multiple goroutines.
func (w *DLQWriter) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}, eventTime time.Time) {
	if err != nil && (errors.Is(err, context.Canceled) || err.Error() == "context canceled" || strings.Contains(err.Error(), "context canceled")) {
		return
	}

	var eventTimeStr string
	if !eventTime.IsZero() {
		eventTimeStr = eventTime.Format(time.RFC3339)
	}

	record := DLQRecord{
		SourceDB:         sourceDB,
		SourceCollection: sourceCollection,
		DocumentID:       documentID,
		Error:            err.Error(),
		Phase:            phase,
		OpType:           opType,
		Timestamp:        time.Now().Format(time.RFC3339),
		EventTime:        eventTimeStr,
		Document:         document,
	}

	data, marshalErr := bson.MarshalExtJSON(record, false, false)
	if marshalErr != nil {
		w.log.Fatalf("DLQ: Failed to marshal record for %s.%s _id=%v: %v", sourceDB, sourceCollection, documentID, marshalErr)
	}

	// Append newline for JSONL format
	data = append(data, '\n')

	w.mu.Lock()
	// Thread-Safety Check: If the DLQ writer has been closed (e.g., during active worker shutdown or test cleanup),
	// w.file is nil. Writing to a nil file raises a nil pointer panic. We check this under the lock and abort safely.
	if w.file == nil {
		w.mu.Unlock()
		w.log.Warnf("DLQ: Attempted to write to a closed DLQ writer [db=%s, collection=%s, _id=%v]", sourceDB, sourceCollection, documentID)
		return
	}
	_, writeErr := w.file.Write(data)
	w.mu.Unlock()

	if writeErr != nil {
		w.log.Fatalf("DLQ: Failed to write record for %s.%s _id=%v: %v", sourceDB, sourceCollection, documentID, writeErr)
	}

	atomic.AddInt64(&w.count, 1)
	w.log.Warnf("DLQ: Document written to dead letter queue [db=%s, collection=%s, _id=%v, phase=%s, opType=%s, error=%v]",
		sourceDB, sourceCollection, documentID, phase, opType, err)
}

// WriteResolved appends a resolution/tombstone record to the DLQ file.
// This indicates a previously failed document ID has now been successfully replicated.
func (w *DLQWriter) WriteResolved(sourceDB, sourceCollection string, documentID interface{}, phase string, eventTime time.Time) {
	var eventTimeStr string
	if !eventTime.IsZero() {
		eventTimeStr = eventTime.Format(time.RFC3339)
	}

	record := DLQRecord{
		SourceDB:         sourceDB,
		SourceCollection: sourceCollection,
		ResolvedID:       documentID,
		Phase:            phase,
		Timestamp:        time.Now().Format(time.RFC3339),
		EventTime:        eventTimeStr,
	}

	data, marshalErr := bson.MarshalExtJSON(record, false, false)
	if marshalErr != nil {
		w.log.Fatalf("DLQ: Failed to marshal resolution record for %s.%s resolved_id=%v: %v", sourceDB, sourceCollection, documentID, marshalErr)
	}

	data = append(data, '\n')

	w.mu.Lock()
	if w.file == nil {
		w.mu.Unlock()
		w.log.Warnf("DLQ: Attempted to write resolution to a closed DLQ writer [db=%s, collection=%s, resolved_id=%v]", sourceDB, sourceCollection, documentID)
		return
	}
	_, writeErr := w.file.Write(data)
	w.mu.Unlock()

	if writeErr != nil {
		w.log.Fatalf("DLQ: Failed to write resolution record for %s.%s resolved_id=%v: %v", sourceDB, sourceCollection, documentID, writeErr)
	}

	atomic.AddInt64(&w.count, 1)
	w.log.Infof("DLQ: Resolution record written to dead letter queue [db=%s, collection=%s, resolved_id=%v, phase=%s]",
		sourceDB, sourceCollection, documentID, phase)
}

// Count returns the total number of records written to this DLQ file.
func (w *DLQWriter) Count() int64 {
	return atomic.LoadInt64(&w.count)
}

// FilePath returns the file path of this DLQ writer.
func (w *DLQWriter) FilePath() string {
	return w.filePath
}
// WriteRawLine appends a raw JSON line string directly to the DLQ file.
func (w *DLQWriter) WriteRawLine(line string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return fmt.Errorf("DLQ writer is closed")
	}
	_, err := w.file.Write([]byte(line + "\n"))
	if err == nil {
		atomic.AddInt64(&w.count, 1)
	}
	return err
}

// Close flushes and closes the DLQ file, logging a summary.
func (w *DLQWriter) Close() {
	if w.file == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		w.log.Errorf("DLQ: Failed to sync file %s: %v", w.filePath, err)
	}

	if err := w.file.Close(); err != nil {
		w.log.Errorf("DLQ: Failed to close file %s: %v", w.filePath, err)
	}

	count := atomic.LoadInt64(&w.count)
	if count > 0 {
		w.log.Warnf("DLQ: %d failed documents written to %s", count, w.filePath)
	} else {
		w.log.Infof("DLQ: No failed documents (file: %s)", w.filePath)
	}

	w.file = nil
}

// NopDLQWriter is a no-op DLQ writer that discards all records.
// Used as a safe default when DLQ is not configured.
type NopDLQWriter struct{}

// WriteFailed is a no-op for NopDLQWriter.
func (w *NopDLQWriter) WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}, eventTime time.Time) {
}

// WriteResolved is a no-op for NopDLQWriter.
func (w *NopDLQWriter) WriteResolved(sourceDB, sourceCollection string, documentID interface{}, phase string, eventTime time.Time) {
}

// FilePath returns empty string for NopDLQWriter.
func (w *NopDLQWriter) FilePath() string { return "" }

// Count always returns 0 for NopDLQWriter.
func (w *NopDLQWriter) Count() int64 { return 0 }

// Close is a no-op for NopDLQWriter.
func (w *NopDLQWriter) Close() {}

// DLQ is the interface that both DLQWriter and NopDLQWriter implement.
type DLQ interface {
	WriteFailed(sourceDB, sourceCollection string, documentID interface{}, err error, phase, opType string, document interface{}, eventTime time.Time)
	WriteResolved(sourceDB, sourceCollection string, documentID interface{}, phase string, eventTime time.Time)
	Count() int64
	Close()
	FilePath() string
}

// PopulateActiveFailedIDs reads the DLQ file and populates a map with active failed document IDs.
// It parses the file chronologically, letting resolutions delete previous failures.
func PopulateActiveFailedIDs(dlqPath string, log *logger.Logger) (map[string]string, error) {
	activeMap := make(map[string]string)

	if _, err := os.Stat(dlqPath); os.IsNotExist(err) {
		return activeMap, nil
	}

	file, err := os.Open(dlqPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file for pre-scan: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	isFirstLine := true
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		if isFirstLine {
			isFirstLine = false
			// Check if this is the version header line
			var header struct {
				DLQVersion string `bson:"dlqVersion"`
			}
			if err := bson.UnmarshalExtJSON([]byte(line), false, &header); err == nil && header.DLQVersion != "" {
				if header.DLQVersion != DLQVersion {
					return nil, fmt.Errorf("DLQ pre-scan: Safety violation: encountered unsupported DLQ version %q (expected %q). Please use the correct migration tool version.", header.DLQVersion, DLQVersion)
				}
				log.Infof("DLQ pre-scan: DLQ file version %q matches tool DLQ version %q", header.DLQVersion, DLQVersion)
				continue // skip header line
			}
			// If it's not a version header, process it as a regular legacy record
		}

		var record DLQRecord
		if err := bson.UnmarshalExtJSON([]byte(line), false, &record); err != nil {
			// Skip corrupted lines (logged as warnings) during pre-scan
			log.Warnf("DLQ pre-scan: skipping corrupted line: %v. Line: %s", err, line)
			continue
		}

		// Calculate unique key
		if record.ResolvedID != nil {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.ResolvedID)
			delete(activeMap, uniqueKey)
		} else {
			uniqueKey := MakeDLQKey(record.SourceDB, record.SourceCollection, record.DocumentID)
			activeMap[uniqueKey] = record.Phase
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading DLQ file during pre-scan: %w", err)
	}

	if len(activeMap) > 0 {
		phaseCounts := make(map[string]int)
		nsCounts := make(map[string]int)
		for key, phase := range activeMap {
			phaseCounts[phase]++
			parts := strings.SplitN(key, ":", 3)
			if len(parts) >= 2 {
				ns := parts[0] + "." + parts[1]
				nsCounts[ns]++
			}
		}

		var phases []string
		for p := range phaseCounts {
			phases = append(phases, p)
		}
		sort.Strings(phases)
		var phaseBreakdown []string
		for _, p := range phases {
			phaseBreakdown = append(phaseBreakdown, fmt.Sprintf("%s: %d", p, phaseCounts[p]))
		}

		var namespaces []string
		for ns := range nsCounts {
			namespaces = append(namespaces, ns)
		}
		sort.Strings(namespaces)
		var nsBreakdown []string
		for _, ns := range namespaces {
			nsBreakdown = append(nsBreakdown, fmt.Sprintf("%s: %d", ns, nsCounts[ns]))
		}

		log.Infof("DLQ pre-scan findings: active failed IDs count = %d [phase breakdown: %s] [namespace breakdown: %s]",
			len(activeMap),
			strings.Join(phaseBreakdown, ", "),
			strings.Join(nsBreakdown, ", "),
		)
	} else {
		log.Info("DLQ pre-scan findings: 0 active failed IDs found")
	}

	return activeMap, nil
}

// SerializeID converts any document ID type into a deterministic unique string.
// Optimizes standard ObjectID and string types to bypass CPU allocations.
func SerializeID(id interface{}) string {
	if id == nil {
		return "nil"
	}
	switch val := id.(type) {
	case string:
		return "s:" + val
	case primitive.ObjectID:
		return "o:" + val.Hex()
	default:
		data, err := bson.Marshal(bson.M{"id": id})
		if err == nil {
			return string(data)
		}
		return fmt.Sprintf("v:%v", id)
	}
}

// MakeDLQKey constructs a deterministic unique key for a document ID within a collection.
func MakeDLQKey(dbName, collName string, docID interface{}) string {
	return dbName + ":" + collName + ":" + SerializeID(docID)
}

// HasActiveFailedRecords checks if the DLQ file exists and contains any non-header, non-empty records.
func HasActiveFailedRecords(filePath string) (bool, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open DLQ file for check: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Skip version header line
		if strings.Contains(line, "dlqVersion") {
			continue
		}
		// Found at least one actual failed record line!
		return true, nil
	}
	return false, scanner.Err()
}

// BackupAndClearDLQ renames the DLQ file if it exists and contains records, appending a timestamp to preserve history.
func BackupAndClearDLQ(filePath string, log *logger.Logger) error {
	hasFailed, err := HasActiveFailedRecords(filePath)
	if err != nil {
		return err
	}
	if !hasFailed {
		return nil
	}
	backupPath := fmt.Sprintf("%s.backup-%s", filePath, time.Now().Format("20060102-150405"))
	log.Infof("DLQ: Active failures from a previous interrupted run found. Backing up DLQ file to %s and starting fresh.", backupPath)
	if err := os.Rename(filePath, backupPath); err != nil {
		log.Fatalf("DLQ: Failed to backup DLQ file from %s to %s: %v", filePath, backupPath, err)
	}
	return nil
}

