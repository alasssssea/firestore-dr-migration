package migration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestDLQWriterInitialCountIsZero(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	if writer.Count() != 0 {
		t.Errorf("expected initial count to be 0, got %d", writer.Count())
	}
}

func TestDLQWriterIncrementsCountOnWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	writer.WriteFailed("source_db", "col_1", "doc_id_123", errors.New("timeout"), "initial", "insert", nil, time.Time{})
	writer.WriteFailed("source_db", "col_1", "doc_id_456", errors.New("dup key"), "incremental", "replace", nil, time.Now())

	if writer.Count() != 2 {
		t.Errorf("expected count to be 2, got %d", writer.Count())
	}
}

func TestDLQWriterWritesJSONLRecordsToDisk(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "test_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	testTime := time.Date(2026, 5, 20, 21, 0, 0, 0, time.UTC)
	err1 := errors.New("connection timed out")
	writer.WriteFailed("source_db", "col_1", "doc_id_123", err1, "initial", "insert", map[string]interface{}{"name": "value"}, time.Time{})

	err2 := errors.New("duplicate key error")
	writer.WriteFailed("source_db", "col_1", "doc_id_456", err2, "incremental", "replace", map[string]interface{}{"name": "value2"}, testTime)

	writer.Close()

	file, err := os.Open(dlqFilePath)
	if err != nil {
		t.Fatalf("failed to open DLQ file for verification: %v", err)
	}
	defer file.Close()

	var records []DLQRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "dlqVersion") {
			continue
		}
		var record DLQRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to parse DLQ line: %v", err)
		}
		records = append(records, record)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records on disk, got %d", len(records))
	}

	// Verify record 1
	r1 := records[0]
	if r1.SourceDB != "source_db" || r1.SourceCollection != "col_1" || r1.DocumentID != "doc_id_123" {
		t.Errorf("invalid record 1 metadata: %+v", r1)
	}
	if r1.Error != "connection timed out" || r1.Phase != "initial" || r1.OpType != "insert" {
		t.Errorf("invalid record 1 error/phase: %+v", r1)
	}

	// Verify record 2
	r2 := records[1]
	if r2.SourceDB != "source_db" || r2.SourceCollection != "col_1" || r2.DocumentID != "doc_id_456" {
		t.Errorf("invalid record 2 metadata: %+v", r2)
	}
	if r2.Error != "duplicate key error" || r2.Phase != "incremental" || r2.OpType != "replace" {
		t.Errorf("invalid record 2 error/phase: %+v", r2)
	}
	if r2.EventTime != testTime.Format(time.RFC3339) {
		t.Errorf("expected EventTime %s, got %s", testTime.Format(time.RFC3339), r2.EventTime)
	}
}

func TestDLQWriterConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "concurrency_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	writesPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				writer.WriteFailed(
					"db",
					"col",
					j,
					errors.New("concurrent error"),
					"initial",
					"insert",
					nil,
					time.Time{},
				)
			}
		}(i)
	}

	wg.Wait()

	expectedCount := int64(numGoroutines * writesPerGoroutine)
	if writer.Count() != expectedCount {
		t.Errorf("expected count to be %d, got %d", expectedCount, writer.Count())
	}
}

func TestNopDLQWriter(t *testing.T) {
	nop := &NopDLQWriter{}
	nop.WriteFailed("db", "col", "id", errors.New("test"), "initial", "insert", nil, time.Time{})

	if nop.Count() != 0 {
		t.Errorf("expected NopDLQWriter count to always be 0")
	}

	// Should not panic
	nop.Close()
}

// TestDLQWriterWriteFailedAfterClose verifies post-close thread-safety and panic prevention.
// When DLQWriter is closed, the underlying file resource is set to nil. Concurrently invoking
// WriteFailed from multiple worker threads must be handled gracefully, raising clean error logs but NEVER panicking.
func TestDLQWriterWriteFailedAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "closed_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	// Close the writer immediately to set w.file = nil
	writer.Close()

	// Concurrently call WriteFailed and expect no panics
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Should log error but NOT panic!
			writer.WriteFailed("db", "col", id, errors.New("write after close"), "incremental", "insert", nil, time.Time{})
		}(i)
	}
	wg.Wait()
}

// TestDLQWriterFiltersContextCanceled verifies that DLQWriter.WriteFailed ignores context cancellation errors.
func TestDLQWriterFiltersContextCanceled(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "canceled_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer writer.Close()

	// 1. Write standard errors (should increment count)
	writer.WriteFailed("db", "col", "id1", errors.New("some standard error"), "initial", "insert", nil, time.Time{})

	// 2. Write context.Canceled error (should be ignored)
	writer.WriteFailed("db", "col", "id2", context.Canceled, "initial", "insert", nil, time.Time{})

	// 3. Write custom string error containing "context canceled" (should be ignored)
	writer.WriteFailed("db", "col", "id3", errors.New("context canceled"), "initial", "insert", nil, time.Time{})
	writer.WriteFailed("db", "col", "id4", errors.New("wrapped error: context canceled"), "initial", "insert", nil, time.Time{})

	if writer.Count() != 1 {
		t.Errorf("expected count to be 1, got %d", writer.Count())
	}
}

// TestDLQWriterBSONTypesExtendedJSON verifies that DLQWriter handles special BSON types
// (like ObjectID, DateTime, Binary, NaN, and Infinity) using Relaxed Extended JSON
// without any serialization failures or precision/data loss.
func TestDLQWriterBSONTypesExtendedJSON(t *testing.T) {
	tmpDir := t.TempDir()
	dlqFilePath := filepath.Join(tmpDir, "bson_types_dlq.jsonl")
	log := logger.New()

	writer, err := NewDLQWriter(dlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}

	docID := primitive.NewObjectID()
	testTime := time.Date(2026, 5, 20, 21, 0, 0, 0, time.UTC)
	binaryData := primitive.Binary{Subtype: 0x00, Data: []byte{1, 2, 3}}
	nanVal := math.NaN()
	infVal := math.Inf(1)
	decimalVal, err := primitive.ParseDecimal128("123.456")
	if err != nil {
		t.Fatalf("failed to parse decimal128: %v", err)
	}

	// Create document containing various standard BSON datatypes
	doc := bson.D{
		{Key: "_id", Value: docID},
		{Key: "double_field", Value: float64(1.23)},
		{Key: "string_field", Value: "hello world"},
		{Key: "doc_field", Value: bson.D{{Key: "nested_key", Value: "nested_val"}}},
		{Key: "array_field", Value: bson.A{1, "two", 3.0}},
		{Key: "binary_field", Value: binaryData},
		{Key: "bool_field", Value: true},
		{Key: "datetime_field", Value: primitive.NewDateTimeFromTime(testTime)},
		{Key: "null_field", Value: nil},
		{Key: "regex_field", Value: primitive.Regex{Pattern: "^abc$", Options: "i"}},
		{Key: "javascript_field", Value: primitive.JavaScript("function() { return 1; }")},
		{Key: "symbol_field", Value: primitive.Symbol("test_symbol")},
		{Key: "code_with_scope_field", Value: primitive.CodeWithScope{Code: "function(y) { return y; }", Scope: bson.D{{Key: "y", Value: 42}}}},
		{Key: "int32_field", Value: int32(100)},
		{Key: "timestamp_field", Value: primitive.Timestamp{T: 12345, I: 2}},
		{Key: "int64_field", Value: int64(9876543210)},
		{Key: "decimal128_field", Value: decimalVal},
		{Key: "minkey_field", Value: primitive.MinKey{}},
		{Key: "maxkey_field", Value: primitive.MaxKey{}},
		{Key: "nan_field", Value: nanVal},
		{Key: "inf_field", Value: infVal},
	}

	// This should NOT fail to marshal, and should successfully write to disk
	writer.WriteFailed("test_db", "test_coll", docID, errors.New("bson validation error"), "initial", "insert", doc, testTime)
	writer.Close()

	// Read and verify EJSON contents
	file, err := os.Open(dlqFilePath)
	if err != nil {
		t.Fatalf("failed to open DLQ file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var line []byte
	for scanner.Scan() {
		l := scanner.Bytes()
		if strings.Contains(string(l), "dlqVersion") {
			continue
		}
		line = l
		break
	}
	if len(line) == 0 {
		t.Fatal("expected at least 1 record in DLQ, got 0")
	}

	// Unmarshal line back using BSON Extended JSON parser to verify zero loss
	var record bson.M
	if err := bson.UnmarshalExtJSON(line, false, &record); err != nil {
		t.Fatalf("failed to unmarshal Extended JSON: %v. Raw line: %s", err, string(line))
	}

	// Verify metadata
	if record["sourceDB"] != "test_db" || record["sourceCollection"] != "test_coll" {
		t.Errorf("invalid sourceDB/Collection metadata: %+v", record)
	}
	parsedDocID, ok := record["documentID"].(primitive.ObjectID)
	if !ok || parsedDocID != docID {
		t.Errorf("expected documentID to be ObjectID %v, got %v (type: %T)", docID, record["documentID"], record["documentID"])
	}
	parsedEventTime, ok := record["eventTime"].(string)
	if !ok || parsedEventTime != testTime.Format(time.RFC3339) {
		t.Errorf("expected eventTime %s, got %v (type: %T)", testTime.Format(time.RFC3339), record["eventTime"], record["eventTime"])
	}

	// Verify parsed bson document types
	rawDoc, ok := record["document"].(bson.M)
	if !ok {
		t.Fatalf("expected document field of type bson.M, got %T", record["document"])
	}

	// 1. ObjectID check
	parsedID, ok := rawDoc["_id"].(primitive.ObjectID)
	if !ok || parsedID != docID {
		t.Errorf("expected ObjectID %v, got %v (type: %T)", docID, rawDoc["_id"], rawDoc["_id"])
	}

	// 2. Double check
	parsedDouble, ok := rawDoc["double_field"].(float64)
	if !ok || parsedDouble != 1.23 {
		t.Errorf("expected Double 1.23, got %v (type: %T)", rawDoc["double_field"], rawDoc["double_field"])
	}

	// 3. String check
	parsedStr, ok := rawDoc["string_field"].(string)
	if !ok || parsedStr != "hello world" {
		t.Errorf("expected String 'hello world', got %v (type: %T)", rawDoc["string_field"], rawDoc["string_field"])
	}

	// 4. Document check
	parsedDoc, ok := rawDoc["doc_field"].(bson.M)
	if !ok || parsedDoc["nested_key"] != "nested_val" {
		t.Errorf("expected nested document nested_key=nested_val, got %+v (type: %T)", rawDoc["doc_field"], rawDoc["doc_field"])
	}

	// 5. Array check
	parsedArr, ok := rawDoc["array_field"].(primitive.A)
	if !ok || len(parsedArr) != 3 || parsedArr[0] != int32(1) || parsedArr[1] != "two" || parsedArr[2] != float64(3.0) {
		t.Errorf("expected Array [1, 'two', 3.0], got %v (type: %T)", rawDoc["array_field"], rawDoc["array_field"])
	}

	// 6. Binary check
	parsedBin, ok := rawDoc["binary_field"].(primitive.Binary)
	if !ok || parsedBin.Subtype != binaryData.Subtype || string(parsedBin.Data) != string(binaryData.Data) {
		t.Errorf("expected Binary %v, got %v (type: %T)", binaryData, rawDoc["binary_field"], rawDoc["binary_field"])
	}

	// 7. Boolean check
	parsedBool, ok := rawDoc["bool_field"].(bool)
	if !ok || !parsedBool {
		t.Errorf("expected Boolean true, got %v (type: %T)", rawDoc["bool_field"], rawDoc["bool_field"])
	}

	// 8. DateTime check
	parsedTime, ok := rawDoc["datetime_field"].(primitive.DateTime)
	if !ok || parsedTime.Time().UTC() != testTime {
		t.Errorf("expected DateTime %v, got %v (type: %T)", testTime, rawDoc["datetime_field"], rawDoc["datetime_field"])
	}

	// 9. Null check
	if rawDoc["null_field"] != nil {
		t.Errorf("expected Null nil, got %v (type: %T)", rawDoc["null_field"], rawDoc["null_field"])
	}

	// 10. Regex check
	parsedRegex, ok := rawDoc["regex_field"].(primitive.Regex)
	if !ok || parsedRegex.Pattern != "^abc$" || parsedRegex.Options != "i" {
		t.Errorf("expected Regex pattern='^abc$', options='i', got %v (type: %T)", rawDoc["regex_field"], rawDoc["regex_field"])
	}

	// 11. JavaScript check
	parsedJS, ok := rawDoc["javascript_field"].(primitive.JavaScript)
	if !ok || string(parsedJS) != "function() { return 1; }" {
		t.Errorf("expected JavaScript code, got %v (type: %T)", rawDoc["javascript_field"], rawDoc["javascript_field"])
	}

	// 12. Symbol check
	parsedSymbol, ok := rawDoc["symbol_field"].(primitive.Symbol)
	if !ok || string(parsedSymbol) != "test_symbol" {
		t.Errorf("expected Symbol 'test_symbol', got %v (type: %T)", rawDoc["symbol_field"], rawDoc["symbol_field"])
	}

	// 13. CodeWithScope check
	parsedCodeScope, ok := rawDoc["code_with_scope_field"].(primitive.CodeWithScope)
	if !ok || parsedCodeScope.Code != "function(y) { return y; }" || parsedCodeScope.Scope == nil {
		t.Errorf("expected CodeWithScope, got %v (type: %T)", rawDoc["code_with_scope_field"], rawDoc["code_with_scope_field"])
	}

	// 14. Int32 check
	parsedInt32, ok := rawDoc["int32_field"].(int32)
	if !ok || parsedInt32 != 100 {
		t.Errorf("expected Int32 100, got %v (type: %T)", rawDoc["int32_field"], rawDoc["int32_field"])
	}

	// 15. BSON Timestamp check
	parsedTS, ok := rawDoc["timestamp_field"].(primitive.Timestamp)
	if !ok || parsedTS.T != 12345 || parsedTS.I != 2 {
		t.Errorf("expected BSON Timestamp T=12345 I=2, got %v (type: %T)", rawDoc["timestamp_field"], rawDoc["timestamp_field"])
	}

	// 16. Int64 check
	parsedInt64, ok := rawDoc["int64_field"].(int64)
	if !ok || parsedInt64 != 9876543210 {
		t.Errorf("expected Int64 9876543210, got %v (type: %T)", rawDoc["int64_field"], rawDoc["int64_field"])
	}

	// 17. Decimal128 check
	parsedDec, ok := rawDoc["decimal128_field"].(primitive.Decimal128)
	if !ok || parsedDec.String() != decimalVal.String() {
		t.Errorf("expected Decimal128 %v, got %v (type: %T)", decimalVal, rawDoc["decimal128_field"], rawDoc["decimal128_field"])
	}

	// 18. MinKey check
	_, ok = rawDoc["minkey_field"].(primitive.MinKey)
	if !ok {
		t.Errorf("expected MinKey, got %v (type: %T)", rawDoc["minkey_field"], rawDoc["minkey_field"])
	}

	// 19. MaxKey check
	_, ok = rawDoc["maxkey_field"].(primitive.MaxKey)
	if !ok {
		t.Errorf("expected MaxKey, got %v (type: %T)", rawDoc["maxkey_field"], rawDoc["maxkey_field"])
	}

	// 20. NaN check
	parsedNaN, ok := rawDoc["nan_field"].(float64)
	if !ok || !math.IsNaN(parsedNaN) {
		t.Errorf("expected NaN float, got %v (type: %T)", rawDoc["nan_field"], rawDoc["nan_field"])
	}

	// 21. Infinity check
	parsedInf, ok := rawDoc["inf_field"].(float64)
	if !ok || !math.IsInf(parsedInf, 1) {
		t.Errorf("expected +Inf float, got %v (type: %T)", rawDoc["inf_field"], rawDoc["inf_field"])
	}
}

func TestHasActiveFailedRecords(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_dlq.jsonl")

	// 1. File does not exist
	hasRecords, err := HasActiveFailedRecords(filePath)
	if err != nil {
		t.Fatalf("unexpected error checking non-existent file: %v", err)
	}
	if hasRecords {
		t.Errorf("expected false for non-existent file")
	}

	// 2. File exists but is empty
	err = os.WriteFile(filePath, []byte(""), 0644)
	if err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}
	hasRecords, err = HasActiveFailedRecords(filePath)
	if err != nil {
		t.Fatalf("unexpected error checking empty file: %v", err)
	}
	if hasRecords {
		t.Errorf("expected false for empty file")
	}

	// 3. File contains only version header
	err = os.WriteFile(filePath, []byte("{\"dlqVersion\":\"v1\"}\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write version header: %v", err)
	}
	hasRecords, err = HasActiveFailedRecords(filePath)
	if err != nil {
		t.Fatalf("unexpected error checking header-only file: %v", err)
	}
	if hasRecords {
		t.Errorf("expected false for header-only file")
	}

	// 4. File contains version header and empty lines
	err = os.WriteFile(filePath, []byte("{\"dlqVersion\":\"v1\"}\n\n  \n"), 0644)
	if err != nil {
		t.Fatalf("failed to write header and empty lines: %v", err)
	}
	hasRecords, err = HasActiveFailedRecords(filePath)
	if err != nil {
		t.Fatalf("unexpected error checking header and empty lines: %v", err)
	}
	if hasRecords {
		t.Errorf("expected false for header and empty lines")
	}

	// 5. File contains version header and an actual record
	err = os.WriteFile(filePath, []byte("{\"dlqVersion\":\"v1\"}\n{\"sourceDB\":\"db\",\"sourceCollection\":\"coll\",\"documentID\":\"id1\"}\n"), 0644)
	if err != nil {
		t.Fatalf("failed to write record: %v", err)
	}
	hasRecords, err = HasActiveFailedRecords(filePath)
	if err != nil {
		t.Fatalf("unexpected error checking non-empty file: %v", err)
	}
	if !hasRecords {
		t.Errorf("expected true for file with record")
	}
}
