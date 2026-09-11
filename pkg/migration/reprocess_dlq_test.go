package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mockTargetCollection struct {
	replaceCount int
	deleteCount  int
	failReplace  bool
	lastReplace  interface{} // captures the last replacement doc for assertions
}

func (m *mockTargetCollection) ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	m.replaceCount++
	m.lastReplace = replacement
	if m.failReplace {
		return nil, errors.New("mock write error")
	}
	return nil, nil
}

func (m *mockTargetCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	m.deleteCount++
	return nil, nil
}

func TestReprocessDLQLoopSuccess(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	// 1. Create a mock DLQ file containing two records of same phase (initial)
	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	records := []string{
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id1","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:00Z","document":{"_id":"id1","name":"val1"}}`,
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id2","error":"err","phase":"initial","opType":"delete","timestamp":"2026-06-10T00:00:01Z"}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection {
		return mockColl
	}

	// Act
	_, _, err = m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, nil)
	if err != nil {
		t.Fatalf("expected reprocessDLQLoop to succeed, got: %v", err)
	}

	// Assert: ReplaceOne and DeleteOne were called correctly
	if mockColl.replaceCount != 1 {
		t.Errorf("expected 1 ReplaceOne call, got %d", mockColl.replaceCount)
	}
	if mockColl.deleteCount != 1 {
		t.Errorf("expected 1 DeleteOne call, got %d", mockColl.deleteCount)
	}

	// Verify new DLQ is empty (succeeded count is 2, active file count is 0)
	if newDLQ.Count() != 0 {
		t.Errorf("expected new DLQ count to be 0, got %d", newDLQ.Count())
	}
}

// TestReprocessDLQLoopSourceResync exercises the source-resync path: found docs
// are re-read fresh from the source and replayed, source-deleted docs are treated
// as resolved (skipped, target untouched), and read errors keep the doc in the DLQ.
func TestReprocessDLQLoopSourceResync(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Three failures, all initial/insert. The stored snapshots carry the ORIGINAL
	// (bad) value; source-resync must ignore them and use the fetcher's fresh copy.
	records := []string{
		`{"sourceDB":"src","sourceCollection":"coll1","documentID":"idFound","error":"too big","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:00Z","document":{"_id":"idFound","name":"OLD_BAD"}}`,
		`{"sourceDB":"src","sourceCollection":"coll1","documentID":"idDeleted","error":"too big","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:01Z","document":{"_id":"idDeleted","name":"OLD_BAD"}}`,
		`{"sourceDB":"src","sourceCollection":"coll1","documentID":"idReadErr","error":"too big","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:02Z","document":{"_id":"idReadErr","name":"OLD_BAD"}}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection { return mockColl }

	// Fake source: idFound returns a FIXED fresh doc, idDeleted is gone, idReadErr errors.
	sourceFetch := func(collection string, id interface{}) (interface{}, bool, error) {
		switch id {
		case "idFound":
			return bson.M{"_id": "idFound", "name": "FIXED_FRESH"}, true, nil
		case "idDeleted":
			return nil, false, nil
		case "idReadErr":
			return nil, false, errors.New("source read boom")
		}
		return nil, false, nil
	}

	_, failed, err := m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, sourceFetch)
	if err != nil {
		t.Fatalf("expected reprocessDLQLoop to succeed, got: %v", err)
	}

	// Only idFound should have been written to the target (idDeleted skipped, idReadErr failed pre-write).
	if mockColl.replaceCount != 1 {
		t.Errorf("expected 1 ReplaceOne (idFound only), got %d", mockColl.replaceCount)
	}
	// The written doc must be the FRESH source copy, not the stored bad snapshot.
	if repl, ok := mockColl.lastReplace.(map[string]interface{}); ok {
		if repl["name"] != "FIXED_FRESH" {
			t.Errorf("expected fresh source doc (name=FIXED_FRESH), got %v", repl["name"])
		}
	} else if repl, ok := mockColl.lastReplace.(bson.M); ok {
		if repl["name"] != "FIXED_FRESH" {
			t.Errorf("expected fresh source doc (name=FIXED_FRESH), got %v", repl["name"])
		}
	} else {
		t.Errorf("unexpected replacement type %T: %v", mockColl.lastReplace, mockColl.lastReplace)
	}
	// idReadErr stays in the DLQ; idDeleted is resolved (not written).
	if failed != 1 {
		t.Errorf("expected failed count 1 (idReadErr), got %d", failed)
	}
	if newDLQ.Count() != 1 {
		t.Errorf("expected new DLQ count 1 (idReadErr kept), got %d", newDLQ.Count())
	}
}

func TestReprocessDLQLoopPhaseMismatch(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Create DLQ with mixed phases: first is "initial", second is "incremental"
	records := []string{
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id1","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:00Z","document":{"_id":"id1"}}`,
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id2","error":"err","phase":"incremental","opType":"insert","timestamp":"2026-06-10T00:00:01Z","document":{"_id":"id2"}}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection {
		return mockColl
	}

	// Act
	_, _, err = m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, nil)

	// Assert: should fail due to safety violation
	if err == nil {
		t.Fatal("expected error due to mixed phases, got nil")
	}
	if !strings.Contains(err.Error(), "Safety violation: encountered mixed phases in DLQ file") {
		t.Errorf("unexpected error: %v", err)
	}

	// Assert: phase mismatch prevents executing any DB writes
	if mockColl.replaceCount != 0 || mockColl.deleteCount != 0 {
		t.Errorf("expected 0 write/delete calls, got replace=%d, delete=%d", mockColl.replaceCount, mockColl.deleteCount)
	}
}

func TestReprocessDLQLoopRecoveryOnCancellation(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Create DLQ with three records
	records := []string{
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id1","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:00Z","document":{"_id":"id1"}}`,
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id2","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:01Z","document":{"_id":"id2"}}`,
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id3","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:02Z","document":{"_id":"id3"}}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection {
		if mockColl.replaceCount == 1 {
			// Cancel context midway to trigger loop cancellation on next iteration
			cancel()
		}
		return mockColl
	}

	// Act
	_, _, err = m.reprocessDLQLoop(ctx, tempFilePath, newDLQ, getCollection, nil)
	if err == nil {
		t.Fatal("expected error due to context cancellation, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}

	// Verify that at least one write was aborted and didn't execute
	if mockColl.replaceCount >= 3 {
		t.Errorf("expected less than 3 processed records due to cancellation, got %d", mockColl.replaceCount)
	}
}

func TestReprocessDLQFileDoesNotExist(t *testing.T) {
	log := logger.New()
	cfg := &config.Config{
		DatabasePairs: []config.DatabasePair{
			{
				Source: config.SourceConfig{Database: "source_db"},
				Target: config.TargetConfig{Database: "target_db"},
			},
		},
	}
	m := NewMigrator(cfg, log)
	ctx := context.Background()

	_ = os.Remove("dlq-global.jsonl")

	err := m.reprocessDLQ(ctx, cfg.DatabasePairs[0], 0)
	if err != nil {
		t.Errorf("expected no error when DLQ file does not exist, got: %v", err)
	}
}

func TestReprocessDLQConnectionFailure(t *testing.T) {
	log := logger.New()

	dlqPath := "dlq-global.jsonl"
	_ = os.Remove(dlqPath)
	_ = os.Remove(dlqPath + ".retry-temp")
	if err := os.WriteFile(dlqPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to create dummy DLQ file: %v", err)
	}
	defer func() {
		_ = os.Remove(dlqPath)
		_ = os.Remove(dlqPath + ".retry-temp")
	}()

	cfg := &config.Config{
		DatabasePairs: []config.DatabasePair{
			{
				Source: config.SourceConfig{ConnectionString: "mongodb://invalid-host-name-xyz:27017/?serverSelectionTimeoutMS=2000", Database: "source_db"},
				Target: config.TargetConfig{ConnectionString: "mongodb://invalid-host-name-xyz:27017/?serverSelectionTimeoutMS=2000", Database: "target_db"},
			},
		},
	}
	m := NewMigrator(cfg, log)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := m.reprocessDLQ(ctx, cfg.DatabasePairs[0], 0)
	if err == nil {
		t.Fatal("expected error due to invalid connection string, got nil")
	}
	if !strings.Contains(err.Error(), "failed to connect to target MongoDB") && !strings.Contains(err.Error(), "no such host") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestReprocessDLQLoopUnmarshalError(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Create DLQ with a corrupted second line
	records := []string{
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id1","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:00Z","document":{"_id":"id1"}}`,
		`{corrupted_invalid_json}`,
		`{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id3","error":"err","phase":"initial","opType":"insert","timestamp":"2026-06-10T00:00:02Z","document":{"_id":"id3"}}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection {
		return mockColl
	}

	// Act
	_, _, err = m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, nil)

	// Assert: should fail due to unmarshal error
	if err == nil {
		t.Fatal("expected error due to corrupted json line, got nil")
	}
	if !strings.Contains(err.Error(), "Failed to unmarshal record") {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify: unmarshal failure in Pass 1 prevents any target collection write execution
	if mockColl.replaceCount != 0 || mockColl.deleteCount != 0 {
		t.Errorf("expected 0 write/delete calls, got replace=%d, delete=%d", mockColl.replaceCount, mockColl.deleteCount)
	}
}

func TestReprocessDLQMergeRecovery(t *testing.T) {
	log := logger.New()

	dlqPath := "dlq-global.jsonl"
	tempPath := dlqPath + ".retry-temp"

	_ = os.Remove(dlqPath)
	_ = os.Remove(tempPath)

	// Write record1 to active DLQ file
	record1 := `{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id1","error":"err","phase":"initial","opType":"insert"}`
	if err := os.WriteFile(dlqPath, []byte(record1+"\n"), 0644); err != nil {
		t.Fatalf("failed to create dummy DLQ file: %v", err)
	}

	// Write record2 to leftover temp file
	record2 := `{"sourceDB":"source_db","sourceCollection":"coll1","documentID":"id2","error":"err","phase":"initial","opType":"insert"}`
	if err := os.WriteFile(tempPath, []byte(record2+"\n"), 0644); err != nil {
		t.Fatalf("failed to create dummy temp file: %v", err)
	}

	defer func() {
		_ = os.Remove(dlqPath)
		_ = os.Remove(tempPath)
		_ = os.Remove(dlqPath + ".retry-temp")
	}()

	cfg := &config.Config{
		DatabasePairs: []config.DatabasePair{
			{
				Source: config.SourceConfig{ConnectionString: "mongodb://invalid-host-name-xyz:27017/?serverSelectionTimeoutMS=100", Database: "source_db"},
				Target: config.TargetConfig{ConnectionString: "mongodb://invalid-host-name-xyz:27017/?serverSelectionTimeoutMS=100", Database: "target_db"},
			},
		},
	}
	m := NewMigrator(cfg, log)
	ctx := context.Background()

	// Run reprocessDLQ. It will merge files first, then fail to connect to DB.
	err := m.reprocessDLQ(ctx, cfg.DatabasePairs[0], 0)
	if err == nil {
		t.Fatal("expected error due to invalid connection string, got nil")
	}

	// Verify that the files were recovery-restored!
	// Since connection fails, the defer block in reprocessDLQ discards the active DLQ
	// and restores tempPath back to dlqPath (containing ONLY the pre-retry record2).
	data, readErr := os.ReadFile(dlqPath)
	if readErr != nil {
		t.Fatalf("failed to read DLQ file after recovery: %v", readErr)
	}
 
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line in DLQ (restored retry-temp), got %d. Content:\n%s", len(lines), string(data))
	}
	if !strings.Contains(lines[0], `"documentID":"id2"`) {
		t.Errorf("expected restored DLQ line to contain id2, got: %s", lines[0])
	}

	// Verify that the temp file was cleaned up
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Errorf("expected temp file to be deleted, but it still exists")
	}
}

func TestSerializeID(t *testing.T) {
	// 1. String ID
	sVal := "my-string-id"
	if res := SerializeID(sVal); res != "s:my-string-id" {
		t.Errorf("expected s:my-string-id, got %q", res)
	}

	// 2. ObjectID ID
	oid := primitive.NewObjectID()
	if res := SerializeID(oid); res != "o:"+oid.Hex() {
		t.Errorf("expected o:%s, got %q", oid.Hex(), res)
	}

	// 3. Complex BSON Document ID
	docID := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "val"}}
	res1 := SerializeID(docID)
	res2 := SerializeID(docID)
	if res1 != res2 {
		t.Errorf("expected serialization of document ID to be deterministic, got first=%q, second=%q", res1, res2)
	}
	if !strings.Contains(res1, "a") || !strings.Contains(res1, "b") {
		t.Errorf("expected serialized document ID to contain keys, got %q", res1)
	}
}

func TestPopulateActiveFailedIDsWithTombstones(t *testing.T) {
	log := logger.New()
	tmpDir := t.TempDir()
	dlqPath := filepath.Join(tmpDir, "test_pre_scan.jsonl")

	// Chronological log of failures and resolution tombstones:
	// - id1: failed (initial)
	// - id2: failed (initial)
	// - id1: resolved
	// - id3: failed (initial)
	// - id2: resolved
	// - id2: failed again (initial)
	records := []string{
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id1","phase":"initial","opType":"insert"}`,
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id2","phase":"initial","opType":"insert"}`,
		`{"sourceDB":"db","sourceCollection":"coll","resolvedID":"id1","phase":"initial"}`,
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id3","phase":"initial","opType":"insert"}`,
		`{"sourceDB":"db","sourceCollection":"coll","resolvedID":"id2","phase":"initial"}`,
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id2","phase":"initial","opType":"update"}`,
	}
	if err := os.WriteFile(dlqPath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	activeMap, err := PopulateActiveFailedIDs(dlqPath, log)
	if err != nil {
		t.Fatalf("PopulateActiveFailedIDs failed: %v", err)
	}

	// Active map should contain only: id2 (phase: initial) and id3 (phase: initial)
	if len(activeMap) != 2 {
		t.Fatalf("expected exactly 2 active failed documents, got %d. Map: %v", len(activeMap), activeMap)
	}

	key2 := MakeDLQKey("db", "coll", "id2")
	if phase, exists := activeMap[key2]; !exists || phase != "initial" {
		t.Errorf("expected key %q to exist with phase 'initial', got exists=%t, phase=%q", key2, exists, phase)
	}

	key3 := MakeDLQKey("db", "coll", "id3")
	if phase, exists := activeMap[key3]; !exists || phase != "initial" {
		t.Errorf("expected key %q to exist with phase 'initial', got exists=%t, phase=%q", key3, exists, phase)
	}

	key1 := MakeDLQKey("db", "coll", "id1")
	if _, exists := activeMap[key1]; exists {
		t.Errorf("expected key %q to have been resolved and deleted, but it still exists", key1)
	}
}

func TestReprocessDLQDeDuplicationAndChronology(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Records for the same document ID:
	// - id1: failed insert (t=0)
	// - id1: resolved (t=1)
	// - id1: failed delete (t=2)
	records := []string{
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id1","phase":"initial","opType":"insert","document":{"_id":"id1","val":"v1"}}`,
		`{"sourceDB":"db","sourceCollection":"coll","resolvedID":"id1","phase":"initial"}`,
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id1","phase":"initial","opType":"delete"}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	mockColl := &mockTargetCollection{}
	getCollection := func(collName string) TargetCollection {
		return mockColl
	}

	// Act
	phase, failedCount, err := m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, nil)
	if err != nil {
		t.Fatalf("expected reprocessDLQLoop to succeed, got: %v", err)
	}
	if phase != "initial" || failedCount != 0 {
		t.Errorf("expected phase 'initial' and 0 remaining failures, got phase=%q, failures=%d", phase, failedCount)
	}

	// Assert: Only the last operation (delete) was executed due to de-duplication!
	if mockColl.replaceCount != 0 {
		t.Errorf("expected 0 ReplaceOne calls (insert was resolved), got %d", mockColl.replaceCount)
	}
	if mockColl.deleteCount != 1 {
		t.Errorf("expected exactly 1 DeleteOne call, got %d", mockColl.deleteCount)
	}
}

func TestReprocessDLQUnsupportedVersion(t *testing.T) {
	log := logger.New()
	m := NewMigrator(&config.Config{}, log)
	tmpDir := t.TempDir()

	tempFilePath := filepath.Join(tmpDir, "temp_dlq.jsonl")
	newDlqFilePath := filepath.Join(tmpDir, "new_dlq.jsonl")

	// Records with incompatible/unsupported version identifier:
	records := []string{
		`{"dlqVersion":"v999"}`,
		`{"sourceDB":"db","sourceCollection":"coll","documentID":"id1","phase":"initial","opType":"insert","document":{"_id":"id1"}}`,
	}
	if err := os.WriteFile(tempFilePath, []byte(strings.Join(records, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("failed to write mock DLQ file: %v", err)
	}

	newDLQ, err := NewDLQWriter(newDlqFilePath, log)
	if err != nil {
		t.Fatalf("failed to create DLQWriter: %v", err)
	}
	defer newDLQ.Close()

	getCollection := func(collName string) TargetCollection {
		return &mockTargetCollection{}
	}

	// Act
	_, _, err = m.reprocessDLQLoop(context.Background(), tempFilePath, newDLQ, getCollection, nil)
	if err == nil {
		t.Fatalf("expected reprocessDLQLoop to abort with error due to unsupported version v999, but it succeeded")
	}
	if !strings.Contains(err.Error(), "unsupported DLQ version \"v999\"") {
		t.Errorf("expected version safety mismatch error, got: %v", err)
	}
}



