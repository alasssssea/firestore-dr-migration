package migration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestStartReplicationSafetyInvariantsRejectsNilStateWithResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	r := &ClientLevelReplicator{}
	ctx := context.TODO()
	err := r.StartReplication(ctx, "some-resume-token", filepath.Join(tmpDir, "path"), nil, filepath.Join(tmpDir, "state-path"), config.DatabasePair{}, false, nil, nil)
	if err == nil {
		t.Errorf("expected error when state is nil but resume token exists")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state file does not exist") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStartReplicationSafetyInvariantsRejectsCompletedStateWithoutResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	r := &ClientLevelReplicator{}
	ctx := context.TODO()
	completedState := &InitialMigrationState{Status: StatusCompleted}
	err := r.StartReplication(ctx, nil, filepath.Join(tmpDir, "path"), completedState, filepath.Join(tmpDir, "state-path"), config.DatabasePair{}, false, nil, nil)
	if err == nil {
		t.Errorf("expected error when state is completed but resume token is nil")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state is marked as completed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStartReplicationSafetyInvariantsAbortsOnFailedState(t *testing.T) {
	tmpDir := t.TempDir()
	r := &ClientLevelReplicator{}
	ctx := context.TODO()
	failedState := &InitialMigrationState{Status: StatusCompletedWithFailures}
	err := r.StartReplication(ctx, "some-resume-token", filepath.Join(tmpDir, "path"), failedState, filepath.Join(tmpDir, "state-path"), config.DatabasePair{}, false, nil, nil)
	if err == nil {
		t.Errorf("expected error when state is CompletedWithFailures")
	} else if !strings.Contains(err.Error(), "cannot start replication: initial migration completed with failures in a previous run") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStartReplicationSafetyInvariantsRejectsSkippedStateWithoutResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	r := &ClientLevelReplicator{}
	ctx := context.TODO()
	skippedState := &InitialMigrationState{Status: StatusSkipped}
	err := r.StartReplication(ctx, nil, filepath.Join(tmpDir, "path"), skippedState, filepath.Join(tmpDir, "state-path"), config.DatabasePair{}, true, nil, nil)
	if err == nil {
		t.Errorf("expected error when state is skipped but resume token is nil")
	} else if !strings.Contains(err.Error(), "safety violation: initial migration state is marked as skipped") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStartReplicationSafetyInvariantsRejectsCustomStartTimeWithResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	r := &ClientLevelReplicator{}
	ctx := context.TODO()
	customStartTime := &primitive.Timestamp{T: 1716234000, I: 1}
	err := r.StartReplication(ctx, "some-resume-token", filepath.Join(tmpDir, "path"), nil, filepath.Join(tmpDir, "state-path"), config.DatabasePair{}, true, customStartTime, nil)
	if err == nil {
		t.Errorf("expected error when liveStartTime is specified and globalResumeToken is not nil")
	} else if !strings.Contains(err.Error(), "safety violation: a custom live-start-timestamp is specified, but a global resume token checkpoint already exists") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDeleteNonExistentInitialMigrationState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-delete-test.json")

	err := DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error deleting non-existent file: %v", err)
	}
}

func TestSaveLoadAndDeleteInitialMigrationState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-delete-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusCompletedWithFailures, 5)
	if err != nil {
		t.Fatalf("failed to save state: %v", err)
	}
	if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		t.Fatalf("expected state file to exist on disk")
	}

	loaded, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if !loaded.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if loaded.Status != StatusCompletedWithFailures {
		t.Errorf("expected Status to be StatusCompletedWithFailures, got %q", loaded.Status)
	}
	if loaded.FailedCount != 5 {
		t.Errorf("expected FailedCount to be 5, got %d", loaded.FailedCount)
	}

	err = DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error deleting existing file: %v", err)
	}

	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Errorf("expected state file to be deleted from disk, but it still exists")
	}
}

// TestStartReplicationFailsOnSaveInitialMigrationStateFailure verifies that when live-only state file checkpointing
// fails (e.g., due to write permissions or non-existent path directories), the error is correctly bubbled up to
// the caller immediately as a terminal replication exit failure instead of being swallowed.
func TestStartReplicationFailsOnSaveInitialMigrationStateFailure(t *testing.T) {
	log := logger.New()
	r := &ClientLevelReplicator{
		log: log,
	}
	ctx := context.Background()

	// Use a non-existent directory to guarantee filesystem write failure
	invalidStatePath := "/nonexistent-dir-12345/state.json"

	err := r.StartReplication(
		ctx,
		nil,                 // globalResumeToken
		"resume-token.json", // globalResumeTokenPath
		nil,                 // initialMigrationState
		invalidStatePath,
		config.DatabasePair{},
		true, // liveOnly = true (skip DB interactions)
		nil,  // liveStartTime
		nil,  // migrator
	)

	if err == nil {
		t.Fatal("expected error due to invalid state file path, but got nil")
	}
	if !strings.Contains(err.Error(), "failed to save initial migration state as skipped") {
		t.Errorf("expected error message to contain 'failed to save initial migration state as skipped', got %v", err)
	}
}

func TestStartReplicationScenarioBPartialFailure(t *testing.T) {
	log := logger.New()
	tmpDir := t.TempDir()
	globalResumeTokenPath := filepath.Join(tmpDir, "resumeToken-global.json")

	r := &ClientLevelReplicator{
		log: log,
		config: &config.Config{
			IncrementalStreamPartitions: 4,
		},
	}
	ctx := context.Background()

	// Scenario B: Create 4 partition files, but write empty/nil token to partition-3 to simulate corruption/empty file
	for i := 0; i < 4; i++ {
		partPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, 4)
		var token interface{} = map[string]interface{}{"_data": "valid-token"}
		if i == 2 { // Partition 3 is invalid/empty
			token = nil
		}
		if err := SaveResumeToken(partPath, token); err != nil {
			t.Fatalf("failed to setup test: %v", err)
		}
	}

	// Expected: Scenario B empty/unreadable partition checkpoint fatal error
	err := r.StartReplication(
		ctx,
		map[string]interface{}{"_data": "legacy-global-token"},
		globalResumeTokenPath,
		&InitialMigrationState{Status: StatusCompleted},
		filepath.Join(tmpDir, "state.json"),
		config.DatabasePair{},
		true, // liveOnly = true (to bypass mongo client operations in test)
		nil,
		nil,
	)

	if err == nil {
		t.Fatal("expected error due to Scenario B partial failure, but got nil")
	}
	if !strings.Contains(err.Error(), "exists but is empty or unreadable") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStartReplicationPartitionCountMismatch(t *testing.T) {
	log := logger.New()
	tmpDir := t.TempDir()
	globalResumeTokenPath := filepath.Join(tmpDir, "resumeToken-global.json")

	r := &ClientLevelReplicator{
		log: log,
		config: &config.Config{
			IncrementalStreamPartitions: 4, // Configured is 4 partitions
		},
	}
	ctx := context.Background()

	// Create 2 partition files on disk (simulating previous run with 2 partitions)
	part1Path := GetPartitionResumeTokenPath(globalResumeTokenPath, 0, 2)
	part2Path := GetPartitionResumeTokenPath(globalResumeTokenPath, 1, 2)
	now := time.Now()
	if err := SaveResumeToken(part1Path, map[string]interface{}{"_data": "token-1"}, now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("failed to setup test: %v", err)
	}
	if err := SaveResumeToken(part2Path, map[string]interface{}{"_data": "token-2"}, now.Add(-1*time.Hour)); err != nil {
		t.Fatalf("failed to setup test: %v", err)
	}

	// Recover panic (since sourceDB is nil in test, starting change stream will panic)
	// and verify that the partition checkpoints were successfully converted to the oldest watermark!
	defer func() {
		recover()

		// Verify all 4 new partition files were created and initialized with the oldest token ("token-1")!
		for i := 0; i < 4; i++ {
			partPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, 4)
			token, err := LoadResumeToken(partPath)
			if err != nil || token == nil {
				t.Fatalf("[Partition %d] expected checkpoint file to exist and load, got: %v", i, err)
			}
			tokenDoc := token.(map[string]interface{})
			if tokenDoc["_data"] != "token-1" {
				t.Errorf("[Partition %d] expected token data 'token-1' (oldest watermark), got %q", i, tokenDoc["_data"])
			}
		}
	}()

	_ = r.StartReplication(
		ctx,
		map[string]interface{}{"_data": "legacy-global-token"},
		globalResumeTokenPath,
		&InitialMigrationState{Status: StatusCompleted},
		filepath.Join(tmpDir, "state.json"),
		config.DatabasePair{},
		true,
		nil,
		nil,
	)
}

func TestStartReplicationLegacyUpgradeSuccess(t *testing.T) {
	log := logger.New()
	tmpDir := t.TempDir()
	globalResumeTokenPath := filepath.Join(tmpDir, "resumeToken-global.json")

	r := &ClientLevelReplicator{
		log: log,
		config: &config.Config{
			IncrementalStreamPartitions: 4, // 4 partitions configured
		},
	}
	ctx := context.Background()

	// Legitimate Upgrade: 0 partition files exist on disk, but base global token exists
	legacyToken := map[string]interface{}{"_data": "legacy-global-token"}
	if err := SaveResumeToken(globalResumeTokenPath, legacyToken); err != nil {
		t.Fatalf("failed to setup test: %v", err)
	}

	// Recover panic (since sourceDB is nil in test, starting change stream will panic)
	// and verify that the partition upgrade checkpoints were written before the panic!
	defer func() {
		recover()

		// Verify all 4 partition files were created and initialized with the legacy token!
		for i := 0; i < 4; i++ {
			partPath := GetPartitionResumeTokenPath(globalResumeTokenPath, i, 4)
			token, err := LoadResumeToken(partPath)
			if err != nil || token == nil {
				t.Fatalf("[Partition %d] expected checkpoint file to exist and load, got: %v", i, err)
			}
			tokenDoc := token.(map[string]interface{})
			if tokenDoc["_data"] != "legacy-global-token" {
				t.Errorf("[Partition %d] expected token data 'legacy-global-token', got %q", i, tokenDoc["_data"])
			}
		}
	}()

	_ = r.StartReplication(
		ctx,
		legacyToken,
		globalResumeTokenPath,
		&InitialMigrationState{Status: StatusCompleted},
		filepath.Join(tmpDir, "state.json"),
		config.DatabasePair{},
		true, // liveOnly = true (skips database connection steps in test)
		nil,
		nil,
	)
}
