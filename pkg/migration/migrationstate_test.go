package migration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNonExistentMigrationStateReturnsNil(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("unexpected error loading non-existent state: %v", err)
	}
	if state != nil {
		t.Errorf("expected state to be nil, got %v", state)
	}
}

func TestSaveAndLoadInProgressMigrationState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusInProgress, 0)
	if err != nil {
		t.Fatalf("failed to save inprogress state: %v", err)
	}

	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load inprogress state: %v", err)
	}
	if state.IsCompleted() {
		t.Errorf("expected IsCompleted to be false")
	}
	if state.Status != StatusInProgress {
		t.Errorf("expected Status to be StatusInProgress, got %q", state.Status)
	}
	if state.FailedCount != 0 {
		t.Errorf("expected FailedCount to be 0, got %d", state.FailedCount)
	}
	if !state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be zero for inprogress state")
	}
}

func TestSaveAndLoadCompletedMigrationStateWithoutFailures(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusCompleted, 0)
	if err != nil {
		t.Fatalf("failed to save completed state: %v", err)
	}

	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load completed state: %v", err)
	}
	if !state.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if state.Status != StatusCompleted {
		t.Errorf("expected Status to be StatusCompleted, got %q", state.Status)
	}
	if state.FailedCount != 0 {
		t.Errorf("expected FailedCount to be 0, got %d", state.FailedCount)
	}
	if state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be non-zero for completed state")
	}
}

func TestSaveAndLoadCompletedMigrationStateWithFailures(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusCompletedWithFailures, 42)
	if err != nil {
		t.Fatalf("failed to save completed with failures state: %v", err)
	}

	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load completed with failures state: %v", err)
	}
	if !state.IsCompleted() {
		t.Errorf("expected IsCompleted to be true")
	}
	if state.Status != StatusCompletedWithFailures {
		t.Errorf("expected Status to be StatusCompletedWithFailures, got %q", state.Status)
	}
	if state.FailedCount != 42 {
		t.Errorf("expected FailedCount to be 42, got %d", state.FailedCount)
	}
	if state.CompletedAt.IsZero() {
		t.Errorf("expected CompletedAt to be non-zero for completed state")
	}
}

func TestSaveAndLoadSkippedMigrationState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusSkipped, 0)
	if err != nil {
		t.Fatalf("failed to save skipped state: %v", err)
	}

	state, err := LoadInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to load skipped state: %v", err)
	}
	if !state.IsCompleted() {
		t.Errorf("expected IsCompleted to be true for skipped state")
	}
	if state.Status != StatusSkipped {
		t.Errorf("expected Status to be StatusSkipped, got %q", state.Status)
	}
}

func TestDeleteNonExistentMigrationStateFile(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistentPath := filepath.Join(tmpDir, "does-not-exist.json")

	err := DeleteInitialMigrationState(nonExistentPath)
	if err != nil {
		t.Errorf("unexpected error deleting non-existent file: %v", err)
	}
}

func TestDeleteExistingMigrationStateFile(t *testing.T) {
	tmpDir := t.TempDir()
	stateFilePath := filepath.Join(tmpDir, "initialMigrationState-test.json")

	err := SaveInitialMigrationState(stateFilePath, StatusCompleted, 0)
	if err != nil {
		t.Fatalf("failed to save state file: %v", err)
	}

	err = DeleteInitialMigrationState(stateFilePath)
	if err != nil {
		t.Fatalf("failed to delete existing state file: %v", err)
	}
	if _, err := os.Stat(stateFilePath); !os.IsNotExist(err) {
		t.Errorf("expected state file to be deleted from disk, but it still exists")
	}
}
