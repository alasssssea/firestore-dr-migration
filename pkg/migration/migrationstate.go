package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Status enum constants for Initial Migration State
const (
	StatusInProgress            = "inprogress"
	StatusCompleted             = "completed"
	StatusCompletedWithFailures = "completed_with_failures"
	StatusSkipped               = "skipped"
)

// InitialMigrationState represents the completion state of the initial migration phase
type InitialMigrationState struct {
	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	FailedCount int64     `json:"failedCount,omitempty"`
}

// IsCompleted helper returns whether the initial migration is finished (regardless of failures)
func (s *InitialMigrationState) IsCompleted() bool {
	return s != nil && (s.Status == StatusCompleted || s.Status == StatusCompletedWithFailures || s.Status == StatusSkipped)
}

// LoadInitialMigrationState loads the initial migration state from a file
func LoadInitialMigrationState(filePath string) (*InitialMigrationState, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read initial migration state file: %w", err)
	}

	var state InitialMigrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse initial migration state: %w", err)
	}

	return &state, nil
}

// SaveInitialMigrationState saves the initial migration state to a file
func SaveInitialMigrationState(filePath string, status string, failedCount int64) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	state := InitialMigrationState{
		Status:      status,
		FailedCount: failedCount,
	}
	if status == StatusCompleted || status == StatusCompletedWithFailures {
		state.CompletedAt = time.Now()
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal initial migration state: %w", err)
	}

	return writeAtomic(filePath, data, 0644)
}

// writeAtomic writes data to a temporary file and atomically renames it to destination, preventing corruption on crashes.
func writeAtomic(filePath string, data []byte, perm os.FileMode) error {
	tmpPath := filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// DeleteInitialMigrationState deletes the initial migration state file
func DeleteInitialMigrationState(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete initial migration state file: %w", err)
	}
	return nil
}

