package config

import (
	"os"
	"testing"
)

// TestGetMaxWorkersForLive verifies the calculation of maximum worker counts for different migration and live phases.
// It validates that the worker counts scale properly based on IncrementalWorkerCount, ConcurrentCollections,
// and InitialMigrationWorkers configs.
func TestGetMaxWorkersForLive(t *testing.T) {
	// Arrange: standard initial migration configuration set
	cfg := &Config{
		IncrementalWorkerCount:  8,
		ConcurrentCollections:   4,
		InitialMigrationWorkers: 8,
	}

	// Act & Assert 1: Under live-only mode, the initial backfill phase is skipped,
	// so the maximum worker threads should be strictly equal to c.IncrementalWorkerCount (8).
	if maxWorkers := cfg.GetMaxWorkersForLive("live-only"); maxWorkers != 8 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live-only\") to be 8, got %d", maxWorkers)
	}

	// Act & Assert 2: Under migrate mode, the system runs the bulk initial backfill phase in parallel.
	// The maximum concurrency is the peak of the backfill (ConcurrentCollections * InitialMigrationWorkers = 4 * 8 = 32).
	if maxWorkers := cfg.GetMaxWorkersForLive("migrate"); maxWorkers != 32 {
		t.Errorf("Expected GetMaxWorkersForLive(\"migrate\") to be 32, got %d", maxWorkers)
	}

	// Act & Assert 3: Under live mode, both the initial bulk backfill and live change stream replication run.
	// Max workers should match the peak backfill concurrency (32).
	if maxWorkers := cfg.GetMaxWorkersForLive("live"); maxWorkers != 32 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live\") to be 32, got %d", maxWorkers)
	}

	// Arrange 2: High live worker count edge case where live replication threads exceed backfill peak
	cfg2 := &Config{
		IncrementalWorkerCount:  64,
		ConcurrentCollections:   4,
		InitialMigrationWorkers: 8,
	}

	// Act & Assert 4: Should return IncrementalWorkerCount (64) because it is larger than the backfill peak (32).
	if maxWorkers := cfg2.GetMaxWorkersForLive("live"); maxWorkers != 64 {
		t.Errorf("Expected GetMaxWorkersForLive(\"live\") with large IncrementalWorkerCount to be 64, got %d", maxWorkers)
	}
}

// TestLoadConfigStrict verifies that the JSON configuration parser acts strictly,
// successfully loading correct config keys and returning validation errors on unrecognized fields.
func TestLoadConfigStrict(t *testing.T) {
	// Helper to write a temporary JSON file to disk
	writeTempFile := func(t *testing.T, content string) string {
		tmpFile, err := os.CreateTemp("", "config_test_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer tmpFile.Close()
		if _, err := tmpFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write temp file: %v", err)
		}
		return tmpFile.Name()
	}

	// 1. Arrange: perfectly valid JSON configuration schema string
	validJSON := `{
		"databasePairs": [
			{
				"source": {
					"connectionString": "mongodb://localhost:27017",
					"database": "source_db"
				},
				"target": {
					"connectionString": "mongodb://localhost:27018",
					"database": "target_db"
				}
			}
		],
		"saveThreshold": 150,
		"checkpointIntervalMinutes": 10
	}`
	validFile := writeTempFile(t, validJSON)
	defer os.Remove(validFile)

	// Act: Load configuration from file path
	cfg, err := LoadConfig(validFile)

	// Assert: Verify successful parsing and parameter mappings
	if err != nil {
		t.Errorf("Expected valid config to load successfully, got error: %v", err)
	}
	if cfg.SaveThreshold != 150 {
		t.Errorf("Expected SaveThreshold to be 150, got %d", cfg.SaveThreshold)
	}
	if cfg.CheckpointIntervalMinutes != 10 {
		t.Errorf("Expected CheckpointIntervalMinutes to be 10, got %d", cfg.CheckpointIntervalMinutes)
	}

	// 2. Arrange: invalid JSON configuration containing unrecognized parameter keys
	invalidJSON := `{
		"databasePairs": [
			{
				"source": {
					"connectionString": "mongodb://localhost:27017",
					"database": "source_db"
				},
				"target": {
					"connectionString": "mongodb://localhost:27018",
					"database": "target_db"
				}
			}
		],
		"unrecognizedFieldKey": "oops",
		"saveThreshold": 150
	}`
	invalidFile := writeTempFile(t, invalidJSON)
	defer os.Remove(invalidFile)

	// Act & Assert: LoadConfig must return a parsing syntax/validation error for unrecognized fields
	_, err = LoadConfig(invalidFile)
	if err == nil {
		t.Error("Expected LoadConfig to fail with unrecognized field key 'unrecognizedFieldKey', but it succeeded without errors")
	}
}

// TestLoadConfigIncrementalStreamPartitions verifies that the partitions count parses correctly and defaults to 1.
func TestLoadConfigIncrementalStreamPartitions(t *testing.T) {
	// Helper to write a temporary JSON file to disk
	writeTempFile := func(t *testing.T, content string) string {
		tmpFile, err := os.CreateTemp("", "config_partitions_test_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer tmpFile.Close()
		if _, err := tmpFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write temp file: %v", err)
		}
		return tmpFile.Name()
	}

	// Case 1: Arrange JSON config missing incrementalStreamPartitions parameter (default trigger check)
	json1 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		]
	}`
	file1 := writeTempFile(t, json1)
	defer os.Remove(file1)

	// Act: Load configuration
	cfg1, err := LoadConfig(file1)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Assert: IncrementalStreamPartitions must default to exactly 1
	if cfg1.IncrementalStreamPartitions != 1 {
		t.Errorf("Expected default IncrementalStreamPartitions to be 1, got %d", cfg1.IncrementalStreamPartitions)
	}

	// Case 2: Arrange JSON config specifying custom incrementalStreamPartitions parameter count
	json2 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		],
		"incrementalStreamPartitions": 4
	}`
	file2 := writeTempFile(t, json2)
	defer os.Remove(file2)

	// Act: Load configuration
	cfg2, err := LoadConfig(file2)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Assert: this fork forces a single stream (Firestore does not support
	// multi-partition change streams), so even a configured value of 4 is
	// clamped down to 1.
	if cfg2.IncrementalStreamPartitions != 1 {
		t.Errorf("Expected IncrementalStreamPartitions to be forced to 1, got %d", cfg2.IncrementalStreamPartitions)
	}
}

// TestLoadConfigBackfillRampUp verifies that backfillRampUp parses correctly and initializes defaults.
func TestLoadConfigBackfillRampUp(t *testing.T) {
	writeTempFile := func(t *testing.T, content string) string {
		tmpFile, err := os.CreateTemp("", "config_rampup_test_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer tmpFile.Close()
		if _, err := tmpFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write temp file: %v", err)
		}
		return tmpFile.Name()
	}

	// Case 1: missing backfillRampUp (should use default rampUp params when disabled/not configured)
	json1 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		]
	}`
	file1 := writeTempFile(t, json1)
	defer os.Remove(file1)

	cfg1, err := LoadConfig(file1)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg1.BackfillRampUp.Enabled {
		t.Errorf("Expected BackfillRampUp.Enabled to be false by default")
	}
	if cfg1.BackfillRampUp.RampRatePerMin != 10000.0 {
		t.Errorf("Expected default RampRatePerMin to be 10000.0, got %f", cfg1.BackfillRampUp.RampRatePerMin)
	}
	if cfg1.BackfillRampUp.UpdateIntervalMs != 1000 {
		t.Errorf("Expected default UpdateIntervalMs to be 1000, got %d", cfg1.BackfillRampUp.UpdateIntervalMs)
	}

	// Case 2: fully custom backfillRampUp configuration
	json2 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		],
		"backfillRampUp": {
			"enabled": true,
			"startQps": 500.0,
			"rampRatePerMin": 5000.0,
			"updateIntervalMs": 500,
			"useStaggeredWorkers": true,
			"workerDelayMs": 200
		}
	}`
	file2 := writeTempFile(t, json2)
	defer os.Remove(file2)

	cfg2, err := LoadConfig(file2)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !cfg2.BackfillRampUp.Enabled {
		t.Errorf("Expected BackfillRampUp.Enabled to be true")
	}
	if cfg2.BackfillRampUp.StartQps != 500.0 {
		t.Errorf("Expected StartQps to be 500.0, got %f", cfg2.BackfillRampUp.StartQps)
	}
	if cfg2.BackfillRampUp.RampRatePerMin != 5000.0 {
		t.Errorf("Expected RampRatePerMin to be 5000.0, got %f", cfg2.BackfillRampUp.RampRatePerMin)
	}
	if cfg2.BackfillRampUp.UpdateIntervalMs != 500 {
		t.Errorf("Expected UpdateIntervalMs to be 500, got %d", cfg2.BackfillRampUp.UpdateIntervalMs)
	}
	if !cfg2.BackfillRampUp.UseStaggeredWorkers {
		t.Errorf("Expected UseStaggeredWorkers to be true")
	}
	if cfg2.BackfillRampUp.WorkerDelayMs != 200 {
		t.Errorf("Expected WorkerDelayMs to be 200, got %d", cfg2.BackfillRampUp.WorkerDelayMs)
	}
}

// TestLoadConfigIDTypeForPartition verifies that the idTypeForPartition configuration parses, defaults, and validates correctly.
func TestLoadConfigIDTypeForPartition(t *testing.T) {
	writeTempFile := func(t *testing.T, content string) string {
		tmpFile, err := os.CreateTemp("", "config_idtype_test_*.json")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer tmpFile.Close()
		if _, err := tmpFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write temp file: %v", err)
		}
		return tmpFile.Name()
	}

	// Case 1: missing idTypeForPartition (should default to "auto")
	json1 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		]
	}`
	file1 := writeTempFile(t, json1)
	defer os.Remove(file1)

	cfg1, err := LoadConfig(file1)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg1.IDTypeForPartition != "auto" {
		t.Errorf("Expected default IDTypeForPartition to be 'auto', got %q", cfg1.IDTypeForPartition)
	}

	// Case 2: custom valid idTypeForPartition "objectid"
	json2 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		],
		"idTypeForPartition": "objectid"
	}`
	file2 := writeTempFile(t, json2)
	defer os.Remove(file2)

	cfg2, err := LoadConfig(file2)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg2.IDTypeForPartition != "objectid" {
		t.Errorf("Expected IDTypeForPartition to be 'objectid', got %q", cfg2.IDTypeForPartition)
	}

	// Case 3: invalid idTypeForPartition (should fail validation)
	json3 := `{
		"databasePairs": [
			{
				"source": { "connectionString": "mongodb://localhost:27017", "database": "db" },
				"target": { "connectionString": "mongodb://localhost:27018", "database": "db" }
			}
		],
		"idTypeForPartition": "invalid_type"
	}`
	file3 := writeTempFile(t, json3)
	defer os.Remove(file3)

	_, err = LoadConfig(file3)
	if err == nil {
		t.Error("Expected LoadConfig to fail validation for invalid idTypeForPartition, but it succeeded")
	}
}
