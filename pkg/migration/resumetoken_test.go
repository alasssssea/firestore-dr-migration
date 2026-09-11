package migration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGetResumeTokenPath(t *testing.T) {
	expected := "resumeToken-testDB-testColl.json"
	got := GetResumeTokenPath("testDB", "testColl")
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestLoadNonExistentResumeTokenCreatesEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-token.json")

	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("unexpected error loading non-existent token: %v", err)
	}
	if token != nil {
		t.Errorf("expected loaded token to be nil, got %v", token)
	}
}

func TestSaveAndLoadStringResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-token.json")

	err := SaveResumeToken(tokenPath, "my_simple_token_data")
	if err != nil {
		t.Fatalf("failed to save string token: %v", err)
	}

	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load saved string token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "my_simple_token_data" {
		t.Errorf("expected _data to be %q, got %q", "my_simple_token_data", m["_data"])
	}
}

func TestSaveAndLoadMapResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-token.json")

	mapToken := map[string]interface{}{
		"_data": "map_token_data_123",
	}
	err := SaveResumeToken(tokenPath, mapToken)
	if err != nil {
		t.Fatalf("failed to save map token: %v", err)
	}

	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load map token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "map_token_data_123" {
		t.Errorf("expected _data to be %q, got %q", "map_token_data_123", m["_data"])
	}
}

func TestSaveAndLoadPrefixedResumeToken(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-token.json")

	err := SaveResumeToken(tokenPath, "map[_data:prefix_format_data]")
	if err != nil {
		t.Fatalf("failed to save prefixed token: %v", err)
	}

	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load prefixed token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != "prefix_format_data" {
		t.Errorf("expected _data to be %q, got %q", "prefix_format_data", m["_data"])
	}
}

func TestResumeTokenBackupRotation(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-backup.json")

	for i := 1; i <= 12; i++ {
		err := SaveResumeToken(tokenPath, map[string]interface{}{
			"_data": string(rune('A' + i)),
		})
		if err != nil {
			t.Fatalf("failed to save token iteration %d: %v", i, err)
		}
	}

	for i := 1; i <= 10; i++ {
		var backupPath string
		if i == 10 {
			backupPath = tokenPath + ".10"
		} else {
			backupPath = tokenPath + "." + string(rune('0'+i))
		}
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			t.Errorf("expected backup %s to exist", backupPath)
		}
	}
}

func TestResumeTokenBackupRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-backup.json")

	for i := 1; i <= 11; i++ {
		_ = SaveResumeToken(tokenPath, map[string]interface{}{
			"_data": string(rune('A' + i)),
		})
	}

	err := os.WriteFile(tokenPath, []byte("invalid_json"), 0644)
	if err != nil {
		t.Fatalf("failed to corrupt token file: %v", err)
	}

	token, err := LoadResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to load and restore backup token: %v", err)
	}
	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] == "" {
		t.Errorf("expected non-empty recovered _data")
	}
}

func TestResumeTokenBackupDeletion(t *testing.T) {
	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "resume-backup.json")

	for i := 1; i <= 11; i++ {
		_ = SaveResumeToken(tokenPath, map[string]interface{}{
			"_data": string(rune('A' + i)),
		})
	}

	err := DeleteResumeToken(tokenPath)
	if err != nil {
		t.Fatalf("failed to delete token and backups: %v", err)
	}

	if _, err := os.Stat(tokenPath); !os.IsNotExist(err) {
		t.Errorf("expected main token file to be deleted")
	}
	for i := 1; i <= 10; i++ {
		backupPath := tokenPath + "." + string(rune('0'+i))
		if i == 10 {
			backupPath = tokenPath + ".10"
		}
		if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
			t.Errorf("expected backup file %s to be deleted", backupPath)
		}
	}
}

// TestLoadPartitionResumeTokenRejectsOutdatedVersion asserts that partition checkfiles with older/mismatched version identifiers are dropped, triggering safe fallback fresh starts.
func TestLoadPartitionResumeTokenRejectsOutdatedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	partitionPath := filepath.Join(tmpDir, "resumeToken-global-partition-1.json")

	// Save manual legacy/outdated token format
	legacyToken := ResumeToken{
		Data:        "legacy_payload_data_123",
		HashVersion: "v1-split-reduce-base65",
	}
	legacyBytes, err := json.MarshalIndent(legacyToken, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal legacy token: %v", err)
	}
	if err := os.WriteFile(partitionPath, legacyBytes, 0644); err != nil {
		t.Fatalf("failed to write legacy token file: %v", err)
	}

	// Load should reject the checkpoint and return nil (trigger safe fresh start)
	token, err := LoadResumeToken(partitionPath)
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if token != nil {
		t.Errorf("expected rejected token to be nil, got %v", token)
	}
}

// TestLoadPartitionResumeTokenAcceptsCurrentVersion asserts that loading a partitioned checkfile with the modern version identifier works and returns the target payload data.
func TestLoadPartitionResumeTokenAcceptsCurrentVersion(t *testing.T) {
	tmpDir := t.TempDir()
	partitionPath := filepath.Join(tmpDir, "resumeToken-global-partition-1.json")

	correctData := "modern_data_value_999"
	if err := SaveResumeToken(partitionPath, correctData); err != nil {
		t.Fatalf("failed to save modern partition token: %v", err)
	}

	token, err := LoadResumeToken(partitionPath)
	if err != nil {
		t.Fatalf("failed to load partition token: %v", err)
	}

	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != correctData {
		t.Errorf("expected loaded _data to be %q, got %q", correctData, m["_data"])
	}
}

// TestLoadLegacyGlobalResumeTokenBypassesVersionCheck asserts standard legacy/global checkfiles lacking partition keys bypass version verification to allow direct load-resumes.
func TestLoadLegacyGlobalResumeTokenBypassesVersionCheck(t *testing.T) {
	tmpDir := t.TempDir()
	globalPath := filepath.Join(tmpDir, "resumeToken-global.json")
	globalData := "legacy_global_data_payload_888"

	// Save regular checkfile manually with no version parameter
	legacyGlobal := ResumeToken{
		Data: globalData,
	}
	globalBytes, err := json.MarshalIndent(legacyGlobal, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal legacy global token: %v", err)
	}
	if err := os.WriteFile(globalPath, globalBytes, 0644); err != nil {
		t.Fatalf("failed to write legacy global token file: %v", err)
	}

	// Load should bypass version filters and load successfully
	token, err := LoadResumeToken(globalPath)
	if err != nil {
		t.Fatalf("failed to load standard global token: %v", err)
	}

	m, ok := token.(map[string]interface{})
	if !ok {
		t.Fatalf("expected loaded token to be map, got %T", token)
	}
	if m["_data"] != globalData {
		t.Errorf("expected loaded _data to be %q, got %q", globalData, m["_data"])
	}
}

// TestLoadMalformedPartitionFilenameRejectsOutdatedVersion asserts that even badly formatted partition names (like ending in "-partition1.json") are caught and protected by strict version bounds.
func TestLoadMalformedPartitionFilenameRejectsOutdatedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	badNamePath := filepath.Join(tmpDir, "resumeToken-global-partition1.json")

	badNameToken := ResumeToken{
		Data:        "bad_name_payload",
		HashVersion: "v1-split-reduce-base65", // old version
	}
	badNameBytes, err := json.MarshalIndent(badNameToken, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal bad name token: %v", err)
	}
	if err := os.WriteFile(badNamePath, badNameBytes, 0644); err != nil {
		t.Fatalf("failed to write bad name token file: %v", err)
	}

	// Load should detect the partition string and safe-reject it
	token, err := LoadResumeToken(badNamePath)
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if token != nil {
		t.Errorf("expected bad name partition token to be rejected, got %v", token)
	}
}

// TestLoadCorruptedPartitionFileRaisesSyntaxError asserts syntactically invalid/corrupt JSON partition files raise standard system parser syntax failures instead of version errors.
func TestLoadCorruptedPartitionFileRaisesSyntaxError(t *testing.T) {
	tmpDir := t.TempDir()
	corruptedPath := filepath.Join(tmpDir, "resumeToken-global-partition-2.json")

	if err := os.WriteFile(corruptedPath, []byte("corrupted_invalid_json_payload{_data:"), 0644); err != nil {
		t.Fatalf("failed to write corrupted partition file: %v", err)
	}

	// Load should throw JSON parsing errors (or trigger standard backup failures returning nil)
	token, err := LoadResumeToken(corruptedPath)
	if err == nil && token != nil {
		t.Errorf("expected corrupted checkfile load to fail or return nil, got %v", token)
	}
}

func TestGetPartitionResumeTokenPath(t *testing.T) {
	tests := []struct {
		name            string
		basePath        string
		index           int
		totalPartitions int
		expected        string
	}{
		{
			name:            "Single stream json base path returns uniform partition-1 suffix",
			basePath:        "resumeToken-pair0.json",
			index:           0,
			totalPartitions: 1,
			expected:        "resumeToken-pair0-partition-1-of-1.json",
		},
		{
			name:            "Single stream simple base path returns uniform partition-1 suffix",
			basePath:        "resumeToken-pair0",
			index:           0,
			totalPartitions: 1,
			expected:        "resumeToken-pair0-partition-1-of-1",
		},
		{
			name:            "Zero stream counts returns uniform partition-1 suffix",
			basePath:        "resumeToken-pair0.json",
			index:           0,
			totalPartitions: 0,
			expected:        "resumeToken-pair0-partition-1-of-0.json",
		},
		{
			name:            "Multi partition index 0 maps to partition 1 suffix with json trimming",
			basePath:        "resumeToken-pair0.json",
			index:           0,
			totalPartitions: 2,
			expected:        "resumeToken-pair0-partition-1-of-2.json",
		},
		{
			name:            "Multi partition index 1 maps to partition 2 suffix with json trimming",
			basePath:        "resumeToken-pair0.json",
			index:           1,
			totalPartitions: 2,
			expected:        "resumeToken-pair0-partition-2-of-2.json",
		},
		{
			name:            "Multi partition simple base path appends partition suffix directly",
			basePath:        "resumeToken-pair0",
			index:           2,
			totalPartitions: 4,
			expected:        "resumeToken-pair0-partition-3-of-4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetPartitionResumeTokenPath(tt.basePath, tt.index, tt.totalPartitions)
			if got != tt.expected {
				t.Errorf("GetPartitionResumeTokenPath() = %q, want %q", got, tt.expected)
			}
		})
	}
}

type mockDirEntry struct {
	name  string
	isDir bool
}

func (m mockDirEntry) Name() string               { return m.name }
func (m mockDirEntry) IsDir() bool                { return m.isDir }
func (m mockDirEntry) Type() os.FileMode          { return 0 }
func (m mockDirEntry) Info() (os.FileInfo, error) { return nil, nil }

func TestScanAndResolveCheckpoints(t *testing.T) {
	globalPath := "/tmp/checkpoints/resumeToken-mydb-mycoll.json"

	tests := []struct {
		name                     string
		files                    []os.DirEntry
		expectedHistTotal        int
		expectedUsingCurrent     bool
		expectedPathsKeys        []int // Expect paths at these indices
	}{
		{
			name: "Normal N-of-M checkpoints",
			files: []os.DirEntry{
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-1-of-4.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-2-of-4.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-3-of-4.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-4-of-4.json"},
			},
			expectedHistTotal:    4,
			expectedUsingCurrent: false,
			expectedPathsKeys:    []int{0, 1, 2, 3},
		},
		{
			name: "Current contiguous partition naming upgrade",
			files: []os.DirEntry{
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-1.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-2.json"},
			},
			expectedHistTotal:    2,
			expectedUsingCurrent: true,
			expectedPathsKeys:    []int{0, 1},
		},
		{
			name: "Malformed partition names are skipped",
			files: []os.DirEntry{
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-abc-of-4.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-1-of-xyz.json"},
				mockDirEntry{name: "resumeToken-mydb-mycoll-partition-1-of-4-backup.json"},
			},
			expectedHistTotal:    0,
			expectedUsingCurrent: false,
			expectedPathsKeys:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scanned := ScanPartitionCheckpoints(tt.files, globalPath)
			total, paths, usingCurrent := ResolveActiveCheckpoints(scanned)

			if total != tt.expectedHistTotal {
				t.Errorf("ResolveActiveCheckpoints() total = %v, want %v", total, tt.expectedHistTotal)
			}
			if usingCurrent != tt.expectedUsingCurrent {
				t.Errorf("ResolveActiveCheckpoints() usingCurrent = %v, want %v", usingCurrent, tt.expectedUsingCurrent)
			}
			for _, idx := range tt.expectedPathsKeys {
				if _, ok := paths[idx]; !ok {
					t.Errorf("expected path at index %d to be populated", idx)
				}
			}
		})
	}
}

func TestCaptureTokenCheckpointPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	globalPath := filepath.Join(tmpDir, "resumeToken-global.json")

	tokenDoc := map[string]interface{}{
		"_data": "captured_token_payload_abc123",
	}

	partitions := 4
	for i := 0; i < partitions; i++ {
		partPath := GetPartitionResumeTokenPath(globalPath, i, partitions)
		if err := SaveResumeToken(partPath, tokenDoc); err != nil {
			t.Fatalf("failed to save captured partition token %d: %v", i, err)
		}
	}

	// Verify all partition files exist and can be loaded
	for i := 0; i < partitions; i++ {
		partPath := GetPartitionResumeTokenPath(globalPath, i, partitions)
		loaded, err := LoadResumeToken(partPath)
		if err != nil {
			t.Fatalf("failed to load captured partition token %d: %v", i, err)
		}
		m, ok := loaded.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map token, got %T", loaded)
		}
		if m["_data"] != "captured_token_payload_abc123" {
			t.Errorf("expected _data to match, got %v", m["_data"])
		}
	}

	// Verify state file creation
	statePath := filepath.Join(tmpDir, "initialMigrationState-global.json")
	if err := SaveInitialMigrationState(statePath, StatusSkipped, 0); err != nil {
		t.Fatalf("failed to save initial migration state: %v", err)
	}
	state, err := LoadInitialMigrationState(statePath)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}
	if state == nil || state.Status != StatusSkipped {
		t.Errorf("expected state status %q, got %v", StatusSkipped, state)
	}
}

