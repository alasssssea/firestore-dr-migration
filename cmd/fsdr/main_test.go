package main

import (
	"testing"
	"time"
)

func TestParseLiveStartTime(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedT   uint32
		expectedErr bool
	}{
		{
			name:        "empty string",
			input:       "",
			expectedT:   0,
			expectedErr: false,
		},
		{
			name:        "valid unix timestamp",
			input:       "1716234000",
			expectedT:   1716234000,
			expectedErr: false,
		},
		{
			name:        "valid RFC3339 UTC",
			input:       "2026-05-20T21:00:00Z",
			expectedT:   uint32(time.Date(2026, 5, 20, 21, 0, 0, 0, time.UTC).Unix()),
			expectedErr: false,
		},
		{
			name:        "valid RFC3339 offset",
			input:       "2026-05-20T21:00:00+02:00",
			expectedT:   uint32(time.Date(2026, 5, 20, 19, 0, 0, 0, time.UTC).Unix()),
			expectedErr: false,
		},
		{
			name:        "invalid string format",
			input:       "not-a-timestamp",
			expectedT:   0,
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := parseStartTimestamp(tc.input)
			if tc.expectedErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.input == "" {
				if res != nil {
					t.Errorf("expected nil result, got %v", res)
				}
				return
			}

			if res == nil {
				t.Fatal("expected non-nil result, got nil")
			}

			if res.T != tc.expectedT {
				t.Errorf("expected T component to be %d, got %d", tc.expectedT, res.T)
			}

			if res.I != 1 {
				t.Errorf("expected I component to be 1, got %d", res.I)
			}
		})
	}
}

func TestIsValidMode(t *testing.T) {
	validModes := []string{
		"migrate",
		"live",
		"live-only",
		"retry-dlq",
		"assess",
		"verify",
		"wizard",
		"console",
	}

	for _, mode := range validModes {
		if !isValidMode(mode) {
			t.Errorf("expected mode %q to be valid, but got false", mode)
		}
	}

	invalidModes := []string{
		"",
		"invalid",
		"capture-resume-token",
		"capture-token",
		"capture",
		"token",
		"backfill",
		"all",
	}

	for _, mode := range invalidModes {
		if isValidMode(mode) {
			t.Errorf("expected mode %q to be invalid, but got true", mode)
		}
	}
}

