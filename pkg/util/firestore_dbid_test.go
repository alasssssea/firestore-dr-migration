package util

import (
	"regexp"
	"testing"
)

var firestoreDBIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{2,61}[a-z0-9]$`)

func TestSanitizeFirestoreDBID(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"MyApp", "myapp"},
		{"user_data", "user-data"},
		{"db", "dbxx"}, // too short -> padded
		{"Sales.2024", "sales-2024"},
		{"__weird__", "weird"},
		{"123db", "db"}, // leading digits stripped -> "db" -> padded... see below
	}
	for _, tc := range cases {
		got := SanitizeFirestoreDBID(tc.in)
		if !firestoreDBIDRe.MatchString(got) {
			t.Errorf("SanitizeFirestoreDBID(%q) = %q, which violates Firestore ID rules", tc.in, got)
		}
	}
}

func TestSanitizeFirestoreDBID_AlwaysValid(t *testing.T) {
	inputs := []string{"", "-", "A", "___", "9999", "a", "verylongnamethatexceedssixtythreecharacterssoitmustbetruncatedforsuretobevalid", "MixedCase-Name_123"}
	for _, in := range inputs {
		got := SanitizeFirestoreDBID(in)
		if !firestoreDBIDRe.MatchString(got) {
			t.Errorf("SanitizeFirestoreDBID(%q) = %q, violates rules (len=%d)", in, got, len(got))
		}
	}
}
