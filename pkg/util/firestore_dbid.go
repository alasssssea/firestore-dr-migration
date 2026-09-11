package util

import (
	"fmt"
	"regexp"
	"strings"
)

// uuidPattern matches a canonical UUID; Firestore forbids UUID-shaped database
// IDs (they collide with its internal UID namespace).
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ValidateFirestoreDBID returns a list of rule violations for a proposed
// Firestore database ID, or nil if it is valid. Rules (DESIGN §7): only
// lowercase letters/digits/hyphens, start with a letter, end with a letter or
// digit, 4–63 characters, and not UUID-shaped.
func ValidateFirestoreDBID(id string) []string {
	var problems []string
	if l := len(id); l < 4 || l > 63 {
		problems = append(problems, fmt.Sprintf("length %d is outside the allowed 4–63 characters", l))
	}
	if id == "" {
		return problems
	}
	if uuidPattern.MatchString(id) {
		problems = append(problems, "must not be UUID-shaped")
	}
	if c := id[0]; !(c >= 'a' && c <= 'z') {
		problems = append(problems, "must start with a lowercase letter")
	}
	if c := id[len(id)-1]; !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
		problems = append(problems, "must end with a lowercase letter or digit")
	}
	for _, r := range id {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
			problems = append(problems, "may only contain lowercase letters, digits, and hyphens")
			break
		}
	}
	return problems
}

// SanitizeFirestoreDBID converts an arbitrary (often MongoDB) database name into
// a candidate Firestore database ID that satisfies Firestore's naming rules:
//
//   - only lowercase letters, digits, and hyphens
//   - must start with a letter
//   - must end with a letter or digit
//   - length between 4 and 63 characters
//
// The result is a suggestion for the operator to confirm — it is not guaranteed
// to be unique within the project. (Firestore also forbids UUID-shaped IDs;
// that check is left to the caller/provisioning step.)
func SanitizeFirestoreDBID(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))

	// Replace any disallowed character with a hyphen.
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	s = b.String()

	// Must start with a letter: drop leading hyphens and digits.
	s = strings.TrimLeft(s, "-0123456789")
	// Must end with a letter or digit: drop trailing hyphens.
	s = strings.TrimRight(s, "-")

	if s == "" {
		s = "db"
	}

	// Pad up to the 4-character minimum (appended letters keep it valid).
	for len(s) < 4 {
		s += "x"
	}

	// Truncate to the 63-character maximum, then re-trim any trailing hyphen.
	if len(s) > 63 {
		s = strings.TrimRight(s[:63], "-")
	}
	return s
}
