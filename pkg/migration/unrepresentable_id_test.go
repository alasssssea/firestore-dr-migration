package migration

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestIDUnrepresentableInFirestore pins the rule that decides when a delete op is
// treated as an auto-resolved no-op instead of a permanent DLQ entry: an _id that
// can never be stored in Firestore (so the target could never contain such a doc).
func TestIDUnrepresentableInFirestore(t *testing.T) {
	cases := []struct {
		name string
		id   interface{}
		want bool
	}{
		{"short-string", "ORD-123", false},
		{"objectid", primitive.NewObjectID(), false},
		{"int", int64(42), false},
		{"oversized-string", strings.Repeat("x", 1600), true},
		{"reserved-name", "__proto__", true},
		{"at-limit-1500", strings.Repeat("y", 1500), false},
		{"over-limit-1501", strings.Repeat("z", 1501), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := idUnrepresentableInFirestore(tc.id); got != tc.want {
				t.Errorf("idUnrepresentableInFirestore(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
