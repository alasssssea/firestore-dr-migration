package idmap

import (
	"path/filepath"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCanonicalID_TypeDistinct(t *testing.T) {
	// int 5 and string "5" must not canonicalize to the same key.
	if CanonicalID(int64(5)) == CanonicalID("5") {
		t.Error("int 5 and string \"5\" collided in canonical form")
	}
	oid := primitive.NewObjectID()
	if CanonicalID(oid) == CanonicalID(oid.Hex()) {
		t.Error("ObjectID and its hex string collided")
	}
}

func TestFileStore_RecordLookupPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idmap.jsonl")

	s, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	if err := s.Record("users", int64(5), "5"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := s.Record("users", 3.14, "3.14"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if got, ok := s.Lookup("users", int64(5)); !ok || got != "5" {
		t.Errorf("Lookup(5) = %q,%v want 5,true", got, ok)
	}
	// Different type, same string form: must be a miss.
	if _, ok := s.Lookup("users", "5"); ok {
		t.Error("Lookup(\"5\") should miss when only int 5 was recorded")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and confirm records persisted and reload into the index.
	s2, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if s2.Len() != 2 {
		t.Errorf("reloaded len = %d, want 2", s2.Len())
	}
	if got, ok := s2.Lookup("users", int64(5)); !ok || got != "5" {
		t.Errorf("post-reload Lookup(5) = %q,%v want 5,true", got, ok)
	}
}

func TestNopStore(t *testing.T) {
	var s Store = NopStore{}
	if err := s.Record("c", 1, "1"); err != nil {
		t.Errorf("NopStore.Record err = %v", err)
	}
	if _, ok := s.Lookup("c", 1); ok {
		t.Error("NopStore.Lookup should always miss")
	}
}
