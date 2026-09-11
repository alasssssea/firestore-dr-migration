// Package idmap persists the lossy _id conversions the migrator performs, so
// verification can reconnect a rewritten target document to its source _id
// (DESIGN §1 #3 / §3). When the engine converts an invalid _id (e.g. a BSON
// type Firestore rejects) to a string, the original→new mapping is appended
// here as an audit record and an input to `-mode=verify`.
package idmap

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
)

// Mapping is one recorded _id conversion.
type Mapping struct {
	Collection string `json:"collection"`
	// OrigCanonical is the canonical Extended-JSON form of the original _id
	// value, preserving its BSON type (so int 5 and string "5" never collide).
	OrigCanonical string `json:"origCanonical"`
	OrigType      string `json:"origType"`
	NewID         string `json:"newId"`
}

// Store records and looks up _id mappings.
type Store interface {
	// Record persists that origID (its true typed value) became newID in the
	// given collection. Safe for concurrent use.
	Record(collection string, origID interface{}, newID string) error
	// Lookup returns the new (string) _id for a source _id, if it was converted.
	Lookup(collection string, origID interface{}) (string, bool)
	// Close flushes and releases resources.
	Close() error
}

// CanonicalID renders any _id value to a stable, type-preserving string using
// Extended JSON. Wrapping in a document lets us canonicalize scalars uniformly.
func CanonicalID(v interface{}) string {
	data, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, false, false)
	if err != nil {
		return fmt.Sprintf("%T:%v", v, v)
	}
	return string(data)
}

// FileStore is an append-only JSONL Store with an in-memory index.
type FileStore struct {
	mu    sync.Mutex
	path  string
	f     *os.File
	w     *bufio.Writer
	index map[string]string // collection\x00origCanonical -> newID
}

// OpenFileStore opens (creating if needed) a JSONL mapping file, loading any
// existing records into the in-memory index for lookups.
func OpenFileStore(path string) (*FileStore, error) {
	index, err := loadIndex(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open id-map file %s: %w", path, err)
	}
	return &FileStore{path: path, f: f, w: bufio.NewWriter(f), index: index}, nil
}

func loadIndex(path string) (map[string]string, error) {
	index := make(map[string]string)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return index, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read id-map file %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var m Mapping
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, fmt.Errorf("corrupt id-map record: %w", err)
		}
		index[indexKey(m.Collection, m.OrigCanonical)] = m.NewID
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("failed scanning id-map file: %w", err)
	}
	return index, nil
}

func indexKey(collection, origCanonical string) string {
	return collection + "\x00" + origCanonical
}

// Record appends a mapping and updates the in-memory index.
func (s *FileStore) Record(collection string, origID interface{}, newID string) error {
	canon := CanonicalID(origID)
	m := Mapping{
		Collection:    collection,
		OrigCanonical: canon,
		OrigType:      fmt.Sprintf("%T", origID),
		NewID:         newID,
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	s.index[indexKey(collection, canon)] = newID
	return nil
}

// Lookup returns the converted _id for a source _id, if present.
func (s *FileStore) Lookup(collection string, origID interface{}) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.index[indexKey(collection, CanonicalID(origID))]
	return v, ok
}

// Len returns the number of recorded mappings (for reporting/tests).
func (s *FileStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.index)
}

// Close flushes and closes the underlying file.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w != nil {
		if err := s.w.Flush(); err != nil {
			return err
		}
	}
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// NopStore is a Store that records nothing (used when id-mapping is disabled).
type NopStore struct{}

func (NopStore) Record(string, interface{}, string) error { return nil }
func (NopStore) Lookup(string, interface{}) (string, bool) { return "", false }
func (NopStore) Close() error                              { return nil }
