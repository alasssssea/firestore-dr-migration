package assess

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// fakeReader is an in-memory sourceReader used to exercise the per-collection
// sampling/aggregation without a live database.
type fakeReader struct{ docs []bson.M }

func (f *fakeReader) listCollections(ctx context.Context) ([]string, error) { return []string{"c"}, nil }
func (f *fakeReader) indexCount(ctx context.Context, coll string) (int, error) { return 1, nil }
func (f *fakeReader) countDocs(ctx context.Context, coll string) (int64, error) {
	return int64(len(f.docs)), nil
}
func (f *fakeReader) sample(ctx context.Context, coll string, total, want int64) ([]bson.M, error) {
	if want > int64(len(f.docs)) {
		want = int64(len(f.docs))
	}
	return f.docs[:want], nil
}
func (f *fakeReader) close(ctx context.Context) {}

// TestAssessCollectionDocsAggregatesSamples proves the assessment aggregates one
// finding per (rule) across the sampled docs, counts every hit, and keeps a few
// concrete offending samples (doc _id + detail) — the "展开看样例" data — without
// exceeding maxSamplesPerFinding.
func TestAssessCollectionDocsAggregatesSamples(t *testing.T) {
	var docs []bson.M
	// 10 docs each with a reserved _id → 10 hits of id-reserved.
	for i := 0; i < 10; i++ {
		docs = append(docs, bson.M{"_id": "__bad" + strings.Repeat("x", i) + "__"})
	}
	fr := &fakeReader{docs: docs}

	inspected, _, findings, err := assessCollectionDocs(context.Background(), fr, "d", "c", int64(len(docs)), SampleConfig{Floor: 100}, nil)
	if err != nil {
		t.Fatalf("assessCollectionDocs: %v", err)
	}
	if inspected != 10 {
		t.Fatalf("inspected=%d want 10", inspected)
	}

	var idFinding *Finding
	for i := range findings {
		if findings[i].Rule == "id-reserved" {
			idFinding = &findings[i]
		}
	}
	if idFinding == nil {
		t.Fatalf("id-reserved not reported; findings=%+v", findings)
	}
	if idFinding.Count != 10 {
		t.Errorf("Count=%d want 10 (every sampled hit counted)", idFinding.Count)
	}
	if len(idFinding.Samples) != maxSamplesPerFinding {
		t.Errorf("kept %d samples, want cap %d", len(idFinding.Samples), maxSamplesPerFinding)
	}
	for _, s := range idFinding.Samples {
		if s.DocID == "" || s.Detail == "" {
			t.Errorf("sample missing docId/detail: %+v", s)
		}
	}
}
