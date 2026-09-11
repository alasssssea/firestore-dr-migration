package assess

import (
	"runtime"
	"testing"

	"firestore-dr-migration/pkg/partition"
)

// TestRecommendTuningPartitionsMatchEngine guards the single-source invariant:
// the recommended MaxReadPartitions must equal what the engine (partition.Count)
// would actually create for the largest collection, using the SAME granularity
// the recommendation advises (MinDocsPerPartition). Historically these diverged
// because the recommender used the enable threshold (50000) as the divisor.
func TestRecommendTuningPartitionsMatchEngine(t *testing.T) {
	cpu := runtime.NumCPU()
	maxParts := 4 * cpu
	if maxParts > maxRecommendedPartitions {
		maxParts = maxRecommendedPartitions
	}

	cases := []int64{60000, 250000, 1000000, 50000000}
	for _, docs := range cases {
		tn := RecommendTuning(&Report{
			TotalDocs:             docs,
			LargestCollectionDocs: docs,
		})
		if !tn.EnableParallelReads {
			t.Fatalf("docs=%d: expected parallel reads enabled (>= %d)", docs, minDocsForParallelReads)
		}
		if tn.MinDocsPerPartition != recommendedMinDocsPerPartition {
			t.Errorf("docs=%d: MinDocsPerPartition=%d, want %d", docs, tn.MinDocsPerPartition, recommendedMinDocsPerPartition)
		}
		want := partition.Count(docs, tn.MinDocsPerPartition, maxParts)
		if want < 2 {
			want = 2
		}
		if tn.MaxReadPartitions != want {
			t.Errorf("docs=%d: MaxReadPartitions=%d, want %d (engine split at advised granularity)",
				docs, tn.MaxReadPartitions, want)
		}
	}
}

// TestRecommendTuningParallelDisabledBelowThreshold verifies the enable gate
// still uses the 50000 threshold, independent of the partition granularity.
func TestRecommendTuningParallelDisabledBelowThreshold(t *testing.T) {
	tn := RecommendTuning(&Report{TotalDocs: 40000, LargestCollectionDocs: 40000})
	if tn.EnableParallelReads {
		t.Errorf("40000 docs is below the %d enable threshold; parallel reads should be off", minDocsForParallelReads)
	}
}
