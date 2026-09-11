package assess

import (
	"fmt"
	"runtime"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/partition"
)

// Tuning is a recommended migration concurrency/throughput configuration derived
// from an assessment Report plus the host's CPU count. It is advisory: the web
// console pre-fills its worker/partition inputs with these values and the user
// can override them before starting.
type Tuning struct {
	// InitialMigrationWorkers is the per-collection write-worker goroutine count
	// for the full (initial) load.
	InitialMigrationWorkers int `json:"initialMigrationWorkers"`
	// ConcurrentCollections is how many collections load in parallel.
	ConcurrentCollections int `json:"concurrentCollections"`
	// IncrementalWorkerCount is the change-event apply worker count for live.
	IncrementalWorkerCount int `json:"incrementalWorkerCount"`
	// EnableParallelReads turns on partitioned source reads for large collections.
	EnableParallelReads bool `json:"enableParallelReads"`
	// MaxReadPartitions is the partition count used when parallel reads are on.
	MaxReadPartitions int `json:"maxReadPartitions"`
	// WorkersPerPartition is the write-worker goroutine count WITHIN each
	// partition. Total read/write parallelism for one big collection is roughly
	// MaxReadPartitions × WorkersPerPartition — this is the depth knob that
	// unsticks a single straggler collection.
	WorkersPerPartition int `json:"workersPerPartition"`
	// MinDocsPerPartition bounds how finely a collection is split: the effective
	// partition count is min(MaxReadPartitions, docs/MinDocsPerPartition). Lower
	// it to split a collection into more partitions.
	MinDocsPerPartition int `json:"minDocsPerPartition"`
	// MinDocsForParallelReads is the collection-size (doc-count) threshold above
	// which partitioned reads kick in; below it a collection loads with a single
	// cursor. Lower it to partition smaller — but byte-heavy — collections that
	// would otherwise stall on one cursor.
	MinDocsForParallelReads int `json:"minDocsForParallelReads"`
	// NumCPU is the host CPU count the recommendation was scaled to.
	NumCPU int `json:"numCPU"`
	// Rationale explains, in one line per decision, why these values were chosen.
	Rationale []string `json:"rationale"`
}

// minDocsForParallelReads mirrors the engine default (config.MinDocsForParallelReads)
// so the recommendation matches when the migrator will actually partition reads.
// This is the ENABLE gate (do we partition at all?), NOT the per-partition
// granularity — see recommendedMinDocsPerPartition.
const minDocsForParallelReads = 50000

// recommendedMinDocsPerPartition mirrors the engine default
// (config.MinDocsPerPartition) and is the per-partition GRANULARITY we advise.
// The recommended MaxReadPartitions is derived from this same value via
// partition.Count, so the advised knobs are internally consistent and match the
// engine's actual split (the two must never diverge again).
const recommendedMinDocsPerPartition = 10000

const (
	// targetPartitionBytes is the byte budget we aim to put in each partition when
	// recommending a per-collection override, so the split is sized by data volume
	// rather than doc count.
	targetPartitionBytes = 256 * 1024 * 1024 // 256 MiB
	// stragglerMinBytes is the estimated total size at or above which a collection
	// that would NOT otherwise be partitioned (doc count below the global
	// threshold) is flagged as a straggler worth partitioning.
	stragglerMinBytes = 1 * 1024 * 1024 * 1024 // 1 GiB
	// maxRecommendedPartitions caps a per-collection recommendation so a single
	// huge collection cannot recommend an unbounded fan-out.
	maxRecommendedPartitions = 32
)

// CollectionTuningRec is a per-collection partition-knob recommendation for a
// "straggler" collection: one the GLOBAL settings would leave on a single cursor
// (its document count is below MinDocsForParallelReads) yet which holds enough
// bytes that a single-cursor load would be slow. The console pre-fills the
// per-collection override inputs with Override and flags the row.
type CollectionTuningRec struct {
	Database      string `json:"database"`
	Collection    string `json:"collection"`
	Docs          int64  `json:"docs"`
	AvgDocBytes   int64  `json:"avgDocBytes"`
	EstTotalBytes int64  `json:"estTotalBytes"`
	Reason        string `json:"reason"`
	// ReasonKey + ReasonArgs are the i18n-localizable form of Reason (stable key +
	// interpolation args); the console renders these in the operator's language and
	// falls back to Reason (English) when the key is unknown. humanBytes strings are
	// passed as args verbatim since their units (B/KiB/MiB…) are language-neutral.
	ReasonKey  string                  `json:"reasonKey,omitempty"`
	ReasonArgs []interface{}           `json:"reasonArgs,omitempty"`
	Override   config.CollectionTuning `json:"override"`
}

// RecommendCollectionTuning decides whether a collection is a byte-heavy /
// low-count straggler and, if so, returns a per-collection override that forces
// it to partition. docs is the true document count; avgDocBytes is the mean
// document size estimated from the assessment sample. It returns (rec, false)
// when no override is warranted.
func RecommendCollectionTuning(db, coll string, docs, avgDocBytes int64) (CollectionTuningRec, bool) {
	if docs <= 0 || avgDocBytes <= 0 {
		return CollectionTuningRec{}, false
	}
	estTotal := docs * avgDocBytes
	// Only flag collections that (a) the global threshold would leave single-cursor
	// and (b) carry enough bytes that single-cursor is actually slow. Large-count
	// collections already trigger the global parallel-read path.
	if docs >= minDocsForParallelReads || estTotal < stragglerMinBytes {
		return CollectionTuningRec{}, false
	}

	parts := int((estTotal + targetPartitionBytes - 1) / targetPartitionBytes)
	if parts < 2 {
		parts = 2
	}
	if parts > maxRecommendedPartitions {
		parts = maxRecommendedPartitions
	}
	if int64(parts) > docs { // can't have more partitions than documents
		parts = int(docs)
	}
	if parts < 1 {
		parts = 1
	}
	minDocsPerPart := int((docs + int64(parts) - 1) / int64(parts))
	if minDocsPerPart < 1 {
		minDocsPerPart = 1
	}
	// Trigger threshold must sit below this collection's own count so the engine's
	// enable gate (count >= threshold) fires for it.
	trigger := 1000
	if int64(trigger) > docs {
		trigger = int(docs)
	}
	enable := true
	return CollectionTuningRec{
		Database:      db,
		Collection:    coll,
		Docs:          docs,
		AvgDocBytes:   avgDocBytes,
		EstTotalBytes: estTotal,
		Reason: fmt.Sprintf("Byte-heavy but few documents: ~%d docs × avg %s ≈ %s; doc count is below the global parallel-read threshold (%d) so it degrades to a single cursor — recommend splitting into %d partitions",
			docs, humanBytes(avgDocBytes), humanBytes(estTotal), minDocsForParallelReads, parts),
		ReasonKey:  "reasonStraggler",
		ReasonArgs: []interface{}{docs, humanBytes(avgDocBytes), humanBytes(estTotal), minDocsForParallelReads, parts},
		Override: config.CollectionTuning{
			ParallelReadsEnabled:    &enable,
			MaxReadPartitions:       parts,
			MinDocsPerPartition:     minDocsPerPart,
			MinDocsForParallelReads: trigger,
		},
	}, true
}

// humanBytes renders a byte count as a short human-readable string (KiB/MiB/GiB).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// RecommendTuning derives a Tuning from an assessment report and the local CPU
// count. It is deliberately conservative: it never recommends more workers than
// there is data or CPU to justify, so small databases don't spin up idle
// goroutines and large ones use the cores available.
func RecommendTuning(r *Report) Tuning {
	cpu := runtime.NumCPU()
	if cpu < 1 {
		cpu = 1
	}
	t := Tuning{NumCPU: cpu}

	// Initial-load write workers: scale with CPU, capped by data volume so a tiny
	// database doesn't over-provision. Roughly one worker per 250k docs, within
	// [2, 2*CPU].
	workers := int(r.TotalDocs/250000) + 1
	if workers < 2 {
		workers = 2
	}
	if max := 2 * cpu; workers > max {
		workers = max
	}
	t.InitialMigrationWorkers = workers
	// Rationale entries are emitted as stable i18n KEYS (not prose) so the console
	// can render them in the user's selected language. All are static (no
	// interpolation); the frontend maps each key via its I18N dictionary and falls
	// back to the key verbatim if unknown.
	t.Rationale = append(t.Rationale, "ratInitWorkers")

	// Concurrent collections: bounded by CPU and the collection count. No point
	// loading more collections at once than we have or than CPU supports.
	cc := cpu
	if cc > r.Collections && r.Collections > 0 {
		cc = r.Collections
	}
	if cc < 1 {
		cc = 1
	}
	if cc > 8 {
		cc = 8
	}
	t.ConcurrentCollections = cc
	t.Rationale = append(t.Rationale, "ratConcColl")

	// Incremental workers: one per CPU is the engine default and a good baseline
	// for change-event apply parallelism.
	t.IncrementalWorkerCount = cpu
	t.Rationale = append(t.Rationale, "ratIncWorkers")

	// Parallel reads pay off only for large collections; enable when the biggest
	// collection crosses the engine's partition threshold.
	if r.LargestCollectionDocs >= minDocsForParallelReads {
		t.EnableParallelReads = true
		// Cap partition fan-out by host resources (4× CPU) and the absolute ceiling.
		maxParts := 4 * cpu
		if maxParts > maxRecommendedPartitions {
			maxParts = maxRecommendedPartitions
		}
		// Derive MaxReadPartitions from the SAME per-partition granularity we
		// recommend (recommendedMinDocsPerPartition) using the engine's own
		// partition.Count, so the advised cap equals what the engine will actually
		// create instead of a separately-invented divisor.
		parts := partition.Count(r.LargestCollectionDocs, recommendedMinDocsPerPartition, maxParts)
		if parts < 2 {
			parts = 2
		}
		t.MaxReadPartitions = parts
		t.Rationale = append(t.Rationale, "ratParallelOn")
	} else {
		t.Rationale = append(t.Rationale, "ratParallelOff")
	}

	// Partition-granularity knobs: whether a collection is split
	// (MinDocsForParallelReads), how finely (MinDocsPerPartition) and the worker
	// depth within each partition (WorkersPerPartition). We surface the engine
	// defaults so they are visible and editable; a byte-heavy collection with few
	// documents never crosses the doc-count threshold and stalls on a single
	// cursor — lower these two thresholds to force it to partition. Per-collection
	// auto-tuning for such collections is handled separately (per-table override).
	t.WorkersPerPartition = 3
	t.MinDocsPerPartition = recommendedMinDocsPerPartition
	t.MinDocsForParallelReads = minDocsForParallelReads
	t.Rationale = append(t.Rationale, "ratPartitionKnobs")

	return t
}
