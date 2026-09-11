// Package partition holds the pure read-partition sizing math shared by the
// migration engine (which uses it to split a collection at run time) and the
// assessment recommender (which uses it to advise the MaxReadPartitions knob).
//
// It is a dependency-free leaf package on purpose: pkg/migration and pkg/assess
// both import it, so the console's recommendation and the engine's actual split
// are computed by ONE function and can never drift. Historically the recommender
// re-derived the partition count with its own divisor, which conflated the
// enable threshold with the per-partition granularity; routing both through
// Count removes that class of bug.
package partition

// Count returns how many read partitions the engine will create for a
// collection of totalCount documents, given the target number of documents per
// partition and the hard cap on partitions. This is the single source of truth
// for partition sizing.
//
// Semantics (must stay identical to what the engine relies on):
//   - a collection smaller than one partition's worth of docs stays whole (1);
//   - otherwise it is floor(totalCount/minDocsPerPartition), capped at
//     maxPartitions, never below 1.
func Count(totalCount int64, minDocsPerPartition, maxPartitions int) int {
	if minDocsPerPartition <= 0 {
		minDocsPerPartition = 1
	}
	if maxPartitions <= 0 {
		maxPartitions = 1
	}
	if totalCount < int64(minDocsPerPartition) {
		return 1
	}
	partitionCount := int(totalCount) / minDocsPerPartition
	if partitionCount > maxPartitions {
		partitionCount = maxPartitions
	}
	if partitionCount < 1 {
		partitionCount = 1
	}
	return partitionCount
}

// DefaultPartitionBudgetBytes is the per-partition byte budget the engine sizes
// against. Firestore's MongoDB-compatible endpoint enforces a hard 128 MiB
// per-query memory ceiling (a range scan + sort that exceeds it fails the whole
// query with InvalidArgument). 64 MiB leaves ~50% headroom for sort buffers and
// document-size skew, so a partition scan stays comfortably under the limit.
const DefaultPartitionBudgetBytes int64 = 64 << 20

// CountByBytes returns how many read partitions are needed so each partition's
// scanned data stays under budgetBytes, given the collection's document count
// and average document size. This is the byte-aware replacement for Count on the
// engine's hot path: Count was blind to document size and capped partitions at a
// static knob, so a large collection produced a handful of oversized partitions
// that blew Firestore's 128 MiB query limit.
//
// safeCount = ceil(totalCount * avgDocSize / budgetBytes) is a MINIMUM: fewer
// partitions than this would put more than budgetBytes behind a single query.
// An explicit manual cap (maxPartitions > 0) can only make partitions FINER;
// when it is below safeCount it cannot be honored without re-breaking the memory
// limit, so byte-safety wins and capOverridden is returned true (callers warn).
func CountByBytes(totalCount, avgDocSize, budgetBytes int64, maxPartitions int) (count int, capOverridden bool) {
	if totalCount <= 0 {
		return 1, false
	}
	if avgDocSize <= 0 {
		avgDocSize = 1024 // conservative fallback when size is unknown
	}
	if budgetBytes <= 0 {
		budgetBytes = DefaultPartitionBudgetBytes
	}

	docsPerPartition := budgetBytes / avgDocSize
	if docsPerPartition < 1 {
		docsPerPartition = 1 // documents larger than the whole budget: one per partition
	}

	safeCount := (totalCount + docsPerPartition - 1) / docsPerPartition // ceil
	count = int(safeCount)
	if count < 1 {
		count = 1
	}
	if int64(count) > totalCount {
		count = int(totalCount) // never more partitions than documents
	}

	// A manual cap below the byte-safe minimum would re-introduce the 128 MiB
	// blowup; byte-safety wins and the caller is told the cap was overridden.
	if maxPartitions > 0 && maxPartitions < count {
		capOverridden = true
	}
	return count, capOverridden
}
