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
