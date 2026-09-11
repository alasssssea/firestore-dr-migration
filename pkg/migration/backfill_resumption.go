package migration

import (
	"bytes"
	"cmp"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ResumptionMode defines the resumption strategy chosen for an initial backfill.
type ResumptionMode int

const (
	// ResumptionModeFresh indicates we do not have a full set of valid checkpoints; perform fresh sampling and backfill from scratch.
	ResumptionModeFresh ResumptionMode = iota
	// ResumptionModeDirect indicates a complete, valid checkpoint set matches current partitions; resume directly using reconstructed filters.
	ResumptionModeDirect
	// ResumptionModeResampleWithGlobalMin indicates a complete historical checkpoint set but partition count changed; resample new partitions and clamp lower boundaries using global min IDs.
	ResumptionModeResampleWithGlobalMin
)

// BackfillResumptionPlan represents the calculated plan for resuming or starting an initial backfill.
type BackfillResumptionPlan struct {
	Mode                 ResumptionMode
	PartitionFilters     []bson.D         // Reconstructed filters when Mode == ResumptionModeDirect
	PartitionInitialDocs []int64          // Initial document counts per historical partition checkpoint. This will not be provided when Mode == ResumptionModeFresh.
	GlobalMinSafeIDs     map[BSONType]any // Global min safe IDs per BSON type when Mode == ResumptionModeResampleWithGlobalMin
}

// TotalDocsMigrated returns the total sum of documents already migrated across all loaded checkpoints.
func (p *BackfillResumptionPlan) TotalDocsMigrated() int64 {
	if p == nil {
		return 0
	}
	var total int64
	for _, count := range p.PartitionInitialDocs {
		total += count
	}
	return total
}

// ValidateCheckpointSet checks if a slice of checkpoints forms a complete, contiguous, and uncorrupted set for expectedSplits.
func ValidateCheckpointSet(checkpoints []*PartitionCheckpoint, expectedSplits int) bool {
	if len(checkpoints) == 0 || len(checkpoints) != expectedSplits || expectedSplits <= 0 {
		return false
	}

	// Each checkpoint should be non-nil, match the expected partition count, have the correct partition index since they are sorted in the list, and have a non-empty TypeProgress map.
	for i, cp := range checkpoints {
		if cp == nil || cp.TotalSplits != expectedSplits || cp.PartitionIndex != i || cp.TypeProgress == nil || len(cp.TypeProgress) == 0 {
			return false
		}
	}
	return true
}

// isCompleteHistoricalSet checks if checkpoints form a complete, valid, contiguous set for some historical total splits count M.
func isCompleteHistoricalSet(checkpoints []*PartitionCheckpoint) bool {
	if len(checkpoints) == 0 || checkpoints[0] == nil {
		return false
	}
	// We want to check if all checkpoints have the same TotalSplits.
	return ValidateCheckpointSet(checkpoints, checkpoints[0].TotalSplits)
}

// CompareBSONValues compares two BSON ID values of the same type.
// Returns -1 if a < b, 0 if a == b, 1 if a > b, or error if types cannot be compared.
func CompareBSONValues(a, b any) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}

	switch valA := a.(type) {
	case primitive.ObjectID:
		if valB, ok := b.(primitive.ObjectID); ok {
			return bytes.Compare(valA[:], valB[:]), nil
		}
	case string:
		if valB, ok := b.(string); ok {
			return cmp.Compare(valA, valB), nil
		}
	case int, int32, int64, float64, float32:
		if numA, okA := toFloat64(valA); okA {
			if numB, okB := toFloat64(b); okB {
				return cmp.Compare(numA, numB), nil
			}
		}
	case primitive.DateTime:
		if valB, ok := b.(primitive.DateTime); ok {
			return cmp.Compare(valA, valB), nil
		}
	case time.Time:
		if valB, ok := b.(time.Time); ok {
			return valA.Compare(valB), nil
		}
	case primitive.Timestamp:
		if valB, ok := b.(primitive.Timestamp); ok {
			return primitive.CompareTimestamp(valA, valB), nil
		}
	case primitive.Binary:
		if valB, ok := b.(primitive.Binary); ok {
			return bytes.Compare(valA.Data, valB.Data), nil
		}
	case []byte:
		if valB, ok := b.([]byte); ok {
			return bytes.Compare(valA, valB), nil
		}
	case bool:
		if valB, ok := b.(bool); ok {
			if !valA && valB {
				return -1, nil
			}
			return 1, nil
		}
	}
	return 0, fmt.Errorf("incompatible types for BSON comparison: %T vs %T", a, b)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	return 0, false
}

// BuildPartitionFilterFromCheckpoint reconstructs the MongoDB query filter for a partition checkpoint.
// Returns an error if the checkpoint is nil or contains no type progress.
func BuildPartitionFilterFromCheckpoint(cp *PartitionCheckpoint) (bson.D, error) {
	if cp == nil {
		return nil, fmt.Errorf("cannot build filter from nil checkpoint")
	}
	if len(cp.TypeProgress) == 0 {
		return nil, fmt.Errorf("cannot build filter from checkpoint with empty type progress (partition %d)", cp.PartitionIndex)
	}

	// Sort BSONType keys deterministically for reproducible filter generation
	types := make([]BSONType, 0, len(cp.TypeProgress))
	for t := range cp.TypeProgress {
		types = append(types, t)
	}
	sort.Slice(types, func(i, j int) bool {
		return types[i] < types[j]
	})

	var typeClauses []bson.D
	for _, typeName := range types {
		boundary := cp.TypeProgress[typeName]
		if boundary == nil {
			return nil, fmt.Errorf("boundary for type %s is nil in partition %d", typeName, cp.PartitionIndex)
		}

		// When total splits > 1, every partition slice must have at least one valid boundary
		if cp.TotalSplits > 1 && boundary.RangeStartID == nil && boundary.RangeEndID == nil {
			return nil, fmt.Errorf("invalid partition boundary for type %s in partition %d of %d: both rangeStartId and rangeEndId are missing", typeName, cp.PartitionIndex, cp.TotalSplits)
		}

		// If this type range has completed in this partition, skip it
		if boundary.SavedLastID != nil && boundary.RangeEndID != nil {
			cmp, err := CompareBSONValues(boundary.SavedLastID, boundary.RangeEndID)
			if err != nil {
				return nil, fmt.Errorf("failed to compare savedLastID with rangeEndID for type %s: %w", typeName, err)
			}
			if cmp >= 0 {
				continue
			}
		}

		// Effective lower bound (SavedLastID takes priority over RangeStartID)
		lowerBound := boundary.SavedLastID
		if lowerBound == nil {
			lowerBound = boundary.RangeStartID
		}

		idConditions := bson.D{{Key: "$type", Value: string(typeName)}}
		if lowerBound != nil {
			idConditions = append(idConditions, bson.E{Key: "$gte", Value: lowerBound})
		}
		if boundary.RangeEndID != nil {
			idConditions = append(idConditions, bson.E{Key: "$lt", Value: boundary.RangeEndID})
		}

		typeClauses = append(typeClauses, bson.D{{Key: "_id", Value: idConditions}})
	}

	if len(typeClauses) == 0 {
		// All types completed in this partition; match no documents
		return bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}, nil
	}

	if len(typeClauses) == 1 {
		return typeClauses[0], nil
	}

	return bson.D{{Key: "$or", Value: typeClauses}}, nil
}

// ExtractGlobalMinSafeIDs scans all checkpoints to find the minimum safe lower bound per BSON type.
// Returns an error if checkpoints is empty or contains invalid/nil checkpoints.
func ExtractGlobalMinSafeIDs(checkpoints []*PartitionCheckpoint) (map[BSONType]any, error) {
	if len(checkpoints) == 0 {
		return nil, fmt.Errorf("cannot extract global min safe IDs from empty checkpoints")
	}

	result := make(map[BSONType]any)
	for i, cp := range checkpoints {
		if cp == nil {
			return nil, fmt.Errorf("checkpoint at index %d is nil", i)
		}
		for typeName, boundary := range cp.TypeProgress {
			if boundary == nil {
				continue
			}
			// If already resolved in an earlier partition, skip
			if _, alreadyFound := result[typeName]; alreadyFound {
				continue
			}

			// If this partition completed this type range, search in subsequent partitions
			if boundary.SavedLastID != nil && boundary.RangeEndID != nil {
				cmp, err := CompareBSONValues(boundary.SavedLastID, boundary.RangeEndID)
				if err != nil {
					return nil, fmt.Errorf("failed to compare IDs for type %s in partition %d: %w", typeName, i, err)
				}
				if cmp >= 0 {
					continue
				}
			}

			// First unfinished partition directly sets the global min for this type, since the partitions are sorted.
			start := boundary.SavedLastID
			if start == nil {
				start = boundary.RangeStartID
			}
			result[typeName] = start
		}
	}

	return result, nil
}

func parseTypeClause(clause bson.D) *TypeRangeBoundary {
	var idConditions bson.D
	for _, elem := range clause {
		if elem.Key == "_id" {
			if doc, ok := elem.Value.(bson.D); ok {
				idConditions = doc
			}
		}
	}
	if len(idConditions) == 0 {
		return nil
	}

	var typeName string
	var startID, endID any
	for _, elem := range idConditions {
		switch elem.Key {
		case "$type":
			if s, ok := elem.Value.(string); ok {
				typeName = s
			}
		case "$gte":
			startID = elem.Value
		case "$lt":
			endID = elem.Value
		}
	}

	bType := ParseBSONType(typeName)
	if bType == "" {
		if startID != nil {
			bType = GetBSONType(startID)
		} else if endID != nil {
			bType = GetBSONType(endID)
		}
	}
	if bType == "" {
		return nil
	}

	return &TypeRangeBoundary{
		BSONType:     bType,
		RangeStartID: startID,
		RangeEndID:   endID,
	}
}

// ExtractTypeRangeBoundariesFromFilter parses a partition query filter and returns the per-type RangeStartID and RangeEndID boundaries.
func ExtractTypeRangeBoundariesFromFilter(filter bson.D) map[BSONType]*TypeRangeBoundary {
	result := make(map[BSONType]*TypeRangeBoundary)
	if len(filter) == 0 {
		return result
	}

	for _, elem := range filter {
		if elem.Key == "$or" {
			if arr, ok := elem.Value.(bson.A); ok {
				for _, item := range arr {
					if d, isD := item.(bson.D); isD {
						if b := parseTypeClause(d); b != nil {
							result[b.BSONType] = b
						}
					}
				}
			} else if slice, ok := elem.Value.([]bson.D); ok {
				for _, d := range slice {
					if b := parseTypeClause(d); b != nil {
						result[b.BSONType] = b
					}
				}
			}
			return result
		}
	}

	if b := parseTypeClause(filter); b != nil {
		result[b.BSONType] = b
	}
	return result
}

// clampBoundary clamps a TypeRangeBoundary to a global min safe ID.
// If the boundary is completely below the min safe ID, returns nil.
func clampBoundary(b *TypeRangeBoundary, minSafeID any) *TypeRangeBoundary {
	if b == nil {
		return nil
	}
	if minSafeID == nil {
		return b
	}
	if b.BSONType != "" && GetBSONType(minSafeID) != "" && b.BSONType != GetBSONType(minSafeID) {
		return b
	}
	if b.RangeEndID != nil {
		if cmp, err := CompareBSONValues(b.RangeEndID, minSafeID); err == nil && cmp <= 0 {
			return nil
		}
	}
	clampedStart := b.RangeStartID
	if clampedStart == nil {
		clampedStart = minSafeID
	} else if cmp, err := CompareBSONValues(clampedStart, minSafeID); err == nil && cmp < 0 {
		clampedStart = minSafeID
	}
	return &TypeRangeBoundary{
		BSONType:     b.BSONType,
		RangeStartID: clampedStart,
		RangeEndID:   b.RangeEndID,
	}
}

// clampPartition clamps a partition filter to global min safe IDs.
func clampPartition(filter bson.D, globalMinSafeIDs map[BSONType]any) bson.D {
	boundaries := ExtractTypeRangeBoundariesFromFilter(filter)
	if len(boundaries) == 0 && len(filter) == 0 {
		for t, minID := range globalMinSafeIDs {
			if minID != nil {
				boundaries[t] = &TypeRangeBoundary{BSONType: t, RangeStartID: minID}
			}
		}
	}

	active := make(map[BSONType]*TypeRangeBoundary)
	for t, b := range boundaries {
		if clamped := clampBoundary(b, globalMinSafeIDs[t]); clamped != nil {
			active[t] = clamped
		}
	}
	if len(active) == 0 {
		return bson.D{{Key: "_id", Value: bson.D{{Key: "$exists", Value: false}}}}
	}
	res, _ := BuildPartitionFilterFromCheckpoint(&PartitionCheckpoint{TypeProgress: active})
	return res
}

// ClampPartitionsWithGlobalMinSafeIDs clamps each partition's lower bound to globalMinSafeIDs,
// and skips already-completed partitions/types entirely.
func ClampPartitionsWithGlobalMinSafeIDs(partitions []bson.D, globalMinSafeIDs map[BSONType]any) []bson.D {
	if len(partitions) == 0 || len(globalMinSafeIDs) == 0 {
		return partitions
	}

	clamped := make([]bson.D, len(partitions))
	for i, p := range partitions {
		clamped[i] = clampPartition(p, globalMinSafeIDs)
	}
	return clamped
}

// IsFilterSkipped checks if a partition filter is configured to match no documents ({_id: {$exists: false}}).
func IsFilterSkipped(filter bson.D) bool {
	for _, elem := range filter {
		if elem.Key == "_id" {
			if doc, ok := elem.Value.(bson.D); ok {
				for _, sub := range doc {
					if sub.Key == "$exists" && sub.Value == false {
						return true
					}
				}
			}
		}
	}
	return false
}

// DetermineBackfillResumptionPlan determines the resumption plan by inspecting checkpoint files on disk.
// If checkpoints are missing, unreadable, or corrupted, it falls back to ResumptionModeFresh.
func DetermineBackfillResumptionPlan(dir, db, collection string, expectedPartitions int) (*BackfillResumptionPlan, error) {
	checkpoints, err := ListPartitionCheckpoints(dir, db, collection)
	if err != nil || len(checkpoints) == 0 {
		return &BackfillResumptionPlan{
			Mode: ResumptionModeFresh,
		}, nil
	}

	// If the checkpoint set is complete and matches the expected partition count, we can resume directly.
	if ValidateCheckpointSet(checkpoints, expectedPartitions) {
		initialDocs := make([]int64, len(checkpoints))
		filters := make([]bson.D, len(checkpoints))
		for i, cp := range checkpoints {
			initialDocs[i] = cp.ApproximateDocsMigrated
			filter, err := BuildPartitionFilterFromCheckpoint(cp)
			if err != nil {
				return &BackfillResumptionPlan{Mode: ResumptionModeFresh}, nil
			}
			filters[i] = filter
		}
		return &BackfillResumptionPlan{
			Mode:                 ResumptionModeDirect,
			PartitionFilters:     filters,
			PartitionInitialDocs: initialDocs,
		}, nil
	}

	// If the checkpoint set is complete but the partition count has changed, we need to resample new partitions and clamp lower boundaries using global min IDs.
	if isCompleteHistoricalSet(checkpoints) {
		initialDocs := make([]int64, len(checkpoints))
		for i, cp := range checkpoints {
			initialDocs[i] = cp.ApproximateDocsMigrated
		}
		globalMinIDs, err := ExtractGlobalMinSafeIDs(checkpoints)
		if err != nil {
			return &BackfillResumptionPlan{Mode: ResumptionModeFresh}, nil
		}
		return &BackfillResumptionPlan{
			Mode:                 ResumptionModeResampleWithGlobalMin,
			GlobalMinSafeIDs:     globalMinIDs,
			PartitionInitialDocs: initialDocs,
		}, nil
	}
	// Fresh start, always the fall back.
	return &BackfillResumptionPlan{
		Mode: ResumptionModeFresh,
	}, nil
}
