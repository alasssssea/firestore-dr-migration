package migration

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/partition"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// isQueryMemoryLimitError reports whether err is Firestore's MongoDB-compatible
// endpoint rejecting a query for exceeding its 128 MiB per-query memory ceiling
// (the endpoint buffers a query's full result and fails atomically with
// InvalidArgument: "The query failed since it attempted to use 128.00 MiB when
// the limit is 128.00 MiB"). Matched on the stable phrasing rather than the exact
// number so a future limit change still triggers the page-shrink fallback on the
// keyset read path.
func isQueryMemoryLimitError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "MiB when the limit is") ||
		(strings.Contains(s, "attempted to use") && strings.Contains(s, "MiB"))
}

// bsonIDValue extracts the _id value from a decoded document. Returns ok=false
// if the document has no _id (should not happen for real collections).
func bsonIDValue(doc bson.D) (interface{}, bool) {
	for _, e := range doc {
		if e.Key == "_id" {
			return e.Value, true
		}
	}
	return nil, false
}

// bsonNumericToFloat coerces the numeric BSON _id types to a float64 for
// ordering. Precision loss on very large int64s is irrelevant here: we only use
// it to assert strict monotonicity of a keyset scan, not to compute values.
func bsonNumericToFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

// compareBSONID orders two _id values the same way the _id index does, returning
// -1/0/1 like bytes.Compare and ok=false when the values are not a known,
// comparable _id type. The keyset read path uses this to PROVE that an unsorted
// range scan really came back in ascending _id order: we dropped the server-side
// sort (Firestore's compat endpoint materializes a sorted range in memory and
// blows its 128 MiB limit on dense partitions), so ascending order now rests on
// the _id index scan returning docs in index order. That is not a documented
// guarantee, so the caller asserts monotonicity on every document and fails the
// migration loudly on any violation rather than silently skipping data.
// ObjectIDs are byte-comparable in the same order the index sorts them.
func compareBSONID(a, b interface{}) (int, bool) {
	switch av := a.(type) {
	case primitive.ObjectID:
		bv, ok := b.(primitive.ObjectID)
		if !ok {
			return 0, false
		}
		return bytes.Compare(av[:], bv[:]), true
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(av, bv), true
	}
	if af, aok := bsonNumericToFloat(a); aok {
		if bf, bok := bsonNumericToFloat(b); bok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

// CollectionPartitioner handles partitioning a collection for parallel reads
type CollectionPartitioner struct {
	sourceCollection    *mongo.Collection
	log                 *logger.Logger
	maxPartitions       int
	minDocsPerPartition int
	sampleSize          int
	idTypeForPartition  string
	// forcedPartitionCount, when > 0, overrides the doc-count/knob formula so the
	// caller can impose a byte-aware partition count (see CountByBytes). The
	// engine computes this once and reuses it for both the checkpoint arity and
	// the actual split so the two can never drift.
	forcedPartitionCount int
}

// SetForcedPartitionCount pins the number of partitions the partitioner will
// produce, bypassing the document-count heuristic. Used by the engine to apply a
// byte-aware count that keeps each partition under Firestore's 128 MiB query
// limit.
func (p *CollectionPartitioner) SetForcedPartitionCount(n int) {
	if n > 0 {
		p.forcedPartitionCount = n
	}
}

// NewCollectionPartitioner creates a new collection partitioner
func NewCollectionPartitioner(sourceCollection *mongo.Collection,
	log *logger.Logger, maxPartitions, minDocsPerPartition, sampleSize int, idTypeForPartition string) *CollectionPartitioner {
	return &CollectionPartitioner{
		sourceCollection:    sourceCollection,
		log:                 log,
		maxPartitions:       maxPartitions,
		minDocsPerPartition: minDocsPerPartition,
		sampleSize:          sampleSize,
		idTypeForPartition:  idTypeForPartition,
	}
}

// CalculatePartitionCount calculates the optimal partition count based on
// document count, min docs per partition, and max partitions. It delegates to
// partition.Count, the single source of truth shared with the assessment
// recommender so the advised MaxReadPartitions matches what the engine creates.
func CalculatePartitionCount(totalCount int64, minDocsPerPartition, maxPartitions int) int {
	return partition.Count(totalCount, minDocsPerPartition, maxPartitions)
}

// CalculatePartitionCount calculates the optimal partition count for the partitioner's configured settings.
// A forced count (set via SetForcedPartitionCount) takes precedence over the
// doc-count/knob heuristic, but is still clamped to [1, count].
func (p *CollectionPartitioner) CalculatePartitionCount(count int64) int {
	if p.forcedPartitionCount > 0 {
		n := p.forcedPartitionCount
		if int64(n) > count {
			n = int(count)
		}
		if n < 1 {
			n = 1
		}
		return n
	}
	return CalculatePartitionCount(count, p.minDocsPerPartition, p.maxPartitions)
}

// EstimateAvgDocSize returns the average document size in bytes for the source
// collection. It first asks the server via collStats (cheap, exact), and falls
// back to sampling and measuring real BSON sizes when collStats is unavailable
// or returns nothing — Firestore's MongoDB-compatible endpoint does not
// implement every admin command, so the sampling path must always work. A 1.3x
// safety factor is applied to the sampled figure to absorb size skew (the sample
// can under-represent the largest documents that actually drive the 128 MiB
// query-memory blowup). Returns a conservative 1024 bytes if everything fails.
func EstimateAvgDocSize(ctx context.Context, coll *mongo.Collection, sampleSize int, log *logger.Logger) int64 {
	const fallback int64 = 1024

	// 1) collStats.avgObjSize — authoritative and cheap when supported.
	statsCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	var stats struct {
		AvgObjSize float64 `bson:"avgObjSize"`
	}
	err := coll.Database().RunCommand(statsCtx, bson.D{{Key: "collStats", Value: coll.Name()}}).Decode(&stats)
	cancel()
	if err == nil && stats.AvgObjSize > 0 {
		return int64(stats.AvgObjSize)
	}
	if log != nil {
		log.Infof("collStats unavailable for %s (%v); estimating average document size by sampling", coll.Name(), err)
	}

	// 2) Sample real documents and measure their marshalled BSON size.
	if sampleSize <= 0 {
		sampleSize = 200
	}
	if sampleSize > 1000 {
		sampleSize = 1000 // cap: sampling is only to size partitions, not to be exact
	}
	sampleCtx, cancel2 := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel2()
	cursor, err := coll.Aggregate(sampleCtx, mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
	})
	if err != nil {
		if log != nil {
			log.Warnf("failed to sample %s for average document size (%v); assuming %d bytes/doc", coll.Name(), err, fallback)
		}
		return fallback
	}
	defer cursor.Close(sampleCtx)

	var total int64
	var n int64
	for cursor.Next(sampleCtx) {
		total += int64(len(cursor.Current)) // cursor.Current is the raw BSON document
		n++
	}
	if n == 0 || total == 0 {
		if log != nil {
			log.Warnf("sampling returned no documents for %s; assuming %d bytes/doc", coll.Name(), fallback)
		}
		return fallback
	}
	avg := total / n
	withSkew := avg + avg*3/10 // 1.3x safety factor
	if withSkew < 1 {
		withSkew = fallback
	}
	return withSkew
}

// Partition creates partitions for a collection
func (p *CollectionPartitioner) Partition(ctx context.Context) ([]bson.D, error) {
	// Count documents to determine if partitioning is needed
	// Use a longer timeout for the count operation
	countCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	count, err := p.sourceCollection.EstimatedDocumentCount(countCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to count documents: %w", err)
	}

	// Calculate optimal partition count
	partitionCount := p.CalculatePartitionCount(count)
	p.log.Infof("Partitioning collection with %d documents into %d partitions", count, partitionCount)

	// If only one partition, return a single empty filter
	if partitionCount == 1 {
		return []bson.D{{}}, nil
	}

	strategy := p.idTypeForPartition
	if strategy == "auto" || strategy == "" {
		detected, err := p.RecommendIDPartitioning(ctx)
		if err != nil {
			p.log.Warnf("Failed to auto-detect partitioning ID type: %v. Falling back to 'mixed'", err)
			strategy = "mixed"
		} else {
			p.log.Infof("Auto-detected partition ID strategy type: %s", detected)
			strategy = detected
		}
	}

	// Determine partition strategy based on strategy
	var partitions []bson.D
	var partErr error
	switch strategy {
	case "objectid":
		partitions, partErr = p.createObjectIDPartitionsWithSampling(ctx, partitionCount)
	case "numeric":
		partitions, partErr = p.createNumericPartitionsWithSampling(ctx, partitionCount)
	case "mixed", "auto", "":
		partitions, partErr = p.createPartitionsGroupedByType(ctx, partitionCount)
	default:
		p.log.Warnf("Unrecognized partitioning strategy '%s', falling back to grouped-by-type mixed mode partitioning", strategy)
		partitions, partErr = p.createPartitionsGroupedByType(ctx, partitionCount)
	}
	if partErr != nil {
		return nil, partErr
	}
	// Every create* helper leaves the final partition open on the upper side
	// ({_id:{$gte:X}}). Under continuous writes, keyset pagination would tail
	// newly-inserted _ids in that partition forever, so backfill never completes
	// and the change-stream/incremental phase never starts. Pin the final partition
	// to the collection's current max _id (a snapshot ceiling); anything written
	// past it is captured by the change stream, whose resume token predates backfill.
	return p.capFinalPartitionUpperBound(ctx, partitions)
}

// capFinalPartitionUpperBound bounds the final open-ended partition at the
// collection's current max _id so backfill terminates under continuous write load.
// It only rewrites a simple {_id:{<lower ops>}} filter with no existing upper bound;
// type-bracketed or $or partitions (mixed-type collections) are left untouched so the
// legacy read path keeps owning them.
func (p *CollectionPartitioner) capFinalPartitionUpperBound(ctx context.Context, partitions []bson.D) ([]bson.D, error) {
	if len(partitions) == 0 {
		return partitions, nil
	}
	last := partitions[len(partitions)-1]
	// Expect exactly {_id: <bson.D of range ops>}.
	if len(last) != 1 || last[0].Key != "_id" {
		return partitions, nil
	}
	inner, ok := last[0].Value.(bson.D)
	if !ok || len(inner) == 0 {
		return partitions, nil
	}
	for _, cond := range inner {
		switch cond.Key {
		case "$lt", "$lte":
			return partitions, nil // already upper-bounded
		case "$type", "$or", "$in":
			return partitions, nil // not a plain range; leave to legacy path
		}
	}
	// We need the TRUE current max _id to cap the final partition. It must be the
	// real max, NOT a $sample-derived approximation: the sampled max sits below the
	// true max, and any document whose _id lands in (sampledMax, trueMax] but which
	// already existed when the change-stream resume token was captured is then lost
	// forever — it is excluded from backfill by the $lte ceiling AND never redelivered
	// by the change stream (its insert predates the resume token). That is the root
	// cause of the "some collections stay a few hundred/thousand docs behind, nothing
	// in the DLQ, and the gap never drains after writes stop" symptom, and the deficit
	// scales as docCount/sampleSize (≈ hundreds–thousands on large collections).
	//
	// We also cannot use FindOne(sort:{_id:-1}): a blocking sort buffers the whole
	// result and blows Firestore's 128 MiB per-query limit on large collections.
	//
	// Instead we seed cheaply from a bounded $sample max, then walk the (small) tail
	// above it with ascending keyset pages. Each page is a tiny _id-only projection
	// far under 128 MiB, the tail is only ~docCount/sampleSize docs, and because per-
	// page read throughput vastly exceeds the incoming write rate the walk converges
	// to the current true max within a handful of pages and always terminates.
	maxID, err := p.findTrueMaxID(ctx, inner)
	if err != nil {
		return nil, err
	}
	if maxID == nil {
		// Empty range (no documents at or above the lower bound): nothing to cap.
		return partitions, nil
	}
	bounded := make(bson.D, len(inner), len(inner)+1)
	copy(bounded, inner)
	bounded = append(bounded, bson.E{Key: "$lte", Value: maxID})
	partitions[len(partitions)-1] = bson.D{{Key: "_id", Value: bounded}}
	p.log.Infof("Bounded final backfill partition at true snapshot max _id (%v) to guarantee termination under live writes without dropping pre-token documents", maxID)
	return partitions, nil
}

// buildKeysetTailFilter builds the {_id: {...}} filter for one ascending keyset
// page: it preserves the partition's lower-bound ops and, when a cursor is known,
// adds a STRICT `$gt` on the last _id already seen. Strictness is load-bearing —
// `$gte` would re-read the boundary document every page (non-termination), while
// omitting the bound would rescan from the start. The lower ops are copied so the
// caller's slice is never mutated across pages.
func buildKeysetTailFilter(lowerOps bson.D, cursorID interface{}) bson.D {
	idOps := make(bson.D, len(lowerOps), len(lowerOps)+1)
	copy(idOps, lowerOps)
	if cursorID != nil {
		idOps = append(idOps, bson.E{Key: "$gt", Value: cursorID})
	}
	return bson.D{{Key: "_id", Value: idOps}}
}

// findTrueMaxID returns the exact maximum _id among documents matching the given
// lower-bound range ops (e.g. {$gte: X}), or nil if the range is empty.
//
// It avoids a blocking sort (which blows Firestore's 128 MiB per-query limit) by
// seeding from a cheap bounded $sample max and then paging forward over the small
// tail above that seed with ascending keyset reads. Correctness does not depend on
// the sample: even if $sample returns nothing, the keyset walk starts from the
// lower bound and still finds the true max. Each page reads only _id (tiny), so no
// single query approaches the memory ceiling.
func (p *CollectionPartitioner) findTrueMaxID(ctx context.Context, lowerOps bson.D) (interface{}, error) {
	sampleSize := p.sampleSize
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	// Seed: cheap sampled max to skip past the bulk of the collection. Best-effort —
	// on error or empty result we simply start the keyset walk from the lower bound.
	var cursorID interface{}
	if cur, err := p.sourceCollection.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "max", Value: bson.D{{Key: "$max", Value: "$_id"}}},
		}}},
	}); err == nil {
		if cur.Next(ctx) {
			var res bson.M
			if derr := cur.Decode(&res); derr == nil {
				cursorID = res["max"]
			}
		}
		cur.Close(ctx)
	}

	// Keyset walk: read the tail in ascending _id order, page by page, tracking the
	// last _id seen. A page is filtered by the partition's lower ops AND _id > cursor
	// (when a cursor/seed is known). Stop when a page returns fewer than pageSize
	// documents — we have reached the current true max.
	const pageSize = 10000
	// Guard against pathological non-termination (e.g. write rate somehow exceeding
	// read throughput). Bounds the walk to pageSize*maxPages documents above the seed.
	const maxPages = 100000

	maxID := cursorID
	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(pageSize).
		SetProjection(bson.D{{Key: "_id", Value: 1}})

	for page := 0; page < maxPages; page++ {
		filter := buildKeysetTailFilter(lowerOps, cursorID)

		cur, err := p.sourceCollection.Find(ctx, filter, findOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to keyset-scan tail for true max _id: %w", err)
		}

		var n int
		for cur.Next(ctx) {
			var doc bson.D
			if derr := cur.Decode(&doc); derr != nil {
				cur.Close(ctx)
				return nil, fmt.Errorf("failed to decode _id during true-max keyset scan: %w", derr)
			}
			if id := extractDocID(doc); id != nil {
				maxID = id
				cursorID = id
			}
			n++
		}
		if cerr := cur.Err(); cerr != nil {
			cur.Close(ctx)
			return nil, fmt.Errorf("cursor error during true-max keyset scan: %w", cerr)
		}
		cur.Close(ctx)

		if n < pageSize {
			// Reached the end of the tail: cursorID is the current true max.
			return maxID, nil
		}
	}

	p.log.Warnf("true-max keyset scan hit page cap (%d pages of %d); using highest _id seen (%v) as final partition ceiling", maxPages, pageSize, maxID)
	return maxID, nil
}

// createObjectIDPartitionsWithSampling creates partitions based on ObjectID sampling
func (p *CollectionPartitioner) createObjectIDPartitionsWithSampling(ctx context.Context, partitionCount int) ([]bson.D, error) {
	// Adjust sample size based on collection size, but ensure it's large enough
	sampleSize := p.sampleSize
	if sampleSize < partitionCount*10 {
		sampleSize = partitionCount * 10 // Ensure at least 10 samples per partition
	}

	p.log.Infof("Sampling %d documents to create %d partitions", sampleSize, partitionCount)

	// Sample documents to understand the _id distribution
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	cursor, err := p.sourceCollection.Aggregate(sampleCtx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close(sampleCtx)

	// Collect all sampled _ids
	var sampledIDs []primitive.ObjectID
	for cursor.Next(sampleCtx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document: %w", err)
		}

		if id, ok := doc["_id"].(primitive.ObjectID); ok {
			sampledIDs = append(sampledIDs, id)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during sampling: %w", err)
	}

	// Deduplicate sampled IDs
	if len(sampledIDs) > 0 {
		uniqueIDs := sampledIDs[:1]
		for _, id := range sampledIDs[1:] {
			if id != uniqueIDs[len(uniqueIDs)-1] {
				uniqueIDs = append(uniqueIDs, id)
			}
		}
		sampledIDs = uniqueIDs
	}

	// If not enough samples, fall back to min/max approach
	if len(sampledIDs) < partitionCount+1 {
		p.log.Warnf("Not enough samples (%d) for %d partitions, falling back to min/max approach",
			len(sampledIDs), partitionCount)
		return p.createObjectIDPartitionsWithMinMax(ctx)
	}

	// Use sampled IDs to create partitions
	partitions := make([]bson.D, 0, partitionCount)

	// Calculate step size to evenly distribute partitions
	step := len(sampledIDs) / (partitionCount + 1)

	// Create partition filters
	for i := 0; i < partitionCount; i++ {
		startIdx := (i + 1) * step // Skip the first step to avoid edge cases
		endIdx := (i + 2) * step

		if endIdx >= len(sampledIDs) {
			endIdx = len(sampledIDs) - 1
		}

		startID := sampledIDs[startIdx]
		endID := sampledIDs[endIdx]

		if i == 0 {
			// First partition includes everything up to the first boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition includes everything from the last boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createObjectIDPartitionsWithMinMax creates partitions based on min/max ObjectIDs
func (p *CollectionPartitioner) createObjectIDPartitionsWithMinMax(ctx context.Context) ([]bson.D, error) {
	// Find min and max ObjectIDs
	var minDoc, maxDoc bson.M

	err := p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}

	err = p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}

	minID, _ := minDoc["_id"].(primitive.ObjectID)
	maxID, _ := maxDoc["_id"].(primitive.ObjectID)

	// For ObjectIDs, we can use timestamp-based partitioning
	minTime := minID.Timestamp()
	maxTime := maxID.Timestamp()
	timeRange := maxTime.Sub(minTime)

	// Calculate optimal partition count based on time range
	partitionCount := p.maxPartitions
	partitionDuration := timeRange / time.Duration(partitionCount)

	// Create partitions
	partitions := make([]bson.D, 0, partitionCount)

	for i := 0; i < partitionCount; i++ {
		startTime := minTime.Add(partitionDuration * time.Duration(i))
		startID := primitive.NewObjectIDFromTimestamp(startTime)

		var endTime time.Time
		if i == partitionCount-1 {
			// Last partition includes the max ID
			endTime = maxTime.Add(time.Second) // Add a second to ensure inclusion
		} else {
			endTime = minTime.Add(partitionDuration * time.Duration(i+1))
		}
		endID := primitive.NewObjectIDFromTimestamp(endTime)

		if i == 0 {
			// First partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createNumericPartitionsWithSampling creates partitions based on numeric _id sampling
func (p *CollectionPartitioner) createNumericPartitionsWithSampling(ctx context.Context, partitionCount int) ([]bson.D, error) {
	// Adjust sample size based on collection size, but ensure it's large enough
	sampleSize := p.sampleSize
	if sampleSize < partitionCount*10 {
		sampleSize = partitionCount * 10 // Ensure at least 10 samples per partition
	}

	p.log.Infof("Sampling %d documents to create %d partitions", sampleSize, partitionCount)

	// Sample documents to understand the _id distribution
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	cursor, err := p.sourceCollection.Aggregate(sampleCtx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close(sampleCtx)

	// Collect all sampled _ids as float64 for consistent handling
	var sampledIDs []float64
	for cursor.Next(sampleCtx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document: %w", err)
		}

		// Convert various numeric types to float64
		var numericID float64
		switch id := doc["_id"].(type) {
		case int:
			numericID = float64(id)
		case int32:
			numericID = float64(id)
		case int64:
			numericID = float64(id)
		case float64:
			numericID = id
		default:
			continue // Skip non-numeric IDs
		}

		sampledIDs = append(sampledIDs, numericID)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during sampling: %w", err)
	}

	// Deduplicate sampled IDs
	if len(sampledIDs) > 0 {
		uniqueIDs := sampledIDs[:1]
		for _, id := range sampledIDs[1:] {
			if id != uniqueIDs[len(uniqueIDs)-1] {
				uniqueIDs = append(uniqueIDs, id)
			}
		}
		sampledIDs = uniqueIDs
	}

	// If not enough samples, fall back to min/max approach
	if len(sampledIDs) < partitionCount+1 {
		p.log.Warnf("Not enough samples (%d) for %d partitions, falling back to min/max approach",
			len(sampledIDs), partitionCount)
		return p.createNumericPartitionsWithMinMax(ctx)
	}

	// Use sampled IDs to create partitions
	partitions := make([]bson.D, 0, partitionCount)

	// Calculate step size to evenly distribute partitions
	step := len(sampledIDs) / (partitionCount + 1)

	// Create partition filters
	for i := 0; i < partitionCount; i++ {
		startIdx := (i + 1) * step // Skip the first step to avoid edge cases
		endIdx := (i + 2) * step

		if endIdx >= len(sampledIDs) {
			endIdx = len(sampledIDs) - 1
		}

		startID := sampledIDs[startIdx]
		endID := sampledIDs[endIdx]

		if i == 0 {
			// First partition includes everything up to the first boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition includes everything from the last boundary
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// createNumericPartitionsWithMinMax creates partitions based on min/max numeric IDs
func (p *CollectionPartitioner) createNumericPartitionsWithMinMax(ctx context.Context) ([]bson.D, error) {
	// Find min and max numeric IDs
	var minDoc, maxDoc bson.M

	err := p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find min _id: %w", err)
	}

	err = p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to find max _id: %w", err)
	}

	// Convert to float64 for consistent handling
	var minID, maxID float64

	switch id := minDoc["_id"].(type) {
	case int:
		minID = float64(id)
	case int32:
		minID = float64(id)
	case int64:
		minID = float64(id)
	case float64:
		minID = id
	}

	switch id := maxDoc["_id"].(type) {
	case int:
		maxID = float64(id)
	case int32:
		maxID = float64(id)
	case int64:
		maxID = float64(id)
	case float64:
		maxID = id
	}

	// Calculate range for each partition
	partitionCount := p.maxPartitions
	idRange := maxID - minID
	partitionSize := idRange / float64(partitionCount)

	// Create partitions
	partitions := make([]bson.D, 0, partitionCount)

	for i := 0; i < partitionCount; i++ {
		startID := minID + (partitionSize * float64(i))

		var endID float64
		if i == partitionCount-1 {
			// Last partition includes the max ID
			endID = maxID + 1 // Add 1 to ensure inclusion
		} else {
			endID = minID + (partitionSize * float64(i+1))
		}

		if i == 0 {
			// First partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
		} else if i == partitionCount-1 {
			// Last partition
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
		} else {
			// Middle partitions
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}

	return partitions, nil
}

// Helper function to interpolate between two hex strings
func interpolateHex(minHex, maxHex string, ratio float64) string {
	if len(minHex) != len(maxHex) {
		return minHex // Fallback
	}

	result := make([]byte, len(minHex))

	for i := 0; i < len(minHex); i++ {
		minVal, _ := strconv.ParseInt(string(minHex[i]), 16, 8)
		maxVal, _ := strconv.ParseInt(string(maxHex[i]), 16, 8)

		interpolated := minVal + int64(ratio*float64(maxVal-minVal))
		if interpolated > 15 {
			interpolated = 15
		}

		result[i] = "0123456789abcdef"[interpolated]
	}

	return string(result)
}

// createPartitionsWithSampling creates partitions using sampling for any sortable _id type
func (p *CollectionPartitioner) createPartitionsWithSampling(ctx context.Context, partitionCount int) ([]bson.D, error) {
	sampleSize := max(p.sampleSize, partitionCount*10)
	p.log.Infof("Sampling %d documents to create %d partitions", sampleSize, partitionCount)

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	cursor, err := p.sourceCollection.Aggregate(sampleCtx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample documents: %w", err)
	}
	defer cursor.Close(sampleCtx)

	type idDoc struct {
		ID interface{} `bson:"_id"`
	}

	var sampledIDs []interface{}
	for cursor.Next(sampleCtx) {
		var doc idDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document: %w", err)
		}
		if doc.ID != nil {
			sampledIDs = append(sampledIDs, doc.ID)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during sampling: %w", err)
	}

	// Deduplicate sampled IDs using reflect.DeepEqual (supports slice/document IDs)
	if len(sampledIDs) > 0 {
		uniqueIDs := sampledIDs[:1]
		for _, id := range sampledIDs[1:] {
			if !reflect.DeepEqual(id, uniqueIDs[len(uniqueIDs)-1]) {
				uniqueIDs = append(uniqueIDs, id)
			}
		}
		sampledIDs = uniqueIDs
	}

	if len(sampledIDs) < partitionCount {
		p.log.Warnf("Not enough samples (%d) for %d partitions, falling back to single partition", len(sampledIDs), partitionCount)
		return []bson.D{{}}, nil
	}

	return buildPartitionFilters(sampledIDs, partitionCount), nil
}

// buildPartitionFilters constructs contiguous range BSON filters from sorted sampled _ids
func buildPartitionFilters(sampledIDs []interface{}, partitionCount int) []bson.D {
	partitions := make([]bson.D, 0, partitionCount)
	step := len(sampledIDs) / partitionCount

	for i := 0; i < partitionCount; i++ {
		if i == 0 {
			// First partition: unbounded lower
			endID := sampledIDs[step]
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: endID}}}})
			continue
		}

		startID := sampledIDs[i*step]

		if i == partitionCount-1 {
			// Last partition: unbounded upper
			partitions = append(partitions, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}}}})
			continue
		}

		// Middle partitions: bounded both sides
		endID := sampledIDs[(i+1)*step]
		partitions = append(partitions, bson.D{
			{Key: "_id", Value: bson.D{{Key: "$gte", Value: startID}, {Key: "$lt", Value: endID}}},
		})
	}

	return partitions
}

// RecommendIDPartitioning samples the collection and recommends the best ID partitioning strategy
func (p *CollectionPartitioner) RecommendIDPartitioning(ctx context.Context) (string, error) {
	// Sample documents
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: p.sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	cursor, err := p.sourceCollection.Aggregate(sampleCtx, pipeline)
	if err != nil {
		return "", fmt.Errorf("failed to sample for recommendation: %w", err)
	}
	defer cursor.Close(sampleCtx)

	var objectIDCount int
	var numericCount int
	var otherCount int
	var totalSampled int

	for cursor.Next(sampleCtx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return "", fmt.Errorf("failed to decode sample doc: %w", err)
		}
		totalSampled++
		idVal, ok := doc["_id"]
		if !ok {
			otherCount++
			continue
		}

		switch idVal.(type) {
		case primitive.ObjectID:
			objectIDCount++
		case int, int32, int64, float64:
			numericCount++
		default:
			otherCount++
		}
	}

	if err := cursor.Err(); err != nil {
		return "", fmt.Errorf("cursor error during sampling for recommendation: %w", err)
	}

	if totalSampled == 0 {
		return "mixed", nil // Fallback/empty collection
	}

	p.log.Infof("ID partitioning analysis for %s: sampled %d docs. ObjectIDs: %d, Numerics: %d, Others: %d",
		p.sourceCollection.Name(), totalSampled, objectIDCount, numericCount, otherCount)

	if objectIDCount == totalSampled {
		return "objectid", nil
	} else if numericCount == totalSampled {
		return "numeric", nil
	} else {
		return "mixed", nil
	}
}

// createPartitionsGroupedByType groups document IDs by BSON type, partitions each type
// using quantile sampling (>= 2,000 docs) or uniform range fallback (< 2,000 docs),
// and merges the filters for different types together via $or.
func (p *CollectionPartitioner) createPartitionsGroupedByType(ctx context.Context, partitionCount int) ([]bson.D, error) {
	typeCounts, err := p.discoverPresentBSONTypes(ctx)
	if err != nil {
		p.log.Warnf("Failed to discover BSON types for partitioning (%v), falling back to legacy sampling", err)
		return p.createPartitionsWithSampling(ctx, partitionCount)
	}

	if len(typeCounts) == 0 {
		return []bson.D{{}}, nil
	}

	slicesPerType := make(map[string][]bson.D)
	for tName, cnt := range typeCounts {
		var slices []bson.D
		var err error
		if cnt >= 2000 {
			slices, err = p.createQuantileSlicesForType(ctx, tName, partitionCount)
			if err != nil || len(slices) != partitionCount {
				p.log.Warnf("Quantile sampling failed for BSON type '%s' (err: %v), falling back to uniform slicing", tName, err)
				slices, _ = p.createUniformSlicesForType(ctx, tName, partitionCount, cnt)
			}
		} else {
			slices, _ = p.createUniformSlicesForType(ctx, tName, partitionCount, cnt)
		}
		if len(slices) == partitionCount {
			slicesPerType[tName] = slices
		}
	}

	if len(slicesPerType) == 0 {
		return []bson.D{{}}, nil
	}

	return mergeTypeSlices(slicesPerType, partitionCount), nil
}

// discoverPresentBSONTypes aggregates document counts per BSON type of the _id field using DiscoverPresentBSONTypeCounts.
func (p *CollectionPartitioner) discoverPresentBSONTypes(ctx context.Context) (map[string]int64, error) {
	counts, err := DiscoverPresentBSONTypeCounts(ctx, p.sourceCollection, 2000)
	if err != nil {
		return nil, err
	}
	typeCounts := make(map[string]int64, len(counts))
	for bType, cnt := range counts {
		typeCounts[string(bType)] = cnt
		p.log.Infof("Discovered present BSON type '%s' (sample count capped at 2000: %d)", bType, cnt)
	}
	return typeCounts, nil
}

// createQuantileSlicesForType samples documents of a specific BSON type to extract numSplits quantile slices.
func (p *CollectionPartitioner) createQuantileSlicesForType(ctx context.Context, typeName string, numSplits int) ([]bson.D, error) {
	sampleSize := max(1000, 64*numSplits)
	p.log.Infof("Type-scoped sampling %d documents for BSON type '%s' into %d slices", sampleSize, typeName, numSplits)

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: typeName}}}}}},
		bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	sampleCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	cursor, err := p.sourceCollection.Aggregate(sampleCtx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to sample for type %s: %w", typeName, err)
	}
	defer cursor.Close(sampleCtx)

	type idDoc struct {
		ID interface{} `bson:"_id"`
	}

	var sampledIDs []interface{}
	for cursor.Next(sampleCtx) {
		var doc idDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode sampled document for type %s: %w", typeName, err)
		}
		if doc.ID != nil {
			sampledIDs = append(sampledIDs, doc.ID)
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error during type-scoped sampling: %w", err)
	}

	if len(sampledIDs) > 0 {
		uniqueIDs := sampledIDs[:1]
		for _, id := range sampledIDs[1:] {
			if !reflect.DeepEqual(id, uniqueIDs[len(uniqueIDs)-1]) {
				uniqueIDs = append(uniqueIDs, id)
			}
		}
		sampledIDs = uniqueIDs
	}

	if len(sampledIDs) < numSplits {
		return nil, fmt.Errorf("not enough unique samples (%d) for %d slices of type %s", len(sampledIDs), numSplits, typeName)
	}

	return buildTypeScopedPartitionFilters(sampledIDs, numSplits, typeName), nil
}

// buildTypeScopedPartitionFilters builds numSplits range queries guarded by $type: typeName.
func buildTypeScopedPartitionFilters(sampledIDs []interface{}, numSplits int, typeName string) []bson.D {
	partitions := make([]bson.D, 0, numSplits)
	if numSplits <= 0 {
		return partitions
	}
	if numSplits == 1 {
		return []bson.D{{{Key: "_id", Value: bson.D{{Key: "$type", Value: typeName}}}}}
	}

	step := len(sampledIDs) / numSplits

	for i := 0; i < numSplits; i++ {
		if i == 0 {
			endID := sampledIDs[step]
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: typeName},
					{Key: "$lt", Value: endID},
				}},
			})
			continue
		}

		startID := sampledIDs[i*step]

		if i == numSplits-1 {
			partitions = append(partitions, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: typeName},
					{Key: "$gte", Value: startID},
				}},
			})
			continue
		}

		endID := sampledIDs[(i+1)*step]
		partitions = append(partitions, bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: typeName},
				{Key: "$gte", Value: startID},
				{Key: "$lt", Value: endID},
			}},
		})
	}

	return partitions
}

// createUniformSlicesForType generates deterministic uniform slices without sampling for small-count types (< 2,000 docs).
func (p *CollectionPartitioner) createUniformSlicesForType(ctx context.Context, typeName string, numSplits int, count int64) ([]bson.D, error) {
	if numSplits <= 1 {
		return []bson.D{{{Key: "_id", Value: bson.D{{Key: "$type", Value: typeName}}}}}, nil
	}

	switch typeName {
	case "number":
		return createNumberUniformSlices(numSplits), nil
	case "objectId":
		slices, err := p.createObjectIdUniformSlices(ctx, numSplits)
		if err != nil {
			return createStringUniformSlices("objectId", numSplits), nil
		}
		return slices, nil
	default:
		return createStringUniformSlices(typeName, numSplits), nil
	}
}

func createNumberUniformSlices(numSplits int) []bson.D {
	slices := make([]bson.D, numSplits)
	for i := 0; i < numSplits; i++ {
		slices[i] = bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "$type", Value: "number"},
				{Key: "$mod", Value: bson.A{numSplits, i}},
			}},
		}
	}
	return slices
}

func createStringUniformSlices(typeName string, numSplits int) []bson.D {
	slices := make([]bson.D, numSplits)
	step := 256 / numSplits
	if step < 1 {
		step = 1
	}

	for i := 0; i < numSplits; i++ {
		if i == 0 {
			endPrefix := fmt.Sprintf("%02x", step)
			slices[i] = bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: typeName},
					{Key: "$lt", Value: endPrefix},
				}},
			}
		} else if i == numSplits-1 {
			startPrefix := fmt.Sprintf("%02x", i*step)
			slices[i] = bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: typeName},
					{Key: "$gte", Value: startPrefix},
				}},
			}
		} else {
			startPrefix := fmt.Sprintf("%02x", i*step)
			endPrefix := fmt.Sprintf("%02x", (i+1)*step)
			slices[i] = bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: typeName},
					{Key: "$gte", Value: startPrefix},
					{Key: "$lt", Value: endPrefix},
				}},
			}
		}
	}
	return slices
}

func (p *CollectionPartitioner) createObjectIdUniformSlices(ctx context.Context, numSplits int) ([]bson.D, error) {
	var minDoc, maxDoc bson.M
	err := p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: 1}})).Decode(&minDoc)
	if err != nil {
		return nil, err
	}
	err = p.sourceCollection.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}})).Decode(&maxDoc)
	if err != nil {
		return nil, err
	}

	minID, ok1 := minDoc["_id"].(primitive.ObjectID)
	maxID, ok2 := maxDoc["_id"].(primitive.ObjectID)
	if !ok1 || !ok2 || minID == maxID {
		return nil, fmt.Errorf("invalid or identical min/max ObjectID")
	}

	minTime := minID.Timestamp()
	maxTime := maxID.Timestamp()
	timeRange := maxTime.Sub(minTime)
	partitionDuration := timeRange / time.Duration(numSplits)

	slices := make([]bson.D, 0, numSplits)
	for i := 0; i < numSplits; i++ {
		startTime := minTime.Add(partitionDuration * time.Duration(i))
		startID := primitive.NewObjectIDFromTimestamp(startTime)

		var endTime time.Time
		if i == numSplits-1 {
			endTime = maxTime.Add(time.Second)
		} else {
			endTime = minTime.Add(partitionDuration * time.Duration(i+1))
		}
		endID := primitive.NewObjectIDFromTimestamp(endTime)

		if i == 0 {
			slices = append(slices, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "objectId"},
					{Key: "$lt", Value: endID},
				}},
			})
		} else if i == numSplits-1 {
			slices = append(slices, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "objectId"},
					{Key: "$gte", Value: startID},
				}},
			})
		} else {
			slices = append(slices, bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "$type", Value: "objectId"},
					{Key: "$gte", Value: startID},
					{Key: "$lt", Value: endID},
				}},
			})
		}
	}
	return slices, nil
}

// mergeTypeSlices combines the i-th slice of every BSON type into an $or query for partition i.
// If only one BSON type exists, the $or wrapper is omitted.
func mergeTypeSlices(slicesPerType map[string][]bson.D, numSplits int) []bson.D {
	var types []string
	for t := range slicesPerType {
		types = append(types, t)
	}
	sort.Strings(types)

	result := make([]bson.D, numSplits)
	for i := 0; i < numSplits; i++ {
		if len(types) == 1 {
			t := types[0]
			if i < len(slicesPerType[t]) {
				result[i] = slicesPerType[t][i]
			} else {
				result[i] = bson.D{}
			}
			continue
		}

		var orClauses []bson.D
		for _, t := range types {
			slices := slicesPerType[t]
			if i < len(slices) && len(slices[i]) > 0 {
				orClauses = append(orClauses, slices[i])
			}
		}

		if len(orClauses) == 1 {
			result[i] = orClauses[0]
		} else if len(orClauses) > 1 {
			orArray := make(bson.A, len(orClauses))
			for idx, clause := range orClauses {
				orArray[idx] = clause
			}
			result[i] = bson.D{{Key: "$or", Value: orArray}}
		} else {
			result[i] = bson.D{}
		}
	}
	return result
}
