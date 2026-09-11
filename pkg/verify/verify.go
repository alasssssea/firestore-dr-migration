package verify

import (
	"context"
	"fmt"
	"sort"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/idmap"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/migration"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Options tunes a verification run.
type Options struct {
	// Hash enables content verification (canonical, order-independent hashing)
	// in addition to the always-run document count comparison. Count-only is
	// cheap; hashing streams every document on both sides.
	Hash bool
	// MaxMismatch bounds how many differing _ids are listed per collection when
	// a hash mismatch is drilled down. 0 means use a sane default.
	MaxMismatch int
	// IDMapPath is the id-mapping JSONL produced during migration. Required to
	// reconcile documents whose _id was converted (lossy) by the migrator.
	IDMapPath string
}

const defaultMaxMismatch = 20

// CollectionResult is the outcome for one verified collection.
type CollectionResult struct {
	Database     string   `json:"database"`
	Source       string   `json:"source"`
	Target       string   `json:"target"`
	SourceCount  int64    `json:"sourceCount"`
	TargetCount  int64    `json:"targetCount"`
	CountMatch   bool     `json:"countMatch"`
	HashChecked  bool     `json:"hashChecked"`
	SourceHash   string   `json:"sourceHash,omitempty"`
	TargetHash   string   `json:"targetHash,omitempty"`
	HashMatch    bool     `json:"hashMatch"`
	Mismatches   []string `json:"mismatches,omitempty"`
	MismatchMore int      `json:"mismatchMore,omitempty"`
	Err          string   `json:"err,omitempty"`
}

// OK reports whether the collection fully verified.
func (c CollectionResult) OK() bool {
	if c.Err != "" || !c.CountMatch {
		return false
	}
	if c.HashChecked {
		return c.HashMatch
	}
	return true
}

// Report aggregates all collection results.
type Report struct {
	Collections []CollectionResult `json:"collections"`
	Pairs       int                `json:"pairs"`
}

// OK reports whether every collection verified.
func (r *Report) OK() bool {
	for _, c := range r.Collections {
		if !c.OK() {
			return false
		}
	}
	return true
}

// Run verifies every database pair and returns a consolidated report.
func Run(ctx context.Context, cfg *config.Config, log *logger.Logger, opts Options) (*Report, error) {
	if opts.MaxMismatch <= 0 {
		opts.MaxMismatch = defaultMaxMismatch
	}

	// The id-map lets verification match converted _ids; absent one, converted
	// documents will appear as mismatches, so we open read-only if present.
	var mapper idmap.Store = idmap.NopStore{}
	if opts.Hash && opts.IDMapPath != "" {
		fs, err := idmap.OpenFileStore(opts.IDMapPath)
		if err != nil {
			log.Warnf("could not open id-map %s (converted _ids may show as mismatches): %v", opts.IDMapPath, err)
		} else {
			defer fs.Close()
			mapper = fs
		}
	}

	// Build the same field transformer the migrator used, so source documents
	// are hashed after the identical transform the target already went through.
	transformer := migration.NewFieldTransformer(cfg.DropEmptyFieldNames, cfg.ConvertLongFieldNamesInNestedDocs, cfg.RetryConfig.ConvertInvalidIds, log)

	report := &Report{}
	for _, pair := range cfg.DatabasePairs {
		report.Pairs++
		results := verifyPair(ctx, pair, mapper, transformer, opts, log)
		report.Collections = append(report.Collections, results...)
	}
	return report, nil
}

func verifyPair(ctx context.Context, pair config.DatabasePair, mapper idmap.Store, transformer *migration.FieldTransformer, opts Options, log *logger.Logger) []CollectionResult {
	// Pre-3.6 sources (oplog-legacy) must use the mgo driver; the target is a
	// modern Firestore MongoDB-compat endpoint, so it always uses the modern one.
	source, err := newSrcReader(pair.Source.ConnectionString, pair.Source.Database, pair.Source.ReplicationMethod, log)
	if err != nil {
		return []CollectionResult{{Database: pair.Source.Database, Err: fmt.Sprintf("connect source: %v", err)}}
	}
	defer source.close(ctx)

	target, err := db.NewMongoDB(pair.Target.ConnectionString, pair.Target.Database, 0, 8, 30*time.Second, nil, log)
	if err != nil {
		return []CollectionResult{{Database: pair.Source.Database, Err: fmt.Sprintf("connect target: %v", err)}}
	}
	defer target.Close(ctx)

	pairs := collectionPairs(ctx, source, pair)
	var out []CollectionResult
	for _, cp := range pairs {
		out = append(out, verifyCollection(ctx, source, target, pair.Source.Database, cp, mapper, transformer, opts, log))
	}
	return out
}

// collectionPair is one source→target collection name mapping to verify.
type collectionPair struct{ source, target string }

func collectionPairs(ctx context.Context, source srcReader, pair config.DatabasePair) []collectionPair {
	if len(pair.Target.Collections) > 0 {
		var out []collectionPair
		for _, c := range pair.Target.Collections {
			out = append(out, collectionPair{source: c.SourceCollection, target: c.TargetCollection})
		}
		return out
	}
	names, err := source.listCollections(ctx)
	if err != nil {
		return nil
	}
	out := make([]collectionPair, 0, len(names))
	for _, n := range names {
		out = append(out, collectionPair{source: n, target: n})
	}
	return out
}

func verifyCollection(ctx context.Context, source srcReader, target *db.MongoDB, dbName string, cp collectionPair, mapper idmap.Store, transformer *migration.FieldTransformer, opts Options, log *logger.Logger) CollectionResult {
	res := CollectionResult{Database: dbName, Source: cp.source, Target: cp.target}

	tgtColl := target.GetCollection(cp.target)

	var err error
	res.SourceCount, err = source.count(ctx, cp.source)
	if err != nil {
		res.Err = fmt.Sprintf("count source: %v", err)
		return res
	}
	res.TargetCount, err = tgtColl.CountDocuments(ctx, bson.D{})
	if err != nil {
		res.Err = fmt.Sprintf("count target: %v", err)
		return res
	}
	res.CountMatch = res.SourceCount == res.TargetCount

	if !opts.Hash {
		return res
	}
	res.HashChecked = true

	// Stream both sides into per-_id hash maps. We hold hashes (not documents)
	// keyed by the canonical target _id so converted _ids reconcile. This is
	// bounded memory relative to document size but O(docs) in count; for very
	// large collections prefer count-only or a sharded run.
	srcHashes, srcAcc, err := hashSource(ctx, source, dbName, cp.source, mapper, transformer, log)
	if err != nil {
		res.Err = fmt.Sprintf("hash source: %v", err)
		return res
	}
	tgtHashes, tgtAcc, err := hashTarget(ctx, tgtColl)
	if err != nil {
		res.Err = fmt.Sprintf("hash target: %v", err)
		return res
	}

	res.SourceHash = srcAcc.hex()
	res.TargetHash = tgtAcc.hex()
	res.HashMatch = res.SourceHash == res.TargetHash

	if !res.HashMatch {
		res.Mismatches, res.MismatchMore = diffHashes(srcHashes, tgtHashes, opts.MaxMismatch)
	}
	return res
}

// hashSource streams the source collection, applies the migrator's transform
// (and id-map _id resolution) to each document, and returns per-target-_id
// hashes plus the streaming XOR fingerprint.
func hashSource(ctx context.Context, source srcReader, dbName, collName string, mapper idmap.Store, transformer *migration.FieldTransformer, log *logger.Logger) (map[string]string, xorAcc, error) {
	var acc xorAcc
	hashes := make(map[string]string)

	err := source.streamDocs(ctx, collName, func(doc bson.M) error {
		origID := doc["_id"]
		targetID := origID
		if newID, ok := mapper.Lookup(collName, origID); ok {
			targetID = newID
			doc["_id"] = newID
		}
		transformed, err := transformer.Transform(doc, dbName, collName, origID)
		if err != nil {
			return fmt.Errorf("transform %s _id=%v: %w", collName, origID, err)
		}
		h, err := HashValue(transformed)
		if err != nil {
			return err
		}
		acc.add(h)
		hashes[idmap.CanonicalID(targetID)] = h
		return nil
	})
	if err != nil {
		return nil, acc, err
	}
	return hashes, acc, nil
}

// hashTarget streams the target collection and returns per-_id hashes plus the
// streaming XOR fingerprint. Target documents are already transformed, so they
// are hashed as-is.
func hashTarget(ctx context.Context, coll *mongo.Collection) (map[string]string, xorAcc, error) {
	var acc xorAcc
	hashes := make(map[string]string)

	cursor, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, acc, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, acc, err
		}
		h, err := HashValue(doc)
		if err != nil {
			return nil, acc, err
		}
		acc.add(h)
		hashes[idmap.CanonicalID(doc["_id"])] = h
	}
	return hashes, acc, cursor.Err()
}

// diffHashes compares two per-_id hash maps and returns up to max human-readable
// mismatch descriptions plus the count of additional mismatches elided.
func diffHashes(src, tgt map[string]string, max int) ([]string, int) {
	var msgs []string
	more := 0
	appendMsg := func(m string) {
		if len(msgs) < max {
			msgs = append(msgs, m)
		} else {
			more++
		}
	}

	// Deterministic ordering for stable output.
	srcKeys := sortedKeys(src)
	for _, k := range srcKeys {
		th, ok := tgt[k]
		if !ok {
			appendMsg(fmt.Sprintf("missing in target: _id=%s", k))
			continue
		}
		if th != src[k] {
			appendMsg(fmt.Sprintf("content differs: _id=%s", k))
		}
	}
	for _, k := range sortedKeys(tgt) {
		if _, ok := src[k]; !ok {
			appendMsg(fmt.Sprintf("extra in target: _id=%s", k))
		}
	}
	return msgs, more
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Format renders a human-readable verification report.
func (r *Report) Format() string {
	out := fmt.Sprintf("Verification: %d database pair(s), %d collection(s)\n", r.Pairs, len(r.Collections))
	okCount := 0
	for _, c := range r.Collections {
		status := "✅"
		if !c.OK() {
			status = "❌"
		} else {
			okCount++
		}
		loc := c.Database + "." + c.Source
		if c.Target != c.Source {
			loc += "→" + c.Target
		}
		out += fmt.Sprintf("  %s %s: source=%d target=%d", status, loc, c.SourceCount, c.TargetCount)
		if c.HashChecked {
			if c.HashMatch {
				out += " hash=match"
			} else {
				out += " hash=MISMATCH"
			}
		}
		out += "\n"
		if c.Err != "" {
			out += "      error: " + c.Err + "\n"
		}
		for _, m := range c.Mismatches {
			out += "      - " + m + "\n"
		}
		if c.MismatchMore > 0 {
			out += fmt.Sprintf("      … and %d more\n", c.MismatchMore)
		}
	}
	out += fmt.Sprintf("\n%d/%d collections verified.\n", okCount, len(r.Collections))
	if r.OK() {
		out += "✅ Verification passed.\n"
	} else {
		out += "❌ Verification found discrepancies.\n"
	}
	return out
}
