package assess

import (
	"context"
	"fmt"
	"sort"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/remediate"
	"go.mongodb.org/mongo-driver/bson"
)

// SampleConfig controls how many documents each collection contributes to the
// assessment. Sampling is per-collection: a collection of size total contributes
// clamp(Floor, Cap, max(Floor, total*Rate)) documents (never more than its own
// size). A pure per-collection floor (the old fixed 1000) gave million-document
// collections a <0.1% sampling rate, so systemic-but-sparse issues were easily
// missed; the proportional Rate keeps large collections covered while Cap keeps
// the cost bounded.
type SampleConfig struct {
	Floor int     `json:"floor"` // minimum docs sampled per collection
	Rate  float64 `json:"rate"`  // fraction of a collection to sample (e.g. 0.001 = 0.1%)
	Cap   int     `json:"cap"`   // hard upper bound per collection (cost control)
}

// DefaultSampleConfig is applied when the caller supplies an empty config.
func DefaultSampleConfig() SampleConfig {
	return SampleConfig{Floor: 1000, Rate: 0.001, Cap: 50000}
}

// isZero reports whether the config carries no meaningful values (so the default
// should be substituted).
func (sc SampleConfig) isZero() bool {
	return sc.Floor <= 0 && sc.Rate <= 0 && sc.Cap <= 0
}

// target returns how many documents to sample from a collection of size total.
func (sc SampleConfig) target(total int64) int64 {
	floor := int64(sc.Floor)
	if floor < 1 {
		floor = 1
	}
	want := floor
	if sc.Rate > 0 {
		if byRate := int64(float64(total) * sc.Rate); byRate > want {
			want = byRate
		}
	}
	if sc.Cap > 0 && want > int64(sc.Cap) {
		want = int64(sc.Cap)
	}
	if want > total {
		want = total
	}
	return want
}

// Report aggregates all findings from an assessment run.
type Report struct {
	Findings      []Finding        `json:"findings"`
	Counts        map[Severity]int `json:"counts"`
	Databases     int              `json:"databases"`
	Collections   int              `json:"collections"`
	DocsInspected int64            `json:"docsInspected"`
	// TotalDocs is the sum of CountDocuments across every assessed collection
	// (the true totals, not just the sampled subset). It drives the tuning
	// recommendation (worker counts, parallel reads).
	TotalDocs int64 `json:"totalDocs"`
	// LargestCollectionDocs is the biggest single-collection document count seen;
	// a large value argues for enabling parallel reads.
	LargestCollectionDocs int64 `json:"largestCollectionDocs"`
	// Sample records the sampling plan actually used, so the UI can explain the
	// effective sampling rate (DocsInspected / TotalDocs).
	Sample SampleConfig `json:"sample"`
	// Remediations is the active fix plan that was applied (in-memory) before the
	// rules were re-run. Findings cleared by a remediation are absent from
	// Findings; the plan is echoed here so the UI can show them as ✅ resolved.
	Remediations []remediate.Remediation `json:"remediations"`
	// Indexes lists, per target database, the total index count that will exist
	// on Firestore after migration. Unlike documents, indexes are NEVER sampled:
	// every collection's indexes are fully enumerated, because Firestore enforces
	// a hard MaxIndexesPerDB limit and even a single collection over budget blocks
	// the whole database. Each entry already includes the extra _id index the
	// migrator creates per collection on Firestore targets.
	Indexes []IndexStat `json:"indexes"`
	// CollectionTuning lists per-collection partition-knob recommendations for
	// byte-heavy/low-count "straggler" collections — those the global settings
	// would load on a single cursor. Empty when no collection warrants an override.
	CollectionTuning []CollectionTuningRec `json:"collectionTuning,omitempty"`
}

// IndexStat is the per-target-database index accounting used to check the
// Firestore per-database index budget (MaxIndexesPerDB).
type IndexStat struct {
	Database    string `json:"database"`    // target database name
	Collections int    `json:"collections"` // collections counted
	Source      int    `json:"source"`      // ALL indexes present on the source (INCLUDES each collection's _id_)
	Secondary   int    `json:"secondary"`   // source non-_id indexes = Source − Collections
	IDIndexes   int    `json:"idIndexes"`   // _id indexes the migrator builds on Firestore (one per collection)
	Total       int    `json:"total"`       // what the target will hold: Secondary + IDIndexes on Firestore, else Source
	Limit       int    `json:"limit"`       // Firestore per-database index limit
}

// Blocking reports whether any hard-block (B) findings were recorded.
func (r *Report) Blocking() bool { return r.Counts[SeverityBlock] > 0 }

// add appends findings and updates counts, deduping identical entries.
func (r *Report) add(fs ...Finding) {
	if r.Counts == nil {
		r.Counts = make(map[Severity]int)
	}
	for _, f := range fs {
		r.Findings = append(r.Findings, f)
		r.Counts[f.Severity]++
	}
}

// Progress reports assessment advancement. Raised sample rates can push a run to
// several minutes with no output, making it look frozen; a ProgressFunc lets the
// caller surface liveness (a CLI heartbeat line, an SSE stream to the browser).
// It fires once per collection, just before that collection is sampled.
type Progress struct {
	Phase      string `json:"phase"`      // currently always "sampling"
	Database   string `json:"database"`   // source database being assessed
	Collection string `json:"collection"` // collection about to be sampled
	Done       int    `json:"done"`       // collections finished so far (across all pairs)
	Total      int    `json:"total"`      // total collections to assess (across all pairs)
	SampleDocs int64  `json:"sampleDocs"` // docs about to be sampled from this collection
	TotalDocs  int64  `json:"totalDocs"`  // total docs in this collection
}

// ProgressFunc receives Progress events. It is always invoked synchronously from
// Run's goroutine, so a callback that writes to a single response stream needs no
// locking. A nil ProgressFunc disables reporting.
type ProgressFunc func(Progress)

// pairWork holds an opened source reader and its enumerated collection list, so
// the grand total (needed for "X/N" progress) is known before any sampling runs.
type pairWork struct {
	pair        config.DatabasePair
	source      sourceReader
	collections []string
}

// Run assesses every database pair's source and returns a consolidated report.
// plan is the active remediation plan (may be nil); its transforms are applied
// in-memory to sampled documents and collection names before the rules run, so a
// clean re-assessment is equivalent to clean post-migration data (the source is
// never mutated).
func Run(ctx context.Context, cfg *config.Config, sample SampleConfig, plan *remediate.Plan, log *logger.Logger) (*Report, error) {
	return RunWithProgress(ctx, cfg, sample, plan, nil, log)
}

// RunWithProgress is Run with per-collection progress reporting. See Progress.
func RunWithProgress(ctx context.Context, cfg *config.Config, sample SampleConfig, plan *remediate.Plan, onProgress ProgressFunc, log *logger.Logger) (*Report, error) {
	if sample.isZero() {
		sample = DefaultSampleConfig()
	}
	report := &Report{Counts: make(map[Severity]int), Sample: sample}
	if plan != nil {
		report.Remediations = plan.Items
	}

	// Per-project database budget: distinct target databases.
	targetDBs := make(map[string]bool)
	for _, pair := range cfg.DatabasePairs {
		targetDBs[pair.Target.Database] = true
	}
	report.add(CheckDatabaseBudget(len(targetDBs))...)

	// Phase 1: open each source and enumerate its collections up front so the
	// grand total is known before sampling begins (progress needs "X of N").
	// Metadata-only ops (connect + listCollections) are cheap next to sampling;
	// readers stay open for phase 2 and are closed here.
	var works []*pairWork
	defer func() {
		for _, w := range works {
			if w.source != nil {
				w.source.close(ctx)
			}
		}
	}()
	totalCollections := 0
	for _, pair := range cfg.DatabasePairs {
		// Read-only assessment. Pre-3.6 sources (oplog-legacy) must use the mgo
		// driver — the modern Go driver rejects their wire version.
		source, err := newSourceReader(pair.Source.ConnectionString, pair.Source.Database, pair.Source.ReplicationMethod, log)
		if err != nil {
			log.Warnf("Assessment of %s: connect source: %v", pair.Source.Database, err)
			continue
		}
		collections, err := collectionsToAssess(ctx, source, pair)
		if err != nil {
			log.Warnf("Assessment of %s: list collections: %v", pair.Source.Database, err)
			source.close(ctx)
			continue
		}
		report.Databases++
		works = append(works, &pairWork{pair: pair, source: source, collections: collections})
		totalCollections += len(collections)
	}

	// Phase 2: sample and assess, reporting progress as each collection starts.
	done := 0
	for _, w := range works {
		if err := assessPairWork(ctx, w, report, sample, plan, onProgress, &done, totalCollections, log); err != nil {
			log.Warnf("Assessment of %s encountered an error: %v", w.pair.Source.Database, err)
		}
	}

	sortFindings(report.Findings)
	return report, nil
}

func assessPairWork(ctx context.Context, w *pairWork, report *Report, sample SampleConfig, plan *remediate.Plan, onProgress ProgressFunc, done *int, totalCollections int, log *logger.Logger) error {
	pair, source, collections := w.pair, w.source, w.collections

	var totalIndexes int
	for _, collName := range collections {
		report.Collections++
		// Check the (possibly remediation-renamed) target name, so a
		// rename-collection fix clears the reserved-name finding. Tag each finding
		// with the SOURCE db/collection so its "应用修复" button registers a
		// remediation keyed the way forCollection matches at migration time.
		nameFindings := CheckCollectionName(plan.CollectionName(pair.Source.Database, collName))
		for i := range nameFindings {
			nameFindings[i].Database = pair.Source.Database
			nameFindings[i].Collection = collName
		}
		report.add(nameFindings...)

		if n, err := source.indexCount(ctx, collName); err == nil {
			totalIndexes += n
		} else {
			log.Debugf("could not list indexes for %s: %v", collName, err)
		}

		// Count first so the progress event can show the sampling scope
		// (want / total) before the potentially slow $sample runs.
		total, err := source.countDocs(ctx, collName)
		if err != nil {
			log.Warnf("could not count %s: %v", collName, err)
			*done++
			continue
		}
		if onProgress != nil {
			onProgress(Progress{
				Phase: "sampling", Database: pair.Source.Database, Collection: collName,
				Done: *done, Total: totalCollections,
				SampleDocs: sample.target(total), TotalDocs: total,
			})
		}

		inspected, avgDocBytes, findings, err := assessCollectionDocs(ctx, source, pair.Source.Database, collName, total, sample, plan)
		*done++
		if err != nil {
			log.Warnf("could not sample %s: %v", collName, err)
			continue
		}
		report.DocsInspected += inspected
		report.TotalDocs += total
		if total > report.LargestCollectionDocs {
			report.LargestCollectionDocs = total
		}
		// Flag byte-heavy/low-count stragglers with a per-collection override that
		// forces them to partition (they would otherwise load single-cursor).
		if rec, ok := RecommendCollectionTuning(pair.Source.Database, collName, total, avgDocBytes); ok {
			report.CollectionTuning = append(report.CollectionTuning, rec)
		}
		// Tag findings with location.
		for i := range findings {
			findings[i].Database = pair.Source.Database
			if findings[i].Collection == "" {
				findings[i].Collection = collName
			}
		}
		report.add(findings...)
	}

	// Index counting is a FULL enumeration (never sampled): the Firestore
	// per-database index budget is a hard limit, so every collection must be
	// counted exactly.
	//
	// totalIndexes is the raw source count and ALREADY includes one _id_ per
	// collection (source enumeration returns _id_). On Firestore the migrator
	// rebuilds exactly one _id per collection, so the target holds the same _id
	// count as the source — we must NOT add len(collections) on top of a number
	// that already contains them (that was a double-count that could falsely trip
	// the budget). Break it out as: secondary (non-_id source indexes) + one _id
	// per collection.
	secondary := totalIndexes - len(collections)
	if secondary < 0 {
		secondary = 0 // defensive: every collection normally has _id_
	}
	stat := IndexStat{
		Database:    pair.Target.Database,
		Collections: len(collections),
		Source:      totalIndexes,
		Secondary:   secondary,
		Limit:       MaxIndexesPerDB,
	}
	if isFirestore(pair) {
		stat.IDIndexes = len(collections)
		stat.Total = secondary + len(collections)
	} else {
		// Real MongoDB auto-creates _id; the target keeps the source's _id_.
		stat.Total = totalIndexes
	}
	report.Indexes = append(report.Indexes, stat)
	report.add(CheckIndexBudget(pair.Source.Database, stat.Total)...)
	return nil
}

func isFirestore(pair config.DatabasePair) bool {
	return containsFirestoreHost(pair.Target.ConnectionString)
}

func containsFirestoreHost(conn string) bool {
	for i := 0; i+13 <= len(conn); i++ {
		if conn[i:i+13] == "firestore.goo" {
			return true
		}
	}
	return false
}

func collectionsToAssess(ctx context.Context, source sourceReader, pair config.DatabasePair) ([]string, error) {
	// Explicit collection list wins; otherwise list them all.
	if len(pair.Target.Collections) > 0 {
		var names []string
		for _, c := range pair.Target.Collections {
			names = append(names, c.SourceCollection)
		}
		return names, nil
	}
	return source.listCollections(ctx)
}

// assessCollectionDocs samples documents and applies document-level rules,
// deduping identical findings across the sample so one systemic issue reports
// once rather than thousands of times. total is the collection's document count,
// already fetched by the caller (so the progress event can report scope first).
func assessCollectionDocs(ctx context.Context, source sourceReader, db, collName string, total int64, sample SampleConfig, plan *remediate.Plan) (inspected int64, avgDocBytes int64, findings []Finding, err error) {
	want := sample.target(total)
	docs, err := source.sample(ctx, collName, total, want)
	if err != nil {
		return 0, 0, nil, err
	}

	var sampledBytes int64 // sum of marshaled sizes, to estimate mean doc size

	// Aggregate to one finding per (rule, severity), but keep a running count and a
	// few concrete sample offenders so the operator can expand a row and see the
	// actual problematic documents before deciding whether to fix.
	byRule := make(map[string]*Finding)
	var order []string
	for _, doc := range docs {
		inspected++
		// Measure the source document size (before any remediation is applied) so
		// the per-collection straggler recommendation reflects the bytes actually
		// read from the source at load time.
		if data, mErr := bson.Marshal(doc); mErr == nil {
			sampledBytes += int64(len(data))
		}
		// Apply the remediation plan in-memory (source is never mutated) so a
		// re-assessment reflects post-migration data: findings cleared by a fix
		// simply stop being produced.
		doc = plan.ApplyDoc(db, collName, doc)
		id := stringifyID(doc["_id"])
		for _, f := range CheckDocument(doc) {
			key := f.Rule + "|" + f.Severity.string()
			agg, ok := byRule[key]
			if !ok {
				fc := f
				fc.Count = 0
				fc.Samples = nil
				byRule[key] = &fc
				order = append(order, key)
				agg = &fc
			}
			agg.Count++
			if len(agg.Samples) < maxSamplesPerFinding {
				agg.Samples = append(agg.Samples, Sample{DocID: id, Detail: f.Detail, DetailKey: f.DetailKey, DetailArgs: f.DetailArgs})
			}
		}
	}
	for _, k := range order {
		findings = append(findings, *byRule[k])
	}
	if inspected > 0 {
		avgDocBytes = sampledBytes / inspected
	}
	return inspected, avgDocBytes, findings, nil
}

// maxSamplesPerFinding bounds how many concrete offenders are kept per aggregated
// finding — enough for the operator to eyeball the shape of the problem without
// bloating the report when a whole collection violates the same rule.
const maxSamplesPerFinding = 5

// stringifyID renders a document _id for display in a sample. It never returns a
// huge string (an over-long _id is itself summarized) so the report stays small.
func stringifyID(id interface{}) string {
	if id == nil {
		return ""
	}
	s := fmt.Sprintf("%v", id)
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return s
}

func (s Severity) string() string { return string(s) }

// sortFindings orders findings by severity (B, then A, then C) for readable output.
func sortFindings(f []Finding) {
	order := map[Severity]int{SeverityBlock: 0, SeverityAutoFix: 1, SeverityWarn: 2}
	sort.SliceStable(f, func(i, j int) bool {
		return order[f[i].Severity] < order[f[j].Severity]
	})
}

// Format renders a human-readable summary of the report.
func (r *Report) Format() string {
	out := fmt.Sprintf("Assessment: %d databases, %d collections, %d documents inspected\n",
		r.Databases, r.Collections, r.DocsInspected)
	rate := 0.0
	if r.TotalDocs > 0 {
		rate = float64(r.DocsInspected) / float64(r.TotalDocs) * 100
	}
	out += fmt.Sprintf("  Sampling: floor=%d rate=%.3g%% cap=%d → inspected %d/%d docs (%.3g%% of total)\n",
		r.Sample.Floor, r.Sample.Rate*100, r.Sample.Cap, r.DocsInspected, r.TotalDocs, rate)
	for _, ix := range r.Indexes {
		out += fmt.Sprintf("  Indexes [%s]: %d secondary + %d _id = %d / %d (full scan, not sampled) — %d headroom\n",
			ix.Database, ix.Secondary, ix.IDIndexes, ix.Total, ix.Limit, ix.Limit-ix.Total)
	}
	out += fmt.Sprintf("  B (blocking): %d   A (auto-fixed): %d   C (warnings): %d\n",
		r.Counts[SeverityBlock], r.Counts[SeverityAutoFix], r.Counts[SeverityWarn])
	if len(r.Findings) == 0 {
		out += "  ✅ No issues found.\n"
		return out
	}
	for _, f := range r.Findings {
		out += "  " + f.String() + "\n"
	}
	if r.Blocking() {
		out += "\n❌ Blocking issues (B) must be resolved before migrating.\n"
	} else {
		out += "\n✅ No blocking issues. Auto-fixed (A) and warnings (C) are safe to proceed.\n"
	}
	return out
}
