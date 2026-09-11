// Package assess implements pre-migration assessment (DESIGN §2): it translates
// each Firestore MongoDB-compatibility limit into a detection rule and grades
// findings as:
//
//	A — the migrator auto-fixes it (informational; e.g. reserved field names)
//	B — a hard block the operator must resolve first (e.g. document > 16 MiB)
//	C — silent degradation / warning (e.g. relying on _id ordering)
//
// The rule functions here are pure and operate on already-decoded documents /
// names, so they are unit-testable without a live server. The runner in
// assess.go samples collections and applies them.
package assess

import (
	"fmt"
	"regexp"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Severity grades a finding per DESIGN §2.
type Severity string

const (
	SeverityAutoFix Severity = "A"
	SeverityBlock   Severity = "B"
	SeverityWarn    Severity = "C"
)

// Firestore MongoDB-compat limits (DESIGN §2 table).
const (
	MaxDocBytes            = 16 * 1024 * 1024
	MaxDepth               = 20
	MaxFieldNameBytes      = 1500
	MaxFieldPathBytes      = 1500
	MaxValueBytes          = 4*1024*1024 - 89
	MaxCollNameBytes       = 1500
	MaxIDBytes             = 1500
	MaxIndexesPerDB        = 1000
	MaxDatabasesPerProject = 100
)

// reservedName matches the Firestore-reserved __.*__ pattern for field names,
// _id strings, and collection names.
var reservedName = regexp.MustCompile(`^__.*__$`)

// Finding is one assessment result, aggregated per (rule, collection). Detail is
// the representative first hit; Samples carries a few concrete offending records
// so the operator can drill in and decide per collection whether to fix.
type Finding struct {
	Severity   Severity `json:"severity"`
	Rule       string   `json:"rule"`
	Database   string   `json:"database,omitempty"`
	Collection string   `json:"collection,omitempty"`
	Detail     string   `json:"detail"`
	// DetailKey + DetailArgs carry the same information as Detail but as a stable
	// i18n key plus its interpolation arguments, so the web console can render the
	// detail in the operator's selected language. Detail itself stays English prose
	// for the CLI report and the in-document dedup key.
	DetailKey  string        `json:"detailKey,omitempty"`
	DetailArgs []interface{} `json:"detailArgs,omitempty"`
	Count      int           `json:"count,omitempty"`   // how many sampled docs hit this (rule, collection)
	Samples    []Sample      `json:"samples,omitempty"` // up to maxSamplesPerFinding example offenders
}

// Sample is one concrete offending record behind a Finding: which document, and
// what exactly is wrong (path + summarized value/size). It is display-only — the
// operator reviews it before deciding whether to click "应用修复".
type Sample struct {
	DocID  string `json:"docId,omitempty"`
	Detail string `json:"detail"`
	// DetailKey/DetailArgs localize this per-document detail; see Finding.
	DetailKey  string        `json:"detailKey,omitempty"`
	DetailArgs []interface{} `json:"detailArgs,omitempty"`
}

// detailFmt maps each stable i18n detail key to the English fmt template used for
// the plain-text Detail (CLI report + in-document dedup). The web console
// localizes via the same key + DetailArgs, so keep the two in lockstep: any new
// finding must add its key here and a matching entry in the console I18N tables.
var detailFmt = map[string]string{
	"detCollNameLength":    "collection name is %d bytes (limit %d)",
	"detCollNameReserved":  "collection name matches reserved __.*__ pattern",
	"detCollNameDollar":    "collection name contains '$'",
	"detCollNameSystem":    "collection name uses reserved 'system.' prefix",
	"detIdLength":          "string _id is %d bytes (limit %d)",
	"detIdReserved":        "string _id matches reserved __.*__ pattern",
	"detIdTypeConvert":     "_id type %s will be converted to string (recorded in id-map)",
	"detDocSize":           "document is %d bytes (limit %d)",
	"detNestingDepth":      "nesting exceeds %d levels at path %q",
	"detValueSizeStr":      "string value at %q is %d bytes (limit %d)",
	"detValueSizeBin":      "binary value at %q is %d bytes (limit %d)",
	"detFieldNameLength":   "field name at %q is %d bytes (limit %d)",
	"detFieldNameReserved": "field name %q at %q matches reserved __.*__ (rejected by Firestore)",
	"detFieldPathLength":   "field path %q is %d bytes (limit %d)",
	"detIndexBudgetB":      "database has %d indexes (limit %d)",
	"detIndexBudgetC":      "database has %d indexes, nearing limit %d",
	"detDatabaseBudget":    "migration needs %d Firestore databases (limit %d per project)",
}

// mkFinding builds a Finding, deriving the English Detail prose from detailFmt[key]
// and args while recording key+args for the console to localize. It is the single
// source of both representations so they never drift.
func mkFinding(sev Severity, rule, key string, args ...interface{}) Finding {
	return Finding{
		Severity:   sev,
		Rule:       rule,
		Detail:     fmt.Sprintf(detailFmt[key], args...),
		DetailKey:  key,
		DetailArgs: args,
	}
}

func (f Finding) String() string {
	loc := f.Database
	if f.Collection != "" {
		loc += "." + f.Collection
	}
	if loc != "" {
		loc = " [" + loc + "]"
	}
	return fmt.Sprintf("%s %s%s: %s", f.Severity, f.Rule, loc, f.Detail)
}

// CheckCollectionName validates a collection name against Firestore rules.
func CheckCollectionName(name string) []Finding {
	var out []Finding
	if len(name) > MaxCollNameBytes {
		f := mkFinding(SeverityBlock, "collection-name-length", "detCollNameLength", len(name), MaxCollNameBytes)
		f.Collection = name
		out = append(out, f)
	}
	if reservedName.MatchString(name) {
		f := mkFinding(SeverityBlock, "collection-name-reserved", "detCollNameReserved")
		f.Collection = name
		out = append(out, f)
	}
	if hasDollar(name) {
		f := mkFinding(SeverityBlock, "collection-name-dollar", "detCollNameDollar")
		f.Collection = name
		out = append(out, f)
	}
	if len(name) >= 7 && name[:7] == "system." {
		f := mkFinding(SeverityBlock, "collection-name-system", "detCollNameSystem")
		f.Collection = name
		out = append(out, f)
	}
	return out
}

func hasDollar(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '$' {
			return true
		}
	}
	return false
}

// IsValidIDType reports whether the migrator stores this _id type as-is (no
// lossy conversion). Any type for which this returns false is rewritten to a
// deterministic string by the engine. This is the SINGLE SOURCE OF TRUTH for
// _id-type validity: pkg/migration's transform delegates to it (isValidIDType),
// so the assessment predicts exactly what migration does. In particular int32
// is NOT valid here — the engine converts int32 _ids to string — so assessment
// must warn about them rather than call them acceptable.
func IsValidIDType(id interface{}) bool {
	switch id.(type) {
	case primitive.ObjectID, string, int64:
		return true
	default:
		return false
	}
}

// CheckID validates a document's _id against Firestore rules.
func CheckID(id interface{}) []Finding {
	var out []Finding
	// String _ids are stored as-is but are subject to Firestore's length and
	// reserved-name limits.
	if s, ok := id.(string); ok {
		if len(s) > MaxIDBytes {
			out = append(out, mkFinding(SeverityBlock, "id-length", "detIdLength", len(s), MaxIDBytes))
		}
		if reservedName.MatchString(s) {
			out = append(out, mkFinding(SeverityBlock, "id-reserved", "detIdReserved"))
		}
		return out
	}
	// Any type the engine does not store as-is is auto-converted to string
	// (lossy) — follow the engine's predicate so the prediction matches reality.
	if !IsValidIDType(id) {
		out = append(out, mkFinding(SeverityAutoFix, "id-type-convert", "detIdTypeConvert", fmt.Sprintf("%T", id)))
	}
	return out
}

// CheckDocument runs all document-level rules (size, depth, field names, paths,
// value sizes, _id) against a decoded document.
func CheckDocument(doc bson.M) []Finding {
	var out []Finding

	// Whole-document BSON size.
	if data, err := bson.Marshal(doc); err == nil {
		if len(data) > MaxDocBytes {
			out = append(out, mkFinding(SeverityBlock, "document-size", "detDocSize", len(data), MaxDocBytes))
		}
	}

	if id, ok := doc["_id"]; ok {
		out = append(out, CheckID(id)...)
	}

	w := &walker{}
	w.walk(doc, "", 1)
	out = append(out, w.findings...)
	return out
}

// walker recurses a document accumulating depth, field-name, path and value
// findings. It dedups repeated rule hits within a single document to keep
// reports readable.
type walker struct {
	findings []Finding
	seen     map[string]bool
}

func (w *walker) add(f Finding) {
	if w.seen == nil {
		w.seen = make(map[string]bool)
	}
	key := f.Rule + "|" + f.Detail
	if w.seen[key] {
		return
	}
	w.seen[key] = true
	w.findings = append(w.findings, f)
}

func (w *walker) walk(v interface{}, path string, depth int) {
	if depth > MaxDepth {
		w.add(mkFinding(SeverityBlock, "nesting-depth", "detNestingDepth", MaxDepth, path))
		return
	}
	switch val := v.(type) {
	case bson.M:
		for k, child := range val {
			w.checkKey(k, path)
			w.walk(child, joinPath(path, k), depth+1)
		}
	case bson.D:
		for _, e := range val {
			w.checkKey(e.Key, path)
			w.walk(e.Value, joinPath(path, e.Key), depth+1)
		}
	case map[string]interface{}:
		for k, child := range val {
			w.checkKey(k, path)
			w.walk(child, joinPath(path, k), depth+1)
		}
	case bson.A:
		for _, child := range val {
			w.walk(child, path, depth+1)
		}
	case []interface{}:
		for _, child := range val {
			w.walk(child, path, depth+1)
		}
	case string:
		if len(val) > MaxValueBytes {
			w.add(mkFinding(SeverityBlock, "value-size", "detValueSizeStr", path, len(val), MaxValueBytes))
		}
	case primitive.Binary:
		if len(val.Data) > MaxValueBytes {
			w.add(mkFinding(SeverityBlock, "value-size", "detValueSizeBin", path, len(val.Data), MaxValueBytes))
		}
	}
}

func (w *walker) checkKey(key, parentPath string) {
	if len(key) > MaxFieldNameBytes {
		w.add(mkFinding(SeverityBlock, "field-name-length", "detFieldNameLength", parentPath, len(key), MaxFieldNameBytes))
	}
	if reservedName.MatchString(key) {
		// Firestore rejects __x__ field names outright. Blocked until the operator
		// approves the sanitize-field-names remediation (whole-doc __x__→_x_); the
		// migrator then rewrites on the write path and re-assessment confirms clean.
		w.add(mkFinding(SeverityBlock, "field-name-reserved", "detFieldNameReserved", key, parentPath))
	}
	full := joinPath(parentPath, key)
	if len(full) > MaxFieldPathBytes {
		w.add(mkFinding(SeverityBlock, "field-path-length", "detFieldPathLength", truncate(full, 60), len(full), MaxFieldPathBytes))
	}
}

func joinPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n]) + "…"
}

// CheckIndexBudget flags a database whose total index count (including _id_
// indexes, and the extra _id index the migrator adds for Firestore) approaches
// or exceeds the per-database limit.
func CheckIndexBudget(database string, totalIndexes int) []Finding {
	if totalIndexes > MaxIndexesPerDB {
		f := mkFinding(SeverityBlock, "index-budget", "detIndexBudgetB", totalIndexes, MaxIndexesPerDB)
		f.Database = database
		return []Finding{f}
	}
	if totalIndexes > MaxIndexesPerDB*9/10 {
		f := mkFinding(SeverityWarn, "index-budget", "detIndexBudgetC", totalIndexes, MaxIndexesPerDB)
		f.Database = database
		return []Finding{f}
	}
	return nil
}

// CheckDatabaseBudget flags exceeding the per-project database count.
func CheckDatabaseBudget(count int) []Finding {
	if count > MaxDatabasesPerProject {
		return []Finding{mkFinding(SeverityBlock, "database-budget", "detDatabaseBudget", count, MaxDatabasesPerProject)}
	}
	return nil
}
