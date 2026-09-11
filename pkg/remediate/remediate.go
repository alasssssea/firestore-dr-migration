// Package remediate turns pre-migration assessment findings (pkg/assess) into
// concrete, deterministic transforms that make a document / collection name
// Firestore-compatible. The SAME transforms are used in two places so that a
// "re-assessment passes" result is equivalent to "the migrated data is clean":
//
//   - pkg/assess applies a Plan in-memory to sampled documents before re-running
//     its rules, so the console can show a finding as ✅ resolved WITHOUT touching
//     the source database (the user chose "transform at migration time, never
//     mutate the source").
//   - the migrator applies the same Plan on the write path, so the data actually
//     landing in Firestore matches what the re-assessment simulated.
//
// Every transform is deterministic (same input → same output), so simulation and
// real migration always agree, and re-clicking a fix is idempotent.
package remediate

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

	"go.mongodb.org/mongo-driver/bson"
)

// Firestore limits — mirror of the constants in pkg/assess/rules.go. They are
// duplicated (not imported) to keep the dependency arrow assess → remediate,
// never the reverse. Keep the two in sync.
const (
	MaxDepth          = 20
	MaxFieldNameBytes = 1500
	MaxFieldPathBytes = 1500
	MaxValueBytes     = 4*1024*1024 - 89
	// chunkSize keeps each chunk-value part comfortably under MaxValueBytes.
	chunkSize = 3 * 1024 * 1024
)

var reservedName = regexp.MustCompile(`^__.*__$`)

// Strategy identifies one transform kind.
type Strategy string

const (
	StrategyRenameCollection   Strategy = "rename-collection"    // __x__ → _x_ (collection name)
	StrategyRewriteID          Strategy = "rewrite-id"           // reserved string _id → _x_
	StrategySerializeSubtree   Strategy = "serialize-subtree"    // over-deep / over-long-path subtree → JSON string
	StrategyRenameField        Strategy = "rename-field"         // over-long field name → truncate+hash
	StrategyChunkValue         Strategy = "chunk-value"          // over-size value → {_chunked_, parts:[…]}
	StrategySanitizeFieldNames Strategy = "sanitize-field-names" // reserved __x__ field names → _x_ (recursive)
)

// WildcardCollection, used as a Remediation.Collection, applies the fix to EVERY
// collection in the database — the answer to "整个库的字段都是 __xx__ 模式". One
// registration covers the whole database instead of clicking per collection.
const WildcardCollection = "*"

// DefaultStrategy maps an assess rule name to the transform that fixes it, plus a
// human-readable note describing the workaround and its trade-off (shown on the
// UI "apply fix" button) and a stable i18n key for that note so the web console
// can render it in the operator's language. Returns ok=false for rules that
// cannot be auto-fixed.
func DefaultStrategy(rule string) (strat Strategy, note, noteKey string, ok bool) {
	switch rule {
	case "collection-name-reserved":
		return StrategyRenameCollection, "Rename target collection __x__→_x_ (recorded in name-map; app must use the new name)", "noteRenameCollection", true
	case "id-reserved":
		return StrategyRewriteID, "Rewrite reserved _id __x__→_x_ (recorded in id-map; cross-collection references must follow the map)", "noteRewriteID", true
	case "nesting-depth":
		return StrategySerializeSubtree, "Serialize the subtree below level 20 to a JSON string (data preserved; that subtree loses queryability)", "noteSerializeNesting", true
	case "field-path-length":
		return StrategySerializeSubtree, "Serialize the subtree at the over-long path to a JSON string (data preserved; that subtree loses queryability)", "noteSerializePath", true
	case "field-name-length":
		return StrategyRenameField, "Truncate + hash-rename the over-long field name (recorded in name-map; app must use the new name)", "noteRenameField", true
	case "field-name-reserved":
		return StrategySanitizeFieldNames, "Rewrite reserved field names __x__→_x_ (recursively across the document; recorded in name-map; app must use the new name)", "noteSanitizeFieldNames", true
	case "value-size":
		return StrategyChunkValue, "Split the over-large value into {_chunked_,parts:[…]} (app must reassemble in order)", "noteChunkValue", true
	default:
		return "", "", "", false
	}
}

// Remediation is one registered fix, keyed by (Rule, Database, Collection).
type Remediation struct {
	Rule       string   `json:"rule"`
	Database   string   `json:"database"`
	Collection string   `json:"collection"`
	Strategy   Strategy `json:"strategy"`
	Note       string   `json:"note"`
	// NoteKey is the stable i18n key for Note; the console localizes via it and
	// falls back to Note (English) when absent.
	NoteKey string `json:"noteKey,omitempty"`
}

func (r Remediation) key() string { return r.Rule + "|" + r.Database + "|" + r.Collection }

// Plan is an ordered, de-duplicated set of remediations.
type Plan struct {
	Items []Remediation `json:"items"`
	// Source is a fingerprint of the source the plan was built against
	// (host:port + database set, hashed — never credentials). The console uses
	// it to auto-invalidate a plan when the operator points at a different
	// source, so remediations for one source's collections never silently apply
	// to a different source that happens to share database/collection names.
	// Empty on plans written before this field existed (treated as "unknown
	// source" → invalidated on the next detect).
	Source string `json:"source,omitempty"`
}

// Add registers a remediation (idempotent by key). Fills Strategy/Note from the
// rule default when the caller left them blank.
func (p *Plan) Add(r Remediation) {
	if r.Strategy == "" {
		if s, note, noteKey, ok := DefaultStrategy(r.Rule); ok {
			r.Strategy, r.Note, r.NoteKey = s, note, noteKey
		}
	}
	for i := range p.Items {
		if p.Items[i].key() == r.key() {
			p.Items[i] = r
			return
		}
	}
	p.Items = append(p.Items, r)
}

// Remove drops a remediation by (rule, db, coll). Returns whether one was removed.
func (p *Plan) Remove(rule, db, coll string) bool {
	want := Remediation{Rule: rule, Database: db, Collection: coll}.key()
	for i := range p.Items {
		if p.Items[i].key() == want {
			p.Items = append(p.Items[:i], p.Items[i+1:]...)
			return true
		}
	}
	return false
}

// forCollection returns the remediations that target a given collection.
func (p *Plan) forCollection(db, coll string) []Remediation {
	if p == nil {
		return nil
	}
	var out []Remediation
	for _, r := range p.Items {
		if r.Database == db && (r.Collection == coll || r.Collection == WildcardCollection) {
			out = append(out, r)
		}
	}
	return out
}

// HasCollection reports whether any remediation targets (db, coll). Callers use
// it to skip the (order-losing) document conversion for collections with no fix.
// Nil-safe.
func (p *Plan) HasCollection(db, coll string) bool {
	return len(p.forCollection(db, coll)) > 0
}

// CollectionName returns the (possibly rewritten) target collection name given
// the active plan. Callers use this both to write to the correct Firestore
// collection and to re-check the sanitized name against the naming rules.
func (p *Plan) CollectionName(db, coll string) string {
	for _, r := range p.forCollection(db, coll) {
		if r.Strategy == StrategyRenameCollection {
			return sanitizeReserved(coll)
		}
	}
	return coll
}

// SanitizedTargetName returns target rewritten to a legal name when a
// rename-collection remediation is registered for (db, sourceColl); otherwise it
// returns target unchanged. sanitizeReserved is a no-op on already-legal names,
// so this is safe to call unconditionally on any target name. Nil-safe.
func (p *Plan) SanitizedTargetName(db, sourceColl, target string) string {
	for _, r := range p.forCollection(db, sourceColl) {
		if r.Strategy == StrategyRenameCollection {
			return sanitizeReserved(target)
		}
	}
	return target
}

// Change records one concrete transformation applied to a document, so the
// migrator can write an audit trail ("修复了什么") for later forensic review.
type Change struct {
	Rule      string   `json:"rule"`             // assess rule the fix targets
	Strategy  Strategy `json:"strategy"`         // transform kind that ran
	Path      string   `json:"path"`             // field path affected ("_id", "blob", "data.level.level…")
	Violation string   `json:"violation"`        // 报错: which Firestore limit the original data broke
	Before    string   `json:"before"`           // 原数据: the offending value/name (summarized if large)
	After     string   `json:"after"`            // 修完: what it became (summarized if large)
	Detail    string   `json:"detail,omitempty"` // human-readable one-liner (legacy/back-compat)
}

// ApplyDoc returns a transformed copy of doc with every strategy registered for
// (db, coll) applied. doc is not mutated. Safe to call with a nil/empty plan
// (returns doc unchanged).
func (p *Plan) ApplyDoc(db, coll string, doc bson.M) bson.M {
	out, _ := p.ApplyDocAudit(db, coll, doc)
	return out
}

// ApplyDocAudit is ApplyDoc plus the list of concrete changes it made (empty when
// the plan touched nothing). The migrator uses the changes to write an audit log;
// assess ignores them. doc is never mutated.
func (p *Plan) ApplyDocAudit(db, coll string, doc bson.M) (bson.M, []Change) {
	rems := p.forCollection(db, coll)
	if len(rems) == 0 {
		return doc, nil
	}
	out := cloneM(doc)
	var changes []Change
	for _, r := range rems {
		switch r.Strategy {
		case StrategyRewriteID:
			if id, ok := out["_id"].(string); ok && reservedName.MatchString(id) {
				nid := sanitizeReserved(id)
				out["_id"] = nid
				changes = append(changes, Change{
					Rule: r.Rule, Strategy: r.Strategy, Path: "_id",
					Violation: fmt.Sprintf("_id %q matches reserved __x__ (Firestore rejects)", id),
					Before:    id, After: nid,
					Detail: fmt.Sprintf("%q → %q", id, nid)})
			}
		case StrategySerializeSubtree:
			out = serializeMap(out, 1, 0, "", r.Rule, &changes)
		case StrategyRenameField:
			out = renameLongFields(out, "", r.Rule, &changes)
		case StrategyChunkValue:
			out = chunkLargeValues(out, "", r.Rule, &changes)
		case StrategySanitizeFieldNames:
			out = sanitizeFieldNames(out, "", r.Rule, &changes)
		}
	}
	return out, changes
}

/* ---------- transforms ---------- */

// sanitizeReserved rewrites a __x__ name to _x_, repeating until it no longer
// matches the reserved pattern (handles ___x___ etc.).
func sanitizeReserved(s string) string {
	for reservedName.MatchString(s) && len(s) >= 2 {
		s = "_" + s[2:len(s)-2] + "_"
	}
	return s
}

// serializeMap recurses a map that itself sits at depth d and whose path length
// is pathLen, replacing any subtree that would push a node past the depth limit
// or a field path past the byte limit with its JSON-string form. The result is
// Firestore-legal by construction: it mirrors assess's walker, which flags ANY
// node reached at depth > MaxDepth and ANY key whose full path > MaxFieldPathBytes.
// Root document map = depth 1, pathLen 0.
func serializeMap(m bson.M, d, pathLen int, path, rule string, acc *[]Change) bson.M {
	out := make(bson.M, len(m))
	for k, v := range m {
		pl := pathLen + len(k)
		if pathLen > 0 {
			pl++ // dot separator
		}
		cp := k
		if path != "" {
			cp = path + "." + k
		}
		out[k] = serializeValue(v, d+1, pl, cp, rule, acc) // the value sits one level deeper
	}
	return out
}

// serializeValue transforms a value that sits at depth d with field-path length
// pathLen. A container is collapsed to a JSON string (which then occupies the
// legal depth/path of its own key) when recursing further would breach a limit.
// path is the field path of this value; each collapse is appended to acc.
func serializeValue(v interface{}, d, pathLen int, path, rule string, acc *[]Change) interface{} {
	record := func(orig interface{}, s, violation string) {
		if acc != nil {
			*acc = append(*acc, Change{
				Rule: rule, Strategy: StrategySerializeSubtree, Path: path,
				Violation: violation,
				Before:    summarize(orig), After: summarizeString(s),
				Detail: fmt.Sprintf("subtree serialized to JSON string (%d bytes)", len(s))})
		}
	}
	switch val := v.(type) {
	case bson.M:
		if reason, hit := collapseMap(val, d, pathLen); hit {
			s := jsonify(val)
			record(val, s, reason)
			return s
		}
		return serializeMap(val, d, pathLen, path, rule, acc)
	case map[string]interface{}:
		if reason, hit := collapseMap(bson.M(val), d, pathLen); hit {
			s := jsonify(val)
			record(val, s, reason)
			return s
		}
		return serializeMap(bson.M(val), d, pathLen, path, rule, acc)
	case bson.A:
		if d >= MaxDepth {
			s := jsonify(val)
			record(val, s, fmt.Sprintf("array at depth %d exceeds MaxDepth %d", d, MaxDepth))
			return s
		}
		out := make(bson.A, len(val))
		for i, e := range val {
			out[i] = serializeValue(e, d+1, pathLen, path, rule, acc) // array elements sit one level deeper
		}
		return out
	case []interface{}:
		if d >= MaxDepth {
			s := jsonify(val)
			record(val, s, fmt.Sprintf("array at depth %d exceeds MaxDepth %d", d, MaxDepth))
			return s
		}
		out := make([]interface{}, len(val))
		for i, e := range val {
			out[i] = serializeValue(e, d+1, pathLen, path, rule, acc)
		}
		return out
	default:
		return v
	}
}

// collapseMap reports whether a map at depth d / path pathLen must be flattened
// to a string: either its children would sit past the depth limit, or one of its
// keys would produce an over-long field path.
func collapseMap(m bson.M, d, pathLen int) (reason string, hit bool) {
	if d >= MaxDepth { // children would be at depth d+1 > MaxDepth
		return fmt.Sprintf("nesting depth %d would exceed MaxDepth %d", d+1, MaxDepth), true
	}
	for k := range m {
		pl := pathLen + len(k)
		if pathLen > 0 {
			pl++
		}
		if pl > MaxFieldPathBytes {
			return fmt.Sprintf("field path %d bytes (via %q) would exceed MaxFieldPathBytes %d", pl, k, MaxFieldPathBytes), true
		}
	}
	return "", false
}

// renameLongFields renames any field whose name exceeds the limit to a
// deterministic truncate+hash form, recursively.
func renameLongFields(m bson.M, path, rule string, acc *[]Change) bson.M {
	out := make(bson.M, len(m))
	for k, v := range m {
		nk := k
		cp := k
		if path != "" {
			cp = path + "." + k
		}
		if len(k) > MaxFieldNameBytes {
			nk = shortenName(k)
			if acc != nil {
				*acc = append(*acc, Change{
					Rule: rule, Strategy: StrategyRenameField, Path: cp,
					Violation: fmt.Sprintf("field name %d bytes exceeds MaxFieldNameBytes %d", len(k), MaxFieldNameBytes),
					Before:    summarizeString(k), After: nk,
					Detail: fmt.Sprintf("field name %d bytes → %q", len(k), nk)})
			}
		}
		ncp := nk
		if path != "" {
			ncp = path + "." + nk
		}
		switch val := v.(type) {
		case bson.M:
			out[nk] = renameLongFields(val, ncp, rule, acc)
		case map[string]interface{}:
			out[nk] = renameLongFields(bson.M(val), ncp, rule, acc)
		default:
			out[nk] = v
		}
	}
	return out
}

// shortenName truncates an over-long field name and appends a hash of the
// original so distinct long names never collide. Result stays under the limit
// and does not match the reserved pattern.
func shortenName(k string) string {
	sum := sha1.Sum([]byte(k))
	suffix := "__h" + hex.EncodeToString(sum[:])[:8]
	keep := MaxFieldNameBytes - len(suffix)
	if keep < 0 {
		keep = 0
	}
	if keep > len(k) {
		keep = len(k)
	}
	return k[:keep] + suffix
}

// chunkLargeValues replaces any oversized string/binary value with a chunk
// marker document, recursively.
func chunkLargeValues(m bson.M, path, rule string, acc *[]Change) bson.M {
	out := make(bson.M, len(m))
	for k, v := range m {
		cp := k
		if path != "" {
			cp = path + "." + k
		}
		out[k] = chunkValue(v, cp, rule, acc)
	}
	return out
}

func chunkValue(v interface{}, path, rule string, acc *[]Change) interface{} {
	switch val := v.(type) {
	case string:
		if len(val) > MaxValueBytes {
			m := chunkString(val)
			if acc != nil {
				parts, _ := m["parts"].(bson.A)
				*acc = append(*acc, Change{
					Rule: rule, Strategy: StrategyChunkValue, Path: path,
					Violation: fmt.Sprintf("value %d bytes exceeds MaxValueBytes %d", len(val), MaxValueBytes),
					Before:    summarizeString(val),
					After:     fmt.Sprintf("{_chunked_:true, len:%d, parts:%d}", len(val), len(parts)),
					Detail:    fmt.Sprintf("%d bytes → %d chunk(s)", len(val), len(parts))})
			}
			return m
		}
		return val
	case bson.M:
		return chunkLargeValues(val, path, rule, acc)
	case map[string]interface{}:
		return chunkLargeValues(bson.M(val), path, rule, acc)
	case bson.A:
		out := make(bson.A, len(val))
		for i, e := range val {
			out[i] = chunkValue(e, path, rule, acc)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, e := range val {
			out[i] = chunkValue(e, path, rule, acc)
		}
		return out
	default:
		return v
	}
}

func chunkString(s string) bson.M {
	var parts bson.A
	for i := 0; i < len(s); i += chunkSize {
		end := i + chunkSize
		if end > len(s) {
			end = len(s)
		}
		parts = append(parts, s[i:end])
	}
	// "_chunked_" (single underscores) deliberately does NOT match the reserved
	// __x__ pattern, so the marker itself introduces no new violation.
	return bson.M{"_chunked_": true, "encoding": "utf8", "len": len(s), "parts": parts}
}

// sanitizeFieldNames rewrites any reserved __x__ field name to _x_ throughout the
// document, recursively (this is the systemic case: a whole collection/DB whose
// field names all follow the __x__ pattern). _id is left to StrategyRewriteID.
// Key collisions (e.g. both "__x__" and "_x_" present, or two reserved names that
// sanitize to the same string) are broken deterministically with a short hash of
// the original name so no field is silently dropped.
func sanitizeFieldNames(m bson.M, path, rule string, acc *[]Change) bson.M {
	out := make(bson.M, len(m))
	// Pre-compute the sanitized target for every key so collision detection sees
	// both the untouched keys and the incoming renamed ones.
	for k, v := range m {
		cp := k
		if path != "" {
			cp = path + "." + k
		}
		nk := k
		if k != "_id" && reservedName.MatchString(k) {
			nk = sanitizeReserved(k)
			if _, clash := out[nk]; clash || collidesWithSource(m, k, nk) {
				sum := sha1.Sum([]byte(k))
				nk = nk + "_h" + hex.EncodeToString(sum[:])[:8]
			}
			if acc != nil {
				*acc = append(*acc, Change{
					Rule: rule, Strategy: StrategySanitizeFieldNames, Path: cp,
					Violation: fmt.Sprintf("field name %q matches reserved __x__ (Firestore rejects)", k),
					Before:    k, After: nk,
					Detail: fmt.Sprintf("%q → %q", k, nk)})
			}
		}
		ncp := nk
		if path != "" {
			ncp = path + "." + nk
		}
		switch val := v.(type) {
		case bson.M:
			out[nk] = sanitizeFieldNames(val, ncp, rule, acc)
		case map[string]interface{}:
			out[nk] = sanitizeFieldNames(bson.M(val), ncp, rule, acc)
		case bson.A:
			out[nk] = sanitizeFieldNamesArray(val, ncp, rule, acc)
		case []interface{}:
			out[nk] = sanitizeFieldNamesArray(bson.A(val), ncp, rule, acc)
		default:
			out[nk] = v
		}
	}
	return out
}

func sanitizeFieldNamesArray(a bson.A, path, rule string, acc *[]Change) bson.A {
	out := make(bson.A, len(a))
	for i, e := range a {
		switch val := e.(type) {
		case bson.M:
			out[i] = sanitizeFieldNames(val, path, rule, acc)
		case map[string]interface{}:
			out[i] = sanitizeFieldNames(bson.M(val), path, rule, acc)
		case bson.A:
			out[i] = sanitizeFieldNamesArray(val, path, rule, acc)
		case []interface{}:
			out[i] = sanitizeFieldNamesArray(bson.A(val), path, rule, acc)
		default:
			out[i] = e
		}
	}
	return out
}

// collidesWithSource reports whether renaming from→to would land on some OTHER
// original key that stays put (i.e. is itself not reserved), which would clobber it.
func collidesWithSource(m bson.M, from, to string) bool {
	other, exists := m[to]
	_ = other
	if !exists || to == from {
		return false
	}
	// The clashing key survives unchanged only if it is not itself reserved.
	return !reservedName.MatchString(to)
}

/* ---------- helpers ---------- */

// summarize renders any value for the audit "before" field. Large strings are
// truncated with a length + hash tag so the log stays small but forensically
// identifiable; small scalars/containers are JSON-encoded verbatim.
func summarize(v interface{}) string {
	if s, ok := v.(string); ok {
		return summarizeString(s)
	}
	return summarizeString(jsonify(v))
}

// summarizeString truncates s to ~200 bytes, appending the full length and an
// sha1 prefix so two different large values never share a summary.
func summarizeString(s string) string {
	const max = 200
	if len(s) <= max {
		return s
	}
	sum := sha1.Sum([]byte(s))
	return fmt.Sprintf("%s…[+%d bytes, sha1:%s]", s[:max], len(s)-max, hex.EncodeToString(sum[:])[:12])
}

func jsonify(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(data)
}

func cloneM(m bson.M) bson.M {
	out := make(bson.M, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
