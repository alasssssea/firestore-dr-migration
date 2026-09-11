package migration

import (
	"encoding/json"
	"fmt"
	"sync"

	"firestore-dr-migration/pkg/assess"
	"firestore-dr-migration/pkg/idmap"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/remediate"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// sharedIDStore is the process-wide sink for _id conversions. Every
// FieldTransformer created while it is set records the invalid-_id → string
// conversions it performs (proactivelyConvertID), so `-mode=verify` can
// reconnect a rewritten target document to its source _id (see pkg/idmap,
// pkg/verify). It defaults to a no-op, so tests and any path that does not
// enable id-mapping are unaffected. The Migrator installs a real FileStore once
// at Start() and restores the no-op when the run ends. A single shared,
// mutex-guarded store (rather than one per transformer) keeps every worker
// appending to one file without interleaving.
var (
	sharedIDStoreMu sync.RWMutex
	sharedIDStore   idmap.Store = idmap.NopStore{}
)

// SetSharedIDStore installs the process-wide _id-conversion sink used by every
// FieldTransformer created afterwards. Pass idmap.NopStore{} (or nil) to disable.
func SetSharedIDStore(s idmap.Store) {
	sharedIDStoreMu.Lock()
	defer sharedIDStoreMu.Unlock()
	if s == nil {
		s = idmap.NopStore{}
	}
	sharedIDStore = s
}

func currentSharedIDStore() idmap.Store {
	sharedIDStoreMu.RLock()
	defer sharedIDStoreMu.RUnlock()
	return sharedIDStore
}

// maxFieldNameLength is the maximum allowed field name length.
// Firestore has a 1,500-byte limit on field names. We use 1,000 as a safety threshold.
// Objects containing field names exceeding this limit are stringified to JSON.
const maxFieldNameLength = 1000

// FieldTransformer handles Firestore compatibility transformations
type FieldTransformer struct {
	dropEmptyFieldNames               bool
	convertLongFieldNamesInNestedDocs bool
	convertInvalidIds                 bool
	log                               *logger.Logger
	// idStore records invalid-_id → string conversions for -mode=verify
	// reconciliation. Captured from the process-wide shared store at
	// construction; a no-op when id-mapping is disabled.
	idStore idmap.Store
	// plan carries the operator-approved remediations from the console assessment
	// (pkg/remediate). It is applied at migration time so the data landing in
	// Firestore matches what the re-assessment simulated — the source is never
	// mutated. A nil/empty plan leaves every document untouched.
	plan *remediate.Plan
}

// NewFieldTransformer creates a new FieldTransformer. It loads the persisted
// remediation plan (if any) so migration-time transforms match the assessment's
// "apply fix" simulation. A missing plan file is not an error (empty plan).
func NewFieldTransformer(dropEmptyFieldNames, convertLongFieldNamesInNestedDocs, convertInvalidIds bool, log *logger.Logger) *FieldTransformer {
	plan, err := remediate.Load(remediate.DefaultPlanFile)
	if err != nil {
		if log != nil {
			log.Warnf("Could not load remediation plan %s: %v", remediate.DefaultPlanFile, err)
		}
		plan = &remediate.Plan{}
	}
	if log != nil && len(plan.Items) > 0 {
		log.Infof("Loaded %d remediation(s) from %s — applying at migration time (source untouched)",
			len(plan.Items), remediate.DefaultPlanFile)
	}
	return &FieldTransformer{
		dropEmptyFieldNames:               dropEmptyFieldNames,
		convertLongFieldNamesInNestedDocs: convertLongFieldNamesInNestedDocs,
		convertInvalidIds:                 convertInvalidIds,
		log:                               log,
		idStore:                           currentSharedIDStore(),
		plan:                              plan,
	}
}

// SanitizeTargetName sanitizes a target collection name when a rename-collection
// remediation is registered for the source collection, so a reserved-name fix
// (__x__ → _x_) redirects writes on the migration path. A no-op when no such fix
// is registered or the name is already legal.
func (t *FieldTransformer) SanitizeTargetName(dbName, sourceColl, target string) string {
	return t.plan.SanitizedTargetName(dbName, sourceColl, target)
}

// extractDocID extracts the _id field from a document for logging purposes.
func extractDocID(doc interface{}) interface{} {
	switch d := doc.(type) {
	case bson.D:
		for _, elem := range d {
			if elem.Key == "_id" {
				return elem.Value
			}
		}
	case bson.M:
		return d["_id"]
	case map[string]interface{}:
		return d["_id"]
	}
	return nil
}

// toComparableIDKey converts any MongoDB _id (including primitive.Binary, primitive.ObjectID, etc.)
// into a type-prefixed comparable string suitable for use as a Go map key without type collisions.
func toComparableIDKey(id interface{}) string {
	if id == nil {
		return "<nil>"
	}
	t, data, err := bson.MarshalValue(id)
	if err != nil {
		return fmt.Sprintf("%T:%v", id, id)
	}
	return fmt.Sprintf("%d:%s", t, string(data))
}

// Transform recursively walks a document and applies Firestore-compatible transformations:
//   - Removes empty field names
//   - Stringifies nested objects that contain field names exceeding maxFieldNameLength
//
// Returns the transformed document and an error if a field name collision is detected.
// Supports bson.D, bson.M, map[string]interface{}, and arrays.
// Logs transformations at Info/Warn level with db, collection, and document ID context.
func (t *FieldTransformer) Transform(doc interface{}, dbName, collName string, docID interface{}) (interface{}, error) {
	hasPlan := doc != nil && t.plan.HasCollection(dbName, collName)
	if !t.dropEmptyFieldNames && !t.convertLongFieldNamesInNestedDocs && !t.convertInvalidIds && !hasPlan {
		return doc, nil
	}
	// Root-level documents cannot be stringified (must remain documents for MongoDB insert).
	// Warn about long keys at root level but don't stringify.
	if doc != nil && t.convertLongFieldNamesInNestedDocs {
		t.warnRootLongKeys(doc, dbName, collName, docID)
	}
	if doc != nil && t.convertInvalidIds {
		doc = t.proactivelyConvertID(doc, dbName, collName)
	}
	out, err := t.transformFieldNamesRecursive(doc, dbName, collName, docID, true)
	if err != nil {
		return nil, err
	}
	// Apply the operator-approved remediation plan last, so it is the authoritative
	// transform for the findings it targets (reserved _id, over-deep nesting,
	// over-long field paths/names, over-size values). Only collections with a
	// registered fix are touched; the default migration path is unchanged.
	if hasPlan {
		if m, ok := toBSONM(out); ok {
			fixed, changes := t.plan.ApplyDocAudit(dbName, collName, m)
			if len(changes) > 0 {
				// Audit trail: record exactly what the fix changed on this document,
				// so a later Firestore discrepancy can be traced to its transform.
				recordRemediations(t.log, dbName, collName,
					t.plan.CollectionName(dbName, collName), fixed["_id"], m["_id"], changes)
			}
			return fixed, nil
		}
		if t.log != nil {
			t.log.Warnf("Remediation plan set for %s.%s but document type %T is not a map — skipped [_id=%v]",
				dbName, collName, out, docID)
		}
	}
	return out, nil
}

// toBSONM deep-converts a document to a bson.M tree (bson.D/bson.M → bson.M,
// arrays → []interface{}) so the remediation transforms — which operate on
// bson.M/map/slice trees — can walk it. Field order is not preserved, which is
// semantically irrelevant to Firestore. Returns false for non-document roots.
func toBSONM(doc interface{}) (bson.M, bool) {
	switch d := doc.(type) {
	case bson.M:
		conv := make(bson.M, len(d))
		for k, v := range d {
			conv[k] = bsonValueToInterface(v)
		}
		return conv, true
	case bson.D:
		return bson.M(bsonDToMap(d)), true
	case map[string]interface{}:
		conv := make(bson.M, len(d))
		for k, v := range d {
			conv[k] = bsonValueToInterface(v)
		}
		return conv, true
	default:
		return nil, false
	}
}

func (t *FieldTransformer) proactivelyConvertID(doc interface{}, dbName, collName string) interface{} {
	switch d := doc.(type) {
	case bson.D:
		for i, elem := range d {
			if elem.Key == "_id" {
				if !t.isValidIDType(elem.Value) {
					originalType := fmt.Sprintf("%T", elem.Value)
					newID := serializeIDDeterministically(elem.Value)
					if t.log != nil {
						t.log.Infof("[%s.%s] Proactively converting invalid _id %v (type: %s) to string: %s (Solution 1, 2 & 4)",
							dbName, collName, elem.Value, originalType, newID)
					}
					t.recordIDConversion(collName, elem.Value, newID)
					newDoc := make(bson.D, len(d))
					copy(newDoc, d)
					newDoc[i].Value = newID
					return newDoc
				}
				break
			}
		}
	case bson.M:
		if id, ok := d["_id"]; ok {
			if !t.isValidIDType(id) {
				originalType := fmt.Sprintf("%T", id)
				newID := serializeIDDeterministically(id)
				if t.log != nil {
					t.log.Infof("[%s.%s] Proactively converting invalid _id %v (type: %s) to string: %s (Solution 1, 2 & 4)",
						dbName, collName, id, originalType, newID)
				}
				t.recordIDConversion(collName, id, newID)
				newDoc := make(bson.M, len(d))
				for k, v := range d {
					newDoc[k] = v
				}
				newDoc["_id"] = newID
				return newDoc
			}
		}
	}
	return doc
}

// recordIDConversion appends the original→converted _id mapping to the shared
// id-map sink (a no-op when id-mapping is disabled). It is keyed by the SOURCE
// collection name to match how -mode=verify looks the mapping up
// (pkg/verify: mapper.Lookup(cp.source, origID)). A record failure is logged
// but never blocks migration — the mapping is a verification/audit aid, not
// part of the write itself.
func (t *FieldTransformer) recordIDConversion(collName string, origID interface{}, newID string) {
	if t.idStore == nil {
		return
	}
	if err := t.idStore.Record(collName, origID, newID); err != nil && t.log != nil {
		t.log.Warnf("[%s] failed to record _id conversion in id-map: %v", collName, err)
	}
}

// isValidIDType delegates to assess.IsValidIDType, the single source of truth
// for which _id types are stored as-is. Keeping one definition guarantees the
// assessment's prediction and the engine's conversion never drift.
func (t *FieldTransformer) isValidIDType(id interface{}) bool {
	return assess.IsValidIDType(id)
}

func serializeIDDeterministically(id interface{}) string {
	switch val := id.(type) {
	case bool:
		return fmt.Sprintf("_converted:bool:%t", val)
	case int32:
		return fmt.Sprintf("_converted:int32:%d", val)
	case int:
		return fmt.Sprintf("_converted:int:%d", val)
	case float64:
		return fmt.Sprintf("_converted:double:%g", val)
	case float32:
		return fmt.Sprintf("_converted:float:%g", val)
	case primitive.DateTime:
		return fmt.Sprintf("_converted:datetime:%d", val)
	case primitive.Binary:
		return fmt.Sprintf("_converted:binary:%x", val.Data)
	case []interface{}:
		data, err := json.Marshal(val)
		if err == nil {
			return fmt.Sprintf("_converted:array:%s", string(data))
		}
		return fmt.Sprintf("_converted:array:%v", val)
	case bson.A:
		data, err := json.Marshal(val)
		if err == nil {
			return fmt.Sprintf("_converted:array:%s", string(data))
		}
		return fmt.Sprintf("_converted:array:%v", val)
	case bson.D, bson.M, map[string]interface{}:
		data, err := json.Marshal(val)
		if err == nil {
			return fmt.Sprintf("_converted:document:%s", string(data))
		}
		return fmt.Sprintf("_converted:document:%v", val)
	default:
		return fmt.Sprintf("_converted:%T:%v", val, val)
	}
}

// warnRootLongKeys logs warnings for any root-level field names exceeding maxFieldNameLength.
// Root documents cannot be stringified, so we can only warn about them.
func (t *FieldTransformer) warnRootLongKeys(doc interface{}, dbName, collName string, docID interface{}) {
	if t.log == nil {
		return
	}
	switch d := doc.(type) {
	case bson.D:
		for _, elem := range d {
			if len(elem.Key) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(elem.Key), elem.Key[:80], dbName, collName, docID)
			}
		}
	case bson.M:
		for k := range d {
			if len(k) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(k), k[:80], dbName, collName, docID)
			}
		}
	case map[string]interface{}:
		for k := range d {
			if len(k) > maxFieldNameLength {
				t.log.Warnf("Root-level field name exceeds %d chars (len=%d): \"%s...\" [db=%s, collection=%s, _id=%v]. Cannot stringify root document.",
					maxFieldNameLength, len(k), k[:80], dbName, collName, docID)
			}
		}
	}
}

// transformFieldNamesRecursive is the internal recursive implementation.
// isRoot=true for the top-level document (skip long key stringification),
// isRoot=false for nested objects (enable long key stringification).
func (t *FieldTransformer) transformFieldNamesRecursive(doc interface{}, dbName, collName string, docID interface{}, isRoot bool) (interface{}, error) {
	if doc == nil {
		return nil, nil
	}

	switch d := doc.(type) {
	case bson.D:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object to avoid Firestore field name errors.
		if !isRoot && t.convertLongFieldNamesInNestedDocs {
			for _, elem := range d {
				if len(elem.Key) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(elem.Key), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(bsonDToMap(d))
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(bson.D, 0, len(d))

		for _, elem := range d {
			// Remove empty field names (Firestore does not support them)
			if elem.Key == "" {
				if t.dropEmptyFieldNames {
					if t.log != nil {
						t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
							dbName, collName, docID)
					}
					continue
				}
			}

			transformedValue, err := t.transformFieldNamesRecursive(elem.Value, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}

			result = append(result, bson.E{
				Key:   elem.Key,
				Value: transformedValue,
			})
		}
		return result, nil

	case bson.M:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object.
		if !isRoot && t.convertLongFieldNamesInNestedDocs {
			for k := range d {
				if len(k) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(k), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(d)
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(bson.M, len(d))

		for k, v := range d {
			// Remove empty field names (Firestore does not support them)
			if k == "" {
				if t.dropEmptyFieldNames {
					if t.log != nil {
						t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
							dbName, collName, docID)
					}
					continue
				}
			}

			transformedValue, err := t.transformFieldNamesRecursive(v, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[k] = transformedValue
		}
		return result, nil

	case map[string]interface{}:
		// For nested objects: if any immediate key exceeds maxFieldNameLength,
		// stringify the entire object.
		if !isRoot && t.convertLongFieldNamesInNestedDocs {
			for k := range d {
				if len(k) > maxFieldNameLength {
					if t.log != nil {
						t.log.Warnf("Field name exceeds %d chars (len=%d) in nested object. Stringifying parent object [db=%s, collection=%s, _id=%v]",
							maxFieldNameLength, len(k), dbName, collName, docID)
					}
					jsonBytes, err := json.Marshal(d)
					if err != nil {
						if t.log != nil {
							t.log.Errorf("Failed to stringify object with long field name [db=%s, collection=%s, _id=%v]: %v",
								dbName, collName, docID, err)
						}
						return nil, fmt.Errorf("failed to stringify object with long field name: %w", err)
					}
					return string(jsonBytes), nil
				}
			}
		}

		result := make(map[string]interface{}, len(d))

		for k, v := range d {
			// Remove empty field names (Firestore does not support them)
			if k == "" {
				if t.dropEmptyFieldNames {
					if t.log != nil {
						t.log.Warnf("Removed empty field name from document [db=%s, collection=%s, _id=%v]",
							dbName, collName, docID)
					}
					continue
				}
			}

			transformedValue, err := t.transformFieldNamesRecursive(v, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[k] = transformedValue
		}
		return result, nil

	case []interface{}:
		result := make([]interface{}, len(d))
		for i, item := range d {
			transformedValue, err := t.transformFieldNamesRecursive(item, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[i] = transformedValue
		}
		return result, nil

	case bson.A:
		result := make(bson.A, len(d))
		for i, item := range d {
			transformedValue, err := t.transformFieldNamesRecursive(item, dbName, collName, docID, false)
			if err != nil {
				return nil, err
			}
			result[i] = transformedValue
		}
		return result, nil

	default:
		// Primitive types (string, int, float, bool, ObjectID, etc.) - return as-is
		return doc, nil
	}
}

// bsonDToMap converts a bson.D to a map for JSON marshaling.
// bson.D is an ordered slice of key-value pairs which json.Marshal handles differently.
func bsonDToMap(d bson.D) map[string]interface{} {
	result := make(map[string]interface{}, len(d))
	for _, elem := range d {
		result[elem.Key] = bsonValueToInterface(elem.Value)
	}
	return result
}

// bsonValueToInterface recursively converts bson types to standard Go types for JSON marshaling.
func bsonValueToInterface(v interface{}) interface{} {
	switch val := v.(type) {
	case bson.D:
		return bsonDToMap(val)
	case bson.M:
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			result[k] = bsonValueToInterface(v)
		}
		return result
	case bson.A:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = bsonValueToInterface(item)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = bsonValueToInterface(item)
		}
		return result
	default:
		return v
	}
}

// TransformBatch applies Transform to each document in a batch.
// Extracts _id from each document for logging context.
func (t *FieldTransformer) TransformBatch(batch []interface{}, dbName, collName string) ([]interface{}, error) {
	if !t.dropEmptyFieldNames && !t.convertLongFieldNamesInNestedDocs && !t.convertInvalidIds && !t.plan.HasCollection(dbName, collName) {
		return batch, nil
	}
	result := make([]interface{}, len(batch))
	for i, doc := range batch {
		docID := extractDocID(doc)
		transformed, err := t.Transform(doc, dbName, collName, docID)
		if err != nil {
			return nil, err
		}
		result[i] = transformed
	}
	return result, nil
}
