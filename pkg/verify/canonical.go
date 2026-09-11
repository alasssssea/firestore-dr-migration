// Package verify implements post-migration verification (DESIGN §3). Because the
// migrator itself rewrites data (field-name transforms, lossy _id conversion),
// verification cannot compare raw source hashes to target hashes — it must first
// apply the SAME transform to the source and resolve converted _ids via the
// id-map, then compare canonical, order-independent hashes.
package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

// canonicalize returns an order-independent representation of a BSON value:
// maps become key-sorted bson.D, arrays keep their order, scalars pass through.
// Two documents that differ only in field ordering canonicalize identically.
func canonicalize(v interface{}) interface{} {
	switch val := v.(type) {
	case bson.M:
		return canonicalizeMap(map[string]interface{}(val))
	case map[string]interface{}:
		return canonicalizeMap(val)
	case bson.D:
		m := make(map[string]interface{}, len(val))
		for _, e := range val {
			m[e.Key] = e.Value
		}
		return canonicalizeMap(m)
	case bson.A:
		out := make(bson.A, len(val))
		for i, e := range val {
			out[i] = canonicalize(e)
		}
		return out
	case []interface{}:
		out := make(bson.A, len(val))
		for i, e := range val {
			out[i] = canonicalize(e)
		}
		return out
	default:
		return val
	}
}

func canonicalizeMap(m map[string]interface{}) bson.D {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(bson.D, 0, len(m))
	for _, k := range keys {
		out = append(out, bson.E{Key: k, Value: canonicalize(m[k])})
	}
	return out
}

// CanonicalBytes returns a deterministic Extended-JSON encoding of a document,
// independent of original field order.
func CanonicalBytes(doc bson.M) ([]byte, error) {
	return bson.MarshalExtJSON(canonicalize(doc), false, false)
}

// HashDoc returns the hex SHA-256 of a document's canonical form.
func HashDoc(doc bson.M) (string, error) {
	return HashValue(doc)
}

// HashValue hashes any BSON value (bson.M, bson.D, map, ...). The migrator's
// TransformFieldNames may return either a bson.D or bson.M, so verification
// hashes the transform output directly rather than forcing a concrete type.
func HashValue(v interface{}) (string, error) {
	data, err := bson.MarshalExtJSON(canonicalize(v), false, false)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// xorAcc is an O(1)-memory accumulator for an order-independent collection
// fingerprint: it XORs each document hash into a fixed buffer as documents
// stream past, so no per-document state is retained.
type xorAcc [sha256.Size]byte

func (x *xorAcc) add(hexHash string) {
	b, err := hex.DecodeString(hexHash)
	if err != nil || len(b) != sha256.Size {
		return
	}
	for i := 0; i < sha256.Size; i++ {
		x[i] ^= b[i]
	}
}

func (x *xorAcc) hex() string { return hex.EncodeToString(x[:]) }

// XORHashes combines per-document hashes into a single order-independent
// collection fingerprint: XOR is commutative, so document iteration order does
// not affect the result. Returns a hex string.
func XORHashes(hexHashes []string) string {
	var acc [sha256.Size]byte
	for _, h := range hexHashes {
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != sha256.Size {
			continue
		}
		for i := 0; i < sha256.Size; i++ {
			acc[i] ^= b[i]
		}
	}
	return hex.EncodeToString(acc[:])
}
