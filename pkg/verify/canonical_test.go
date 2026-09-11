package verify

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestCanonical_OrderIndependent(t *testing.T) {
	a := bson.M{"_id": 1, "name": "x", "nested": bson.M{"b": 2, "a": 1}}
	b := bson.M{"nested": bson.M{"a": 1, "b": 2}, "name": "x", "_id": 1}
	ha, err := HashDoc(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := HashDoc(b)
	if err != nil {
		t.Fatal(err)
	}
	if ha != hb {
		t.Errorf("field order changed hash: %s != %s", ha, hb)
	}
}

func TestCanonical_ArrayOrderMatters(t *testing.T) {
	a := bson.M{"_id": 1, "arr": bson.A{1, 2, 3}}
	b := bson.M{"_id": 1, "arr": bson.A{3, 2, 1}}
	ha, _ := HashDoc(a)
	hb, _ := HashDoc(b)
	if ha == hb {
		t.Error("array order should affect hash")
	}
}

func TestCanonical_ContentMatters(t *testing.T) {
	a := bson.M{"_id": 1, "v": "hello"}
	b := bson.M{"_id": 1, "v": "world"}
	ha, _ := HashDoc(a)
	hb, _ := HashDoc(b)
	if ha == hb {
		t.Error("different content should differ")
	}
}

func TestHashValue_AcceptsBsonD(t *testing.T) {
	d := bson.D{{Key: "b", Value: 2}, {Key: "a", Value: 1}}
	m := bson.M{"a": 1, "b": 2}
	hd, err := HashValue(d)
	if err != nil {
		t.Fatal(err)
	}
	hm, err := HashValue(m)
	if err != nil {
		t.Fatal(err)
	}
	if hd != hm {
		t.Errorf("bson.D and equivalent bson.M should hash equally: %s != %s", hd, hm)
	}
}

func TestXOR_OrderIndependentAndSelfCancel(t *testing.T) {
	h1, _ := HashDoc(bson.M{"_id": 1})
	h2, _ := HashDoc(bson.M{"_id": 2})
	h3, _ := HashDoc(bson.M{"_id": 3})

	forward := XORHashes([]string{h1, h2, h3})
	shuffled := XORHashes([]string{h3, h1, h2})
	if forward != shuffled {
		t.Error("XOR fingerprint should be order-independent")
	}

	// XORing a hash twice cancels it out.
	if XORHashes([]string{h1, h2, h1}) != XORHashes([]string{h2}) {
		t.Error("duplicate hash should cancel under XOR")
	}
}

func TestXORAcc_MatchesXORHashes(t *testing.T) {
	h1, _ := HashDoc(bson.M{"_id": 1})
	h2, _ := HashDoc(bson.M{"_id": 2})
	var acc xorAcc
	acc.add(h1)
	acc.add(h2)
	if acc.hex() != XORHashes([]string{h1, h2}) {
		t.Error("streaming xorAcc should match batch XORHashes")
	}
}

func TestDiffHashes(t *testing.T) {
	src := map[string]string{"a": "1", "b": "2", "c": "3"}
	tgt := map[string]string{"a": "1", "b": "X", "d": "4"}
	msgs, more := diffHashes(src, tgt, 20)
	if more != 0 {
		t.Errorf("unexpected elided count: %d", more)
	}
	// Expect: c missing in target, b content differs, d extra in target.
	joined := ""
	for _, m := range msgs {
		joined += m + "\n"
	}
	for _, want := range []string{"missing in target: _id=c", "content differs: _id=b", "extra in target: _id=d"} {
		if !contains(joined, want) {
			t.Errorf("missing expected diff %q in:\n%s", want, joined)
		}
	}
}

func TestDiffHashes_Bounded(t *testing.T) {
	src := map[string]string{}
	tgt := map[string]string{}
	for i := 0; i < 50; i++ {
		src[string(rune('A'+i%26))+string(rune('0'+i/26))] = "s"
	}
	msgs, more := diffHashes(src, tgt, 10)
	if len(msgs) != 10 {
		t.Errorf("expected 10 bounded messages, got %d", len(msgs))
	}
	if more != 40 {
		t.Errorf("expected 40 elided, got %d", more)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
