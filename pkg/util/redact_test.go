package util

import "testing"

func TestRedactURI(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"user and password", "mongodb://alice:s3cret@host:27017/db", "mongodb://alice:***@host:27017/db"},
		{"password with percent-encoded special chars", "mongodb://u:p%40ss%3Aw0rd@host/db", "mongodb://u:***@host/db"},
		{"multi host list", "mongodb://u:pw@h1:27017,h2:27017/db?replicaSet=rs0", "mongodb://u:***@h1:27017,h2:27017/db?replicaSet=rs0"},
		{"srv scheme", "mongodb+srv://u:pw@cluster.example.net/db", "mongodb+srv://u:***@cluster.example.net/db"},
		{"username only", "mongodb://alice@host/db", "mongodb://alice@host/db"},
		{"no credentials", "mongodb://host:27017/db", "mongodb://host:27017/db"},
		{"firestore endpoint", "mongodb://uid.asia-northeast1.firestore.goog:443/mydb?loadBalanced=true&tls=true", "mongodb://uid.asia-northeast1.firestore.goog:443/mydb?loadBalanced=true&tls=true"},
		{"not a uri", "just-a-string", "just-a-string"},
		{"empty", "", ""},
		{"at sign only in query", "mongodb://host/db?authSource=admin&x=a@b", "mongodb://host/db?authSource=admin&x=a@b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RedactURI(tc.in); got != tc.want {
				t.Errorf("RedactURI(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
