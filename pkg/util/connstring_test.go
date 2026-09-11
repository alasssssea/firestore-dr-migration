package util

import (
	"strings"
	"testing"
)

func TestBuildFirestoreURI(t *testing.T) {
	got := BuildFirestoreURI("uid.asia-northeast1.firestore.goog", "mydb", "user", "p@ss:w0rd")

	// Host gets :443 appended.
	if !strings.Contains(got, "uid.asia-northeast1.firestore.goog:443") {
		t.Errorf("expected :443 appended, got %q", got)
	}
	// Mandatory Firestore params present.
	for _, want := range []string{"loadBalanced=true", "tls=true", "retryWrites=false", "authMechanism=SCRAM-SHA-256"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q in URI, got %q", want, got)
		}
	}
	// Database in path.
	if !strings.Contains(got, "/mydb?") {
		t.Errorf("expected /mydb in path, got %q", got)
	}
	// Credentials percent-encoded (raw '@' and ':' in password must not leak).
	if strings.Contains(got, "p@ss:w0rd") {
		t.Errorf("password should be percent-encoded, got %q", got)
	}
	// Redaction still works on the generated URI.
	if red := RedactURI(got); strings.Contains(red, "w0rd") {
		t.Errorf("redaction failed on generated URI: %q", red)
	}
}

func TestBuildFirestoreURI_ExistingPort(t *testing.T) {
	got := BuildFirestoreURI("localhost:27017", "db", "", "")
	if strings.Contains(got, ":27017:443") {
		t.Errorf("should not append :443 when port present, got %q", got)
	}
	if strings.Contains(got, "@") {
		t.Errorf("no credentials expected, got %q", got)
	}
}

func TestBuildFirestoreURIOIDC(t *testing.T) {
	got := BuildFirestoreURIOIDC("uid.asia-northeast1.firestore.goog", "mydb")
	for _, want := range []string{
		"uid.asia-northeast1.firestore.goog:443",
		"loadBalanced=true", "tls=true", "retryWrites=false",
		"authMechanism=MONGODB-OIDC",
		"authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE",
		"/mydb?",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected %q in OIDC URI, got %q", want, got)
		}
	}
	// OIDC uses no embedded credentials.
	if strings.Contains(got, "@") {
		t.Errorf("OIDC URI must not embed credentials, got %q", got)
	}
	// authMechanismProperties separators must stay literal, not percent-encoded.
	if strings.Contains(got, "%3A") || strings.Contains(got, "%2C") {
		t.Errorf("OIDC properties should not be percent-encoded, got %q", got)
	}
}

func TestBuildSourceURI_LegacyOmitsMechanism(t *testing.T) {
	got := BuildSourceURI("old:27017", "admin", "u", "p", "", true)
	if strings.Contains(got, "authMechanism=") {
		t.Errorf("legacy source must omit authMechanism, got %q", got)
	}
	if !strings.Contains(got, "authSource=admin") {
		t.Errorf("expected authSource=admin default, got %q", got)
	}
}

func TestBuildSourceURI_ModernSetsMechanism(t *testing.T) {
	got := BuildSourceURI("host:27017", "app", "u", "p", "app", false)
	if !strings.Contains(got, "authMechanism=SCRAM-SHA-256") {
		t.Errorf("modern source should set SCRAM-SHA-256, got %q", got)
	}
	if !strings.Contains(got, "authSource=app") {
		t.Errorf("explicit authSource should be honored, got %q", got)
	}
}

func TestBuildSourceURI_NoCreds(t *testing.T) {
	got := BuildSourceURI("host:27017", "", "", "", "", false)
	if strings.Contains(got, "authSource") || strings.Contains(got, "authMechanism") {
		t.Errorf("no-credential source should not set auth params, got %q", got)
	}
	if strings.Contains(got, "@") {
		t.Errorf("no credentials expected, got %q", got)
	}
}
