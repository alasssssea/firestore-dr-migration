package util

import (
	"net/url"
	"strings"
)

// BuildFirestoreURI builds a Firestore (MongoDB-compatibility) connection string
// for a target database. Firestore requires an explicit set of parameters that
// operators frequently get wrong when hand-writing them:
//
//   - loadBalanced=true and tls=true          (Firestore's managed endpoint)
//   - retryWrites=false                        (Firestore does not support retryable writes)
//   - authMechanism=SCRAM-SHA-256              (must be explicit; the target will
//     not negotiate it down like a self-hosted server)
//
// host is the endpoint such as "uid.asia-northeast1.firestore.goog" (":443" is
// appended if no port is present). dbID is the Firestore database ID. Credentials
// are percent-encoded. This is the target-side counterpart to the source rule in
// DESIGN §6 (old sources omit authMechanism; the Firestore target must set it).
func BuildFirestoreURI(host, dbID, username, password string) string {
	if !strings.Contains(host, ":") {
		host += ":443"
	}
	u := url.URL{
		Scheme: "mongodb",
		Host:   host,
		Path:   "/" + strings.TrimPrefix(dbID, "/"),
	}
	if username != "" {
		u.User = url.UserPassword(username, password)
	}
	// Fixed, deterministic parameter order for readability and stable tests.
	u.RawQuery = "loadBalanced=true&tls=true&retryWrites=false&authMechanism=SCRAM-SHA-256"
	return u.String()
}

// BuildFirestoreURIOIDC builds a Firestore target connection string that
// authenticates with the ambient GCP service account via OIDC instead of a
// static SCRAM username/password. This is the recommended mode for long-running
// migrations (DESIGN §6): a SCRAM password can expire mid-run, whereas the OIDC
// token is refreshed from the environment (Cloud Run / GCE / ADC).
//
// It sets authMechanism=MONGODB-OIDC with
// authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE, plus the same
// mandatory loadBalanced/tls/retryWrites parameters. No credentials are embedded.
func BuildFirestoreURIOIDC(host, dbID string) string {
	if !strings.Contains(host, ":") {
		host += ":443"
	}
	u := url.URL{
		Scheme: "mongodb",
		Host:   host,
		Path:   "/" + strings.TrimPrefix(dbID, "/"),
	}
	// url.Values would percent-encode the ':' and ',' in authMechanismProperties,
	// which the driver does not require; build the query literally for a clean,
	// stable string.
	u.RawQuery = "loadBalanced=true&tls=true&retryWrites=false&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"
	return u.String()
}

// BuildSourceURI builds a source connection string that is deliberately
// asymmetric to the target (DESIGN §6): old MongoDB sources must OMIT
// authMechanism so the driver negotiates the right SCRAM variant (hard-coding a
// modern mechanism fails against 3.x). Callers pass legacy=true for pre-4.0
// sources; authSource defaults to "admin" when a username is given and no
// explicit authSource is provided.
func BuildSourceURI(host, database, username, password, authSource string, legacy bool) string {
	u := url.URL{
		Scheme: "mongodb",
		Host:   host,
	}
	if database != "" {
		u.Path = "/" + strings.TrimPrefix(database, "/")
	}
	if username != "" {
		u.User = url.UserPassword(username, password)
	}

	params := url.Values{}
	if username != "" {
		if authSource == "" {
			authSource = "admin"
		}
		params.Set("authSource", authSource)
	}
	// Only modern sources get an explicit authMechanism; legacy sources rely on
	// negotiation (saslSupportedMechs) to avoid a mechanism mismatch.
	if !legacy && username != "" {
		params.Set("authMechanism", "SCRAM-SHA-256")
	}
	u.RawQuery = params.Encode()
	return u.String()
}
