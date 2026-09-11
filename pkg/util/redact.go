// Package util holds small cross-cutting helpers shared across the migration tool.
package util

import "strings"

// RedactURI masks the password in a MongoDB connection string so it can be
// safely written to logs. For example:
//
//	mongodb://user:secret@host:27017/db  ->  mongodb://user:***@host:27017/db
//
// Strings that carry no credentials (no userinfo, or username only) are
// returned unchanged. The function is deliberately string-based rather than
// url.Parse-based because MongoDB URIs allow comma-separated host lists
// (mongodb://u:p@h1,h2/db) which net/url does not parse.
func RedactURI(uri string) string {
	scheme := strings.Index(uri, "://")
	if scheme < 0 {
		return uri
	}
	rest := uri[scheme+3:]

	// The userinfo section, if present, ends at the first '@'. A '/' or '?'
	// appearing before that '@' means the '@' belongs to the path/query, not
	// to credentials (e.g. a query value containing '@').
	at := strings.Index(rest, "@")
	if at < 0 {
		return uri
	}
	userinfo := rest[:at]
	if strings.IndexAny(userinfo, "/?") >= 0 {
		return uri
	}

	colon := strings.Index(userinfo, ":")
	if colon < 0 {
		return uri // username only, no password to redact
	}

	redacted := userinfo[:colon] + ":***"
	return uri[:scheme+3] + redacted + rest[at:]
}
