package util

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseServerVersion parses a MongoDB version string such as "3.2.22" or
// "6.0.5-rc1" into numeric components [major, minor, patch]. Any build or
// pre-release suffix after '-' is ignored, and missing trailing components
// default to 0.
func ParseServerVersion(v string) ([]int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, fmt.Errorf("empty version string")
	}
	// Drop any build/pre-release suffix, e.g. "6.0.5-rc1" -> "6.0.5".
	if i := strings.IndexByte(v, '-'); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	out := make([]int, 0, 3)
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			break // stop at the first non-numeric component
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no numeric components in version %q", v)
	}
	for len(out) < 3 {
		out = append(out, 0)
	}
	return out, nil
}

// VersionAtLeast reports whether the parsed version v is >= major.minor.
func VersionAtLeast(v []int, major, minor int) bool {
	if len(v) == 0 {
		return false
	}
	if v[0] != major {
		return v[0] > major
	}
	vMinor := 0
	if len(v) > 1 {
		vMinor = v[1]
	}
	return vMinor >= minor
}
