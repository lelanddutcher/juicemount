package catalog

import (
	"bytes"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// Saved-query bounds. A definition is small and fixed: a host evaluates it
// against the files it can see; the catalog stores it and nothing else.
const (
	MaxQueryPrefixes       = 16
	MaxQueryPrefixBytes    = 4096
	MaxQueryNameContains   = 256
	MaxQueryExtensions     = 32
	MaxQueryExtensionBytes = 16
	MaxQueryWithinSeconds  = 10 * 366 * 24 * 3600
	MaxQueryLimit          = 500
	maxQueryDefinition     = 128 << 10
)

// QueryID is `qry_` and 32 lowercase hex digits, not all zero.
type QueryID string

func (id QueryID) Validate() error {
	s := string(id)
	if len(s) != 36 || s[:4] != "qry_" {
		return invalid("query identity")
	}
	zero := true
	for _, c := range s[4:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return invalid("query identity")
		}
		zero = zero && c == '0'
	}
	if zero {
		return invalid("query identity")
	}
	return nil
}

// QueryDefinition is a deterministic file query: every file under one of
// Prefixes (volume-relative folders, no leading slash), optionally whose
// name contains NameContains (ignoring case), whose extension is one of
// Extensions (lowercase, without the dot) and that was modified within
// ModifiedWithinSeconds (0: any time); at most Limit results. The catalog
// stores and validates it; it never evaluates it.
//
// Canonical form: Prefixes and Extensions sorted, without duplicates, and
// never null (an empty list encodes []). Wire order: prefixes,
// name_contains, extensions, modified_within_seconds, limit.
type QueryDefinition struct {
	Prefixes              []string `json:"prefixes"`
	NameContains          string   `json:"name_contains"`
	Extensions            []string `json:"extensions"`
	ModifiedWithinSeconds int64    `json:"modified_within_seconds"`
	Limit                 int      `json:"limit"`
}

// ValidQueryPrefix: a canonical volume-relative folder path with no
// hidden (dot) component.
func ValidQueryPrefix(p string) bool {
	if p == "" || len(p) > MaxQueryPrefixBytes || !utf8.ValidString(p) || strings.ContainsRune(p, 0) || path.Clean(p) != p {
		return false
	}
	for _, c := range strings.Split(p, "/") {
		if c == "" || c == "." || c == ".." || strings.HasPrefix(c, ".") {
			return false
		}
	}
	return true
}

// ValidQueryExtension: 1-16 lowercase ASCII letters or digits.
func ValidQueryExtension(e string) bool {
	if e == "" || len(e) > MaxQueryExtensionBytes {
		return false
	}
	for _, c := range e {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// canonical returns a sorted, non-nil copy, or ok=false when the
// definition is out of bounds or holds duplicates.
func (d QueryDefinition) canonical() (QueryDefinition, bool) {
	out := QueryDefinition{NameContains: d.NameContains, ModifiedWithinSeconds: d.ModifiedWithinSeconds, Limit: d.Limit}
	out.Prefixes = append([]string{}, d.Prefixes...)
	out.Extensions = append([]string{}, d.Extensions...)
	sort.Strings(out.Prefixes)
	sort.Strings(out.Extensions)
	if len(out.Prefixes) < 1 || len(out.Prefixes) > MaxQueryPrefixes || len(out.Extensions) > MaxQueryExtensions ||
		!validText(out.NameContains, MaxQueryNameContains, false) || strings.ContainsRune(out.NameContains, '/') ||
		out.ModifiedWithinSeconds < 0 || out.ModifiedWithinSeconds > MaxQueryWithinSeconds || out.Limit < 1 || out.Limit > MaxQueryLimit {
		return QueryDefinition{}, false
	}
	for i, p := range out.Prefixes {
		if !ValidQueryPrefix(p) || (i > 0 && out.Prefixes[i-1] == p) {
			return QueryDefinition{}, false
		}
	}
	for i, e := range out.Extensions {
		if !ValidQueryExtension(e) || (i > 0 && out.Extensions[i-1] == e) {
			return QueryDefinition{}, false
		}
	}
	return out, true
}

// encodeDefinition is the stored form: the canonical definition's compact
// JSON.
func encodeDefinition(d QueryDefinition) ([]byte, error) {
	c, ok := d.canonical()
	if !ok {
		return nil, invalid("query definition")
	}
	data, err := json.Marshal(c)
	if err != nil || len(data) > maxQueryDefinition {
		return nil, invalid("query definition")
	}
	return data, nil
}

// decodeDefinition reads a stored definition strictly: it must be valid and
// byte-for-byte canonical.
func decodeDefinition(data []byte) (QueryDefinition, error) {
	var d QueryDefinition
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil || dec.More() {
		return QueryDefinition{}, corrupt("query definition")
	}
	again, err := encodeDefinition(d)
	if err != nil || !bytes.Equal(again, data) {
		return QueryDefinition{}, corrupt("query definition")
	}
	c, _ := d.canonical()
	return c, nil
}

// CreateQuery reserves a new identity with a 1-256 byte name and its
// definition; it conflicts with any existing query, including a removed one.
// A definition never changes: a different query is a new one.
type CreateQuery struct {
	QueryID    QueryID         `json:"query_id"`
	Name       string          `json:"name"`
	Definition QueryDefinition `json:"definition"`
}

// RenameQuery sets the name of an active query.
type RenameQuery struct {
	QueryID QueryID `json:"query_id"`
	Name    string  `json:"name"`
}

// RemoveQuery tombstones an active query; a removed query accepts no
// further change and its ID is never reused.
type RemoveQuery struct {
	QueryID QueryID `json:"query_id"`
}

// Query includes tombstones.
type Query struct {
	LibraryID       continuity.LibraryID
	ID              QueryID
	Name            string
	Definition      QueryDefinition
	CreatedSequence int64
	UpdatedSequence int64
	Removed         bool
}
