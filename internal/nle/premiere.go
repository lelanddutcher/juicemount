package nle

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

// parsePremiere parses a .prproj file. The file is gzipped XML; the
// XML is a verbose Premiere-internal serialization with media file
// paths scattered across many element types. We don't try to model
// the schema — we walk the token stream and harvest the character
// data of any element whose local name is a known path-bearing tag.
//
// Tags we harvest (case-sensitive, matches Premiere's serialiser):
//   - ActualMediaFilePath  — primary master path of an imported clip
//   - FilePath             — generic file reference
//   - pproUrl              — newer (CC 2019+) URL-form path
//   - SourceFilePath       — used by some asset wrappers
//
// pproUrl values are file:// URLs and need URL-decoding. The other
// tags are usually plain absolute paths but we run them through the
// same normaliser to handle the rare URL-form fallback.
func parsePremiere(path string) ([]MediaRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("nle/premiere: open: %w", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("nle/premiere: gunzip: %w", err)
	}
	defer gz.Close()

	return premiereHarvest(gz)
}

// pathBearingTags is the set of XML local names whose character data
// we treat as a media-path reference. Membership is case-sensitive
// because Premiere's serialiser is consistent.
var pathBearingTags = map[string]bool{
	"ActualMediaFilePath": true,
	"FilePath":            true,
	"pproUrl":             true,
	"SourceFilePath":      true,
}

// optionalTags is the subset that points at proxies/peaks rather than
// the master clip. Marked Optional=true on the resulting MediaRef.
var optionalTags = map[string]bool{
	"ProxyPath":     true,
	"PeakFilePath":  true,
	"AudioPeakPath": true,
}

func premiereHarvest(r io.Reader) ([]MediaRef, error) {
	dec := xml.NewDecoder(r)
	// Premiere's XML occasionally uses non-UTF8 sequences in pre-CC
	// projects. Permit them.
	dec.Strict = false

	var (
		refs       []MediaRef
		curTag     string
		curBuf     strings.Builder
		curOption bool
	)

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("nle/premiere: xml: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			name := t.Name.Local
			if pathBearingTags[name] {
				curTag = name
				curBuf.Reset()
				curOption = false
			} else if optionalTags[name] {
				curTag = name
				curBuf.Reset()
				curOption = true
			}
		case xml.CharData:
			if curTag != "" {
				curBuf.Write(t)
			}
		case xml.EndElement:
			if t.Name.Local == curTag && curTag != "" {
				raw := strings.TrimSpace(curBuf.String())
				if raw != "" {
					p := normalizePremierePath(raw)
					if p != "" {
						refs = append(refs, MediaRef{
							Path:     p,
							Format:   extOf(p),
							Optional: curOption,
						})
					}
				}
				curTag = ""
				curBuf.Reset()
				curOption = false
			}
		}
	}
	return dedupeRefs(refs), nil
}

// normalizePremierePath converts a raw Premiere path string into an
// absolute filesystem path, or returns "" if the input is unusable.
//
// Inputs we handle:
//   - file:///Volumes/foo/bar.mov                  (URL form)
//   - file://localhost/Volumes/foo/bar.mov         (URL form, host)
//   - /Volumes/foo/bar.mov                         (already absolute, POSIX)
//   - Z:\Film Projects\foo.mp4                     (Windows-form, kept as-is)
//   - %20-encoded URLs                             (decoded)
//
// Premiere object-id references like "<FilePath>1129270354</FilePath>"
// (the same tag is reused for both real paths and internal IDs in
// some sub-trees) are rejected by looking for any path-separator.
// A bare integer with no slash and no backslash is not a path.
func normalizePremierePath(raw string) string {
	if strings.HasPrefix(raw, "file://") {
		u, err := url.Parse(raw)
		if err != nil {
			return ""
		}
		// url.Parse leaves the path %-encoded; PathUnescape it.
		p, err := url.PathUnescape(u.Path)
		if err != nil {
			return ""
		}
		return p
	}
	// Some Premiere serialisations percent-encode without the file://
	// scheme. Try to decode but fall back to the raw value.
	if strings.Contains(raw, "%") && !strings.HasPrefix(raw, "/") {
		if dec, err := url.PathUnescape(raw); err == nil {
			raw = dec
		}
	}
	// Reject Premiere internal-ID values that share the FilePath tag.
	// Real paths always contain at least one path separator; numeric
	// IDs never do.
	if !strings.ContainsAny(raw, "/\\") {
		return ""
	}
	return raw
}
