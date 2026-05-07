package nle

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// parseFCPX parses a Final Cut Pro X project. The path may be either:
//
//   - a plain .fcpxml file (XML, no compression)
//   - a .fcpbundle directory, which is an event-bundle directory
//     containing a CurrentVersion.fcpxml entry point.
//
// Inside the XML we look for <asset> elements with src= attributes
// (the canonical media reference) and <media-rep> elements with src=
// (newer FCPX revisions sometimes prefer this form). Both forms encode
// the path as a file:// URL.
func parseFCPX(path string) ([]MediaRef, error) {
	xmlPath, err := resolveFCPXEntryPoint(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(xmlPath)
	if err != nil {
		return nil, fmt.Errorf("nle/fcpx: open: %w", err)
	}
	defer f.Close()
	return fcpxHarvest(f)
}

// resolveFCPXEntryPoint maps a user-supplied path to the actual
// .fcpxml file to parse. For a bundle we look for the conventional
// CurrentVersion.fcpxml; if that doesn't exist we fall back to any
// .fcpxml directly inside the bundle.
func resolveFCPXEntryPoint(path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("nle/fcpx: stat: %w", err)
	}
	if !fi.IsDir() {
		return path, nil
	}
	// Directory bundle. Try CurrentVersion.fcpxml at root, then scan.
	candidate := filepath.Join(path, "CurrentVersion.fcpxml")
	if _, err := os.Stat(candidate); err == nil {
		return candidate, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", fmt.Errorf("nle/fcpx: readdir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".fcpxml") {
			return filepath.Join(path, e.Name()), nil
		}
	}
	return "", fmt.Errorf("nle/fcpx: no .fcpxml found in bundle %s", path)
}

func fcpxHarvest(r io.Reader) ([]MediaRef, error) {
	dec := xml.NewDecoder(r)
	dec.Strict = false

	var refs []MediaRef
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("nle/fcpx: xml: %w", err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch se.Name.Local {
		case "asset", "media-rep":
			// Both elements carry the media path as src=.
			src := attrValue(se.Attr, "src")
			if src == "" {
				continue
			}
			p := normalizeFCPXPath(src)
			if p == "" {
				continue
			}
			r := MediaRef{Path: p, Format: extOf(p)}
			// FCPX records file size in <asset hasVideo="..." hasAudio="..."
			// duration="..."> but not size. Leave Size=0.
			refs = append(refs, r)
		}
	}
	return dedupeRefs(refs), nil
}

// attrValue returns the value of the named XML attribute (matching by
// local name only, ignoring namespace) or "" if absent.
func attrValue(attrs []xml.Attr, name string) string {
	for _, a := range attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// normalizeFCPXPath converts an FCPX src= URL into an absolute path.
// FCPX always uses file:// URLs with percent-encoding for spaces and
// non-ASCII filename characters.
func normalizeFCPXPath(src string) string {
	if !strings.HasPrefix(src, "file://") {
		// Some test fixtures and rare hand-edited FCPXMLs use bare
		// paths. Accept them.
		return src
	}
	u, err := url.Parse(src)
	if err != nil {
		return ""
	}
	p, err := url.PathUnescape(u.Path)
	if err != nil {
		return ""
	}
	return p
}
