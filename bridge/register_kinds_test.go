package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// REGISTER-ROUTE (2026-08-04). The kind table is the security boundary of the
// widened route: it pins the on-disk filename AND assigns the media type
// server-side. These pin both properties against drift.

// The reserved filenames MUST match spec/WRITE_PLACEMENT.md §2 exactly. A drift
// here means a consumer writes one name and we register another — the blob-exists
// check would 409 and contribute-back would silently never work.
func TestContributableKindsMatchTheReservedFilenames(t *testing.T) {
	want := map[string]string{
		"proxy":     "proxy.mp4",
		"thumbnail": "poster.jpg",
		"waveform":  "waveform.json",
	}
	for kind, file := range want {
		got, ok := contributableKinds[kind]
		if !ok {
			t.Errorf("kind %q must be consumer-registrable", kind)
			continue
		}
		if got.file != file {
			t.Errorf("kind %q reserved file = %q, want %q (WRITE_PLACEMENT §2)", kind, got.file, file)
		}
	}
	// `ai` keeps the empty sentinel so its dual-name resolution still runs.
	if ai, ok := contributableKinds["ai"]; !ok || ai.file != "" {
		t.Errorf(`kind "ai" must keep file=="" so ResolveAIBlobName picks logger/loupe; got %+v`, ai)
	}
	// filmstrip stays CLOSED until its reader contract is specified.
	if _, ok := contributableKinds["filmstrip"]; ok {
		t.Error("filmstrip must NOT be consumer-registrable — its reader contract (canvas, gutters, orientation) is unspecified")
	}
}

// media_type is assigned from the kind, never echoed from the request: /blob
// pins its response Content-Type from this row field, so a contributor-supplied
// value would be a contributor-controlled Content-Type on our origin.
func TestContributableKindMediaTypesAreServerAssigned(t *testing.T) {
	want := map[string]string{
		"ai":        "application/json",
		"waveform":  "application/json",
		"proxy":     "video/mp4",
		"thumbnail": "image/jpeg",
	}
	for kind, mt := range want {
		if got := contributableKinds[kind].mediaType; got != mt {
			t.Errorf("kind %q media type = %q, want %q", kind, got, mt)
		}
	}
	// Nothing may declare a scriptable type — the whole point of pinning it.
	for kind, spec := range contributableKinds {
		switch spec.mediaType {
		case "text/html", "application/javascript", "image/svg+xml", "":
			t.Errorf("kind %q has media type %q — scriptable/empty types must never be served from our origin", kind, spec.mediaType)
		}
	}
}

// Every registrable kind must also be a kind the manifest schema knows, or the
// row we write fails the consumer's own validation of /derivatives.
func TestContributableKindsAreValidManifestKinds(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "contract", "spec", "schema", "derivatives.schema.json"))
	if err != nil {
		t.Skipf("vendored contract unavailable: %v", err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var kindEnum []any
	var walk func(any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			if k, ok := v["kind"].(map[string]any); ok {
				if e, ok := k["enum"].([]any); ok && kindEnum == nil {
					kindEnum = e
				}
			}
			for _, c := range v {
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(doc)
	if kindEnum == nil {
		t.Skip("could not locate the manifest kind enum")
	}
	valid := map[string]bool{}
	for _, k := range kindEnum {
		if s, ok := k.(string); ok {
			valid[s] = true
		}
	}
	for kind := range contributableKinds {
		if !valid[kind] {
			t.Errorf("registrable kind %q is not in derivatives.schema.json's kind enum — the row we write would fail the consumer's validation", kind)
		}
	}
}
