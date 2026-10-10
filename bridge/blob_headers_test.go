package main

import (
	"os"
	"strings"
	"testing"
)

// /blob's contract with the consumer, asserted at source level.
//
// Both of these exist because the consumer could not act on what /blob told it:
//
//  1. Every miss is a 404 with a prose body, and "we do not serve this kind"
//     was indistinguishable from "the row exists but its bytes are gone".
//     Those are opposite problems — an integration bug on their side vs a data
//     gap on ours. Measured 2026-08-18: 214 of 400 sampled ready rows point at
//     a blob_rel_path with no file on the volume, so the data-gap case is the
//     COMMON one.
//
//  2. The D3 trust gate compares the manifest's source_size against a live
//     stat before applying identity-bearing state. Without it on the response,
//     the consumer must fetch /derivatives as well as /blob for every asset —
//     two round trips to answer one question, on a link whose cost model is
//     round trips.
//
// Source-level because the handler needs a live mount, a derivative store and a
// populated manifest to exercise end-to-end; that is an integration test, and
// its absence is exactly how these gaps survived.

func blobSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("cbridge.go")
	if err != nil {
		t.Fatalf("read cbridge.go: %v", err)
	}
	return string(b)
}

func TestEveryBlobMissCarriesAReason(t *testing.T) {
	src := blobSource(t)
	for _, want := range []string{
		`X-JM-Blob-Miss", "no-ready-row"`,
		`X-JM-Blob-Miss", "stale-source"`,
		`X-JM-Blob-Miss", "blob-absent"`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("a /blob 404 path is missing its reason header (%s). The "+
				"consumer cannot tell an unsupported kind from a data gap, and "+
				"the data gap is the common case.", want)
		}
	}
}

func TestBlobSuccessPathsPublishTheRowVouch(t *testing.T) {
	src := blobSource(t)
	if n := strings.Count(src, "blobVouchHeaders(w, kind, row)"); n < 3 {
		t.Errorf("only %d /blob success path(s) publish the vouch headers; there "+
			"are three (local cache hit, read-through, and the large/Range "+
			"fallback). A consumer that happens to hit an unstamped path loses "+
			"its D3 gate with no signal.", n)
	}
	for _, h := range []string{"X-JM-Source-Size", "X-JM-Source-Mtime", "X-JM-Hash", "X-JM-Kind"} {
		if !strings.Contains(src, h) {
			t.Errorf("vouch header %s is not published", h)
		}
	}
}
