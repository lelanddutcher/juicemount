package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

// OPTIONAL blob_size on POST /derivatives/register (contract (AO) Q1.2).
//
// register validates that the blob exists, is a regular file, is non-zero, and
// that the live source's size+mtime match what the consumer vouched. It never
// reads the consumer's blob BYTES. So a blob truncated at a plausible non-zero
// length passes every check and mints a `ready` row over partial data, and
// nothing downstream detects it — a reader gets 200 and half a file.
//
// We stat the blob anyway, so comparing a vouched size is free. Additive:
// omitting blob_size must behave exactly as it did before.
func registerWith(t *testing.T, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
	return rr
}

// blobSizeFixture seeds a live source + a blob of `blobBytes` and wires the
// globals, returning the register body (without blob_size).
func blobSizeFixture(t *testing.T, rel string, inode uint64, blobBytes []byte) map[string]any {
	t.Helper()
	tmp := t.TempDir()
	ms, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ds.Close() })

	ino, mt := seedSource(t, tmp, ms, rel, inode, 2048)
	writeBlob(t, tmp, ino, "poster.jpg", blobBytes)

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = ms, ds, tmp
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	})
	return regBody(ino, "thumbnail", 2048, mt)
}

func TestRegisterBlobSizeMismatchIs409BlobTruncated(t *testing.T) {
	body := blobSizeFixture(t, "A/trunc.mov", 2_000_001, bytes.Repeat([]byte("J"), 400))
	body["blob_size"] = 4096 // the consumer believes it wrote 4096; 400 landed

	rr := registerWith(t, body)
	if rr.Code != 409 {
		t.Fatalf("status %d, want 409 — a truncated blob was accepted and a ready row "+
			"minted over partial data; body %s", rr.Code, rr.Body.String())
	}
	var got registerConflict
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("409 body is not the shared conflict shape (%v): %s", err, rr.Body.String())
	}
	if got.Code != "blob_truncated" {
		t.Errorf("code = %q, want %q", got.Code, "blob_truncated")
	}
	if got.Retryable {
		t.Error("retryable = true — retrying the SAME request cannot change either number; " +
			"the consumer must rewrite the blob. A permanent condition marked retryable " +
			"burns the whole budget and then publishes nothing")
	}
	// The message must name BOTH numbers: without them the consumer cannot tell
	// a short write from a stale vouch.
	for _, want := range []string{"400", "4096"} {
		if !bytes.Contains([]byte(got.Message), []byte(want)) {
			t.Errorf("message does not name %s: %q", want, got.Message)
		}
	}
}

func TestRegisterBlobSizeMatchAccepts(t *testing.T) {
	body := blobSizeFixture(t, "A/exact.mov", 2_000_002, bytes.Repeat([]byte("J"), 400))
	body["blob_size"] = 400

	rr := registerWith(t, body)
	if rr.Code != 200 {
		t.Fatalf("status %d for a MATCHING blob_size, want 200 — the guard is rejecting "+
			"honest registrations; body %s", rr.Code, rr.Body.String())
	}
}

// ADDITIVE: omitting blob_size must behave exactly as before it existed.
func TestRegisterWithoutBlobSizeUnchanged(t *testing.T) {
	body := blobSizeFixture(t, "A/silent.mov", 2_000_003, bytes.Repeat([]byte("J"), 400))
	// no blob_size key at all
	rr := registerWith(t, body)
	if rr.Code != 200 {
		t.Fatalf("status %d with blob_size ABSENT, want 200 — the field is optional and "+
			"every existing consumer omits it; body %s", rr.Code, rr.Body.String())
	}
}

// blob_size:0 is a SUPPLIED value, not an absent one — it must be compared,
// not silently ignored the way a non-pointer int64 zero value would be. (The
// blob is non-empty, so the comparison is what rejects it, and it lands as
// blob_truncated rather than blob_empty.)
func TestRegisterBlobSizeZeroIsAVouchNotAnOmission(t *testing.T) {
	body := blobSizeFixture(t, "A/zerovouch.mov", 2_000_004, bytes.Repeat([]byte("J"), 400))
	body["blob_size"] = 0

	rr := registerWith(t, body)
	if rr.Code != 409 {
		t.Fatalf("status %d for blob_size:0 against a 400-byte blob, want 409 — a supplied "+
			"zero was treated as \"not supplied\"; body %s", rr.Code, rr.Body.String())
	}
	var got registerConflict
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != "blob_truncated" {
		t.Errorf("code = %q, want blob_truncated", got.Code)
	}
}
