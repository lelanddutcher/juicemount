package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

// A SYNTHETIC inode must never mint a manifest row, and must fail RETRYABLY.
//
// THE BUG, as reproduced by adversarial review. juiceFS.Create mints a synthetic
// inode via nextSyntheticInode() and then InsertToCache's it
// (nfs/handler.go:3247), so a synthetic inode is a LIVE CACHE KEY and
// LookupByInode SUCCEEDS. An earlier version of this guard sat inside the
// `entry == nil` branch and therefore never ran for the real case: register
// returned 200 and committed a row keyed to an identifier that vanishes once
// Redis reconcile assigns the true inode. The blob is then stranded at
// .juicemount/derivatives/<vanished>/ with nothing referencing it, and there is
// no GC for derivative rows. Silent data loss behind a success code.
//
// The exposure is not just the drain window: files created OFFLINE keep a
// synthetic inode indefinitely.
//
// This test seeds through the PRODUCTION path (InsertToCache) precisely because
// the previous version did not — it seeded nothing, so every request fell into
// `entry == nil` and the test could not see the branch where the bug lived.
func TestRegisterSyntheticInodeNeverMintsARow(t *testing.T) {
	tmp := t.TempDir()
	const relPath = "REEL_0065/A001C001.mov"
	// nextSyntheticInode is `counter.Add(1) | 1<<63`, so real values are >= 2^63+1.
	const synthetic = uint64(1<<63 + 44)

	src := filepath.Join(tmp, relPath)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000000, 0)
	_ = os.Chtimes(src, mt, mt)

	// The blob the consumer wrote, under the SYNTHETIC inode's directory —
	// exactly what a consumer that trusted its own stat() would have produced.
	blobDir := filepath.Join(tmp, ".juicemount", "derivatives",
		itoa(synthetic))
	_ = os.MkdirAll(blobDir, 0o755)
	_ = os.WriteFile(filepath.Join(blobDir, "poster.jpg"), []byte("JPEGDATA"), 0o644)

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()

	// PRODUCTION SEEDING: this is what juiceFS.Create does on a fresh file.
	mstore.InsertToCache(&metadata.Entry{
		Path: relPath, Name: "A001C001.mov", ParentPath: "REEL_0065",
		Inode: synthetic, Size: 4096, Mtime: mt, LocalOnly: true,
	})
	if got := mstore.LookupByInode(synthetic); got == nil {
		t.Fatal("seeding failed: the synthetic inode is not a live cache key, so this " +
			"test would not exercise the branch the bug lived in")
	}

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	b, _ := json.Marshal(map[string]any{
		"inode": synthetic, "kind": "thumbnail", "producer": "on-device",
		"source_size": 4096, "source_mtime": mt.Unix(), "blob_rel_path": "poster.jpg",
	})
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))

	if rr.Code == 200 {
		t.Fatalf("register SUCCEEDED on a synthetic inode — the row is keyed to an "+
			"identifier that ceases to exist, orphaning the blob permanently. body=%s",
			rr.Body.String())
	}
	if rr.Code != 409 {
		t.Fatalf("status %d, want 409 (retryable); body %s", rr.Code, rr.Body.String())
	}

	// THE ORPHAN ITSELF: no manifest row may exist under the synthetic inode.
	// Asserting on the status code alone would not catch a handler that 409'd
	// AFTER writing the row.
	if rows, err := dstore.Manifest(synthetic); err == nil && len(rows) > 0 {
		kinds := make([]string, 0, len(rows))
		for _, r := range rows {
			kinds = append(kinds, r.Kind)
		}
		t.Errorf("manifest row(s) %v were committed under synthetic inode %d despite the "+
			"409 — this is the orphan the guard exists to prevent", kinds, synthetic)
	}

	var c struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
		RealInode *int64 `json:"real_inode"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &c); err != nil {
		t.Fatalf("409 body is not JSON (%v) — with six distinct 409 conditions on this "+
			"route the consumer cannot pick a remedy without a discriminator; body=%s",
			err, rr.Body.String())
	}
	// retryable=FALSE deliberately. The branch is a pure function of req.Inode,
	// which comes from the request body, so an UNCHANGED retry lands here every
	// time — and an offline-created file holds a synthetic inode indefinitely.
	// Both remedies (re-resolve the inode; move the blob) change the request.
	// Marking it retryable made the consumer burn its whole budget on a request
	// whose outcome cannot change, and risked swallowing real_inode — the one
	// actionable field — inside a body its generic retry loop treats as transient.
	// real_inode must be PRESENT even when unresolved (0). It is documented to
	// the consumer as "real_inode":0, and `omitempty` on a uint64 would drop the
	// key entirely — the consumer already had a near-miss reading this as nil.
	if c.RealInode == nil {
		t.Error("real_inode key is absent from the 409 body — it is documented as present " +
			"with value 0 when unresolved; omitempty would silently drop it")
	}
	if c.Code != "synthetic_inode" || c.Retryable {
		t.Errorf("409 body = %+v, want code=synthetic_inode retryable=false", c)
	}
}

// A REAL inode must register normally even when it is LARGE.
//
// This is the anti-overfit case. Adversarial review showed the previous test was
// satisfied by the bogus predicate `req.Inode > 1<<40`, which would 409-loop
// forever on any legitimate inode above that threshold. Only bit 63 marks a
// synthetic inode; magnitude alone means nothing.
func TestRegisterLargeRealInodeStillWorks(t *testing.T) {
	tmp := t.TempDir()
	const relPath = "REEL_0065/B002C002.mov"
	const bigReal = uint64(1<<62 + 7) // huge, but bit 63 CLEAR => a real inode

	src := filepath.Join(tmp, relPath)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("y"), 2048), 0o644); err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1700000001, 0)
	_ = os.Chtimes(src, mt, mt)
	blobDir := filepath.Join(tmp, ".juicemount", "derivatives", itoa(bigReal))
	_ = os.MkdirAll(blobDir, 0o755)
	_ = os.WriteFile(filepath.Join(blobDir, "poster.jpg"), []byte("JPEGDATA"), 0o644)

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()
	mstore.InsertToCache(&metadata.Entry{
		Path: relPath, Name: "B002C002.mov", ParentPath: "REEL_0065",
		Inode: bigReal, Size: 2048, Mtime: mt,
	})

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	b, _ := json.Marshal(map[string]any{
		"inode": bigReal, "kind": "thumbnail", "producer": "on-device",
		"source_size": 2048, "source_mtime": mt.Unix(), "blob_rel_path": "poster.jpg",
	})
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
	if rr.Code != 200 {
		t.Fatalf("a LARGE but real inode (bit 63 clear) was rejected: status %d, body %s — "+
			"only bit 63 marks a synthetic inode; a magnitude test would 409-loop forever "+
			"on legitimate inodes", rr.Code, rr.Body.String())
	}
}

// An inode that is genuinely absent must still be 404, not 409 — collapsing
// every miss into "retryable" would make consumers retry forever on files that
// do not exist.
func TestRegisterUnknownRealInodeStays404(t *testing.T) {
	tmp := t.TempDir()
	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	b, _ := json.Marshal(map[string]any{
		"inode": 999404, "kind": "thumbnail", "producer": "on-device",
		"source_size": 1, "source_mtime": 1700000000, "blob_rel_path": "poster.jpg",
	})
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
	if rr.Code != 404 {
		t.Errorf("unknown real inode: status %d, want 404", rr.Code)
	}
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
