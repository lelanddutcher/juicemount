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

// resolveFixture builds the state a file with a SYNTHETIC inode is actually in:
// the mirror's path entry has been reconciled to the real inode, while the
// consumer still holds (and wrote its blob under) the synthetic one.
type resolveFixture struct {
	tmp                 string
	ms                  *metadata.Store
	ds                  *derivatives.Store
	synthetic, real     uint64
	relPath, blobName   string
	mtime               time.Time
	sourceSize          int
	syntheticDir, realD string
}

func newResolveFixture(t *testing.T, relPath string, synthetic, real uint64) *resolveFixture {
	t.Helper()
	f := &resolveFixture{
		tmp: t.TempDir(), synthetic: synthetic, real: real,
		relPath: relPath, blobName: "poster.jpg",
		mtime: time.Unix(1700002000, 0), sourceSize: 4096,
	}
	src := filepath.Join(f.tmp, relPath)
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, bytes.Repeat([]byte("s"), f.sourceSize), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(src, f.mtime, f.mtime)

	var err error
	if f.ms, err = metadata.Open(":memory:"); err != nil {
		t.Fatal(err)
	}
	if f.ds, err = derivatives.Open(":memory:"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.ms.Close(); f.ds.Close() })

	// The mirror resolves the PATH to the real inode (post-reconcile), and the
	// synthetic handle still remembers the path it was minted for.
	f.ms.InsertToCache(&metadata.Entry{
		Path: relPath, Name: filepath.Base(relPath), ParentPath: filepath.Dir(relPath),
		Inode: real, Size: int64(f.sourceSize), Mtime: f.mtime,
	})
	f.ms.RecordSyntheticHandle(synthetic, relPath)

	f.syntheticDir = filepath.Join(f.tmp, ".juicemount", "derivatives", itoa(synthetic))
	f.realD = filepath.Join(f.tmp, ".juicemount", "derivatives", itoa(real))

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = f.ms, f.ds, f.tmp
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	})
	return f
}

func (f *resolveFixture) writeConsumerBlob(t *testing.T, data []byte) {
	t.Helper()
	writeBlob(t, f.tmp, f.synthetic, f.blobName, data)
}

func (f *resolveFixture) post(t *testing.T, extra map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body := regBody(f.synthetic, "thumbnail", f.sourceSize, f.mtime.Unix())
	for k, v := range extra {
		body[k] = v
	}
	b, _ := json.Marshal(body)
	rr := httptest.NewRecorder()
	handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
	return rr
}

// The happy path: a resolvable synthetic inode registers, the row lands under
// the REAL inode, and the bytes end up there too.
func TestResolveSyntheticRegistersUnderRealInode(t *testing.T) {
	f := newResolveFixture(t, "REEL/offline.mov", 1<<63+501, 1_700_001)
	blob := []byte("JPEG-from-the-consumer")
	f.writeConsumerBlob(t, blob)

	rr := f.post(t, nil)
	if rr.Code != 200 {
		t.Fatalf("status %d, want 200 — an offline-created file must be able to contribute; body %s",
			rr.Code, rr.Body.String())
	}
	rows, err := f.ds.Manifest(f.real)
	if err != nil || len(rows) != 1 {
		t.Fatalf("expected one row under the real inode, got %v (err %v)", rows, err)
	}
	if orphan, _ := f.ds.Manifest(f.synthetic); len(orphan) != 0 {
		t.Errorf("a row was left under the synthetic inode")
	}
	got, err := os.ReadFile(filepath.Join(f.realD, f.blobName))
	if err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("blob not present/intact under the real inode: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.syntheticDir, f.blobName)); err == nil {
		t.Error("the pre-resolution copy was not dropped after the row committed")
	}
	var resp struct {
		Inode uint64 `json:"inode"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Inode != f.real {
		t.Errorf("response inode = %d, want the resolved %d", resp.Inode, f.real)
	}
}

// THE CASE THAT REVERTED THE FIRST ATTEMPT. A rejection that happens AFTER the
// point where relocation used to occur must leave the consumer's bytes exactly
// where it wrote them, so it can act on the remedy it was given and retry.
//
// The first implementation renamed the blob before these gates ran: the request
// was then rejected, the bytes were gone from under the consumer, and — because
// the move refused a non-empty destination — every subsequent attempt failed
// permanently. The rejection destroyed the caller's ability to comply with its
// own remedy.
func TestRejectionAfterResolveLeavesConsumerBytesInPlace(t *testing.T) {
	f := newResolveFixture(t, "REEL/offline2.mov", 1<<63+502, 1_700_002)
	blob := []byte("JPEG-that-must-not-move")
	f.writeConsumerBlob(t, blob)

	// A vouch mismatch — rejected well past where the old code had already moved
	// the bytes.
	rr := f.post(t, map[string]any{"source_size": 999999})
	if rr.Code != 409 {
		t.Fatalf("status %d, want 409 source_stale; body %s", rr.Code, rr.Body.String())
	}

	got, err := os.ReadFile(filepath.Join(f.syntheticDir, f.blobName))
	if err != nil || !bytes.Equal(got, blob) {
		t.Fatalf("THE CONSUMER'S BYTES MOVED (or vanished) on a REJECTED request: %v. "+
			"It was told to recompute and register again, which it cannot do if its blob "+
			"is no longer where it wrote it.", err)
	}
	if _, err := os.Stat(filepath.Join(f.realD, f.blobName)); err == nil {
		t.Error("a blob was left under the real inode with no row — that stranded copy " +
			"then blocks every future attempt")
	}
	if rows, _ := f.ds.Manifest(f.real); len(rows) != 0 {
		t.Errorf("a row was committed despite the rejection: %v", rows)
	}

	// And the retry, done exactly as instructed, must now succeed.
	if rr2 := f.post(t, nil); rr2.Code != 200 {
		t.Fatalf("the corrected retry failed with %d (%s) — the first rejection left the "+
			"consumer unable to comply with its own remedy", rr2.Code, rr2.Body.String())
	}
}

// What the resolution path DOES protect: a vouch that no longer matches the file
// the path resolves to is refused, and the consumer's bytes are left alone.
//
// This deliberately does NOT claim to close the identity hole. If the file at
// the remembered path is replaced by a different file with IDENTICAL size and
// mtime, the vouch passes and the wrong derivative is published — see the note
// in cbridge.go. A test asserting otherwise would be claiming a guarantee the
// code does not deliver, which is worse than the gap itself.
func TestResolveRefusesWhenTheVouchNoLongerMatches(t *testing.T) {
	f := newResolveFixture(t, "REEL/fileA.mov", 1<<63+503, 1_700_003)
	f.writeConsumerBlob(t, []byte("POSTER-OF-FILE-A"))

	// The file at the remembered path is replaced with different CONTENT LENGTH
	// — the detectable case.
	src := filepath.Join(f.tmp, "REEL/fileA.mov")
	if err := os.WriteFile(src, bytes.Repeat([]byte("x"), f.sourceSize*2), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(src, f.mtime, f.mtime); err != nil {
		t.Fatal(err)
	}

	rr := f.post(t, nil)
	if rr.Code == 200 {
		t.Fatalf("registered against a source whose size no longer matches the vouch — "+
			"the derivative would describe bytes that are gone; body %s", rr.Body.String())
	}
	if rows, _ := f.ds.Manifest(f.real); len(rows) != 0 {
		t.Errorf("a row landed despite the vouch mismatch: %v", rows)
	}
	if _, err := os.Stat(filepath.Join(f.syntheticDir, f.blobName)); err != nil {
		t.Errorf("the consumer's blob was moved on a refused request: %v", err)
	}
}

// An UNRESOLVABLE synthetic inode still 409s, unchanged — the honest failure
// must stay honest.
func TestUnresolvableSyntheticStill409s(t *testing.T) {
	f := newResolveFixture(t, "REEL/offline4.mov", 1<<63+504, 1_700_004)
	f.writeConsumerBlob(t, []byte("JPEG"))
	// No path resolution: drop the mirror entry the synthetic handle points at.
	f.ms.InsertToCache(&metadata.Entry{
		Path: "REEL/offline4.mov", Name: "offline4.mov", ParentPath: "REEL",
		Inode: 1<<63 + 999, Size: int64(f.sourceSize), Mtime: f.mtime, // still synthetic
	})
	rr := f.post(t, nil)
	if rr.Code != 409 {
		t.Fatalf("status %d, want 409 for an unresolvable synthetic inode; body %s",
			rr.Code, rr.Body.String())
	}
	var c registerConflict
	_ = json.Unmarshal(rr.Body.Bytes(), &c)
	if c.Code != "synthetic_inode" {
		t.Errorf("code = %q, want synthetic_inode", c.Code)
	}
}
