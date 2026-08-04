package main

// C5 regression suite — stale/wrong derivatives served after the source file
// changed.
//
// Derivatives (thumbnails, proxies, AI bundles) are keyed by INODE, and an
// inode is a location, not a content identity: overwrite a media file in place
// (re-export, re-transcode, an NLE round-trip) or let JuiceFS recycle a deleted
// inode, and every derivative row + cached blob under that key now depicts
// bytes that no longer exist. Before the fix the serve paths short-circuited on
// ds.Known(inode) and served whatever was indexed, so QuickLook and
// GET /blob?inode=N returned the OLD — or a FOREIGN file's — poster/proxy
// indefinitely, surviving restarts through the persistent thumb cache.
//
// The gate compares the row's own source_size/source_mtime vouch (stamped by
// internal/farm at generation, and already enforced on the AI contribute-back
// POST path) against the live size/mtime from the IN-RAM metadata mirror.
// TestBlobServeReadsMirrorNotTheBackend and TestBlobServeHappyPathZeroExtraSourceReads
// are the load-bearing perf guards: users run at ~500ms RTT, so this gate must
// never cost a backend/FUSE stat.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
)

const (
	freshInode  = uint64(990001)
	freshRel    = "Project_C5/clip.mov"
	freshBlobBy = "POSTER-BYTES-v1"
)

func i64p(v int64) *int64 { return &v }

// freshSeed is a self-contained control plane for the C5 tests: a real temp
// mount holding only the derivative BLOB, a metadata mirror entry describing
// the live source, a derivative row carrying (or omitting) the vouch, and a
// real persistent thumb cache.
//
// The source file itself is deliberately NOT created unless a test asks for it
// — that absence is a structural proof that the serve path never stats the
// source: if it did, every "fresh" case here would fail closed.
type freshSeed struct {
	mount string
	ds    *derivatives.Store
	store *metadata.Store
	tc    *thumbcache.Cache
	tcDir string
}

type freshOpts struct {
	kind        string     // derivative kind (default "thumbnail")
	status      string     // row status (default "ready")
	blobRel     string     // blob file name (default "poster.jpg")
	mirrorSize  int64      // live size the mirror reports
	mirrorMtime int64      // live mtime (unix) the mirror reports
	rowSize     *int64     // row vouch; nil == NULL column
	rowMtime    *int64     // row vouch; nil == NULL column
	noMirror    bool       // omit the mirror entry entirely
	srcOnDisk   []byte     // if non-nil, materialize the source file with these bytes
	srcMtime    *time.Time // if set, stamp the on-disk source with this mtime
}

func seedFreshness(t *testing.T, o freshOpts) (*freshSeed, func()) {
	t.Helper()
	if o.kind == "" {
		o.kind = "thumbnail"
	}
	if o.blobRel == "" {
		o.blobRel = "poster.jpg"
	}
	if o.status == "" {
		o.status = "ready"
	}
	mount := t.TempDir()

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	if !o.noMirror {
		mstore.InsertToCache(&metadata.Entry{
			Path: freshRel, Name: filepath.Base(freshRel), ParentPath: filepath.Dir(freshRel),
			Size: o.mirrorSize, Mtime: time.Unix(o.mirrorMtime, 0), Inode: freshInode,
		})
	}

	ds, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatalf("derivatives.Open: %v", err)
	}
	if err := ds.PutSource(freshInode, sp("c5hash")); err != nil {
		t.Fatalf("PutSource: %v", err)
	}
	mt := "image/jpeg"
	if o.kind == "proxy" {
		mt = "video/mp4"
	}
	if err := ds.PutDeriv(freshInode, derivatives.DerivRow{
		Kind: o.kind, Status: o.status, Producer: "linux-farm", Version: 1, Hash: sp("c5hash"),
		BlobRelPath: &o.blobRel, MediaType: &mt,
		SourceSize: o.rowSize, SourceMtime: o.rowMtime,
	}); err != nil {
		t.Fatalf("PutDeriv: %v", err)
	}

	// The derivative blob is real; the SOURCE is not (unless asked for).
	blobDir := filepath.Join(mount, derivatives.DerivDirRel(freshInode))
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatalf("mkdir blobdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(blobDir, o.blobRel), []byte(freshBlobBy), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	if o.srcOnDisk != nil {
		src := filepath.Join(mount, freshRel)
		if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
			t.Fatalf("mkdir src: %v", err)
		}
		if err := os.WriteFile(src, o.srcOnDisk, 0o644); err != nil {
			t.Fatalf("write src: %v", err)
		}
		if o.srcMtime != nil {
			if err := os.Chtimes(src, *o.srcMtime, *o.srcMtime); err != nil {
				t.Fatalf("chtimes src: %v", err)
			}
		}
	}

	tcDir := t.TempDir()
	tc, err := thumbcache.Open(tcDir, 1<<20)
	if err != nil {
		t.Fatalf("thumbcache.Open: %v", err)
	}

	globalMu.Lock()
	old := struct {
		store             *metadata.Store
		deriv             *derivatives.Store
		tc                *thumbcache.Cache
		mount, fuse, want string
	}{globalStore, globalDerivStore, globalThumbCache, globalMountPath, globalFUSEPath, globalWantMountPoint}
	globalStore = mstore
	globalDerivStore = ds
	globalThumbCache = tc
	globalMountPath = mount
	globalFUSEPath = mount
	globalWantMountPoint = mount
	globalMu.Unlock()

	return &freshSeed{mount: mount, ds: ds, store: mstore, tc: tc, tcDir: tcDir}, func() {
		globalMu.Lock()
		globalStore = old.store
		globalDerivStore = old.deriv
		globalThumbCache = old.tc
		globalMountPath = old.mount
		globalFUSEPath = old.fuse
		globalWantMountPoint = old.want
		globalMu.Unlock()
		tc.Close()
		ds.Close()
		mstore.Close()
	}
}

func getBlob(t *testing.T, kind string) *httptest.ResponseRecorder {
	t.Helper()
	target := "/blob?inode=990001"
	if kind != "" {
		target += "&kind=" + kind
	}
	rr := httptest.NewRecorder()
	handleBlobHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
	return rr
}

// withLiveSource swaps the gate's live-source accessor for a counting fake and
// returns the call counter + a restore func.
func withLiveSource(t *testing.T, fn func(uint64) liveSource) (*int, func()) {
	t.Helper()
	n := 0
	prev := liveSourceFor
	liveSourceFor = func(inode uint64) liveSource {
		n++
		return fn(inode)
	}
	return &n, func() { liveSourceFor = prev }
}

// --- the decision core -----------------------------------------------------

func TestDerivRowStaleDecisions(t *testing.T) {
	const (
		size  = int64(1240000000)
		mtime = int64(1750000000)
	)
	cases := []struct {
		name string
		row  derivatives.DerivRow
		live liveSource
		want bool
	}{
		{"unchanged source is fresh",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{size: size, mtime: mtime, ok: true}, false},
		{"size changed is stale",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{size: size + 1, mtime: mtime, ok: true}, true},
		{"both changed is stale",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{size: 4096, mtime: mtime + 9999, ok: true}, true},
		// MTIME IS NOT COMPARABLE ACROSS THE CLIENT/BACKEND BOUNDARY. The row's
		// vouch comes from an os.Stat on the BACKEND (farm stampSource); the
		// mirror's mtime for anything THIS client wrote is local wall-clock
		// time.Now() (onSpoolDrained / writeFile.Close → Store.UpdateSize), while
		// the drainer restores the file's REAL mtime onto the backend inode
		// (#103). They disagree BY CONSTRUCTION for every client-written file
		// until a reconcile heals the mirror — so comparing them condemned
		// perfectly valid derivatives, and rejection DELETES the local blob.
		{"mtime differs at identical size is FRESH (client/backend mtime skew)",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{size: size, mtime: mtime + 1, ok: true}, false},
		{"mtime differs wildly at identical size is still FRESH",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{size: size, mtime: 1, ok: true}, false},
		// DOCUMENTED BACK-COMPAT: rows written before the columns existed carry
		// no vouch. Nothing to compare — serve rather than blank every legacy
		// derivative on the volume.
		{"unvouched row (both NULL) is served",
			derivatives.DerivRow{},
			liveSource{size: 12345, mtime: 999, ok: true}, false},
		{"size-only vouch, size matches",
			derivatives.DerivRow{SourceSize: i64p(size)},
			liveSource{size: size, mtime: 1, ok: true}, false},
		{"size-only vouch, size differs",
			derivatives.DerivRow{SourceSize: i64p(size)},
			liveSource{size: size - 1, mtime: 1, ok: true}, true},
		// A row with ONLY an mtime vouch has nothing comparable at all.
		{"mtime-only vouch is served whatever the mtime says",
			derivatives.DerivRow{SourceMtime: i64p(mtime)},
			liveSource{size: 0, mtime: mtime + 1, ok: true}, false},
		// DOCUMENTED: the mirror is the only cheap oracle. When it has no entry
		// the alternative is a ~500ms backend stat on the serve path, which is a
		// worse failure than a stale image — so serve.
		{"mirror has no entry: serve rather than stall",
			derivatives.DerivRow{SourceSize: i64p(size), SourceMtime: i64p(mtime)},
			liveSource{}, false},
	}
	for _, c := range cases {
		if got := derivRowStale(c.row, c.live); got != c.want {
			t.Errorf("%s: derivRowStale = %v, want %v", c.name, got, c.want)
		}
	}
}

// --- GET /blob -------------------------------------------------------------

func TestBlobServeStaleSourceSizeIsMiss(t *testing.T) {
	// Row was generated against 1,240,000,000 bytes; the file was re-exported
	// in place and is now a different length under the SAME inode.
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 990000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (stale derivative must not be served); body = %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() == freshBlobBy {
		t.Fatal("served the stale blob bytes")
	}
}

// TestBlobServeMtimeOnlyMismatchIsServed is the S4 behavior fix.
//
// The gate used to 404 (and DELETE the cached blob) whenever the row's vouched
// mtime disagreed with the mirror's. But the two mtimes are not comparable: the
// vouch is an os.Stat of the file ON THE BACKEND (farm stampSource), while the
// mirror's mtime for any file THIS client wrote is local wall-clock time.Now()
// — nfs/handler.go onSpoolDrained and writeFile.Close both publish through
// Store.UpdateSize with `now`, and the drainer separately restores the file's
// REAL mtime onto the backend inode (#103). So they disagree by construction,
// and the gate condemned a perfectly good derivative for it.
func TestBlobServeMtimeOnlyMismatchIsServed(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750009999,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — an mtime-only disagreement is client/backend skew, not a "+
			"source change, and rejecting it DELETES a good thumbnail; body = %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != freshBlobBy {
		t.Fatalf("body = %q, want %q", rr.Body.String(), freshBlobBy)
	}
}

// TestClientWrittenFileMtimeSkewStillServesItsThumbnail is S4's workflow test —
// the exact sequence C5's own target case runs, end to end:
//
//	the user re-exports over clip.mov → the spool drains it, restoring the
//	client's real mtime onto the backend inode (#103) while publishing the
//	mirror's mtime as local wall-clock now → the farm re-derives against the
//	backend and stamps the row with the BACKEND mtime → QuickLook asks for the
//	poster.
//
// The row is FRESH (it was generated from exactly these bytes; the size agrees),
// but the two mtimes cannot agree until a reconcile heals the mirror. Pre-fix
// this 404'd AND deleted the cached poster, so macOS fell back to its own
// generator and read the full source over a 500 ms link — the precise latency
// regression the thumbnail plane exists to prevent.
func TestClientWrittenFileMtimeSkewStillServesItsThumbnail(t *testing.T) {
	const size = int64(1240000000)
	backendMtime := int64(1750000000) // the real mtime the drainer restored
	mirrorMtime := time.Now().Unix()  // what UpdateSize published locally
	if mirrorMtime == backendMtime {  // paranoia; they are ~decades apart
		t.Fatal("test setup: mtimes must differ")
	}

	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: size, mirrorMtime: mirrorMtime,
		rowSize: i64p(size), rowMtime: i64p(backendMtime),
	})
	defer restore()

	// The poster is already resident locally (warmed on the previous folder visit).
	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("GOOD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	cached, _ := seed.tc.Path(freshInode, "thumbnail")

	// /thumb-local's cache-hit path (the QuickLook appex surface).
	if _, _, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); !ok {
		t.Fatal("QuickLook 404'd a VALID poster on client/backend mtime skew — macOS now falls back to " +
			"its own generator and reads the whole source over the link")
	}
	if _, err := os.Stat(cached); err != nil {
		t.Fatalf("the valid cached poster was DELETED by the freshness gate: %v", err)
	}
	if n := seed.tc.Stats().Invalidations; n != 0 {
		t.Errorf("thumbcache Invalidations = %d, want 0 — a good blob was dropped", n)
	}

	// ...and the manifest resolution + GET /blob agree.
	if _, _, ok := resolveThumbBlobPath(freshInode); !ok {
		t.Error("resolveThumbBlobPath = miss for a client-written file (the folder-open warmer would skip it)")
	}
	if rr := getBlob(t, "thumbnail"); rr.Code != http.StatusOK {
		t.Errorf("GET /blob = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}
}

// TestBlobServeSizeChangeStillRejectsAfterTheMtimeRelaxation is the guard on the
// OTHER side of S4: size stays authoritative, so the actual C5 bug (an in-place
// re-export under the same inode) is still caught and the stale blob still
// dropped.
func TestBlobServeSizeChangeStillRejectsAfterTheMtimeRelaxation(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 990000000, mirrorMtime: 1750000000, // same mtime, different size
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if rr := getBlob(t, "thumbnail"); rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — size must remain authoritative; body = %s", rr.Code, rr.Body.String())
	}
}

// TestBlobServeFreshSourceIsServed is the common path — the one a regression
// here would break for every user.
func TestBlobServeFreshSourceIsServed(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != freshBlobBy {
		t.Fatalf("body = %q, want %q", rr.Body.String(), freshBlobBy)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Errorf("Content-Type = %q, want image/jpeg", ct)
	}
}

// TestBlobServeUnvouchedRowIsServed pins the documented back-compat choice for
// rows that predate source_size/source_mtime: they are served.
func TestBlobServeUnvouchedRowIsServed(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 7, mirrorMtime: 1, // wildly different from anything
		rowSize: nil, rowMtime: nil, // NULL columns
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (legacy unvouched rows stay servable); body = %s", rr.Code, rr.Body.String())
	}
}

// TestBlobServeUnmirroredInodeIsServed pins the documented choice when the
// mirror cannot answer: serve, never block on the network.
func TestBlobServeUnmirroredInodeIsServed(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		noMirror: true,
		rowSize:  i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (no mirror entry => serve, do not stat the backend); body = %s", rr.Code, rr.Body.String())
	}
}

// TestBlobServeProxyStaleIsMiss covers the byte-range kind: a proxy is served
// through the same manifest row, so the same gate must apply.
func TestBlobServeProxyStaleIsMiss(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		kind: "proxy", blobRel: "proxy.mp4",
		mirrorSize: 500, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	rr := getBlob(t, "proxy")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for a stale proxy; body = %s", rr.Code, rr.Body.String())
	}
}

// --- thumb-cache invalidation ---------------------------------------------

// TestBlobServeStaleInvalidatesThumbCache is the persistence half of the bug:
// dropping the index entry is not enough, because thumbcache.Open rebuilds its
// index by walking the directory — the FILE has to go, or the stale poster
// returns at the next launch.
func TestBlobServeStaleInvalidatesThumbCache(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	// A previously-warmed poster for this inode is resident locally.
	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("OLD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	cached, ok := seed.tc.Path(freshInode, "thumbnail")
	if !ok {
		t.Fatal("seeded thumb cache entry not resident")
	}

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", rr.Code, rr.Body.String())
	}
	if rr.Body.String() == "OLD-POSTER" {
		t.Fatal("served the stale poster from the local cache")
	}
	// In memory...
	if seed.tc.Has(freshInode, "thumbnail") {
		t.Error("stale thumb-cache entry still indexed after a stale serve")
	}
	// ...and on disk.
	if _, err := os.Stat(cached); !os.IsNotExist(err) {
		t.Errorf("stale thumb-cache FILE survived: stat err = %v", err)
	}
	if n := seed.tc.Stats().Invalidations; n != 1 {
		t.Errorf("thumbcache Invalidations = %d, want 1", n)
	}
	// The restart proof: a fresh Open over the same dir must not find it.
	seed.tc.Close()
	tc2, err := thumbcache.Open(seed.tcDir, 1<<20)
	if err != nil {
		t.Fatalf("reopen thumbcache: %v", err)
	}
	defer tc2.Close()
	if tc2.Has(freshInode, "thumbnail") {
		t.Error("stale poster came back after a thumb-cache reopen (restart)")
	}
}

// --- performance guards ----------------------------------------------------

// TestBlobServeReadsMirrorNotTheBackend discriminates the two possible
// implementations. The mirror says the source is 1,240,000,000 bytes (matching
// the row's vouch); the actual file ON DISK under the mount is 9 bytes. A
// mirror read => fresh => 200. A stat() of the source => size mismatch => 404.
// Only one of those adds a ~500ms round-trip on a cellular link.
func TestBlobServeReadsMirrorNotTheBackend(t *testing.T) {
	mt := time.Unix(1700000000, 0) // on-disk mtime also disagrees with the vouch
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
		srcOnDisk: []byte("9-bytes!!"), srcMtime: &mt,
	})
	defer restore()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — the freshness gate read the SOURCE FILE instead of the in-RAM mirror, "+
			"which puts a backend round-trip on every serve; body = %s", rr.Code, rr.Body.String())
	}
}

// TestBlobServeHappyPathZeroExtraSourceReads pins the gate's cost on the common
// path: EXACTLY ONE resolution of the live source, through the injectable
// mirror seam and nowhere else. Combined with the seed never materializing the
// source file (so any stat would fail closed and 404), this is the guard
// against a future change silently reintroducing a round-trip.
func TestBlobServeHappyPathZeroExtraSourceReads(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	// Structural proof: the source does not exist on disk at all.
	if _, err := os.Stat(filepath.Join(seed.mount, freshRel)); !os.IsNotExist(err) {
		t.Fatalf("test setup: source file must not exist; stat err = %v", err)
	}

	calls, unpatch := withLiveSource(t, func(uint64) liveSource {
		return liveSource{size: 1240000000, mtime: 1750000000, ok: true}
	})
	defer unpatch()

	rr := getBlob(t, "thumbnail")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rr.Code, rr.Body.String())
	}
	if *calls != 1 {
		t.Errorf("live-source resolutions per serve = %d, want exactly 1", *calls)
	}
}

// TestMirrorSourceIsARamRead pins that the production accessor answers from the
// metadata mirror — the values come back from the store even though no such
// file exists anywhere on disk.
func TestMirrorSourceIsARamRead(t *testing.T) {
	st, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	defer st.Close()
	st.InsertToCache(&metadata.Entry{
		Path: "a/b.mov", Name: "b.mov", ParentPath: "a",
		Size: 4242, Mtime: time.Unix(1751111111, 0), Inode: 4242,
	})

	got := mirrorSource(st, 4242)
	if !got.ok || got.size != 4242 || got.mtime != 1751111111 {
		t.Fatalf("mirrorSource = %+v, want {4242 1751111111 true}", got)
	}
	if miss := mirrorSource(st, 777); miss.ok {
		t.Errorf("mirrorSource(unmirrored) = %+v, want ok=false", miss)
	}
	if nilStore := mirrorSource(nil, 4242); nilStore.ok {
		t.Errorf("mirrorSource(nil store) = %+v, want ok=false", nilStore)
	}
}

// --- resolveThumbBlobPath (the warmer + /thumb-local read-through) ----------

func TestResolveThumbBlobPathStaleIsMiss(t *testing.T) {
	_, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, p, ok := resolveThumbBlobPath(freshInode); ok {
		t.Fatalf("resolveThumbBlobPath = (%q, true) for a stale row, want a miss "+
			"(otherwise the folder-open warmer re-hydrates the wrong poster)", p)
	}
}

func TestResolveThumbBlobPathFreshResolves(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	gotMount, p, ok := resolveThumbBlobPath(freshInode)
	if !ok {
		t.Fatal("resolveThumbBlobPath = miss for an unchanged source")
	}
	// Root and relative path are returned SEPARATELY on purpose: a joined
	// absolute path can only be opened with O_NOFOLLOW on its last component,
	// which leaves the per-inode directory swappable for a symlink. Assert the
	// split survives — collapsing it back into one string is the regression.
	if gotMount != seed.mount {
		t.Errorf("mount = %q, want %q", gotMount, seed.mount)
	}
	want := derivatives.DerivBlobRel(freshInode, "poster.jpg")
	if p != want {
		t.Fatalf("blob rel = %q, want %q", p, want)
	}
	if filepath.IsAbs(p) {
		t.Errorf("blob rel %q is absolute — it must stay relative to the mount", p)
	}
}

// TestResolveThumbBlobPathStaleInvalidatesCache: the warmer skips an inode
// whose blob is already cached (cache.Has), so the stale copy must be evicted
// here or it is never re-derived.
func TestResolveThumbBlobPathStaleInvalidatesCache(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("OLD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	if _, _, ok := resolveThumbBlobPath(freshInode); ok {
		t.Fatal("stale row resolved")
	}
	if seed.tc.Has(freshInode, "thumbnail") {
		t.Error("stale thumb-cache entry survived resolveThumbBlobPath")
	}
}

// --- /thumb-local cache-hit gate ------------------------------------------

// The QuickLook appex path reads the persistent cache BEFORE any manifest
// lookup, so it is where a stale poster would be served straight off local disk
// without ever reaching resolveThumbBlobPath.
func TestFreshThumbCachePathStaleIsMiss(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("OLD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	cached, _ := seed.tc.Path(freshInode, "thumbnail")

	if _, p, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); ok {
		t.Fatalf("freshThumbCachePath = (%q, true) for a stale row — QuickLook would draw the wrong poster", p)
	}
	if seed.tc.Has(freshInode, "thumbnail") {
		t.Error("stale entry still indexed")
	}
	if _, err := os.Stat(cached); !os.IsNotExist(err) {
		t.Errorf("stale cache FILE survived: stat err = %v", err)
	}
}

func TestFreshThumbCachePathFreshHits(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 1240000000, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("GOOD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	root, p, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode)
	if !ok {
		t.Fatal("freshThumbCachePath = miss for an unchanged source (would blank every thumbnail)")
	}
	// (root, rel), not a joined path — the serve side opens it with the anchored
	// walk, so a provider collapsing this back into one string is the regression.
	if filepath.IsAbs(p) {
		t.Errorf("cache rel %q is absolute — it must stay relative to the cache root", p)
	}
	body, err := os.ReadFile(filepath.Join(root, p))
	if err != nil || string(body) != "GOOD-POSTER" {
		t.Fatalf("cached body = %q (err %v), want GOOD-POSTER", body, err)
	}
}

// TestFreshThumbCachePathMissSkipsTheGate pins the cost discipline: a cache
// MISS returns before touching the derivative index or resolving the live
// source, exactly as before the fix.
func TestFreshThumbCachePathMissSkipsTheGate(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	calls, unpatch := withLiveSource(t, func(uint64) liveSource { return liveSource{} })
	defer unpatch()

	if _, _, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); ok {
		t.Fatal("empty cache reported a hit")
	}
	if *calls != 0 {
		t.Errorf("live-source resolutions on a cache miss = %d, want 0", *calls)
	}
}

// TestFreshThumbCachePathIgnoresNonReadyRowVouch is N1.
//
// freshThumbCachePath judged the FIRST thumbnail row of ANY status and then
// broke, unlike resolveThumbBlobPath which filters Status == "ready". That is an
// unsound inference: only a READY row describes bytes that were actually
// produced, so only a READY row can vouch for what is sitting in the local
// cache. A failed (or, in future, pending) re-derive row carries the vouch for
// the CURRENT source, which the old code then used to VALIDATE a blob generated
// from entirely different bytes — or, when the non-ready row's vouch disagreed,
// to DELETE a cached poster the row says nothing about.
//
// This pins the deletion half, because it is the one that costs the user a full
// source read over the link. With no ready row the cached blob is unvouched, and
// unvouched means served — the same documented choice as
// TestFreshThumbCachePathNoRowIsServed.
func TestFreshThumbCachePathIgnoresNonReadyRowVouch(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		status:     "failed", // the farm's re-derive attempt did not produce a blob
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("CACHED-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	cached, _ := seed.tc.Path(freshInode, "thumbnail")

	if _, _, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); !ok {
		t.Fatal("N1: a NON-READY row's vouch was used to judge the cached blob — the row describes a " +
			"derivation that produced nothing, so it can neither validate nor condemn what is cached")
	}
	if _, err := os.Stat(cached); err != nil {
		t.Fatalf("N1: the cached poster was DELETED on a non-ready row's vouch: %v", err)
	}
}

// TestFreshThumbCachePathStillJudgesTheReadyRow is N1's other side: filtering by
// status must not weaken the gate for the row that DOES vouch for the cached
// bytes.
func TestFreshThumbCachePathStillJudgesTheReadyRow(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		mirrorSize: 111, mirrorMtime: 1750000000, // size disagrees with the vouch
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("OLD-POSTER")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	if _, _, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); ok {
		t.Fatal("a READY row whose vouched SIZE disagrees must still reject")
	}
}

// TestFreshThumbCachePathNoRowIsServed pins the documented choice for a cache
// entry with no thumbnail row in the index (reconcile pending, index rebuilt):
// there is no vouch to judge it by, so it is served.
func TestFreshThumbCachePathNoRowIsServed(t *testing.T) {
	seed, restore := seedFreshness(t, freshOpts{
		kind: "waveform", blobRel: "wave.json",
		mirrorSize: 111, mirrorMtime: 1750000000,
		rowSize: i64p(1240000000), rowMtime: i64p(1750000000),
	})
	defer restore()

	if _, err := seed.tc.Put(freshInode, "thumbnail", strings.NewReader("ORPHAN")); err != nil {
		t.Fatalf("seed thumb cache: %v", err)
	}
	if _, _, ok := freshThumbCachePath(seed.ds, seed.tc, freshInode); !ok {
		t.Fatal("cache entry with no matching manifest row was withheld; want served")
	}
}
