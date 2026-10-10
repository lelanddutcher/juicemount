//go:build consumer_acceptance

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/testguard"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type consumerCacheSlot struct {
	Inode       uint64 `json:"inode"`
	SourceHash  string `json:"sourceHash"`
	SourceSize  int64  `json:"sourceSize"`
	SourceMtime int64  `json:"sourceMtime"`
	DisplayPath string `json:"displayPath"`
	JPEGSHA256  string `json:"jpegSHA256"`
	JPEGSize    int64  `json:"jpegSize"`
}

type consumerCacheFixture struct {
	Hot   consumerCacheSlot `json:"hot"`
	Stale consumerCacheSlot `json:"stale"`
	Cold  consumerCacheSlot `json:"cold"`
}

type consumerCacheStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *consumerCacheStatusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *consumerCacheStatusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func consumerCachePath(root string) string {
	return filepath.Join(root, "state", "thumbnail-fixture.json")
}

// The parent joins each original test process before hiding the owned backing
// directory. No process inherits a live store, cache index, or provider global.
func TestConsumerThumbnailCacheProcessRestart(t *testing.T) {
	role := os.Getenv("JM_CONSUMER_CACHE_ROLE")
	if role == "warm" || role == "restart" {
		consumerCacheGeneration(t, role)
		fmt.Printf("QUIESCENT cache-%s\n", role) // generation defers have joined HTTP, owner, and stores.
		return
	}
	if role != "" {
		t.Fatalf("unexpected consumer cache child role %q", role)
	}
	probe := consumerMediaPinned(t, "JM_CONSUMER_PROBE")
	source := consumerMediaPinned(t, "JM_CONSUMER_MEDIA_SOURCE")
	if fi, err := os.Stat(source); err != nil || !fi.Mode().IsRegular() || fi.Size() != 223153 {
		t.Fatalf("retained real source: info=%v err=%v", fi, err)
	}
	_ = consumerMediaPinned(t, "JM_CONSUMER_FFMPEG")
	_ = consumerMediaPinned(t, "JM_CONSUMER_FFPROBE")
	root := t.TempDir()
	consumerCacheGoChild(t, "warm", root)
	if _, err := os.Stat(consumerCachePath(root)); err != nil {
		t.Fatalf("warm child did not finish its durable descriptor: %v", err)
	}
	backing := filepath.Join(root, "backing")
	if err := os.Rename(backing, filepath.Join(root, "backing-unavailable")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(backing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old backing path remains available: %v", err)
	}
	consumerCacheGoChild(t, "restart", root)
	// A third independent index open proves the stale file did not merely leave
	// the restart child's RAM map; the disk record itself is absent.
	fixture := consumerCacheRead(t, consumerCachePath(root))
	if b, err := json.Marshal(fixture); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("completed three-slot source/JPEG descriptor: %s", b)
	}
	index, err := thumbcache.Open(filepath.Join(root, "state", "thumbs"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !index.Has(fixture.Hot.Inode, "thumbnail") || index.Has(fixture.Stale.Inode, "thumbnail") || index.Has(fixture.Cold.Inode, "thumbnail") {
		t.Errorf("post-join disk index lost hot or retained stale/cold: %+v", index.Stats())
	}
	if err := index.Close(); err != nil {
		t.Error(err)
	}
	staleFile := filepath.Join(root, "state", "thumbs", fmt.Sprintf("%02x", byte(fixture.Stale.Inode)), fmt.Sprintf("%d_thumbnail", fixture.Stale.Inode))
	if _, err := os.Lstat(staleFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale canonical cache file survived: %v", err)
	}
	t.Logf("actual production client=%s, independent warm/restart provider processes joined; backing path absent", probe)
}

func consumerCacheGoChild(t *testing.T, role, root string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(self, "-test.run=^TestConsumerThumbnailCacheProcessRestart$", "-test.timeout=170s", "-test.v")
	cmd.WaitDelay = 2 * time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // private process group also owns nested Swift child.
	cmd.Env = append(os.Environ(), "JM_CONSUMER_CACHE_ROLE="+role, "JM_CONSUMER_CACHE_ROOT="+root)
	if role == "restart" {
		cmd.Env = append(cmd.Env, testguard.RefuseFUSEContentReadsEnv+"=1")
	} else {
		cmd.Env = append(cmd.Env, testguard.RefuseFUSEContentReadsEnv+"=0")
	}
	var output consumerMediaLog
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var waitErr error
	timedOut, forced := false, false
	select {
	case waitErr = <-done:
	case <-time.After(150 * time.Second):
		timedOut = true
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case waitErr = <-done:
		case <-time.After(10 * time.Second):
			forced = true
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			waitErr = <-done
		}
	}
	log, overflow := output.Snapshot()
	hash := sha256.Sum256([]byte(log))
	t.Logf("cache child role=%s pid=%d original_wait=%v timed_out=%v forced_kill=%v log_overflow=%v log_bytes=%d log_sha256=%x\n%s", role, cmd.Process.Pid, waitErr, timedOut, forced, overflow, len(log), hash, log)
	if forced || waitErr != nil || !strings.Contains(log, "QUIESCENT cache-"+role+"\n") {
		// A forced exit, panic, or test timeout cannot vouch for independent
		// farm tool groups. Retain the owned root without TempDir cleanup.
		fmt.Fprintf(os.Stderr, "INCOMPLETE descendant custody: cache child role=%s root=%s pid=%d\n", role, root, cmd.Process.Pid)
		os.Exit(2)
	}
	if waitErr != nil || timedOut || forced || overflow || !strings.Contains(log, "PASS cache-"+role+":") {
		t.Fatalf("consumer cache child %s failed: wait=%v timeout=%v forced=%v overflow=%v", role, waitErr, timedOut, forced, overflow)
	}
}

func consumerCacheSwiftChild(t *testing.T, ctx context.Context, probe, base, mode, fixture string) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("cache child interrupted before Swift launch: %v", err)
	}
	cmd := exec.Command(probe, base, mode)
	cmd.WaitDelay = 2 * time.Second
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), "JM_CONSUMER_MEDIA_FIXTURE="+fixture)
	var output consumerMediaLog
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var waitErr error
	interrupted, forced := false, false
	select {
	case waitErr = <-done:
	case <-ctx.Done():
		interrupted = true
	case <-time.After(30 * time.Second):
		interrupted = true
	}
	if interrupted {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case waitErr = <-done:
		case <-time.After(3 * time.Second):
			forced = true
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			waitErr = <-done
		}
	}
	log, overflow := output.Snapshot()
	hash := sha256.Sum256([]byte(log))
	t.Logf("thumbnail Swift pid=%d mode=%s original_wait=%v interrupted=%v forced=%v overflow=%v bytes=%d sha256=%x\n%s", cmd.Process.Pid, mode, waitErr, interrupted, forced, overflow, len(log), hash, log)
	if waitErr != nil || interrupted || forced || overflow || !strings.Contains(log, "PASS "+mode+":") {
		t.Fatalf("thumbnail Swift child failed: mode=%s wait=%v interrupted=%v forced=%v overflow=%v", mode, waitErr, interrupted, forced, overflow)
	}
}

func consumerCacheSave(t *testing.T, path string, fixture consumerCacheFixture) {
	t.Helper()
	b, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write(b); err != nil || n != len(b) {
		_ = f.Close()
		t.Fatalf("descriptor write %d/%d: %v", n, len(b), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func consumerCacheRead(t *testing.T, path string) consumerCacheFixture {
	t.Helper()
	var f consumerCacheFixture
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func consumerCacheGeneration(t *testing.T, role string) {
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	root := os.Getenv("JM_CONSUMER_CACHE_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("cache child requires owned absolute root")
	}
	probe := consumerMediaPinned(t, "JM_CONSUMER_PROBE")
	source := consumerMediaPinned(t, "JM_CONSUMER_MEDIA_SOURCE")
	if fi, err := os.Stat(source); err != nil || !fi.Mode().IsRegular() || fi.Size() != 223153 {
		t.Fatalf("retained real source: info=%v err=%v", fi, err)
	}
	ffmpeg := consumerMediaPinned(t, "JM_CONSUMER_FFMPEG")
	ffprobe := consumerMediaPinned(t, "JM_CONSUMER_FFPROBE")
	state := filepath.Join(root, "state")
	backing := filepath.Join(root, "backing")
	if role == "warm" {
		for _, p := range []string{state, filepath.Join(backing, "Project_Foo"), filepath.Join(state, "scratch")} {
			if err := os.MkdirAll(p, 0o700); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		if _, err := os.Lstat(backing); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("backing exists on restart: %v", err)
		}
		if os.Getenv(testguard.RefuseFUSEContentReadsEnv) != "1" {
			t.Fatal("restart content guard absent")
		}
	}
	metaPath := filepath.Join(state, "metadata.db")
	mirror, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := mirror.Close(); err != nil {
			t.Error(err)
		}
	}()
	provider, err := derivatives.Open(derivStorePath(metaPath))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := provider.Close(); err != nil {
			t.Error(err)
		}
	}()
	thumbDir := filepath.Join(state, "thumbs")
	thumbs, err := thumbcache.Open(thumbDir, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := thumbs.Close(); err != nil {
			t.Error(err)
		}
	}()

	const logicalURL = "redis://127.0.0.1:1/3"
	selection := []string{"consumer-cache-acceptance", "", ""}
	const uuid = "12345678-1234-abcd-5678-123456789abc"
	backend, err := cache.BackendIdentity(logicalURL, selection...)
	if err != nil {
		t.Fatal(err)
	}
	binding := cache.VolumeBinding{Version: 1, Backend: backend, UUID: uuid, BlockSize: 1 << 20}
	if role == "warm" {
		if err := cache.SaveVolumeBinding(mirror, binding); err != nil {
			t.Fatal(err)
		}
	}
	cacheDir := filepath.Join(root, "cache", uuid)
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_CACHE_DIR", cacheDir) // owned child only; HOME is untouched.
	opts, err := redis.ParseURL(logicalURL)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newBridgeCacheOwner(mirror, logicalURL, selection, opts, state, "")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.stop()
	if got, ok := owner.session.SnapshotBinding(); !ok || got != binding || owner.session.Qualified() || owner.session.Reader() != nil || !errors.Is(owner.session.ReaderError(), cache.ErrReaderUnavailable) {
		t.Fatalf("cold cache session binding/qualification: binding=%+v present=%v qualified=%v reader=%v err=%v", got, ok, owner.session.Qualified(), owner.session.Reader(), owner.session.ReaderError())
	}
	if foreign, err := newBridgeCacheOwner(mirror, logicalURL, []string{"foreign", "", ""}, opts, state, ""); !errors.Is(err, cache.ErrVolumeBinding) {
		if foreign != nil && foreign.session != nil {
			foreign.stop()
		}
		t.Fatalf("changed selector was admitted: %v", err)
	}

	var fixture consumerCacheFixture
	if role == "warm" {
		fixture = consumerCacheWarm(t, ctx, root, source, ffmpeg, ffprobe, mirror, provider)
		consumerCacheSave(t, consumerCachePath(root), fixture)
	} else {
		fixture = consumerCacheRead(t, consumerCachePath(root))
		for _, slot := range []consumerCacheSlot{fixture.Hot, fixture.Stale, fixture.Cold} {
			entry := mirror.LookupByPath(strings.TrimPrefix(slot.DisplayPath, "/Volumes/zpool/"))
			byInode := mirror.LookupByInode(slot.Inode)
			known, hash := provider.Known(slot.Inode)
			if entry == nil || entry.Inode != slot.Inode || byInode == nil || byInode.Path != entry.Path || !known || hash == nil || *hash != slot.SourceHash {
				t.Fatalf("persisted mirror/derivative absent for inode %d: entry=%v byInode=%v known=%v hash=%v", slot.Inode, entry, byInode, known, hash)
			}
			rows, err := provider.Manifest(slot.Inode)
			if err != nil {
				t.Fatal(err)
			}
			ready := false
			for _, row := range rows {
				if row.Kind == "thumbnail" && row.Status == "ready" && row.SourceSize != nil && *row.SourceSize == slot.SourceSize {
					ready = true
				}
			}
			if !ready {
				t.Fatalf("persisted ready thumbnail row absent for inode %d", slot.Inode)
			}
			wantSize := slot.SourceSize
			if slot.Inode == fixture.Stale.Inode {
				wantSize++
			}
			if entry.Size != wantSize {
				t.Fatalf("persisted size inode=%d got=%d want=%d", slot.Inode, entry.Size, wantSize)
			}
		}
		if !thumbs.Has(fixture.Hot.Inode, "thumbnail") || !thumbs.Has(fixture.Stale.Inode, "thumbnail") || thumbs.Has(fixture.Cold.Inode, "thumbnail") || thumbs.Stats().Puts != 0 {
			t.Fatalf("new cache index lost hot/stale or prewarmed cold: %+v", thumbs.Stats())
		}
	}

	globalMu.Lock()
	oldStore, oldProvider, oldThumb := globalStore, globalDerivStore, globalThumbCache
	oldFUSE, oldMount, oldWant, oldName := globalFUSEPath, globalMountPath, globalWantMountPoint, globalVolumeName
	oldCaps, oldMetrics := globalCapabilities, globalMetricsAddr
	globalStore, globalDerivStore, globalThumbCache = mirror, provider, thumbs
	globalFUSEPath, globalMountPath, globalWantMountPoint, globalVolumeName = backing, "/Volumes/zpool", "/Volumes/zpool", "zpool"
	globalCapabilities = cplane.DeriveCapabilities(productionGUIServedRoutes())
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalThumbCache = oldStore, oldProvider, oldThumb
		globalFUSEPath, globalMountPath, globalWantMountPoint, globalVolumeName = oldFUSE, oldMount, oldWant, oldName
		globalCapabilities, globalMetricsAddr = oldCaps, oldMetrics
		globalMu.Unlock()
	}()
	type observed struct {
		inode                                                          uint64
		status                                                         int
		reason, kind, hash, sourceSize, sourceMtime, mediaType, length string
	}
	var mu sync.Mutex
	var requests []observed
	responseDone := make(chan struct{}, 8)
	s := metrics.NewServer("127.0.0.1:0", metrics.NewRegistry())
	s.ExtraRoutes = map[string]http.HandlerFunc{
		"/whoami": handleWhoamiHTTP, "/lookup": handleLookupHTTP, "/derivatives": handleDerivativesHTTP,
		"/blob": func(w http.ResponseWriter, r *http.Request) {
			capture := &consumerCacheStatusWriter{ResponseWriter: w}
			handleBlobHTTP(capture, r)
			h := w.Header()
			mu.Lock()
			requests = append(requests, observed{inode: cacheInodeQuery(r), status: capture.status, reason: h.Get("X-JM-Blob-Miss"), kind: h.Get("X-JM-Kind"), hash: h.Get("X-JM-Hash"), sourceSize: h.Get("X-JM-Source-Size"), sourceMtime: h.Get("X-JM-Source-Mtime"), mediaType: h.Get("Content-Type"), length: h.Get("Content-Length")})
			mu.Unlock()
			select {
			case responseDone <- struct{}{}:
			default: // the exact response-count check also rejects extra requests.
			}
		},
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop() // HTTP retired before owner/cache/stores by LIFO.
	globalMu.Lock()
	globalMetricsAddr = s.Addr()
	globalMu.Unlock()
	base := "http://" + s.Addr()
	run := func(name, mode string, wantStatus int, wantReason string) {
		t.Setenv("JM_CONSUMER_CACHE_SLOT", name)
		var wantInode uint64
		switch name {
		case "hot":
			wantInode = fixture.Hot.Inode
		case "stale":
			wantInode = fixture.Stale.Inode
		case "cold":
			wantInode = fixture.Cold.Inode
		}
		mu.Lock()
		before := len(requests)
		mu.Unlock()
		consumerCacheSwiftChild(t, ctx, probe, base, mode, consumerCachePath(root))
		select {
		case <-responseDone:
		case <-time.After(2 * time.Second):
			t.Fatal("blob response completed in the client without a joined provider observation")
		}
		mu.Lock()
		got := append([]observed(nil), requests[before:]...)
		mu.Unlock()
		if len(got) != 1 || got[0].inode != wantInode || got[0].status != wantStatus || got[0].reason != wantReason {
			t.Fatalf("%s blob responses=%+v want status=%d reason=%q", name, got, wantStatus, wantReason)
		}
		if wantStatus == http.StatusOK {
			var slot consumerCacheSlot
			switch name {
			case "hot":
				slot = fixture.Hot
			case "stale":
				slot = fixture.Stale
			case "cold":
				slot = fixture.Cold
			}
			v := got[0]
			if v.kind != "thumbnail" || v.hash != slot.SourceHash || v.sourceSize != strconv.FormatInt(slot.SourceSize, 10) || v.sourceMtime != strconv.FormatInt(slot.SourceMtime, 10) || v.mediaType != "image/jpeg" || v.length != strconv.FormatInt(slot.JPEGSize, 10) {
				t.Fatalf("%s successful blob lost source-vouch/media/length headers: %+v", name, v)
			}
		}
	}
	if role == "warm" {
		for _, name := range []string{"hot", "stale"} {
			run(name, "thumbnail-positive", 200, "")
		}
		// A real on-miss /derivatives call makes cold known, but no /blob reads it.
		q := httptest.NewRecorder()
		handleDerivativesHTTP(q, httptest.NewRequest("GET", fmt.Sprintf("/derivatives?inode=%d", fixture.Cold.Inode), nil))
		if q.Code != 200 {
			t.Fatalf("cold derivative reconciliation: %d %s", q.Code, q.Body.String())
		}
		for _, slot := range []consumerCacheSlot{fixture.Hot, fixture.Stale, fixture.Cold} {
			if known, _ := provider.Known(slot.Inode); !known {
				t.Fatalf("provider failed sidecar reconciliation for %d", slot.Inode)
			}
		}
		if !thumbs.Has(fixture.Hot.Inode, "thumbnail") || !thumbs.Has(fixture.Stale.Inode, "thumbnail") || thumbs.Has(fixture.Cold.Inode, "thumbnail") || thumbs.Stats().Puts != 2 {
			t.Fatalf("warm cache shape: %+v", thumbs.Stats())
		}
		stale := mirror.LookupByPath(strings.TrimPrefix(fixture.Stale.DisplayPath, "/Volumes/zpool/"))
		if stale == nil {
			t.Fatal("stale mirror entry absent before size mutation")
		}
		changed := *stale
		changed.Size++ // Persist a changed live size before the new process opens.
		if err := mirror.Insert(&changed); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("PASS cache-warm: two actual JPEG read-through puts; cold known and never resident; persisted stale size\n")
	} else {
		run("hot", "thumbnail-positive", 200, "")
		if st := thumbs.Stats(); st.Hits == 0 || st.Puts != 0 {
			t.Fatalf("hot was not served from new-process cache: %+v", st)
		}
		run("stale", "thumbnail-negative", 404, "stale-source")
		if thumbs.Has(fixture.Stale.Inode, "thumbnail") || thumbs.Stats().Invalidations == 0 {
			t.Fatalf("stale cache was not invalidated: %+v", thumbs.Stats())
		}
		run("cold", "thumbnail-negative", 503, "test-content-guard")
		if thumbs.Has(fixture.Cold.Inode, "thumbnail") || thumbs.Stats().Puts != 0 {
			t.Fatalf("cold path touched resident cache: %+v", thumbs.Stats())
		}
		fmt.Printf("PASS cache-restart: hot JPEG 200 local hit; stale 404 invalidated; known cold 503 guarded; zero puts\n")
	}
}

func cacheInodeQuery(r *http.Request) uint64 {
	var inode uint64
	_, _ = fmt.Sscanf(r.URL.Query().Get("inode"), "%d", &inode)
	return inode
}

func consumerCacheWarm(t *testing.T, parentCtx context.Context, root, source, ffmpeg, ffprobe string, mirror *metadata.Store, provider *derivatives.Store) consumerCacheFixture {
	t.Helper()
	farmStore, err := derivatives.Open(filepath.Join(root, "state", "farm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := farmStore.Close(); err != nil {
			t.Error(err)
		}
	}()
	var slots [3]consumerCacheSlot
	for i, name := range []string{"hot", "stale", "cold"} {
		if err := parentCtx.Err(); err != nil {
			t.Fatalf("cache warm interrupted before farm launch: %v", err)
		}
		rel := "Project_Foo/" + name + ".mov"
		path := filepath.Join(root, "backing", rel)
		consumerMediaCopy(t, source, path)
		copiedSHA, err := consumerMediaSHA(path)
		if err != nil || copiedSHA != os.Getenv("JM_CONSUMER_MEDIA_SOURCE_SHA256") {
			t.Fatalf("owned %s source differs from pinned real media: hash=%s err=%v", name, copiedSHA, err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatal("source inode unavailable")
		}
		inode := uint64(st.Ino)
		if err := mirror.Insert(&metadata.Entry{Path: rel, Name: name + ".mov", ParentPath: "Project_Foo", Size: fi.Size(), Mtime: fi.ModTime(), MtimeNsecKnown: true, Inode: inode, Mode: fi.Mode()}); err != nil {
			t.Fatal(err)
		}
		if known, _ := provider.Known(inode); known {
			t.Fatal("provider was seeded before actual on-miss reconciliation")
		}
		ctx, cancel := context.WithTimeout(parentCtx, 25*time.Second)
		res := farm.Process(farmStore, path, farm.Options{Context: ctx, Mount: filepath.Join(root, "backing"), EncodeScratchDir: filepath.Join(root, "state", "scratch"), Blobs: true, ThumbMaxDim: 320, FFmpegBin: ffmpeg, FFprobeBin: ffprobe, Producer: "linux-farm", Version: 1})
		cancel()
		if res.Err != nil || res.BlobErr != nil || !res.ThumbWrote || res.Inode != inode {
			t.Fatalf("actual farm poster %s: %+v", name, res)
		}
		poster := filepath.Join(root, "backing", derivatives.DerivBlobRel(inode, "poster.jpg"))
		pfi, err := os.Stat(poster)
		if err != nil || pfi.Size() <= 0 {
			t.Fatalf("poster %s: info=%v err=%v", name, pfi, err)
		}
		jpegSHA, err := consumerMediaSHA(poster)
		if err != nil {
			t.Fatal(err)
		}
		slots[i] = consumerCacheSlot{Inode: inode, SourceHash: res.Hash, SourceSize: fi.Size(), SourceMtime: fi.ModTime().Unix(), DisplayPath: "/Volumes/zpool/" + rel, JPEGSHA256: jpegSHA, JPEGSize: pfi.Size()}
	}
	if slots[0].Inode == slots[1].Inode || slots[1].Inode == slots[2].Inode || slots[0].Inode == slots[2].Inode {
		t.Fatal("three source slots reused an inode")
	}
	return consumerCacheFixture{Hot: slots[0], Stale: slots[1], Cold: slots[2]}
}
