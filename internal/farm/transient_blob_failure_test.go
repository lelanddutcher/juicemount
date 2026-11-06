package farm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func transientFixtureClip(t *testing.T) (mount, src string) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not on PATH")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not on PATH")
	}
	mount = t.TempDir()
	src = filepath.Join(mount, "clip.mov")
	if out, err := exec.Command("ffmpeg", "-y", "-v", "error", "-f", "lavfi",
		"-i", "testsrc2=size=320x180:rate=24:duration=2",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", src).CombinedOutput(); err != nil {
		t.Fatalf("make fixture: %v %s", err, out)
	}
	return mount, src
}

func transientFixtureStore(t *testing.T) *derivatives.Store {
	t.Helper()
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func rowOfKind(t *testing.T, store *derivatives.Store, inode uint64, kind string) (derivatives.DerivRow, bool) {
	t.Helper()
	rows, err := store.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == kind {
			return row, true
		}
	}
	return derivatives.DerivRow{}, false
}

// faultDerivDir makes every derivative-directory open fail with err until the
// returned restore is called; a test cannot make a tree read-only to root or
// unmount it.
func faultDerivDir(t *testing.T, err error) (restore func()) {
	t.Helper()
	openDerivDir = func(string, uint64) (*os.File, error) {
		return nil, fmt.Errorf("derivatives: mkdir under mount: %w", err)
	}
	restored := false
	restore = func() {
		if !restored {
			openDerivDir = derivDirFor
			restored = true
		}
	}
	t.Cleanup(restore)
	return restore
}

// A derivative tree that is briefly unusable (EROFS, ENOSPC, an unmount) is a
// fact about the environment, not the source. It used to be stored as a
// same-build failed row, which kindsComplete counted as complete: the retry
// skipped the asset as fresh, the job ended done, and the poster stayed missing
// until a new build was deployed.
func TestTransientBlobFailureIsRetriedWhenTheFaultClears(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}

	restore := faultDerivDir(t, syscall.EROFS)
	r1 := Process(store, src, opt)
	if r1.Err != nil {
		t.Fatalf("pass 1 hard error: %v", r1.Err)
	}
	if r1.BlobErr == nil || r1.ThumbWrote {
		t.Fatalf("pass 1 = blobErr %v thumb %v, want a blob failure", r1.BlobErr, r1.ThumbWrote)
	}
	row, ok := rowOfKind(t, store, r1.Inode, "thumbnail")
	if !ok || row.Status != "pending" {
		t.Fatalf("pass 1 thumbnail row = %+v (present %v), want a pending row", row, ok)
	}
	restore()

	r2 := Process(store, src, opt)
	if r2.Err != nil || r2.BlobErr != nil {
		t.Fatalf("pass 2 err %v blobErr %v", r2.Err, r2.BlobErr)
	}
	if r2.SkippedFresh || !r2.ThumbWrote {
		t.Fatalf("pass 2 skippedFresh=%v thumb=%v: the retry did not regenerate the poster", r2.SkippedFresh, r2.ThumbWrote)
	}
	if row, _ := rowOfKind(t, store, r2.Inode, "thumbnail"); row.Status != "ready" {
		t.Fatalf("pass 2 thumbnail row = %+v, want ready", row)
	}

	// Complete now, so a third pass is a fresh skip.
	if r3 := Process(store, src, opt); !r3.SkippedFresh {
		t.Fatalf("pass 3 = %+v, want a fresh skip once the poster exists", r3)
	}
}

// A decode failure is a fact about the source and is still remembered for the
// build, so a broken clip is not re-decoded on every sweep.
func TestDecodeBlobFailureStaysPermanentForBuild(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	ffmpeg := filepath.Join(t.TempDir(), "ffmpeg")
	script := "#!/bin/sh\necho 'Invalid data found when processing input' >&2\nexit 1\n"
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true, FFmpegBin: ffmpeg}

	r1 := Process(store, src, opt)
	if r1.Err != nil || r1.BlobErr == nil {
		t.Fatalf("pass 1 err %v blobErr %v, want a blob failure", r1.Err, r1.BlobErr)
	}
	if row, _ := rowOfKind(t, store, r1.Inode, "thumbnail"); row.Status != "failed" {
		t.Fatalf("thumbnail row = %+v, want failed", row)
	}
	if r2 := Process(store, src, opt); !r2.SkippedFresh {
		t.Fatalf("pass 2 = %+v, want the same-build decode failure to be terminal", r2)
	}
}

// Only faults that clear on their own are retryable. An ffmpeg fault is read
// from what ffmpeg printed, never from the paths in the message.
func TestBlobFailureStatusClassifiesEnvironmentFaults(t *testing.T) {
	exit := errors.New("exit status 1")
	src := "/vol/Input/Output Error/a.mov"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"no derivative dir", errNoDerivDir, "pending"},
		{"wrapped no derivative dir", fmt.Errorf("waveform: %w", errNoDerivDir), "pending"},
		{"no derivative dir: read-only", fmt.Errorf("%w: %w", errNoDerivDir, syscall.EROFS), "pending"},
		{"no derivative dir: symlink", fmt.Errorf("%w: %w", errNoDerivDir, syscall.ELOOP), "failed"},
		{"no derivative dir: not a directory", fmt.Errorf("%w: %w", errNoDerivDir, syscall.ENOTDIR), "failed"},
		{"enospc", &os.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC}, "pending"},
		{"erofs", fmt.Errorf("commit: %w", syscall.EROFS), "pending"},
		{"directory at blob name", fmt.Errorf("derivatives: commit: %w", &os.LinkError{Op: "renameat", Err: syscall.EISDIR}), "failed"},
		{"symlink loop", &os.PathError{Op: "openat", Path: "x", Err: syscall.ELOOP}, "failed"},
		{"permission", &os.PathError{Op: "openat", Path: "x", Err: syscall.EACCES}, "pending"},
		{"no derivative dir: permission", fmt.Errorf("%w: %w", errNoDerivDir, syscall.EACCES), "pending"},
		{"no derivative dir: not permitted", fmt.Errorf("%w: %w", errNoDerivDir, syscall.EPERM), "pending"},
		{"no derivative dir: name too long", fmt.Errorf("%w: %w", errNoDerivDir, syscall.ENAMETOOLONG), "failed"},
		{"stage name exists", errors.New("derivatives: stage: file exists"), "failed"},
		{"ffmpeg disk full", newToolFailure("ffmpeg thumbnail", exit, []byte("av_interleaved_write_frame(): No space left on device"), src), "pending"},
		{"ffmpeg source eio", newToolFailure("ffmpeg thumbnail", exit, []byte(src+": Input/output error"), src), "pending"},
		{"ffmpeg missing", newToolFailure("ffmpeg thumbnail", &exec.Error{Name: "ffmpeg", Err: exec.ErrNotFound}, nil, src), "pending"},
		{"decode failure in a folder named like a fault", newToolFailure(
			"ffmpeg thumbnail \""+src+"\": exit status 1: "+src+": Invalid data found when processing input",
			exit, []byte(src+": Invalid data found when processing input"), src), "failed"},
		{"plain error naming a fault", errors.New("waveform \"/vol/Input/Output Error/a.mov\": no audio samples"), "failed"},
		{"no samples", errors.New("waveform: indexed audio has no decodable samples"), "failed"},
	} {
		if got, _, _ := blobFailure(nil, "thumbnail", "h", tc.err, Options{}); got != tc.want {
			t.Errorf("%s: blobFailure(%v) = %q, want %q", tc.name, tc.err, got, tc.want)
		}
	}
}

// A directory sitting at the blob's name never goes away by itself. It used to
// be retried as an environment fault, with a full decode on every pass.
func TestDirectoryAtBlobNameIsAPermanentFailure(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}
	fi, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	inode := fi.Sys().(*syscall.Stat_t).Ino
	if err := os.MkdirAll(filepath.Join(mount, derivatives.DerivBlobRel(inode, "poster.jpg")), 0o755); err != nil {
		t.Fatal(err)
	}
	r1 := Process(store, src, opt)
	if r1.Err != nil || r1.BlobErr == nil {
		t.Fatalf("pass 1 err %v blobErr %v, want a blob failure", r1.Err, r1.BlobErr)
	}
	if row, _ := rowOfKind(t, store, r1.Inode, "thumbnail"); row.Status != "failed" {
		t.Fatalf("thumbnail row = %+v, want failed", row)
	}
	if r2 := Process(store, src, opt); !r2.SkippedFresh {
		t.Fatalf("pass 2 = %+v, want no second decode", r2)
	}
}

// A source path that happens to contain a fault's wording does not make its
// decode failure retryable.
func TestDecodeFailureUnderFaultNamedFolderIsPermanent(t *testing.T) {
	mount, clip := transientFixtureClip(t)
	dir := filepath.Join(mount, "Input", "Output Error")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "clip.mov")
	if err := os.Rename(clip, src); err != nil {
		t.Fatal(err)
	}
	store := transientFixtureStore(t)
	// Like ffmpeg, the stub names its input in the error it prints.
	ffmpeg := filepath.Join(t.TempDir(), "ffmpeg")
	script := "#!/bin/sh\nfor a; do case \"$a\" in *.mov) echo \"$a: Invalid data found when processing input\" >&2;; esac; done\nexit 1\n"
	if err := os.WriteFile(ffmpeg, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true, FFmpegBin: ffmpeg}
	r1 := Process(store, src, opt)
	if r1.Err != nil || r1.BlobErr == nil {
		t.Fatalf("pass 1 err %v blobErr %v, want a blob failure", r1.Err, r1.BlobErr)
	}
	if row, _ := rowOfKind(t, store, r1.Inode, "thumbnail"); row.Status != "failed" {
		t.Fatalf("thumbnail row = %+v (blobErr %v), want failed", row, r1.BlobErr)
	}
}

// An environment fault that never clears is retried until it has recurred
// maxPendingBlobAttempts times over at least the retry window, then recorded as
// a failure for the build so sweeps stop re-decoding the source.
func TestPersistentEnvironmentFaultStopsRetryingForTheBuild(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}
	faultDerivDir(t, syscall.EROFS)
	start := time.Now()
	t.Cleanup(func() { blobRetryNow = time.Now })
	pass := func(n int, at time.Time, want string) {
		t.Helper()
		blobRetryNow = func() time.Time { return at }
		r := Process(store, src, opt)
		if r.SkippedFresh || r.BlobErr == nil {
			t.Fatalf("pass %d = %+v, want a blob failure", n, r)
		}
		if row, _ := rowOfKind(t, store, r.Inode, "thumbnail"); row.Status != want {
			t.Fatalf("pass %d thumbnail row = %s (attempts %d since %d), want %s", n, row.Status, row.PendingAttempts, row.PendingSince, want)
		}
	}
	// Enough attempts, but within the window: still retried.
	for n := 1; n <= maxPendingBlobAttempts+1; n++ {
		pass(n, start.Add(time.Duration(n)*time.Minute), "pending")
	}
	// The window has passed since the first attempt: a failure for the build.
	pass(maxPendingBlobAttempts+2, start.Add(blobRetryWindow()+time.Minute), "failed")
	if r := Process(store, src, opt); !r.SkippedFresh {
		t.Fatalf("pass after the cap = %+v, want a fresh skip", r)
	}
	// A new build gets its own attempts.
	opt.Version++
	if r := Process(store, src, opt); r.SkippedFresh {
		t.Fatal("a new producer build skipped the failed blob")
	}
}

// A short outage spanning a few passes used to reach the attempt cap and
// publish a failure for the build, which every host then treated as complete.
func TestShortOutageDoesNotBecomeABuildFailure(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}
	restore := faultDerivDir(t, syscall.EROFS)
	for n := 1; n <= maxPendingBlobAttempts; n++ {
		if r := Process(store, src, opt); r.BlobErr == nil {
			t.Fatalf("outage pass %d = %+v, want a blob failure", n, r)
		}
	}
	restore()
	r := Process(store, src, opt)
	if r.SkippedFresh || !r.ThumbWrote {
		t.Fatalf("pass after the outage = skippedFresh %v thumb %v; want the poster regenerated", r.SkippedFresh, r.ThumbWrote)
	}
	if row, _ := rowOfKind(t, store, r.Inode, "thumbnail"); row.Status != "ready" {
		t.Fatalf("thumbnail row after the outage = %+v, want ready", row)
	}
}

// A symlink where the derivative tree belongs never goes away by itself. The
// reason the directory could not be opened decides, not the fact that it
// could not: it used to be retried like an outage.
func TestSymlinkedDerivativeTreeIsAPermanentFailure(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}
	if err := os.Symlink(t.TempDir(), filepath.Join(mount, ".juicemount")); err != nil {
		t.Fatal(err)
	}
	r1 := Process(store, src, opt)
	if r1.Err != nil || r1.BlobErr == nil {
		t.Fatalf("pass 1 err %v blobErr %v, want a blob failure", r1.Err, r1.BlobErr)
	}
	if row, _ := rowOfKind(t, store, r1.Inode, "thumbnail"); row.Status != "failed" {
		t.Fatalf("thumbnail row = %+v (blobErr %v), want failed", row, r1.BlobErr)
	}
	if r2 := Process(store, src, opt); !r2.SkippedFresh {
		t.Fatalf("pass 2 = %+v, want no second decode", r2)
	}
}

// A pending row is local: the shared manifest must not carry it, and must keep
// the current row another worker published for the same source.
func TestManifestSidecarOmitsPendingRows(t *testing.T) {
	mount := t.TempDir()
	store := transientFixtureStore(t)
	const inode = 4242
	hash := "0123456789abcdef"
	if err := store.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	for _, row := range []derivatives.DerivRow{
		{Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 7, Hash: &hash},
		{Kind: "thumbnail", Status: "pending", Producer: "linux-farm", Version: 7, Hash: &hash},
	} {
		if err := store.PutDeriv(inode, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteManifestSidecar(store, mount, inode); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(mount, derivatives.DerivBlobRel(inode, "manifest.json")))
	if err != nil {
		t.Fatal(err)
	}
	var sc ManifestSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	for _, row := range sc.Derivatives {
		if row.Status == "pending" {
			t.Fatalf("manifest carries a pending row: %+v", row)
		}
	}
	if len(sc.Derivatives) != 1 || sc.Derivatives[0].Kind != "tech" {
		t.Fatalf("manifest rows = %+v, want only tech", sc.Derivatives)
	}
}

// A worker that may not write the derivative tree (the wrong user, or root on
// a root-squashed mount) is fixed by redeploying it. Failing at once published
// failed rows that every host counted as complete until the next build; the
// fault is now retried within the window like an outage.
func TestDerivativeTreePermissionFaultIsRetried(t *testing.T) {
	mount, src := transientFixtureClip(t)
	store := transientFixtureStore(t)
	opt := Options{Producer: "linux-farm", Version: 7, Mount: mount, Blobs: true}
	restore := faultDerivDir(t, syscall.EACCES)
	r1 := Process(store, src, opt)
	if r1.BlobErr == nil {
		t.Fatalf("pass 1 = %+v, want a blob failure", r1)
	}
	if row, _ := rowOfKind(t, store, r1.Inode, "thumbnail"); row.Status != "pending" {
		t.Fatalf("thumbnail row = %+v, want pending", row)
	}
	restore()
	if r2 := Process(store, src, opt); r2.SkippedFresh || !r2.ThumbWrote {
		t.Fatalf("pass after the fix = %+v, want the poster regenerated", r2)
	}
}

// A first-fault time in the future (stamped while the host clock was wrong)
// would hold the retry window open forever; it restarts at the current time.
func TestFutureFirstFaultTimeRestartsTheWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	blobRetryNow = func() time.Time { return now }
	t.Cleanup(func() { blobRetryNow = time.Now })
	hash := "h"
	prior := map[string]derivatives.DerivRow{"thumbnail": {
		Kind: "thumbnail", Status: "pending", Producer: "linux-farm", Version: 7, Hash: &hash,
		PendingAttempts: maxPendingBlobAttempts, PendingSince: now.Add(30 * 24 * time.Hour).Unix(),
	}}
	opt := Options{Producer: "linux-farm", Version: 7}
	status, attempts, since := blobFailure(prior, "thumbnail", hash, syscall.EROFS, opt)
	if status != "pending" || attempts != maxPendingBlobAttempts+1 || since != now.Unix() {
		t.Fatalf("blobFailure = %s attempts %d since %d; want pending %d since %d",
			status, attempts, since, maxPendingBlobAttempts+1, now.Unix())
	}
	// Once the window has passed from the restarted time, the fault fails.
	prior["thumbnail"] = derivatives.DerivRow{Kind: "thumbnail", Status: "pending", Producer: "linux-farm",
		Version: 7, Hash: &hash, PendingAttempts: attempts, PendingSince: since}
	now = now.Add(blobRetryWindow() + time.Minute)
	if status, _, _ := blobFailure(prior, "thumbnail", hash, syscall.EROFS, opt); status != "failed" {
		t.Fatalf("after the restarted window: %s, want failed", status)
	}
}
