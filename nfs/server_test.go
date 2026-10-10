package nfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
)

var (
	testRedisURL = os.Getenv("JM_TEST_VOLUME_REDIS")
	testFUSEPath = os.Getenv("JM_TEST_FUSE_PATH")
)

func requireLiveTestVolume(t *testing.T) {
	t.Helper()
	if testRedisURL == "" || testFUSEPath == "" {
		t.Skip("requires JM_TEST_VOLUME_REDIS and JM_TEST_FUSE_PATH for a populated live volume")
	}
	if _, err := redisurl.Parse(testRedisURL); err != nil {
		t.Fatalf("invalid JM_TEST_VOLUME_REDIS: %v", err)
	}
	if !filepath.IsAbs(testFUSEPath) || filepath.Clean(testFUSEPath) != testFUSEPath {
		t.Fatalf("JM_TEST_FUSE_PATH must be an absolute clean path: %q", testFUSEPath)
	}
	info, err := os.Stat(testFUSEPath)
	if err != nil || !info.IsDir() {
		t.Fatalf("JM_TEST_FUSE_PATH is not an accessible directory: %v", err)
	}
}

func requireMountedTest(t *testing.T) {
	t.Helper()
	if os.Getenv("JM_TEST_NFS_MOUNT") == "" {
		t.Skip("requires JM_TEST_NFS_MOUNT=1 for a caller-owned mounted test")
	}
	if os.Getenv("JM_TEST_NFS_MOUNT") != "1" {
		t.Fatal("JM_TEST_NFS_MOUNT must be 1 when set")
	}
}

func setupTestServer(t *testing.T) (*Server, *metadata.Store) {
	t.Helper()
	requireLiveTestVolume(t)

	// Create metadata store and sync from Redis
	store, err := metadata.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close test metadata store: %v", err)
		}
	})
	initServerTestSpoolSchema(t, store)

	rc, err := metadata.NewRedisClient(testRedisURL, store)
	if err != nil {
		t.Fatalf("configured JM_TEST_VOLUME_REDIS unreachable: %v", err)
	}
	t.Cleanup(func() { rc.Stop() })

	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	count, _ := store.Count()
	t.Logf("Synced %d entries from Redis", count)

	// Create NFS server
	srv := NewServer(Config{
		ListenAddr: "127.0.0.1:0", // random port
		FUSEPath:   testFUSEPath,
	}, store)
	t.Cleanup(func() {
		if err := srv.Stop(); err != nil {
			t.Errorf("stop test NFS listener: %v", err)
		}
		if !srv.handler.StopHandler() {
			t.Error("test NFS handler did not settle")
		}
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return srv, store
}

func mountNFS(t *testing.T, srv *Server) string {
	t.Helper()
	requireMountedTest(t)
	requireLiveTestVolume(t)
	base := os.Getenv("TMPDIR")
	if base == "" || !filepath.IsAbs(base) || filepath.Clean(base) != base || base == "/tmp" || base == "/private/tmp" || base == "/var/tmp" {
		t.Fatal("mounted tests require an absolute caller TMPDIR")
	}
	addr := srv.Addr()

	// Parse host:port
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		t.Fatalf("invalid addr: %s", addr)
	}
	host := parts[0]
	port := parts[1]
	mountPoint, err := os.MkdirTemp(base, "jm-nfs-test-")
	if err != nil {
		t.Fatalf("create private mountpoint: %v", err)
	}
	mountAttempted, mounted := false, false
	t.Cleanup(func() {
		if !mountAttempted {
			if err := os.Remove(mountPoint); err != nil {
				t.Errorf("remove unused mountpoint %s: %v", mountPoint, err)
			}
			return
		}
		if !mounted {
			t.Logf("retained mountpoint after uncertain failed mount: %s", mountPoint)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sudo", "-n", "umount", "-f", mountPoint)
		cmd.WaitDelay = time.Second
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Errorf("unmount %s failed; mountpoint retained: %v\n%s", mountPoint, err, out)
			return
		}
		if err := os.Remove(mountPoint); err != nil {
			t.Errorf("remove unmounted mountpoint %s: %v", mountPoint, err)
		}
	})

	// macOS mount_nfs: port goes in -o options, server:/path is the source
	opts := fmt.Sprintf("port=%s,mountport=%s,soft,intr,timeo=50,retrans=3,nolocks,locallocks,rsize=1048576,wsize=1048576,actimeo=3600,vers=3,tcp", port, port)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sudo", "-n", "mount_nfs",
		"-o", opts,
		host+":"+srv.ExportPath(),
		mountPoint,
	)
	cmd.WaitDelay = time.Second

	mountAttempted = true
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("mount_nfs failed: %v\n%s", err, out)
	}
	mounted = true

	t.Logf("Mounted NFS at %s (server %s)", mountPoint, addr)
	return mountPoint
}

func TestServerStartStop(t *testing.T) {
	srv, _ := setupTestServer(t)
	addr := srv.Addr()
	if addr == "" {
		t.Fatal("server has no address")
	}
	t.Logf("Server listening on %s", addr)
}

func TestServerRunningLifecycle(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "lifecycle.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()
	initServerTestSpoolSchema(t, store)

	srv := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: t.TempDir()}, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !srv.IsRunning() {
		t.Fatal("server must report running after successful listen")
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if srv.IsRunning() {
		t.Fatal("server must report stopped after listener close")
	}
}

func TestServerPreparedBeforeListenerStarts(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "prepared.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()
	initServerTestSpoolSchema(t, store)

	srv := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: t.TempDir()}, store)
	defer srv.Handler().StopHandler()
	if srv.Handler() == nil {
		t.Fatal("handler must be available for dependency wiring before Start")
	}
	if srv.IsRunning() || srv.Addr() != "" {
		t.Fatalf("prepared server accepted traffic before Start: running=%v addr=%q", srv.IsRunning(), srv.Addr())
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()
	if !srv.IsRunning() || srv.Addr() == "" {
		t.Fatalf("started server did not publish listener: running=%v addr=%q", srv.IsRunning(), srv.Addr())
	}
}

func TestServerStartIsIdempotent(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "idempotent.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()
	initServerTestSpoolSchema(t, store)

	srv := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: t.TempDir()}, store)
	defer srv.Handler().StopHandler()
	if err := srv.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer srv.Stop()
	firstAddr := srv.Addr()
	if err := srv.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if got := srv.Addr(); got != firstAddr {
		t.Fatalf("idempotent Start replaced listener: first=%q second=%q", firstAddr, got)
	}
}

func initServerTestSpoolSchema(t *testing.T, store *metadata.Store) {
	t.Helper()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatalf("InitSpoolSchema: %v", err)
	}
}

func TestServerDisableRestartKeepsListenerUntilStop(t *testing.T) {
	listener := &shutdownTestListener{}
	srv := &Server{listener: listener}
	srv.running.Store(true)
	srv.DisableRestart()
	if !srv.IsRunning() {
		t.Fatal("listener closed before unmount handshake")
	}
	if listener.closed {
		t.Fatal("DisableRestart closed listener")
	}
	if err := srv.DisableStart(); err != nil {
		t.Fatal(err)
	}
	if srv.IsRunning() {
		t.Fatal("listener remained open after DisableStart")
	}
	if !listener.closed {
		t.Fatal("DisableStart did not close listener")
	}
	if err := srv.Start(); err == nil {
		t.Fatal("retired server reopened listener")
	}
}

type shutdownTestListener struct{ closed bool }

func (l *shutdownTestListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l *shutdownTestListener) Close() error {
	l.closed = true
	return nil
}
func (l *shutdownTestListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestClosedListenerIsRecognizedAsNormalShutdown(t *testing.T) {
	err := &net.OpError{Op: "accept", Net: "tcp", Err: net.ErrClosed}
	if unexpectedServeError(err) {
		t.Fatal("wrapped listener close was classified as a server failure")
	}
	if unexpectedServeError(nil) {
		t.Fatal("nil serve result was classified as a server failure")
	}
	if !unexpectedServeError(errors.New("unexpected serve failure")) {
		t.Fatal("real serve error was suppressed")
	}
}

func TestNFSMountAndStat(t *testing.T) {
	requireMountedTest(t)
	srv, _ := setupTestServer(t)
	mountPoint := mountNFS(t, srv)

	// stat the mount point
	info, err := os.Stat(mountPoint)
	if err != nil {
		t.Fatalf("Stat mount point: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("mount point should be a directory")
	}
	t.Logf("Mount point stat OK: %s (dir=%v)", info.Name(), info.IsDir())
}

func TestNFSReadDir(t *testing.T) {
	requireMountedTest(t)
	srv, _ := setupTestServer(t)
	mountPoint := mountNFS(t, srv)

	// Read root directory
	entries, err := os.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("expected entries in root directory")
	}

	t.Logf("Root directory has %d entries:", len(entries))
	for i, e := range entries {
		if i < 10 {
			t.Logf("  %s (dir=%v)", e.Name(), e.IsDir())
		}
	}
	if len(entries) > 10 {
		t.Logf("  ... and %d more", len(entries)-10)
	}
}

func TestNFSStatFile(t *testing.T) {
	requireMountedTest(t)
	srv, store := setupTestServer(t)
	mountPoint := mountNFS(t, srv)

	// Find a file in our metadata store to stat
	children, err := store.ListChildren(".")
	if err != nil || len(children) == 0 {
		t.Skip("no root entries to test")
	}

	// Find a non-directory entry (skip ._* files — fast-rejected by NFS handler)
	isStatAble := func(e *metadata.Entry) bool {
		return !e.IsDir && !strings.HasPrefix(e.Name, "._") &&
			e.Name != ".DS_Store" && e.Name != ".Trashes"
	}
	var testPath string
	for _, e := range children {
		if isStatAble(e) {
			testPath = e.Path
			break
		}
	}
	if testPath == "" {
		// Try a file inside a directory
		for _, e := range children {
			if e.IsDir {
				subChildren, _ := store.ListChildren(e.Path)
				for _, sc := range subChildren {
					if isStatAble(sc) {
						testPath = sc.Path
						break
					}
				}
				if testPath != "" {
					break
				}
			}
		}
	}
	if testPath == "" {
		t.Skip("no files found in metadata")
	}

	fullPath := filepath.Join(mountPoint, testPath)
	start := time.Now()
	info, err := os.Stat(fullPath)
	statDur := time.Since(start)

	if err != nil {
		t.Fatalf("Stat(%s): %v", testPath, err)
	}

	t.Logf("Stat %s: size=%d dir=%v latency=%v", testPath, info.Size(), info.IsDir(), statDur)

	if statDur > 10*time.Millisecond {
		t.Logf("WARNING: stat latency >10ms (expected <1ms from SQLite)")
	}
}

func TestNFSStatLatency(t *testing.T) {
	requireMountedTest(t)
	srv, store := setupTestServer(t)
	mountPoint := mountNFS(t, srv)

	children, _ := store.ListChildren(".")
	if len(children) == 0 {
		t.Skip("no entries")
	}

	// Warm up
	testPath := filepath.Join(mountPoint, children[0].Path)
	os.Stat(testPath)

	// Measure 100 stats
	iterations := 100
	start := time.Now()
	for i := 0; i < iterations; i++ {
		os.Stat(testPath)
	}
	elapsed := time.Since(start)
	avg := elapsed / time.Duration(iterations)

	t.Logf("Average stat latency: %v (%d iterations in %v)", avg, iterations, elapsed)

	if avg > 5*time.Millisecond {
		t.Errorf("stat latency too high: %v (target <1ms)", avg)
	}
}
