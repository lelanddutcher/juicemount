package nfs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

const (
	testRedisURL  = "redis://127.0.0.1:6379/1"
	testFUSEPath  = "/Users/USER/.juicemount/fuse-internal"
	testMountBase = "/tmp/jm5-test-mount"
)

func setupTestServer(t *testing.T) (*Server, *metadata.Store) {
	t.Helper()

	// Create metadata store and sync from Redis
	store, err := metadata.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}

	rc, err := metadata.NewRedisClient(testRedisURL, store)
	if err != nil {
		t.Skipf("Redis not reachable: %v", err)
	}

	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	rc.Stop()

	count, _ := store.Count()
	t.Logf("Synced %d entries from Redis", count)

	// Create NFS server
	srv := NewServer(Config{
		ListenAddr: "127.0.0.1:0", // random port
		FUSEPath:   testFUSEPath,
	}, store)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() {
		srv.Stop()
		store.Close()
	})

	return srv, store
}

func mountNFS(t *testing.T, addr string) string {
	t.Helper()

	mountPoint := filepath.Join(testMountBase, fmt.Sprintf("test_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", mountPoint, err)
	}

	// Parse host:port
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		t.Fatalf("invalid addr: %s", addr)
	}
	host := parts[0]
	port := parts[1]

	// macOS mount_nfs: port goes in -o options, server:/path is the source
	opts := fmt.Sprintf("port=%s,mountport=%s,soft,intr,timeo=50,retrans=3,nolocks,locallocks,rsize=1048576,wsize=1048576,actimeo=3600,vers=3,tcp", port, port)
	cmd := exec.Command("sudo", "mount_nfs",
		"-o", opts,
		fmt.Sprintf("%s:/", host),
		mountPoint,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		os.Remove(mountPoint)
		t.Skipf("mount_nfs failed: %v\n%s", err, out)
	}

	t.Cleanup(func() {
		exec.Command("sudo", "umount", "-f", mountPoint).Run()
		os.Remove(mountPoint)
	})

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

func TestNFSMountAndStat(t *testing.T) {
	srv, _ := setupTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

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
	srv, _ := setupTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

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
	srv, store := setupTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

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
	srv, store := setupTestServer(t)
	mountPoint := mountNFS(t, srv.Addr())

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
