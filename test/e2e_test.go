package test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

const (
	redisURL  = "redis://192.168.0.210:6379/1"
	fusePath  = "/Users/LelandDutcher/.juicemount/fuse-internal"
	mountBase = "/tmp/jm5-e2e"
)

// e2eEnv holds the full stack for E2E testing.
type e2eEnv struct {
	store   *metadata.Store
	rc      *metadata.RedisClient
	cr      *cache.Reader
	srv     *jmnfs.Server
	monitor *health.HealthMonitor
	mount   string
	rdb     *redis.Client
	t       *testing.T
}

func setupE2E(t *testing.T) *e2eEnv {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "e2e.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}

	rc, err := metadata.NewRedisClient(redisURL, store)
	if err != nil {
		t.Skipf("Redis not reachable: %v", err)
	}

	t.Log("Syncing metadata from Redis...")
	start := time.Now()
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	count, _ := store.Count()
	t.Logf("Synced %d entries in %v", count, time.Since(start).Round(time.Millisecond))

	// Start background sync
	rc.Start()

	// Cache reader
	cacheDir := cache.DetectCacheDir()
	addr, db, _ := metadata.ParseRedisURL(redisURL)
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})

	var cr *cache.Reader
	if cacheDir != "" {
		cr = cache.NewReader(cacheDir, cache.DefaultBlockSize, rdb)
	}

	// NFS server
	srv := jmnfs.NewServer(jmnfs.Config{
		ListenAddr: "127.0.0.1:0",
		FUSEPath:   fusePath,
	}, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start NFS: %v", err)
	}
	if cr != nil {
		srv.Handler().SetCacheReader(cr)
	}
	srv.Handler().SetRedisClient(rc)

	// Health monitor
	mon := health.New(health.Config{
		RedisURL:      "192.168.0.210:6379",
		MinIOURL:      "http://192.168.0.212:9000",
		FUSEPath:      fusePath,
		NFSMountPoint: "",
	})
	mon.Start()

	// Mount NFS
	mountPoint := filepath.Join(mountBase, fmt.Sprintf("e2e_%d", time.Now().UnixNano()))
	os.MkdirAll(mountPoint, 0755)

	parts := splitAddr(srv.Addr())
	opts := fmt.Sprintf("port=%s,mountport=%s,soft,intr,timeo=50,retrans=3,nolocks,locallocks,rsize=1048576,wsize=1048576,noac,vers=3,tcp", parts[1], parts[1])
	cmd := exec.Command("sudo", "mount_nfs", "-o", opts, fmt.Sprintf("%s:/", parts[0]), mountPoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("mount_nfs failed: %v\n%s", err, out)
	}

	t.Logf("E2E stack ready: mount=%s server=%s", mountPoint, srv.Addr())

	env := &e2eEnv{
		store: store, rc: rc, cr: cr, srv: srv,
		monitor: mon, mount: mountPoint, rdb: rdb, t: t,
	}

	t.Cleanup(func() {
		exec.Command("sudo", "umount", "-f", mountPoint).Run()
		os.Remove(mountPoint)
		mon.Stop()
		srv.Handler().StopHandler()
		srv.Stop()
		if cr != nil {
			cr.Stop()
		}
		rc.Stop()
		rdb.Close()
		store.Close()
	})

	return env
}

func splitAddr(addr string) [2]string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return [2]string{addr[:i], addr[i+1:]}
		}
	}
	return [2]string{addr, ""}
}

func TestE2E_FullStackBoot(t *testing.T) {
	env := setupE2E(t)

	count, _ := env.store.Count()
	if count == 0 {
		t.Fatal("metadata store is empty")
	}
	t.Logf("Metadata store: %d entries", count)

	status := env.monitor.Status()
	t.Logf("Health: Redis=%v MinIO=%v FUSE=%v", status.Redis.Healthy, status.MinIO.Healthy, status.FUSE.Healthy)
}

func TestE2E_ReadDir1000(t *testing.T) {
	env := setupE2E(t)

	// Measure readdir performance for root (should be <1ms from SQLite)
	start := time.Now()
	entries, err := os.ReadDir(env.mount)
	dur := time.Since(start)

	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	t.Logf("ReadDir root: %d entries in %v", len(entries), dur)

	if dur > 100*time.Millisecond {
		t.Errorf("ReadDir too slow: %v (expected <100ms)", dur)
	}
}

func TestE2E_WriteReadVerify(t *testing.T) {
	env := setupE2E(t)

	// Write 10MB of structured data
	testFile := filepath.Join(env.mount, fmt.Sprintf("__e2e_write_%d.bin", time.Now().UnixNano()))
	data := make([]byte, 10*1024*1024)
	for i := range data {
		data[i] = byte(i % 251) // prime modulus for pattern
	}
	writeHash := sha256.Sum256(data)

	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read back
	readBack, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	readHash := sha256.Sum256(readBack)

	if writeHash != readHash {
		t.Fatal("SHA256 MISMATCH: data integrity failure")
	}

	os.Remove(testFile)
	t.Logf("10MB write/read: SHA256 verified (%x)", writeHash[:8])
}

func TestE2E_DittoCopy10Files(t *testing.T) {
	env := setupE2E(t)

	srcDir := filepath.Join(env.mount, fmt.Sprintf("__e2e_ditto_src_%d", time.Now().UnixNano()))
	dstDir := filepath.Join(env.mount, fmt.Sprintf("__e2e_ditto_dst_%d", time.Now().UnixNano()))
	os.Mkdir(srcDir, 0755)

	// Create 10 files of varying sizes
	hashes := make([][32]byte, 10)
	for i := 0; i < 10; i++ {
		size := (i + 1) * 10 * 1024 // 10KB to 100KB
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i*7 + j*13) % 256)
		}
		hashes[i] = sha256.Sum256(data)
		os.WriteFile(filepath.Join(srcDir, fmt.Sprintf("file_%d.bin", i)), data, 0644)
	}

	// ditto copy
	out, err := exec.Command("ditto", srcDir, dstDir).CombinedOutput()
	if err != nil {
		t.Fatalf("ditto: %v\n%s", err, out)
	}

	// Verify
	failures := 0
	for i := 0; i < 10; i++ {
		data, err := os.ReadFile(filepath.Join(dstDir, fmt.Sprintf("file_%d.bin", i)))
		if err != nil {
			t.Errorf("file_%d: read error: %v", i, err)
			failures++
			continue
		}
		h := sha256.Sum256(data)
		if h != hashes[i] {
			t.Errorf("file_%d: SHA256 mismatch", i)
			failures++
		}
	}

	os.RemoveAll(srcDir)
	os.RemoveAll(dstDir)

	if failures > 0 {
		t.Fatalf("ditto copy: %d/%d failures", failures, 10)
	}
	t.Log("ditto copy 10 files: 100% success, all SHA256 verified")
}

func TestE2E_SubscribePropagation(t *testing.T) {
	env := setupE2E(t)

	// Publish a test event via Redis
	ctx := context.Background()
	testPath := fmt.Sprintf("__e2e_subscribe_%d.txt", time.Now().UnixNano())
	evt := metadata.MetadataEvent{
		Op: "create", Path: testPath,
		Size: 42, Mtime: time.Now().Unix(),
		Inode: 9999999,
	}
	if err := env.rc.PublishEvent(ctx, evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for SUBSCRIBE to deliver it
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e := env.store.LookupByPath(testPath); e != nil {
			dur := time.Since(deadline.Add(-5 * time.Second))
			t.Logf("SUBSCRIBE propagation: %v", dur)
			// Clean up
			env.store.Delete(testPath)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("SUBSCRIBE event not received within 5s")
}

func TestE2E_CachedReadThroughput(t *testing.T) {
	env := setupE2E(t)

	// Find a file that's in the SSD cache
	var testFile string
	children, _ := env.store.ListChildren(".")
	for _, e := range children {
		if e.IsDir {
			sub, _ := env.store.ListChildren(e.Path)
			for _, sc := range sub {
				if !sc.IsDir && sc.Size > 1*1024*1024 {
					// Check if any blocks are cached
					testFile = sc.Path
					break
				}
			}
		}
		if testFile != "" {
			break
		}
	}
	if testFile == "" {
		t.Skip("no suitable file for throughput test")
	}

	nfsPath := filepath.Join(env.mount, testFile)

	// First read (may be cache miss)
	f, err := os.Open(nfsPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	h := sha256.New()
	start := time.Now()
	n, _ := io.Copy(h, f)
	dur := time.Since(start)
	f.Close()

	throughput := float64(n) / dur.Seconds() / (1024 * 1024)
	t.Logf("Read 1: %.1fMB in %v = %.1f MB/s", float64(n)/(1024*1024), dur, throughput)

	// Second read (NFS kernel cache should help)
	f2, _ := os.Open(nfsPath)
	start2 := time.Now()
	n2, _ := io.Copy(io.Discard, f2)
	dur2 := time.Since(start2)
	f2.Close()

	throughput2 := float64(n2) / dur2.Seconds() / (1024 * 1024)
	t.Logf("Read 2: %.1fMB in %v = %.1f MB/s (kernel cached)", float64(n2)/(1024*1024), dur2, throughput2)
}

func TestE2E_HealthStatus(t *testing.T) {
	env := setupE2E(t)

	// Wait for first health check
	time.Sleep(2 * time.Second)

	status := env.monitor.Status()
	if !status.Redis.Healthy {
		t.Error("Redis should be healthy")
	}
	if !status.MinIO.Healthy {
		t.Error("MinIO should be healthy")
	}
	if !status.FUSE.Healthy {
		t.Error("FUSE should be healthy")
	}
	t.Logf("Health status: Redis=%v MinIO=%v FUSE=%v", status.Redis.Healthy, status.MinIO.Healthy, status.FUSE.Healthy)
}
