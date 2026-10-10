package nfs

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func setupReadTestServer(t *testing.T) (*Server, *metadata.Store) {
	t.Helper()
	requireMountedTest(t)
	requireLiveTestVolume(t)
	cacheDir := os.Getenv("JM_TEST_CACHE_DIR")
	if cacheDir != "" {
		if !filepath.IsAbs(cacheDir) || filepath.Clean(cacheDir) != cacheDir {
			t.Fatalf("JM_TEST_CACHE_DIR must be an absolute clean path: %q", cacheDir)
		}
		info, err := os.Stat(cacheDir)
		if err != nil || !info.IsDir() {
			t.Fatalf("JM_TEST_CACHE_DIR is not an accessible directory: %v", err)
		}
	}

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

	var cr *cache.Reader
	if cacheDir != "" {
		opts, err := redisurl.Parse(testRedisURL)
		if err != nil {
			t.Fatalf("invalid JM_TEST_VOLUME_REDIS for cache reader: %v", err)
		}
		rdb := redis.NewClient(opts)
		t.Cleanup(func() {
			if cr == nil {
				if err := rdb.Close(); err != nil {
					t.Errorf("close test cache Redis client: %v", err)
				}
			}
		})
		cr = cache.NewReader(cacheDir, cache.DefaultBlockSize, rdb)
		t.Cleanup(func() { cr.Stop() })
		t.Logf("Cache reader enabled at %s", cacheDir)
	}

	srv := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		FUSEPath:   testFUSEPath,
	}, store)

	if cr != nil {
		srv.handler.SetCacheReader(cr)
	}
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

func TestNFSReadFile(t *testing.T) {
	srv, store := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv)

	// Find a small file to read (skip ._* files — they're fast-rejected by the NFS handler)
	isReadable := func(e *metadata.Entry) bool {
		if e.IsDir || e.Size <= 0 || e.Size >= 10*1024*1024 {
			return false
		}
		if strings.HasPrefix(e.Name, "._") || e.Name == ".DS_Store" {
			return false
		}
		// Verify file actually exists on FUSE (metadata may be stale from previous test runs)
		_, err := os.Stat(filepath.Join(testFUSEPath, e.Path))
		return err == nil
	}
	var testFile *metadata.Entry
	children, _ := store.ListChildren(".")
	for _, e := range children {
		if isReadable(e) {
			testFile = e
			break
		}
	}

	if testFile == nil {
		// Search in subdirectories
		for _, e := range children {
			if e.IsDir {
				subChildren, _ := store.ListChildren(e.Path)
				for _, sc := range subChildren {
					if isReadable(sc) {
						testFile = sc
						break
					}
				}
				if testFile != nil {
					break
				}
			}
		}
	}

	if testFile == nil {
		t.Skip("No small files found in metadata")
	}

	t.Logf("Testing read of %s (size=%d)", testFile.Path, testFile.Size)

	// Read via NFS mount
	nfsPath := filepath.Join(mountPoint, testFile.Path)
	start := time.Now()
	nfsData, err := os.ReadFile(nfsPath)
	nfsDur := time.Since(start)
	if err != nil {
		t.Fatalf("ReadFile via NFS: %v", err)
	}

	// Read same file directly via FUSE for comparison
	fusePath := filepath.Join(testFUSEPath, testFile.Path)
	fuseData, err := os.ReadFile(fusePath)
	if err != nil {
		t.Fatalf("ReadFile via FUSE: %v", err)
	}

	// Verify data integrity
	nfsHash := sha256.Sum256(nfsData)
	fuseHash := sha256.Sum256(fuseData)

	if nfsHash != fuseHash {
		t.Fatalf("DATA INTEGRITY FAILURE: NFS SHA256=%x, FUSE SHA256=%x", nfsHash[:8], fuseHash[:8])
	}

	t.Logf("Read OK: %d bytes, SHA256 match, NFS read took %v", len(nfsData), nfsDur)
}

func TestNFSReadLargeFile(t *testing.T) {
	srv, store := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv)

	// Find a larger file (>1MB, skip ._* files)
	var testFile *metadata.Entry
	children, _ := store.ListChildren(".")
	for _, e := range children {
		if e.IsDir {
			subChildren, _ := store.ListChildren(e.Path)
			for _, sc := range subChildren {
				if !sc.IsDir && sc.Size > 1*1024*1024 && sc.Size < 100*1024*1024 &&
					!strings.HasPrefix(sc.Name, "._") &&
					func() bool { _, err := os.Stat(filepath.Join(testFUSEPath, sc.Path)); return err == nil }() {
					testFile = sc
					break
				}
			}
		}
		if testFile != nil {
			break
		}
	}

	if testFile == nil {
		t.Skip("No medium-sized files found")
	}

	t.Logf("Testing large read of %s (size=%d, %.1fMB)",
		testFile.Path, testFile.Size, float64(testFile.Size)/(1024*1024))

	nfsPath := filepath.Join(mountPoint, testFile.Path)

	// Read via NFS
	start := time.Now()
	nfsFile, err := os.Open(nfsPath)
	if err != nil {
		t.Fatalf("Open via NFS: %v", err)
	}
	nfsHash := sha256.New()
	nfsBytes, err := io.Copy(nfsHash, nfsFile)
	nfsFile.Close()
	nfsDur := time.Since(start)

	if err != nil {
		t.Fatalf("Read via NFS: %v", err)
	}

	// Read via FUSE for comparison
	fusePath := filepath.Join(testFUSEPath, testFile.Path)
	fuseFile, err := os.Open(fusePath)
	if err != nil {
		t.Fatalf("Open via FUSE: %v", err)
	}
	fuseHash := sha256.New()
	io.Copy(fuseHash, fuseFile)
	fuseFile.Close()

	if string(nfsHash.Sum(nil)) != string(fuseHash.Sum(nil)) {
		t.Fatal("DATA INTEGRITY FAILURE: SHA256 mismatch between NFS and FUSE reads")
	}

	throughput := float64(nfsBytes) / nfsDur.Seconds() / (1024 * 1024)
	t.Logf("Read OK: %d bytes (%.1fMB), SHA256 match, %.1f MB/s via NFS", nfsBytes, float64(nfsBytes)/(1024*1024), throughput)
}

func TestNFSFDPoolStats(t *testing.T) {
	srv, _ := setupReadTestServer(t)
	mountPoint := mountNFS(t, srv)

	// Read a few files to populate the fd pool
	entries, _ := os.ReadDir(mountPoint)
	readCount := 0
	for _, e := range entries {
		if !e.IsDir() {
			info, _ := e.Info()
			if info != nil && info.Size() > 0 && info.Size() < 1024*1024 {
				path := filepath.Join(mountPoint, e.Name())
				f, err := os.Open(path)
				if err == nil {
					buf := make([]byte, 4096)
					f.Read(buf)
					f.Close()
					readCount++
				}
				if readCount >= 3 {
					break
				}
			}
		}
	}

	open, active := srv.handler.fdPool.Stats()
	t.Logf("FD pool: %d open, %d active (after %d reads)", open, active, readCount)
}
