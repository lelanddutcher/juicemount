package nfs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/testguard"
)

func TestCellularContentGuardKeepsLocalServingAndWritesButRefusesFUSEMiss(t *testing.T) {
	t.Setenv(testguard.RefuseFUSEContentReadsEnv, "1")
	jfs, store, fuseRoot := newIntegrityHarness(t)
	h := jfs.handler
	h.SetCacheReader(cache.NewReader(t.TempDir(), cache.DefaultBlockSize, nil))

	// Metadata operations remain mirror-only and available.
	seedFile(t, jfs, store, fuseRoot, "media/clip.mov", []byte("original-media"))
	sidecarName := "media/._clip.mov"
	sidecarBody := []byte("local-appledouble")
	seedFile(t, jfs, store, fuseRoot, sidecarName, sidecarBody)
	if _, err := jfs.Stat("media/clip.mov"); err != nil {
		t.Fatalf("metadata Stat under guard: %v", err)
	}
	if got, err := jfs.ReadDir("media"); err != nil || len(got) != 2 {
		t.Fatalf("mirror ReadDir under guard = %d, %v", len(got), err)
	}

	// A complete, mirror-validated RAM sidecar hit is still served and never
	// acquires a FUSE descriptor.
	entry := store.LookupByPath(sidecarName)
	if entry == nil {
		t.Fatal("sidecar missing from mirror")
	}
	h.sidecar.put(sidecarName, sidecarBody, entry.Mtime.Unix(), entry.Size)
	localFile, err := jfs.OpenFile(sidecarName, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	local := localFile.(*cachedFile)
	buf := make([]byte, len(sidecarBody))
	if n, readErr := local.ReadAt(buf, 0); readErr != nil || n != len(buf) || !bytes.Equal(buf, sidecarBody) {
		t.Fatalf("local sidecar hit = %d, %v, %q", n, readErr, buf)
	}
	if local.fuseFD != nil {
		t.Fatal("local cache hit acquired a FUSE descriptor")
	}
	_ = local.Close()

	// A direct-SSD miss is refused before lazy FUSE acquisition. The error is
	// both test-identifiable and compatible with the existing NFS offline map,
	// and contains no source path.
	coldFile, err := jfs.OpenFile("media/clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	cold := coldFile.(*cachedFile)
	_, readErr := cold.ReadAt(make([]byte, 4), 0)
	if !errors.Is(readErr, testguard.ErrFUSEContentReadRefused) ||
		!errors.Is(readErr, pin.ErrOfflineNotAvailable) {
		t.Fatalf("cold read error = %v, want joined test/offline refusal", readErr)
	}
	if strings.Contains(readErr.Error(), "clip.mov") || strings.Contains(readErr.Error(), fuseRoot) {
		t.Fatalf("guard error leaked a source path: %q", readErr)
	}
	if cold.fuseFD != nil {
		t.Fatal("guarded miss acquired a FUSE descriptor")
	}
	_ = cold.Close()

	// The guard is read-only. Existing writes retain their ordinary FUSE path.
	wf, err := jfs.OpenFile("media/clip.mov", os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("write open under guard: %v", err)
	}
	if n, writeErr := wf.(interface {
		WriteAt([]byte, int64) (int, error)
	}).WriteAt([]byte("NEW"), 0); writeErr != nil || n != 3 {
		t.Fatalf("write under guard = %d, %v", n, writeErr)
	}
	if err := wf.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(fuseRoot, "media/clip.mov"))
	if err != nil || !bytes.HasPrefix(got, []byte("NEW")) {
		t.Fatalf("write result = %q, %v", got, err)
	}
}

func TestCellularContentGuardCoversMetadataMissFileAndBackgroundReaders(t *testing.T) {
	t.Setenv(testguard.RefuseFUSEContentReadsEnv, "1")
	dir := t.TempDir()
	p := filepath.Join(dir, "local.bin")
	if err := os.WriteFile(p, []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()

	h := &JuiceMountHandler{testRefuseFUSEContentReads: true}
	bf := &billyFile{File: fd, name: "local.bin", handler: h}
	if _, err := bf.ReadAt(make([]byte, 1), 0); !errors.Is(err, testguard.ErrFUSEContentReadRefused) {
		t.Fatalf("billyFile.ReadAt = %v, want test refusal", err)
	}
	if _, err := bf.Read(make([]byte, 1)); !errors.Is(err, testguard.ErrFUSEContentReadRefused) {
		t.Fatalf("billyFile.Read = %v, want test refusal", err)
	}

	rm := NewReadaheadManager(dir, nil, nil)
	rm.refuseFUSEContentReads = true
	defer rm.Stop()
	for i := 0; i < 10; i++ {
		rm.OnRead(1, int64(i)*readaheadBlockSize, readaheadBlockSize, "local.bin")
	}
	if triggered, prefetched := rm.Stats(); triggered != 0 || prefetched != 0 {
		t.Fatalf("guarded readahead = triggered %d prefetched %d", triggered, prefetched)
	}

	mb := NewMemoryBuffer(1024, 4096)
	mb.refuseFUSEContentReads = true
	defer mb.Stop()
	mb.mu.Lock()
	mb.entries["resident"] = &memBufEntry{data: []byte("ram"), size: 3}
	mb.totalSize = 3
	mb.mu.Unlock()
	if got := mb.Get("resident", 3, p); !bytes.Equal(got, []byte("ram")) {
		t.Fatalf("resident RAM hit = %q", got)
	}
	if got := mb.Get("miss", 5, p); got != nil {
		t.Fatalf("guarded RAM miss unexpectedly loaded %q", got)
	}
	mb.mu.Lock()
	_, spawned := mb.entries["miss"]
	mb.mu.Unlock()
	if spawned {
		t.Fatal("guarded RAM miss spawned a FUSE loader")
	}
}
