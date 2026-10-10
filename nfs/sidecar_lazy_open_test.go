package nfs

import (
	"bytes"
	"errors"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
)

func TestSidecarRAMOpenDoesNotAcquireBackend(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		name := "nil-reader"
		if disabled {
			name = "disabled-reader"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			h := jfs.handler
			if disabled {
				h.SetCacheReader(cache.NewReader(t.TempDir(), cache.DefaultBlockSize, nil))
				previous := cacheReaderServeEnabled
				cacheReaderServeEnabled = false
				t.Cleanup(func() { cacheReaderServeEnabled = previous })
			}
			const rel = "media/._clip.mov"
			body := []byte("complete-metadata-body")
			seedStrongSidecar(t, jfs, store, root, rel, body)
			if !h.warmSidecar(rel, filepath.Join(root, rel)) {
				t.Fatal("warm failed")
			}
			f, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			cf, ok := f.(*cachedFile)
			if !ok {
				t.Fatalf("read handle=%T", f)
			}
			if cf.fuseFD != nil {
				t.Error("complete RAM metadata hit acquired backend descriptor at Open")
			}
			got := make([]byte, len(body))
			if n, err := f.ReadAt(got, 0); n != len(body) || err != nil || !bytes.Equal(got, body) {
				t.Fatalf("ReadAt=%d,%v body=%q", n, err, got)
			}
			if cf.fuseFD != nil {
				t.Error("complete RAM metadata hit acquired backend descriptor at ReadAt")
			}
		})
	}
}

// Strong proofs require actual descriptor identity, unlike seedFile's synthetic
// mirror inode (which is useful for its unrelated FDPool replacement tests).
func seedStrongSidecar(t *testing.T, jfs *juiceFS, store *metadata.Store, root, rel string, body []byte) {
	t.Helper()
	seedFile(t, jfs, store, root, rel, body)
	info, err := os.Stat(filepath.Join(root, rel))
	if err != nil {
		t.Fatal(err)
	}
	entry := metadata.MakeEntry(rel, false, info.Size(), info.ModTime(), info.Sys().(*syscall.Stat_t).Ino)
	entry.MtimeNsecKnown = true
	if err := store.Insert(entry); err != nil {
		t.Fatal(err)
	}
}

func TestStrongSidecarProofInvalidation(t *testing.T) {
	for _, change := range []string{"inode", "nanosecond", "revision", "writer", "retag", "evict", "legacy-put"} {
		t.Run(change, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			h := jfs.handler
			const rel = "._movie.mov"
			body := []byte("verified-body")
			seedStrongSidecar(t, jfs, store, root, rel, body)
			if !h.warmSidecar(rel, filepath.Join(root, rel)) {
				t.Fatal("warm")
			}
			raw, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			f := raw.(*cachedFile)
			if size, ok, err := f.CompleteCachedSize(); size != int64(len(body)) || !ok || err != nil {
				t.Fatalf("size=%d,%v,%v", size, ok, err)
			}
			check, release, err := f.BeginReadAdmission()
			if err != nil {
				t.Fatal(err)
			}
			if release != nil {
				defer release()
			}
			entry := store.LookupByPath(rel)
			switch change {
			case "inode":
				entry.Inode++
				err = store.Insert(entry)
			case "nanosecond":
				entry.Mtime = entry.Mtime.Add(time.Nanosecond)
				err = store.Insert(entry)
			case "revision":
				err = store.Insert(entry)
			case "writer":
				h.incActiveWriter(rel)
				h.decActiveWriter(rel)
			case "retag":
				h.sidecar.retag(rel, entry.Mtime.Unix(), entry.Size)
			case "evict":
				h.sidecar.invalidate(rel)
			case "legacy-put":
				h.sidecar.put(rel, body, entry.Mtime.Unix(), entry.Size)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := check(); !errors.Is(err, pin.ErrSpoolIncomplete) {
				t.Fatalf("reply admission=%v", err)
			}
			out := make([]byte, len(body))
			if n, err := f.ReadAt(out, 0); n != 0 || !errors.Is(err, pin.ErrSpoolIncomplete) {
				t.Fatalf("read=%d,%v", n, err)
			}
			if f.fuseFD != nil {
				t.Fatal("invalid clamped proof fell through to backend")
			}
		})
	}
}

func TestStrongSidecarRejectsStaleLowAndOldDescriptor(t *testing.T) {
	for _, change := range []string{"stale-low", "old-descriptor"} {
		t.Run(change, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			h := jfs.handler
			const rel = "._movie.mov"
			body := []byte("body-with-more-than-a-prefix")
			seedStrongSidecar(t, jfs, store, root, rel, body)
			fd, err := os.Open(filepath.Join(root, rel))
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()
			if change == "stale-low" {
				e := store.LookupByPath(rel)
				e.Size = 4
				if err := store.Insert(e); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(filepath.Join(root, rel), filepath.Join(root, "old")); err != nil {
					t.Fatal(err)
				}
				seedStrongSidecar(t, jfs, store, root, rel, bytes.Repeat([]byte{'x'}, len(body)))
			}
			h.cacheWrittenMetadataFromFUSE(rel, fd)
			if h.strongSidecarRead(rel) != nil {
				t.Fatal("unbound/incomplete body became authoritative")
			}
			if change == "stale-low" {
				h.warmSidecar(rel, filepath.Join(root, rel))
				if h.strongSidecarRead(rel) != nil {
					t.Fatal("warmer admitted stale-low prefix")
				}
				f := &cachedFile{fusePath: filepath.Join(root, rel)}
				defer f.Close()
				if size, err := f.AuthoritativeSize(); size != int64(len(body)) || err != nil {
					t.Fatalf("size=%d,%v", size, err)
				}
			}
		})
	}
}

func TestStrongSidecarSnapshotRejectsInterveningWriter(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	h := jfs.handler
	const rel = "._movie.mov"
	body := []byte("body")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	proof := h.beginStrongSidecarRead(rel)
	if proof == nil {
		t.Fatal("no pre-I/O identity")
	}
	h.incActiveWriter(rel)
	h.decActiveWriter(rel)
	h.publishStrongSidecarRead(proof, body)
	if h.strongSidecarRead(rel) != nil {
		t.Fatal("read spanning a writer was published")
	}
}

func TestStrongSidecarRestoredBodiesRemainUntrusted(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	h := jfs.handler
	const rel = "._movie.mov"
	body := []byte("body")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	if !h.warmSidecar(rel, filepath.Join(root, rel)) {
		t.Fatal("warm")
	}
	if h.strongSidecarRead(rel) == nil {
		t.Fatal("no proof before save")
	}
	h.sidecar.persistPath = filepath.Join(t.TempDir(), "cache.gob")
	h.sidecar.saveToDisk()
	restored := newSidecarCache()
	restored.persistPath = h.sidecar.persistPath
	restored.loadFromDisk()
	h.sidecar = restored
	if h.strongSidecarRead(rel) != nil {
		t.Fatal("process-local proof survived restart")
	}
	if _, ok := restored.get(rel, store.LookupByPath(rel).Mtime.Unix(), int64(len(body))); !ok {
		t.Fatal("fixture did not restore a legacy body")
	}
}

func TestAuthoritativeSizeReturnsDescriptorFailure(t *testing.T) {
	for _, closed := range []bool{false, true} {
		f := &cachedFile{fusePath: filepath.Join(t.TempDir(), "absent")}
		if closed {
			fd, err := os.Create(filepath.Join(t.TempDir(), "closed"))
			if err != nil {
				t.Fatal(err)
			}
			fd.Close()
			f.fuseFD = fd
		}
		if _, err := f.AuthoritativeSize(); err == nil {
			t.Fatal("missing/closed backend accepted as mirror EOF")
		}
	}
}

func TestStrongSidecarReadReturnsEOF(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const rel = "._movie.mov"
	body := []byte("body")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	jfs.handler.warmSidecar(rel, filepath.Join(root, rel))
	f, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	n, err := f.ReadAt(buf, 0)
	if n != len(body) || !errors.Is(err, io.EOF) || !bytes.Equal(buf[:n], body) {
		t.Fatalf("read=%d,%v", n, err)
	}
}

func TestStrongSidecarWarmerDoesNotTakeForegroundStatGate(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const rel = "._movie.mov"
	body := []byte("body")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	for i := 0; i < cap(fuseFstatGate); i++ {
		fuseFstatGate <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(fuseFstatGate); i++ {
			<-fuseFstatGate
		}
	}()
	if !jfs.handler.warmSidecar(rel, filepath.Join(root, rel)) || jfs.handler.strongSidecarRead(rel) == nil {
		t.Fatal("warmer used foreground stat budget")
	}
}

func TestStrongSidecarPartialDemandReadCannotMintProof(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const rel = "._movie.mov"
	body := []byte("body-with-a-tail")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	f, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if n, err := f.ReadAt(make([]byte, 4), 0); n != 4 || err != nil {
		t.Fatalf("partialread=%d,%v", n, err)
	}
	if jfs.handler.strongSidecarRead(rel) != nil {
		t.Fatal("partial demand read claimed complete body")
	}
}

func TestStrongSidecarConcurrentDemandCandidates(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	const rel = "._movie.mov"
	body := []byte("body-with-a-tail")
	seedStrongSidecar(t, jfs, store, root, rel, body)
	raw, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	f := raw.(*cachedFile)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.AuthoritativeSize(); err != nil {
				t.Error(err)
				return
			}
			buf := make([]byte, len(body))
			n, err := f.ReadAt(buf, 0)
			if n != len(body) || err != nil || !bytes.Equal(buf, body) {
				t.Errorf("read=%d,%v", n, err)
			}
		}()
	}
	wg.Wait()
	if jfs.handler.strongSidecarRead(rel) == nil {
		t.Fatal("no complete candidate survived")
	}
}

func TestStrongSidecarColdTerminalProofLifetime(t *testing.T) {
	for _, change := range []string{"evict", "writer", "identity"} {
		t.Run(change, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			const rel = "._movie.mov"
			body := []byte("complete-cold-metadata")
			seedStrongSidecar(t, jfs, store, root, rel, body)
			raw, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			f := raw.(*cachedFile)
			check, release, err := f.BeginReadAdmission()
			if err != nil {
				t.Fatal(err)
			}
			if release != nil {
				defer release()
			}
			buf := make([]byte, len(body))
			if n, err := f.ReadAt(buf, 0); n != len(body) || err != nil || !bytes.Equal(buf, body) {
				t.Fatalf("read=%d,%v", n, err)
			}
			if f.sidecarProof != nil {
				t.Fatal("cold read replaced immutable Open-time proof")
			}
			proof := f.completedSidecarSourceProof()
			if proof == nil || !bytes.Equal(proof.data, body) {
				t.Fatal("no complete byte-bound proof")
			}
			clear(buf)
			if !bytes.Equal(proof.data, body) {
				t.Fatal("proof borrowed caller's reusable buffer")
			}
			switch change {
			case "evict":
				jfs.handler.sidecar.invalidate(rel)
			case "writer":
				jfs.handler.incActiveWriter(rel)
				jfs.handler.decActiveWriter(rel)
			case "identity":
				e := store.LookupByPath(rel)
				e.Mtime = e.Mtime.Add(time.Nanosecond)
				if err := store.Insert(e); err != nil {
					t.Fatal(err)
				}
			}
			size, ok, err := f.CompleteCachedSize()
			if change == "evict" {
				if size != int64(len(body)) || !ok || err != nil || check() != nil {
					t.Fatalf("eviction invalidated already-read bytes: %d,%v,%v", size, ok, err)
				}
			} else if ok || !errors.Is(err, pin.ErrSpoolIncomplete) || !errors.Is(check(), pin.ErrSpoolIncomplete) {
				t.Fatalf("mutated identity survived sizing/final admission: %d,%v,%v", size, ok, err)
			}
		})
	}
}
