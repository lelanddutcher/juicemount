//go:build latency_acceptance

package nfs

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const p79LogicalURL = "redis://127.0.0.1:6379/0"

// The probe never dials Redis. Its deliberately unavailable direct Reader
// proves that the saved volume binding is independent of optional SSD reads.
func p79BoundSession(t *testing.T, store *metadata.Store, stateDir string) *cache.Session {
	t.Helper()
	session, err := cache.NewSession(cache.SessionConfig{
		Store: store, LogicalURL: p79LogicalURL, Options: &redis.Options{Addr: "127.0.0.1:6379"},
		StateDir: stateDir, VerifyReader: func(*cache.Reader) error { return cache.ErrReaderUnavailable },
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func p79SavedVolume(t *testing.T, store *metadata.Store) {
	t.Helper()
	digest, err := cache.BackendIdentity(p79LogicalURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SaveVolumeBinding(store, cache.VolumeBinding{
		Version: 1, Backend: digest, UUID: "P79-test-volume", BlockSize: cache.DefaultBlockSize,
	}); err != nil {
		t.Fatal(err)
	}
}

func p79RestoredSnapshot(t *testing.T, mutateSaved func(string)) (*juiceFS, *metadata.Store, *cache.Session, string, string, []byte, func()) {
	t.Helper()
	stateDir, root := t.TempDir(), t.TempDir()
	dbPath := filepath.Join(stateDir, "metadata.db")
	savePath := filepath.Join(stateDir, "sidecars.gob")
	const rel = "._movie.mov"
	body := bytes.Repeat([]byte{'s'}, 4096)
	func() {
		store, err := metadata.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		p79SavedVolume(t, store)
		session := p79BoundSession(t, store, stateDir)
		defer session.Stop()
		h := NewHandler(store, root)
		defer h.StopHandler()
		h.SetBoundCacheSession(session, nil, nil)
		h.SidecarPersistEnable(savePath)
		defer h.SidecarPersistStop()
		jfs := &juiceFS{handler: h}
		seedStrongSidecar(t, jfs, store, root, rel, body)
		if !h.warmSidecar(rel, filepath.Join(root, rel)) || h.beginSidecarSnapshotRead(rel) == nil {
			t.Fatal("complete descriptor capture did not create selected snapshot")
		}
	}()
	if mutateSaved != nil {
		mutateSaved(savePath)
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	session := p79BoundSession(t, store, stateDir)
	h := NewHandler(store, root)
	h.SetBoundCacheSession(session, nil, nil)
	h.SidecarPersistEnable(savePath)
	var once sync.Once
	release := func() {
		once.Do(func() {
			h.SidecarPersistStop()
			if !h.StopHandler() {
				t.Error("handler did not settle")
			}
			session.Stop()
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(release)
	return &juiceFS{handler: h}, store, session, root, rel, body, release
}

func p79Loopback(t *testing.T, h *JuiceMountHandler) (*latencyClient, func()) {
	t.Helper()
	server := nfslib.NewManagedServer(h)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			conn.Close()
			listener.Close()
			if !server.Quiesce(3 * time.Second) {
				t.Error("server did not quiesce")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("Serve did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return newLatencyClient(conn), stop
}

func p79WireRead(t *testing.T, client *latencyClient, h *JuiceMountHandler, rel string, offset uint64, count uint32, want []byte, wantEOF uint32) {
	t.Helper()
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{rel}), Offset: offset, Count: count})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := latencyOK(reply)
	if err != nil {
		t.Fatal(err)
	}
	if err := latencyPostAttr(reader); err != nil {
		t.Fatal(err)
	}
	gotCount, err := xdr.ReadUint32(reader)
	if err != nil {
		t.Fatal(err)
	}
	eof, err := xdr.ReadUint32(reader)
	if err != nil {
		t.Fatal(err)
	}
	data, err := latencyOpaque(reader)
	if err != nil || gotCount != uint32(len(want)) || eof != wantEOF || !bytes.Equal(data, want) || reader.Len() != 0 {
		t.Fatalf("READ count=%d want=%d EOF=%d want=%d err=%v trailing=%d", gotCount, len(want), eof, wantEOF, err, reader.Len())
	}
}

// A restored 4 KiB image must satisfy Finder's actual 1 MiB request and
// terminal EOF without a source file, Reader, Redis client or open SQL DB.
func TestP79RestoredSnapshotWideWireRead(t *testing.T) {
	jfs, store, _, root, rel, body, _ := p79RestoredSnapshot(t, nil)
	h := jfs.handler
	if h.strongSidecarRead(rel) != nil || h.beginSidecarSnapshotRead(rel) == nil {
		t.Fatal("restored read snapshot was confused with process-local source proof")
	}
	if _, ok := h.offlineMetadataSeed(rel); ok {
		t.Fatal("read snapshot became write seed")
	}
	if _, ok := h.sidecar.get(rel, store.LookupByPath(rel).Mtime.Unix(), int64(len(body))); ok {
		t.Fatal("read snapshot reached weak serve")
	}
	if h.sidecar.retag(rel, store.LookupByPath(rel).Mtime.Unix(), int64(len(body))) {
		t.Fatal("read snapshot downgraded by retag")
	}
	if err := os.Remove(filepath.Join(root, rel)); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Close(); err != nil {
		t.Fatal(err)
	}
	client, _ := p79Loopback(t, h)
	p79WireRead(t, client, h, rel, 0, 1<<20, body, 1)
	p79WireRead(t, client, h, rel, uint64(len(body)-4), 4, body[len(body)-4:], 1)
	p79WireRead(t, client, h, rel, uint64(len(body)), 1<<20, nil, 1)
	p79WireRead(t, client, h, rel, uint64(len(body)), 0, nil, 1)
}

func TestP79SnapshotRevokedBeforeReply(t *testing.T) {
	for _, change := range []string{"writer", "high-water", "same-size-revision", "larger-successor", "mode", "delete", "delete-recreate", "volume-refusal", "evict"} {
		t.Run(change, func(t *testing.T) {
			jfs, store, session, _, rel, body, _ := p79RestoredSnapshot(t, nil)
			raw, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			f := raw.(*cachedFile)
			if f.snapshotProof == nil {
				t.Fatal("snapshot handle absent")
			}
			check, release, err := f.BeginReadAdmission()
			if err != nil {
				t.Fatal(err)
			}
			if release != nil {
				defer release()
			}
			if size, ok, err := f.CompleteCachedSize(); size != int64(len(body)) || !ok || err != nil {
				t.Fatalf("size=%d,%v,%v", size, ok, err)
			}
			switch change {
			case "writer":
				jfs.handler.incActiveWriter(rel)
				jfs.handler.decActiveWriter(rel)
			case "high-water":
				jfs.handler.writeSizeMu.Lock()
				jfs.handler.writeSizes[rel] = int64(len(body) + 1)
				jfs.handler.writeSizeMu.Unlock()
			case "same-size-revision":
				if err := store.Insert(store.LookupByPath(rel)); err != nil {
					t.Fatal(err)
				}
			case "larger-successor":
				e := store.LookupByPath(rel)
				e.Size = int64(len(body) + 1)
				if err := store.Insert(e); err != nil {
					t.Fatal(err)
				}
			case "mode":
				if err := store.UpdateMode(rel, 0600); err != nil {
					t.Fatal(err)
				}
			case "delete":
				if err := store.Delete(rel); err != nil {
					t.Fatal(err)
				}
			case "delete-recreate":
				old := store.LookupByPath(rel)
				if err := store.Delete(rel); err != nil {
					t.Fatal(err)
				}
				next := metadata.MakeEntry(rel, false, int64(len(body)+1), old.Mtime, old.Inode+1)
				next.MtimeNsecKnown = true
				if err := store.Insert(next); err != nil {
					t.Fatal(err)
				}
			case "volume-refusal":
				session.RefuseIdentity(cache.ErrVolumeBinding)
			case "evict":
				jfs.handler.sidecar.invalidate(rel)
			}
			if size, ok, err := f.CompleteCachedSize(); size != 0 || ok || !errors.Is(err, pin.ErrSpoolIncomplete) {
				t.Fatalf("stale size=%d,%v,%v", size, ok, err)
			}
			if err := check(); !errors.Is(err, pin.ErrSpoolIncomplete) {
				t.Fatalf("stale final admission=%v", err)
			}
			if n, err := f.ReadAt(make([]byte, 1<<20), 0); n != 0 || !errors.Is(err, pin.ErrSpoolIncomplete) {
				t.Fatalf("stale read=%d,%v", n, err)
			}
			if f.fuseFD != nil {
				t.Fatal("revoked snapshot fell through to source")
			}
		})
	}
}

func TestP79SavedSnapshotRefusal(t *testing.T) {
	for _, change := range []string{"corrupt-body", "wrong-volume", "wrong-backend-digest", "wrong-revision", "wrong-mode"} {
		t.Run(change, func(t *testing.T) {
			mutate := func(saved string) {
				f, err := os.Open(saved)
				if err != nil {
					t.Fatal(err)
				}
				var pf sidecarPersistFile
				if err := gob.NewDecoder(f).Decode(&pf); err != nil {
					f.Close()
					t.Fatal(err)
				}
				f.Close()
				if len(pf.Entries) != 1 || pf.Entries[0].Snapshot == nil || len(pf.Blobs) != 1 {
					t.Fatal("missing saved image")
				}
				switch change {
				case "corrupt-body":
					pf.Blobs[0][0] ^= 0xff
				case "wrong-volume":
					pf.Entries[0].Snapshot.Volume.UUID = "other-volume"
				case "wrong-backend-digest":
					pf.Entries[0].Snapshot.Volume.Backend = strings.Repeat("a", 64)
				case "wrong-revision":
					pf.Entries[0].Snapshot.Token.Revision++
				case "wrong-mode":
					pf.Entries[0].Snapshot.Mode = 0600
				}
				f, err = os.Create(saved)
				if err != nil {
					t.Fatal(err)
				}
				if err := gob.NewEncoder(f).Encode(pf); err != nil {
					f.Close()
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			jfs, _, _, _, rel, _, _ := p79RestoredSnapshot(t, mutate)
			if jfs.handler.beginSidecarSnapshotRead(rel) != nil {
				t.Fatal("invalid saved image admitted")
			}
			if _, ok := jfs.handler.offlineMetadataSeed(rel); ok {
				t.Fatal("invalid read image became write seed")
			}
			if jfs.handler.strongSidecarRead(rel) != nil {
				t.Fatal("saved image became process-local source proof")
			}
			if entry := jfs.handler.sidecar.captureEntry(rel); entry != nil && entry.snapshot == nil {
				t.Fatal("invalid snapshot was downgraded to weak body")
			}
		})
	}
}

// Existing v2 bodies remain weak until an exact bounded source read proves
// complete EOF. The first oversized READ upgrades them for later restarts.
func TestP79LegacyWideReadUpgradesFromSource(t *testing.T) {
	mutate := func(saved string) {
		f, err := os.Open(saved)
		if err != nil {
			t.Fatal(err)
		}
		var pf sidecarPersistFile
		if err := gob.NewDecoder(f).Decode(&pf); err != nil {
			f.Close()
			t.Fatal(err)
		}
		f.Close()
		pf.Version = 2
		for i := range pf.Entries {
			pf.Entries[i].Snapshot = nil
		}
		f, err = os.Create(saved)
		if err != nil {
			t.Fatal(err)
		}
		if err := gob.NewEncoder(f).Encode(pf); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	jfs, _, _, root, rel, body, release := p79RestoredSnapshot(t, mutate)
	h := jfs.handler
	if h.beginSidecarSnapshotRead(rel) != nil || h.strongSidecarRead(rel) != nil {
		t.Fatal("v2 body gained terminal proof")
	}
	client, stopWire := p79Loopback(t, h)
	p79WireRead(t, client, h, rel, 0, 1<<20, body, 1) // onRead clamps to 4096 before ReadAt
	stopWire()
	if h.beginSidecarSnapshotRead(rel) == nil {
		t.Fatal("complete source read did not upgrade v2")
	}
	savePath := h.sidecar.persistPath
	release() // join saver, close first handler/session/SQLite before reopening
	if err := os.Remove(filepath.Join(root, rel)); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Dir(savePath)
	store, err := metadata.Open(filepath.Join(stateDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	session := p79BoundSession(t, store, stateDir)
	h2 := NewHandler(store, root)
	h2.SetBoundCacheSession(session, nil, nil)
	h2.SidecarPersistEnable(savePath)
	t.Cleanup(func() { h2.SidecarPersistStop(); h2.StopHandler(); session.Stop(); store.Close() })
	if h2.beginSidecarSnapshotRead(rel) == nil {
		t.Fatal("upgraded v3 image did not survive restart")
	}
	client2, stopWire2 := p79Loopback(t, h2)
	defer stopWire2()
	p79WireRead(t, client2, h2, rel, 0, 1<<20, body, 1) // physical source is absent

}

func TestP79LiveProofSeedAndPositiveRefusal(t *testing.T) {
	stateDir, root := t.TempDir(), t.TempDir()
	store, err := metadata.Open(filepath.Join(stateDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p79SavedVolume(t, store)
	session := p79BoundSession(t, store, stateDir)
	defer session.Stop()
	h := NewHandler(store, root)
	defer h.StopHandler()
	h.SetBoundCacheSession(session, nil, nil)
	jfs := &juiceFS{handler: h}
	const rel = "._movie.mov"
	body := bytes.Repeat([]byte{'b'}, 4096)
	seedStrongSidecar(t, jfs, store, root, rel, body)
	if !h.warmSidecar(rel, filepath.Join(root, rel)) || h.beginSidecarSnapshotRead(rel) == nil || h.strongSidecarRead(rel) == nil {
		t.Fatal("missing live source and read image")
	}
	if seed, ok := h.offlineMetadataSeed(rel); !ok || !bytes.Equal(seed, body) {
		t.Fatal("live source proof lost existing write-seed continuity")
	}
	session.RefuseIdentity(cache.ErrVolumeBinding)
	h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
	if h.beginSidecarSnapshotRead(rel) != nil || h.strongSidecarRead(rel) != nil {
		t.Fatal("known wrong volume retained read proof")
	}
	if _, ok := h.offlineMetadataSeed(rel); ok {
		t.Fatal("known wrong volume retained write seed")
	}
	raw, err := jfs.OpenFile(rel, os.O_RDONLY, 0)
	if raw != nil || !errors.Is(err, cache.ErrVolumeBinding) {
		if raw != nil {
			_ = raw.Close()
		}
		t.Fatalf("OpenFile after positive volume refusal = %T, %v", raw, err)
	}
}

// Mutate after the image body was copied but before onRead writes the reply.
// The old 4096-byte terminal clamp must not escape for a larger successor.
func TestP79SnapshotWireRefusesLargerSuccessorBeforeReply(t *testing.T) {
	jfs, store, _, _, rel, body, _ := p79RestoredSnapshot(t, nil)
	h := jfs.handler
	var calls atomic.Int32
	h.testReadCachedInfo = func(name string) {
		if name != rel {
			return
		}
		if calls.Add(1) == 2 {
			e := store.LookupByPath(rel)
			e.Size = int64(len(body) + 4096)
			if err := store.Insert(e); err != nil {
				t.Error(err)
			}
		}
	}
	client, stop := p79Loopback(t, h)
	defer stop()
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{rel}), Count: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(reply.body) < 4 || binary.BigEndian.Uint32(reply.body[:4]) != uint32(nfslib.NFSStatusJukebox) {
		t.Fatalf("copied old EOF escaped: cached-info calls=%d reply=%v", calls.Load(), reply.body)
	}
}

// A live source proof and its separately saved read image preserve the existing
// complete-body write seed. Drive the same two O_RDWR calls as NFS WRITE RPCs.
func TestP79LiveSnapshotOnlineWriteOfflineContinuation(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	jfs, spool, _, root := newSpoolWiredHandlerNoDrain(t)
	h := jfs.handler
	store := h.store
	p79SavedVolume(t, store)
	session := p79BoundSession(t, store, t.TempDir())
	t.Cleanup(session.Stop)
	h.SetBoundCacheSession(session, nil, nil)
	const rel = "._p79-write-continuation"
	body := bytes.Repeat([]byte{'w'}, 4096)
	seedStrongSidecar(t, jfs, store, root, rel, body)
	if !h.warmSidecar(rel, filepath.Join(root, rel)) || h.beginSidecarSnapshotRead(rel) == nil || h.strongSidecarRead(rel) == nil {
		t.Fatal("live source and read snapshot did not qualify")
	}
	first, err := jfs.OpenFile(rel, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := first.(*spoolWriteFile); !ok {
		_ = first.Close()
		t.Fatalf("first online WRITE used %T, want seeded spool", first)
	}
	if _, err := first.(io.WriterAt).WriteAt([]byte("online-prefix"), 0); err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil { // NFS closes each WRITE RPC handle.
		t.Fatal(err)
	}
	seeded, ok := spool.LookupActive(rel)
	if !ok {
		t.Fatal("online WRITE lost its seeded spool generation")
	}
	pin.SetOffline(true)
	second, err := jfs.OpenFile(rel, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("offline continuation: %v", err)
	}
	if _, ok := second.(*spoolWriteFile); !ok {
		_ = second.Close()
		t.Fatalf("offline WRITE used %T, want same spool shadow", second)
	}
	if _, err := second.(io.WriterAt).WriteAt([]byte("offline-tail"), 152); err != nil {
		_ = second.Close()
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	continued, ok := spool.LookupActive(rel)
	if !ok || continued != seeded {
		t.Fatal("offline continuation did not retain the online spool generation")
	}
	want := append([]byte(nil), body...)
	copy(want, []byte("online-prefix"))
	copy(want[152:], []byte("offline-tail"))
	got, err := os.ReadFile(continued.SpoolFilePath())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("spool body after both WRITE RPCs: size=%d want=%d err=%v", len(got), len(want), err)
	}
}

func TestP79RestoredReadSnapshotCannotSeedOfflineWrite(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	jfs, store, _, _, rel, _, _ := p79RestoredSnapshot(t, nil)
	if jfs.handler.beginSidecarSnapshotRead(rel) == nil || jfs.handler.strongSidecarRead(rel) != nil {
		t.Fatal("fixture lacks restored-only read image")
	}
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(t.TempDir(), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	jfs.handler.SetSpool(spool, nil) // An erroneous seed now succeeds, so refusal is discriminating.
	pin.SetOffline(true)
	if f, err := jfs.OpenFile(rel, os.O_RDWR, 0o644); err == nil {
		f.Close()
		t.Fatal("restored read image authorized a write seed")
	} else if !errors.Is(err, pin.ErrOfflineNotAvailable) {
		t.Fatalf("restored-only write refusal=%v", err)
	}
	if _, ok := spool.LookupActive(rel); ok {
		t.Fatal("restored read image created a spool write generation")
	}
}

// A selected session refused before capture must never mint an unscoped
// source proof. The actual READ route must also refuse the weak RAM fallback.
func TestP79RefusedScopeCannotWarmOrWireServe(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })
	jfs, store, root := newIntegrityHarness(t)
	h := jfs.handler
	p79SavedVolume(t, store)
	session := p79BoundSession(t, store, t.TempDir())
	t.Cleanup(session.Stop)
	h.SetBoundCacheSession(session, nil, nil)
	const rel = "._p79-refused-capture"
	body := bytes.Repeat([]byte{'r'}, 4096)
	seedStrongSidecar(t, jfs, store, root, rel, body)
	session.RefuseIdentity(cache.ErrVolumeBinding)
	h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
	_ = h.warmSidecar(rel, filepath.Join(root, rel))
	if h.strongSidecarRead(rel) != nil || h.beginSidecarSnapshotRead(rel) != nil {
		t.Fatal("refused selected scope minted a source or read proof")
	}
	if _, ok := h.offlineMetadataSeed(rel); ok {
		t.Fatal("refused selected scope became a write seed")
	}
	if err := os.Remove(filepath.Join(root, rel)); err != nil {
		t.Fatal(err)
	}
	client, stop := p79Loopback(t, h)
	defer stop()
	pending, err := client.send(100003, 6, latencyReadArgs{Handle: h.ToHandle(nil, []string{rel}), Count: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.body) < 4 || binary.BigEndian.Uint32(reply.body[:4]) == 0 || bytes.Contains(reply.body, body) {
		t.Fatalf("refused scope returned metadata READ: %v", reply.body)
	}
}

// A previously valid unbound proof cannot acquire selected-volume authority
// when a bound Session is attached later in the same process.
func TestP79UnscopedProofCannotGainSelectedBinding(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	h := jfs.handler
	const rel = "._p79-unscoped"
	body := bytes.Repeat([]byte{'u'}, 4096)
	seedStrongSidecar(t, jfs, store, root, rel, body)
	if !h.warmSidecar(rel, filepath.Join(root, rel)) || h.strongSidecarRead(rel) == nil {
		t.Fatal("unbound control did not establish ordinary live proof")
	}
	p79SavedVolume(t, store)
	session := p79BoundSession(t, store, t.TempDir())
	t.Cleanup(session.Stop)
	h.SetBoundCacheSession(session, nil, nil)
	if h.strongSidecarRead(rel) != nil || h.beginSidecarSnapshotRead(rel) != nil {
		t.Fatal("prebinding proof was promoted into selected scope")
	}
}
