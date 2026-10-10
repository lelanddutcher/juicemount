package metadata

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testCachePathBinding = CachePathBinding{Digest: strings.Repeat("a", 64), FormatUUID: "format-one"}

func cachePathTestEntry(name string, dir bool, inode uint64, sec int64) *Entry {
	e := MakeEntry(name, dir, 12, time.Unix(sec, 123), inode)
	e.MtimeNsecKnown = true
	return e
}

func cachePathTestProof(o CachePathObservation, uuid string) RedisPathProof {
	proof := RedisPathProof{FormatUUID: uuid, Path: o.Path, Root: RedisPathNode{Path: ".", Inode: 1, ParentInode: 1, Type: jfsTypeDir}, Nodes: make([]RedisPathNode, len(o.Chain))}
	parent := uint64(1)
	for i, n := range o.Chain {
		typ := uint8(jfsTypeFile)
		if n.IsDir {
			typ = jfsTypeDir
		}
		proof.Nodes[i] = RedisPathNode{Path: n.Path, Inode: n.Inode, ParentInode: parent, Size: uint64(n.Size), Type: typ, MtimeSec: n.MtimeSec, MtimeNsec: n.MtimeNsec}
		parent = n.Inode
	}
	return proof
}

func qualifiedCachePathFixture(t *testing.T, dbPath string) (*Store, *CachePathEligibility, CachePathObservation, CachedFileIdentity) {
	t.Helper()
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for _, e := range []*Entry{cachePathTestEntry("a", true, 11, 1700000000), cachePathTestEntry("a/clip.mov", false, 22, 1700000001)} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := OpenCachePathEligibility(s)
	if err != nil {
		t.Fatal(err)
	}
	obs, err := gate.CaptureCachePath("a/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	id, ok := s.LookupCachedFileIdentity("a/clip.mov")
	if !ok {
		t.Fatal("missing resident file identity")
	}
	return s, gate, obs, id
}

func TestCachePathEligibilityWrongPathAndBindingCollision(t *testing.T) {
	_, gate, obs, id := qualifiedCachePathFixture(t, ":memory:")
	proof := cachePathTestProof(obs, testCachePathBinding.FormatUUID)
	proof.Path = "b/clip.mov" // identical inode/size/mtime, wrong edge path
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, proof); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("wrong path accepted: %v", err)
	}
	proof = cachePathTestProof(obs, testCachePathBinding.FormatUUID)
	proof.Nodes[0].Path = "b" // path field alone is insufficient
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, proof); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("wrong parent accepted: %v", err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, cachePathTestProof(obs, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	if !gate.CachePathEligible(testCachePathBinding, id) {
		t.Fatal("qualified path refused")
	}
	other := CachePathBinding{Digest: strings.Repeat("b", 64), FormatUUID: testCachePathBinding.FormatUUID}
	if gate.CachePathEligible(other, id) {
		t.Fatal("same UUID, different backend digest accepted")
	}
	other.FormatUUID = "other-uuid"
	other.Digest = testCachePathBinding.Digest
	if gate.CachePathEligible(other, id) {
		t.Fatal("same backend digest, different UUID accepted")
	}
}

func TestCachePathEligibilityChangedAncestorBetweenProofAndPublication(t *testing.T) {
	s, gate, obs, id := qualifiedCachePathFixture(t, ":memory:")
	proof := cachePathTestProof(obs, testCachePathBinding.FormatUUID)
	if err := s.Insert(cachePathTestEntry("a", true, 11, 1700000009)); err != nil {
		t.Fatal(err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, proof); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("changed ancestor accepted: %v", err)
	}
	if gate.CachePathEligible(testCachePathBinding, id) {
		t.Fatal("failed proof became eligible")
	}
}

func TestCachePathEligibilityLocalOnlyAndUnknownPrecision(t *testing.T) {
	s := newTestStore(t)
	gate, err := OpenCachePathEligibility(s)
	if err != nil {
		t.Fatal(err)
	}
	dir := cachePathTestEntry("local", true, 31, 1700000010)
	dir.LocalOnly = true
	if err := s.Insert(dir); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(cachePathTestEntry("local/clip.mov", false, 32, 1700000011)); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.CaptureCachePath("local/clip.mov"); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("local-only ancestor accepted: %v", err)
	}
	dir.LocalOnly = false
	if err := s.Insert(dir); err != nil {
		t.Fatal(err)
	}
	leaf := cachePathTestEntry("local/clip.mov", false, 32, 1700000011)
	leaf.MtimeNsecKnown = false
	if err := s.Insert(leaf); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.CaptureCachePath("local/clip.mov"); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("unknown timestamp precision accepted: %v", err)
	}
}

func TestCachePathEligibilityOfflineReopenAndReplacementABA(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []*Entry{cachePathTestEntry("a", true, 11, 1700000000), cachePathTestEntry("a/clip.mov", false, 22, 1700000001)} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := OpenCachePathEligibility(s)
	if err != nil {
		t.Fatal(err)
	}
	obs, err := gate.CaptureCachePath("a/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, cachePathTestProof(obs, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gate, err = OpenCachePathEligibility(s)
	if err != nil {
		t.Fatal(err)
	}
	id, ok := s.LookupCachedFileIdentity("a/clip.mov")
	if !ok || !gate.CachePathEligible(testCachePathBinding, id) {
		t.Fatal("durable path proof unavailable after offline reopen")
	}
	if err := s.Delete("a/clip.mov"); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(cachePathTestEntry("a/clip.mov", false, 22, 1700000001)); err != nil {
		t.Fatal(err)
	}
	newID, ok := s.LookupCachedFileIdentity("a/clip.mov")
	if !ok {
		t.Fatal("replacement not resident")
	}
	if gate.CachePathEligible(testCachePathBinding, id) || gate.CachePathEligible(testCachePathBinding, newID) {
		t.Fatal("same-path same-inode replacement inherited stale proof")
	}
	fresh, err := gate.CaptureCachePath("a/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, fresh, cachePathTestProof(fresh, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	if gate.CachePathEligible(testCachePathBinding, id) || !gate.CachePathEligible(testCachePathBinding, newID) {
		t.Fatal("requalification gave old handle successor bytes")
	}
}

func TestCachePathEligibilityHotCheckDoesNotWaitForSQLite(t *testing.T) {
	s, gate, obs, id := qualifiedCachePathFixture(t, ":memory:")
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, cachePathTestProof(obs, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	// Occupy the only pooled SQL connection. A hot check that touches SQLite
	// would wait here until release; a RAM-only check completes immediately.
	s.db.SetMaxOpenConns(1)
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() { done <- gate.CachePathEligible(testCachePathBinding, id) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("resident qualified path refused")
		}
	case <-time.After(500 * time.Millisecond):
		conn.Close()
		<-done
		t.Fatal("hot eligibility waited on occupied SQLite connection")
	}
	conn.Close()
}

func TestCachePathCaptureReleasesRAMLockBeforeSQLWait(t *testing.T) {
	s, gate, obs, id := qualifiedCachePathFixture(t, ":memory:")
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, cachePathTestProof(obs, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxOpenConns(1)
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waits := s.db.Stats().WaitCount
	captureDone := make(chan error, 1)
	go func() {
		_, err := gate.CaptureCachePath("a/clip.mov")
		captureDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.db.Stats().WaitCount == waits && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.db.Stats().WaitCount == waits {
		t.Fatal("capture never reached held SQLite connection")
	}
	// This writer would queue behind a capture that kept s.mu.RLock across
	// SQL. Go's RWMutex then blocks new hot readers behind the queued writer.
	writerDone := make(chan struct{})
	go func() {
		s.mu.Lock()
		s.mu.Unlock()
		close(writerDone)
	}()
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Store RAM writer blocked behind held SQLite capture")
	}
	hotDone := make(chan bool, 1)
	go func() { hotDone <- gate.CachePathEligible(testCachePathBinding, id) }()
	select {
	case ok := <-hotDone:
		if !ok {
			t.Fatal("qualified hot read refused while capture waits on SQL")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("qualified hot read blocked while capture waits on SQL")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-captureDone; err != nil {
		t.Fatal(err)
	}
}

func TestCachePathEligibilityRejectsNonRegularLeafModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode fs.FileMode
	}{
		{"symlink", fs.ModeSymlink},
		{"named-pipe", fs.ModeNamedPipe},
		{"socket", fs.ModeSocket},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			if err := s.Insert(cachePathTestEntry("a", true, 11, 1700000000)); err != nil {
				t.Fatal(err)
			}
			leaf := cachePathTestEntry("a/clip.mov", false, 22, 1700000001)
			leaf.Mode = leaf.Mode.Perm() | tc.mode
			if err := s.Insert(leaf); err != nil {
				t.Fatal(err)
			}
			gate, err := OpenCachePathEligibility(s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := gate.CaptureCachePath(leaf.Path); !errors.Is(err, ErrCachePathIneligible) {
				t.Fatalf("non-regular RAM leaf admitted: %v", err)
			}
			if _, err := durableCachePathNode(s.db, leaf.Path); !errors.Is(err, ErrCachePathIneligible) {
				t.Fatalf("non-regular durable leaf admitted: %v", err)
			}
		})
	}
}

func TestCachePathEligibilityRejectsModeChangeDuringConfirmation(t *testing.T) {
	for _, mode := range []fs.FileMode{fs.ModeSymlink, fs.ModeNamedPipe} {
		t.Run(mode.String(), func(t *testing.T) {
			s, gate, obs, _ := qualifiedCachePathFixture(t, ":memory:")
			proof := cachePathTestProof(obs, testCachePathBinding.FormatUUID)
			changed := cachePathTestEntry("a/clip.mov", false, 22, 1700000001)
			changed.Mode = changed.Mode.Perm() | mode
			if err := s.Insert(changed); err != nil {
				t.Fatal(err)
			}
			if err := gate.ConfirmCachePath(testCachePathBinding, obs, proof); !errors.Is(err, ErrCachePathIneligible) {
				t.Fatalf("regular-file proof qualified changed type: %v", err)
			}
		})
	}
}

func TestCachePathEligibilityRejectsDurableModeMismatch(t *testing.T) {
	s, gate, obs, _ := qualifiedCachePathFixture(t, ":memory:")
	proof := cachePathTestProof(obs, testCachePathBinding.FormatUUID)
	// Emulate a corrupt/external SQL row whose token and store epoch still
	// match RAM. This isolates the durable mode check from the normal revision
	// and epoch fences, which already reject an ordinary Store mode change.
	if _, err := s.db.Exec(`DROP TRIGGER mirror_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE entries SET mode=? WHERE path=?`, uint32(fs.ModeSymlink|0644), obs.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.CaptureCachePath(obs.Path); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("capture ignored durable symlink mode: %v", err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, obs, proof); !errors.Is(err, ErrCachePathIneligible) {
		t.Fatalf("confirmation ignored durable symlink mode: %v", err)
	}
}

func TestCachePathObservationCurrentSeesChangedAncestor(t *testing.T) {
	s, gate, observed, retained := qualifiedCachePathFixture(t, ":memory:")
	if !gate.CachePathObservationCurrent(observed) {
		t.Fatal("fresh chain refused")
	}
	if err := s.Insert(cachePathTestEntry("a", true, 11, 1700000019)); err != nil {
		t.Fatal(err)
	}
	if !gate.CachePathObservationCurrent(observed) {
		if leaf, ok := s.LookupCachedFileIdentity(retained.Path); !ok || leaf != retained {
			t.Fatal("test did not keep the leaf identity unchanged")
		}
	} else {
		t.Fatal("changed ancestor retained in-flight observation")
	}
}

func TestCachePathContextLockDeadlinesLeaveHotReadsLive(t *testing.T) {
	for _, which := range []string{"writeMu", "cachePublicationMu"} {
		for _, op := range []string{"capture", "confirm"} {
			t.Run(which+"/"+op, func(t *testing.T) {
				s, gate, old, id := qualifiedCachePathFixture(t, ":memory:")
				if err := gate.ConfirmCachePath(testCachePathBinding, old, cachePathTestProof(old, testCachePathBinding.FormatUUID)); err != nil {
					t.Fatal(err)
				}
				leaf := cachePathTestEntry("a/new.mov", false, 23, 1700000002)
				if err := s.Insert(leaf); err != nil {
					t.Fatal(err)
				}
				candidate, err := gate.CaptureCachePath(leaf.Path)
				if err != nil {
					t.Fatal(err)
				}
				var unlock func()
				if which == "writeMu" {
					s.writeMu.Lock()
					unlock = s.writeMu.Unlock
				} else {
					s.cachePublicationMu.Lock()
					unlock = s.cachePublicationMu.Unlock
				}
				ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
				started := time.Now()
				if op == "capture" {
					_, err = gate.CaptureCachePathContext(ctx, leaf.Path)
				} else {
					err = gate.ConfirmCachePathContext(ctx, testCachePathBinding, candidate, cachePathTestProof(candidate, testCachePathBinding.FormatUUID))
				}
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
					unlock()
					t.Fatalf("bounded %s wait: err=%v elapsed=%s", which, err, time.Since(started))
				}
				if !gate.CachePathEligible(testCachePathBinding, id) {
					unlock()
					t.Fatal("hot qualified path blocked or refused during lock wait")
				}
				unlock()
				if op == "confirm" {
					newID, ok := s.LookupCachedFileIdentity(leaf.Path)
					if !ok || gate.CachePathEligible(testCachePathBinding, newID) {
						t.Fatal("timed-out confirm published eligibility after return")
					}
				}
			})
		}
	}
}

func TestCachePathContextSQLiteDeadlinesReleaseLocks(t *testing.T) {
	for _, op := range []string{"capture", "confirm"} {
		t.Run(op, func(t *testing.T) {
			s, gate, old, id := qualifiedCachePathFixture(t, ":memory:")
			if err := gate.ConfirmCachePath(testCachePathBinding, old, cachePathTestProof(old, testCachePathBinding.FormatUUID)); err != nil {
				t.Fatal(err)
			}
			leaf := cachePathTestEntry("a/new.mov", false, 23, 1700000002)
			if err := s.Insert(leaf); err != nil {
				t.Fatal(err)
			}
			candidate, err := gate.CaptureCachePath(leaf.Path)
			if err != nil {
				t.Fatal(err)
			}
			s.db.SetMaxOpenConns(1)
			conn, err := s.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
			started := time.Now()
			if op == "capture" {
				_, err = gate.CaptureCachePathContext(ctx, leaf.Path)
			} else {
				err = gate.ConfirmCachePathContext(ctx, testCachePathBinding, candidate, cachePathTestProof(candidate, testCachePathBinding.FormatUUID))
			}
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
				conn.Close()
				t.Fatalf("SQL deadline: err=%v elapsed=%s", err, time.Since(started))
			}
			if !gate.CachePathEligible(testCachePathBinding, id) {
				conn.Close()
				t.Fatal("hot check waited on occupied SQL connection")
			}
			if !s.writeMu.TryLock() {
				conn.Close()
				t.Fatal("writeMu retained after SQL deadline")
			}
			s.writeMu.Unlock()
			if !s.cachePublicationMu.TryLock() {
				conn.Close()
				t.Fatal("cachePublicationMu retained after SQL deadline")
			}
			s.cachePublicationMu.Unlock()
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if op == "confirm" {
				var count int
				if err := s.db.QueryRow(`SELECT count(*) FROM cache_path_eligibility WHERE path=?`, leaf.Path).Scan(&count); err != nil || count != 0 {
					t.Fatalf("timed-out SQL published row: count=%d err=%v", count, err)
				}
			}
		})
	}
}
