package metadata

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mirrorLockStore(t *testing.T) *Store {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// mirrorDurableEntry bypasses the serving accessor, which can fall back to RAM.
func mirrorDurableEntry(t *testing.T, s *Store, name string) *Entry {
	t.Helper()
	e, err := scanEntry(s.db.QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatalf("durable row %q: %v", name, err)
	}
	return e
}

func TestMirrorCaptureOrdersCacheOnlySuccessor(t *testing.T) {
	for _, operation := range []string{"mirror", "directory"} {
		t.Run(operation, func(t *testing.T) {
			s := mirrorLockStore(t)
			stamp := time.Unix(1700000000, 0)
			name := "target"
			if operation == "mirror" {
				if err := s.Insert(MakeEntry(name, false, 42, stamp, 100)); err != nil {
					t.Fatal(err)
				}
			} else {
				s.InsertToCache(MakeEntry(name, true, 0, stamp, 0))
			}
			release, baseline := holdPublicationSQLPool(t, s)
			type result struct {
				token MirrorToken
				epoch StoreEpoch
				err   error
			}
			captureDone := make(chan result, 1)
			captureSettled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				var r result
				if operation == "mirror" {
					r.token, r.err = s.CaptureMirror(name)
				} else {
					r.token, r.epoch, r.err = s.CaptureDirectoryForFUSE(name)
				}
				captureDone <- r
			}()
			defer func() {
				release()
				if !captureSettled {
					select {
					case <-captureDone:
					case <-time.After(5 * time.Second):
						t.Error("capture did not settle")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("cache-only successor did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if s.mu.TryLock() {
				s.mu.Unlock()
			} else {
				t.Error("capture retained cache lock during SQL")
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				t.Fatal("capture did not retain publication gate")
			}
			go func() {
				if operation == "mirror" {
					s.InsertToCache(MakeEntry(name, false, 77, stamp.Add(time.Second), 101))
				} else {
					s.InsertToCache(MakeEntry(name, true, 0, stamp.Add(time.Second), 200))
				}
				successorDone <- struct{}{}
			}()
			successorStarted = true
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor overtook held capture")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			var captured result
			select {
			case captured = <-captureDone:
				captureSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not finish")
			}
			if captured.err != nil {
				t.Fatal(captured.err)
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not finish")
			}
			got := s.lookupByPathRAM(name)
			if operation == "mirror" {
				if captured.token.IsZero() || got == nil || got.Inode != 101 || got.Size != 77 || !got.Version.IsZero() {
					t.Fatalf("capture/successor mismatch: token=%+v cache=%+v", captured.token, got)
				}
				if alias := s.lookupByInodeRAM(100); alias == nil || alias.Path != name || alias.Inode != 101 || alias.Size != 77 || alias.Version != got.Version {
					t.Fatalf("old handle alias did not redirect to replacement: %+v", alias)
				}
				if durable := mirrorDurableEntry(t, s, name); durable == nil || durable.Inode != 100 || durable.Version != captured.token {
					t.Fatalf("capture durable result changed: %+v", durable)
				}
			} else {
				if captured.token.Incarnation != "" || captured.token.StoreID == "" || got == nil || got.Inode != 200 || !got.IsDir {
					t.Fatalf("directory capture/successor mismatch: %+v, %+v", captured, got)
				}
				if captured.epoch.CacheGeneration == 0 || captured.epoch.CacheGeneration == s.cacheGeneration {
					t.Fatalf("queued publication did not advance generation: captured=%+v current=%d", captured.epoch, s.cacheGeneration)
				}
				if _, err := s.ConfirmAbsentDirectoryFromFUSE(captured.token, captured.epoch, MakeEntry(name, true, 0, stamp, 999)); !errors.Is(err, ErrMirrorConflict) {
					t.Fatalf("old captured generation admitted confirmation: %v", err)
				}
			}
		})
	}
}

func TestMirrorPublishOrdersCacheOnlySuccessor(t *testing.T) {
	for _, operation := range []string{"publish", "confirm"} {
		for _, successor := range []string{"replace", "unlink"} {
			t.Run(operation+"/"+successor, func(t *testing.T) {
				s := mirrorLockStore(t)
				stamp := time.Unix(1700000000, 0)
				name := "target"
				input := MakeEntry(name, false, 42, stamp, 100)
				var expected MirrorToken
				var observed StoreEpoch
				var err error
				if operation == "confirm" {
					input = MakeEntry(name, true, 0, stamp, 999)
					s.InsertToCache(MakeEntry(name, true, 0, stamp, 0))
					expected, observed, err = s.CaptureDirectoryForFUSE(name)
				} else {
					expected, err = s.CaptureMirror(name)
				}
				if err != nil {
					t.Fatal(err)
				}
				release, baseline := holdPublicationSQLPool(t, s)
				type result struct {
					entry *Entry
					err   error
				}
				publishDone := make(chan result, 1)
				publishSettled := false
				successorDone := make(chan struct{}, 1)
				successorStarted := false
				successorSettled := false
				go func() {
					var r result
					if operation == "confirm" {
						r.entry, r.err = s.ConfirmAbsentDirectoryFromFUSE(expected, observed, input)
					} else {
						r.entry, r.err = s.PublishExpected(expected, input)
					}
					publishDone <- r
				}()
				defer func() {
					release()
					if !publishSettled {
						select {
						case <-publishDone:
						case <-time.After(5 * time.Second):
							t.Error("publisher did not settle")
						}
					}
					if successorStarted && !successorSettled {
						select {
						case <-successorDone:
						case <-time.After(5 * time.Second):
							t.Error("successor did not settle")
						}
					}
				}()
				waitForPublicationSQLWait(t, s, baseline)
				if s.mu.TryLock() {
					s.mu.Unlock()
				} else {
					t.Error("publisher retained cache lock during SQL")
				}
				if s.cachePublicationMu.TryLock() {
					s.cachePublicationMu.Unlock()
					t.Fatal("publisher did not retain publication gate")
				}
				go func() {
					if successor == "replace" {
						s.InsertToCache(MakeEntry(name, operation == "confirm", 77, stamp.Add(time.Second), 101))
					} else {
						s.DeleteFromCache(name)
					}
					successorDone <- struct{}{}
				}()
				successorStarted = true
				select {
				case <-successorDone:
					successorSettled = true
					t.Fatal("cache-only successor overtook held publisher")
				case <-time.After(25 * time.Millisecond):
				}
				release()
				var published result
				select {
				case published = <-publishDone:
					publishSettled = true
				case <-time.After(5 * time.Second):
					t.Fatal("publisher did not finish")
				}
				if published.err != nil || published.entry == nil {
					t.Fatalf("publisher = %+v", published)
				}
				select {
				case <-successorDone:
					successorSettled = true
				case <-time.After(5 * time.Second):
					t.Fatal("cache-only successor did not finish")
				}
				wantInode := uint64(100)
				if operation == "confirm" {
					wantInode = 0
				}
				proposalInode := uint64(100)
				if operation == "confirm" {
					proposalInode = 999
				}
				if published.entry.Inode != wantInode || published.entry.Version.IsZero() || input.Inode != proposalInode {
					t.Fatalf("committed result lost identity or changed caller: %+v, input=%+v", published.entry, input)
				}
				if durable := mirrorDurableEntry(t, s, name); durable == nil || durable.Inode != wantInode || durable.Version != published.entry.Version {
					t.Fatalf("durable publication mismatch: %+v versus %+v", durable, published.entry)
				}
				got := s.lookupByPathRAM(name)
				alias := s.lookupByInodeRAM(wantInode)
				if successor == "replace" {
					if got == nil || got.Inode != 101 || got.Size != 77 || !got.Version.IsZero() || alias == nil || alias.Path != name || alias.Inode != 101 || alias.Size != 77 || alias.Version != got.Version {
						t.Fatalf("replacement/old handle alias mismatch: cache=%+v alias=%+v", got, alias)
					}
				} else if got != nil || alias != nil {
					t.Fatalf("unlink retained authoritative cache/handle: cache=%+v alias=%+v", got, alias)
				}
			})
		}
	}
}

func TestMirrorDirectoryEligibilityAndErrorPrecedence(t *testing.T) {
	stamp := time.Unix(1700000000, 0)
	for _, fixture := range []struct {
		name      string
		seed      func(*testing.T, *Store)
		wantErr   bool
		wantInode uint64
	}{
		{"absent", nil, false, 999},
		{"zero-shadow-inode-zero", func(t *testing.T, s *Store) { s.InsertToCache(MakeEntry("target", true, 0, stamp, 0)) }, false, 0},
		{"cached-file", func(t *testing.T, s *Store) { s.InsertToCache(MakeEntry("target", false, 0, stamp, 1)) }, true, 0},
		{"durable-file", func(t *testing.T, s *Store) {
			if err := s.Insert(MakeEntry("target", false, 0, stamp, 1)); err != nil {
				t.Fatal(err)
			}
			s.DeleteFromCache("target")
		}, true, 0},
		{"zero-shadow-over-durable", func(t *testing.T, s *Store) {
			if err := s.Insert(MakeEntry("target", true, 0, stamp, 1)); err != nil {
				t.Fatal(err)
			}
			s.InsertToCache(MakeEntry("target", true, 0, stamp, 2))
		}, true, 0},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			s := mirrorLockStore(t)
			if fixture.seed != nil {
				fixture.seed(t, s)
			}
			expected, observed, err := s.CaptureDirectoryForFUSE("target")
			if fixture.wantErr {
				if !errors.Is(err, ErrMirrorConflict) {
					t.Fatalf("capture error=%v", err)
				}
				return
			}
			if err != nil || expected.Incarnation != "" || expected.StoreID == "" || observed.StoreID != expected.StoreID || observed.Epoch != expected.Epoch {
				t.Fatalf("directory capture = %+v, %+v, %v", expected, observed, err)
			}
			input := MakeEntry("target", true, 0, stamp, 999)
			published, err := s.ConfirmAbsentDirectoryFromFUSE(expected, observed, input)
			if err != nil || published == nil || published.Inode != fixture.wantInode || published.Version.IsZero() || input.Inode != 999 {
				t.Fatalf("directory confirmation = %+v, %v; input=%+v", published, err, input)
			}
			if durable := mirrorDurableEntry(t, s, "target"); durable == nil || durable.Inode != fixture.wantInode || durable.Version != published.Version {
				t.Fatalf("durable directory = %+v", durable)
			}
		})
	}
	t.Run("durable-directory-capture", func(t *testing.T) {
		s := mirrorLockStore(t)
		if err := s.Insert(MakeEntry("target", true, 0, stamp, 7)); err != nil {
			t.Fatal(err)
		}
		v, epoch, err := s.CaptureDirectoryForFUSE("target")
		if err != nil || v.Incarnation == "" || epoch.StoreID == "" {
			t.Fatalf("durable directory capture = %+v, %+v, %v", v, epoch, err)
		}
		if cached := s.lookupByPathRAM("target"); cached == nil || cached.Version != v {
			t.Fatalf("durable directory token not matched to cache: %+v", cached)
		}
	})
	t.Run("publish-rejects-cached-mismatch", func(t *testing.T) {
		s := mirrorLockStore(t)
		expected, err := s.CaptureMirror("target")
		if err != nil {
			t.Fatal(err)
		}
		s.InsertToCache(MakeEntry("target", false, 1, stamp, 7))
		if got, err := s.PublishExpected(expected, MakeEntry("target", false, 2, stamp, 8)); got != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("cached mismatch publication = %+v, %v", got, err)
		}
		if cached := s.lookupByPathRAM("target"); cached == nil || cached.Inode != 7 || cached.Size != 1 {
			t.Fatalf("cached mismatch changed RAM: %+v", cached)
		}
		if durable := mirrorDurableEntry(t, s, "target"); durable != nil {
			t.Fatalf("cached mismatch wrote SQL: %+v", durable)
		}
	})

	t.Run("publish-cache-conflict-before-SQL-error", func(t *testing.T) {
		s := mirrorLockStore(t)
		expected, err := s.CaptureMirror("target")
		if err != nil {
			t.Fatal(err)
		}
		s.InsertToCache(MakeEntry("target", false, 1, stamp, 7))
		if err := s.db.Close(); err != nil {
			t.Fatal(err)
		}
		if got, err := s.PublishExpected(expected, MakeEntry("target", false, 2, stamp, 8)); got != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("cache conflict did not precede unavailable SQL: %+v, %v", got, err)
		}
		if cached := s.lookupByPathRAM("target"); cached == nil || cached.Inode != 7 || cached.Size != 1 {
			t.Fatalf("cached mismatch changed RAM: %+v", cached)
		}
	})

	t.Run("SQL-error-before-cache-conflict", func(t *testing.T) {
		s := mirrorLockStore(t)
		if err := s.Insert(MakeEntry("target", true, 0, stamp, 1)); err != nil {
			t.Fatal(err)
		}
		s.InsertToCache(MakeEntry("target", false, 0, stamp, 2))
		if _, err := s.db.Exec(`DROP TABLE entries`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CaptureMirror("target"); err == nil || errors.Is(err, ErrMirrorConflict) || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("CaptureMirror precedence error=%v", err)
		}
		if _, _, err := s.CaptureDirectoryForFUSE("target"); err == nil || errors.Is(err, ErrMirrorConflict) || !strings.Contains(err.Error(), "entries") {
			t.Fatalf("CaptureDirectory precedence error=%v", err)
		}
	})
}

func TestConfirmDirectoryRejectsStaleAndReservedObservations(t *testing.T) {
	for _, change := range []string{"durable-epoch", "cache-generation", "wrong-store", "wrong-epoch", "prepared-name"} {
		t.Run(change, func(t *testing.T) {
			s := mirrorLockStore(t)
			stamp := time.Unix(1700000000, 0)
			expected, observed, err := s.CaptureDirectoryForFUSE("target")
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "durable-epoch":
				if err := s.Insert(MakeEntry("other", false, 1, stamp, 77)); err != nil {
					t.Fatal(err)
				}
			case "cache-generation":
				s.InsertToCache(MakeEntry("other", false, 1, stamp, 77))
			case "wrong-store":
				observed.StoreID = "different-store"
			case "wrong-epoch":
				observed.Epoch++
			case "prepared-name":
				if _, err := s.PrepareCreate(expected, "mount-generation"); err != nil {
					t.Fatal(err)
				}
				// Isolate the reservation check from the ordinary generation check.
				observed.CacheGeneration = s.cacheGeneration
			}
			if got, err := s.ConfirmAbsentDirectoryFromFUSE(expected, observed, MakeEntry("target", true, 0, stamp, 999)); got != nil || !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("%s confirmation = %+v, %v", change, got, err)
			}
			if got := s.lookupByPathRAM("target"); got != nil {
				t.Fatalf("%s confirmation changed cache: %+v", change, got)
			}
			if got := mirrorDurableEntry(t, s, "target"); got != nil {
				t.Fatalf("%s confirmation changed durable mirror: %+v", change, got)
			}
		})
	}
}

func TestMirrorPublicationFailureRollbackRetryAndReopen(t *testing.T) {
	for _, operation := range []string{"publish", "confirm"} {
		t.Run(operation, func(t *testing.T) {
			t.Setenv("JM_FTS_DEFER", "0")
			t.Setenv("JM_SERVE_FROM_SQLITE", "0")
			file := filepath.Join(t.TempDir(), "mirror.db")
			s, err := Open(file)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if s != nil {
					_ = s.Close()
				}
			})
			stamp := time.Unix(1700000000, 0)
			input := MakeEntry("target", false, 42, stamp, 100)
			var expected MirrorToken
			var observed StoreEpoch
			if operation == "confirm" {
				input = MakeEntry("target", true, 0, stamp, 999)
				s.InsertToCache(MakeEntry("target", true, 0, stamp, 0))
				expected, observed, err = s.CaptureDirectoryForFUSE("target")
			} else {
				expected, err = s.CaptureMirror("target")
			}
			if err != nil {
				t.Fatal(err)
			}
			before := s.lookupByPathRAM("target")
			if _, err := s.db.Exec(`CREATE TRIGGER fail_publication BEFORE INSERT ON entries WHEN NEW.path='target' BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
				t.Fatal(err)
			}
			publish := func() (*Entry, error) {
				if operation == "confirm" {
					return s.ConfirmAbsentDirectoryFromFUSE(expected, observed, input)
				}
				return s.PublishExpected(expected, input)
			}
			if got, err := publish(); err == nil || got != nil {
				t.Fatalf("trigger accepted publication: %+v, %v", got, err)
			}
			after := s.lookupByPathRAM("target")
			if (before == nil) != (after == nil) || (before != nil && (after.Path != before.Path || after.Inode != before.Inode || after.Version != before.Version)) {
				t.Fatalf("failed publication changed cache: before=%+v after=%+v", before, after)
			}
			if durable := mirrorDurableEntry(t, s, "target"); durable != nil {
				t.Fatalf("failed publication wrote row: %+v", durable)
			}
			for _, lock := range []struct {
				name   string
				try    func() bool
				unlock func()
			}{
				{"writer", s.writeMu.TryLock, s.writeMu.Unlock},
				{"publication", s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock},
				{"cache", s.mu.TryLock, s.mu.Unlock},
			} {
				if !lock.try() {
					t.Fatalf("%s retained after failure", lock.name)
				}
				lock.unlock()
			}
			if _, err := s.db.Exec(`DROP TRIGGER fail_publication`); err != nil {
				t.Fatal(err)
			}
			published, err := publish()
			if err != nil || published == nil || published.Version.IsZero() {
				t.Fatalf("retry = %+v, %v", published, err)
			}
			wantInode := uint64(100)
			if operation == "confirm" {
				wantInode = 0
			}
			if published.Inode != wantInode {
				t.Fatalf("retry inode=%d want=%d", published.Inode, wantInode)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = nil
			reopened, err := Open(file)
			if err != nil {
				t.Fatal(err)
			}
			s = reopened
			for _, e := range []*Entry{s.lookupByPathRAM("target"), mirrorDurableEntry(t, s, "target")} {
				if e == nil || e.Inode != wantInode || e.Path != "target" || e.IsDir != input.IsDir || e.Size != input.Size || e.Version != published.Version {
					t.Fatalf("reopened publication mismatch: %+v versus %+v", e, published)
				}
			}
		})
	}
}
