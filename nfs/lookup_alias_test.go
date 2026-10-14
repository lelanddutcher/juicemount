package nfs

import (
	"bytes"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

func unicodeLookupFixture(t *testing.T) (*juiceFS, *metadata.Store) {
	t.Helper()
	store, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, filepath.Join(t.TempDir(), "never-created-fuse"))
	t.Cleanup(func() { h.StopHandler(); store.Close() })
	return &juiceFS{handler: h}, store
}

func TestUnicodeLookupStoredIdentityAndExactNames(t *testing.T) {
	for _, names := range [][2]string{{"cafe\u0301.mov", "café.mov"}, {"café.mov", "cafe\u0301.mov"}, {"dir\u0301", "diŕ"}, {"K", "K"}} {
		t.Run(names[0], func(t *testing.T) {
			fs, s := unicodeLookupFixture(t)
			stored := path.Join("d", names[0])
			s.InsertToCache(metadata.MakeEntry(stored, names[0] == "dir\u0301", 81, time.Now(), 42))
			got, err := fs.ResolveLookupAlias("d", names[1])
			if err != nil || got == nil || got.Name != names[0] || got.Info.Size() != 81 {
				t.Fatalf("alias=%+v err=%v", got, err)
			}
			if !bytes.Equal(got.Handle, fs.handler.ToHandle(fs, []string{"d", names[0]})) {
				t.Fatal("handle differs from stored identity")
			}
			_, p, err := fs.handler.FromHandle(got.Handle)
			if err != nil || path.Join(p...) != stored {
				t.Fatalf("handle resolved %q: %v", p, err)
			}
			if s.LookupByPath(path.Join("d", names[1])) != nil {
				t.Fatal("alias was inserted into store")
			}
			children, err := s.ListChildrenForReadDir("d")
			if err != nil || len(children) != 1 || children[0].Name != names[0] {
				t.Fatal("listing spelling changed")
			}
			// Even the fallback's exact recheck must prefer the new exact inode.
			s.InsertToCache(metadata.MakeEntry(path.Join("d", names[1]), false, 19, time.Now(), 43))
			got, err = fs.ResolveLookupAlias("d", names[1])
			if err != nil || got.Name != names[1] || got.Info.Size() != 19 || !bytes.Equal(got.Handle, fs.handler.export.handle(43)) {
				t.Fatalf("exact sibling lost: %+v %v", got, err)
			}
		})
	}
}

func TestUnicodeLookupRecordsSyntheticStoredName(t *testing.T) {
	fs, s := unicodeLookupFixture(t)
	const inode = uint64(1<<63 | 42)
	const stored = "d/cafe\u0301.mov"
	s.InsertToCache(metadata.MakeEntry(stored, false, 81, time.Now(), inode))
	if _, ok := s.SyntheticHandlePath(inode); ok {
		t.Fatal("fixture already recorded a handle")
	}
	got, err := fs.ResolveLookupAlias("d", "café.mov")
	if err != nil || got == nil || !bytes.Equal(got.Handle, fs.handler.export.handle(inode)) {
		t.Fatalf("synthetic alias=%+v err=%v", got, err)
	}
	// Check before invoking ordinary ToHandle, which would mask a missing
	// RecordSyntheticHandle call in the alias resolver.
	if name, ok := s.SyntheticHandlePath(inode); !ok || name != stored {
		t.Fatalf("synthetic bookkeeping=%q/%v, want original %q", name, ok, stored)
	}
}

func TestUnicodeLookupAmbiguityIncludesLocalReservations(t *testing.T) {
	for _, kind := range []string{"durable", "active", "pending", "symlink", "conflict", "continuation", "failed", "private-birth"} {
		t.Run(kind, func(t *testing.T) {
			fs, s := unicodeLookupFixture(t)
			s.InsertToCache(metadata.MakeEntry("d/A\u030a", false, 1, time.Now(), 42))
			initServerTestSpoolSchema(t, s)
			sp := &SpoolStore{meta: metadata.NewSpoolStore(s.DB()), index: NewSpoolIndex()}
			fs.handler.spool.Store(sp)
			// StopHandler must not own this deliberately minimal test spool.
			t.Cleanup(func() { fs.handler.spool.Store(nil) })
			switch kind {
			case "durable":
				if err := s.Insert(metadata.MakeEntry("d/Å", false, 2, time.Now(), 43)); err != nil {
					t.Fatal(err)
				}
				s.DeleteFromCache("d/Å") // hidden by RAM-only enumeration
			case "active":
				sp.index.Insert("d/Å", &SpoolEntry{})
			case "pending":
				if _, err := sp.meta.Insert("d/Å", "private-file"); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := sp.meta.PutPendingSymlink("d/Å", "target"); err != nil {
					t.Fatal(err)
				}
			case "conflict":
				sp.conflictByPath = map[string]*conflictReadView{"d/Å": {name: "d/Å"}}
			case "continuation":
				sp.pendingContinuationPaths = map[string]int64{"d/Å": 1}
			case "failed":
				sp.readFaults.Store("d/Å", int64(1))
			case "private-birth":
				sp.pendingBirths = map[string]*PrivateBirthCustody{"pending": {}}
			}
			if got, err := fs.ResolveLookupAlias("d", "Å"); got != nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("ambiguous %s resolved %+v %v", kind, got, err)
			}
		})
	}
}

func TestUnicodeLookupConflictStoredIdentity(t *testing.T) {
	fs, s := unicodeLookupFixture(t)
	initServerTestSpoolSchema(t, s)
	sp := &SpoolStore{meta: metadata.NewSpoolStore(s.DB()), index: NewSpoolIndex()}
	name := "d/cafe\u0301.mov"
	file := filepath.Join(t.TempDir(), "accepted")
	if err := os.WriteFile(file, []byte("save"), 0600); err != nil {
		t.Fatal(err)
	}
	v := &conflictReadView{name: name, file: file, size: 4, inode: 52, store: sp}
	if err := unix.Stat(file, &v.source); err != nil {
		t.Fatal(err)
	}
	sp.conflictByPath = map[string]*conflictReadView{name: v}
	fs.handler.spool.Store(sp)
	defer fs.handler.spool.Store(nil)
	got, err := fs.ResolveLookupAlias("d", "café.mov")
	if err != nil || got == nil || got.Name != path.Base(name) || !bytes.Equal(got.Handle, fs.handler.export.handle(52)) {
		t.Fatalf("conflict alias %+v %v", got, err)
	}
}

func TestUnicodeLookupRefusesIncompleteAndBlockedNamespace(t *testing.T) {
	fs, s := unicodeLookupFixture(t)
	s.InsertToCache(metadata.MakeEntry("d/cafe\u0301.mov", false, 1, time.Now(), 42))
	for _, name := range []string{"ascii", "é/child", "é\x00", string([]byte{0xff}), "CAFÉ.mov", "ｃafé.mov"} {
		if got, err := fs.ResolveLookupAlias("d", name); got != nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ineligible %q: %+v %v", name, got, err)
		}
	}
	op := fs.handler.namespace.begin([]string{"d/café.mov"})
	if op == nil {
		t.Fatal("namespace fixture failed")
	}
	op.blockReads()
	if got, err := fs.ResolveLookupAlias("d", "café.mov"); got != nil || err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("namespace refusal became miss: %+v %v", got, err)
	}
	op.finish(nil)
	if _, err := s.DB().Exec("DROP TABLE prepared_creates"); err != nil {
		t.Fatal(err)
	}
	if got, err := fs.ResolveLookupAlias("d", "café.mov"); got != nil || err == nil {
		t.Fatalf("partial inventory used: %+v %v", got, err)
	}
}

func TestUnicodeLookupPendingAndStickyAttrsMatchExact(t *testing.T) {
	for _, state := range []string{"pending", "sticky"} {
		t.Run(state, func(t *testing.T) {
			fs, s := unicodeLookupFixture(t)
			const stored = "d/cafe\u0301.mov"
			const inode = uint64(1<<63 | 81)
			s.InsertToCache(metadata.MakeEntry(stored, false, 0, time.Unix(1700000000, 0), inode))
			if state == "pending" {
				sp := newTestSpoolStore(t, 1024)
				e, err := sp.OpenWrite(stored)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				e.SetInode(inode)
				if n, err := e.WriteAt(bytes.Repeat([]byte("x"), 81), 0); err != nil || n != 81 {
					t.Fatalf("write=%d %v", n, err)
				}
				if err := e.Close(); err != nil {
					t.Fatal(err)
				}
				row, err := sp.meta.Get(e.id)
				if err != nil || row == nil || row.DrainState != metadata.DrainReady {
					t.Fatalf("pending fixture=%+v %v", row, err)
				}
				fs.handler.spool.Store(sp)
				defer fs.handler.spool.Store(nil)
			} else {
				fs.handler.trackWriteSize(stored, 81)
			}
			before, err := fs.Lstat(stored) // guaranteed local mirror/spool hit
			if err != nil || before.Size() != 81 {
				t.Fatalf("exact attrs=%+v %v", before, err)
			}
			got, err := fs.ResolveLookupAlias("d", "café.mov")
			if err != nil || got == nil {
				t.Fatalf("alias=%+v %v", got, err)
			}
			if got.Info.Size() != before.Size() || got.Info.Mode() != before.Mode() || !got.Info.ModTime().Equal(before.ModTime()) || !bytes.Equal(got.Handle, fs.handler.export.handle(inode)) {
				t.Fatalf("alias attrs differ from exact %s: alias=%+v exact=%+v", state, got.Info, before)
			}
			if s.LookupByPath(stored).Size != 0 {
				t.Fatal("alias mutated stored metadata size")
			}
		})
	}
}

func TestUnicodeLookupRefusesChangedSnapshot(t *testing.T) {
	for _, change := range []string{"conflict-published", "active-replaced", "mirror-replaced", "namespace-blocked"} {
		t.Run(change, func(t *testing.T) {
			fs, s := unicodeLookupFixture(t)
			const stored = "d/cafe\u0301.mov"
			s.InsertToCache(metadata.MakeEntry(stored, false, 1, time.Now(), 42))
			sp := &SpoolStore{index: NewSpoolIndex()}
			fs.handler.spool.Store(sp)
			defer fs.handler.spool.Store(nil)
			var op *namespaceOperation
			defer func() {
				if op != nil {
					op.finish(nil)
				}
			}()
			got, err := fs.boundLookupAliasSnapshot(stored, func() {
				switch change {
				case "conflict-published":
					sp.conflictMu.Lock()
					sp.conflictByPath = map[string]*conflictReadView{stored: {name: stored, inode: 52}}
					sp.conflictMu.Unlock()
				case "active-replaced":
					e := &SpoolEntry{}
					e.SetInode(42)
					sp.index.Insert(stored, e)
				case "mirror-replaced":
					s.InsertToCache(metadata.MakeEntry(stored, false, 9, time.Now(), 43))
				case "namespace-blocked":
					op = fs.handler.namespace.begin([]string{stored})
					if op == nil {
						t.Fatal("namespace fixture")
					}
					op.blockReads()
				}
			})
			if got != nil || err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("changed %s cohort returned alias/miss: %+v %v", change, got, err)
			}
		})
	}
}
