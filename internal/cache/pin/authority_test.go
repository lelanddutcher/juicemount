package pin

import (
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestOwnedReadyWitnessPersistsButNeedsLocalRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pin.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetReadyValidator(func(e Entry) bool { return len(e.Authority) != 0 }); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin("/volume/a.mov", 4, "/volume"); err != nil {
		t.Fatal(err)
	}
	ready := markTestPinReady(t, s, "/volume/a.mov")
	if !s.IsPinnedReady(ready.Path) {
		t.Fatal("producer witness not admitted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.SetReadyValidator(func(e Entry) bool { return len(e.Authority) != 0 }); err != nil {
		t.Fatal(err)
	}
	if reopened.IsPinnedReady(ready.Path) {
		t.Fatal("SQLite Ready alone admitted after restart")
	}
	persisted, ok := reopened.Get(ready.Path)
	if !ok || persisted.Generation != ready.Generation || string(persisted.Authority) != string(ready.Authority) {
		t.Fatalf("witness did not persist: %+v", persisted)
	}
	admitted, err := reopened.AdmitReady(*persisted)
	if err != nil || !admitted || !reopened.IsPinnedReady(ready.Path) {
		t.Fatalf("local restore: admitted=%v err=%v", admitted, err)
	}
}

func TestOwnedReadyRootUnpinRevokesOnlyRemovedGenerations(t *testing.T) {
	s := newTestStore(t)
	var revoked []string
	if err := s.SetReadyRevoker(func(path, generation string) { revoked = append(revoked, path+"\x00"+generation) }); err != nil {
		t.Fatal(err)
	}
	want := make([]string, 0, 2)
	for _, tc := range []struct{ path, root string }{
		{"/volume/a.mov", "/volume"}, {"/volume/b.mov", "/volume"}, {"/other/c.mov", "/other"},
	} {
		if err := s.Pin(tc.path, 4, tc.root); err != nil {
			t.Fatal(err)
		}
		e := markTestPinReady(t, s, tc.path)
		if tc.root == "/volume" {
			want = append(want, e.Path+"\x00"+e.Generation)
		}
	}
	if n, err := s.Unpin("/volume"); err != nil || n != 2 {
		t.Fatalf("unpin count=%d err=%v", n, err)
	}
	sort.Strings(revoked)
	sort.Strings(want)
	if len(revoked) != len(want) {
		t.Fatalf("revokes=%q want=%q", revoked, want)
	}
	for i := range want {
		if revoked[i] != want[i] {
			t.Fatalf("revokes=%q want=%q", revoked, want)
		}
	}
	if s.IsPinnedReady("/volume/a.mov") || s.IsPinnedReady("/volume/b.mov") || !s.IsPinnedReady("/other/c.mov") {
		t.Fatal("root unpin changed unrelated admission")
	}
}

func TestOwnedReadyTouchKeepsAuthorityAndRejectsSuccessor(t *testing.T) {
	s := newTestStore(t)
	const path = "/volume/a.mov"
	if err := s.Pin(path, 4, "/volume"); err != nil {
		t.Fatal(err)
	}
	old := markTestPinReady(t, s, path)
	if _, err := s.db.Exec(`UPDATE pinned_files SET last_prefetched=1 WHERE path=?`, path); err != nil {
		t.Fatal(err)
	}
	if touched, err := s.TouchReady(old); err != nil || !touched {
		t.Fatalf("touch=%v err=%v", touched, err)
	}
	current, ok := s.Get(path)
	if !ok || current.Generation != old.Generation || string(current.Authority) != string(old.Authority) || current.LastPrefetched.Before(time.UnixMilli(2)) || !s.IsPinnedReady(path) {
		t.Fatalf("touch revoked valid witness: %+v", current)
	}
	if err := s.Pin(path, 4, "/volume"); err != nil {
		t.Fatal(err)
	}
	successor, ok := s.Get(path)
	if !ok || successor.Generation == old.Generation {
		t.Fatalf("repin missing: %+v", successor)
	}
	if touched, err := s.TouchReady(old); err != nil || touched {
		t.Fatalf("stale touch=%v err=%v", touched, err)
	}
	after, _ := s.Get(path)
	if after.Generation != successor.Generation || after.Status != StatusPending || s.IsPinnedReady(path) {
		t.Fatalf("stale rewarm changed successor: %+v", after)
	}
}

func TestOwnedReadyRepinRevokesOnlyAffectedPath(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{"/volume/a.mov", "/volume/b.mov"} {
		if err := s.Pin(name, 4, "/volume"); err != nil {
			t.Fatal(err)
		}
		markTestPinReady(t, s, name)
	}
	old, _ := s.Get("/volume/a.mov")
	if err := s.Pin("/volume/a.mov", 4, "/volume"); err != nil {
		t.Fatal(err)
	}
	if s.IsPinnedReady("/volume/a.mov") || !s.IsPinnedReady("/volume/b.mov") {
		t.Fatal("repin revoked the wrong admission")
	}
	if admitted, err := s.AdmitReady(*old); err != nil || admitted {
		t.Fatalf("stale generation admitted: %v %v", admitted, err)
	}
}
