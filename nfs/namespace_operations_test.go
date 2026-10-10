package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func awaitNamespaceIdle(t *testing.T, c *namespaceCoordinator) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		c.mu.Lock()
		n, changed := len(c.active), c.changed
		c.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-changed:
		case <-timer.C:
			t.Fatal("namespace operation did not retire")
		}
	}
}

func TestNamespaceTimedOutRemoveOwnsNameUntilPublication(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "20")
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "dir/take.mov", []byte("original"))
	entered, release := make(chan struct{}), make(chan struct{})
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase == "remove" && name == "dir/take.mov" {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- jfs.Remove("dir/take.mov") }()
	<-entered
	if err := <-done; !errors.Is(err, errFUSETimeout) {
		t.Fatalf("Remove = %v, want timeout", err)
	}
	// The short deadline forces only the stalled Remove to return. Conflicting
	// names must still refuse admission; normal durable creates use the
	// production budget instead of becoming an unrelated 20 ms latency test.
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "")
	for name, attempt := range map[string]func() error{
		"create": func() error { f, err := jfs.Create("dir/take.mov"); _ = closeNamespaceFile(f); return err },
		"open create": func() error {
			f, err := jfs.OpenFile("dir/take.mov", os.O_RDWR|os.O_CREATE, 0600)
			_ = closeNamespaceFile(f)
			return err
		},
		"ancestor rename": func() error { return jfs.Rename("dir", "moved") },
		"ancestor remove": func() error { return jfs.Remove("dir") },
		"mkdir":           func() error { return jfs.MkdirAll("dir/take.mov/child", 0700) },
		"symlink":         func() error { return jfs.Symlink("other", "dir/take.mov") },
	} {
		if err := attempt(); !errors.Is(err, errFUSETimeout) {
			t.Errorf("%s = %v, want namespace conflict", name, err)
		}
	}
	// Unrelated paths remain usable while the old syscall is stalled.
	f, err := jfs.Create("unrelated.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitNamespaceIdle(t, &jfs.handler.namespace)
	f, err = jfs.Create("dir/take.mov")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "dir/take.mov"))
	if err != nil || string(got) != "replacement" {
		t.Fatalf("replacement = %q, %v", got, err)
	}
	var size int64
	if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = 'dir/take.mov'`).Scan(&size); err != nil || size != int64(len(got)) {
		t.Fatalf("durable replacement size = %d, %v", size, err)
	}
}

func TestNamespaceRenameRetainsBothTreesThroughPublication(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "20")
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "original/take.mov", []byte("original"))
	mirror(t, store, root, "original", true)
	for _, name := range []string{"original", "original/take.mov"} {
		if err := store.Insert(store.LookupByPath(name)); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var barrier sync.Once
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase == "publish" && name == "original" {
			barrier.Do(func() { close(entered); <-release })
		}
	}
	done := make(chan error, 1)
	go func() { done <- jfs.Rename("original", "renamed") }()
	<-entered
	if _, err := os.Stat(filepath.Join(root, "renamed/take.mov")); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, errFUSETimeout) {
		t.Fatalf("Rename = %v", err)
	}
	for _, name := range []string{"original", "original/take.mov", "renamed/take.mov"} {
		if f, err := jfs.Create(name); !errors.Is(err, errFUSETimeout) {
			_ = closeNamespaceFile(f)
			t.Fatalf("Create(%q) while publication pending = %v", name, err)
		}
	}
	close(release)
	awaitNamespaceIdle(t, &jfs.handler.namespace)
	var count int
	if err := store.DB().QueryRow(`SELECT count(*) FROM entries WHERE path = 'renamed/take.mov'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable descendant count = %d, %v", count, err)
	}
	if err := jfs.MkdirAll("original", 0700); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.Create("original/take.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRow(`SELECT count(*) FROM entries WHERE path IN ('original/take.mov', 'renamed/take.mov')`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("durable new and renamed descendants = %d, %v", count, err)
	}
}

func TestNamespaceShutdownWaitsForTimedOutWorker(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "20")
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "take.mov", []byte("original"))
	entered, release := make(chan struct{}), make(chan struct{})
	jfs.handler.namespace.phase = func(phase, name string) {
		if phase == "remove" {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- jfs.Remove("take.mov") }()
	<-entered
	if err := <-done; !errors.Is(err, errFUSETimeout) {
		t.Fatal(err)
	}
	if jfs.handler.stopHandlerWithDeadline(time.Millisecond) {
		t.Fatal("shutdown accepted outstanding syscall")
	}
	if err := store.DB().Ping(); err != nil {
		t.Fatal("store closed before worker finished", err)
	}
	select {
	case <-jfs.handler.verifierStop:
		t.Fatal("handler resources stopped early")
	default:
	}
	close(release)
	awaitNamespaceIdle(t, &jfs.handler.namespace)
	if !jfs.handler.stopHandlerWithDeadline(time.Second) {
		t.Fatal("resolved worker still blocks shutdown")
	}
}

func TestNamespacePublicationFailurePoisonsNameAndShutdown(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "take.mov", []byte("original"))
	if err := store.Insert(store.LookupByPath("take.mov")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_namespace_delete BEFORE DELETE ON entries BEGIN SELECT RAISE(FAIL, 'namespace publication failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := jfs.Remove("take.mov"); err == nil || errors.Is(err, errFUSETimeout) {
		t.Fatalf("Remove = %v, want publication error", err)
	}
	if f, err := jfs.Create("take.mov"); !errors.Is(err, errFUSETimeout) {
		_ = closeNamespaceFile(f)
		t.Fatalf("Create on poisoned scope = %v", err)
	}
	if jfs.handler.stopHandlerWithDeadline(time.Millisecond) {
		t.Fatal("shutdown accepted unresolved namespace publication")
	}
	select {
	case <-jfs.handler.verifierStop:
		t.Fatal("handler resources stopped after publication failure")
	default:
	}
	// The production scope remains poisoned. Only the test tears down its
	// otherwise idle handler after asserting the fail-closed boundary.
	jfs.handler.namespace.mu.Lock()
	clear(jfs.handler.namespace.active)
	jfs.handler.namespace.mu.Unlock()
}

func TestNamespaceNoOpSpoolFailureDoesNotPoison(t *testing.T) {
	jfs, _, _, root := newSpoolWiredHandlerNoDrain(t)
	if err := os.MkdirAll(filepath.Join(root, "dir"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir/child"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []func() error{
		func() error { return jfs.Remove("dir") },
		func() error { return jfs.Rename("missing", "target") },
		func() error { return jfs.Rename("dir/child", "dir") },
	} {
		if err := attempt(); err == nil || errors.Is(err, errFUSETimeout) {
			t.Fatalf("expected backing syscall error, got %v", err)
		}
		jfs.handler.namespace.mu.Lock()
		active := len(jfs.handler.namespace.active)
		jfs.handler.namespace.mu.Unlock()
		if active != 0 {
			t.Fatalf("no-op spool failure left %d poisoned scope(s)", active)
		}
		f, err := jfs.Create("dir/probe")
		if err != nil {
			t.Fatal("child create after expected failure", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := jfs.Remove("dir/probe"); err != nil {
			t.Fatal("child remove after expected failure", err)
		}
	}
	f, err := jfs.Create("unrelated")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if !jfs.handler.stopHandlerWithDeadline(time.Second) {
		t.Fatal("no-op spool failure blocked shutdown")
	}
}

func TestNamespaceSpoolCancelSQLFailurePreservesBytes(t *testing.T) {
	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	f, err := jfs.Create("take.mov")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("only copy")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	e, ok := spool.LookupActive("take.mov")
	if !ok {
		t.Fatal("spool entry missing")
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jfs.handler.store.DB().Exec(`CREATE TRIGGER fail_namespace_spool_delete BEFORE DELETE ON spool_entries BEGIN SELECT RAISE(FAIL, 'cancel failed'); END`); err != nil {
		t.Fatal(err)
	}
	if err := jfs.Remove("take.mov"); err == nil {
		t.Fatal("Remove acknowledged failed cancellation")
	}
	got, err := os.ReadFile(row.SpoolFile)
	if err != nil || string(got) != "only copy" {
		t.Fatalf("spool bytes = %q, %v", got, err)
	}
	if jfs.handler.stopHandlerWithDeadline(time.Millisecond) {
		t.Fatal("uncertain cancellation accepted shutdown")
	}
	jfs.handler.namespace.mu.Lock()
	clear(jfs.handler.namespace.active)
	jfs.handler.namespace.mu.Unlock()
}
