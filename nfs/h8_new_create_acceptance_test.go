//go:build h8_acceptance

package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestH8AcceptanceNewCreateCompletesPreparedIntentAcrossRestart(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})

	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("clean.mov")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close created file: %v", err)
	}
	if !h.StopHandler() {
		t.Fatal("handler did not stop after successful CREATE")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after completed CREATE: %v", err)
	}
	defer reopened.Close()
	if entry := reopened.LookupByPath("clean.mov"); entry == nil {
		t.Fatal("completed CREATE missing from reopened mirror")
	}
}

func TestH8AcceptanceNewCreateConfirmsUnmirroredBackingParentAcrossRestart(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			h.StopHandler()
		}
		_ = store.Close()
	})
	if entry := store.LookupByPath("dir"); entry != nil {
		t.Fatalf("fixture unexpectedly has parent mirror row: %+v", entry)
	}

	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("dir/new.mov")
	if err != nil {
		t.Fatalf("CREATE under unmirrored backing parent: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close created file: %v", err)
	}
	for _, name := range []string{"dir", "dir/new.mov"} {
		if entry := store.LookupByPath(name); entry == nil {
			t.Fatalf("CREATE did not admit mirror row %q", name)
		}
	}
	if !h.StopHandler() {
		t.Fatal("handler did not stop after successful CREATE")
	}
	stopped = true
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after parent-confirmed CREATE: %v", err)
	}
	defer reopened.Close()
	for _, name := range []string{"dir", "dir/new.mov"} {
		if entry := reopened.LookupByPath(name); entry == nil {
			t.Errorf("reopened mirror missing %q", name)
		}
	}
}

func TestH8AcceptanceNewCreateRefusesReplacedBackingParentBeforeChildIntent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})

	phaseFired := false
	var replacement string
	var replaceErr error
	h.namespace.phase = func(phase, name string) {
		if phase != "confirm-parent" || name != "dir" || phaseFired {
			return
		}
		phaseFired = true
		replacement = filepath.Join(root, "dir-replaced-old")
		if err := os.Rename(filepath.Join(root, "dir"), replacement); err != nil {
			replaceErr = err
			return
		}
		if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
			replaceErr = err
			return
		}
		replaceErr = os.WriteFile(filepath.Join(root, "dir", "winner.keep"), []byte("replacement"), 0o600)
	}

	jfs := &juiceFS{handler: h}
	f, createErr := jfs.Create("dir/new.mov")
	if f != nil {
		_ = f.Close()
	}
	if !phaseFired {
		t.Fatal("confirm-parent phase hook did not fire")
	}
	if replaceErr != nil {
		t.Fatalf("replace observed parent during admission: %v", replaceErr)
	}
	if createErr == nil {
		t.Fatal("CREATE accepted a replaced backing parent")
	}
	if _, err := os.Lstat(filepath.Join(root, "dir", "new.mov")); !os.IsNotExist(err) {
		t.Fatalf("CREATE materialized child in replacement directory: err=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, "dir", "winner.keep"))
	if err != nil || string(data) != "replacement" {
		t.Fatalf("replacement directory winner = (%q, %v), want unchanged sentinel", data, err)
	}
	if _, err := os.Stat(filepath.Join(replacement, "new.mov")); !os.IsNotExist(err) {
		t.Fatalf("CREATE materialized child in displaced original directory: err=%v", err)
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 0 {
		t.Fatalf("parent replacement left prepared child intents: (%+v, %v)", pending, err)
	}
}

func TestH8AcceptanceNewCreateParentReplacementAfterIntentRefusesAndRetainsCustody(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			discardFailedNamespaceForTest(h)
			h.StopHandler()
		}
		_ = store.Close()
	})

	phaseFired := false
	var replacement string
	var replaceErr error
	h.namespace.phase = func(phase, name string) {
		if phase != "create-intent" || name != "dir/new.mov" || phaseFired {
			return
		}
		phaseFired = true
		replacement = filepath.Join(root, "dir-replaced-old")
		if err := os.Rename(filepath.Join(root, "dir"), replacement); err != nil {
			replaceErr = err
			return
		}
		if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
			replaceErr = err
			return
		}
		replaceErr = os.WriteFile(filepath.Join(root, "dir", "winner.keep"), []byte("replacement"), 0o600)
	}

	jfs := &juiceFS{handler: h}
	f, createErr := jfs.Create("dir/new.mov")
	if f != nil {
		_ = f.Close()
	}
	if !phaseFired {
		t.Fatal("create-intent phase hook did not fire")
	}
	if replaceErr != nil {
		t.Fatalf("replace backing parent after intent: %v", replaceErr)
	}
	if createErr == nil {
		t.Fatal("CREATE accepted parent replacement after durable child intent")
	}
	if _, err := os.Lstat(filepath.Join(root, "dir", "new.mov")); !os.IsNotExist(err) {
		t.Fatalf("CREATE materialized child in replacement directory: err=%v", err)
	}
	data, err := os.ReadFile(filepath.Join(root, "dir", "winner.keep"))
	if err != nil || string(data) != "replacement" {
		t.Fatalf("replacement directory winner = (%q, %v), want unchanged sentinel", data, err)
	}
	if child := store.LookupByPath("dir/new.mov"); child != nil {
		t.Fatalf("failed CREATE published child in mirror: %+v", child)
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0].Path != "dir/new.mov" {
		t.Fatalf("unresolved CREATE custody = (%+v, %v), want exact child intent", pending, err)
	}
	if _, err := os.Stat(filepath.Join(replacement, "new.mov")); !os.IsNotExist(err) {
		t.Fatalf("CREATE unexpectedly created child in displaced parent: err=%v", err)
	}

	discardFailedNamespaceForTest(h)
	if !h.StopHandler() {
		t.Fatal("handler did not stop after crash simulation")
	}
	stopped = true
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.Open(dbPath)
	if reopened != nil {
		_ = reopened.Close()
	}
	var pendingErr *metadata.PendingPreparedCreate
	if !errors.As(err, &pendingErr) || len(pendingErr.Creates) != 1 || pendingErr.Creates[0].Path != "dir/new.mov" {
		t.Fatalf("restart after parent replacement = (%v, %v), want exact path-specific refusal", reopened, err)
	}
}

func TestH8AcceptanceNewCreateNestedAncestorReplacementRetainsCustody(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "top", "mid"), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() {
		discardFailedNamespaceForTest(h)
		h.StopHandler()
		_ = store.Close()
	})

	var replaceErr error
	phaseFired := false
	h.namespace.phase = func(phase, name string) {
		if phase != "create-intent" || name != "top/mid/new.mov" || phaseFired {
			return
		}
		phaseFired = true
		replaceErr = os.Rename(filepath.Join(root, "top"), filepath.Join(root, "top-old"))
		if replaceErr == nil {
			replaceErr = os.MkdirAll(filepath.Join(root, "top", "mid"), 0o755)
		}
	}

	f, createErr := (&juiceFS{handler: h}).Create("top/mid/new.mov")
	if f != nil {
		_ = f.Close()
	}
	if !phaseFired || replaceErr != nil {
		t.Fatalf("ancestor replacement: phase=%t err=%v", phaseFired, replaceErr)
	}
	if createErr == nil {
		t.Fatal("CREATE accepted replacement of a non-immediate ancestor")
	}
	for _, dir := range []string{"top", "top-old"} {
		if _, err := os.Lstat(filepath.Join(root, dir, "mid", "new.mov")); !os.IsNotExist(err) {
			t.Fatalf("CREATE wrote under %s: %v", dir, err)
		}
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0].Path != "top/mid/new.mov" {
		t.Fatalf("nested ancestor custody = (%+v, %v)", pending, err)
	}
}

func TestH8AcceptanceNewCreateSymlinkParentReplacementRetainsCustody(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"original/sub", "replacement/sub"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("original", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() {
		discardFailedNamespaceForTest(h)
		h.StopHandler()
		_ = store.Close()
	})

	var replaceErr error
	phaseFired := false
	h.namespace.phase = func(phase, name string) {
		if phase != "create-intent" || name != "link/sub/new.mov" || phaseFired {
			return
		}
		phaseFired = true
		replaceErr = os.Rename(filepath.Join(root, "link"), filepath.Join(root, "link-old"))
		if replaceErr == nil {
			replaceErr = os.Symlink("replacement", filepath.Join(root, "link"))
		}
	}

	f, createErr := (&juiceFS{handler: h}).Create("link/sub/new.mov")
	if f != nil {
		_ = f.Close()
	}
	if !phaseFired || replaceErr != nil {
		t.Fatalf("symlink replacement: phase=%t err=%v", phaseFired, replaceErr)
	}
	if createErr == nil {
		t.Fatal("CREATE accepted a replaced symlink parent")
	}
	for _, dir := range []string{"original", "replacement"} {
		if _, err := os.Lstat(filepath.Join(root, dir, "sub", "new.mov")); !os.IsNotExist(err) {
			t.Fatalf("CREATE wrote under %s: %v", dir, err)
		}
	}
	pending, err := store.InspectPreparedCreates()
	if err != nil || len(pending) != 1 || pending[0].Path != "link/sub/new.mov" {
		t.Fatalf("symlink parent custody = (%+v, %v)", pending, err)
	}
}

func TestH8AcceptanceNewCreateDefinitiveEEXISTLeavesNoPendingIntent(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	if err := os.WriteFile(filepath.Join(root, "existing.mov"), []byte("winner"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	t.Cleanup(func() {
		h.StopHandler()
		_ = store.Close()
	})

	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("existing.mov")
	if f != nil {
		_ = f.Close()
	}
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("CREATE over existing path = %v, want EEXIST", err)
	}
	if !h.StopHandler() {
		t.Fatal("handler did not stop after definitive EEXIST")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen after definitive EEXIST: %v; intent should have been aborted", err)
	}
	defer reopened.Close()
	if entry := reopened.LookupByPath("existing.mov"); entry != nil {
		t.Fatalf("failed CREATE published mirror entry: %+v", entry)
	}
	data, err := os.ReadFile(filepath.Join(root, "existing.mov"))
	if err != nil || string(data) != "winner" {
		t.Fatalf("existing backing winner = (%q, %v), want unchanged content", data, err)
	}
}

func TestH8AcceptanceNewCreateMirrorFailureRefusesRestart(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			discardFailedNamespaceForTest(h)
			h.StopHandler()
		}
		_ = store.Close()
	})
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_new_create_mirror BEFORE INSERT ON entries
BEGIN SELECT RAISE(FAIL, 'injected new-create mirror failure'); END`); err != nil {
		t.Fatal(err)
	}

	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("new.mov")
	if f != nil {
		_ = f.Close()
	}
	if err == nil {
		t.Fatal("new CREATE acknowledged failed mirror insertion")
	}
	if info, statErr := os.Stat(filepath.Join(root, "new.mov")); statErr != nil || info.Size() != 0 {
		t.Fatalf("exclusive backing create = (%v, %v), want retained zero-byte file", info, statErr)
	}
	if _, err := jfs.Stat("new.mov"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("same-process unresolved CREATE stat = %v, want EIO", err)
	}

	// Simulate a crash: in-memory poison is lost, while the backing namespace
	// and SQLite journal are retained for startup recovery inspection.
	discardFailedNamespaceForTest(h)
	if !h.StopHandler() {
		t.Fatal("handler did not stop after crash simulation")
	}
	stopped = true
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.Open(dbPath)
	if reopened != nil {
		defer reopened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "new.mov") {
		t.Fatalf("restart after unresolved new CREATE: reopened=%t err=%v, want path-specific refusal", reopened != nil, err)
	}
}
