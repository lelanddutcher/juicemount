/*
 * JuiceMount-owned test (patch 0022). Not part of upstream JuiceFS.
 */

package meta

import (
	"syscall"
	"testing"
)

type jmFolderInodeMeta interface {
	Meta
	JMReserveInodes(n uint32) (Ino, uint32, syscall.Errno)
	JMMkdirInode(ctx Context, parent Ino, name string, mode, cumask uint16, ino Ino, attr *Attr) syscall.Errno
}

func newJMTestMeta(t *testing.T) jmFolderInodeMeta {
	t.Helper()
	m, err := newKVMeta("memkv", "jm-reserved-inode", testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Init(testFormat(), true); err != nil {
		t.Fatal(err)
	}
	if err := m.NewSession(false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.CloseSession() })
	jm, ok := m.(jmFolderInodeMeta)
	if !ok {
		t.Fatalf("%T does not implement the JuiceMount folder inode calls", m)
	}
	return jm
}

func TestJMMkdirInodeUsesReservedNumber(t *testing.T) {
	m := newJMTestMeta(t)
	ctx := Background()
	first, count, st := m.JMReserveInodes(8)
	if st != 0 || count != 8 || first <= RootInode {
		t.Fatalf("reserve: first=%d count=%d st=%s", first, count, st)
	}
	// An ordinary create after the reservation never takes a reserved number.
	var other Ino
	if st := m.Mkdir(ctx, RootInode, "plain", 0o755, 0, 0, &other, nil); st != 0 {
		t.Fatal(st)
	}
	if other >= first && other < first+Ino(count) {
		t.Fatalf("ordinary mkdir took reserved inode %d (range %d+%d)", other, first, count)
	}
	want := first + 3
	var attr Attr
	if st := m.JMMkdirInode(ctx, RootInode, "spooled", 0o750, 0, want, &attr); st != 0 {
		t.Fatal(st)
	}
	var got Ino
	if st := m.Lookup(ctx, RootInode, "spooled", &got, &attr, true); st != 0 || got != want || attr.Typ != TypeDirectory {
		t.Fatalf("lookup: ino=%d want %d type=%d st=%s", got, want, attr.Typ, st)
	}
	if attr.Mode != 0o750 || attr.Parent != RootInode {
		t.Fatalf("attr: mode=%o parent=%d", attr.Mode, attr.Parent)
	}
	// A child in the new folder works like any other.
	var child Ino
	if st := m.Mkdir(ctx, want, "child", 0o755, 0, 0, &child, nil); st != 0 {
		t.Fatal(st)
	}
	// Once used, the number is gone.
	if st := m.JMMkdirInode(ctx, RootInode, "again", 0o755, 0, want, nil); st != syscall.ESRCH {
		t.Fatalf("reuse: %s, want ESRCH", st)
	}
	// A number this process never reserved is refused without a write.
	if st := m.JMMkdirInode(ctx, RootInode, "forged", 0o755, 0, first+Ino(count)+100, nil); st != syscall.ESRCH {
		t.Fatalf("forged: %s, want ESRCH", st)
	}
	if st := m.Lookup(ctx, RootInode, "forged", &got, &attr, true); st != syscall.ENOENT {
		t.Fatalf("forged name exists: %s", st)
	}
}

func TestJMMkdirInodeKeepsNumberAfterRefusal(t *testing.T) {
	m := newJMTestMeta(t)
	ctx := Background()
	first, _, st := m.JMReserveInodes(2)
	if st != 0 {
		t.Fatal(st)
	}
	var taken Ino
	if st := m.Mkdir(ctx, RootInode, "taken", 0o755, 0, 0, &taken, nil); st != 0 {
		t.Fatal(st)
	}
	if st := m.JMMkdirInode(ctx, RootInode, "taken", 0o755, 0, first, nil); st != syscall.EEXIST {
		t.Fatalf("existing name: %s, want EEXIST", st)
	}
	if st := m.JMMkdirInode(ctx, 999999, "orphan", 0o755, 0, first, nil); st != syscall.ENOENT {
		t.Fatalf("missing parent: %s, want ENOENT", st)
	}
	// Both refusals wrote nothing, so the number is still usable.
	if st := m.JMMkdirInode(ctx, RootInode, "fresh", 0o755, 0, first, nil); st != 0 {
		t.Fatal(st)
	}
	var got Ino
	var attr Attr
	if st := m.Lookup(ctx, RootInode, "fresh", &got, &attr, true); st != 0 || got != first {
		t.Fatalf("lookup fresh: ino=%d want %d st=%s", got, first, st)
	}
	if st := m.Lookup(ctx, RootInode, "taken", &got, &attr, true); st != 0 || got != taken {
		t.Fatalf("existing folder changed: ino=%d want %d st=%s", got, taken, st)
	}
}

func TestJMReserveInodesBounds(t *testing.T) {
	m := newJMTestMeta(t)
	if _, _, st := m.JMReserveInodes(0); st != syscall.EINVAL {
		t.Fatalf("zero: %s", st)
	}
	if _, _, st := m.JMReserveInodes(jmReserveMax + 1); st != syscall.EINVAL {
		t.Fatalf("too many: %s", st)
	}
	a, n, st := m.JMReserveInodes(16)
	b, _, st2 := m.JMReserveInodes(16)
	if st != 0 || st2 != 0 || b < a+Ino(n) {
		t.Fatalf("ranges overlap: %d+%d then %d (%s %s)", a, n, b, st, st2)
	}
}
