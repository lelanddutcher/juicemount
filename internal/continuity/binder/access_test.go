package binder

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func under(dir string) func(string) bool {
	return func(p string) bool { return strings.HasPrefix(p, dir+"/") }
}

func (f *fixture) access(ino uint64, allow func(string) bool) (Access, error) {
	f.t.Helper()
	return f.b.CheckAccess(context.Background(), AssetID(testUUID, ino), allow)
}

func TestCheckAccessSeesHiddenNamesAndLinks(t *testing.T) {
	f := newFixture(t, 0, true)
	x := f.create("Clients/A/x.mov")
	f.feed(Event{Op: "create", Path: "Clients/A/x.mov", Inode: x})
	if a, err := f.access(x, under("Clients/A")); err != nil || !a.Allowed || a.Name != "Clients/A/x.mov" {
		t.Fatalf("own project: %+v %v", a, err)
	}
	if a, err := f.access(x, under("Clients/B")); err != nil || a.Allowed || a.Missing || a.Name != "Clients/A/x.mov" {
		t.Fatalf("other project: %+v %v", a, err)
	}
	// Moved into a dot folder of B: the binder retires it, but the file is
	// live and in B, not missing.
	f.resolve("Clients/B/.Proj-old")
	f.rename("Clients/A/x.mov", "Clients/B/.Proj-old/x.mov")
	f.feed(Event{Op: "rename", Path: "Clients/B/.Proj-old/x.mov", OldPath: "Clients/A/x.mov", Inode: x})
	f.requireRemoved(x)
	if a, err := f.access(x, under("Clients/A")); err != nil || a.Allowed || a.Missing || a.Name != "Clients/B/.Proj-old/x.mov" {
		t.Fatalf("hidden in B, checked for A: %+v %v", a, err)
	}
	if a, err := f.access(x, under("Clients/B")); err != nil || !a.Allowed {
		t.Fatalf("hidden in B, checked for B: %+v %v", a, err)
	}
	// Trash (and a permanent delete) are missing.
	f.unlink("Clients/B/.Proj-old/x.mov", false)
	if a, err := f.access(x, under("Clients/A")); err != nil || !a.Missing {
		t.Fatalf("trashed: %+v %v", a, err)
	}
	y := f.create("Clients/A/y.mov")
	f.feed(Event{Op: "create", Path: "Clients/A/y.mov", Inode: y})
	f.unlink("Clients/A/y.mov", true)
	if a, err := f.access(y, under("Clients/B")); err != nil || !a.Missing {
		t.Fatalf("deleted: %+v %v", a, err)
	}
	// A hard link: every name must pass (the proxy's rule), whichever the
	// hint is.
	h := f.create("Clients/A/h.mov")
	f.feed(Event{Op: "create", Path: "Clients/A/h.mov", Inode: h})
	f.link(h, "Clients/B/h2.mov")
	if a, err := f.access(h, under("Clients/A")); err != nil || a.Allowed || a.Missing || a.Name != "Clients/B/h2.mov" {
		t.Fatalf("linked file for A: %+v %v", a, err)
	}
	if a, err := f.access(h, under("Clients/B")); err != nil || a.Allowed || a.Name != "Clients/A/h.mov" {
		t.Fatalf("linked file for B: %+v %v", a, err)
	}
	if a, err := f.access(h, under("Clients")); err != nil || !a.Allowed {
		t.Fatalf("linked file for both: %+v %v", a, err)
	}
	// A name that cannot be read is unknown, never missing or denied.
	rdb := openRedis(t, f.url)
	defer rdb.Close()
	if err := rdb.HDel(context.Background(), fmt.Sprintf("d%d", f.resolve("Clients/B")), "h2.mov").Err(); err != nil {
		t.Fatal(err)
	}
	if a, err := f.access(h, under("Clients")); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("unreadable name: %+v %v", a, err)
	}
	// Over MaxLinks: a refused hint still refuses, but passing needs every
	// name, which is not listed.
	m := f.create("Clients/A/m.mov")
	f.feed(Event{Op: "create", Path: "Clients/A/m.mov", Inode: m})
	for i := 0; i < MaxLinks; i++ {
		f.link(m, fmt.Sprintf("Clients/B/m%d.mov", i))
	}
	if a, err := f.access(m, under("Clients/B")); err != nil || a.Allowed || a.Name != "Clients/A/m.mov" {
		t.Fatalf("many links, hint refused: %+v %v", a, err)
	}
	if a, err := f.access(m, under("Clients")); !errors.Is(err, ErrUnresolved) || !strings.Contains(err.Error(), "hard links") {
		t.Fatalf("many links, hint allowed: %+v %v", a, err)
	}
	// A canceled request reads nothing more.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.b.CheckAccess(ctx, AssetID(testUUID, h), under("Clients/A")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}
