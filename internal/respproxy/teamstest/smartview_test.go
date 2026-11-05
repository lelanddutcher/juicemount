package teamstest

import (
	"context"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// TestSmartViewRemovalEndToEnd drives the real proxy in enforce mode with
// the JuiceFS metadata client, as the Manager's smart folders do
// (internal/smartfolders/jfsmeta: Rmdir without the trash). The manager
// principal removes an empty view it created under its /Collections; every
// other removal or rename of a folder at project depth stays refused,
// including an empty folder of its own moved into /Collections (as a
// migration's would be) and one it made under a Mac's /Queries.
func TestSmartViewRemovalEndToEnd(t *testing.T) {
	e := &env{volume: newVolume(t), mode: respproxy.ModeEnforce, smartRoots: []string{"Recents", "Collections", "Queries"}}
	e.startProxy(nil)
	mgr, mac := e.client("mgr1"), e.client("mac-john")
	ctx := meta.Background()
	mkdir := func(m meta.Meta, parent meta.Ino, name string) meta.Ino {
		t.Helper()
		var ino meta.Ino
		var attr meta.Attr
		if st := m.Mkdir(ctx, parent, name, 0o705, 0, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir %s: %v", name, st)
		}
		return ino
	}
	root := mkdir(mgr, meta.RootInode, "Collections")
	view := mkdir(mgr, root, "Shoot")
	full := mkdir(mgr, root, "Full")
	var lino meta.Ino
	var lattr meta.Attr
	if st := mgr.Symlink(ctx, full, "clip.mov", "../../Clients/A/g.txt", &lino, &lattr); st != 0 {
		t.Fatalf("symlink: %v", st)
	}
	mkdir(mac, root, "MacMade")
	mkdir(mgr, root, "Aside")
	other := mkdir(mgr, meta.RootInode, "Other")
	mkdir(mgr, other, "Shoot")
	queries := mkdir(mac, meta.RootInode, "Queries")
	mkdir(mgr, queries, "Shoot")
	mkdir(mgr, e.ino["/Clients/A"], "migrated")
	var mino meta.Ino
	var mattr meta.Attr
	if st := mgr.Rename(ctx, e.ino["/Clients/A"], "migrated", root, "migrated", 0, &mino, &mattr); st != 0 {
		t.Fatalf("move the folder into Collections: %v", st)
	}

	refused := func(st syscall.Errno, parent meta.Ino, name, what string) {
		t.Helper()
		if st == 0 {
			t.Fatalf("%s: allowed", what)
		}
		if !e.exists(parent, name) {
			t.Fatalf("%s: refused (%v) but the folder is gone", what, st)
		}
	}
	// The Mac may not remove the Manager's view, nor the Manager a Mac's
	// empty folder, a view with an entry, an empty folder under another
	// top-level folder or an admin's project root.
	refused(mac.Rmdir(ctx, root, "Shoot", true), root, "Shoot", "Mac rmdir of a view")
	refused(mgr.Rmdir(ctx, root, "MacMade", true), root, "MacMade", "manager rmdir of a Mac's folder")
	refused(mgr.Rmdir(ctx, root, "Full", true), root, "Full", "manager rmdir of a non-empty view")
	refused(mgr.Rmdir(ctx, other, "Shoot", true), other, "Shoot", "manager rmdir under a non-smart root")
	refused(mgr.Rmdir(ctx, queries, "Shoot", true), queries, "Shoot", "manager rmdir under a Mac's Queries")
	refused(mgr.Rmdir(ctx, root, "migrated", true), root, "migrated", "manager rmdir of a folder moved into Collections")
	refused(mgr.Rmdir(ctx, e.ino["/Clients"], "D", true), e.ino["/Clients"], "D", "manager rmdir of an admin's empty project root")
	// Only rmdir: not into the trash, not a rename aside.
	refused(mgr.Rmdir(ctx, root, "Aside"), root, "Aside", "manager rmdir into the trash")
	var rino meta.Ino
	var rattr meta.Attr
	refused(mgr.Rename(ctx, root, "Aside", root, ".juicemount-smart-folder.tmp-1", meta.RenameNoReplace, &rino, &rattr),
		root, "Aside", "manager rename of a view to a hidden name")

	// The Manager's empty view goes. Its record is written once its mkdir's
	// reply came back from Redis; the folders moved in or made under the
	// Mac's root have none.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ok, err := e.rdb.SIsMember(context.Background(), respproxy.SmartViewsKey, strconv.FormatUint(uint64(view), 10)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the view was never recorded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n, err := e.rdb.SCard(context.Background(), respproxy.SmartViewsKey).Result(); err != nil || n != 3 {
		t.Fatalf("view record holds %d inodes (%v), want 3: Shoot, Full, Aside", n, err)
	}
	if st := mgr.Rmdir(ctx, root, "Shoot", true); st != 0 {
		t.Fatalf("manager rmdir of its empty view: %v", st)
	}
	if e.exists(root, "Shoot") {
		t.Fatal("the view is still there")
	}
	var attr meta.Attr
	if st := e.admin.GetAttr(ctx, view, &attr); st != syscall.ENOENT {
		t.Fatalf("the view's inode after rmdir: %v", st)
	}

	// The audit log has the committed removal.
	var intent uint64
	for _, r := range e.records(auditlog.Filter{Principal: "mgr1", Types: []string{auditlog.TypeIntent}}) {
		for _, op := range r.Ops {
			if op.Parent == uint64(root) && op.Name == "Shoot" && r.Action == "remove" {
				intent = r.Seq
			}
		}
	}
	if intent == 0 {
		t.Fatal("no audit intent for the view removal")
	}
	committed := false
	for _, r := range e.records(auditlog.Filter{Principal: "mgr1", Types: []string{auditlog.TypeOutcome}}) {
		committed = committed || (r.Unit == intent && r.Outcome == auditlog.OutcomeCommitted)
	}
	if !committed {
		t.Fatal("the view removal's audit intent has no committed outcome")
	}
}
