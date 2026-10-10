//go:build latency_acceptance

package nfs

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// A cold directory can be locally enumerable while its mirror publication is
// stalled. The fast-link budget must cover publication as well as ReadDir.
func TestColdDirectoryBudgetIncludesSQL(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	t.Setenv("JM_SYNC_COLD_POPULATE", "1")
	if metadata.ServeFromSQLite() {
		t.Fatal("fixture requires RAM serving")
	}
	class := netprofile.ClassFast
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	jfs, store, root := newDirRefreshHarness(t)
	mkFUSEDir(t, root, "cold", "a.txt", "b.txt")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.QuiesceForLatencyAcceptance(ctx); err != nil {
		t.Fatal(err)
	}
	store.DB().SetMaxOpenConns(1)
	held, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = held.Close() }) }
	defer release()
	waits := store.DB().Stats().WaitCount
	type result struct {
		infos   []os.FileInfo
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() { infos, err := jfs.ReadDir("cold"); done <- result{infos, err, time.Since(start)} }()
	joined := false
	defer func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("cold reader did not join")
			}
		}
		if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
			t.Error("refresh did not join")
		}
	}()
	if !pollUntil(t, time.Second, func() bool { return store.DB().Stats().WaitCount > waits }) {
		t.Fatal("cold publication did not reach held SQL after background GC was joined")
	}
	select {
	case r := <-done:
		joined = true
		if r.err != nil || len(r.infos) != 0 {
			t.Errorf("held SQL cold result count=%d err=%v", len(r.infos), r.err)
		}
		if r.elapsed > 100*time.Millisecond {
			t.Errorf("cold directory took %s, exceeds 100ms allowance for 50ms budget", r.elapsed)
		}
		t.Logf("cold directory returned before SQL release in %s", r.elapsed)
		// Repeated fast-link reads share the still-blocked publication job.
		for i := 0; i < 12; i++ {
			if infos, err := jfs.ReadDir("cold"); err != nil || len(infos) != 0 {
				t.Fatalf("coalesced read: count=%d err=%v", len(infos), err)
			}
		}
		jfs.handler.dirRefreshMu.Lock()
		active, keys := jfs.handler.dirRefreshActive, len(jfs.handler.dirRefreshInFlight)
		jfs.handler.dirRefreshMu.Unlock()
		if active != 1 || keys != 1 || len(jfs.handler.dirRefreshSem) != 1 {
			t.Fatalf("duplicate cold job: active=%d keys=%d", active, keys)
		}
		if store.DB().Stats().WaitCount != waits+1 {
			t.Fatal("more than the owned cold publication waited on SQL")
		}
		for i := 0; i < 2; i++ {
			if jfs.handler.stopHandlerWithDeadline(5 * time.Millisecond) {
				t.Error("handler reported stopped while SQL publication was still active")
			}
			if jfs.handler.startDirRefresh("after-stop", func() { t.Error("work admitted after stop") }) {
				t.Error("refresh admission reopened")
			}
		}
	case <-time.After(250 * time.Millisecond):
		t.Error("cold directory remained blocked on SQL beyond its complete-operation budget")
	}
	release()
	if !joined {
		select {
		case <-done:
			joined = true
		case <-time.After(3 * time.Second):
			t.Fatal("cold reader did not finish after SQL release")
		}
	}
	if !pollUntil(t, 3*time.Second, func() bool { kids, _ := store.ListChildren("cold"); return len(kids) == 2 }) {
		t.Fatal("released cold job did not publish both children")
	}
	if !jfs.handler.stopHandlerWithDeadline(time.Second) {
		t.Fatal("handler did not join released refresh")
	}
}

// Exercise the production RPC path as well: READDIRPLUS must write its empty
// reply before SQL is released, then freshly enumerate exact children afterward.
func TestColdDirectoryWireBudgetIncludesSQL(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	t.Setenv("JM_SYNC_COLD_POPULATE", "1")
	if metadata.ServeFromSQLite() {
		t.Fatal("fixture requires RAM serving")
	}
	class := netprofile.ClassFast
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	jfs, store, root := newDirRefreshHarness(t)
	mkFUSEDir(t, root, "cold", "a.txt", "b.txt")
	if err := store.Insert(metadata.MakeEntry("cold", true, 0, time.Unix(1700000000, 0), 3001)); err != nil {
		t.Fatal(err)
	}
	expected := map[string]uint64{}
	for _, name := range []string{"a.txt", "b.txt"} {
		fi, err := os.Stat(filepath.Join(root, "cold", name))
		if err != nil {
			t.Fatal(err)
		}
		expected[name] = fi.Sys().(*syscall.Stat_t).Ino
	}
	conn, err := net.DialTimeout("tcp", bindingRPCAddress(t, jfs.handler), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	client := newLatencyClient(conn)
	handle := jfs.handler.ToHandle(jfs, []string{"cold"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.QuiesceForLatencyAcceptance(ctx); err != nil {
		t.Fatal(err)
	}
	store.DB().SetMaxOpenConns(1)
	held, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() { once.Do(func() { _ = held.Close() }) }
	defer func() {
		release()
		if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
			t.Error("RPC refresh did not join")
		}
	}()
	waits := store.DB().Stats().WaitCount
	pending, err := client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: handle, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, time.Second, func() bool { return store.DB().Stats().WaitCount > waits }) {
		t.Fatal("RPC cold job did not reach held SQL")
	}
	reply, err := pending.await(time.Now().Add(250 * time.Millisecond))
	if err != nil {
		t.Fatal("READDIRPLUS blocked behind SQL:", err)
	}
	if elapsed := reply.at.Sub(pending.sent); elapsed > 100*time.Millisecond {
		t.Errorf("cold wire response took %s, exceeds 100ms allowance", elapsed)
	} else {
		t.Logf("cold READDIRPLUS returned before SQL release in %s", elapsed)
	}
	if err := checkColdWireListing(reply, map[string]uint64{}); err != nil {
		t.Fatal(err)
	}
	release()
	if !pollUntil(t, 3*time.Second, func() bool { kids, _ := store.ListChildren("cold"); return len(kids) == 2 }) {
		t.Fatal("cold RPC job did not publish both children")
	}
	pending, err = client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: handle, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	reply, err = pending.await(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkColdWireListing(reply, expected); err != nil {
		t.Fatal(err)
	}
}

func checkColdWireListing(reply latencyReply, expected map[string]uint64) error {
	reader, err := latencyOK(reply)
	if err != nil {
		return err
	}
	if err := latencyPostAttr(reader); err != nil {
		return err
	}
	var verifier uint64
	if err := xdr.Read(reader, &verifier); err != nil {
		return err
	}
	next, err := xdr.ReadUint32(reader)
	if err != nil {
		return err
	}
	seen := map[string]uint64{}
	for next != 0 {
		var inode, cookie uint64
		if err := xdr.Read(reader, &inode); err != nil {
			return err
		}
		name, err := latencyOpaque(reader)
		if err != nil {
			return err
		}
		if err := xdr.Read(reader, &cookie); err != nil {
			return err
		}
		if err := latencyPostAttr(reader); err != nil {
			return err
		}
		present, err := xdr.ReadUint32(reader)
		if err != nil {
			return err
		}
		if present != 0 {
			handle, err := latencyOpaque(reader)
			if err != nil || len(handle) != 28 {
				return fmt.Errorf("invalid child handle size=%d err=%v", len(handle), err)
			}
		}
		key := string(name)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate entry %q", key)
		}
		if key != "." && key != ".." && present == 0 {
			return fmt.Errorf("missing child handle for %q", key)
		}
		seen[key] = inode
		next, err = xdr.ReadUint32(reader)
		if err != nil {
			return err
		}
	}
	eof, err := xdr.ReadUint32(reader)
	if err != nil || eof != 1 || reader.Len() != 0 || len(seen) != len(expected)+2 {
		return fmt.Errorf("listing boundary: eof=%d remaining=%d count=%d err=%v", eof, reader.Len(), len(seen), err)
	}
	if seen["."] != 3001 {
		return fmt.Errorf("directory inode %d", seen["."])
	}
	if _, ok := seen[".."]; !ok {
		return fmt.Errorf("missing parent entry")
	}
	for name, inode := range expected {
		if seen[name] != inode {
			return fmt.Errorf("%s inode=%d want=%d", name, seen[name], inode)
		}
	}
	return nil
}
