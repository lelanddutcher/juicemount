//go:build latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type navigationPinnedPaths map[string]struct{}

func (p navigationPinnedPaths) PinnedPaths() (map[string]struct{}, error) { return p, nil }

// Exercise production TCP dispatch and real eviction while one cold-directory
// publication waits on SQLite's writer lock. WAL readers remain available, so
// eviction must not turn a complete directory into an incomplete RAM listing.
// The bulk RAM entries supply pressure only; the entire known directory is
// durably committed before the writer is held. This is not a mounted/WAN test.
func TestNavigationDuringEvictionAndSQLWriter(t *testing.T) {
	navigationDuringEvictionAndSQLWriter(t, 32768, 16384, 250*time.Millisecond)
}

// A small overflow at the production cache limit must not stop navigation for
// a whole-catalog sort/rebuild. The longer observation deadline retains exact
// payloads after a timing miss; the reply budget remains 100ms in both cases.
func TestNavigationDefaultCacheEvictionAndSQLWriter(t *testing.T) {
	navigationDuringEvictionAndSQLWriter(t, metadata.DefaultMaxCacheSize+500, metadata.DefaultMaxCacheSize, 3*time.Second)
}

func navigationDuringEvictionAndSQLWriter(t *testing.T, pressure, budget int, responseWait time.Duration) {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	t.Setenv("JM_DURABLE_SERVE_FALLBACK", "1")
	t.Setenv("JM_SYNC_COLD_POPULATE", "1")
	if metadata.ServeFromSQLite() {
		t.Fatal("fixture requires default RAM point serving")
	}
	class := netprofile.ClassFast
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	jfs, store, root := newDirRefreshHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.QuiesceForLatencyAcceptance(ctx); err != nil {
		t.Fatal(err)
	}
	// Exclude speculative warmers from writer attribution. The separate cold
	// refresh admission and production SQL publication remain enabled.
	for i := 0; i < cap(jfs.handler.prefetchSem); i++ {
		jfs.handler.prefetchSem <- struct{}{}
	}
	defer func() {
		for len(jfs.handler.prefetchSem) > 0 {
			<-jfs.handler.prefetchSem
		}
	}()
	stamp := time.Unix(1700000000, 0)
	entries := []*metadata.Entry{
		metadata.MakeEntry("known", true, 0, stamp, 3002),
		metadata.MakeEntry("cold", true, 0, stamp, 3001),
		metadata.MakeEntry("bulk", true, 0, stamp, 3003),
	}
	known := map[string]*metadata.Entry{}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("item-%03d.bin", i)
		entry := metadata.MakeEntry("known/"+name, false, int64(i+1), stamp, uint64(1001+i))
		entries = append(entries, entry)
		known[name] = entry
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatal(err)
	}
	var committed int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM entries WHERE parent_path='known'`).Scan(&committed); err != nil || committed != 100 {
		t.Fatalf("known durable count=%d err=%v", committed, err)
	}
	store.SetPinChecker(navigationPinnedPaths{"known/item-050.bin": {}})
	for i := 0; i < pressure; i++ {
		store.InsertToCache(metadata.MakeEntry(fmt.Sprintf("bulk/file-%05d", i), false, 1, stamp.Add(time.Duration(i+1)*time.Second), uint64(10000+i)))
	}
	store.SetCacheBudgetForLatencyAcceptance(budget)
	mkFUSEDir(t, root, "cold", "a.txt", "b.txt")
	cold := map[string]*metadata.Entry{}
	for _, name := range []string{"a.txt", "b.txt"} {
		fi, err := os.Stat(filepath.Join(root, "cold", name))
		if err != nil {
			t.Fatal(err)
		}
		cold[name] = metadata.MakeEntry("cold/"+name, false, fi.Size(), fi.ModTime(), fi.Sys().(*syscall.Stat_t).Ino)
	}
	conn, err := net.DialTimeout("tcp", bindingRPCAddress(t, jfs.handler), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	client := newLatencyClient(conn)
	knownHandle := jfs.handler.ToHandle(jfs, []string{"known"})
	coldHandle := jfs.handler.ToHandle(jfs, []string{"cold"})
	childHandle := jfs.handler.ToHandle(jfs, []string{"known", "item-050.bin"})
	knownHandles := make(map[string][]byte, len(known))
	for name := range known {
		knownHandles[name] = jfs.handler.ToHandle(jfs, []string{"known", name})
	}

	// A real BEGIN IMMEDIATE holds only the writer. Do not exhaust the pool:
	// READDIRPLUS must use committed SQLite after actual RAM eviction.
	held, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := held.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		_ = held.Close()
		t.Fatal(err)
	}
	var once sync.Once
	released := false
	release := func() {
		once.Do(func() {
			released = true
			if _, err := held.ExecContext(context.Background(), "ROLLBACK"); err != nil {
				t.Errorf("release writer: %v", err)
			}
			if err := held.Close(); err != nil {
				t.Errorf("close held connection: %v", err)
			}
		})
	}
	defer func() {
		release()
		if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
			t.Error("cold refresh did not join")
		}
	}()
	var journal string
	if err := store.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("WAL reader preflight mode=%q err=%v", journal, err)
	}
	if store.DB().Stats().InUse != 1 {
		t.Fatal("unattributed SQL connection before cold job")
	}
	pendingCold, err := client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: coldHandle, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, time.Second, func() bool { return store.WriterBusyForLatencyAcceptance() && store.DB().Stats().InUse == 2 }) {
		t.Fatal("cold job did not own writer and wait on its SQL connection")
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE parent_path='cold'`).Scan(&committed); err != nil || committed != 0 {
		t.Fatalf("held cold publication count=%d err=%v", committed, err)
	}
	foregroundBefore := metrics.Default().Snapshot().FUSEAttrib.BySource["foreground"].Calls
	type evictionResult struct {
		elapsed  time.Duration
		finished time.Time
	}
	evictionDone := make(chan evictionResult, 1)
	go func() {
		start := time.Now()
		store.EvictForLatencyAcceptance()
		finished := time.Now()
		evictionDone <- evictionResult{finished.Sub(start), finished}
	}()
	joined := false
	defer func() {
		if !joined {
			select {
			case <-evictionDone:
			case <-time.After(20 * time.Second):
				t.Error("eviction did not join")
			}
		}
	}()
	observed := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.CacheLockActiveForLatencyAcceptance() {
			observed = true
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	if !observed {
		t.Fatal("did not observe actual eviction holding serving lock")
	}
	calls := []latencyNavCall{}
	for _, spec := range []struct {
		kind string
		proc nfslib.NFSProcedure
		args any
	}{
		{"getattr", nfslib.NFSProcedureGetAttr, childHandle},
		{"lookup", nfslib.NFSProcedureLookup, nfslib.DirOpArg{Handle: knownHandle, Filename: []byte("item-050.bin")}},
		{"readdirplus", nfslib.NFSProcedureReadDirPlus, latencyDirArgs{Handle: knownHandle, DirCount: 65536, MaxCount: 65536}},
	} {
		p, err := client.send(100003, uint32(spec.proc), spec.args)
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, latencyNavCall{spec.kind, p})
	}
	deadline = time.Now().Add(responseWait)
	for _, call := range calls {
		reply, err := call.p.await(deadline)
		if err != nil {
			t.Fatalf("%s blocked while writer held: %v", call.kind, err)
		}
		if released {
			t.Fatal("writer released before navigation")
		}
		if call.kind == "readdirplus" {
			err = checkPressureListing(reply, 3002, known, knownHandles)
		} else {
			_, err = latencyCheckNav(call.kind, reply, childHandle)
		}
		if err != nil {
			t.Errorf("%s payload: %v", call.kind, err)
		}
		elapsed := reply.at.Sub(call.p.sent)
		t.Logf("held_writer %s latency=%s", call.kind, elapsed)
		if elapsed > 100*time.Millisecond {
			t.Errorf("%s exceeded 100ms: %s", call.kind, elapsed)
		}
	}
	reply, err := pendingCold.await(time.Now().Add(250 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkColdWireListing(reply, map[string]uint64{}); err != nil {
		t.Fatal(err)
	}
	elapsed := reply.at.Sub(pendingCold.sent)
	t.Logf("held_writer cold READDIRPLUS latency=%s", elapsed)
	if elapsed > 100*time.Millisecond {
		t.Errorf("cold reply exceeded 100ms: %s", elapsed)
	}
	select {
	case result := <-evictionDone:
		joined = true
		t.Logf("actual eviction=%s", result.elapsed)
		for _, call := range calls {
			if !call.p.sent.Before(result.finished) {
				t.Errorf("inconclusive overlap: %s sent after eviction finished", call.kind)
			}
			t.Logf("%s sent %s before eviction finished", call.kind, result.finished.Sub(call.p.sent))
		}
	case <-time.After(20 * time.Second):
		t.Fatal("eviction did not complete")
	}
	if count, _ := store.CacheStats(); count != budget {
		t.Errorf("residency=%d want=%d", count, budget)
	}
	if store.PathCachedForLatencyAcceptance("known/item-000.bin") || !store.PathCachedForLatencyAcceptance("known/item-050.bin") {
		t.Fatal("fixture did not evict ordinary child and retain pinned child")
	}
	if _, ok := store.LookupRecentlyEvicted(1001); !ok {
		t.Fatal("evicted child recovery record missing")
	}
	// A prompt first response can precede victim removal in the corrected
	// implementation. Also require the complete durable listing after eviction,
	// while the SAME SQLite writer is still held.
	pendingComplete, err := client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: knownHandle, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	complete, err := pendingComplete.await(time.Now().Add(250 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPressureListing(complete, 3002, known, knownHandles); err != nil {
		t.Fatal(err)
	}
	if elapsed := complete.at.Sub(pendingComplete.sent); elapsed > 100*time.Millisecond {
		t.Errorf("post-eviction durable listing exceeded100ms: %s", elapsed)
	} else {
		t.Logf("post-eviction held_writer durable READDIRPLUS latency=%s", elapsed)
	}
	if !store.WriterBusyForLatencyAcceptance() || store.DB().Stats().InUse != 2 {
		t.Fatal("SQL publication was not still blocked after navigation")
	}
	if after := metrics.Default().Snapshot().FUSEAttrib.BySource["foreground"].Calls; after != foregroundBefore {
		t.Errorf("foreground FUSE calls during navigation=%d", after-foregroundBefore)
	}
	release()
	if !pollUntil(t, 3*time.Second, inFlightEmpty(jfs.handler)) {
		t.Fatal("cold publication did not finish")
	}
	coldHandles := map[string][]byte{}
	for name, want := range cold {
		entry := store.LookupByPath(want.Path)
		if entry == nil || entry.Inode != want.Inode || entry.Size != want.Size {
			t.Fatalf("cold publication mismatch %s", name)
		}
		coldHandles[name] = jfs.handler.ToHandle(jfs, []string{"cold", name})
	}
	pending, err := client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: coldHandle, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	reply, err = pending.await(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPressureListing(reply, 3001, cold, coldHandles); err != nil {
		t.Fatal(err)
	}
	t.Logf("complete durable known children=100; post-release cold children=2; foreground FUSE=0")
}

// Validate the whole listing, including each child's wire attributes and exact
// handle, so a fast truncated RAM listing cannot satisfy the latency oracle.
func checkPressureListing(reply latencyReply, directory uint64, expected map[string]*metadata.Entry, handles map[string][]byte) error {
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
		present, err := xdr.ReadUint32(reader)
		if err != nil {
			return err
		}
		var attr nfslib.FileAttribute
		if present != 0 {
			if err := xdr.Read(reader, &attr); err != nil {
				return err
			}
		}
		hasHandle, err := xdr.ReadUint32(reader)
		if err != nil {
			return err
		}
		var handle []byte
		if hasHandle != 0 {
			handle, err = latencyOpaque(reader)
			if err != nil {
				return err
			}
		}
		key := string(name)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate child %q", key)
		}
		seen[key] = inode
		if key != "." && key != ".." {
			want, ok := expected[key]
			if !ok || want.Inode != inode || present != 1 || attr.Fileid != want.Inode || attr.Filesize != uint64(want.Size) || hasHandle != 1 || !bytes.Equal(handle, handles[key]) {
				return fmt.Errorf("child %q identity/attributes/handle mismatch", key)
			}
		}
		next, err = xdr.ReadUint32(reader)
		if err != nil {
			return err
		}
	}
	eof, err := xdr.ReadUint32(reader)
	if err != nil || eof != 1 || reader.Len() != 0 || len(seen) != len(expected)+2 || seen["."] != directory {
		return fmt.Errorf("listing boundary eof=%d count=%d directory=%d remaining=%d err=%v", eof, len(seen), seen["."], reader.Len(), err)
	}
	if _, ok := seen[".."]; !ok {
		return fmt.Errorf("missing parent")
	}
	return nil
}
