package metadata

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// jfsAttr is a juicefs i-key value, made by juicefs itself.
func jfsAttr(mtime, ctime time.Time, size uint64) string {
	return string((&meta.Attr{
		Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Length: size, Parent: 1,
		Mtime: mtime.Unix(), Mtimensec: uint32(mtime.Nanosecond()),
		Ctime: ctime.Unix(), Ctimensec: uint32(ctime.Nanosecond()),
	}).Marshal())
}

func TestDecodeInodeAttrCtimeMatchesJuiceFS(t *testing.T) {
	m, c := time.Unix(1700000000, 123), time.Unix(1700000007, 456789)
	raw := []byte(jfsAttr(m, c, 4096))
	gotM, size, ok := decodeInodeAttrTime(raw)
	gotC, cok := decodeInodeAttrCtime(raw)
	if !ok || !cok || !gotM.Equal(m) || !gotC.Equal(c) || size != 4096 {
		t.Fatalf("decoded mtime %v ctime %v size %d (ok %v %v)", gotM, gotC, size, ok, cok)
	}
}

// mtimeSpanHarness is a mirrored file "media.mov" (inode 42) and the attr
// push over a Redis whose i42 value the test sets.
func mtimeSpanHarness(t *testing.T) (*RedisClient, *Store, func(string), *[]string, uint64) {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	value := make([]byte, 9)
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	var mu sync.Mutex
	attr := jfsAttr(time.Unix(1700000000, 500), time.Unix(1700000000, 500), 4096)
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			return map[string]string{"media.mov": string(value)}, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()
			out := make([]interface{}, len(keys))
			for i := range keys {
				out[i] = attr
			}
			return out, nil
		},
	}
	t.Cleanup(func() { close(rc.stopCh) })
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	var fired []string
	store.SetOnScanContentInvalidated(func(p string, _ uint64) { fired = append(fired, p) })
	gen, _, _ := rc.backendAdmission()
	set := func(a string) { mu.Lock(); attr = a; mu.Unlock() }
	return rc, store, set, &fired, gen
}

func nfsMtime(t *testing.T, store *Store) time.Time {
	t.Helper()
	e := store.LookupByPath("media.mov")
	if e == nil {
		t.Fatal("no row")
	}
	return e.FileInfo().ModTime()
}

// A teammate's save stamps its final mtime at its first commit and commits
// the other parts later; those change only the ctime. The Mac keeps pages
// until it sees another mtime, so after every late commit the NFS mtime must
// move, and the content hooks must run. It never goes back, and the true
// mtime stays in the row.
func TestAttrPushLateCommitMovesTheNFSMtime(t *testing.T) {
	rc, store, set, fired, gen := mtimeSpanHarness(t)
	push := func() {
		t.Helper()
		if retry, err := rc.reconcileFileAttrs([]uint64{42}, gen); err != nil || retry != nil {
			t.Fatalf("push: retry %v err %v", retry, err)
		}
	}
	final := time.Unix(1700000100, 250)
	// First commit of the save: the final mtime arrives (an ordinary edit).
	set(jfsAttr(final, final.Add(10*time.Millisecond), 8192))
	push()
	if got := nfsMtime(t, store); !got.Equal(final) || len(*fired) != 1 {
		t.Fatalf("first commit: NFS mtime %v (want %v), hooks %d", got, final, len(*fired))
	}
	// Late commits: same mtime and size, newer ctime.
	var seen []time.Time
	for k := 1; k <= 3; k++ {
		set(jfsAttr(final, final.Add(time.Duration(k)*time.Second), 8192))
		push()
		got := nfsMtime(t, store)
		if !got.Equal(final.Add(time.Duration(k))) || len(*fired) != 1+k {
			t.Fatalf("late commit %d: NFS mtime %v, hooks %d", k, got, len(*fired))
		}
		if row := store.LookupByPath("media.mov"); !row.Mtime.Equal(final) || row.MtimeBump != uint32(k) {
			t.Fatalf("late commit %d: row mtime %v bump %d; the true mtime must stay", k, row.Mtime, row.MtimeBump)
		}
		if got.Unix() != final.Unix() {
			t.Fatalf("the shown second moved: %v", got)
		}
		seen = append(seen, got)
	}
	// The same observation again (another event, no commit): nothing moves.
	push()
	if got := nfsMtime(t, store); !got.Equal(seen[2]) || len(*fired) != 4 {
		t.Fatalf("repeat: NFS mtime %v hooks %d", got, len(*fired))
	}
	// A folder reconcile republishes the row without the bump: it stays.
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	if got := nfsMtime(t, store); !got.Equal(seen[2]) {
		t.Fatalf("after a folder reconcile the NFS mtime went back to %v", got)
	}
	// The next save: a new mtime, reported as it is.
	next := time.Unix(1700000200, 0)
	set(jfsAttr(next, next, 8192))
	push()
	if got := nfsMtime(t, store); !got.Equal(next) {
		t.Fatalf("next save: NFS mtime %v, want %v", got, next)
	}
}

// This client's own save comes back with an mtime the last observation did
// not have, and the first ctime of a file is only recorded: neither moves
// the NFS mtime or runs the hooks. A ctime-only change seen before any
// observation of that mtime is not counted either.
func TestAttrPushOwnSaveEchoKeepsTheMtime(t *testing.T) {
	rc, store, set, fired, gen := mtimeSpanHarness(t)
	// The push has seen the file before (its ctime is known).
	if _, err := rc.reconcileFileAttrs([]uint64{42}, gen); err != nil || len(*fired) != 0 {
		t.Fatalf("first observation: err %v hooks %d", err, len(*fired))
	}
	own := time.Unix(1700000300, 0) // the spool file's mtime, which the drain restores
	// The drain published the row itself.
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	row := store.LookupByPath("media.mov")
	e := MakeEntry("media.mov", false, 9000, own, 42)
	e.Mode, e.MtimeNsecKnown = row.Mode, true
	if _, err := store.ReconcileUpsertBatch([]*Entry{e}, epoch); err != nil {
		t.Fatal(err)
	}
	*fired = nil
	// Its echo: the restored mtime and the drain's later ctime (Chtimes).
	set(jfsAttr(own, own.Add(40*time.Second), 9000))
	if _, err := rc.reconcileFileAttrs([]uint64{42}, gen); err != nil {
		t.Fatal(err)
	}
	if got := nfsMtime(t, store); !got.Equal(own) || len(*fired) != 0 {
		t.Fatalf("own echo: NFS mtime %v (want %v), hooks %d", got, own, len(*fired))
	}
}

// The bump survives only a publication under the same inode and mtime.
func TestCarryMtimeBump(t *testing.T) {
	m := time.Unix(1700000000, 0)
	old := &Entry{Inode: 7, Mtime: m, MtimeBump: 3}
	for name, c := range map[string]struct {
		e    Entry
		want uint32
	}{
		"same mtime, no bump":   {Entry{Inode: 7, Mtime: m}, 3},
		"same mtime, higher":    {Entry{Inode: 7, Mtime: m, MtimeBump: 5}, 5},
		"another mtime":         {Entry{Inode: 7, Mtime: m.Add(time.Second)}, 0},
		"another inode":         {Entry{Inode: 8, Mtime: m}, 0},
		"another mtime, bumped": {Entry{Inode: 7, Mtime: m.Add(time.Second), MtimeBump: 1}, 1},
	} {
		e := c.e
		carryMtimeBump(old, &e)
		if e.MtimeBump != c.want {
			t.Errorf("%s: bump %d, want %d", name, e.MtimeBump, c.want)
		}
	}
}
