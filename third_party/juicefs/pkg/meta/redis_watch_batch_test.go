//go:build !noredis

package meta

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Tests for redis_watch_batch.go (JuiceMount patch 0019). They need
// JM_WATCH_BATCH_TEST_REDIS_URL=redis://host:port/db, a disposable Redis 7.4
// database of their own: each test flushes it.

func watchBatchURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("JM_WATCH_BATCH_TEST_REDIS_URL")
	if u == "" {
		t.Skip("set JM_WATCH_BATCH_TEST_REDIS_URL to a disposable Redis database of its own (it is flushed)")
	}
	if !strings.HasPrefix(u, "redis://") || !strings.Contains(strings.TrimPrefix(u, "redis://"), "/") {
		t.Fatal("JM_WATCH_BATCH_TEST_REDIS_URL must be an explicit redis://host:port/db URL")
	}
	return u
}

// writeCounter counts client writes: go-redis writes one command or one
// pipeline per Write, so each write is one round trip.
type writeCounter struct{ n atomic.Int64 }

type countedConn struct {
	net.Conn
	c *writeCounter
}

func (c countedConn) Write(p []byte) (int, error) {
	c.c.n.Add(1)
	return c.Conn.Write(p)
}

// countedMeta is a formatted volume whose Redis client has one connection
// and counts round trips.
func countedMeta(t *testing.T) (*redisMeta, *writeCounter) {
	t.Helper()
	u := watchBatchURL(t)
	store, err := newRedisMeta("redis", strings.TrimPrefix(u, "redis://"), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	m := store.(*redisMeta)
	if err := m.rdb.FlushDB(Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := m.Init(&Format{Name: "watchbatch", UUID: "watch-batch-test", BlockSize: 4096}, true); err != nil {
		t.Fatal(err)
	}
	opt := *m.rdb.(*redis.Client).Options()
	opt.PoolSize, opt.MinIdleConns, opt.MaxIdleConns = 1, 0, 1
	wc := &writeCounter{}
	var d net.Dialer
	opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return countedConn{c, wc}, nil
	}
	old := m.rdb
	m.rdb = redis.NewClient(&opt)
	_ = old.Close()
	t.Cleanup(func() { _ = m.rdb.Close() })
	if err := m.rdb.Ping(Background()).Err(); err != nil { // dial and handshake now
		t.Fatal(err)
	}
	return m, wc
}

// uncounted is a second client of the same database whose round trips are
// not counted.
func uncounted(m *redisMeta) *redis.Client {
	opt := *m.rdb.(*redis.Client).Options()
	opt.Dialer = nil
	return redis.NewClient(&opt)
}

func setWatchBatch(t *testing.T, on bool) {
	prev := watchBatchOff
	watchBatchOff = !on
	t.Cleanup(func() { watchBatchOff = prev })
}

// One small file: create, then commit one slice on close. Upstream needs 5
// round trips for the create and 7 for the commit; this build needs 2 each.
func TestJMWatchBatchRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		on                   bool
		wantMknod, wantWrite int64
	}{
		// Off: the WATCH, UNWATCH and birth-guard WATCH round trips remain;
		// only the reads this patch pipelines explicitly are joined.
		{false, 4, 5},
		{true, 2, 2},
	} {
		m, wc := countedMeta(t)
		setWatchBatch(t, tc.on)
		ctx := Background()
		ino := Ino(1000)
		attr := &Attr{Typ: TypeFile, Mode: 0644, Nlink: 1, Parent: RootInode, Full: true}
		wc.n.Store(0)
		if st := m.doMknod(ctx, RootInode, "small", TypeFile, 0644, 022, "", &ino, attr); st != 0 {
			t.Fatalf("mknod: %v", st)
		}
		if got := wc.n.Load(); got != tc.wantMknod {
			t.Errorf("batch=%v: create took %d round trips, want %d", tc.on, got, tc.wantMknod)
		}
		var n int
		var delta dirStat
		wc.n.Store(0)
		if st := m.doWrite(ctx, ino, 0, 0, Slice{Id: 7, Size: 5, Len: 5}, time.Now(), &n, &delta, attr); st != 0 {
			t.Fatalf("write: %v", st)
		}
		if got := wc.n.Load(); got != tc.wantWrite {
			t.Errorf("batch=%v: commit took %d round trips, want %d", tc.on, got, tc.wantWrite)
		}
		if attr.Length != 5 || n != 1 {
			t.Fatalf("batch=%v: length %d slices %d", tc.on, attr.Length, n)
		}
		assertNoWatches(t, m)
	}
}

// watchCount reads the WATCH count of the client's only connection (Redis
// 7.4 CLIENT INFO field watch=).
func watchCount(t *testing.T, m *redisMeta) string {
	t.Helper()
	info, err := m.rdb.Do(Background(), "CLIENT", "INFO").Text()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range strings.Fields(info) {
		if strings.HasPrefix(f, "watch=") {
			return strings.TrimPrefix(f, "watch=")
		}
	}
	t.Fatalf("no watch= in CLIENT INFO (Redis 7.4 or later required): %s", info)
	return ""
}

func assertNoWatches(t *testing.T, m *redisMeta) {
	t.Helper()
	if w := watchCount(t, m); w != "0" {
		t.Fatalf("a pooled connection still holds %s WATCHed keys", w)
	}
}

// A change after the transaction's first read aborts its EXEC; the batched
// WATCH covers that read exactly as a separate WATCH did.
func TestJMWatchBatchCoversReads(t *testing.T) {
	m, _ := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	other := uncounted(m)
	defer other.Close()
	k, k2 := m.prefix+"wb:a", m.prefix+"wb:b"
	m.rdb.Set(ctx, k, "1", 0)
	m.rdb.Set(ctx, k2, "1", 0)

	run := func(txf func(tx *redis.Tx) error, keys ...string) error {
		return m.watchTxn(ctx, txf, keys)
	}
	commit := func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, m.prefix+"wb:out", "x", 0)
			return nil
		})
		return err
	}

	// The initial WATCH, sent with the first read.
	err := run(func(tx *redis.Tx) error {
		if err := tx.Get(ctx, k).Err(); err != nil {
			return err
		}
		other.Set(ctx, k, "2", 0)
		return commit(tx)
	}, k)
	if err != redis.TxFailedErr {
		t.Fatalf("change after the first read: %v, want an aborted EXEC", err)
	}
	// A WATCH added inside the callback, sent with the next read.
	err = run(func(tx *redis.Tx) error {
		if err := tx.Get(ctx, k).Err(); err != nil {
			return err
		}
		_ = tx.Watch(ctx, k2)
		if err := tx.Get(ctx, k2).Err(); err != nil {
			return err
		}
		other.Set(ctx, k2, "2", 0)
		return commit(tx)
	}, k)
	if err != redis.TxFailedErr {
		t.Fatalf("change after a later WATCH and its read: %v, want an aborted EXEC", err)
	}
	// A WATCH followed directly by MULTI/EXEC is sent before MULTI.
	err = run(func(tx *redis.Tx) error {
		_ = tx.Watch(ctx, k2)
		if err := tx.Get(ctx, k).Err(); err != nil {
			return err
		}
		other.Set(ctx, k2, "3", 0)
		return commit(tx)
	}, k)
	if err != redis.TxFailedErr {
		t.Fatalf("change after a WATCH sent with an unrelated read: %v, want an aborted EXEC", err)
	}
	// A change before the first read is seen by that read, and the
	// transaction commits on what it read.
	var got string
	err = run(func(tx *redis.Tx) error {
		other.Set(ctx, k, "before", 0)
		var err error
		got, err = tx.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return commit(tx)
	}, k)
	if err != nil || got != "before" {
		t.Fatalf("change before the first read: read %q, err %v", got, err)
	}
	assertNoWatches(t, m)
}

// No WATCH stays on a pooled connection, whatever way the callback ends.
func TestJMWatchBatchLeavesNoWatches(t *testing.T) {
	m, wc := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	k := m.prefix + "wb:c"
	other := uncounted(m)
	defer other.Close()
	for _, tc := range []struct {
		name  string
		txf   func(tx *redis.Tx) error
		trips int64
	}{
		{"returns before any command", func(tx *redis.Tx) error { return syscall.ENOENT }, 0},
		{"returns after a read", func(tx *redis.Tx) error { tx.Get(ctx, k); return syscall.ENOENT }, 2},
		{"commits", func(tx *redis.Tx) error {
			tx.Get(ctx, k)
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Incr(ctx, k); return nil })
			return err
		}, 2},
		{"aborted EXEC", func(tx *redis.Tx) error {
			tx.Get(ctx, k)
			other.Incr(ctx, k)
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Incr(ctx, k); return nil })
			return err
		}, 2},
		{"EXECABORT", func(tx *redis.Tx) error {
			tx.Get(ctx, k)
			_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Do(ctx, "NOSUCHCOMMAND")
				return nil
			})
			return err
		}, 2},
		{"later WATCH after EXEC", func(tx *redis.Tx) error {
			tx.Get(ctx, k)
			if _, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Incr(ctx, k); return nil }); err != nil {
				return err
			}
			tx.Watch(ctx, k)
			return tx.Get(ctx, k).Err()
		}, 4},
	} {
		wc.n.Store(0)
		_ = m.watchTxn(ctx, tc.txf, []string{k})
		if got := wc.n.Load(); got != tc.trips {
			t.Errorf("%s: %d round trips, want %d", tc.name, got, tc.trips)
		}
		assertNoWatches(t, m)
	}
}

// A WATCH that Redis refuses fails the transaction with that error; nothing
// later in the callback reaches Redis, so no unwatched EXEC can commit.
func TestJMWatchBatchFailedWatchStopsTheTransaction(t *testing.T) {
	m, _ := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	user := "jm-watch-batch-nowatch"
	if err := m.rdb.Do(ctx, "ACL", "SETUSER", user, "reset", "on", ">pw", "~*", "+@all", "-watch").Err(); err != nil {
		t.Fatal(err)
	}
	defer m.rdb.Do(ctx, "ACL", "DELUSER", user)
	opt := *m.rdb.(*redis.Client).Options()
	opt.Dialer, opt.Username, opt.Password = nil, user, "pw"
	nw := &redisMeta{rdb: redis.NewClient(&opt)}
	defer nw.rdb.Close()
	k := m.prefix + "wb:d"
	var readErr error
	err := nw.watchTxn(ctx, func(tx *redis.Tx) error {
		readErr = tx.Get(ctx, k).Err()
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error { p.Set(ctx, k, "written", 0); return nil })
		if err == nil {
			return errors.New("EXEC ran")
		}
		return nil // the WATCH error must still be returned
	}, []string{k})
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("transaction error %v, want the WATCH's NOPERM", err)
	}
	if readErr == nil || !strings.Contains(readErr.Error(), "NOPERM") {
		t.Fatalf("first read error %v, want the WATCH's NOPERM", readErr)
	}
	if n, _ := m.rdb.Exists(ctx, k).Result(); n != 0 {
		t.Fatal("a transaction whose WATCH failed wrote its key")
	}
}
