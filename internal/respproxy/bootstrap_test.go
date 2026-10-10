package respproxy

// Bootstrap measurement on a synthetic large volume (H8 plan §5.2). Opt-in:
//
//	JM_H8B_BOOTSTRAP_BENCH=1 JM_RESPPROXY_TEST_REDIS=127.0.0.1:<port> \
//	  go test -run TestH8BBootstrapScale -v ./internal/respproxy/

import (
	"bufio"
	"context"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func rssKiB(t *testing.T) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return n
}

func heapMiB() float64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

func TestH8BBootstrapScale(t *testing.T) {
	if os.Getenv("JM_H8B_BOOTSTRAP_BENCH") == "" {
		t.Skip("set JM_H8B_BOOTSTRAP_BENCH=1")
	}
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	const (
		top     = 50 // depth-1 folders
		perTop  = 20 // projects per folder -> 1000 projects
		perProj = 49 // subdirectories per project
		files   = 450000
	)
	next := uint64(2)
	alloc := func() uint64 { n := next; next++; return n }
	pipe := rdb.Pipeline()
	nCmd := 0
	flush := func(force bool) {
		if nCmd >= 5000 || (force && nCmd > 0) {
			c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if _, err := pipe.Exec(c); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			nCmd = 0
		}
	}
	put := func(parent uint64, name string, ino uint64, typ byte) {
		pipe.Set(ctx, "i"+inoStr(ino), mkAttr(typ, parent), 0)
		pipe.HSet(ctx, "d"+inoStr(parent), name, mkEntry(typ, ino))
		nCmd += 2
		flush(false)
	}
	pipe.Set(ctx, "i1", mkAttr(typeDir, 1), 0)
	var leaves []uint64
	dirs := 1
	for i := 0; i < top; i++ {
		d1 := alloc()
		put(1, "top"+strconv.Itoa(i), d1, typeDir)
		dirs++
		for j := 0; j < perTop; j++ {
			p := alloc()
			put(d1, "proj"+strconv.Itoa(j), p, typeDir)
			dirs++
			for k := 0; k < perProj; k++ {
				s := alloc()
				put(p, "sub"+strconv.Itoa(k), s, typeDir)
				dirs++
				leaves = append(leaves, s)
			}
		}
	}
	fileInos := make([]uint64, 0, files)
	for i := 0; i < files; i++ {
		f := alloc()
		put(leaves[i%len(leaves)], "f"+strconv.Itoa(i), f, typeFile)
		fileInos = append(fileInos, f)
	}
	flush(true)
	inodes := int(next - 1)
	t.Logf("synthetic volume: %d inodes, %d directories, %d projects", inodes, dirs, top*perTop)

	heap0, rss0 := heapMiB(), rssKiB(t)
	side := &SidePool{dial: func() (net.Conn, *bufio.Reader, error) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, nil, err
		}
		return c, bufio.NewReaderSize(c, 64<<10), nil
	}, max: 8}
	defer side.Close()
	eng := NewLeaseEngine(LeaseConfig{Side: side, VolumeDB: testDB, Logf: t.Logf, SweepEvery: time.Hour})
	t0 := time.Now()
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	startDur := time.Since(t0)
	defer eng.Close()
	if !eng.WaitScan(5 * time.Minute) {
		t.Fatal("background scan did not finish in 5 min")
	}
	total := time.Since(t0)
	bs := eng.Bootstrap()
	if bs.ScanErr != "" || bs.ScanKeys < dirs-1 {
		t.Fatalf("scan: %+v (want >= %d directory hashes)", bs, dirs-1)
	}
	if got := eng.anc.dirCount(); got < dirs {
		t.Fatalf("parent map has %d dirs, want %d", got, dirs)
	}
	heap1, rss1 := heapMiB(), rssKiB(t)

	// Resolution cost: cold files (side GET each), then warm.
	rng := rand.New(rand.NewSource(1))
	sample := make([]uint64, 2000)
	for i := range sample {
		sample[i] = fileInos[rng.Intn(len(fileInos))]
	}
	sr0 := eng.anc.sideReads.Load()
	t1 := time.Now()
	for _, f := range sample {
		locs, err := eng.anc.resolveInode(f)
		if err != nil || len(locs) != 1 || locs[0].kind != nsProject {
			t.Fatalf("resolve %d: %v %+v", f, err, locs)
		}
	}
	cold := time.Since(t1) / time.Duration(len(sample))
	coldReads := eng.anc.sideReads.Load() - sr0
	t2 := time.Now()
	for _, f := range sample {
		eng.anc.resolveInode(f)
	}
	warm := time.Since(t2) / time.Duration(len(sample))

	t.Logf("BOOTSTRAP sync roots: %s (%d dirs read); Start total: %s", bs.SyncDur.Round(time.Millisecond), bs.SyncDirs, startDur.Round(time.Millisecond))
	t.Logf("BOOTSTRAP background scan: %s, %d d* hashes, %d edges added; start->scan done %s",
		bs.ScanDur.Round(time.Millisecond), bs.ScanKeys, bs.ScanEdges, total.Round(time.Millisecond))
	t.Logf("MEMORY heap %.1f -> %.1f MiB (+%.1f); RSS %d -> %d KiB (+%.1f MiB)", heap0, heap1, heap1-heap0, rss0, rss1, float64(rss1-rss0)/1024)
	t.Logf("RESOLVE file -> project after scan: cold %s/op (%d side reads for %d files), warm %s/op", cold, coldReads, len(sample), warm)
	rdb.FlushDB(ctx)
}
