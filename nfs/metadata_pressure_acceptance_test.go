//go:build latency_acceptance

package nfs

// This opt-in acceptance uses the production NFS server and Store over one
// loopback TCP connection. A private FIFO blocks the real foreground os.Open
// in the fd pool; the catalog is a disposable, locally seeded SQLite mirror.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const metadataPressureCatalogEntries = 131000

func TestMetadataPressureRAMProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "ram-open" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=ram-open for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolation(t, false)
}

func TestMetadataPressureSQLiteProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "sqlite-open" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=sqlite-open for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolation(t, true)
}

// These cases navigate the large directory itself, including real continuation
// cookies, rather than a small sibling folder inside a large mirror.
func TestMetadataPressureRAMCatalogProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "ram-catalog-open" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=ram-catalog-open for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolationTarget(t, false, true, false)
}
func TestMetadataPressureSQLiteCatalogProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "sqlite-catalog-open" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=sqlite-catalog-open for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolationTarget(t, true, true, false)
}

func TestMetadataPressureRAMCatalogIngestProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "ram-catalog-ingest" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=ram-catalog-ingest for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolationTarget(t, false, true, true)
}
func TestMetadataPressureSQLiteCatalogIngestProductionOpenIsolation(t *testing.T) {
	if os.Getenv("JM_METADATA_PRESSURE_ACCEPTANCE") != "sqlite-catalog-ingest" {
		t.Skip("set JM_METADATA_PRESSURE_ACCEPTANCE=sqlite-catalog-ingest for this bounded local acceptance")
	}
	metadataPressureBackendOpenIsolationTarget(t, true, true, true)
}

// The substrate flag is cached once per process, so the two named cases are
// intentionally run as separate go test invocations.
func metadataPressureBackendOpenIsolation(t *testing.T, sqlite bool) {
	metadataPressureBackendOpenIsolationTarget(t, sqlite, false, false)
}

func metadataPressureBackendOpenIsolationTarget(t *testing.T, sqlite, catalog, ingest bool) {
	t.Setenv("JM_FTS_DEFER", "0")
	if sqlite {
		t.Setenv("JM_SERVE_FROM_SQLITE", "1")
		t.Setenv("JM_READDIR_PAGINATED", "1")
	} else {
		t.Setenv("JM_SERVE_FROM_SQLITE", "0")
		t.Setenv("JM_READDIR_PAGINATED", "0")
	}
	if got := metadata.ServeFromSQLite(); got != sqlite {
		t.Fatalf("serve substrate selected SQLite=%v, want %v", got, sqlite)
	}
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() { pin.SetOffline(false); pin.ResetFUSEIdentityForTest() })

	backing := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	var handler *JuiceMountHandler
	var ingestion *metadataPressureIngest
	t.Cleanup(func() {
		if ingestion != nil && !ingestion.stopAndJoin(5*time.Second) {
			t.Error("ingestion worker did not join; Store retained")
			return
		}
		if handler != nil {
			if !handler.StopHandler() {
				t.Error("handler Stop did not join; store retained")
				return
			}
			t.Log("handler Stop joined before Store close")
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	stamp := time.Unix(1700000000, 0)
	entries := make([]*metadata.Entry, 0, metadataPressureCatalogEntries+102)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("item-%03d.bin", i)
		if err := os.WriteFile(filepath.Join(backing, name), bytes.Repeat([]byte{'x'}, i+1), 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, metadata.MakeEntry(name, false, int64(i+1), stamp, uint64(1001+i)))
	}
	fifoPath := mkfifoT(t, backing, "slow.bin")
	entries = append(entries, metadata.MakeEntry("slow.bin", false, int64(len(latencyPayload)), stamp, 2001))
	if err := os.Mkdir(filepath.Join(backing, "catalog"), 0o700); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, metadata.MakeEntry("catalog", true, 0, stamp, 3001))
	for i := 0; i < metadataPressureCatalogEntries; i++ {
		entries = append(entries, metadata.MakeEntry(fmt.Sprintf("catalog/%06d.mov", i), false, 4096, stamp, uint64(4000+i)))
	}
	if ingest {
		entries = append(entries, metadata.MakeEntry("ingest", true, 0, stamp, 900001))
	}
	seedStart := time.Now()
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatal(err)
	}
	t.Logf("sqlite_serve=%v catalog_entries=%d mirror_rows=%d seed_elapsed=%s", sqlite, metadataPressureCatalogEntries, len(entries), time.Since(seedStart))
	// Verify an entry from the far end of the mirror, as well as the root
	// entries used by the wire-level navigation requests.
	if last := store.LookupByPath("catalog/130999.mov"); last == nil || last.Inode != 134999 {
		t.Fatalf("catalog tail missing or incorrect: %+v", last)
	}

	handler = NewHandler(store, backing)
	server := nfslib.NewManagedServer(handler)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	var conn net.Conn
	var fifoPartner *os.File
	t.Cleanup(func() {
		if fifoPartner == nil {
			fifoPartner, err = os.OpenFile(fifoPath, os.O_RDWR|syscall.O_NONBLOCK, 0)
			if err != nil {
				t.Errorf("cleanup FIFO release: %v", err)
			}
		}
		if conn != nil {
			_ = conn.Close()
		}
		_ = listener.Close()
		if !server.Quiesce(3 * time.Second) {
			t.Error("NFS server did not quiesce")
		}
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("NFS Serve goroutine did not stop")
		}
		if fifoPartner != nil {
			_ = fifoPartner.Close()
		}
	})
	go func() { serveDone <- server.Serve(listener) }()
	conn, err = net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client := newLatencyClient(conn)
	mount, err := client.send(100005, 1, []byte(handler.ExportPath()))
	if err != nil {
		t.Fatal(err)
	}
	mountReply, err := mount.await(time.Now().Add(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	mountBody, err := latencyOK(mountReply)
	if err != nil {
		t.Fatal(err)
	}
	rootHandle, err := latencyOpaque(mountBody)
	if err != nil || len(rootHandle) == 0 {
		t.Fatalf("MOUNT root handle %x: %v", rootHandle, err)
	}
	childHandle := handler.ToHandle(nil, []string{"item-050.bin"})
	slowHandle := handler.ToHandle(nil, []string{"slow.bin"})
	if len(childHandle) == 0 || len(slowHandle) == 0 {
		t.Fatal("seeded handles missing")
	}

	read, err := client.send(100003, uint32(nfslib.NFSProcedureRead), latencyReadArgs{Handle: slowHandle, Count: 4})
	if err != nil {
		t.Fatal(err)
	}
	waitReadOpenCondition(t, "production foreground open admission", func() bool { return len(nfsLstatGate) == 1 })
	waitOpensInFlight(t, handler.fdPool, 1)
	rounds := 16
	if catalog {
		// Bound malformed continuation loops; successful enumeration reaches exact EOF.
		rounds = 1000
	}
	var calls []latencyNavCall
	samples := map[string][]time.Duration{}
	observationDeadline := 2 * time.Second
	if catalog {
		observationDeadline = 5 * time.Second
		if ingest {
			ingestion = startMetadataPressureIngest(store)
			select {
			case <-ingestion.ready:
			case <-ingestion.done:
				t.Fatalf("ingestion stopped before first commit: %v", ingestion.err)
			case <-time.After(10 * time.Second):
				t.Fatal("ingestion did not commit first bounded batch")
			}
		}
		navStarted := time.Now()
		var commitsBefore int64
		if ingestion != nil {
			commitsBefore = ingestion.commits.Load()
		}
		calls, samples = metadataPressureCatalogPages(t, handler, client, rootHandle, childHandle, rounds, ingestion)
		navFinished := time.Now()
		if ingestion != nil {
			commitsAfter := ingestion.commits.Load()
			select {
			case <-ingestion.done:
				t.Fatalf("ingestion ended before navigation completed: %v", ingestion.err)
			default:
			}
			if !ingestion.stopAndJoin(5 * time.Second) {
				t.Fatal("ingestion did not join after navigation")
			}
			if ingestion.err != nil {
				t.Fatalf("ingestion failed: %v", ingestion.err)
			}
			if commitsAfter-commitsBefore < 2 {
				t.Fatalf("insufficient actual concurrent batch commits: before=%d after=%d", commitsBefore, commitsAfter)
			}
			ingestion.report(t, navStarted, navFinished, commitsBefore, commitsAfter)
		}
		rounds = len(samples["readdirplus"])
		if rounds <= 16 {
			t.Fatalf("full catalog only used %d pages", rounds)
		}
	} else {
		for i := 0; i < rounds; i++ {
			for _, spec := range []struct {
				kind string
				proc nfslib.NFSProcedure
				args interface{}
			}{
				{"getattr", nfslib.NFSProcedureGetAttr, childHandle},
				{"lookup", nfslib.NFSProcedureLookup, nfslib.DirOpArg{Handle: rootHandle, Filename: []byte("item-050.bin")}},
				{"readdirplus", nfslib.NFSProcedureReadDirPlus, latencyDirArgs{Handle: rootHandle, DirCount: 65536, MaxCount: 65536}},
			} {
				p, err := client.send(100003, uint32(spec.proc), spec.args)
				if err != nil {
					t.Fatal(err)
				}
				calls = append(calls, latencyNavCall{kind: spec.kind, p: p})
			}
		}
		deadline := time.Now().Add(2 * time.Second)
		for _, call := range calls {
			reply, err := call.p.await(deadline)
			if err != nil {
				t.Fatalf("%s response missed context deadline while backend OpenFile was held: %v", call.kind, err)
			}
			if call.kind == "readdirplus" {
				if _, err := latencyListingWithCount(reply, 104); err != nil {
					t.Fatalf("%s content: %v", call.kind, err)
				}
			} else if _, err := latencyCheckNav(call.kind, reply, childHandle); err != nil {
				t.Fatalf("%s content: %v", call.kind, err)
			}
			samples[call.kind] = append(samples[call.kind], reply.at.Sub(call.p.sent))
		}
	}
	for _, kind := range []string{"getattr", "lookup", "readdirplus"} {
		values := samples[kind]
		if len(values) != rounds {
			t.Fatalf("%s samples=%d want=%d", kind, len(values), rounds)
		}
		p50, p99, max := latencySamples(values)
		t.Logf("held_production_open %s count=%d p50=%s p99=%s max=%s observation_deadline=%s", kind, len(values), p50, p99, max, observationDeadline)
		if p99 > 250*time.Millisecond || max > 500*time.Millisecond {
			t.Errorf("%s local navigation latency exceeded 250ms p99 / 500ms max while backend open held: p99=%s max=%s", kind, p99, max)
		}
	}
	if len(nfsLstatGate) != 1 {
		t.Fatalf("production foreground open retired before navigation: in_flight=%d", len(nfsLstatGate))
	}
	waitOpensInFlight(t, handler.fdPool, 1)
	fifoPartner, err = os.OpenFile(fifoPath, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("release private FIFO: %v", err)
	}
	readReply, err := read.await(time.Now().Add(2 * time.Second))
	if err != nil {
		t.Fatalf("held READ did not settle after backend open release: %v", err)
	}
	if len(readReply.body) < 4 {
		t.Fatalf("held READ reply too short: %x", readReply.body)
	}
	// Full traversal can outlast the foreground open deadline; the actual
	// syscall must still remain owned until the FIFO is released above.
	status := binary.BigEndian.Uint32(readReply.body[:4])
	if status != uint32(nfslib.NFSStatusIO) && (!catalog || status != uint32(nfslib.NFSStatusJukebox)) {
		t.Fatalf("production FIFO READ status=%d, want unseekable I/O or bounded-open JUKEBOX", status)
	}
	t.Logf("production FIFO READ status=%d; underlying open remained held until all catalog replies", status)
	waitReadOpenCondition(t, "production foreground open retirement", func() bool { return len(nfsLstatGate) == 0 })
	waitOpensInFlight(t, handler.fdPool, 0)
	t.Logf("wire_navigation_completed_before_unrelated_backend_open_release=%d/%d", len(calls), len(calls))
}

// metadataPressureCatalogPages validates all 131000 children through the final
// EOF while the unrelated production foreground open syscall remains held.
func metadataPressureCatalogPages(t *testing.T, handler *JuiceMountHandler, client *latencyClient, rootHandle, childHandle []byte, rounds int, ingestion *metadataPressureIngest) ([]latencyNavCall, map[string][]time.Duration) {
	t.Helper()
	catalogHandle := handler.ToHandle(nil, []string{"catalog"})
	if len(catalogHandle) == 0 {
		t.Fatal("catalog handle missing")
	}
	var calls []latencyNavCall
	samples := map[string][]time.Duration{}
	var cookie, verifier uint64
	nextChild := 0
	started := time.Now()
	overallDeadline := started.Add(60 * time.Second)
	if err := client.conn.SetWriteDeadline(overallDeadline); err != nil {
		t.Fatal(err)
	}
	defer client.conn.SetWriteDeadline(time.Time{})
	for i := 0; i < rounds; i++ {
		if !time.Now().Before(overallDeadline) {
			t.Fatal("full catalog exceeded 60s observation cap")
		}
		var round []latencyNavCall
		for _, spec := range []struct {
			kind string
			proc nfslib.NFSProcedure
			args interface{}
		}{
			{"readdirplus", nfslib.NFSProcedureReadDirPlus, latencyDirArgs{Handle: catalogHandle, Cookie: cookie, CookieVerif: verifier, DirCount: 65536, MaxCount: 65536}},
			{"getattr", nfslib.NFSProcedureGetAttr, childHandle},
			{"lookup", nfslib.NFSProcedureLookup, nfslib.DirOpArg{Handle: rootHandle, Filename: []byte("item-050.bin")}},
		} {
			pending, err := client.send(100003, uint32(spec.proc), spec.args)
			if err != nil {
				t.Fatal(err)
			}
			round = append(round, latencyNavCall{kind: spec.kind, p: pending})
		}
		calls = append(calls, round...)
		deadline := time.Now().Add(5 * time.Second)
		if overallDeadline.Before(deadline) {
			deadline = overallDeadline
		}
		for _, call := range round {
			reply, err := call.p.await(deadline)
			if err != nil {
				t.Fatalf("catalog page%d %s missed observation deadline: %v", i, call.kind, err)
			}
			elapsed := reply.at.Sub(call.p.sent)
			if call.kind == "readdirplus" {
				count, last, gotVerifier, err := metadataPressureCatalogListingPage(reply, nextChild, i == 0, func(name string) []byte { return handler.ToHandle(nil, []string{"catalog", name}) })
				if err != nil {
					t.Fatalf("catalog page%d content: %v", i, err)
				}
				if i > 0 && gotVerifier != verifier {
					t.Fatalf("verifier changed page%d", i)
				}
				nextChild += count
				cookie = last
				verifier = gotVerifier
				var ingestCommits int64
				if ingestion != nil {
					ingestCommits = ingestion.commits.Load()
				}
				t.Logf("catalog page=%d children=%d total_checked=%d elapsed=%s cookie=%d ingest_commits=%d", i, count, nextChild, elapsed, cookie, ingestCommits)
			} else if _, err := latencyCheckNav(call.kind, reply, childHandle); err != nil {
				t.Fatalf("%s content: %v", call.kind, err)
			}
			samples[call.kind] = append(samples[call.kind], elapsed)
		}
		if nextChild == metadataPressureCatalogEntries {
			break
		}
	}
	if nextChild != metadataPressureCatalogEntries {
		t.Fatalf("checked catalog children=%d, want exactly%d including terminal EOF", nextChild, metadataPressureCatalogEntries)
	}
	if time.Now().After(overallDeadline) {
		t.Fatal("full catalog exceeded total observation deadline")
	}
	t.Logf("full_catalog_verified children=%d pages=%d wire_replies=%d elapsed=%s", nextChild, len(samples["readdirplus"]), len(calls), time.Since(started))
	return calls, samples
}

// Validate every ordinary entry, attr, cookie and handle, including exact
// terminal EOF; missing/duplicate entries or early/late EOF are errors.
func metadataPressureCatalogListingPage(reply latencyReply, start int, first bool, wantHandle func(string) []byte) (int, uint64, uint64, error) {
	r, err := latencyOK(reply)
	if err != nil {
		return 0, 0, 0, err
	}
	if err = latencyPostAttr(r); err != nil {
		return 0, 0, 0, err
	}
	var verifier uint64
	if err = xdr.Read(r, &verifier); err != nil {
		return 0, 0, 0, err
	}
	if verifier == 0 {
		return 0, 0, 0, fmt.Errorf("missing verifier")
	}
	next, err := xdr.ReadUint32(r)
	if err != nil {
		return 0, 0, 0, err
	}
	count, dots := 0, 0
	var last uint64
	for next != 0 {
		var id, cookie uint64
		if err = xdr.Read(r, &id); err != nil {
			return 0, 0, 0, err
		}
		name, err := latencyOpaque(r)
		if err != nil {
			return 0, 0, 0, err
		}
		if err = xdr.Read(r, &cookie); err != nil {
			return 0, 0, 0, err
		}
		attrPresent, err := xdr.ReadUint32(r)
		if err != nil {
			return 0, 0, 0, err
		}
		var attr nfslib.FileAttribute
		if attrPresent != 0 {
			if err = xdr.Read(r, &attr); err != nil {
				return 0, 0, 0, err
			}
		}
		handlePresent, err := xdr.ReadUint32(r)
		if err != nil {
			return 0, 0, 0, err
		}
		var handle []byte
		if handlePresent != 0 {
			handle, err = latencyOpaque(r)
			if err != nil {
				return 0, 0, 0, err
			}
		}
		n := string(name)
		if n == "." || n == ".." {
			if !first || (dots == 0 && n != ".") || (dots == 1 && n != "..") || dots > 1 || cookie != uint64(dots) {
				return 0, 0, 0, fmt.Errorf("invalid dot %q cookie%d", n, cookie)
			}
			dots++
		} else {
			idx := start + count
			if idx >= metadataPressureCatalogEntries || n != fmt.Sprintf("%06d.mov", idx) || id != uint64(4000+idx) || cookie != uint64(idx+2) || attrPresent != 1 || attr.Fileid != id || attr.Filesize != 4096 || !bytes.Equal(handle, wantHandle(n)) {
				return 0, 0, 0, fmt.Errorf("idx%d name%q id%d cookie%d attr%+v handle%d", idx, n, id, cookie, attr, len(handle))
			}
			count++
			last = cookie
		}
		next, err = xdr.ReadUint32(r)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	eof, err := xdr.ReadUint32(r)
	wantEOF := uint32(0)
	if start+count == metadataPressureCatalogEntries {
		wantEOF = 1
	}
	if err != nil || eof != wantEOF || r.Len() != 0 || count == 0 || (first && dots != 2) {
		return 0, 0, 0, fmt.Errorf("page count%d dots%d eof%d trailing%d err%v", count, dots, eof, r.Len(), err)
	}
	return count, last, verifier, nil
}

// The pressure worker uses the production, epoch-qualified SCAN publication
// method on a bounded independent 4,000-path set. The first eight 500-row
// batches ingest new names; later batches rescan changed sizes/mtimes. It never
// changes catalog names, so the stable traversal oracle remains exact.
type metadataPressureCommit struct{ started, finished time.Time }
type metadataPressureIngest struct {
	stop, done, ready chan struct{}
	stopOnce          sync.Once
	commits           atomic.Int64
	started           time.Time
	samples           []metadataPressureCommit
	err               error // read only after done is closed
}

func startMetadataPressureIngest(store *metadata.Store) *metadataPressureIngest {
	p := &metadataPressureIngest{stop: make(chan struct{}), done: make(chan struct{}), ready: make(chan struct{}), started: time.Now()}
	go func() {
		defer close(p.done)
		epoch, err := store.CaptureStoreEpoch()
		if err != nil {
			p.err = err
			return
		}
		deadline := p.started.Add(70 * time.Second)
		for batchNumber := 0; batchNumber < 4096; batchNumber++ {
			select {
			case <-p.stop:
				return
			default:
			}
			if time.Now().After(deadline) {
				p.err = fmt.Errorf("ingestion exceeded 70s lifetime")
				return
			}
			batch := make([]*metadata.Entry, 500)
			for j := range batch {
				index := (batchNumber%8)*500 + j
				batch[j] = metadata.MakeEntry(fmt.Sprintf("ingest/%06d.mov", index), false, int64(4096+batchNumber), time.Unix(1800000000+int64(batchNumber), 0), uint64(1000000+index))
			}
			started := time.Now()
			epoch, err = store.ScanUpsertBatch(batch, epoch, nil)
			finished := time.Now()
			if err != nil {
				p.err = err
				return
			}
			p.samples = append(p.samples, metadataPressureCommit{started, finished})
			p.commits.Add(1)
			if batchNumber == 0 {
				close(p.ready)
			}
		}
		p.err = fmt.Errorf("ingestion hit bounded4096batch cap before stop")
	}()
	return p
}
func (p *metadataPressureIngest) stopAndJoin(timeout time.Duration) bool {
	p.stopOnce.Do(func() { close(p.stop) })
	select {
	case <-p.done:
		return true
	case <-time.After(timeout):
		return false
	}
}
func (p *metadataPressureIngest) report(t *testing.T, start, end time.Time, before, after int64) {
	t.Helper()
	durations := make([]time.Duration, 0, len(p.samples))
	inside := 0
	for i, s := range p.samples {
		durations = append(durations, s.finished.Sub(s.started))
		if !s.finished.Before(start) && !s.finished.After(end) {
			inside++
		}
		t.Logf("ingest_commit batch=%d rows=500 start_ns=%d end_ns=%d", i, s.started.Sub(p.started).Nanoseconds(), s.finished.Sub(p.started).Nanoseconds())
	}
	p50, p99, max := latencySamples(durations)
	t.Logf("ingest_overlap navigation_start_ns=%d navigation_end_ns=%d before=%d after=%d committed_during=%d total_batches=%d rows_written=%d batch_p50=%s batch_p99=%s batch_max=%s worker_joined=true", start.Sub(p.started).Nanoseconds(), end.Sub(p.started).Nanoseconds(), before, after, inside, len(p.samples), len(p.samples)*500, p50, p99, max)
	if inside < 2 {
		t.Errorf("fewer than2 commits completed in actual navigation interval: %d", inside)
	}
}
