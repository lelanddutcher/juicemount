//go:build latency_acceptance

package nfs

// This acceptance test uses one real loopback NFSv3 TCP connection. The only
// synthetic part is a channel held at the slow file's ReadAt boundary. It is
// a deterministic backend-wait model, not a physical network outage or a
// substitute for a Finder-on-macOS acceptance run.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const latencyPayload = "slow-read-content"

type latencyGate struct {
	entered    chan struct{}
	release    chan struct{}
	once       sync.Once
	releasedAt atomic.Int64
	started    atomic.Int64
}

func newLatencyGate() *latencyGate {
	return &latencyGate{entered: make(chan struct{}, nfslib.DefaultRPCSemaphoreSize+1), release: make(chan struct{})}
}

func (g *latencyGate) wait() {
	g.started.Add(1)
	g.entered <- struct{}{}
	<-g.release
}

func (g *latencyGate) Release() {
	g.once.Do(func() {
		g.releasedAt.Store(time.Now().UnixNano())
		close(g.release)
	})
}

type latencyFile struct {
	billy.File
	gate *latencyGate
}

// Keep the file's read-admission hook visible to onRead while delaying only
// the backend ReadAt call used by this acceptance fixture.
func (f *latencyFile) BeginReadAdmission() (func() error, func(), error) {
	if provider, ok := f.File.(interface {
		BeginReadAdmission() (func() error, func(), error)
	}); ok {
		return provider.BeginReadAdmission()
	}
	return nil, nil, nil
}

func (f *latencyFile) ReadAt(p []byte, off int64) (int, error) {
	f.gate.wait()
	return f.File.ReadAt(p, off)
}

type latencyFS struct {
	*juiceFS // Retain StatCacheOnly and other optional serving interfaces.
	gate     *latencyGate
}

// onRead calls Open, so overriding OpenFile alone would not intercept reads:
// the promoted juiceFS.Open dispatches to its own concrete OpenFile receiver.
func (f *latencyFS) Open(name string) (billy.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *latencyFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	file, err := f.juiceFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	if f.gate != nil && strings.TrimPrefix(filepath.Clean(name), string(os.PathSeparator)) == "slow.bin" && flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return &latencyFile{File: file, gate: f.gate}, nil
	}
	return file, nil
}

type latencyHandler struct {
	*JuiceMountHandler
	gate *latencyGate
}

func (h *latencyHandler) wrap(fs billy.Filesystem) billy.Filesystem {
	if concrete, ok := fs.(*juiceFS); ok {
		return &latencyFS{juiceFS: concrete, gate: h.gate}
	}
	return fs
}

func (h *latencyHandler) Mount(ctx context.Context, conn net.Conn, req nfslib.MountRequest) (nfslib.MountStatus, billy.Filesystem, []nfslib.AuthFlavor) {
	status, fs, auth := h.JuiceMountHandler.Mount(ctx, conn, req)
	return status, h.wrap(fs), auth
}

func (h *latencyHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	fs, names, err := h.JuiceMountHandler.FromHandle(handle)
	return h.wrap(fs), names, err
}

type latencyReply struct {
	body []byte
	at   time.Time
	err  error
}

type latencyPending struct {
	ch   chan latencyReply
	sent time.Time
}

type latencyClient struct {
	conn    net.Conn
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[uint32]chan latencyReply
	nextXID atomic.Uint32
}

func newLatencyClient(conn net.Conn) *latencyClient {
	c := &latencyClient{conn: conn, pending: make(map[uint32]chan latencyReply)}
	go c.readLoop()
	return c
}

func (c *latencyClient) readLoop() {
	for {
		var marker [4]byte
		if _, err := io.ReadFull(c.conn, marker[:]); err != nil {
			c.failAll(err)
			return
		}
		n := binary.BigEndian.Uint32(marker[:])
		if n&0x80000000 == 0 || n&0x7fffffff > 1<<20 || n&0x7fffffff < 24 {
			c.failAll(fmt.Errorf("invalid RPC record marker %#x", n))
			return
		}
		frame := make([]byte, n&0x7fffffff)
		if _, err := io.ReadFull(c.conn, frame); err != nil {
			c.failAll(err)
			return
		}
		xid := binary.BigEndian.Uint32(frame[:4])
		result := latencyReply{at: time.Now()}
		if binary.BigEndian.Uint32(frame[4:8]) != 1 || // RPC REPLY
			binary.BigEndian.Uint32(frame[8:12]) != 0 || // MSG_ACCEPTED
			binary.BigEndian.Uint32(frame[12:16]) != 0 || // AUTH_NULL
			binary.BigEndian.Uint32(frame[16:20]) != 0 || // verifier length
			binary.BigEndian.Uint32(frame[20:24]) != 0 { // SUCCESS
			result.err = fmt.Errorf("xid %d: rejected RPC envelope %x", xid, frame[:24])
		} else {
			result.body = frame[24:]
		}
		c.mu.Lock()
		ch := c.pending[xid]
		delete(c.pending, xid)
		c.mu.Unlock()
		if ch != nil {
			ch <- result
		}
	}
}

func (c *latencyClient) failAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for xid, ch := range c.pending {
		ch <- latencyReply{err: err}
		delete(c.pending, xid)
	}
}

func (c *latencyClient) send(program, proc uint32, args interface{}) (latencyPending, error) {
	var payload bytes.Buffer
	xid := c.nextXID.Add(1)
	for _, word := range []uint32{xid, 0} { // xid, CALL
		if err := binary.Write(&payload, binary.BigEndian, word); err != nil {
			return latencyPending{}, err
		}
	}
	version := uint32(3)
	if program == 100005 {
		version = 3
	}
	if err := xdr.Write(&payload, rpc.Header{Rpcvers: 2, Prog: program, Vers: version, Proc: proc, Cred: rpc.AuthNull, Verf: rpc.AuthNull}); err != nil {
		return latencyPending{}, err
	}
	if args != nil {
		if err := xdr.Write(&payload, args); err != nil {
			return latencyPending{}, err
		}
	}
	frame := make([]byte, payload.Len()+4)
	binary.BigEndian.PutUint32(frame[:4], uint32(payload.Len())|0x80000000)
	copy(frame[4:], payload.Bytes())
	p := latencyPending{ch: make(chan latencyReply, 1), sent: time.Now()}
	c.mu.Lock()
	c.pending[xid] = p.ch
	c.mu.Unlock()
	c.writeMu.Lock()
	_, err := c.conn.Write(frame)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, xid)
		c.mu.Unlock()
	}
	return p, err
}

func (p latencyPending) await(deadline time.Time) (latencyReply, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return latencyReply{}, fmt.Errorf("RPC deadline elapsed")
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case r := <-p.ch:
		return r, r.err
	case <-timer.C:
		return latencyReply{}, fmt.Errorf("RPC reply timed out")
	}
}

func latencyOK(r latencyReply) (*bytes.Reader, error) {
	if r.err != nil {
		return nil, r.err
	}
	if len(r.body) < 4 {
		return nil, fmt.Errorf("short NFS result: %x", r.body)
	}
	if status := binary.BigEndian.Uint32(r.body[:4]); status != 0 {
		return nil, fmt.Errorf("NFS status %d", status)
	}
	return bytes.NewReader(r.body[4:]), nil
}

// The imported xdr.ReadOpaque leaves XDR alignment bytes in the reader.
// Decode bounded opaque fields locally so the next union discriminant starts
// on its correct four-byte boundary.
func latencyOpaque(reader *bytes.Reader) ([]byte, error) {
	n, err := xdr.ReadUint32(reader)
	if err != nil {
		return nil, err
	}
	padding := (4 - n%4) % 4
	if n > 1<<20 || uint64(n)+uint64(padding) > uint64(reader.Len()) {
		return nil, fmt.Errorf("opaque length %d exceeds remaining %d bytes", n, reader.Len())
	}
	value := make([]byte, n)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, err
	}
	if _, err := reader.Seek(int64(padding), io.SeekCurrent); err != nil {
		return nil, err
	}
	return value, nil
}

func latencyPostAttr(reader io.Reader) error {
	present, err := xdr.ReadUint32(reader)
	if err != nil {
		return err
	}
	if present != 0 {
		var attr nfslib.FileAttribute
		return xdr.Read(reader, &attr)
	}
	return nil
}

func latencyListing(r latencyReply) (int, error) {
	return latencyListingWithCount(r, 103)
}

func latencyListingWithCount(r latencyReply, wantEntries int) (int, error) {
	reader, err := latencyOK(r)
	if err != nil {
		return 0, err
	}
	if err := latencyPostAttr(reader); err != nil {
		return 0, err
	}
	var verifier uint64
	if err := xdr.Read(reader, &verifier); err != nil {
		return 0, err
	}
	next, err := xdr.ReadUint32(reader)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]uint64)
	for next != 0 {
		var fileID, cookie uint64
		if err := xdr.Read(reader, &fileID); err != nil {
			return 0, err
		}
		name, err := latencyOpaque(reader)
		if err != nil {
			return 0, err
		}
		if err := xdr.Read(reader, &cookie); err != nil {
			return 0, err
		}
		if err := latencyPostAttr(reader); err != nil {
			return 0, err
		}
		hasHandle, err := xdr.ReadUint32(reader)
		if err != nil {
			return 0, err
		}
		if hasHandle != 0 {
			handle, err := latencyOpaque(reader)
			if err != nil || len(handle) == 0 {
				return 0, fmt.Errorf("listing handle: length %d, error %v", len(handle), err)
			}
		}
		if _, duplicate := seen[string(name)]; duplicate {
			return 0, fmt.Errorf("duplicate listing entry %q", name)
		}
		seen[string(name)] = fileID
		next, err = xdr.ReadUint32(reader)
		if err != nil {
			return 0, err
		}
	}
	eof, err := xdr.ReadUint32(reader)
	if err != nil || eof == 0 || len(seen) != wantEntries {
		return 0, fmt.Errorf("listing entries=%d eof=%d err=%v", len(seen), eof, err)
	}
	if reader.Len() != 0 {
		return 0, fmt.Errorf("listing trailing %d bytes", reader.Len())
	}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("item-%03d.bin", i)
		if seen[name] != uint64(1001+i) {
			return 0, fmt.Errorf("listing %s inode=%d want=%d", name, seen[name], 1001+i)
		}
	}
	if seen["slow.bin"] != 2001 {
		return 0, fmt.Errorf("listing slow.bin inode=%d", seen["slow.bin"])
	}
	if wantEntries == 104 && seen["catalog"] != 3001 {
		return 0, fmt.Errorf("listing catalog inode=%d", seen["catalog"])
	}
	if _, ok := seen["."]; !ok {
		return 0, fmt.Errorf("listing missing dot entry")
	}
	if _, ok := seen[".."]; !ok {
		return 0, fmt.Errorf("listing missing dotdot entry")
	}
	return 100, nil
}

func latencyCheckNav(kind string, r latencyReply, wantHandle []byte) (int, error) {
	reader, err := latencyOK(r)
	if err != nil {
		return 0, err
	}
	switch kind {
	case "getattr":
		var attr nfslib.FileAttribute
		if err := xdr.Read(reader, &attr); err != nil {
			return 0, err
		}
		if attr.Fileid != 1051 || attr.Filesize != 51 || reader.Len() != 0 {
			return 0, fmt.Errorf("GETATTR id=%d size=%d trailing=%d, want 1051/51/0", attr.Fileid, attr.Filesize, reader.Len())
		}
		return 0, nil
	case "lookup":
		handle, err := latencyOpaque(reader)
		if err != nil || !bytes.Equal(handle, wantHandle) {
			return 0, fmt.Errorf("LOOKUP handle=%x want=%x err=%v", handle, wantHandle, err)
		}
		if err := latencyPostAttr(reader); err != nil {
			return 0, fmt.Errorf("LOOKUP child attrs: %w", err)
		}
		if err := latencyPostAttr(reader); err != nil {
			return 0, fmt.Errorf("LOOKUP parent attrs: %w", err)
		}
		if reader.Len() != 0 {
			return 0, fmt.Errorf("LOOKUP trailing %d bytes", reader.Len())
		}
		return 0, nil
	case "readdirplus":
		return latencyListing(r)
	default:
		return 0, fmt.Errorf("unknown nav kind %q", kind)
	}
}

// A saturated READ may be refused with JUKEBOX, but it must become an exact
// successful read when the identical request is retried after gate release.
func latencyCheckRead(r latencyReply) (retry bool, err error) {
	if r.err != nil {
		return false, r.err
	}
	if len(r.body) < 4 {
		return false, fmt.Errorf("short READ body: %x", r.body)
	}
	if status := binary.BigEndian.Uint32(r.body[:4]); status == uint32(nfslib.NFSStatusJukebox) {
		reader := bytes.NewReader(r.body[4:])
		if err := latencyPostAttr(reader); err != nil {
			return false, fmt.Errorf("READ JUKEBOX post-op attrs: %w", err)
		}
		if reader.Len() != 0 {
			return false, fmt.Errorf("READ JUKEBOX trailing %d bytes", reader.Len())
		}
		return true, nil
	} else if status != uint32(nfslib.NFSStatusOk) {
		return false, fmt.Errorf("READ NFS status %d", status)
	}
	reader, err := latencyOK(r)
	if err != nil {
		return false, err
	}
	if err := latencyPostAttr(reader); err != nil {
		return false, fmt.Errorf("READ attributes: %w", err)
	}
	var count, eof uint32
	if err := xdr.Read(reader, &count); err != nil {
		return false, err
	}
	if err := xdr.Read(reader, &eof); err != nil {
		return false, err
	}
	data, err := latencyOpaque(reader)
	if err != nil || count != 4 || eof != 0 || string(data) != latencyPayload[:4] || reader.Len() != 0 {
		return false, fmt.Errorf("READ count=%d eof=%d data=%q trailing=%d err=%v", count, eof, data, reader.Len(), err)
	}
	return false, nil
}

type latencyReadArgs struct {
	Handle []byte
	Offset uint64
	Count  uint32
}

type latencyDirArgs struct {
	Handle      []byte
	Cookie      uint64
	CookieVerif uint64
	DirCount    uint32
	MaxCount    uint32
}

type latencyNavCall struct {
	kind string
	p    latencyPending
}

func latencySamples(samples []time.Duration) (time.Duration, time.Duration, time.Duration) {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2], samples[(99*len(samples)+99)/100-1], samples[len(samples)-1]
}

func TestNavigationLatency(t *testing.T) {
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() { pin.SetOffline(false); pin.ResetFUSEIdentityForTest() })
	for _, tc := range []struct {
		name string
		hold time.Duration
		held bool
	}{
		{"quiet-0ms", 0, false},
		{"backend-100ms", 100 * time.Millisecond, false},
		{"backend-300ms", 300 * time.Millisecond, false},
		{"backend-1000ms", time.Second, false},
		{"held-backend-stand-in", 300 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			latencyScenario(t, tc.hold, tc.held)
		})
	}
}

func latencyScenario(t *testing.T, hold time.Duration, held bool) {
	fuseRoot := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	var h *JuiceMountHandler
	t.Cleanup(func() {
		if h != nil {
			h.StopHandler()
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	entries := make([]*metadata.Entry, 0, 101)
	now := time.Now()
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("item-%03d.bin", i)
		if err := os.WriteFile(filepath.Join(fuseRoot, name), bytes.Repeat([]byte{'x'}, i+1), 0o644); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, metadata.MakeEntry(name, false, int64(i+1), now, uint64(1001+i)))
	}
	if err := os.WriteFile(filepath.Join(fuseRoot, "slow.bin"), []byte(latencyPayload), 0o644); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, metadata.MakeEntry("slow.bin", false, int64(len(latencyPayload)), now, 2001))
	if err := store.BulkInsert(entries, 101); err != nil {
		t.Fatal(err)
	}
	h = NewHandler(store, fuseRoot)
	var gate *latencyGate
	if hold > 0 {
		gate = newLatencyGate()
	}
	server := nfslib.NewManagedServer(&latencyHandler{JuiceMountHandler: h, gate: gate})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	var conn net.Conn
	t.Cleanup(func() {
		if gate != nil {
			gate.Release()
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
	})
	go func() { serveDone <- server.Serve(listener) }()
	conn, err = net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client := newLatencyClient(conn)

	mount, err := client.send(100005, 1, []byte(h.ExportPath()))
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
	childHandle := h.ToHandle(nil, []string{"item-050.bin"})
	slowHandle := h.ToHandle(nil, []string{"slow.bin"})
	if len(childHandle) == 0 || len(slowHandle) == 0 {
		t.Fatal("seeded handles missing")
	}

	var reads []latencyPending
	if gate != nil {
		for i := 0; i < nfslib.DefaultRPCSemaphoreSize+1; i++ {
			p, err := client.send(100003, uint32(nfslib.NFSProcedureRead), latencyReadArgs{Handle: slowHandle, Count: 4})
			if err != nil {
				t.Fatal(err)
			}
			reads = append(reads, p)
		}
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for i := 0; i < nfslib.DefaultRPCSemaphoreSize; i++ {
			select {
			case <-gate.entered:
			case <-deadline.C:
				gate.Release()
				t.Fatalf("only %d/%d slow READs entered backend", i, nfslib.DefaultRPCSemaphoreSize)
			}
		}
	}

	var calls []latencyNavCall
	for i := 0; i < 4; i++ {
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
				if gate != nil {
					gate.Release()
				}
				t.Fatal(err)
			}
			calls = append(calls, latencyNavCall{kind: spec.kind, p: p})
		}
	}
	holdStart := time.Now()
	if gate != nil {
		timer := time.AfterFunc(hold, gate.Release)
		defer timer.Stop()
	}
	deadline := time.Now().Add(2 * time.Second)
	samples := make([]time.Duration, 0, len(calls))
	beforeRelease := 0
	listingCount := 0
	var failures []string
	for _, call := range calls {
		r, err := call.p.await(deadline)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", call.kind, err))
			continue
		}
		if count, err := latencyCheckNav(call.kind, r, childHandle); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", call.kind, err))
		} else if count > 0 {
			listingCount = count
		}
		samples = append(samples, r.at.Sub(call.p.sent))
		if gate == nil || gate.releasedAt.Load() == 0 || r.at.UnixNano() < gate.releasedAt.Load() {
			beforeRelease++
		}
	}
	if gate != nil {
		if started := gate.started.Load(); started > int64(nfslib.DefaultRPCSemaphoreSize) {
			failures = append(failures, fmt.Sprintf("backend READ admission exceeded cap: %d", started))
		}
		if held {
			// A disconnected-backend stand-in releases as soon as navigation
			// has been assessed, with the timer as a bounded RED watchdog.
			gate.Release()
		} else {
			select {
			case <-gate.release:
			case <-time.After(hold + 100*time.Millisecond):
				failures = append(failures, "backend timer did not release gate")
				gate.Release()
			}
			if elapsed := time.Since(holdStart); elapsed < hold {
				failures = append(failures, fmt.Sprintf("backend hold %s shorter than requested %s", elapsed, hold))
			}
		}
		readDeadline := time.Now().Add(3 * time.Second)
		jukeboxes := 0
		for i, p := range reads {
			r, err := p.await(readDeadline)
			if err != nil {
				failures = append(failures, fmt.Sprintf("READ[%d]: %v", i, err))
				continue
			}
			retry, err := latencyCheckRead(r)
			if err != nil {
				failures = append(failures, fmt.Sprintf("READ[%d]: %v", i, err))
				continue
			}
			if retry {
				jukeboxes++
				if r.at.UnixNano() >= gate.releasedAt.Load() {
					failures = append(failures, "overload refusal arrived only after backend release")
				}
				again, err := client.send(100003, uint32(nfslib.NFSProcedureRead), latencyReadArgs{Handle: slowHandle, Count: 4})
				if err == nil {
					var reply latencyReply
					reply, err = again.await(readDeadline)
					if err == nil {
						var retryAgain bool
						retryAgain, err = latencyCheckRead(reply)
						if retryAgain {
							err = fmt.Errorf("READ retry still JUKEBOX")
						}
					}
				}
				if err != nil {
					failures = append(failures, fmt.Sprintf("READ[%d] retry: %v", i, err))
				}
			}
		}
		if jukeboxes != 1 {
			failures = append(failures, fmt.Sprintf("overload refusals=%d, want exactly 1 for capacity128/burst129", jukeboxes))
		}
		t.Logf("slow_read_jukeboxes=%d retried_after_release=%d", jukeboxes, jukeboxes)
	}
	if len(samples) == 0 {
		t.Fatalf("no navigation replies: %v", failures)
	}
	p50, p99, max := latencySamples(samples)
	t.Logf("mode=%s sample_count=%d p50=%s p99=%s max=%s actual_children=%d before_backend_release=%d/%d slow_reads=%d", os.Getenv("JM_SERVE_FROM_SQLITE"), len(samples), p50, p99, max, listingCount, beforeRelease, len(calls), len(reads))
	if len(failures) > 0 || len(samples) != len(calls) || listingCount != 100 || p99 > 100*time.Millisecond || max > 250*time.Millisecond || beforeRelease != len(calls) {
		t.Errorf("navigation responsiveness/content failure: failures=%v samples=%d/%d children=%d p99=%s max=%s pre-release=%d/%d", failures, len(samples), len(calls), listingCount, p99, max, beforeRelease, len(calls))
	}
}
