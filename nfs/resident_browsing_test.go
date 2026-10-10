//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// heldRedisProxy owns each upstream and downstream connection. Once armed it
// forwards requests but retains server response bytes until explicit release.
// This models an unanswered metadata service without sleeping in the product.
type heldRedisProxy struct {
	listener       net.Listener
	upstream       string
	wg             sync.WaitGroup
	mu             sync.Mutex
	closing        bool
	conns          map[net.Conn]struct{}
	gate           atomic.Pointer[redisResponseGate]
	controlPeer    atomic.Pointer[string]
	controlForward atomic.Uint64
	controlHeld    atomic.Uint64
	heldSignal     chan struct{}
	forwardSignal  chan struct{}
	closeOnce      sync.Once
}

type redisResponseGate struct {
	release chan struct{}
	once    sync.Once
}

func newHeldRedisProxy(t *testing.T, raw string) (*heldRedisProxy, string) {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	upstream := u.Host
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &heldRedisProxy{listener: listener, upstream: upstream, conns: make(map[net.Conn]struct{}), heldSignal: make(chan struct{}, 1), forwardSignal: make(chan struct{}, 1)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closing {
				p.mu.Unlock()
				_ = client.Close()
				return
			}
			p.wg.Add(1)
			p.mu.Unlock()
			go func() { defer p.wg.Done(); p.serve(client) }()
		}
	}()
	t.Cleanup(func() { p.close(t) })
	u.Host = listener.Addr().String()
	return p, u.String()
}

func (p *heldRedisProxy) serve(client net.Conn) {
	defer client.Close()
	upstream, err := net.DialTimeout("tcp", p.upstream, 2*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.conns[client], p.conns[upstream] = struct{}{}, struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, client); delete(p.conns, upstream); p.mu.Unlock() }()
	peer := client.RemoteAddr().String()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		defer client.Close()
		defer upstream.Close()
		buf := make([]byte, 32<<10)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				written, writeErr := writeAllProxy(upstream, buf[:n])
				if gate := p.gate.Load(); gate != nil && p.isControl(peer) {
					p.controlForward.Add(uint64(written))
					if written > 0 {
						select {
						case p.forwardSignal <- struct{}{}:
						default:
						}
					}
				}
				if writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := upstream.Read(buf)
		if n > 0 {
			if gate := p.gate.Load(); gate != nil {
				if p.isControl(peer) {
					p.controlHeld.Add(uint64(n))
					select {
					case p.heldSignal <- struct{}{}:
					default:
					}
				}
				<-gate.release
			}
			if _, writeErr := writeAllProxy(client, buf[:n]); writeErr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = client.Close()
	_ = upstream.Close()
	<-requestDone
}

func writeAllProxy(conn net.Conn, buf []byte) (int, error) {
	written := 0
	for written < len(buf) {
		n, err := conn.Write(buf[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (p *heldRedisProxy) isControl(peer string) bool {
	control := p.controlPeer.Load()
	return control != nil && *control == peer
}

func (p *heldRedisProxy) arm() {
	p.gate.Store(&redisResponseGate{release: make(chan struct{})})
}

func (p *heldRedisProxy) release() {
	if gate := p.gate.Load(); gate != nil {
		gate.once.Do(func() { close(gate.release) })
	}
}

func (p *heldRedisProxy) close(t *testing.T) {
	t.Helper()
	p.closeOnce.Do(func() {
		p.release()
		p.mu.Lock()
		p.closing = true
		_ = p.listener.Close()
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		joined := make(chan struct{})
		go func() { p.wg.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("Redis proxy accept/copy workers did not join")
		}
	})
}

func heldControlState(t *testing.T, p *heldRedisProxy, pingDone <-chan struct{}) {
	t.Helper()
	gate := p.gate.Load()
	if gate == nil || p.controlForward.Load() == 0 || p.controlHeld.Load() == 0 {
		t.Fatal("control PING was not forwarded and held")
	}
	select {
	case <-gate.release:
		t.Fatal("Redis response gate released early")
	default:
	}
	select {
	case <-pingDone:
		t.Fatal("control PING completed before release")
	default:
	}
}

func browseGetattr(t *testing.T, client *latencyClient, handle []byte, inode, size uint64, kind nfslib.FileType) {
	t.Helper()
	pending, err := client.send(100003, uint32(nfslib.NFSProcedureGetAttr), handle)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := latencyOK(reply)
	if err != nil {
		t.Fatal(err)
	}
	var attr nfslib.FileAttribute
	if err := xdr.Read(reader, &attr); err != nil {
		t.Fatal(err)
	}
	if reader.Len() != 0 || attr.Fileid != inode || attr.Filesize != size || attr.Type != kind {
		t.Fatalf("GETATTR inode=%d size=%d type=%d trailing=%d", attr.Fileid, attr.Filesize, attr.Type, reader.Len())
	}
}

func browseLookup(t *testing.T, client *latencyClient, parent, child []byte, name string, inode, size uint64, kind nfslib.FileType) {
	t.Helper()
	pending, err := client.send(100003, uint32(nfslib.NFSProcedureLookup), nfslib.DirOpArg{Handle: parent, Filename: []byte(name)})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := latencyOK(reply)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := latencyOpaque(reader)
	if err != nil || !bytes.Equal(handle, child) {
		t.Fatalf("LOOKUP handle mismatch: %v", err)
	}
	present, err := xdr.ReadUint32(reader)
	if err != nil || present != 1 {
		t.Fatalf("LOOKUP child attrs absent: %v", err)
	}
	var attr nfslib.FileAttribute
	if err := xdr.Read(reader, &attr); err != nil {
		t.Fatal(err)
	}
	if attr.Fileid != inode || attr.Filesize != size || attr.Type != kind {
		t.Fatalf("LOOKUP child attrs %+v", attr)
	}
	if err := latencyPostAttr(reader); err != nil || reader.Len() != 0 {
		t.Fatalf("LOOKUP parent attrs/trailing: %v %d", err, reader.Len())
	}
}

func browseListing(t *testing.T, client *latencyClient, parent, child []byte, directory uint64, name string, entry *metadata.Entry, kind nfslib.FileType) {
	t.Helper()
	pending, err := client.send(100003, uint32(nfslib.NFSProcedureReadDirPlus), latencyDirArgs{Handle: parent, DirCount: 65536, MaxCount: 65536})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := pending.await(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPressureListing(reply, directory, map[string]*metadata.Entry{name: entry}, map[string][]byte{name: child}); err != nil {
		t.Fatal(err)
	}
	// A separate explicit child GETATTR checks type, which the shared listing
	// decoder's identity/size/handle checks do not inspect.
	browseGetattr(t, client, child, entry.Inode, uint64(entry.Size), kind)
}

func assertNoForegroundFUSE(t *testing.T, before map[string]uint64) {
	t.Helper()
	for sourceOp, row := range fuseCellCounts(t) {
		if strings.HasPrefix(sourceOp, "foreground/") && row.Calls != before[sourceOp] {
			t.Fatalf("foreground FUSE %s changed %d -> %d", sourceOp, before[sourceOp], row.Calls)
		}
	}
}

func TestResidentBrowsingCompletesWhileRedisResponseHeld(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory buffer")
	}
	raw := redistestfixture.Start(t)
	proxy, proxied := newHeldRedisProxy(t, raw)
	x := newBoundCacheFixtureWithRedis(t, proxied)
	client := localSidecarLoopback(t, x.jfs.handler)
	// LIFO: release before NFS quiesce, fixture Stop, proxy close, Redis kill.
	t.Cleanup(proxy.release)
	h := x.jfs.handler
	root := h.ToHandle(nil, nil)
	parent := h.ToHandle(nil, []string{"media"})
	child := h.ToHandle(nil, []string{"media", "clip.mov"})
	parentEntry := x.store.LookupByPath("media")
	entry := x.store.LookupByPath("media/clip.mov")
	if parentEntry == nil || entry == nil {
		t.Fatal("fixture parent or child absent")
	}
	// Warm the exact metadata, path eligibility, slice mapping and SSD block.
	browseGetattr(t, client, root, 1, 0, nfslib.FileTypeDirectory)
	browseLookup(t, client, root, parent, "media", 2, 0, nfslib.FileTypeDirectory)
	browseListing(t, client, root, parent, 1, "media", parentEntry, nfslib.FileTypeDirectory)
	browseGetattr(t, client, parent, 2, 0, nfslib.FileTypeDirectory)
	browseLookup(t, client, parent, child, "clip.mov", 3, uint64(len(x.want)), nfslib.FileTypeRegular)
	browseListing(t, client, parent, child, 2, "clip.mov", entry, nfslib.FileTypeRegular)
	residentWireRead(t, client, h, 0, 4096, x.want[:4096], 0)
	controlOpts, err := redisurl.Parse(proxied)
	if err != nil {
		t.Fatal(err)
	}
	controlOpts.ContextTimeoutEnabled = true
	controlOpts.MaxRetries = -1
	controlOpts.ReadTimeout = 20 * time.Second
	controlOpts.WriteTimeout = 3 * time.Second
	controlOpts.DialTimeout = 2 * time.Second
	controlOpts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, addr)
		if err == nil {
			peer := conn.LocalAddr().String()
			proxy.controlPeer.Store(&peer)
		}
		return conn, err
	}
	controlClient := redis.NewClient(controlOpts)
	defer controlClient.Close()
	control := controlClient.Conn()
	defer control.Close()
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := control.Ping(warmCtx).Err(); err != nil {
		warmCancel()
		t.Fatal(err)
	}
	warmCancel()
	controlCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy.arm()
	pingDone := make(chan struct{})
	var pingErr error
	go func() { pingErr = control.Ping(controlCtx).Err(); close(pingDone) }()
	joined := false
	defer func() {
		proxy.release()
		if !joined {
			select {
			case <-pingDone:
			case <-time.After(20 * time.Second):
				t.Error("control PING did not join")
			}
		}
	}()
	witnessDeadline := time.NewTimer(5 * time.Second)
	defer witnessDeadline.Stop()
	for _, signal := range []<-chan struct{}{proxy.forwardSignal, proxy.heldSignal} {
		select {
		case <-signal:
		case <-witnessDeadline.C:
			t.Fatal("control Redis request-forward/response-hold witnesses incomplete")
		}
	}
	heldControlState(t, proxy, pingDone)
	before := map[string]uint64{}
	for key, row := range fuseCellCounts(t) {
		if strings.HasPrefix(key, "foreground/") {
			before[key] = row.Calls
		}
	}
	sequenceDeadline := time.Now().Add(10 * time.Second)
	if err := client.conn.SetDeadline(sequenceDeadline); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		if time.Until(sequenceDeadline) < 3*time.Second {
			t.Fatal("aggregate NFS sequence deadline exhausted")
		}
		heldControlState(t, proxy, pingDone)
		assertNoForegroundFUSE(t, before)
	}
	check()
	browseGetattr(t, client, root, 1, 0, nfslib.FileTypeDirectory)
	check()
	browseLookup(t, client, root, parent, "media", 2, 0, nfslib.FileTypeDirectory)
	check()
	browseListing(t, client, root, parent, 1, "media", parentEntry, nfslib.FileTypeDirectory)
	check()
	browseGetattr(t, client, parent, 2, 0, nfslib.FileTypeDirectory)
	check()
	browseLookup(t, client, parent, child, "clip.mov", 3, uint64(len(x.want)), nfslib.FileTypeRegular)
	check()
	browseListing(t, client, parent, child, 2, "clip.mov", entry, nfslib.FileTypeRegular)
	check()
	cacheBefore := metrics.Default().Snapshot().DirectSSDCacheHit
	residentWireRead(t, client, h, 61440, 65536, x.want[61440:], 1)
	check()
	if hits := metrics.Default().Snapshot().DirectSSDCacheHit - cacheBefore; hits != 1 {
		t.Fatalf("held tail SSD hits=%d want 1", hits)
	}
	browseListing(t, client, root, parent, 1, "media", parentEntry, nfslib.FileTypeDirectory)
	check()
	proxy.release()
	select {
	case <-pingDone:
		if pingErr != nil {
			t.Fatalf("control PONG after release: %v", pingErr)
		}
		joined = true
	case <-time.After(5 * time.Second):
		t.Fatal("control PING did not complete after release")
	}
	if proxy.controlForward.Load() == 0 || proxy.controlHeld.Load() == 0 {
		t.Fatal("no causal control traffic")
	}
	t.Logf("held Redis control: forwarded=%d response_held=%d; root-to-media GETATTR LOOKUP READDIRPLUS, cached tail READ and root listing repeat completed before release; SSD hits=1; foreground FUSE calls unchanged; PONG joined after release",
		proxy.controlForward.Load(), proxy.controlHeld.Load())
}
