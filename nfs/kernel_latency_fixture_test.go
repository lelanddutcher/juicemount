//go:build darwin && latency_acceptance

package nfs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

type kernelLatencyGate struct {
	mu               sync.Mutex
	armed            bool
	released         bool
	watchdogReleased bool
	admissions       int
	releaseNS        int64
	done             chan struct{}
}

type kernelGateStatus struct {
	Armed            bool  `json:"armed"`
	Released         bool  `json:"released"`
	Admissions       int   `json:"admissions"`
	ReleaseNS        int64 `json:"release_ns"`
	WatchdogReleased bool  `json:"watchdog_released"`
}

func newKernelLatencyGate() *kernelLatencyGate {
	return &kernelLatencyGate{done: make(chan struct{})}
}

func (g *kernelLatencyGate) arm() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.armed || g.released {
		return errors.New("slow-read gate was already armed or released")
	}
	g.armed = true
	return nil
}

func (g *kernelLatencyGate) holdRead() {
	g.mu.Lock()
	if !g.armed {
		g.mu.Unlock()
		return
	}
	g.admissions++
	done := g.done
	g.mu.Unlock()
	<-done
}

func (g *kernelLatencyGate) release(watchdog bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return
	}
	g.released = true
	g.watchdogReleased = watchdog
	g.releaseNS = time.Now().UnixNano()
	close(g.done)
}

func (g *kernelLatencyGate) snapshot() kernelGateStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	return kernelGateStatus{g.armed, g.released, g.admissions, g.releaseNS, g.watchdogReleased}
}

type kernelLatencyFile struct {
	billy.File
	gate  *kernelLatencyGate
	name  string
	trace *kernelMediaTrace
}

func (f *kernelLatencyFile) BeginReadAdmission() (func() error, func(), error) {
	if provider, ok := f.File.(interface {
		BeginReadAdmission() (func() error, func(), error)
	}); ok {
		return provider.BeginReadAdmission()
	}
	return nil, nil, nil
}

func (f *kernelLatencyFile) ReadAt(p []byte, off int64) (int, error) {
	if f.gate != nil {
		f.gate.holdRead()
	}
	if f.trace == nil {
		return f.File.ReadAt(p, off)
	}
	started := time.Now()
	n, err := f.File.ReadAt(p, off)
	if f.trace != nil {
		f.trace.record("read", f.name, off, len(p), n, started, err)
	}
	return n, err
}

type kernelLatencyFS struct {
	*juiceFS // Preserve optional StatCacheOnly and read-admission interfaces.
	gate     *kernelLatencyGate
	trace    *kernelMediaTrace
}

func (f *kernelLatencyFS) Open(name string) (billy.File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *kernelLatencyFS) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	var started time.Time
	if f.trace != nil {
		started = time.Now()
	}
	file, err := f.juiceFS.OpenFile(name, flag, perm)
	if f.trace != nil {
		f.trace.record("open", name, 0, 0, 0, started, err)
	}
	if err != nil {
		return nil, err
	}
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		var gate *kernelLatencyGate
		if strings.TrimPrefix(filepath.Clean(name), string(os.PathSeparator)) == "slow.bin" {
			gate = f.gate
		}
		if gate != nil || f.trace != nil {
			return &kernelLatencyFile{File: file, gate: gate, name: name, trace: f.trace}, nil
		}
	}
	return file, nil
}

type kernelLatencyHandler struct {
	*JuiceMountHandler
	export string
	gate   *kernelLatencyGate
	mu     sync.Mutex
	peers  []string
	trace  *kernelMediaTrace
}

func (h *kernelLatencyHandler) wrap(fs billy.Filesystem) billy.Filesystem {
	if concrete, ok := fs.(*juiceFS); ok {
		return &kernelLatencyFS{juiceFS: concrete, gate: h.gate, trace: h.trace}
	}
	return fs
}

func (h *kernelLatencyHandler) Mount(ctx context.Context, conn net.Conn, req nfslib.MountRequest) (nfslib.MountStatus, billy.Filesystem, []nfslib.AuthFlavor) {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil || !net.ParseIP(host).IsLoopback() || string(req.Dirpath) != h.export {
		return nfslib.MountStatusErrAcces, nil, nil
	}
	status, fs, auth := h.JuiceMountHandler.Mount(ctx, conn, req)
	if status != nfslib.MountStatusOk || fs == nil {
		return status, nil, auth
	}
	h.mu.Lock()
	h.peers = append(h.peers, conn.RemoteAddr().String())
	h.mu.Unlock()
	return status, h.wrap(fs), auth
}

func (h *kernelLatencyHandler) FromHandle(handle []byte) (billy.Filesystem, []string, error) {
	fs, names, err := h.JuiceMountHandler.FromHandle(handle)
	return h.wrap(fs), names, err
}

func (h *kernelLatencyHandler) acceptedPeers() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.peers...)
}

type kernelMountTuple struct {
	FSID  [2]int32 `json:"fsid"`
	Type  string   `json:"type"`
	On    string   `json:"on"`
	From  string   `json:"from"`
	Owner uint32   `json:"owner"`
	Flags uint64   `json:"flags"`
}

func kernelCString(b []byte) string {
	if n := bytes.IndexByte(b, 0); n >= 0 {
		b = b[:n]
	}
	return string(b)
}

func kernelRelevantMounts(root, export string) ([]kernelMountTuple, error) {
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	capacity := count + 16
	if capacity < 32 {
		capacity = 32
	}
	for attempt := 0; attempt < 8 && capacity <= 8192; attempt++ {
		rows := make([]unix.Statfs_t, capacity)
		n, err := unix.Getfsstat(rows, unix.MNT_NOWAIT)
		if err != nil {
			return nil, err
		}
		if n >= len(rows) {
			capacity *= 2
			continue
		}
		mounts := make([]kernelMountTuple, 0)
		for _, st := range rows[:n] {
			on := kernelCString(st.Mntonname[:])
			from := kernelCString(st.Mntfromname[:])
			if on != root && !strings.HasPrefix(on, root+string(os.PathSeparator)) && !(export != "" && strings.Contains(from, export)) {
				continue
			}
			mounts = append(mounts, kernelMountTuple{
				FSID: st.Fsid.Val, Type: kernelCString(st.Fstypename[:]), On: on, From: from,
				Owner: st.Owner, Flags: uint64(st.Flags),
			})
		}
		return mounts, nil
	}
	return nil, errors.New("mount table continued growing beyond bounded snapshot capacity")
}

type kernelControlRequest struct {
	Nonce string `json:"nonce"`
	Op    string `json:"op"`
}

type kernelControlResponse struct {
	OK          bool                           `json:"ok"`
	Error       string                         `json:"error,omitempty"`
	PID         int                            `json:"pid"`
	Nonce       string                         `json:"nonce"`
	Addr        string                         `json:"addr"`
	Export      string                         `json:"export"`
	Gate        kernelGateStatus               `json:"gate"`
	RPC         map[string]uint64              `json:"rpc"`
	Mounts      []kernelMountTuple             `json:"mounts"`
	Connections int                            `json:"connections"`
	Peers       []string                       `json:"peers"`
	ServeError  string                         `json:"serve_error"`
	Stopped     bool                           `json:"stopped"`
	Transport   *nfslib.LatencyTransportStatus `json:"transport,omitempty"`
	Pressure    *kernelPressureStatus          `json:"pressure,omitempty"`
	MediaTrace  *kernelMediaTraceStatus        `json:"media_trace,omitempty"`
}

type kernelFixture struct {
	root      string
	nonce     string
	export    string
	addr      string
	gate      *kernelLatencyGate
	handler   *kernelLatencyHandler
	server    *nfslib.Server
	listener  net.Listener
	store     *metadata.Store
	serveMu   sync.Mutex
	serveErr  string
	closing   atomic.Bool
	stopped   atomic.Bool
	rpc       [22]atomic.Uint64
	heartbeat atomic.Int64
	transport *nfslib.LatencyTransport
	pressure  *kernelCachePressure
}

func (f *kernelFixture) response() kernelControlResponse {
	r := kernelControlResponse{
		OK: true, PID: os.Getpid(), Nonce: f.nonce, Addr: f.addr, Export: f.export,
		Gate: f.gate.snapshot(), RPC: make(map[string]uint64),
		Connections: int(f.server.ActiveConnections()), Peers: f.handler.acceptedPeers(), Stopped: f.stopped.Load(),
		Pressure: f.pressure.snapshot(),
	}
	for proc := range f.rpc {
		r.RPC[fmt.Sprint(proc)] = f.rpc[proc].Load()
	}
	if f.transport != nil {
		status := f.transport.Status()
		r.Transport = &status
	}
	if f.handler.trace != nil {
		status := f.handler.trace.status()
		r.MediaTrace = &status
	}
	f.serveMu.Lock()
	r.ServeError = f.serveErr
	f.serveMu.Unlock()
	mounts, err := kernelRelevantMounts(f.root, f.export)
	if err != nil {
		r.OK = false
		r.Error = fmt.Sprintf("mount snapshot: %v", err)
	} else {
		r.Mounts = mounts
	}
	if r.Mounts == nil {
		r.Mounts = []kernelMountTuple{}
	}
	return r
}

func (f *kernelFixture) stop() error {
	f.gate.release(false)
	mounts, err := kernelRelevantMounts(f.root, f.export)
	if err != nil {
		return fmt.Errorf("mount snapshot before stop: %w", err)
	}
	if len(mounts) != 0 {
		return fmt.Errorf("refusing stop while %d relevant mount(s) remain", len(mounts))
	}
	if err := f.pressure.join(5 * time.Second); err != nil {
		return err
	}
	f.closing.Store(true)
	if err := f.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("close NFS listener: %w", err)
	}
	if !f.server.Quiesce(3 * time.Second) {
		return errors.New("managed NFS server did not quiesce; serving dependencies retained")
	}
	if !f.handler.StopHandler() {
		return errors.New("NFS handler did not stop; metadata retained")
	}
	if err := f.store.Close(); err != nil {
		return fmt.Errorf("close metadata: %w", err)
	}
	f.stopped.Store(true)
	return nil
}

func (f *kernelFixture) handleControl(conn net.Conn) bool {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(io.LimitReader(conn, 4097)).ReadBytes('\n')
	if err != nil || len(line) > 4096 || bytes.Count(line, []byte{'\n'}) != 1 {
		_ = json.NewEncoder(conn).Encode(kernelControlResponse{OK: false, Error: "one newline-terminated JSON request of at most 4096 bytes required"})
		return false
	}
	var req kernelControlRequest
	if err := json.Unmarshal(bytes.TrimSpace(line), &req); err != nil || req.Nonce != f.nonce {
		_ = json.NewEncoder(conn).Encode(kernelControlResponse{OK: false, Error: "invalid JSON or nonce"})
		return false
	}
	if req.Op != "status" && req.Op != "arm" && req.Op != "release" && req.Op != "stop" && req.Op != "evict" {
		_ = json.NewEncoder(conn).Encode(kernelControlResponse{OK: false, Error: "unknown operation"})
		return false
	}
	// Stop may spend up to the handler's own bounded 30-second shutdown
	// interval. Keep the one-shot control reply writable for that attempt.
	writeDeadline := 3 * time.Second
	if req.Op == "stop" {
		writeDeadline = 40 * time.Second
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	f.heartbeat.Store(time.Now().UnixNano())
	var opErr error
	switch req.Op {
	case "arm":
		opErr = f.gate.arm()
	case "release":
		f.gate.release(false)
	case "evict":
		if f.closing.Load() || f.transport == nil || f.gate.snapshot().Admissions == 0 || f.gate.snapshot().Released {
			opErr = errors.New("eviction requires an active held backend read and transport trace")
		} else {
			opErr = f.pressure.start(f.store, func() int64 { return f.transport.Status().NowNS })
		}
	case "stop":
		opErr = f.stop()
	}
	r := f.response()
	if opErr != nil {
		r.OK = false
		r.Error = opErr.Error()
	}
	_ = json.NewEncoder(conn).Encode(r)
	return f.stopped.Load()
}

func kernelFixtureRoot(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.New("fixture root must be an absolute clean path")
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	if wd != root {
		return fmt.Errorf("fixture cwd=%q differs from root=%q", wd, root)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if resolved != root {
		return fmt.Errorf("fixture root resolves to %q, not the configured path", resolved)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("fixture root must be an existing real 0700 directory: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Getuid()) {
		return errors.New("fixture root must be owned by current uid")
	}
	return nil
}

func kernelFixtureSeed(root string, store *metadata.Store, phases ...string) error {
	if len(phases) == 0 {
		phases = []string{"quiet", "held"}
	}
	backing := filepath.Join(root, "backing")
	if err := os.Mkdir(backing, 0o700); err != nil {
		return err
	}
	writeFile := func(rel string, size int) error {
		file, err := os.OpenFile(filepath.Join(backing, filepath.FromSlash(rel)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(bytes.Repeat([]byte{'x'}, size))
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	stamp := time.Now()
	entries := make([]*metadata.Entry, 0, 1+413*len(phases))
	slow, err := os.OpenFile(filepath.Join(backing, "slow.bin"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := slow.Write([]byte(latencyPayload))
	closeErr := slow.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	entries = append(entries, metadata.MakeEntry("slow.bin", false, int64(len(latencyPayload)), stamp, 2001))
	nextInode := uint64(3000)
	for _, phase := range phases {
		if err := os.Mkdir(filepath.Join(backing, phase), 0o700); err != nil {
			return err
		}
		entries = append(entries, metadata.MakeEntry(phase, true, 0, stamp, nextInode))
		nextInode++
		for i := 0; i < 4; i++ {
			for _, spec := range []struct {
				prefix string
				size   int
			}{{"attr", 33 + i}, {"lookup", 65 + i}} {
				rel := fmt.Sprintf("%s/%s-%d.bin", phase, spec.prefix, i)
				if err := writeFile(rel, spec.size); err != nil {
					return err
				}
				entries = append(entries, metadata.MakeEntry(rel, false, int64(spec.size), stamp, nextInode))
				nextInode++
			}
			list := fmt.Sprintf("%s/list-%d", phase, i)
			if err := os.Mkdir(filepath.Join(backing, filepath.FromSlash(list)), 0o700); err != nil {
				return err
			}
			entries = append(entries, metadata.MakeEntry(list, true, 0, stamp, nextInode))
			nextInode++
			for j := 0; j < 100; j++ {
				rel := fmt.Sprintf("%s/item-%03d.bin", list, j)
				if err := writeFile(rel, j+1); err != nil {
					return err
				}
				entries = append(entries, metadata.MakeEntry(rel, false, int64(j+1), stamp, nextInode))
				nextInode++
			}
		}
	}
	return store.BulkInsert(entries, 500)
}

func TestOwnedKernelLatencyFixture(t *testing.T) {
	root, nonce := os.Getenv("JM_KERNEL_LATENCY_DIR"), os.Getenv("JM_KERNEL_LATENCY_NONCE")
	if root == "" || nonce == "" {
		t.Skip("owned kernel fixture requires run directory and nonce")
	}
	decoded, err := hex.DecodeString(nonce)
	if len(nonce) != 32 || len(decoded) != 16 || err != nil || nonce != strings.ToLower(nonce) {
		t.Fatal("nonce must be exactly 32 lowercase hexadecimal characters")
	}
	if err := kernelFixtureRoot(root); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	export := "" // no server identity exists before handler construction
	if mounts, err := kernelRelevantMounts(root, export); err != nil || len(mounts) != 0 {
		t.Fatalf("fixture root/export already has relevant mount or cannot be inspected: mounts=%+v err=%v", mounts, err)
	}
	for _, name := range []string{"backing", "meta.db", "meta.db-wal", "meta.db-shm", "control.sock", "transport-trace.json"} {
		if _, err := os.Lstat(filepath.Join(root, name)); err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fixture object %q already exists or cannot be inspected: %v", name, err)
		}
	}
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	store, err := metadata.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	phases := []string{"quiet", "held"}
	if os.Getenv("JM_KERNEL_LATENCY_PRESSURE") == "1" {
		// Never visited before eviction: the kernel cannot supply cached names
		// instead of exercising the complete durable directory fallback.
		phases = append(phases, "post")
	}
	seed := func() error { return kernelFixtureSeed(root, store, phases...) }
	if os.Getenv("JM_KERNEL_LATENCY_MEDIA") == "1" {
		if os.Getenv("JM_KERNEL_LATENCY_PRESSURE") == "1" {
			_ = store.Close()
			t.Fatal("media and pressure fixtures are mutually exclusive")
		}
		seed = func() error { return kernelSeedMedia(root, store) }
	}
	if err := seed(); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	handler := NewHandler(store, filepath.Join(root, "backing"))
	export = handler.ExportPath()
	if export == "" {
		_ = handler.StopHandler()
		_ = store.Close()
		t.Fatal("handler did not initialize an export")
	}
	gate := newKernelLatencyGate()
	wrapped := &kernelLatencyHandler{JuiceMountHandler: handler, export: export, gate: gate}
	if os.Getenv("JM_KERNEL_LATENCY_MEDIA") == "1" {
		wrapped.trace, err = newKernelMediaTrace(filepath.Join(root, "media-reads.jsonl"))
		if err != nil {
			_ = handler.StopHandler()
			_ = store.Close()
			t.Fatal(err)
		}
		defer func() {
			if err := wrapped.trace.close(); err != nil {
				t.Error(err)
			}
		}()
	}
	server := nfslib.NewManagedServer(wrapped)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = handler.StopHandler()
		_ = store.Close()
		t.Fatal(err)
	}
	control, err := net.Listen("unix", "control.sock")
	if err != nil {
		_ = listener.Close()
		_ = handler.StopHandler()
		_ = store.Close()
		t.Fatal(err)
	}
	if err := os.Chmod("control.sock", 0o600); err != nil {
		_ = control.Close()
		_ = listener.Close()
		_ = handler.StopHandler()
		_ = store.Close()
		t.Fatal(err)
	}
	fixture := &kernelFixture{root: root, nonce: nonce, export: export, addr: listener.Addr().String(),
		gate: gate, handler: wrapped, server: server, listener: listener, store: store}
	if os.Getenv("JM_KERNEL_LATENCY_PRESSURE") == "1" {
		fixture.pressure = prepareKernelCachePressure(store, metadata.DefaultMaxCacheSize+500, metadata.DefaultMaxCacheSize)
	}
	fixture.heartbeat.Store(time.Now().UnixNano())
	restoreObserver := nfslib.ObserveLatencyReplies(func(proc uint32) {
		fixture.rpc[proc].Add(1)
	})
	var restoreTransport func()
	if os.Getenv("JM_KERNEL_LATENCY_TRACE") == "1" {
		fixture.transport, err = nfslib.NewLatencyTransport(16384)
		if err != nil {
			t.Fatal(err)
		}
		listener = fixture.transport.WrapListener(listener)
		restoreTransport = fixture.transport.ObserveHandlers()
	}
	go func() {
		err := server.Serve(listener)
		if !fixture.closing.Load() {
			fixture.serveMu.Lock()
			fixture.serveErr = fmt.Sprint(err)
			fixture.serveMu.Unlock()
			gate.release(false)
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if fixture.stopped.Load() {
				return
			}
			if time.Since(time.Unix(0, fixture.heartbeat.Load())) > 10*time.Second {
				state := gate.snapshot()
				if state.Armed && !state.Released {
					gate.release(true)
				}
			}
		}
	}()
	for {
		conn, err := control.Accept()
		if err != nil {
			// Never tear down the server merely because the control listener
			// reported an error. The owner may still need it for recovery.
			t.Logf("control accept error; preserving serving process: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if fixture.handleControl(conn) {
			_ = control.Close()
			if fixture.transport != nil {
				restoreTransport()
				traceFile, err := os.OpenFile(filepath.Join(root, "transport-trace.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				err = errors.Join(fixture.transport.WriteJSON(traceFile), traceFile.Sync(), traceFile.Close())
				if err != nil {
					t.Fatal(err)
				}
			}
			restoreObserver()
			pin.SetOffline(false)
			pin.ResetFUSEIdentityForTest()
			return
		}
	}
}
