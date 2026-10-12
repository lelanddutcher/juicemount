//go:build consumer_acceptance && (darwin || linux)

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/testguard"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

const reconnectSelector = "^TestConsumerRetainedClientProviderReconnect$"
const reconnectSourceSHA = "63e23e0935533df54264a0ccb0cfdcddb7ce1116a52a0760abc1c4ff83572f3f"

// These are retained IPC receipts, never HTTP readiness probes.
type reconnectEvent struct {
	Phase      string  `json:"phase"`
	PID        int     `json:"pid"`
	Generation string  `json:"generation,omitempty"`
	Address    string  `json:"address,omitempty"`
	Client     string  `json:"client,omitempty"`
	Session    string  `json:"session,omitempty"`
	ElapsedMS  float64 `json:"elapsedMS,omitempty"`
	JPEG       string  `json:"jpeg,omitempty"`
}

func reconnectEmit(v reconnectEvent) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Printf("JM_RECONNECT %s\n", b)
}
func reconnectSave(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, e1 := f.Write(append(b, '\n'))
	e2 := f.Sync()
	e3 := f.Close()
	if e1 != nil || e2 != nil || e3 != nil {
		t.Fatalf("receipt write: %v %v %v", e1, e2, e3)
	}
}
func reconnectPrivate(t *testing.T, p string) {
	t.Helper()
	r, e := filepath.EvalSymlinks(p)
	if e != nil || r != p || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		t.Fatal("owned canonical local evidence directory required")
	}
	for q := p; ; q = filepath.Dir(q) {
		st, e := os.Lstat(q)
		if e != nil || !st.IsDir() {
			t.Fatal("missing evidence ancestor")
		}
		ns, ok := st.Sys().(*syscall.Stat_t)
		if !ok || (ns.Uid != 0 && ns.Uid != uint32(os.Geteuid())) || st.Mode().Perm()&0022 != 0 {
			t.Fatal("unsafe evidence ancestor")
		}
		if q == p && (ns.Uid != uint32(os.Geteuid()) || st.Mode().Perm() != 0700) {
			t.Fatal("evidence directory must be owned0700")
		}
		if q == filepath.Dir(q) {
			break
		}
	}
	var fs syscall.Statfs_t
	if e := syscall.Statfs(p, &fs); e != nil {
		t.Fatal(e)
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < 40805695488 {
		t.Fatal("40,805,695,488-byte primary reserve unavailable")
	}
}

type reconnectChild struct {
	cmd                 *exec.Cmd
	stdin               io.WriteCloser
	done                chan struct{}
	events              chan reconnectEvent
	waitErr, scanErr    error
	logs                []*os.File
	joined, inputClosed bool
	label               string
}

func reconnectStart(t *testing.T, root, label string, args, env []string, extra []*os.File) *reconnectChild {
	t.Helper()
	p := &reconnectChild{cmd: exec.Command(args[0], args[1:]...), done: make(chan struct{}), events: make(chan reconnectEvent, 12), label: label}
	p.cmd.Env = env
	p.cmd.ExtraFiles = extra
	for _, suffix := range []string{"stdout", "stderr"} {
		f, e := os.OpenFile(filepath.Join(root, label+"."+suffix), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		p.logs = append(p.logs, f)
	}
	var err error
	p.stdin, err = p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = p.logs[1]
	if err := p.cmd.Start(); err != nil {
		p.stdin.Close()
		for _, f := range p.logs {
			f.Close()
		}
		t.Fatal(err)
	}
	// Scanner owns the original stdout pipe. Wait starts only after EOF so it
	// cannot close a pipe before the final receipt has been retained.
	go func() {
		scan := bufio.NewScanner(io.TeeReader(out, p.logs[0]))
		scan.Buffer(make([]byte, 4096), 64<<10)
		for scan.Scan() {
			line := scan.Text()
			if strings.HasPrefix(line, "JM_RECONNECT ") {
				var v reconnectEvent
				if e := json.Unmarshal([]byte(strings.TrimPrefix(line, "JM_RECONNECT ")), &v); e != nil {
					p.scanErr = e
					continue
				}
				select {
				case p.events <- v:
				default:
					p.scanErr = errors.New("IPC receipt overflow")
				}
			}
		}
		if e := scan.Err(); e != nil {
			p.scanErr = e
			_, _ = io.Copy(p.logs[0], out)
		}
		p.waitErr = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		if p.joined {
			return
		}
		if !p.inputClosed {
			p.inputClosed = true
			_ = p.stdin.Close()
		}
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			t.Errorf("HOLD_UNJOINED %s pid=%d; retaining original Cmd/Wait/logs, no signal", label, p.cmd.Process.Pid)
			<-p.done
		}
		p.joined = true
		for _, f := range p.logs {
			_ = f.Close()
		}
		t.Logf("retained failed-case original join %s: %v", label, p.waitErr)
	})
	return p
}
func reconnectEventWait(t *testing.T, p *reconnectChild, phase string, bound time.Duration) reconnectEvent {
	t.Helper()
	select {
	case v := <-p.events:
		if v.Phase != phase || v.PID != p.cmd.Process.Pid {
			t.Fatalf("unexpected original-child receipt: %+v", v)
		}
		return v
	case <-p.done:
		// EOF may race an already buffered final message.
		select {
		case v := <-p.events:
			if v.Phase == phase && v.PID == p.cmd.Process.Pid {
				return v
			}
		default:
		}
		t.Fatalf("%s exited before %s: %v / %v", p.label, phase, p.waitErr, p.scanErr)
	case <-time.After(bound):
		t.Fatalf("phase %s exceeded bound; cleanup retains original child until ordinary EOF join", phase)
	}
	return reconnectEvent{}
}
func reconnectCommand(t *testing.T, p *reconnectChild, command string) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatal("command after original child exit")
	default:
	}
	if _, e := io.WriteString(p.stdin, command+"\n"); e != nil {
		t.Fatal(e)
	}
}
func reconnectJoin(t *testing.T, root string, p *reconnectChild) {
	t.Helper()
	if !p.inputClosed {
		p.inputClosed = true
		if e := p.stdin.Close(); e != nil {
			t.Fatal(e)
		}
	}
	late := false
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		late = true
		t.Errorf("HOLD_UNJOINED %s; retaining original wait, no signal/retry", p.label)
		<-p.done
	}
	p.joined = true
	for _, f := range p.logs {
		if e := f.Close(); e != nil {
			t.Error(e)
		}
	}
	out, e1 := consumerMediaSHA(filepath.Join(root, p.label+".stdout"))
	errhash, e2 := consumerMediaSHA(filepath.Join(root, p.label+".stderr"))
	reconnectSave(t, filepath.Join(root, p.label+"-join.json"), map[string]any{"pid": p.cmd.Process.Pid, "exit": p.cmd.ProcessState.ExitCode(), "error": fmt.Sprint(p.waitErr), "stdout_sha256": out, "stderr_sha256": errhash, "late": late})
	if late || p.waitErr != nil || p.scanErr != nil || e1 != nil || e2 != nil || t.Failed() {
		t.Fatalf("original child %s did not retire cleanly", p.label)
	}
}

type reconnectHTTP struct {
	Method, Path, Query, Hash, Kind, Size, Mtime, MediaType, Length, JPEG string
	Status                                                                int
	Bytes                                                                 int64
}
type reconnectWriter struct {
	http.ResponseWriter
	h      hash.Hash
	status int
	n      int64
}

func (w *reconnectWriter) WriteHeader(n int) { w.status = n; w.ResponseWriter.WriteHeader(n) }
func (w *reconnectWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, e := w.ResponseWriter.Write(b)
	_, _ = w.h.Write(b[:n])
	w.n += int64(n)
	return n, e
}

// The exact production handlers and persistent stores are served by a small
// owned HTTP host. This is not the app's full service lifecycle or cache owner.
func reconnectProvider(t *testing.T, generation string) {
	root := os.Getenv("JM_CONSUMER_RECONNECT_ROOT")
	reconnectPrivate(t, root)
	backing := filepath.Join(root, "backing")
	state := filepath.Join(root, "state")
	if generation == "1" {
		for _, p := range []string{state, filepath.Join(backing, "Project_Foo"), filepath.Join(state, "scratch")} {
			if e := os.MkdirAll(p, 0700); e != nil {
				t.Fatal(e)
			}
		}
	} else {
		if generation != "2" {
			t.Fatal("unknown provider generation")
		}
		if _, e := os.Lstat(backing); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("backing must remain absent")
		}
		if os.Getenv(testguard.RefuseFUSEContentReadsEnv) != "1" {
			t.Fatal("content guard missing")
		}
	}
	mirror, e := metadata.Open(filepath.Join(state, "metadata.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := mirror.Close(); e != nil {
			t.Error(e)
		}
	}()
	provider, e := derivatives.Open(derivStorePath(filepath.Join(state, "metadata.db")))
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := provider.Close(); e != nil {
			t.Error(e)
		}
	}()
	thumbs, e := thumbcache.Open(filepath.Join(state, "thumbs"), 64<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := thumbs.Close(); e != nil {
			t.Error(e)
		}
	}()
	var fixture consumerCacheFixture
	if generation == "1" {
		source := consumerMediaPinned(t, "JM_CONSUMER_MEDIA_SOURCE")
		if os.Getenv("JM_CONSUMER_MEDIA_SOURCE_SHA256") != reconnectSourceSHA {
			t.Fatal("unexpected retained source")
		}
		ffmpeg := consumerMediaPinned(t, "JM_CONSUMER_FFMPEG")
		ffprobe := consumerMediaPinned(t, "JM_CONSUMER_FFPROBE")
		path := filepath.Join(backing, "Project_Foo", "hot.mov")
		consumerMediaCopy(t, source, path)
		fi, e := os.Stat(path)
		if e != nil || fi.Size() != 223153 {
			t.Fatal("source size mismatch")
		}
		inode := uint64(fi.Sys().(*syscall.Stat_t).Ino)
		if e := mirror.Insert(&metadata.Entry{Path: "Project_Foo/hot.mov", Name: "hot.mov", ParentPath: "Project_Foo", Size: fi.Size(), Mtime: fi.ModTime(), MtimeNsecKnown: true, Inode: inode, Mode: fi.Mode()}); e != nil {
			t.Fatal(e)
		}
		fs, e := derivatives.Open(filepath.Join(state, "farm.db"))
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		res := farm.Process(fs, path, farm.Options{Context: ctx, Mount: backing, EncodeScratchDir: filepath.Join(state, "scratch"), Blobs: true, ThumbMaxDim: 320, FFmpegBin: ffmpeg, FFprobeBin: ffprobe, Producer: "linux-farm", Version: 1})
		cancel()
		if e := fs.Close(); e != nil {
			t.Fatal(e)
		}
		if res.Err != nil || res.BlobErr != nil || !res.ThumbWrote || res.Inode != inode {
			t.Fatalf("ordinary farm poster failed: %+v", res)
		}
		poster := filepath.Join(backing, derivatives.DerivBlobRel(inode, "poster.jpg"))
		pi, e := os.Stat(poster)
		if e != nil || pi.Size() <= 0 {
			t.Fatal("poster missing")
		}
		digest, e := consumerMediaSHA(poster)
		if e != nil {
			t.Fatal(e)
		}
		fixture.Hot = consumerCacheSlot{Inode: inode, SourceHash: res.Hash, SourceSize: fi.Size(), SourceMtime: fi.ModTime().Unix(), DisplayPath: "/Volumes/zpool/Project_Foo/hot.mov", JPEGSHA256: digest, JPEGSize: pi.Size()}
		consumerCacheSave(t, consumerCachePath(root), fixture)
	} else {
		fixture = consumerCacheRead(t, consumerCachePath(root))
		known, h := provider.Known(fixture.Hot.Inode)
		entry := mirror.LookupByInode(fixture.Hot.Inode)
		if !known || h == nil || *h != fixture.Hot.SourceHash || entry == nil || entry.Size != fixture.Hot.SourceSize || !thumbs.Has(fixture.Hot.Inode, "thumbnail") || thumbs.Stats().Puts != 0 {
			t.Fatal("persisted source/cache binding missing")
		}
	}
	// Each provider is a fresh test process; no live global is inherited.
	globalMu.Lock()
	globalStore, globalDerivStore, globalThumbCache = mirror, provider, thumbs
	globalFUSEPath, globalMountPath, globalWantMountPoint, globalVolumeName = backing, "/Volumes/zpool", "/Volumes/zpool", "zpool"
	globalCapabilities = cplane.DeriveCapabilities(productionGUIServedRoutes())
	globalMu.Unlock()
	var mu sync.Mutex
	var captures []reconnectHTTP
	mux := http.NewServeMux()
	for path, handler := range map[string]http.HandlerFunc{"/whoami": handleWhoamiHTTP, "/lookup": handleLookupHTTP, "/derivatives": handleDerivativesHTTP, "/blob": handleBlobHTTP} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			cw := &reconnectWriter{ResponseWriter: w, h: sha256.New()}
			handler(cw, r)
			h := w.Header()
			v := reconnectHTTP{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Status: cw.status, Hash: h.Get("X-JM-Hash"), Kind: h.Get("X-JM-Kind"), Size: h.Get("X-JM-Source-Size"), Mtime: h.Get("X-JM-Source-Mtime"), MediaType: h.Get("Content-Type"), Length: h.Get("Content-Length"), JPEG: hex.EncodeToString(cw.h.Sum(nil)), Bytes: cw.n}
			mu.Lock()
			captures = append(captures, v)
			mu.Unlock()
		})
	}
	var listener net.Listener
	if generation == "1" {
		listener, e = net.Listen("tcp4", "127.0.0.1:0")
	} else {
		f := os.NewFile(3, "owned-loopback-reservation")
		if f == nil {
			t.Fatal("reservation FD absent")
		}
		sa, err := syscall.Getsockname(int(f.Fd()))
		if err != nil {
			t.Fatal(err)
		}
		expected := os.Getenv("JM_CONSUMER_RECONNECT_ADDRESS")
		v, ok := sa.(*syscall.SockaddrInet4)
		if !ok || v.Addr != [4]byte{127, 0, 0, 1} || net.JoinHostPort("127.0.0.1", strconv.Itoa(v.Port)) != expected {
			t.Fatal("inherited socket endpoint mismatch")
		}
		for _, option := range []int{syscall.SO_REUSEADDR, unix.SO_REUSEPORT} {
			value, err := syscall.GetsockoptInt(int(f.Fd()), syscall.SOL_SOCKET, option)
			if err != nil || value != 0 {
				t.Fatal("inherited reservation is not exclusive")
			}
		}
		if err := syscall.Listen(int(f.Fd()), 128); err != nil {
			t.Fatal(err)
		}
		listener, e = net.FileListener(f)
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	globalMu.Lock()
	globalMetricsAddr = address
	globalMu.Unlock()
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()
	reconnectEmit(reconnectEvent{Phase: "ready", PID: os.Getpid(), Generation: generation, Address: address})
	// Provider has no command actions: the sole normal shutdown is stdin EOF.
	b, e := io.ReadAll(io.LimitReader(os.Stdin, 2))
	if e != nil || len(b) != 0 {
		t.Error("provider stdin expected EOF only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownErr := srv.Shutdown(ctx)
	cancel()
	if shutdownErr != nil {
		t.Errorf("HOLD_HTTP_SHUTDOWN: %v; retaining original server", shutdownErr)
		// No forced Close can turn unknown handler retirement into PASS.
		select {}
	}
	if e := <-serveDone; !errors.Is(e, http.ErrServerClosed) {
		t.Errorf("Serve join: %v", e)
	}
	mu.Lock()
	got := append([]reconnectHTTP(nil), captures...)
	mu.Unlock()
	reconnectSave(t, filepath.Join(root, "provider"+generation+"-http.json"), got)
	want := []string{"/whoami", "/lookup", "/derivatives", "/whoami", "/blob"}
	var paths []string
	for _, r := range got {
		paths = append(paths, r.Path)
		if r.Method != "GET" || r.Status != 200 {
			t.Errorf("unexpected captured request %+v", r)
		}
	}
	if !reflect.DeepEqual(paths, want) {
		t.Errorf("actual HTTP calls=%v want=%v", paths, want)
	}
	if len(got) == 5 {
		v := got[4]
		slot := fixture.Hot
		if v.Query != "inode="+strconv.FormatUint(slot.Inode, 10)+"&kind=thumbnail" && v.Query != "kind=thumbnail&inode="+strconv.FormatUint(slot.Inode, 10) {
			t.Error("blob query identity changed")
		}
		if v.Hash != slot.SourceHash || v.Kind != "thumbnail" || v.Size != strconv.FormatInt(slot.SourceSize, 10) || v.Mtime != strconv.FormatInt(slot.SourceMtime, 10) || v.MediaType != "image/jpeg" || v.Length != strconv.FormatInt(slot.JPEGSize, 10) || v.Bytes != slot.JPEGSize || v.JPEG != slot.JPEGSHA256 {
			t.Errorf("captured blob binding differs: %+v", v)
		}
	}
	stats := thumbs.Stats()
	if !thumbs.Has(fixture.Hot.Inode, "thumbnail") || (generation == "1" && stats.Puts != 1) || (generation == "2" && (stats.Puts != 0 || stats.Hits == 0)) {
		t.Errorf("cache generation %s stats=%+v", generation, stats)
	}
	reconnectSave(t, filepath.Join(root, "provider"+generation+"-cache.json"), stats)
}

// Non-listening bind reserves the exact formerly owned endpoint during outage.
// A conflicting bind is a failure; no alternate port or HTTP probe is attempted.
func reconnectReserve(t *testing.T, address string) *os.File {
	t.Helper()
	host, port, e := net.SplitHostPort(address)
	n, e2 := strconv.Atoi(port)
	if e != nil || e2 != nil || host != "127.0.0.1" || n < 1 || n > 65535 {
		t.Fatal("invalid owned endpoint")
	}
	fd, e := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if e != nil {
		t.Fatal(e)
	}
	syscall.CloseOnExec(fd)
	f := os.NewFile(uintptr(fd), "retained-loopback-reservation")
	if e := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e := syscall.Bind(fd, &syscall.SockaddrInet4{Port: n, Addr: [4]byte{127, 0, 0, 1}}); e != nil {
		f.Close()
		t.Fatalf("HOLD exact endpoint reservation failed: %v", e)
	}
	// Reuse is needed only to bind after the owned listener's TIME_WAIT.
	// Clear it before the unavailable request, then observe exclusivity with
	// one owned bind-only contender; neither socket listens during this gate.
	if e := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0); e != nil {
		f.Close()
		t.Fatal(e)
	}
	for _, option := range []int{syscall.SO_REUSEADDR, unix.SO_REUSEPORT} {
		value, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, option)
		if err != nil || value != 0 {
			f.Close()
			t.Fatal("reservation is not exclusive")
		}
	}
	contender, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	defer func() { // close this original FD exactly once, before return.
		if err := syscall.Close(contender); err != nil {
			f.Close()
			t.Fatalf("owned contender close failed: %v", err)
		}
	}()
	syscall.CloseOnExec(contender)
	if err := syscall.SetsockoptInt(contender, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := syscall.Bind(contender, &syscall.SockaddrInet4{Port: n, Addr: [4]byte{127, 0, 0, 1}}); !errors.Is(err, syscall.EADDRINUSE) {
		f.Close()
		t.Fatalf("HOLD owned reusable contender was not refused with EADDRINUSE: %v", err)
	}
	sa, e := syscall.Getsockname(fd)
	v, ok := sa.(*syscall.SockaddrInet4)
	if e != nil || !ok || v.Port != n || v.Addr != [4]byte{127, 0, 0, 1} {
		f.Close()
		t.Fatal("reservation identity mismatch")
	}
	return f
}

func TestConsumerRetainedClientProviderReconnect(t *testing.T) {
	if generation := os.Getenv("JM_CONSUMER_RECONNECT_GENERATION"); generation != "" {
		reconnectProvider(t, generation)
		if !t.Failed() {
			reconnectEmit(reconnectEvent{Phase: "retired", PID: os.Getpid(), Generation: generation})
		}
		return
	}
	if os.Getenv("JM_CONSUMER_RECONNECT_TEST") != "1" {
		t.Skip("requires isolated retained-client acceptance")
	}
	if os.Getenv("JM_CONSUMER_RECONNECT_LOCAL_FS") != "1" {
		t.Fatal("ordinary local fixture admission required")
	}
	duration, e := time.ParseDuration(flag.Lookup("test.timeout").Value.String())
	if e != nil || duration != 0 {
		t.Fatal("retained owner requires -timeout=0")
	}
	base := os.Getenv("JM_CONSUMER_RECONNECT_EVIDENCE_DIR")
	reconnectPrivate(t, base)
	root := filepath.Join(base, "reconnect")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal("exclusive attempt directory: ", e)
	}
	probe := consumerMediaPinned(t, "JM_CONSUMER_PROBE")
	for _, name := range []string{"JM_CONSUMER_MEDIA_SOURCE", "JM_CONSUMER_FFMPEG", "JM_CONSUMER_FFPROBE"} {
		_ = consumerMediaPinned(t, name)
	}
	if os.Getenv("JM_CONSUMER_MEDIA_SOURCE_SHA256") != reconnectSourceSHA {
		t.Fatal("retained source hash mismatch")
	}
	self, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	startProvider := func(generation, address string, extra []*os.File) *reconnectChild {
		env := append(os.Environ(), "JM_CONSUMER_RECONNECT_GENERATION="+generation, "JM_CONSUMER_RECONNECT_ROOT="+root, "JM_CONSUMER_RECONNECT_ADDRESS="+address, testguard.RefuseFUSEContentReadsEnv+"="+map[string]string{"1": "0", "2": "1"}[generation])
		return reconnectStart(t, root, "provider"+generation, []string{self, "-test.run=" + reconnectSelector, "-test.timeout=0", "-test.v"}, env, extra)
	}
	first := startProvider("1", "", nil)
	ready := reconnectEventWait(t, first, "ready", 40*time.Second)
	if ready.Generation != "1" {
		t.Fatal("first generation identity mismatch")
	}
	host, _, e := net.SplitHostPort(ready.Address)
	if e != nil || host != "127.0.0.1" {
		t.Fatal("provider reported nonloopback endpoint")
	}
	fixture := consumerCacheRead(t, consumerCachePath(root))
	swift := reconnectStart(t, root, "swift", []string{probe, "http://" + ready.Address, "thumbnail-reconnect"}, append(os.Environ(), "JM_CONSUMER_MEDIA_FIXTURE="+consumerCachePath(root), "JM_CONSUMER_CACHE_SLOT=hot"), nil)
	identity := reconnectEventWait(t, swift, "ready", 8*time.Second)
	if identity.Client == "" || identity.Session == "" || identity.Address != "http://"+ready.Address {
		t.Fatal("persistent client identity missing")
	}
	var phases []reconnectEvent
	phase := func(name string, bound time.Duration) reconnectEvent {
		reconnectCommand(t, swift, name)
		v := reconnectEventWait(t, swift, name, bound)
		if v.Client != identity.Client || v.Session != identity.Session || v.Address != identity.Address {
			t.Fatal("client/session/endpoint replaced")
		}
		if name != "unavailable" && v.JPEG != fixture.Hot.JPEGSHA256 {
			t.Fatal("Swift JPEG identity changed")
		}
		phases = append(phases, v)
		reconnectSave(t, filepath.Join(root, "swift-"+name+".json"), v)
		return v
	}
	phase("initial", 10*time.Second)
	reconnectJoin(t, root, first)
	retired := reconnectEventWait(t, first, "retired", time.Second)
	if retired.Generation != "1" {
		t.Fatal("original provider retirement mismatch")
	}
	reservation := reconnectReserve(t, ready.Address)
	defer reservation.Close()
	reconnectSave(t, filepath.Join(root, "reservation.json"), map[string]any{"address": ready.Address, "reuseaddr": 0, "reuseport": 0, "owned_reusable_contender_bind": "EADDRINUSE", "contender_closed": true, "listened": false})
	if e := os.Rename(filepath.Join(root, "backing"), filepath.Join(root, "backing-unavailable")); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Lstat(filepath.Join(root, "backing")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("backing still accessible")
	}
	unavailable := phase("unavailable", 8*time.Second)
	if unavailable.ElapsedMS < 0 || unavailable.ElapsedMS > 6500 || unavailable.JPEG != "" {
		t.Fatal("unavailable call was not bounded/nil")
	}
	second := startProvider("2", ready.Address, []*os.File{reservation})
	ready2 := reconnectEventWait(t, second, "ready", 10*time.Second)
	if ready2.Generation != "2" || ready2.Address != ready.Address || second.cmd.Process.Pid == first.cmd.Process.Pid {
		t.Fatal("fresh provider endpoint/identity mismatch")
	}
	if e := reservation.Close(); e != nil {
		t.Fatal(e)
	} // child has confirmed inherited endpoint.
	phase("reconnected", 10*time.Second)
	reconnectJoin(t, root, swift)
	finished := reconnectEventWait(t, swift, "finished", time.Second)
	if finished.Client != identity.Client || finished.Session != identity.Session {
		t.Fatal("Swift retirement identity mismatch")
	}
	reconnectJoin(t, root, second)
	retired2 := reconnectEventWait(t, second, "retired", time.Second)
	if retired2.Generation != "2" {
		t.Fatal("fresh provider retirement mismatch")
	}
	if _, e := os.Lstat(filepath.Join(root, "backing")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("backing resurrected")
	}
	for _, name := range []string{"JM_CONSUMER_PROBE", "JM_CONSUMER_MEDIA_SOURCE", "JM_CONSUMER_FFMPEG", "JM_CONSUMER_FFPROBE"} {
		_ = consumerMediaPinned(t, name)
	}
	reconnectSave(t, filepath.Join(root, "result.json"), map[string]any{"state": "PASS_RETAINED_CLIENT_PROVIDER_RECONNECT_BOUNDED", "provider_before": first.cmd.Process.Pid, "provider_after": second.cmd.Process.Pid, "swift_pid": swift.cmd.Process.Pid, "endpoint": ready.Address, "client": identity.Client, "session": identity.Session, "phases": phases, "source_sha256": reconnectSourceSHA, "fixture": fixture.Hot, "all_original_children_joined": true, "limits": []string{"production handlers and persistent thumbnail store only; no full app/cache-owner requalification", "unavailable client.blob call may fail in its whoami prerequisite", "no application retry, UI, real Link, physical handoff, FUSE or NFS acceptance"}})
}
