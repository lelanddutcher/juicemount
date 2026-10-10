//go:build darwin && video_nav_acceptance && latency_acceptance

package main

// This opt-in fixture owns a new read-only JuiceFS child, private local stores,
// loopback NFS and the production thumbnail endpoint. It never starts the app
// or opens its metadata/spool database for writing.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

type videoNavConfig struct {
	Root, Nonce, RedisURL, Bucket, Folder, JuiceFS, JuiceFSSHA256 string
	SeedFile, SeedSHA256                                          string
}
type videoNavMount struct {
	FSID           [2]int32 `json:"fsid"`
	On, From, Type string
	Flags          uint32
	Owner          uint32
}

func videoNavMounts(root string) ([]videoNavMount, error) {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	rows := make([]unix.Statfs_t, n+32)
	n, err = unix.Getfsstat(rows, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	if n >= len(rows) {
		return nil, errors.New("mount inventory grew")
	}
	str := func(b []byte) string { return string(bytes.TrimRight(b, "\x00")) }
	var out []videoNavMount
	for _, s := range rows[:n] {
		on := str(s.Mntonname[:])
		if on == root || strings.HasPrefix(on, root+"/") {
			out = append(out, videoNavMount{s.Fsid.Val, on, str(s.Mntfromname[:]), str(s.Fstypename[:]), s.Flags, s.Owner})
		}
	}
	return out, nil
}

type videoNavStatus struct {
	Nonce                                     string `json:"nonce"`
	PID, BackendPID                           int
	Phase, Error, Addr, Export, Mount, Folder string
	MountOptions, DirectCachePath             string
	Backend                                   *videoNavMount
	Mounts                                    []videoNavMount
	RPC                                       map[string]uint64
	Metrics                                   metrics.Snapshot
	Profile                                   netprofile.Snapshot
	Thumbs                                    []videoNavThumb
}
type videoNavThumb struct {
	Name  string
	Code  int
	Label string
	MS    float64
	Bytes int
}
type videoNavWriter struct {
	http.ResponseWriter
	code, bytes int
}

func (w *videoNavWriter) WriteHeader(code int) { w.code = code; w.ResponseWriter.WriteHeader(code) }
func (w *videoNavWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(200)
	}
	n, e := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, e
}

func TestOwnedVideoNavigationFixture(t *testing.T) {
	configPath := os.Getenv("JM_VIDEO_NAV_CONFIG")
	if configPath == "" {
		t.Skip("owned video fixture requires a private config")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg videoNavConfig
	if err = json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Lstat(cfg.Root)
	if err != nil || wd != cfg.Root || !filepath.IsAbs(wd) || filepath.Clean(wd) != wd || st.Mode().Perm() != 0700 || !st.IsDir() {
		t.Fatal("private 0700 fixture cwd required")
	}
	if owner, ok := st.Sys().(*syscall.Stat_t); !ok || owner.Uid != uint32(os.Getuid()) {
		t.Fatal("fixture owner differs")
	}
	nonce, err := hex.DecodeString(cfg.Nonce)
	if err != nil || len(nonce) != 16 {
		t.Fatal("128-bit nonce required")
	}
	if cfg.Folder == "" || path.Clean(cfg.Folder) != cfg.Folder || strings.HasPrefix(cfg.Folder, "/") || strings.HasPrefix(cfg.Folder, "../") {
		t.Fatal("canonical backend folder required")
	}
	binary, err := os.ReadFile(cfg.JuiceFS)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binary)
	if hex.EncodeToString(sum[:]) != cfg.JuiceFSSHA256 {
		t.Fatal("JuiceFS pin changed")
	}
	if mounts, err := videoNavMounts(cfg.Root); err != nil || len(mounts) > 0 {
		t.Fatal("fixture already mounted", err)
	}
	if entries, err := os.ReadDir(wd); err != nil || len(entries) != 0 {
		t.Fatal("fixture root must be empty", err)
	}
	for _, name := range []string{"backend", "view", "cache", "home", "tmp"} {
		if err := os.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// Reserve the appex's production loopback port before creating a backend.
	httpListener, err := net.Listen("tcp", "127.0.0.1:11050")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { httpListener.Close() })
	control, err := net.Listen("unix", "control.sock")
	if err != nil {
		httpListener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	if err := os.Chmod("control.sock", 0600); err != nil {
		t.Fatal(err)
	}
	state := videoNavStatus{Nonce: cfg.Nonce, PID: os.Getpid(), Phase: "backend-starting", Mount: filepath.Join(wd, "view"), Folder: cfg.Folder}
	var stateMu sync.Mutex
	var rpc [22]atomic.Uint64
	save := func() {
		stateMu.Lock()
		defer stateMu.Unlock()
		b, _ := json.MarshalIndent(state, "", "  ")
		_ = os.WriteFile("state.json.pending", b, 0600)
		_ = os.Rename("state.json.pending", "state.json")
	}
	backendPath := filepath.Join(wd, "backend")
	// Keep Finder away from the raw backend. Browsing Computer otherwise
	// opens a root directory handle on this internal volume, contaminating the
	// NFS-only measurement and preventing ordinary fixture retirement.
	args := []string{"mount", cfg.RedisURL, backendPath, "--read-only", "--no-bgjob", "--backup-meta", "0", "--no-usage-report", "--no-syslog", "--cache-dir", filepath.Join(wd, "cache"), "--cache-size", "256", "--buffer-size", "16", "--max-readahead", "0", "--prefetch", "0", "--metrics", "127.0.0.1:0", "-o", "ro,rdonly,nobrowse"}
	if cfg.Bucket != "" {
		args = append(args, "--bucket", cfg.Bucket)
	}
	cmd := exec.Command(cfg.JuiceFS, args...)
	// Direct service stage: one exact foreground child; no JuiceFS supervisor.
	for _, key := range []string{"USER", "LOGNAME", "PATH", "LANG", "TZ"} {
		if v, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+filepath.Join(wd, "home"), "TMPDIR="+filepath.Join(wd, "tmp"), fmt.Sprintf("JFS_SUPERVISOR=%d", os.Getpid()))
	backendLog, err := os.OpenFile("backend.log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { backendLog.Close() })
	cmd.Stdout = backendLog
	cmd.Stderr = backendLog
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	state.BackendPID = cmd.Process.Pid
	backendDone := make(chan struct{})
	var backendErr error
	go func() { backendErr = cmd.Wait(); close(backendDone) }()
	// Register custody before any fallible setup. Every initialization failure
	// invokes this same idempotent teardown as nonce stop.
	var server *jmnfs.Server
	var httpServer *http.Server
	var warmer *jmnfs.ThumbWarmer
	var rc *metadata.RedisClient
	var store *metadata.Store
	var ds *derivatives.Store
	var cacheReader *cache.Reader
	var restore func()
	closed := false
	stopFailure := ""
	cleanup := func() bool {
		if closed {
			return true
		}
		fail := func(message string) bool {
			state.Phase = "cleanup-incomplete"
			state.Error = message
			save()
			return false
		}
		if stopFailure != "" {
			return fail(stopFailure)
		}
		mounts, err := videoNavMounts(wd)
		if err != nil {
			return fail(err.Error())
		}
		if len(mounts) > 0 && (state.Backend == nil || len(mounts) != 1 || mounts[0] != *state.Backend) {
			return fail("NFS/foreign/unrecorded/changed mount remains; exact child retained")
		}
		if server != nil {
			server.DisableRestart()
			if err := server.Stop(); err != nil {
				stopFailure = "NFS listener retirement unproved: " + err.Error()
				return fail(stopFailure)
			}
			if !server.QuiesceClients(5*time.Second) || !server.Handler().StopHandler() {
				return fail("NFS work remains; dependencies and child retained")
			}
			server = nil
		}
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := httpServer.Shutdown(ctx)
			cancel()
			if err != nil {
				return fail("HTTP work remains: " + err.Error())
			}
			httpServer = nil
		}
		if warmer != nil {
			warmer.Stop()
			warmer = nil
		}
		if cacheReader != nil {
			cacheReader.Stop()
			cacheReader = nil
		}
		if rc != nil {
			rc.Stop()
			rc = nil
		}
		if restore != nil {
			restore()
			restore = nil
		}
		nfslib.SetObserver(nil)
		if store != nil {
			if err := store.Close(); err != nil {
				return fail(err.Error())
			}
			store = nil
		}
		if ds != nil {
			_ = ds.Close()
			ds = nil
		}
		mounts, err = videoNavMounts(wd)
		if err != nil {
			return fail(err.Error())
		}
		if len(mounts) > 0 {
			if state.Backend == nil || len(mounts) != 1 || mounts[0] != *state.Backend {
				return fail("backend identity changed; exact child retained")
			}
			if out, err := exec.Command("/sbin/umount", backendPath).CombinedOutput(); err != nil {
				return fail(fmt.Sprintf("owned unmount: %v %s", err, out))
			}
		}
		// No mounts remain: the exact exec.Cmd child can be terminated safely.
		mounts, err = videoNavMounts(wd)
		if err != nil || len(mounts) != 0 {
			return fail("mount retirement unproved; child retained")
		}
		select {
		case <-backendDone:
		case <-time.After(2 * time.Second):
			// A late mount can race the empty snapshot. SIGKILL avoids the
			// child's pathname-based signal cleanup; verify mounts after reap.
			_ = cmd.Process.Kill()
			select {
			case <-backendDone:
			case <-time.After(2 * time.Second):
				return fail("exact child not reaped")
			}
		}
		mounts, err = videoNavMounts(wd)
		if err != nil || len(mounts) != 0 {
			return fail("child reaped but mount retirement unproved")
		}
		closed = true
		state.Phase = "stopped"
		state.Error = ""
		save()
		return true
	}
	t.Cleanup(func() {
		if !cleanup() {
			t.Errorf("owned teardown incomplete; inspect private state.json")
		}
	})
	save()
	deadline := time.Now().Add(25 * time.Second)
	for state.Backend == nil && time.Now().Before(deadline) {
		mounts, err := videoNavMounts(wd)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range mounts {
			if m.On == backendPath {
				copy := m
				state.Backend = &copy
				save()
				if m.Flags&unix.MNT_RDONLY == 0 {
					t.Fatal("backend not kernel read-only")
				}
				if m.Flags&unix.MNT_DONTBROWSE == 0 {
					t.Fatal("backend unexpectedly browsable by Finder")
				}
			}
		}
		if state.Backend != nil {
			break
		}
		select {
		case <-backendDone:
			state.Phase = "backend-exited"
			state.Error = fmt.Sprint(backendErr)
			save()
			t.Fatal("owned backend exited; inspect private backend.log")
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	if state.Backend == nil {
		state.Phase = "backend-readiness-unknown"
		save()
		t.Fatal("backend readiness unknown; registered teardown will check custody")
	}
	state.Phase = "seeding"
	save()
	store, err = videoNavOpenPrivateStore(filepath.Join(wd, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	// Mirror actual attributes before timing a browse, just as an already-synced
	// app does. No source-media bytes are read by preparation.
	var entries []*metadata.Entry
	if cfg.SeedFile != "" {
		seedInfo, err := os.Lstat(cfg.SeedFile)
		if err != nil || !seedInfo.Mode().IsRegular() || seedInfo.Mode().Perm() != 0600 {
			t.Fatal("seed must be a private regular snapshot")
		}
		if owner, ok := seedInfo.Sys().(*syscall.Stat_t); !ok || owner.Uid != uint32(os.Getuid()) {
			t.Fatal("seed owner differs")
		}
		seed, err := os.ReadFile(cfg.SeedFile)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(seed)
		if hex.EncodeToString(h[:]) != cfg.SeedSHA256 {
			t.Fatal("seed snapshot pin changed")
		}
		if err := json.Unmarshal(seed, &entries); err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e == nil || path.Clean(e.Path) != e.Path || strings.HasPrefix(e.Path, "/") || strings.HasPrefix(e.Path, "../") || e.Path == ".." {
				t.Fatal("invalid seed path")
			}
			if !(e.Path == cfg.Folder || strings.HasPrefix(cfg.Folder, e.Path+"/") || path.Dir(e.Path) == cfg.Folder) {
				t.Fatal("seed outside selected folder and ancestors")
			}
		}
	} else {
		add := func(rel string, fi os.FileInfo) {
			st := fi.Sys().(*syscall.Stat_t)
			e := metadata.MakeEntry(rel, fi.IsDir(), fi.Size(), fi.ModTime(), st.Ino)
			e.MtimeNsecKnown = true
			e.Mode = fi.Mode()
			entries = append(entries, e)
		}
		var ancestors []string
		for p := cfg.Folder; p != "."; p = path.Dir(p) {
			ancestors = append(ancestors, p)
		}
		for i := len(ancestors) - 1; i >= 0; i-- {
			fi, err := os.Lstat(filepath.Join(backendPath, ancestors[i]))
			if err != nil {
				t.Fatal(err)
			}
			add(ancestors[i], fi)
		}
		children, err := os.ReadDir(filepath.Join(backendPath, cfg.Folder))
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range children {
			if child.Type()&os.ModeSymlink != 0 {
				continue
			}
			fi, err := child.Info()
			if err != nil {
				t.Fatal(err)
			}
			add(path.Join(cfg.Folder, child.Name()), fi)
		}
	}
	if err := store.BulkInsert(entries, 500); err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.MarshalIndent(entries, "", "  ")
	if err := os.WriteFile("entries.json", manifest, 0600); err != nil {
		t.Fatal(err)
	}
	pin.SetOffline(false)
	pin.SetFUSEIdentityPath(backendPath)
	rc, err = metadata.NewRedisClient(cfg.RedisURL, store)
	if err != nil {
		t.Fatal(err)
	}
	ds, err = derivatives.Open(filepath.Join(wd, "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	tc, err := thumbcache.Open(filepath.Join(wd, "thumbs"), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	globalStore = store
	globalRC = rc
	globalDerivStore = ds
	globalThumbCache = tc
	globalFUSEPath = backendPath
	globalWantMountPoint = state.Mount
	globalMu.Unlock()
	server = jmnfs.NewServer(jmnfs.Config{ListenAddr: "127.0.0.1:0", FUSEPath: backendPath}, store)
	server.Handler().SetRedisClient(rc)
	// Use the production direct-cache reader against this child's private
	// cache. Omitting it changes READ's open path and can manufacture remote
	// FUSE metadata waits even when the requested metadata body is cached.
	chunks, err := videoNavColdCacheRoot(filepath.Join(wd, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	addr, db, err := metadata.ParseRedisURL(cfg.RedisURL)
	if err != nil {
		t.Fatal(err)
	}
	cacheReader = cache.NewReader(chunks, cache.DefaultBlockSize, redis.NewClient(cacheRedisOptions(addr, db)))
	if err := cacheReader.SetMappingSnapshotPath(filepath.Join(wd, "cache-mappings.gob")); err != nil {
		t.Fatal(err)
	}
	server.Handler().SetCacheReader(cacheReader)
	state.DirectCachePath = chunks
	warmer = jmnfs.NewThumbWarmer(tc, resolveWarmThumbBlobPath, func(dir string) []jmnfs.ThumbChildRef {
		es, err := store.ListChildren(dir)
		if err != nil {
			return nil
		}
		var refs []jmnfs.ThumbChildRef
		for _, e := range es {
			refs = append(refs, jmnfs.ThumbChildRef{Inode: e.Inode, Name: e.Name, IsDir: e.IsDir})
		}
		return refs
	})
	globalMu.Lock()
	globalThumbWarmer = warmer
	globalMu.Unlock()
	server.Handler().SetThumbWarmer(warmer)
	// Actual transport samples feed the same production classification policy.
	parsed, err := url.Parse(cfg.RedisURL)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", parsed.Host, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
		netprofile.Default().ObserveRTT(time.Since(start))
	}
	restore = nfslib.ObserveLatencyReplies(func(proc uint32) {
		if int(proc) < len(rpc) {
			rpc[proc].Add(1)
		}
	})
	nfslib.SetObserver(metrics.ObserveRPC)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	state.Addr = server.Addr()
	state.Export = server.ExportPath()
	_, port, err := net.SplitHostPort(state.Addr)
	if err != nil {
		t.Fatal(err)
	}
	// Consumers must mount with these options, generated by the same policy
	// as the app. Only read-only and hidden-volume safety restrictions differ.
	state.MountOptions = nfsMountOpts(port) + ",ro,nobrowse"
	mux := http.NewServeMux()
	mux.HandleFunc("/thumb-local", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &videoNavWriter{ResponseWriter: w}
		handleThumbLocalHTTP(wrapped, r)
		stateMu.Lock()
		state.Thumbs = append(state.Thumbs, videoNavThumb{Name: path.Base(r.URL.Query().Get("path")), Code: wrapped.code, Label: w.Header().Get("X-JM-Thumb"), MS: float64(time.Since(start)) / float64(time.Millisecond), Bytes: wrapped.bytes})
		stateMu.Unlock()
	})
	httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go httpServer.Serve(httpListener)
	state.Phase = "ready"
	save()
	t.Logf("owned video fixture ready pid=%d children=%d; waiting for nonce control", os.Getpid(), len(entries))
	for {
		conn, err := control.Accept()
		if err != nil {
			t.Log(err)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
		line, err := bufio.NewReader(io.LimitReader(conn, 4096)).ReadBytes('\n')
		var req struct{ Nonce, Op string }
		if err != nil || json.Unmarshal(line, &req) != nil || req.Nonce != cfg.Nonce {
			conn.Close()
			continue
		}
		stopped := false
		switch req.Op {
		case "status":
		case "stop":
			stopped = cleanup()
		default:
			state.Error = "unknown control operation"
		}
		mounts, err := videoNavMounts(wd)
		if err != nil {
			state.Error = err.Error()
		}
		stateMu.Lock()
		state.Mounts = mounts
		state.RPC = map[string]uint64{}
		for i := range rpc {
			state.RPC[fmt.Sprint(i)] = rpc[i].Load()
		}
		state.Metrics = metrics.Default().Snapshot()
		state.Profile = netprofile.Default().Snapshot()
		_ = json.NewEncoder(conn).Encode(state)
		stateMu.Unlock()
		conn.Close()
		save()
		if stopped {
			control.Close()
			backendLog.Close()
			return
		}
	}
}

// Construct the full private database before any live-route acceptance. NFS
// startup checks the recovery tables even when this fixture owns no spool rows.
func videoNavOpenPrivateStore(dbPath string) (*metadata.Store, error) {
	store, err := metadata.Open(dbPath)
	if err != nil {
		return nil, err
	}
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// JuiceFS creates raw/chunks lazily, and can leave only the volume directory
// when disk pressure suppresses caching. Provision empty local directories so
// the diagnostic exercises the production reader without warming any bytes.
func videoNavColdCacheRoot(root string) (string, error) {
	volumes, err := filepath.Glob(filepath.Join(root, "*"))
	if err != nil || len(volumes) != 1 {
		return "", fmt.Errorf("exactly one owned JuiceFS cache volume required: %v", err)
	}
	volume := volumes[0]
	resolved, err := filepath.EvalSymlinks(volume)
	if err != nil || resolved != volume {
		return "", errors.New("private cache volume must not traverse symlinks")
	}
	for _, p := range []string{volume, filepath.Join(volume, "raw"), filepath.Join(volume, "raw", "chunks")} {
		fi, err := os.Lstat(p)
		if os.IsNotExist(err) && p != volume {
			if err := os.Mkdir(p, 0700); err != nil {
				return "", err
			}
			fi, err = os.Lstat(p)
		}
		if err != nil {
			return "", err
		}
		owner, ok := fi.Sys().(*syscall.Stat_t)
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 || !ok || owner.Uid != uint32(os.Getuid()) {
			return "", errors.New("private cache component must be an owned directory")
		}
	}
	return filepath.Join(volume, "raw", "chunks"), nil
}

func TestVideoNavigationColdCacheRoot(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	volume := filepath.Join(root, "volume")
	if err := os.Mkdir(volume, 0700); err != nil {
		t.Fatal(err)
	}
	chunks, err := videoNavColdCacheRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(chunks); err != nil || len(entries) != 0 {
		t.Fatalf("cold cache must stay empty: %v %v", entries, err)
	}
	if err := os.Remove(chunks); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), chunks); err != nil {
		t.Fatal(err)
	}
	if _, err := videoNavColdCacheRoot(root); err == nil {
		t.Fatal("accepted a foreign symlink in the private cache")
	}
}

func TestVideoNavigationPrivateStoreStartup(t *testing.T) {
	root := t.TempDir()
	store, err := videoNavOpenPrivateStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, table := range []string{"spool_entries", "spool_bindings", "pending_symlinks"} {
		var count int
		if err := store.DB().QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("private %s not empty/ready: count=%d err=%v", table, count, err)
		}
	}
	server := jmnfs.NewServer(jmnfs.Config{ListenAddr: "127.0.0.1:0", FUSEPath: filepath.Join(root, "unused-backend")}, store)
	defer func() {
		server.DisableRestart()
		if err := server.Stop(); err != nil {
			t.Error(err)
		}
		if !server.QuiesceClients(5*time.Second) || !server.Handler().StopHandler() {
			t.Error("private startup fixture did not quiesce")
		}
	}()
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if server.Addr() == "" || server.ExportPath() == "" {
		t.Fatal("private server did not start")
	}
}
