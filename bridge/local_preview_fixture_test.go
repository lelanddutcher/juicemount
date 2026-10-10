//go:build darwin && video_nav_acceptance && latency_acceptance

package main

// The P47 fixture serves one private local movie through the production NFS
// handler and one prepared poster through the production thumbnail endpoint.
// The host owns the read-only mount and Finder visit; this process never mounts.
import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/thumbcache"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
	"golang.org/x/sys/unix"
)

type localPreviewConfig struct {
	Root, Nonce, SourcePath, SourceSHA256, PosterPath, PosterSHA256 string
	SidecarPath, SidecarSHA256                                      string
}

type localPreviewRequest struct {
	ID          uint64  `json:"id"`
	Event       string  `json:"event"`
	At          string  `json:"at"`
	Name        string  `json:"name"`
	RequestType string  `json:"request_type"`
	Status      int     `json:"status,omitempty"`
	Label       string  `json:"label,omitempty"`
	Bytes       int     `json:"bytes,omitempty"`
	ElapsedMS   float64 `json:"elapsed_ms,omitempty"`
	BodySHA256  string  `json:"body_sha256,omitempty"`
	Canceled    bool    `json:"canceled,omitempty"`
	Error       string  `json:"error,omitempty"`
}

type localPreviewStatus struct {
	Nonce, Phase, Error, Addr, Export, Mount, Folder, File, MountOptions             string
	SourceSHA256, PosterSHA256                                                       string
	SidecarPresent                                                                   bool
	SidecarPath, SidecarSHA256                                                       string
	PID                                                                              int
	SourceSize, PosterSize                                                           int64
	SourceInode                                                                      uint64
	SidecarSize                                                                      int64
	SidecarInode                                                                     uint64
	Mounts                                                                           []videoNavMount
	Requests                                                                         []localPreviewRequest
	HTTPEntries, HTTPCompleted, ProviderCandidateEntries, ProviderCandidateCompleted uint64
	ReadAttempts, ReadOK, ReadErrors                                                 uint64
	ReadElapsedNS                                                                    uint64
	DependenciesRetainedUntilExit                                                    bool
	RPC                                                                              map[string]uint64
	Metrics                                                                          metrics.Snapshot
}

// The wrapper hashes only bounded response bodies. Bytes and status are
// measured on actual writes, including a short write or a client disconnect.
type localPreviewWriter struct {
	http.ResponseWriter
	code, bytes int
	hash        hash.Hash
	oversized   bool
	writeErr    error
}

func (w *localPreviewWriter) WriteHeader(code int) {
	if w.code != 0 {
		return
	}
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *localPreviewWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	if w.bytes <= 2<<20 {
		_, _ = w.hash.Write(p[:n])
	} else {
		w.oversized = true
	}
	return n, err
}

func localPreviewPinnedFile(name, expected string, max int64) ([]byte, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return nil, errors.New("media path must be absolute and canonical")
	}
	if len(expected) != 64 || strings.ToLower(expected) != expected {
		return nil, errors.New("media SHA-256 must be lowercase hex")
	}
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	owner, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) || !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > max {
		return nil, errors.New("media must be an owned, bounded regular file")
	}
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != fi.Size() {
		return nil, errors.New("media size changed during read")
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != expected {
		return nil, errors.New("media SHA-256 changed")
	}
	return b, nil
}

// Accept the documented AppleDouble envelope and the two entries a custom
// icon needs. The resource fork is opaque here: the host supplies and pins
// canonical bytes produced by macOS, rather than this fixture encoding them.
func localPreviewValidateAppleDouble(b []byte) error {
	const headerSize = 26
	const descriptorSize = 12
	if len(b) < headerSize || binary.BigEndian.Uint32(b[:4]) != 0x00051607 ||
		binary.BigEndian.Uint32(b[4:8]) != 0x00020000 {
		return errors.New("AppleDouble magic or version invalid")
	}
	count := int(binary.BigEndian.Uint16(b[24:26]))
	tableEnd := headerSize + count*descriptorSize
	if count == 0 || tableEnd > len(b) {
		return errors.New("AppleDouble descriptor table invalid")
	}
	type region struct{ start, end uint64 }
	var regions []region
	seen := make(map[uint32]bool, count)
	finderInfo, resourceFork := 0, 0
	for i := 0; i < count; i++ {
		p := headerSize + i*descriptorSize
		id := binary.BigEndian.Uint32(b[p : p+4])
		off := uint64(binary.BigEndian.Uint32(b[p+4 : p+8]))
		size := uint64(binary.BigEndian.Uint32(b[p+8 : p+12]))
		if seen[id] {
			return fmt.Errorf("duplicate AppleDouble entry %d", id)
		}
		seen[id] = true
		if off > uint64(len(b)) || size > uint64(len(b))-off || (size > 0 && off < uint64(tableEnd)) {
			return fmt.Errorf("AppleDouble entry %d outside body", id)
		}
		if size > 0 {
			for _, previous := range regions {
				if off < previous.end && previous.start < off+size {
					return fmt.Errorf("AppleDouble entry %d overlaps another entry", id)
				}
			}
			regions = append(regions, region{off, off + size})
		}
		switch id {
		case 9: // FinderInfo; finderFlags are big-endian bytes 8..9.
			finderInfo++
			if size < 32 || binary.BigEndian.Uint16(b[off+8:off+10])&0x0400 == 0 {
				return errors.New("AppleDouble FinderInfo has no custom-icon flag")
			}
		case 2: // Resource fork containing the icon family, kept opaque.
			resourceFork++
			if size == 0 {
				return errors.New("AppleDouble resource fork is empty")
			}
		}
	}
	if finderInfo != 1 || resourceFork != 1 {
		return errors.New("AppleDouble needs one FinderInfo and one resource fork")
	}
	return nil
}

func TestLocalPreviewAppleDoubleEnvelope(t *testing.T) {
	// Opaque one-byte fork exercises only envelope rejection. It is not an
	// icon resource encoder or a substitute for the host's canonical donor.
	valid := make([]byte, 26+2*12+32+1)
	binary.BigEndian.PutUint32(valid[0:4], 0x00051607)
	binary.BigEndian.PutUint32(valid[4:8], 0x00020000)
	binary.BigEndian.PutUint16(valid[24:26], 2)
	binary.BigEndian.PutUint32(valid[26:30], 9)
	binary.BigEndian.PutUint32(valid[30:34], 50)
	binary.BigEndian.PutUint32(valid[34:38], 32)
	binary.BigEndian.PutUint32(valid[38:42], 2)
	binary.BigEndian.PutUint32(valid[42:46], 82)
	binary.BigEndian.PutUint32(valid[46:50], 1)
	binary.BigEndian.PutUint16(valid[58:60], 0x0400)
	valid[82] = 1
	if err := localPreviewValidateAppleDouble(valid); err != nil {
		t.Fatalf("bounded envelope rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"truncated table", func(b []byte) []byte { return b[:49] }},
		{"bad magic", func(b []byte) []byte { b[0] = 1; return b }},
		{"bad version", func(b []byte) []byte { b[7] = 1; return b }},
		{"header overlap", func(b []byte) []byte { binary.BigEndian.PutUint32(b[30:34], 49); return b }},
		{"payload overlap", func(b []byte) []byte { binary.BigEndian.PutUint32(b[42:46], 81); return b }},
		{"duplicate entry", func(b []byte) []byte { binary.BigEndian.PutUint32(b[38:42], 9); return b }},
		{"short FinderInfo", func(b []byte) []byte { binary.BigEndian.PutUint32(b[34:38], 31); return b }},
		{"missing icon bit", func(b []byte) []byte { b[58] = 0; return b }},
		{"empty resource fork", func(b []byte) []byte { binary.BigEndian.PutUint32(b[46:50], 0); return b }},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := test.change(append([]byte(nil), valid...))
			if err := localPreviewValidateAppleDouble(b); err == nil {
				t.Fatal("accepted malformed AppleDouble envelope")
			}
		})
	}
}

func TestOwnedLocalPreviewFixture(t *testing.T) {
	configPath := os.Getenv("JM_LOCAL_PREVIEW_CONFIG")
	if configPath == "" {
		t.Skip("owned local preview fixture requires private config")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg localPreviewConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(cfg.Root)
	if err != nil || cfg.Root != wd || resolved != wd || !filepath.IsAbs(wd) || filepath.Clean(wd) != wd {
		t.Fatal("absolute canonical fixture cwd required", err)
	}
	rootInfo, err := os.Lstat(wd)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode().Perm() != 0700 {
		t.Fatal("private 0700 fixture root required", err)
	}
	owner, ok := rootInfo.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		t.Fatal("fixture root owner differs")
	}
	nonceBytes, err := hex.DecodeString(cfg.Nonce)
	if err != nil || len(nonceBytes) != 16 || hex.EncodeToString(nonceBytes) != cfg.Nonce {
		t.Fatal("lowercase 128-bit nonce required")
	}
	if mounts, err := videoNavMounts(wd); err != nil || len(mounts) != 0 {
		t.Fatal("fixture root already has mounts", err)
	}
	if entries, err := os.ReadDir(wd); err != nil || len(entries) != 0 {
		t.Fatal("fixture root must start empty", err)
	}
	source, err := localPreviewPinnedFile(cfg.SourcePath, cfg.SourceSHA256, 64<<20)
	if err != nil {
		t.Fatal("source: ", err)
	}
	poster, err := localPreviewPinnedFile(cfg.PosterPath, cfg.PosterSHA256, 2<<20)
	if err != nil {
		t.Fatal("poster: ", err)
	}
	if len(source) < 12 || string(source[4:8]) != "ftyp" || len(poster) < 4 || !bytes.Equal(poster[:3], []byte{0xff, 0xd8, 0xff}) {
		t.Fatal("expected MP4 source and JPEG poster")
	}
	if (cfg.SidecarPath == "") != (cfg.SidecarSHA256 == "") {
		t.Fatal("sidecar path and SHA-256 must both be supplied or both absent")
	}
	var sidecar []byte
	if cfg.SidecarPath != "" {
		sidecar, err = localPreviewPinnedFile(cfg.SidecarPath, cfg.SidecarSHA256, 4<<20)
		if err != nil {
			t.Fatal("sidecar: ", err)
		}
		if err := localPreviewValidateAppleDouble(sidecar); err != nil {
			t.Fatal("sidecar: ", err)
		}
	}

	// Bind the appex's exact loopback port before creating any private state.
	httpListener, err := net.Listen("tcp", "127.0.0.1:11050")
	if err != nil {
		t.Fatal("thumbnail port occupied: ", err)
	}
	defer httpListener.Close()
	for _, dir := range []string{"media", "view", "thumbcache"} {
		if err := os.Mkdir(filepath.Join(wd, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	control, err := net.Listen("unix", filepath.Join(wd, "control.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := os.Chmod(filepath.Join(wd, "control.sock"), 0600); err != nil {
		t.Fatal(err)
	}

	folder := "preview-" + cfg.Nonce
	file := "video-" + cfg.Nonce + ".mp4"
	media := filepath.Join(wd, "media")
	mediaFolder := filepath.Join(media, folder)
	if err := os.Mkdir(mediaFolder, 0700); err != nil {
		t.Fatal(err)
	}
	mediaFile := filepath.Join(mediaFolder, file)
	if err := os.WriteFile(mediaFile, source, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := localPreviewPinnedFile(mediaFile, cfg.SourceSHA256, 64<<20); err != nil {
		t.Fatal("copied source: ", err)
	}
	fi, err := os.Lstat(mediaFile)
	if err != nil {
		t.Fatal(err)
	}
	fileStat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || fileStat.Ino == 0 || fi.Size() != int64(len(source)) {
		t.Fatal("copied source identity invalid")
	}
	var sidecarInfo os.FileInfo
	var sidecarStat *syscall.Stat_t
	sidecarRel := ""
	if sidecar != nil {
		sidecarRel = path.Join(folder, "._"+file)
		copiedSidecar := filepath.Join(media, filepath.FromSlash(sidecarRel))
		if err := os.WriteFile(copiedSidecar, sidecar, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := localPreviewPinnedFile(copiedSidecar, cfg.SidecarSHA256, 4<<20); err != nil {
			t.Fatal("copied sidecar: ", err)
		}
		sidecarInfo, err = os.Lstat(copiedSidecar)
		if err != nil {
			t.Fatal(err)
		}
		var statOK bool
		sidecarStat, statOK = sidecarInfo.Sys().(*syscall.Stat_t)
		if !statOK || sidecarStat.Ino == 0 || sidecarStat.Ino == fileStat.Ino || sidecarInfo.Size() != int64(len(sidecar)) || sidecarStat.Uid != fileStat.Uid {
			t.Fatal("copied sidecar identity invalid")
		}
	}
	state := localPreviewStatus{
		Nonce: cfg.Nonce, PID: os.Getpid(), Phase: "preparing", Mount: filepath.Join(wd, "view"),
		Folder: folder, File: file, SourceSHA256: cfg.SourceSHA256, PosterSHA256: cfg.PosterSHA256,
		SourceSize: fi.Size(), PosterSize: int64(len(poster)), SourceInode: fileStat.Ino,
	}
	if sidecarInfo != nil {
		state.SidecarPresent = true
		state.SidecarPath, state.SidecarSHA256 = sidecarRel, cfg.SidecarSHA256
		state.SidecarSize, state.SidecarInode = sidecarInfo.Size(), sidecarStat.Ino
	}
	var stateMu sync.Mutex
	var readAttempts, readOK, readErrors, readElapsedNS atomic.Uint64
	var rpc [22]atomic.Uint64
	saveLocked := func() error {
		state.ReadAttempts, state.ReadOK, state.ReadErrors = readAttempts.Load(), readOK.Load(), readErrors.Load()
		state.ReadElapsedNS = readElapsedNS.Load()
		state.RPC = make(map[string]uint64, len(rpc))
		for i := range rpc {
			state.RPC[fmt.Sprint(i)] = rpc[i].Load()
		}
		state.Metrics = metrics.Default().Snapshot()
		b, err := json.MarshalIndent(state, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(wd, "state.json.pending"), b, 0600); err != nil {
			return err
		}
		return os.Rename(filepath.Join(wd, "state.json.pending"), filepath.Join(wd, "state.json"))
	}
	update := func(fn func()) error {
		stateMu.Lock()
		defer stateMu.Unlock()
		fn()
		return saveLocked()
	}
	if err := update(func() {}); err != nil {
		t.Fatal(err)
	}

	var store *metadata.Store
	var ds *derivatives.Store
	var tc *thumbcache.Cache
	var rc *metadata.RedisClient
	var warmer *jmnfs.ThumbWarmer
	var server *jmnfs.Server
	var httpServer *http.Server
	var httpDone chan error
	var httpServeJoined bool
	var httpServeErr error
	var restoreReplies func()
	stopped := false
	cleanup := func() bool {
		if stopped {
			return true
		}
		fail := func(message string) bool {
			_ = update(func() { state.Phase, state.Error = "cleanup-incomplete", message })
			return false
		}
		mounts, err := videoNavMounts(wd)
		if err != nil {
			return fail("mount inventory: " + err.Error())
		}
		if len(mounts) != 0 {
			return fail("mount remains under private root; host must unmount exact owned view first")
		}
		if server != nil {
			server.DisableRestart()
			if err := server.Stop(); err != nil {
				return fail("NFS listener retirement unproved: " + err.Error())
			}
			if !server.QuiesceClients(5*time.Second) || !server.Handler().StopHandler() {
				return fail("NFS clients or handler remain; dependencies retained")
			}
			server = nil
		}
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := httpServer.Shutdown(ctx)
			cancel()
			if err != nil {
				return fail("HTTP shutdown incomplete: " + err.Error())
			}
			if !httpServeJoined {
				select {
				case httpServeErr = <-httpDone:
					httpServeJoined = true
				case <-time.After(5 * time.Second):
					return fail("HTTP serving goroutine not joined")
				}
			}
			if httpServeErr != nil && !errors.Is(httpServeErr, http.ErrServerClosed) {
				return fail("HTTP serve failed: " + httpServeErr.Error())
			}
			httpServer = nil
		}
		if warmer != nil {
			warmer.Stop()
			warmer = nil
		}
		if restoreReplies != nil {
			restoreReplies()
			restoreReplies = nil
		}
		nfslib.SetObserver(nil)
		// StopHandler does not prove that every speculative READDIR worker has
		// joined. Keep its private store, derivative index, thumb cache and RC
		// alive until this one-test process exits instead of closing a database
		// under a possible late reader. No production client remains admitted.
		_ = store
		_ = ds
		_ = tc
		_ = rc
		if mounts, err := videoNavMounts(wd); err != nil || len(mounts) != 0 {
			return fail("mount retirement changed during shutdown")
		}
		stopped = true
		_ = update(func() {
			state.Phase, state.Error = "stopped", ""
			state.DependenciesRetainedUntilExit = true
		})
		return true
	}
	t.Cleanup(func() {
		if !cleanup() {
			t.Error("owned teardown incomplete; inspect private state.json")
		}
	})

	store, err = videoNavOpenPrivateStore(filepath.Join(wd, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	makeEntry := func(rel string, info os.FileInfo) *metadata.Entry {
		st := info.Sys().(*syscall.Stat_t)
		e := metadata.MakeEntry(rel, info.IsDir(), info.Size(), info.ModTime(), st.Ino)
		e.MtimeNsecKnown = true
		e.Mode = info.Mode()
		return e
	}
	folderInfo, err := os.Lstat(mediaFolder)
	if err != nil {
		t.Fatal(err)
	}
	entries := []*metadata.Entry{makeEntry(folder, folderInfo), makeEntry(path.Join(folder, file), fi)}
	if sidecarInfo != nil {
		entries = append(entries, makeEntry(sidecarRel, sidecarInfo))
	}
	if err := store.BulkInsert(entries, len(entries)); err != nil {
		t.Fatal(err)
	}
	if e := store.LookupByPath(path.Join(folder, file)); e == nil || e.Inode != fileStat.Ino || e.Size != fi.Size() {
		t.Fatal("private mirror did not retain source identity")
	}
	if sidecarInfo != nil {
		if e := store.LookupByPath(sidecarRel); e == nil || e.Inode != sidecarStat.Ino || e.Size != sidecarInfo.Size() || !e.Mtime.Equal(sidecarInfo.ModTime()) || e.Mode != sidecarInfo.Mode() {
			t.Fatal("private mirror did not retain truthful sidecar identity")
		}
	}
	ds, err = derivatives.Open(filepath.Join(wd, "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ds.PutSource(fileStat.Ino, &cfg.SourceSHA256); err != nil {
		t.Fatal(err)
	}
	blobDir := filepath.Join(media, derivatives.DerivDirRel(fileStat.Ino))
	if err := os.MkdirAll(blobDir, 0700); err != nil {
		t.Fatal(err)
	}
	blobRel := "poster.jpg"
	mediaType := "image/jpeg"
	if err := os.WriteFile(filepath.Join(blobDir, blobRel), poster, 0600); err != nil {
		t.Fatal(err)
	}
	size, mtime := fi.Size(), fi.ModTime().Unix()
	if err := ds.PutDeriv(fileStat.Ino, derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "fixture-prepared", Version: 1,
		Hash: &cfg.PosterSHA256, BlobRelPath: &blobRel, MediaType: &mediaType,
		SourceSize: &size, SourceMtime: &mtime,
	}); err != nil {
		t.Fatal(err)
	}
	tc, err = thumbcache.Open(filepath.Join(wd, "thumbcache"), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := tc.Put(fileStat.Ino, "thumbnail", bytes.NewReader(poster)); err != nil || n != int64(len(poster)) {
		t.Fatal("private poster cache seed failed", err)
	}
	// Deferred construction is lazy and never starts Redis. The dummy address
	// must not be contacted; the RC exists only for handleThumbLocalHTTP's
	// production store lookup, and is deliberately not attached to NFS.
	rc, err = metadata.NewRedisClientDeferred("redis://127.0.0.1:1/0", store)
	if err != nil {
		t.Fatal(err)
	}
	pin.SetOffline(false)
	pin.SetFUSEIdentityPath(media)
	globalMu.Lock()
	globalStore, globalRC, globalDerivStore, globalThumbCache = store, rc, ds, tc
	globalFUSEPath, globalWantMountPoint = media, state.Mount
	globalMu.Unlock()
	warmer = jmnfs.NewThumbWarmer(tc, resolveWarmThumbBlobPath, func(dir string) []jmnfs.ThumbChildRef {
		children, err := store.ListChildren(dir)
		if err != nil {
			return nil
		}
		out := make([]jmnfs.ThumbChildRef, 0, len(children))
		for _, child := range children {
			out = append(out, jmnfs.ThumbChildRef{Inode: child.Inode, Name: child.Name, IsDir: child.IsDir})
		}
		return out
	})
	globalMu.Lock()
	globalThumbWarmer = warmer
	globalMu.Unlock()

	server = jmnfs.NewServer(jmnfs.Config{ListenAddr: "127.0.0.1:0", FUSEPath: media}, store)
	server.Handler().SetThumbWarmer(warmer)
	restoreReplies = nfslib.ObserveLatencyReplies(func(proc uint32) {
		if int(proc) < len(rpc) {
			rpc[proc].Add(1)
		}
		if proc == 6 {
			readOK.Add(1)
		}
	})
	nfslib.SetObserver(func(program, proc uint32, elapsed time.Duration, err error) {
		metrics.ObserveRPC(program, proc, elapsed, err)
		if program == 100003 && proc == 6 {
			readAttempts.Add(1)
			readElapsedNS.Add(uint64(elapsed.Nanoseconds()))
			if err != nil {
				readErrors.Add(1)
			}
		}
	})
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	addr, export := server.Addr(), server.ExportPath()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := update(func() {
		state.Addr, state.Export = addr, export
		state.MountOptions = nfsMountOpts(port) + ",ro,nobrowse"
	}); err != nil {
		t.Fatal(err)
	}

	var nextID atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/thumb-local", func(w http.ResponseWriter, r *http.Request) {
		id := nextID.Add(1)
		kind := "provider-candidate"
		if r.Header.Get("X-JM-Fixture-Control-Nonce") == cfg.Nonce {
			kind = "control"
		}
		name := path.Base(r.URL.Query().Get("path"))
		entry := localPreviewRequest{ID: id, Event: "entry", At: time.Now().UTC().Format(time.RFC3339Nano), Name: name, RequestType: kind}
		if err := update(func() {
			state.Requests = append(state.Requests, entry)
			state.HTTPEntries++
			if kind == "provider-candidate" {
				state.ProviderCandidateEntries++
			}
		}); err != nil {
			http.Error(w, "fixture evidence write failed", http.StatusInternalServerError)
			return
		}
		start := time.Now()
		wrapped := &localPreviewWriter{ResponseWriter: w, hash: sha256.New()}
		defer func() {
			terminal := localPreviewRequest{ID: id, Event: "terminal", At: time.Now().UTC().Format(time.RFC3339Nano), Name: name, RequestType: kind,
				Status: wrapped.code, Label: wrapped.Header().Get("X-JM-Thumb"), Bytes: wrapped.bytes,
				ElapsedMS: float64(time.Since(start)) / float64(time.Millisecond)}
			if terminal.Status == 0 {
				terminal.Status = http.StatusOK
			}
			if !wrapped.oversized {
				terminal.BodySHA256 = hex.EncodeToString(wrapped.hash.Sum(nil))
			}
			if wrapped.writeErr != nil {
				terminal.Error = "response write: " + wrapped.writeErr.Error()
			}
			if ctxErr := r.Context().Err(); ctxErr != nil {
				terminal.Canceled = true
				if terminal.Error != "" {
					terminal.Error += "; "
				}
				terminal.Error += "request context: " + ctxErr.Error()
			}
			p := recover()
			if p != nil {
				if terminal.Error != "" {
					terminal.Error += "; "
				}
				terminal.Error += "panic: " + fmt.Sprint(p)
			}
			_ = update(func() {
				state.Requests = append(state.Requests, terminal)
				state.HTTPCompleted++
				if kind == "provider-candidate" {
					state.ProviderCandidateCompleted++
				}
			})
			if p != nil {
				panic(p)
			}
		}()
		handleThumbLocalHTTP(wrapped, r)
	})
	httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	httpDone = make(chan error, 1)
	go func() { httpDone <- httpServer.Serve(httpListener) }()
	if err := update(func() { state.Phase = "ready" }); err != nil {
		t.Fatal(err)
	}
	t.Logf("private preview ready pid=%d file=%s; waiting for nonce control", os.Getpid(), file)

	for {
		conn, err := control.Accept()
		if err != nil {
			if stopped {
				return
			}
			t.Log("control accept: ", err)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
		line, err := bufio.NewReader(io.LimitReader(conn, 4096)).ReadBytes('\n')
		var request struct{ Nonce, Op string }
		if err != nil || json.Unmarshal(line, &request) != nil || request.Nonce != cfg.Nonce {
			conn.Close()
			continue
		}
		stopNow := false
		if request.Op == "stop" {
			stopNow = cleanup()
		} else if request.Op != "status" {
			_ = json.NewEncoder(conn).Encode(map[string]string{"error": "unknown control operation"})
			conn.Close()
			continue
		}
		mounts, mountErr := videoNavMounts(wd)
		stateMu.Lock()
		snapshot := state
		stateMu.Unlock()
		snapshot.Mounts = mounts
		if mountErr != nil {
			snapshot.Error = "mount inventory: " + mountErr.Error()
		}
		snapshot.RPC = make(map[string]uint64)
		for i := range rpc {
			snapshot.RPC[fmt.Sprint(i)] = rpc[i].Load()
		}
		snapshot.ReadAttempts, snapshot.ReadOK, snapshot.ReadErrors = readAttempts.Load(), readOK.Load(), readErrors.Load()
		snapshot.ReadElapsedNS = readElapsedNS.Load()
		snapshot.Metrics = metrics.Default().Snapshot()
		_ = json.NewEncoder(conn).Encode(snapshot)
		conn.Close()
		if stopNow {
			return
		}
	}
}
