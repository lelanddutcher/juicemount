//go:build darwin && latency_acceptance

package nfs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// This optional kernel fixture accepts only flat, owned synthetic seed files.
// It has no network source and never copies native xattrs: callers must supply
// explicit AppleDouble bytes so the NFS client exercises the real sidecar path.
type kernelMediaSeed struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func kernelReadOwned(path string, limit int64) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Uid != uint32(os.Getuid()) || info.Size() > limit {
		return nil, fmt.Errorf("seed must be an owned regular file of at most %d bytes", limit)
	}
	body, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(body)) > limit {
		err = errors.New("seed grew beyond limit")
	}
	return body, err
}

func kernelSeedMedia(root string, store *metadata.Store) error {
	seedDir := filepath.Join(root, "seed")
	info, err := os.Lstat(seedDir)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0700 || st.Uid != uint32(os.Getuid()) {
		return errors.New("seed directory must be real, owned and 0700")
	}
	manifest, err := kernelReadOwned(filepath.Join(root, "media-seed.json"), 16384)
	if err != nil {
		return err
	}
	var seeds []kernelMediaSeed
	if err := json.Unmarshal(manifest, &seeds); err != nil {
		return err
	}
	if len(seeds) == 0 || len(seeds) > 8 {
		return errors.New("one to eight seed files required")
	}
	bodies := make([][]byte, len(seeds))
	seen := make(map[string]bool)
	var total int64
	for i, seed := range seeds {
		if seed.Name == "" || seed.Name == "." || seed.Name == ".." || len(seed.Name) > 128 ||
			strings.ContainsAny(seed.Name, "/\\\x00") || seen[seed.Name] || seed.Bytes < 0 || seed.Bytes > 32<<20 {
			return errors.New("invalid or duplicate flat seed name/size")
		}
		seen[seed.Name] = true
		total += seed.Bytes
		if total > 64<<20 {
			return errors.New("aggregate seed exceeds 64 MiB")
		}
		body, err := kernelReadOwned(filepath.Join(seedDir, seed.Name), seed.Bytes)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(body)
		if int64(len(body)) != seed.Bytes || hex.EncodeToString(digest[:]) != seed.SHA256 {
			return fmt.Errorf("seed identity mismatch for %s", seed.Name)
		}
		bodies[i] = body
	}
	// Validate the complete manifest before creating backing files or rows.
	backing := filepath.Join(root, "backing")
	if err := os.Mkdir(backing, 0700); err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(backing, "media"), 0700); err != nil {
		return err
	}
	stamp := time.Now()
	entries := []*metadata.Entry{metadata.MakeEntry("media", true, 0, stamp, 2000)}
	for i, seed := range seeds {
		f, err := os.OpenFile(filepath.Join(backing, "media", seed.Name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(bodies[i])
		if err := errors.Join(writeErr, f.Close()); err != nil {
			return err
		}
		entries = append(entries, metadata.MakeEntry("media/"+seed.Name, false, seed.Bytes, stamp, uint64(2001+i)))
	}
	return store.BulkInsert(entries, 500)
}

type kernelMediaTraceStatus struct {
	Events  int    `json:"events"`
	Dropped int    `json:"dropped"`
	Error   string `json:"error,omitempty"`
}

type kernelMediaTrace struct {
	mu    sync.Mutex
	f     *os.File
	state kernelMediaTraceStatus
}

func newKernelMediaTrace(path string) (*kernelMediaTrace, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &kernelMediaTrace{f: f}, nil
}

// Record logical ReadAt requests and returned bytes, including cache-backed
// sidecars. These are filesystem reads, not a count of WAN bytes or RPC packets.
func (t *kernelMediaTrace) record(op, name string, off int64, requested, returned int, started time.Time, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state.Events >= 10000 || t.state.Error != "" {
		t.state.Dropped++
		return
	}
	value := struct {
		Op         string `json:"op"`
		Name       string `json:"name"`
		Offset     int64  `json:"offset"`
		Requested  int    `json:"requested"`
		Returned   int    `json:"returned"`
		StartedNS  int64  `json:"started_ns"`
		DurationNS int64  `json:"duration_ns"`
		Error      string `json:"error,omitempty"`
	}{op, name, off, requested, returned, started.UnixNano(), time.Since(started).Nanoseconds(), ""}
	if err != nil {
		value.Error = err.Error()
	}
	if err := json.NewEncoder(t.f).Encode(value); err != nil {
		t.state.Error = err.Error()
		return
	}
	t.state.Events++
}

func (t *kernelMediaTrace) status() kernelMediaTraceStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *kernelMediaTrace) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return errors.Join(t.f.Sync(), t.f.Close())
}

func TestKernelMediaSeedRejectsUnownedInputShape(t *testing.T) {
	for _, mode := range []string{"traversal", "symlink", "wrong-hash", "duplicate", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "seed"), 0700); err != nil {
				t.Fatal(err)
			}
			body := []byte("owned-media")
			digest := sha256.Sum256(body)
			seed := kernelMediaSeed{"clip.mp4", int64(len(body)), hex.EncodeToString(digest[:])}
			if err := os.WriteFile(filepath.Join(root, "seed", seed.Name), body, 0600); err != nil {
				t.Fatal(err)
			}
			seeds := []kernelMediaSeed{seed}
			switch mode {
			case "traversal":
				seeds[0].Name = "../clip.mp4"
			case "symlink":
				seeds[0].Name = "link.mp4"
				if err := os.Symlink("clip.mp4", filepath.Join(root, "seed", "link.mp4")); err != nil {
					t.Fatal(err)
				}
			case "wrong-hash":
				seeds[0].SHA256 = strings.Repeat("0", 64)
			case "duplicate":
				seeds = append(seeds, seed)
			case "oversize":
				seeds[0].Bytes = (32 << 20) + 1
			}
			data, _ := json.Marshal(seeds)
			if err := os.WriteFile(filepath.Join(root, "media-seed.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := kernelSeedMedia(root, nil); err == nil {
				t.Fatal("invalid seed accepted")
			}
			if _, err := os.Lstat(filepath.Join(root, "backing")); !os.IsNotExist(err) {
				t.Fatalf("backing created before admission: %v", err)
			}
		})
	}
}

func TestKernelMediaTraceCapturesPartialRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "reads.jsonl")
	trace, err := newKernelMediaTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(strings.Repeat("x", 4113))
	if err := os.WriteFile(filepath.Join(root, "clip.mp4"), body, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(root, "clip.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	wrapped := &kernelLatencyFile{File: &billyFile{File: file}, name: "media/clip.mp4", trace: trace}
	buf := make([]byte, 8192)
	n, readErr := wrapped.ReadAt(buf, 4096)
	if n != 17 || !errors.Is(readErr, io.EOF) || string(buf[:n]) != strings.Repeat("x", 17) {
		t.Fatalf("partial read changed: n=%d err=%v", n, readErr)
	}
	if got := trace.status(); got.Events != 1 || got.Dropped != 0 || got.Error != "" {
		t.Fatalf("trace status: %+v", got)
	}
	if err := trace.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	if value["offset"] != float64(4096) || value["requested"] != float64(8192) || value["returned"] != float64(17) || value["error"] != "EOF" {
		t.Fatalf("incorrect trace: %s", data)
	}
}

func TestKernelMediaSeedPreservesBodyAndMetadata(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "seed"), 0700); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seeds := []kernelMediaSeed{}
	want := map[string][]byte{"clip.mp4": []byte("synthetic original"), "._clip.mp4": []byte("nonempty resource metadata")}
	for name, body := range want {
		digest := sha256.Sum256(body)
		seeds = append(seeds, kernelMediaSeed{name, int64(len(body)), hex.EncodeToString(digest[:])})
		if err := os.WriteFile(filepath.Join(root, "seed", name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := json.Marshal(seeds)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "media-seed.json"), manifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := kernelSeedMedia(root, store); err != nil {
		t.Fatal(err)
	}
	for name, body := range want {
		got, err := os.ReadFile(filepath.Join(root, "backing", "media", name))
		if err != nil || string(got) != string(body) {
			t.Fatalf("seed body %q changed: %v", name, err)
		}
		entry := store.LookupByPath("media/" + name)
		if entry == nil || entry.Size != int64(len(body)) {
			t.Fatalf("seed metadata %q wrong: %+v", name, entry)
		}
	}
	entries, err := store.ListChildren("media")
	if err != nil || len(entries) != 2 {
		t.Fatalf("seed listing: len=%d err=%v", len(entries), err)
	}
}
