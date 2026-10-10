package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func residentFlushFixture(t *testing.T, hashed bool) (*Reader, []byte, []string) {
	t.Helper()
	container := filepath.Join(t.TempDir(), "volumes")
	chunks := filepath.Join(container, "flush-volume", "raw", "chunks")
	body := bytes.Repeat([]byte("resident-image-"), 10003)
	r := NewReader(chunks, 64<<10, nil)
	r.binding = VolumeBinding{Version: 1, Backend: strings.Repeat("a", 64), UUID: "flush-volume", BlockSize: 64 << 10}
	attr := ResidentAttr{Size: int64(len(body)), MtimeNS: 123, Valid: true}
	flushTestPair(r, "3_0", 77, int64(len(body)))
	r.inodeAttr[3] = attr
	snapshot := filepath.Join(t.TempDir(), "mapping.gob")
	if err := r.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	var names []string
	for off := int64(0); off < int64(len(body)); off += r.blockSize {
		amount := min(r.blockSize, int64(len(body))-off)
		name := r.blockPath(77, off/r.blockSize, amount)
		if hashed {
			name = r.hashBlockPath(77, off/r.blockSize, amount)
		}
		writeVerifiedCacheFile(t, name, body[off:off+amount])
		names = append(names, name)
	}
	return r, body, names
}

func TestFlushResidentImageReopensWithoutRedis(t *testing.T) {
	for _, hashed := range []bool{false, true} {
		name := "plain"
		if hashed {
			name = "hashed"
		}
		t.Run(name, func(t *testing.T) {
			r, body, names := residentFlushFixture(t, hashed)
			var synced []string
			files := 0
			mappingFlushed := false
			r.testSyncResident = func(f *os.File, directory bool) error {
				if mappingFlushed {
					t.Error("block/directory sync followed mapping publication")
				}
				if !directory {
					files++
				}
				synced = append(synced, f.Name())
				return f.Sync()
			}
			r.testSyncMappingDirectory = func(f *os.File) error { mappingFlushed = true; return f.Sync() }
			attr := r.inodeAttr[3]
			want := sha256.Sum256(body)
			graph, err := r.FlushResidentImage(context.Background(), 3, attr, want)
			if err != nil || !graph.Token.Valid() || graph.Inode != 3 {
				t.Fatalf("handoff=%+v err=%v", graph, err)
			}
			if files != len(names) || len(synced) != len(names)+6 || !mappingFlushed {
				t.Fatalf("actual sync coverage files=%d dirs=%d mapping=%v", files, len(synced)-files, mappingFlushed)
			}
			r.testSyncMappingDirectory = nil
			// Load the flushed snapshot before Stop can persist another one.
			fresh := NewReader(r.cacheDir, r.blockSize, nil)
			fresh.binding = r.binding
			t.Cleanup(fresh.Stop)
			if err := fresh.SetMappingSnapshotPath(r.mappingPath); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(body))
			token, err := fresh.ReadResidentRange(3, 0, got, attr)
			if err != nil || !token.Valid() || !bytes.Equal(got, body) {
				t.Fatalf("fresh offline reader: %v", err)
			}
		})
	}
}

func TestFlushResidentImageRejectsUncertainBytes(t *testing.T) {
	for _, kind := range []string{"wrong-digest", "missing-block", "corrupt-crc", "no-trailer", "file-symlink", "hardlink", "parent-symlink", "hole", "missing-attr", "file-sync", "directory-sync", "replace-block", "edit-same-inode", "replace-parent", "replace-container", "invalidate", "cancel", "mapping-sync", "replace-after-mapping", "replace-mapping-file", "replace-mapping-parent"} {
		t.Run(kind, func(t *testing.T) {
			r, body, names := residentFlushFixture(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			attr := r.inodeAttr[3]
			want := sha256.Sum256(body)
			injected := errors.New("resident-flush-injected")
			calls := 0
			mappingCalls := 0
			replace := func(name string) {
				t.Helper()
				original, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(name, name+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "wrong-digest":
				want[0] ^= 1
			case "missing-block":
				if err := os.Remove(names[1]); err != nil {
					t.Fatal(err)
				}
			case "corrupt-crc":
				f, err := os.OpenFile(names[0], os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte{255}, 0)
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "no-trailer":
				if err := os.Truncate(names[0], r.blockSize); err != nil {
					t.Fatal(err)
				}
			case "file-symlink":
				if err := os.Rename(names[0], names[0]+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(names[0]+".retained", names[0]); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(names[0], names[0]+".linked"); err != nil {
					t.Fatal(err)
				}
			case "parent-symlink":
				dir := filepath.Dir(names[0])
				if err := os.Rename(dir, dir+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(dir+".retained", dir); err != nil {
					t.Fatal(err)
				}
			case "hole":
				r.sliceCache["3_0"] = []SliceInfo{}
			case "missing-attr":
				delete(r.attrWitness, "3_0")
			}
			r.testSyncResident = func(f *os.File, directory bool) error {
				calls++
				if kind == "file-sync" && !directory || kind == "directory-sync" && directory {
					return injected
				}
				if calls == 1 {
					switch kind {
					case "replace-block":
						replace(names[0])
					case "edit-same-inode":
						info, err := os.Stat(names[0])
						if err != nil {
							t.Fatal(err)
						}
						edit, err := os.OpenFile(names[0], os.O_WRONLY, 0)
						if err != nil {
							t.Fatal(err)
						}
						_, err = edit.WriteAt([]byte{255}, 0)
						_ = edit.Close()
						if err != nil {
							t.Fatal(err)
						}
						if err := os.Chtimes(names[0], info.ModTime(), info.ModTime()); err != nil {
							t.Fatal(err)
						}
					case "replace-parent":
						dir := filepath.Dir(names[0])
						if err := os.Rename(dir, dir+".retained"); err != nil {
							t.Fatal(err)
						}
						if err := os.Mkdir(dir, 0700); err != nil {
							t.Fatal(err)
						}
					case "replace-container":
						dir := filepath.Dir(filepath.Dir(filepath.Dir(r.cacheDir)))
						if err := os.Rename(dir, dir+".retained"); err != nil {
							t.Fatal(err)
						}
						if err := os.Mkdir(dir, 0700); err != nil {
							t.Fatal(err)
						}
					case "invalidate":
						r.InvalidateSliceCache(3)
					case "cancel":
						cancel()
					}
				}
				return f.Sync()
			}
			r.testSyncMappingDirectory = func(f *os.File) error {
				mappingCalls++
				if kind == "mapping-sync" {
					return injected
				}
				if kind == "replace-after-mapping" && mappingCalls == 1 {
					replace(names[0])
				}
				if kind == "replace-mapping-file" && mappingCalls == 1 {
					replace(r.mappingPath)
				}
				if kind == "replace-mapping-parent" && mappingCalls == 1 {
					dir := filepath.Dir(r.mappingPath)
					if err := os.Rename(dir, dir+".retained"); err != nil {
						t.Fatal(err)
					}
					// Keep the retained test directory within cleanup custody.
					t.Cleanup(func() { _ = os.RemoveAll(dir + ".retained") })
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
				}
				return f.Sync()
			}
			graph, err := r.FlushResidentImage(ctx, 3, attr, want)
			if err == nil || graph.Token.Valid() {
				t.Fatalf("uncertain image accepted: %v", err)
			}
			if kind == "wrong-digest" && !errors.Is(err, ErrResidentDigest) {
				t.Fatalf("digest cause lost: %v", err)
			}
			if strings.HasSuffix(kind, "sync") && !errors.Is(err, injected) {
				t.Fatalf("sync cause lost: %v", err)
			}
			if (kind == "replace-after-mapping" || kind == "replace-mapping-file" || kind == "replace-mapping-parent") && mappingCalls != 1 {
				t.Fatalf("substitution missed mapping boundary: %d", mappingCalls)
			}
			// Stop may persist ordinary unqualified cache mappings; that never confers
			// a successful image handoff or permission to discard private saved bytes.
			r.testSyncMappingDirectory = nil
		})
	}
}

func TestFlushResidentImageEmptyNeedsEOF(t *testing.T) {
	r := NewReader(filepath.Join(t.TempDir(), "absent-volume", "raw", "chunks"), 64<<10, nil)
	r.binding = VolumeBinding{Version: 1, Backend: strings.Repeat("a", 64), UUID: "absent-volume", BlockSize: 64 << 10}
	t.Cleanup(r.Stop)
	if err := r.SetMappingSnapshotPath(filepath.Join(t.TempDir(), "mapping.gob")); err != nil {
		t.Fatal(err)
	}
	attr := ResidentAttr{Size: 0, MtimeNS: 123, Valid: true}
	if _, err := r.FlushResidentImage(context.Background(), 3, attr, sha256.Sum256(nil)); err == nil {
		t.Fatal("empty image fabricated EOF")
	}
	flushTestPair(r, "3_0", 77, 0)
	r.sliceCache["3_0"] = []SliceInfo{}
	r.inodeAttr[3] = attr
	graph, err := r.FlushResidentImage(context.Background(), 3, attr, sha256.Sum256(nil))
	if err != nil || !graph.Token.Valid() {
		t.Fatalf("observed empty image: %v", err)
	}
	if _, err := os.Stat(r.cacheDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty flush fabricated cache blocks: %v", err)
	}
}

func TestFlushResidentImageStopJoinsOwnedIO(t *testing.T) {
	r, body, _ := residentFlushFixture(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.testSyncResident = func(f *os.File, directory bool) error {
		if !directory {
			once.Do(func() { close(entered); <-release })
		}
		return f.Sync()
	}
	result := make(chan error, 1)
	stopped := make(chan struct{})
	var unblock sync.Once
	var workers sync.WaitGroup
	attr := r.inodeAttr[3]
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := r.FlushResidentImage(context.Background(), 3, attr, sha256.Sum256(body))
		result <- err
	}()
	t.Cleanup(func() { unblock.Do(func() { close(release) }); workers.Wait() })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sync not entered")
	}
	workers.Add(1)
	go func() { defer workers.Done(); r.Stop(); close(stopped) }()
	deadline := time.After(5 * time.Second)
	for !r.stopped.Load() {
		select {
		case <-deadline:
			t.Fatal("Stop not entered")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-stopped:
		t.Error("Stop abandoned active flush")
	default:
	}
	if _, err := r.FlushResidentImage(context.Background(), 3, attr, sha256.Sum256(body)); err == nil {
		t.Error("stopped reader admitted flush")
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-result:
		if err == nil {
			t.Error("flush succeeded after Stop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not join")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not join original flush")
	}
}
