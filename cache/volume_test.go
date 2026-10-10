//go:build redisoptionsacceptance

package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

const activeTestUUID = "22222222-2222-4222-8222-222222222222"
const foreignTestUUID = "11111111-1111-4111-8111-111111111111"

type volumeTestStore struct {
	values map[string]string
	fail   error
}

func (s *volumeTestStore) GetMeta(key string) (string, bool, error) {
	v, ok := s.values[key]
	return v, ok, nil
}

func (s *volumeTestStore) SetMeta(key, value string) error {
	if s.fail != nil {
		return s.fail
	}
	s.values[key] = value
	return nil
}

func newVolumeTestStore() *volumeTestStore { return &volumeTestStore{values: make(map[string]string)} }

func volumeFixture(t *testing.T) (string, *redis.Client, VolumeBinding) {
	t.Helper()
	rawURL := redistestfixture.Start(t)
	opts, err := redisurl.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+activeTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	binding, err := ReadVolumeBinding(ctx, rdb, rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return rawURL, rdb, binding
}

func seedVolumeSlice(t *testing.T, rdb *redis.Client) {
	t.Helper()
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 77)
	binary.BigEndian.PutUint32(record[12:16], 4096)
	binary.BigEndian.PutUint32(record[20:24], 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.RPush(ctx, "c3_0", string(record[:])).Err(); err != nil {
		t.Fatal(err)
	}
}

func seedVolumeBytes(t *testing.T, uuid string, body []byte) {
	t.Helper()
	chunks := filepath.Join(os.Getenv("HOME"), ".juicefs", "cache", uuid, "raw", "chunks")
	writeVerifiedCacheFile(t, filepath.Join(chunks, "0", "0", "77_0_4096"), body)
}

func TestVolumeCacheSelectsActiveBody(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	foreign := bytes.Repeat([]byte{'A'}, 4096)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, foreignTestUUID, foreign)
	seedVolumeBytes(t, activeTestUUID, want)
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := make([]byte, len(want))
	if n, err := r.ReadBlock(ctx, 3, 0, got); err != nil || n != len(want) {
		t.Fatalf("read = %d, %v", n, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("selected cache returned another volume's CRC-valid body")
	}
}

func TestVolumeCacheColdDirectoryBecomesReadable(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	// A warm sibling must not substitute for the selected volume's absent cache.
	seedVolumeBytes(t, foreignTestUUID, bytes.Repeat([]byte{'A'}, 4096))
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if _, err := os.Stat(r.cacheDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cold directory = %v", err)
	}
	defer juicefsbin.SetForTest(fakeBundledJuiceFS(t, "juicefs version 1.3.1+2025-12-02.e0032b2ae5e9-jm1"))()
	if err := r.Verify(); err != nil {
		t.Fatalf("production compatibility check refused cold attachment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := make([]byte, 4096)
	if _, err := r.ReadBlock(ctx, 3, 0, got); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("cold read = %v", err)
	}
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	if n, err := r.ReadBlock(ctx, 3, 0, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("same reader after fill = %d, %v, match=%t", n, err, bytes.Equal(got, want))
	}
}

func TestVolumeCacheOfflineReaderReload(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	rawURL, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "maps.gob")
	first, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := first.ReadBlock(ctx, 3, 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	first.Stop() // flush actual snapshot and close the online pool
	saved, ok, err := LoadVolumeBinding(store, rawURL)
	if err != nil || !ok {
		t.Fatalf("saved binding = %t, %v", ok, err)
	}
	second, err := NewVolumeReader(saved, nil) // no network client exists
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	if err := second.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if n, err := second.ReadBlock(context.Background(), 3, 0, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("offline reloaded read = %d, %v, match=%t", n, err, bytes.Equal(got, want))
	}
	// Offline local hits require no Redis metadata; an unknown mapping misses.
	if _, err := second.ReadBlock(context.Background(), 4, 0, got); err == nil {
		t.Fatal("unknown offline mapping unexpectedly readable")
	}
}

func TestVolumeCacheRefusesChangedFormatWithIdenticalSlices(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	seedVolumeBytes(t, activeTestUUID, bytes.Repeat([]byte{'B'}, 4096))
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+foreignTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.RevalidateMappings(ctx); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("empty mappings identity classification = %v", err)
	}
	if _, err := r.ReadBlock(ctx, 3, 0, make([]byte, 4096)); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("different format classification = %v", err)
	}
	if len(r.sliceCache) != 0 {
		t.Fatal("foreign mappings published")
	}
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+activeTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadBlock(ctx, 3, 0, make([]byte, 4096)); err != nil {
		t.Fatalf("same reader cannot recover when expected volume returns: %v", err)
	}
}

func TestVolumeCacheSnapshotIdentityPreserved(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, _, binding := volumeFixture(t)
	for _, kind := range []string{"backend", "uuid", "block-size", "legacy", "unbound"} {
		t.Run(kind, func(t *testing.T) {
			r, err := NewVolumeReader(binding, nil)
			if err != nil {
				t.Fatal(err)
			}
			s := mappingSnapshot{Version: mappingVersion, Binding: binding, CacheDir: r.cacheDir, Entries: map[string][]SliceInfo{"3_0": {{SliceID: 77, Size: 4096, Len: 4096}}}}
			switch kind {
			case "backend":
				s.Binding.Backend = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			case "uuid":
				s.Binding.UUID = foreignTestUUID
			case "block-size":
				s.Binding.BlockSize = 2 << 20
			case "legacy":
				s.Version = 1
				s.Binding = VolumeBinding{}
			case "unbound":
				s.Binding = VolumeBinding{}
			}
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(s); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "map.gob")
			if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			if err := r.SetMappingSnapshotPath(path); !errors.Is(err, ErrMappingIdentity) {
				t.Fatalf("mismatched snapshot = %v", err)
			}
			if len(r.sliceCache) != 0 {
				t.Fatal("mismatched snapshot entered RAM")
			}
			r.Stop()
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, buf.Bytes()) {
				t.Fatalf("refusal destroyed another snapshot: %v", err)
			}
		})
	}
}

func TestVolumeCacheBindingUsesEffectiveDatabase(t *testing.T) {
	a, err := BackendIdentity("redis://user:secret@EXAMPLE.test:6379/2?db=3&read_timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	b, err := BackendIdentity("redis://changed:rotated@example.test/3")
	if err != nil || a != b {
		t.Fatalf("same effective database changed identity: %v", err)
	}
	c, err := BackendIdentity("redis://example.test/2")
	if err != nil || a == c {
		t.Fatal("different effective DB shares identity")
	}
	d, err := BackendIdentity("rediss://example.test/3")
	if err != nil || a == d {
		t.Fatal("different transport shares identity")
	}
}

func TestVolumeCachePersistedBindingRefusesSettingsChange(t *testing.T) {
	rawURL, _, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if _, ok, err := LoadVolumeBinding(store, rawURL); err != nil || ok {
		t.Fatalf("empty binding = %t, %v", ok, err)
	}
	store.fail = errors.New("injected persist failure")
	if err := SaveVolumeBinding(store, binding); err == nil || len(store.values) != 0 {
		t.Fatal("failed binding persist reported success")
	}
	store.fail = nil
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	before := store.values[volumeBindingKey]
	if _, _, err := LoadVolumeBinding(store, "redis://different.example/3"); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("settings change = %v", err)
	}
	changed := binding
	changed.UUID = foreignTestUUID
	if err := SaveVolumeBinding(store, changed); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("volume change = %v", err)
	}
	if store.values[volumeBindingKey] != before {
		t.Fatal("refusal overwrote previous library binding")
	}
}

func TestVolumeCacheDirectoryOverrideBoundToUUID(t *testing.T) {
	base := t.TempDir()
	for _, suffix := range []string{"", filepath.Join("raw", "chunks")} {
		t.Setenv("JM_CACHE_DIR", filepath.Join(base, activeTestUUID, suffix))
		got, err := VolumeCacheDir(activeTestUUID)
		if err != nil || got != filepath.Join(base, activeTestUUID, "raw", "chunks") {
			t.Fatalf("qualified cold override = %q, %v", got, err)
		}
	}
	t.Setenv("JM_CACHE_DIR", filepath.Join(base, foreignTestUUID))
	if _, err := VolumeCacheDir(activeTestUUID); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("foreign override = %v", err)
	}
	for _, uuid := range []string{"", "..", "../other", "one/two", "one\\two", "/absolute"} {
		if _, err := VolumeCacheDir(uuid); !errors.Is(err, ErrVolumeBinding) {
			t.Fatalf("invalid UUID %q = %v", uuid, err)
		}
	}
}

func TestVolumeCacheClassifiesInvalidFormat(t *testing.T) {
	rawURL, rdb, _ := volumeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, kind := range []string{"missing", "malformed", "oversized", "wrong-type", "unsafe-uuid", "unsupported-block"} {
		t.Run(kind, func(t *testing.T) {
			if err := rdb.Del(ctx, "setting").Err(); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "malformed":
				if err := rdb.Set(ctx, "setting", "{", 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				if err := rdb.Set(ctx, "setting", bytes.Repeat([]byte{'x'}, 65537), 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "wrong-type":
				if err := rdb.HSet(ctx, "setting", "UUID", activeTestUUID).Err(); err != nil {
					t.Fatal(err)
				}
			case "unsafe-uuid":
				if err := rdb.Set(ctx, "setting", `{"UUID":"../other","BlockSize":4096}`, 0).Err(); err != nil {
					t.Fatal(err)
				}
			case "unsupported-block":
				if err := rdb.Set(ctx, "setting", `{"UUID":"`+activeTestUUID+`","BlockSize":3}`, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ReadVolumeBinding(ctx, rdb, rawURL); !errors.Is(err, ErrVolumeBinding) {
				t.Fatalf("%s classification = %v", kind, err)
			}
		})
	}
	// A transport failure is not evidence that a different volume answered.
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadVolumeBinding(ctx, rdb, rawURL); err == nil || errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("closed transport classification = %v", err)
	}
}

func TestVolumeCacheRequiresEffectiveDeadline(t *testing.T) {
	rawURL, rdb, binding := volumeFixture(t)
	if _, err := ReadVolumeBinding(context.Background(), rdb, rawURL); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("unbounded observation = %v", err)
	}
	opts := *rdb.Options()
	opts.ContextTimeoutEnabled = false
	unbounded := redis.NewClient(&opts)
	defer unbounded.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ReadVolumeBinding(ctx, unbounded, rawURL); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("ineffective deadline = %v", err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	r, err := NewVolumeReader(binding, unbounded)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if err := r.ValidateVolume(ctx); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("ineffective validation deadline = %v", err)
	}
}

func TestVolumeCacheQualifiedSnapshotPathMigratesWithoutRelabeling(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	rawURL, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	state := t.TempDir()
	legacy := filepath.Join(state, "cache-mappings.gob")
	var buf bytes.Buffer
	s := mappingSnapshot{Version: 1, CacheDir: filepath.Join(os.Getenv("HOME"), ".juicefs", "cache", activeTestUUID, "raw", "chunks"), Entries: map[string][]SliceInfo{"3_0": {{SliceID: 999, Size: 4096, Len: 4096}}}}
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := r.MappingSnapshotPath(state)
	if snapshot == legacy {
		t.Fatal("qualified reader reused unqualified legacy path")
	}
	if err := r.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := r.ReadBlock(ctx, 3, 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	r.Stop()
	gotLegacy, err := os.ReadFile(legacy)
	if err != nil || !bytes.Equal(gotLegacy, buf.Bytes()) {
		t.Fatalf("legacy data changed: %v", err)
	}
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	saved, ok, err := LoadVolumeBinding(store, rawURL)
	if err != nil || !ok {
		t.Fatal(err)
	}
	offline, err := NewVolumeReader(saved, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer offline.Stop()
	if offline.MappingSnapshotPath(state) != snapshot {
		t.Fatal("offline snapshot path changed")
	}
	if err := offline.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err := offline.ReadBlock(context.Background(), 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("migrated offline read = %v, match=%t", err, bytes.Equal(got, want))
	}
}

func TestVolumeCacheRouteSelectionChangesBinding(t *testing.T) {
	a, err := BackendIdentity("redis://example.test/3", "bucket-a", "link-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := BackendIdentity("redis://example.test/3", "bucket-b", "link-a")
	if err != nil || a == b {
		t.Fatal("bucket selection ignored")
	}
	c, err := BackendIdentity("redis://example.test/3", "bucket-a", "link-b")
	if err != nil || a == c {
		t.Fatal("private-network selection ignored")
	}
	d, err := BackendIdentity("redis://example.test/3", "bucket-alink-a")
	if err != nil || a == d {
		t.Fatal("selection encoding ambiguous")
	}
}

func TestVolumeCacheCorruptSlicesAreNotVolumeMismatch(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "c3_0", "not-a-slice-list", 0).Err(); err != nil {
		t.Fatal(err)
	}
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if err := r.ValidateVolume(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.fetchSliceRecords(ctx, rdb, "c3_0"); err == nil || errors.Is(err, ErrVolumeBinding) || !redis.HasErrorPrefix(err, "WRONGTYPE") {
		t.Fatalf("slice type error classification = %v", err)
	}
	if _, err := r.ReadBlock(ctx, 3, 0, make([]byte, 4096)); !errors.Is(err, ErrCacheMiss) || errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("corrupt mapping read classification = %v", err)
	}
	r.sliceCache["3_0"] = []SliceInfo{{SliceID: 77, Size: 4096, Len: 4096}}
	if _, _, err := r.RevalidateMappings(ctx); err == nil || errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("revalidation classification = %v", err)
	}
	if len(r.sliceCache["3_0"]) != 1 {
		t.Fatal("failed validation partially changed maps")
	}
}

func TestVolumeCacheProbeHonorsInternalDeadline(t *testing.T) {
	rawURL, rdb, _ := volumeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := rdb.Do(ctx, "CLIENT", "PAUSE", 6000, "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := ReadVolumeBinding(ctx, rdb, rawURL)
	elapsed := time.Since(start)
	if err == nil || errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("paused transport = %v", err)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("probe ignored 2s internal deadline: %v", elapsed)
	}
	t.Logf("owned paused Redis returned in %v: %v", elapsed, err)
}
