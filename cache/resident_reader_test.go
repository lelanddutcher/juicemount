//go:build redisoptionsacceptance

package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

func residentTestAttr(size uint64) []byte {
	return (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: size, Mtime: 1700000000, Mtimensec: 123}).Marshal()
}

func residentWant(size int64) ResidentAttr {
	return ResidentAttr{Size: size, MtimeNS: 1700000000*1_000_000_000 + 123, Valid: true}
}

func residentReaderFixture(t *testing.T) (*Reader, *redis.Client, VolumeBinding) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t) // owned isolated Redis, never ambient :6379
	seedVolumeSlice(t, rdb)
	seedVolumeBytes(t, activeTestUUID, bytes.Repeat([]byte{'B'}, 4096))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "i3", residentTestAttr(4096), 0).Err(); err != nil {
		t.Fatal(err)
	}
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	return r, rdb, binding
}

func residentWarm(t *testing.T, r *Reader) ResidentToken {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := r.ReadBlock(ctx, 3, 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	token, err := r.ReadResidentRange(3, 0, buf, residentWant(4096))
	if err != nil || !bytes.Equal(buf, bytes.Repeat([]byte{'B'}, 4096)) || !token.Valid() {
		t.Fatalf("resident warm: err=%v valid=%v", err, token.Valid())
	}
	return token
}

func TestResidentJointWitnessPersistsAndLegacyDeclines(t *testing.T) {
	r, _, binding := residentReaderFixture(t)
	snapshot := filepath.Join(t.TempDir(), "mapping.gob")
	if err := r.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	token := residentWarm(t, r)
	if err := r.persistMappingSnapshot(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewVolumeReader(binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Stop()
	if err := reloaded.SetMappingSnapshotPath(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.ReadResidentRange(3, 0, make([]byte, 4096), residentWant(4096)); err != nil {
		t.Fatalf("reloaded joint proof: %v", err)
	}
	if !token.Valid() {
		t.Fatal("snapshot persistence revoked source token")
	}
	legacy := filepath.Join(t.TempDir(), "legacy.gob")
	f, err := os.Create(legacy)
	if err != nil {
		t.Fatal(err)
	}
	old := mappingSnapshot{Version: mappingVersion, Binding: binding, CacheDir: r.cacheDir,
		Entries: map[string][]SliceInfo{"3_0": {{SliceID: 77, Size: 4096, Len: 4096}}}}
	if err := gob.NewEncoder(f).Encode(old); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	legacyReader, err := NewVolumeReader(binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyReader.Stop()
	if err := legacyReader.SetMappingSnapshotPath(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyReader.ReadResidentRange(3, 0, make([]byte, 4096), residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("legacy snapshot minted EOF: %v", err)
	}
	if n, err := legacyReader.ReadBlock(context.Background(), 3, 0, make([]byte, 4096)); err != nil || n != 4096 {
		t.Fatalf("legacy byte serving regressed: %d %v", n, err)
	}
}

func TestResidentCrossChunkGrowthRevokesOldTail(t *testing.T) {
	r, rdb, _ := residentReaderFixture(t)
	token := residentWarm(t, r)
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 88)
	binary.BigEndian.PutUint32(record[12:16], 4096)
	binary.BigEndian.PutUint32(record[20:24], 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe := rdb.Pipeline()
	pipe.Set(ctx, "i3", residentTestAttr(ChunkSize+4096), 0)
	pipe.RPush(ctx, "c3_1", string(record[:]))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.getSlices(ctx, 3, 1); err != nil {
		t.Fatal(err)
	}
	if token.Valid() {
		t.Fatal("old tail token survived newer attr on another chunk")
	}
	if _, err := r.ReadResidentRange(3, 0, make([]byte, 4096), residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("old tail still terminal: %v", err)
	}
	if _, err := r.ResidentEOF(3, residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("old EOF still terminal: %v", err)
	}
}

func TestResidentRevalidationCannotPublishAfterNewFetch(t *testing.T) {
	r, rdb, _ := residentReaderFixture(t)
	old := residentWarm(t, r)
	entered, release := make(chan struct{}), make(chan struct{})
	r.testBeforeRevalidatePublish = func() { close(entered); <-release }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	var releaseOnce sync.Once
	joined := false
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		if !joined {
			select {
			case <-result:
				joined = true
			case <-time.After(5 * time.Second):
				t.Error("held revalidation worker did not join")
			}
		}
	})
	go func() { _, _, err := r.RevalidateMappings(ctx); result <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := rdb.Set(ctx, "i3", residentTestAttr(8192), 0).Err(); err != nil {
		t.Fatal(err)
	}
	r.InvalidateSliceCache(3)
	if _, err := r.getSlices(ctx, 3, 0); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if old.Valid() {
		t.Fatal("old token survived explicit invalidation")
	}
	if got := r.inodeAttr[3]; got != residentWant(8192) {
		t.Fatalf("stale revalidation overwrote newer attr: %+v", got)
	}
}

func TestResidentMissingMappingNeverDialsRedis(t *testing.T) {
	source, _, binding := residentReaderFixture(t)
	residentWarm(t, source)
	var dials atomic.Int32
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", Dialer: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected Redis dial")
	}})
	defer client.Close()
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if _, err := r.ReadResidentRange(3, 0, make([]byte, 4096), residentWant(4096)); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("missing range: %v", err)
	}
	source.sliceMu.RLock()
	r.sliceMu.Lock()
	r.sliceCache["3_0"] = append([]SliceInfo(nil), source.sliceCache["3_0"]...)
	r.attrWitness["3_0"] = source.attrWitness["3_0"]
	r.inodeAttr[3] = source.inodeAttr[3]
	r.sliceMu.Unlock()
	source.sliceMu.RUnlock()
	body := make([]byte, 4096)
	token, err := r.ReadResidentRange(3, 0, body, residentWant(4096))
	if err != nil || !token.Valid() || !bytes.Equal(body, bytes.Repeat([]byte{'B'}, 4096)) {
		t.Fatalf("resident range with refused Redis: %v", err)
	}
	if token, err := r.ResidentEOF(3, residentWant(4096)); err != nil || !token.Valid() {
		t.Fatalf("resident EOF with refused Redis: %v", err)
	}
	if dials.Load() != 0 {
		t.Fatalf("resident read dialed Redis %d times", dials.Load())
	}
}
