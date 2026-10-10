//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
)

// This qualifies the durable resident-byte prerequisite, not local settlement:
// the READY row, private image and old mirror remain under replacement custody.
func TestReplacementPublishedSDKCacheSurvivesReaderReopen(t *testing.T) {
	body := bytes.Repeat([]byte("sdk-resident-save-"), 550003)
	x, spool, d, row, witness, _ := replacementNestedFixtureBody(t, body)
	fs, _ := replacementProxyAndFSConfig(t, x, spool, d, func(c *chunk.Config) {
		chunks, err := cache.VolumeCacheDir(x.volumeUUID)
		if err != nil {
			t.Fatal(err)
		}
		c.CacheDir = filepath.Dir(filepath.Dir(chunks))
		var disk syscall.Statfs_t
		if err := syscall.Statfs(c.CacheDir, &disk); err != nil {
			t.Fatal(err)
		}
		if uint64(disk.Bavail)*uint64(disk.Bsize) < 128<<20 {
			t.Fatal("owned SDK cache fixture needs 128 MiB available")
		}
		// This small isolated fixture does not require the SDK production
		// default's ten percent of the entire host disk as free headroom.
		c.FreeSpace = 0.01
		c.CacheSize, c.CacheMode = 64<<20, 0600
		c.AutoCreate, c.CacheLargeWrite, c.CacheFullBlock = true, true, true
		c.CacheChecksum, c.CacheEviction = chunk.CsExtend, "none"
	})
	usedBefore, _ := spool.Capacity()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	factory := func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return &replacementSDKIO{fs: fs, m: fs.Meta(), w: w, ctx: jfsmeta.Background(), parent: 4}, func() {}, nil
	}
	bindingCheck := func(b metadata.CachePathBinding) error {
		if b != witness.Binding {
			return metadata.ErrMirrorConflict
		}
		return nil
	}
	if err := d.publishReplacement(ctx, row.ID, factory, bindingCheck); err != nil {
		t.Fatal(err)
	}
	current, err := spool.Meta().Get(row.ID)
	if err != nil || current.DrainState != metadata.DrainReady {
		t.Fatalf("READY custody: %+v %v", current, err)
	}
	journal, err := spool.Meta().ReplacementPublication(current)
	if err != nil || journal == nil || journal.Phase != metadata.ReplacementPublicationCommitted {
		t.Fatalf("journal=%+v err=%v", journal, err)
	}
	receipt, err := replaceguard.Decode([]byte(journal.Record))
	if err != nil || receipt.Stage.Inode == 0 || receipt.Stage.Inode == receipt.Destination.Inode {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	inode := receipt.Stage.Inode
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatalf("requalify fixture after offline acceptance: %v", err)
	}
	r := x.session.Reader()
	r.InvalidateSliceCache(inode)
	for i := int64(0); i <= (int64(len(body))-1)/cache.ChunkSize; i++ {
		if err := r.PrepareChunkMapping(ctx, inode, i); err != nil {
			t.Fatal(err)
		}
	}
	attr := cache.ResidentAttr{Size: int64(len(body)), MtimeNS: receipt.Stage.MtimeNS, Valid: true}
	// SDK disk-cache writes are asynchronous. Wait only for the exact local
	// byte image; no retrying publication, metadata qualification or settlement.
	got := make([]byte, len(body))
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		token, readErr := r.ReadResidentRange(inode, 0, got, attr)
		if readErr == nil && token.Valid() && bytes.Equal(got, body) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatalf("SDK did not populate verified disk cache: %v", readErr)
		case <-ticker.C:
		}
	}
	graph, err := r.FlushResidentImage(ctx, inode, attr, sha256.Sum256(body))
	if err != nil || !graph.Token.Valid() {
		t.Fatalf("SDK resident flush: %v", err)
	}
	binding, ok := r.VolumeBinding()
	if !ok {
		t.Fatal("reader lost binding")
	}
	// A distinct Reader with no Redis client loads the exact flushed snapshot
	// before the old reader can write another snapshot during Stop.
	fresh, err := cache.NewVolumeReader(binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fresh.Stop)
	if err := fresh.SetMappingSnapshotPath(r.MappingSnapshotPath(x.state)); err != nil {
		t.Fatal(err)
	}
	clear(got)
	token, err := fresh.ReadResidentRange(inode, 0, got, attr)
	if err != nil || !token.Valid() || !bytes.Equal(got, body) {
		t.Fatalf("fresh no-Redis image: %v", err)
	}
	if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("private source changed: %v", err)
	}
	if usedAfter, _ := spool.Capacity(); usedAfter != usedBefore {
		t.Fatal("cache proof released private capacity")
	}
	after, err := spool.Meta().Get(row.ID)
	if err != nil || after.DrainState != metadata.DrainReady || after.SpoolFile != row.SpoolFile {
		t.Fatalf("cache proof settled row: %+v %v", after, err)
	}
}
