//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/slicegraph"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

const replacementOriginalSize = int64(cache.ChunkSize + 12)

// The remote file has two observed chunks and a sparse middle. Its FUSE
// backing stays small: graph preparation transfers metadata, not 64 MiB.
func newOfflineReplacementFixture(t *testing.T, prepareSecondChunk bool) (*boundCacheFixture, *SpoolStore, string) {
	t.Helper()
	x := newBoundCacheFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stamp := time.Unix(1700000000, 123)
	attr := (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(replacementOriginalSize), Mtime: 1700000000, Mtimensec: 123}).Marshal()
	var second [24]byte
	binary.BigEndian.PutUint64(second[4:12], 78)
	binary.BigEndian.PutUint32(second[12:16], 12)
	binary.BigEndian.PutUint32(second[20:24], 12)
	pipe := x.rdb.Pipeline()
	pipe.Set(ctx, "i3", attr, 0)
	pipe.RPush(ctx, "c3_1", string(second[:]))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	// Match the remote logical length with a sparse local backing file. This
	// costs only a small inode extent and detects accidental FUSE replacement.
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	f, err := os.OpenFile(backing, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(replacementOriginalSize); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(backing, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	entry := metadata.MakeEntry("media/clip.mov", false, replacementOriginalSize, stamp, 3)
	entry.MtimeNsecKnown = true
	if err := x.store.Insert(entry); err != nil {
		t.Fatal(err)
	}
	cf := openBoundCacheFile(t, x)
	warm := make([]byte, len(x.want))
	if n, err := cf.ReadAt(warm, 0); err != nil || n != len(warm) || !bytes.Equal(warm, x.want) || cf.fuseFD != nil {
		t.Fatalf("original qualified SSD read n=%d err=%v FUSE=%v", n, err, cf.fuseFD != nil)
	}
	if err := cf.Close(); err != nil {
		t.Fatal(err)
	}
	reader := x.session.Reader()
	if reader == nil {
		t.Fatal("bound reader missing")
	}
	if prepareSecondChunk {
		if err := reader.PrepareChunkMapping(ctx, 3, 1); err != nil {
			t.Fatalf("prepare second original chunk: %v", err)
		}
		export, err := reader.ExportContentGraph(3, cache.ResidentAttr{Size: replacementOriginalSize, MtimeNS: stamp.UnixNano(), Valid: true})
		if err != nil || len(export.Graph().Chunks) != 2 || export.Graph().Length != uint64(replacementOriginalSize) {
			t.Fatalf("incomplete original graph: %+v err=%v", export.Graph(), err)
		}
	}
	binding, ok := reader.VolumeBinding()
	if !ok {
		t.Fatal("bound volume identity missing")
	}
	identity, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || !x.paths.CachePathEligible(metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}, identity) {
		t.Fatal("original path was not qualified")
	}
	if err := metadata.InitSpoolSchema(x.store.DB()); err != nil {
		t.Fatal(err)
	}
	spoolRoot := t.TempDir()
	spool, err := NewSpoolStore(spoolRoot, 0, metadata.NewSpoolStore(x.store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	// Construct without Start, as production does before recovery. This installs
	// the local absence/base probe; no dispatcher or remote writes run.
	if _, err := NewDrainer(spool, DrainerConfig{FuseRoot: x.fuseRoot}); err != nil {
		t.Fatal(err)
	}
	x.jfs.handler.SetSpool(spool, nil)
	return x, spool, spoolRoot
}

func assertOfflineOriginalBacking(t *testing.T, name string) {
	t.Helper()
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() != replacementOriginalSize {
		t.Fatalf("original backing size %v: %v", info, err)
	}
	first := make([]byte, 65536)
	if n, err := f.ReadAt(first, 0); err != nil || n != len(first) || !bytes.Equal(first, bytes.Repeat([]byte{'F'}, len(first))) {
		t.Fatalf("original backing head changed: n=%d err=%v", n, err)
	}
	tail := make([]byte, 12)
	if n, err := f.ReadAt(tail, cache.ChunkSize); err != nil || n != len(tail) || !bytes.Equal(tail, make([]byte, len(tail))) {
		t.Fatalf("original backing sparse tail changed: n=%d err=%v", n, err)
	}
}

func assertOfflineReplacementRead(t *testing.T, jfs *juiceFS, want []byte) {
	t.Helper()
	f, err := jfs.OpenFile("media/clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, len(want))
	n, err := f.ReadAt(got, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("replacement read: %v", err)
	}
	if n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("replacement read n=%d, want=%d", n, len(want))
	}
}

func assertOfflineReplacementRow(t *testing.T, spool *SpoolStore, want []byte) (*metadata.SpoolRow, *metadata.SpoolReplacementWitness) {
	t.Helper()
	row, err := spool.Meta().LookupByPath("media/clip.mov")
	if err != nil || row.DrainState != metadata.DrainReady || row.Size != int64(len(want)) {
		t.Fatalf("accepted row %+v: %v", row, err)
	}
	hash := sha256.Sum256(want)
	if !bytes.Equal(row.SHA256, hash[:]) || row.PunchedEnd != 0 {
		t.Fatalf("accepted image metadata %+v", row)
	}
	w, err := spool.Meta().ReplacementWitness(row)
	if err != nil || w == nil || w.Image.Size != row.Size || !bytes.Equal(w.Image.SHA256, hash[:]) ||
		w.Destination.Path != row.NFSPath || w.SourceFile != row.SpoolFile {
		t.Fatalf("accepted witness %+v: %v", w, err)
	}
	publication, err := spool.Meta().ReplacementPublication(row)
	if err != nil || publication == nil || publication.SourceRowID != row.ID || publication.OperationID != w.OperationID ||
		publication.SourceFile != row.SpoolFile || publication.SourceIdentity != w.SourceIdentity {
		t.Fatalf("accepted publication %+v: %v", publication, err)
	}
	return row, w
}

func TestOfflineReplacementWritingAndSecondSaveRestart(t *testing.T) {
	x, spool, spoolRoot := newOfflineReplacementFixture(t, true)
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	assertOfflineOriginalBacking(t, backing)
	first := []byte("first fully saved offline image")
	const firstTemp = "media/.clip-first.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, firstTemp, first, len(first))
	writing, err := spool.Meta().LookupByPath(firstTemp)
	if err != nil || writing.DrainState != metadata.DrainWriting {
		t.Fatalf("NFS CLOSE must leave writing row: %+v %v", writing, err)
	}
	base, _, err := spool.Meta().DrainBase(writing.ID)
	if err != nil || base.Kind != metadata.SpoolBaseAbsent {
		t.Fatalf("temporary source was not captured absent: %+v %v", base, err)
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "setting", "d2", "i3", "c3_0", "c3_1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := x.jfs.Rename(firstTemp, "media/clip.mov"); err != nil {
		t.Fatalf("first offline replacement rename: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(x.fuseRoot, firstTemp)); !os.IsNotExist(err) {
		t.Fatalf("local temp unexpectedly reached FUSE: %v", err)
	}
	assertOfflineReplacementRead(t, x.jfs, first)
	firstRow, firstWitness := assertOfflineReplacementRow(t, spool, first)
	if f, err := x.jfs.OpenFile("media/clip.mov", os.O_RDWR, 0); err == nil {
		_ = f.Close()
		t.Fatal("sealed replacement reopened for in-place write")
	}
	accepted, _ := spool.LookupActive("media/clip.mov")
	if accepted == nil {
		t.Fatal("accepted read owner missing")
	}
	if err := accepted.CloseAndDelete("late caller"); err == nil {
		t.Fatal("late cleanup discarded accepted read owner")
	}
	if done, err := spool.MarkDrainComplete(firstRow.ID, firstRow.NFSPath, firstRow.SpoolFile, firstRow.Size); done || err == nil {
		t.Fatalf("late completion discarded accepted image: %v %v", done, err)
	}
	assertOfflineReplacementRead(t, x.jfs, first)
	if claimed, err := spool.Meta().MarkDraining(firstRow.ID); claimed || err == nil {
		t.Fatalf("accepted image entered generic drain: %v %v", claimed, err)
	}

	if firstRow.ID != writing.ID || firstWitness.PredecessorRowID != 0 {
		t.Fatalf("first source binding row=%+v witness=%+v", firstRow, firstWitness)
	}
	graph, err := slicegraph.Decode(firstWitness.Graph)
	if err != nil || graph.Length != uint64(replacementOriginalSize) || len(graph.Chunks) != 2 || firstWitness.Image.Size == int64(graph.Length) {
		t.Fatalf("original graph was replaced with new-image length: %+v err=%v", graph, err)
	}
	assertOfflineOriginalBacking(t, backing)
	if got, err := os.ReadFile(firstRow.SpoolFile); err != nil || !bytes.Equal(got, first) {
		t.Fatalf("first private image changed: %v", err)
	}
	if err := x.jfs.Rename("media/clip.mov", "media/elsewhere.mov"); err == nil {
		t.Fatal("accepted local carrier moved by unsupported ordinary rename")
	}
	assertOfflineReplacementRead(t, x.jfs, first)

	second := []byte("second and longer fully saved offline image")
	const secondTemp = "media/.clip-second.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, secondTemp, second, len(second))
	if err := x.jfs.Rename(secondTemp, "media/clip.mov"); err != nil {
		t.Fatalf("second offline replacement rename: %v", err)
	}
	assertOfflineReplacementRead(t, x.jfs, second)
	secondRow, secondWitness := assertOfflineReplacementRow(t, spool, second)
	prior, err := spool.Meta().Get(firstRow.ID)
	if err != nil || prior.RetiredBy != secondRow.ID || secondWitness.PredecessorRowID != firstRow.ID ||
		!reflect.DeepEqual(firstWitness.Destination, secondWitness.Destination) ||
		firstWitness.Binding != secondWitness.Binding || !bytes.Equal(firstWitness.Graph, secondWitness.Graph) ||
		firstWitness.GraphDigest != secondWitness.GraphDigest || firstWitness.AcceptedEpoch != secondWitness.AcceptedEpoch {
		t.Fatalf("second save lost original proof: prior=%+v witness=%+v err=%v", prior, secondWitness, err)
	}
	assertOfflineOriginalBacking(t, backing)
	if got, err := os.ReadFile(firstRow.SpoolFile); err != nil || !bytes.Equal(got, first) {
		t.Fatalf("retired first private image changed: %v", err)
	}
	if got, err := os.ReadFile(secondRow.SpoolFile); err != nil || !bytes.Equal(got, second) {
		t.Fatalf("second private image changed: %v", err)
	}

	if !x.jfs.handler.StopHandler() {
		t.Fatal("first handler did not stop")
	}
	x.session.Stop()
	x.rc.Stop()
	if err := x.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.Open(x.db)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := metadata.InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	recoveredSpool, err := NewSpoolStore(spoolRoot, 0, metadata.NewSpoolStore(reopened.DB()))
	if err != nil {
		t.Fatal(err)
	}
	reopened.SetSpoolStore(recoveredSpool.Meta())
	if _, err := recoveredSpool.RecoverOnBoot(context.Background()); err != nil {
		t.Fatalf("recover accepted spool rows: %v", err)
	}
	recoveredHandler := NewHandler(reopened, x.fuseRoot)
	defer recoveredHandler.StopHandler()
	recoveredHandler.SetSpool(recoveredSpool, nil)
	reopenedJFS := &juiceFS{handler: recoveredHandler}
	assertOfflineReplacementRead(t, reopenedJFS, second)
	reopenedRow, reopenedWitness := assertOfflineReplacementRow(t, recoveredSpool, second)
	mirror := reopened.LookupByPath("media/clip.mov")
	if reopenedRow.ID != secondRow.ID || !reflect.DeepEqual(reopenedWitness, secondWitness) ||
		mirror == nil || mirror.Size != int64(len(second)) {
		t.Fatalf("recovered accepted image row=%+v witness=%+v mirror=%+v", reopenedRow, reopenedWitness, mirror)
	}
}

func TestOfflineReplacementRefusesMissingOriginalGraph(t *testing.T) {
	x, spool, _ := newOfflineReplacementFixture(t, false)
	const temp = "media/.clip-unmapped.tmp"
	body := []byte("saved but original graph incomplete")
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	before, err := spool.Meta().LookupByPath(temp)
	if err != nil {
		t.Fatal(err)
	}
	original := x.store.LookupByPath("media/clip.mov")
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "c3_1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := x.jfs.Rename(temp, "media/clip.mov"); err == nil {
		t.Fatal("accepted incomplete original graph")
	}
	after, err := spool.Meta().LookupByPath(temp)
	if err != nil || after.ID != before.ID || after.NFSPath != temp || after.DrainState != metadata.DrainWriting {
		t.Fatalf("refusal changed source row: %+v %v", after, err)
	}
	if got := x.store.LookupByPath("media/clip.mov"); got == nil || got.Version != original.Version || got.Size != original.Size {
		t.Fatalf("refusal changed original mirror: %+v", got)
	}
	if got, err := os.ReadFile(before.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("refusal changed saved private image: %v", err)
	}
}

func TestOfflineReplacementRefusesStaleParentAndPunchedSource(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *boundCacheFixture, *SpoolStore, *metadata.SpoolRow)
	}{
		{"stale-parent", func(t *testing.T, x *boundCacheFixture, _ *SpoolStore, _ *metadata.SpoolRow) {
			if _, err := x.store.DB().Exec("UPDATE entries SET revision=revision+1 WHERE path='media'"); err != nil {
				t.Fatal(err)
			}
		}},
		{"punched-source", func(t *testing.T, _ *boundCacheFixture, spool *SpoolStore, row *metadata.SpoolRow) {
			if err := spool.Meta().SetPunchedEnd(row.ID, 1); err != nil {
				t.Fatal(err)
			}
		}},
		{"private-path-substitution", func(t *testing.T, _ *boundCacheFixture, _ *SpoolStore, row *metadata.SpoolRow) {
			body, err := os.ReadFile(row.SpoolFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(row.SpoolFile, row.SpoolFile+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(row.SpoolFile, body, 0600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x, spool, _ := newOfflineReplacementFixture(t, true)
			const temp = "media/.clip-refused.tmp"
			body := []byte("private bytes survive a refused rename")
			simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
			before, err := spool.Meta().LookupByPath(temp)
			if err != nil {
				t.Fatal(err)
			}
			destination := x.store.LookupByPath("media/clip.mov")
			tc.mutate(t, x, spool, before)
			x.session.SetOnline(false)
			pin.SetOffline(true)
			if err := x.jfs.Rename(temp, "media/clip.mov"); err == nil {
				t.Fatal("accepted a stale or punched source")
			}
			after, err := spool.Meta().LookupByPath(temp)
			if err != nil || after.ID != before.ID || after.NFSPath != temp || after.DrainState != metadata.DrainWriting {
				t.Fatalf("refusal changed source row: %+v %v", after, err)
			}
			if got := x.store.LookupByPath("media/clip.mov"); got == nil || got.Version != destination.Version || got.Size != destination.Size {
				t.Fatalf("refusal changed destination mirror: %+v", got)
			}
			if got, err := os.ReadFile(before.SpoolFile); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("refusal changed private image: %v", err)
			}
		})
	}
}

func TestOfflineReplacementRefusesCancelledTemp(t *testing.T) {
	x, spool, _ := newOfflineReplacementFixture(t, true)
	const temp = "media/.clip-cancelled.tmp"
	body := []byte("cancelled temporary image")
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	before := x.store.LookupByPath("media/clip.mov")
	spool.CancelForDelete(temp)
	if _, ok := spool.LookupActive(temp); ok {
		t.Fatal("cancelled temp still indexed")
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.jfs.Rename(temp, "media/clip.mov"); err == nil {
		t.Fatal("accepted a cancelled temp without private bytes")
	}
	if got := x.store.LookupByPath("media/clip.mov"); got == nil || got.Version != before.Version || got.Size != before.Size {
		t.Fatalf("cancelled temp changed destination mirror: %+v", got)
	}
	assertOfflineOriginalBacking(t, filepath.Join(x.fuseRoot, "media/clip.mov"))
}

func TestOfflineReplacementNeverStartsStreamer(t *testing.T) {
	x, spool, _ := newOfflineReplacementFixture(t, true)
	oldMin, oldMargin, oldHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = 8, 1, 1
	t.Cleanup(func() { spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = oldMin, oldMargin, oldHead })
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	body := []byte("stream eligible sequential offline save")
	const temp = "media/.stream-save.tmp"
	simulateNFSCreateThenWrite(t, x.jfs, temp, body, len(body))
	e, _ := spool.LookupActive(temp)
	if why := streamIneligible(e); why != "" {
		t.Fatalf("fixture was not streaming eligible: %s", why)
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	// A currently admitted streamer must cause a bounded refusal, without
	// waiting for its potentially slow remote I/O or accepting a moving image.
	e.streamAdmission.Lock()
	err := x.jfs.Rename(temp, "media/clip.mov")
	e.streamAdmission.Unlock()
	if err == nil {
		t.Fatal("accepted save while streamer held source admission")
	}
	if err := x.jfs.Rename(temp, "media/clip.mov"); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "must-remain-absent")
	d := &Drainer{spool: spool, fuseRoot: root, streamer: &streamer{sessions: map[int64]*streamSession{}}}
	d.streamOnce()
	if len(d.streamer.sessions) != 0 {
		t.Fatal("accepted carrier created a stream session")
	}
	// Even a stale session entry cannot enter the closed-file finish branch.
	d.streamer.sessions[e.ID()] = &streamSession{}
	d.streamOnce()
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("streamer touched backing: %v", err)
	}
	if e.PunchedEnd() != 0 || e.StreamDestPath() != "" {
		t.Fatal("streamer mutated accepted image")
	}
	assertOfflineReplacementRead(t, x.jfs, body)
	assertOfflineOriginalBacking(t, filepath.Join(x.fuseRoot, "media/clip.mov"))
}

func TestOfflineReplacementReadyAndEmptyImages(t *testing.T) {
	for _, body := range [][]byte{[]byte("already finalized save"), {}} {
		name := "ready"
		if len(body) == 0 {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			x, spool, _ := newOfflineReplacementFixture(t, true)
			const temp = "media/.ready-save.tmp"
			simulateNFSCreateThenWrite(t, x.jfs, temp, body, max(1, len(body)))
			e, _ := spool.LookupActive(temp)
			if err := e.Finalize(); err != nil {
				t.Fatal(err)
			}
			row, err := spool.Meta().LookupByPath(temp)
			if err != nil || row.DrainState != metadata.DrainReady {
				t.Fatalf("source not ready: %+v %v", row, err)
			}
			x.session.SetOnline(false)
			pin.SetOffline(true)
			if err := x.jfs.Rename(temp, "media/clip.mov"); err != nil {
				t.Fatal(err)
			}
			assertOfflineReplacementRow(t, spool, body)
			assertOfflineReplacementRead(t, x.jfs, body)
			assertOfflineOriginalBacking(t, filepath.Join(x.fuseRoot, "media/clip.mov"))
		})
	}
}
