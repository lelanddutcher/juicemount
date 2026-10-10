//go:build darwin && h8_acceptance

package nfs

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A published stream must finish the same custody lifecycle as a whole-file drain.
func TestH8AcceptanceDefaultStreamRetiresPublishedEntry(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "") // Exercise the production default.
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = 1<<20, 64<<10, 64<<10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	spool, d := newTestDrainer(t, DrainerConfig{})
	d.streamer = &streamer{sessions: make(map[int64]*streamSession)}
	t.Cleanup(d.closeAllStreamSessions)
	const name = "DCIM/clip.mov"
	e, err := spool.OpenWrite(name)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 2<<20)
	for i := range want {
		want[i] = byte((i*31 + i/997) % 251)
	}
	if _, err := e.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}

	d.streamOnce() // Production streamer: stage, verify, persist, punch.
	if e.PunchedEnd() <= 0 {
		t.Fatalf("stream did not punch a prefix (eligibility=%q)", streamIneligible(e))
	}
	if err := e.Close(); err != nil { // Production finalize: MarkReady.
		t.Fatal(err)
	}
	ready, err := spool.Meta().Get(e.ID())
	if err != nil || ready.DrainState != metadata.DrainReady || ready.PunchedEnd <= 0 {
		t.Fatalf("finalized stream row = (%+v, %v), want punched ready row", ready, err)
	}

	d.streamOnce() // Production finishStreamSession: publish hidden sibling.
	backing := filepath.Join(d.fuseRoot, name)
	got, err := os.ReadFile(backing)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("published stream differs from source: bytes=%d err=%v", len(got), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx) // Exercise the production ListReady/MarkDraining dispatch path.

	row, err := spool.Meta().Get(e.ID())
	if err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if err == nil && row.DrainState != metadata.DrainDone {
		t.Errorf("published stream row remains %q; completion did not retire it", row.DrainState)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("published stream retains %d bytes of spool capacity", used)
	}
	if _, active := spool.LookupActive(name); active {
		t.Error("published stream remains indexed; subsequent opens still route through the old spool entry")
	}
	if _, err := os.Stat(e.SpoolFilePath()); !os.IsNotExist(err) {
		t.Errorf("published stream still holds its spool file: %v", err)
	}
	// The published path is the subsequent access target once the spool index retires.
	if got, err := os.ReadFile(backing); err != nil || !bytes.Equal(got, want) {
		t.Errorf("subsequent access to published file: bytes=%d err=%v", len(got), err)
	}
}

// A destination created after staging belongs to another writer. Publishing
// this stream must not replace that writer or lose the punched stream's copy.
func TestH8AcceptanceStreamPublishPreservesRemoteReplacement(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = 1<<20, 64<<10, 64<<10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	spool, d := newTestDrainer(t, DrainerConfig{})
	d.streamer = &streamer{sessions: make(map[int64]*streamSession)}
	t.Cleanup(d.closeAllStreamSessions)
	const name = "DCIM/conflict.mov"
	e, err := spool.OpenWrite(name)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 2<<20)
	for i := range want {
		want[i] = byte((i*17 + i/173) % 251)
	}
	if _, err := e.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	d.streamOnce() // The real streamer creates, verifies, and punches the stage.
	punched := e.PunchedEnd()
	stage := e.StreamDestPath()
	public := filepath.Join(d.fuseRoot, name)
	if punched <= 0 || stage == "" || stage == public {
		t.Fatalf("stream did not establish a punched private stage: punched=%d stage=%q", punched, stage)
	}
	if _, err := os.Stat(stage); err != nil {
		t.Fatalf("private stage missing before race: %v", err)
	}
	if _, err := os.Stat(public); !os.IsNotExist(err) {
		t.Fatalf("public path occupied before remote replacement: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// A different writer wins the public path after this stream started but
	// before its finish tick. Keep its inode and bytes as the public authority.
	replacement := []byte("newer remote writer owns this public path")
	if err := os.WriteFile(public, replacement, 0o644); err != nil {
		t.Fatal(err)
	}
	replacementInfo, err := os.Stat(public)
	if err != nil {
		t.Fatal(err)
	}
	d.streamOnce() // Production finishStreamSession calls finishStream.

	got, err := os.ReadFile(public)
	if err != nil || !bytes.Equal(got, replacement) {
		t.Errorf("stream publish replaced remote writer: public size=%d matches stream=%t err=%v", len(got), bytes.Equal(got, want), err)
	}
	publicInfo, err := os.Stat(public)
	if err != nil || !os.SameFile(replacementInfo, publicInfo) {
		t.Errorf("stream publish changed remote writer identity: stat err=%v", err)
	}
	stageAfter := e.StreamDestPath()
	if stageAfter == "" || stageAfter == public {
		t.Errorf("conflicted stream lost its private stage reference: %q", stageAfter)
	} else if staged, err := os.ReadFile(stageAfter); err != nil || int64(len(staged)) < punched || !bytes.Equal(staged[:punched], want[:punched]) {
		t.Errorf("conflicted stream lost its sole punched-prefix copy: stage=%q bytes=%d err=%v", stageAfter, len(staged), err)
	}
	if _, err := os.Stat(e.SpoolFilePath()); err != nil {
		t.Errorf("conflicted stream discarded its remaining spool source: %v", err)
	}
}

// Recovery must retain the stage that owns bytes already punched from source.
func TestH8AcceptanceBootSweepPreservesPunchedStage(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = 1<<20, 64<<10, 64<<10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})

	spool, d := newTestDrainer(t, DrainerConfig{})
	d.streamer = &streamer{sessions: make(map[int64]*streamSession)}
	t.Cleanup(d.closeAllStreamSessions)
	const name = "DCIM/interrupted.mov"
	e, err := spool.OpenWrite(name)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 2<<20)
	for i := range want {
		want[i] = byte((i*29 + i/311) % 251)
	}
	if _, err := e.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	d.streamOnce()
	punched := e.PunchedEnd()
	stage := e.StreamDestPath()
	public := filepath.Join(d.fuseRoot, name)
	if punched <= 0 || stage == "" || stage == public {
		t.Fatalf("stream did not establish a punched private stage: punched=%d stage=%q", punched, stage)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil || row.PunchedEnd != punched {
		t.Fatalf("durable punched boundary = (%+v, %v); want %d", row, err, punched)
	}
	if _, err := os.Stat(public); !os.IsNotExist(err) {
		t.Fatalf("public destination before publish: %v", err)
	}
	stagedBefore, err := os.ReadFile(stage)
	if err != nil || int64(len(stagedBefore)) < punched || !bytes.Equal(stagedBefore[:punched], want[:punched]) {
		t.Fatalf("stage lacks verified prefix before sweep: bytes=%d err=%v", len(stagedBefore), err)
	}
	source, err := os.ReadFile(e.SpoolFilePath())
	if err != nil || int64(len(source)) < punched {
		t.Fatalf("punched source unreadable before sweep: bytes=%d err=%v", len(source), err)
	}
	for i := int64(0); i < punched; i++ {
		if source[i] != 0 {
			t.Fatalf("source byte %d was not punched: %d", i, source[i])
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	d.closeAllStreamSessions() // Simulate the process losing its open stage handles.

	var pending *metadata.PendingPunchedSpool
	if err := d.SweepStreamPartials(); !errors.As(err, &pending) ||
		len(pending.Rows) != 1 || pending.Rows[0].ID != e.ID() {
		t.Fatalf("boot sweep = %v, want typed punched-row refusal", err)
	}
	staged, err := os.ReadFile(stage)
	if err != nil || int64(len(staged)) < punched || !bytes.Equal(staged[:punched], want[:punched]) {
		t.Fatalf("boot sweep discarded the sole punched-prefix copy: stage=%q bytes=%d err=%v", stage, len(staged), err)
	}
	if source, err := os.ReadFile(e.SpoolFilePath()); err != nil || int64(len(source)) < punched {
		t.Fatalf("boot sweep discarded punched source: bytes=%d err=%v", len(source), err)
	}
	if _, err := os.Stat(public); !os.IsNotExist(err) {
		t.Fatalf("boot sweep published or changed public path: %v", err)
	}
}
