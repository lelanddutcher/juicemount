package nfs

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

// #104 zero-tail detection tests.
//
// The signature under test: an interrupted preallocate-then-write download
// (Chrome→mount) finalizes a spool entry whose writtenEnd (declared/high-water
// size) exceeds the bytes actually written — the never-filled ranges drain to
// the backend as ZEROS in a full-size file, and Premiere black-frames it.
// Detection is finalize-time only, and NEVER blocks: the drain of a flagged
// entry must proceed byte-identically to an unflagged one.
//
// On (c) "sparse-but-legitimate": at finalize time a file an app INTENDED to
// be sparse (VM image, torrent preallocation) is byte-for-byte
// indistinguishable from an interrupted download — both are "declared size
// with never-written ranges". There is no signal in the write stream that
// distinguishes intent, so we do not try: the false-positive bound is that
// detection only counts/logs/marks (nothing is held, damaged, or slowed), the
// one systematic legitimate-sparse writer we have empirical evidence for
// (._ AppleDouble sidecars assembled by copyfile with seeks, task #100) is
// excluded by name, and the whole detector sits behind JM_ZERO_TAIL_DETECT.

// TestZeroTailHoleBytes unit-tests the pure hole math against the extent
// invariants advanceContiguousLocked maintains.
func TestZeroTailHoleBytes(t *testing.T) {
	cases := []struct {
		name       string
		written    int64
		contiguous int64
		exts       []spoolExtent
		want       int64
	}{
		{"fully contiguous", 1000, 1000, nil, 0},
		{"empty file", 0, 0, nil, 0},
		{"pure zero tail (preallocate, writes stopped)", 65536, 16384, nil, 49152},
		{"mid-file gap with landed extent above", 12288, 4096, []spoolExtent{{8192, 12288}}, 4096},
		{"two extents, two gaps", 20480, 4096, []spoolExtent{{8192, 12288}, {16384, 20480}}, 8192},
		{"extent clamped to writtenEnd (defensive)", 10000, 4096, []spoolExtent{{8192, 12288}}, 4096},
		{"extent below contiguous clamped (defensive)", 10000, 4096, []spoolExtent{{0, 8192}}, 10000 - 4096 - (8192 - 4096)},
	}
	for _, c := range cases {
		if got := zeroTailHoleBytes(c.written, c.contiguous, c.exts); got != c.want {
			t.Errorf("%s: zeroTailHoleBytes(%d, %d, %v) = %d, want %d",
				c.name, c.written, c.contiguous, c.exts, got, c.want)
		}
	}
}

// TestZeroTailCleanContiguousNotFlagged is scenario (a): a clean sequential
// write finalizes with contiguousEnd == writtenEnd and must NOT be flagged —
// no row marker, no per-entry status flag, response counter 0.
func TestZeroTailCleanContiguousNotFlagged(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/Movies/clean.mov")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xAA}, 8192), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail != "" {
		t.Fatalf("clean contiguous write flagged suspect_zero_tail=%q, want empty", row.SuspectZeroTail)
	}
	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("BuildSpoolStatus: %v", err)
	}
	if resp.SuspectZeroTail != 0 {
		t.Errorf("response suspect_zero_tail=%d, want 0", resp.SuspectZeroTail)
	}
	for _, v := range resp.Entries {
		if v.SuspectZeroTail {
			t.Errorf("entry %q flagged, want clean", v.Path)
		}
	}
}

// TestZeroTailInterruptedPreallocateFlagged is scenario (b), the task #104
// shape: the client pre-sets the size (SETATTR/ftruncate grow — Chrome
// preallocating the download), sequential writes stop early, the entry
// idle-finalizes. Must be flagged with exact hole accounting, the metrics
// counter must move, and the row detail JSON must carry
// {detected_at,size,contiguous,holes}.
func TestZeroTailInterruptedPreallocateFlagged(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	before := metrics.Default().Snapshot().ZeroTailSuspect

	e, err := s.OpenWrite("/Downloads/big-download.mp4")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	// Preallocate 64K (the declared size), write only the first 16K, "drop".
	if err := e.Truncate(65536); err != nil {
		t.Fatalf("Truncate grow: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xBB}, 16384), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close (finalize): %v", err)
	}

	if got := metrics.Default().Snapshot().ZeroTailSuspect; got < before+1 {
		t.Errorf("zero_tail_suspect_total = %d, want >= %d", got, before+1)
	}

	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail == "" {
		t.Fatalf("interrupted preallocate NOT flagged — suspect_zero_tail empty")
	}
	var det zeroTailDetail
	if err := json.Unmarshal([]byte(row.SuspectZeroTail), &det); err != nil {
		t.Fatalf("detail unmarshal %q: %v", row.SuspectZeroTail, err)
	}
	if det.Size != 65536 || det.Contiguous != 16384 || det.Holes != 49152 {
		t.Errorf("detail = %+v, want size=65536 contiguous=16384 holes=49152", det)
	}
	if det.Extents != 0 {
		t.Errorf("detail extents = %d, want 0 (pure tail, no out-of-order regions)", det.Extents)
	}
	if det.DetectedAt == "" {
		t.Errorf("detail detected_at empty")
	} else if _, perr := time.Parse(time.RFC3339Nano, det.DetectedAt); perr != nil {
		t.Errorf("detail detected_at %q not RFC3339Nano: %v", det.DetectedAt, perr)
	}
	// The row must still have finalized normally: ready with full size —
	// detection must not perturb the MarkReady lifecycle.
	if row.DrainState != metadata.DrainReady {
		t.Errorf("drain_state=%q, want ready (detection must not alter lifecycle)", row.DrainState)
	}
	if row.Size != 65536 {
		t.Errorf("row size=%d, want 65536", row.Size)
	}
}

// TestZeroTailOutOfOrderGapUsesExtentMath proves hole accounting EXCLUDES
// out-of-order regions that actually landed: [0,4K) + [8K,12K) leaves ONE 4K
// hole, not 8K — the recorded extent above the gap carries real bytes.
func TestZeroTailOutOfOrderGapUsesExtentMath(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/Downloads/oo-interrupted.bin")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x11}, 4096), 0); err != nil {
		t.Fatalf("w0: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x22}, 4096), 8192); err != nil {
		t.Fatalf("w8: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail == "" {
		t.Fatalf("out-of-order gap at finalize NOT flagged")
	}
	var det zeroTailDetail
	if err := json.Unmarshal([]byte(row.SuspectZeroTail), &det); err != nil {
		t.Fatalf("detail unmarshal: %v", err)
	}
	if det.Holes != 4096 {
		t.Errorf("holes = %d, want 4096 (the landed [8K,12K) extent must not count as hole)", det.Holes)
	}
	if det.Extents != 1 {
		t.Errorf("extents = %d, want 1", det.Extents)
	}
	if det.Size != 12288 || det.Contiguous != 4096 {
		t.Errorf("detail = %+v, want size=12288 contiguous=4096", det)
	}
}

// TestZeroTailAppleDoubleNotFlagged is the distinguishable slice of scenario
// (c): ._ AppleDouble sidecars are assembled sparsely by copyfile as NORMAL
// behavior (#100), so they are excluded by name — a hole-y ._ finalize must
// not flag, count, or mark.
func TestZeroTailAppleDoubleNotFlagged(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/Movies/._clip.mov")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	// Sparse assembly: header at 0, xattr blob far above, gap never filled.
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x11}, 512), 0); err != nil {
		t.Fatalf("w0: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0x22}, 512), 4096); err != nil {
		t.Fatalf("w4k: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail != "" {
		t.Fatalf("._ AppleDouble flagged suspect_zero_tail=%q — #100 sparse sidecars are expected, not suspects", row.SuspectZeroTail)
	}
}

// TestZeroTailKillSwitch proves JM_ZERO_TAIL_DETECT=0 disables everything:
// same interrupted-preallocate shape as the flagged test, but no row marker
// and no counter movement.
func TestZeroTailKillSwitch(t *testing.T) {
	t.Setenv("JM_ZERO_TAIL_DETECT", "0")
	s := newTestSpoolStore(t, 0)
	before := metrics.Default().Snapshot().ZeroTailSuspect

	e, err := s.OpenWrite("/Downloads/killswitch.mp4")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if err := e.Truncate(65536); err != nil {
		t.Fatalf("Truncate grow: %v", err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xCC}, 16384), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail != "" {
		t.Fatalf("JM_ZERO_TAIL_DETECT=0 but row flagged: %q", row.SuspectZeroTail)
	}
	if got := metrics.Default().Snapshot().ZeroTailSuspect; got != before {
		t.Errorf("counter moved under kill switch: %d -> %d", before, got)
	}
}

// TestZeroTailFlagSurvivesToStatusJSON is scenario (d): the finalize-time
// marker must surface through BuildSpoolStatus — response-level count,
// per-entry flag, detail passthrough — and serialize under the documented
// JSON keys.
func TestZeroTailFlagSurvivesToStatusJSON(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	// One suspect, one clean.
	sus, err := s.OpenWrite("/Downloads/suspect.mp4")
	if err != nil {
		t.Fatalf("OpenWrite suspect: %v", err)
	}
	if err := sus.Truncate(32768); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := sus.WriteAt(bytes.Repeat([]byte{0xDD}, 8192), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := sus.Close(); err != nil {
		t.Fatalf("Close suspect: %v", err)
	}
	clean, err := s.OpenWrite("/Downloads/clean.mp4")
	if err != nil {
		t.Fatalf("OpenWrite clean: %v", err)
	}
	if _, err := clean.WriteAt(bytes.Repeat([]byte{0xEE}, 8192), 0); err != nil {
		t.Fatalf("WriteAt clean: %v", err)
	}
	if err := clean.Close(); err != nil {
		t.Fatalf("Close clean: %v", err)
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("BuildSpoolStatus: %v", err)
	}
	if resp.SuspectZeroTail != 1 {
		t.Errorf("response suspect_zero_tail=%d, want 1", resp.SuspectZeroTail)
	}
	var flagged, cleanSeen bool
	for _, v := range resp.Entries {
		switch v.Path {
		case "/Downloads/suspect.mp4":
			flagged = v.SuspectZeroTail
			if v.SuspectZeroTailDetail == "" {
				t.Errorf("suspect entry missing detail passthrough")
			} else {
				var det zeroTailDetail
				if err := json.Unmarshal([]byte(v.SuspectZeroTailDetail), &det); err != nil {
					t.Errorf("entry detail not valid JSON: %v", err)
				} else if det.Holes != 32768-8192 {
					t.Errorf("entry detail holes=%d, want %d", det.Holes, 32768-8192)
				}
			}
		case "/Downloads/clean.mp4":
			cleanSeen = true
			if v.SuspectZeroTail {
				t.Errorf("clean entry flagged")
			}
			if v.SuspectZeroTailDetail != "" {
				t.Errorf("clean entry has detail %q, want omitted", v.SuspectZeroTailDetail)
			}
		}
	}
	if !flagged {
		t.Errorf("suspect entry not flagged in status entries")
	}
	if !cleanSeen {
		t.Errorf("clean entry missing from status entries")
	}

	// Wire-format check: the documented keys must appear in the serialized
	// payload the menu bar / Manager UI consumes.
	blob, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	js := string(blob)
	for _, key := range []string{
		`"suspect_zero_tail":1`,         // response-level count
		`"suspect_zero_tail":true`,      // per-entry flag
		`"suspect_zero_tail_detail":"{`, // detail passthrough (JSON-in-string)
	} {
		if !strings.Contains(js, key) {
			t.Errorf("status JSON missing %s in: %s", key, js)
		}
	}
	// The clean entry must serialize `"suspect_zero_tail":false` WITHOUT a
	// detail key (omitempty) — key-absent decodes fine in Swift, JSON null
	// would not.
	if strings.Count(js, `"suspect_zero_tail_detail"`) != 1 {
		t.Errorf("detail key should appear exactly once (suspect entry only): %s", js)
	}
}

// TestZeroTailFlaggedEntryStillDrains is the never-blocking law: a flagged
// entry drains EXACTLY like a clean one — row reaches done, the backend file
// exists at FULL declared size with the written prefix intact and the hole
// as zeros, and nothing quarantines or holds it.
func TestZeroTailFlaggedEntryStillDrains(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})

	e, err := spool.OpenWrite("/Films/interrupted-download.mov")
	if err != nil {
		t.Fatalf("OpenWrite: %v", err)
	}
	if err := e.Truncate(65536); err != nil {
		t.Fatalf("Truncate grow: %v", err)
	}
	prefix := bytes.Repeat([]byte{0x7E}, 16384)
	if _, err := e.WriteAt(prefix, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row: %v", err)
	}
	if row.SuspectZeroTail == "" {
		t.Fatalf("precondition failed: entry not flagged")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 1 {
		t.Fatalf("expected 1 row drained, got %d", n)
	}

	row, err = spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("Get row post-drain: %v", err)
	}
	if row.DrainState != metadata.DrainDone {
		t.Fatalf("drain_state=%q (last_error=%q), want done — the flag must NEVER hold user bytes", row.DrainState, row.LastError)
	}
	if row.SuspectZeroTail == "" {
		t.Errorf("marker lost across drain — surfacing should survive to the done row")
	}

	dest := filepath.Join(d.fuseRoot, "Films/interrupted-download.mov")
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read drained dest: %v", err)
	}
	if len(got) != 65536 {
		t.Fatalf("drained size=%d, want 65536 (full declared size — bytes unchanged)", len(got))
	}
	if !bytes.Equal(got[:16384], prefix) {
		t.Errorf("written prefix corrupted in drained file")
	}
	if !bytes.Equal(got[16384:], make([]byte, 65536-16384)) {
		t.Errorf("hole did not drain as zeros")
	}
}
