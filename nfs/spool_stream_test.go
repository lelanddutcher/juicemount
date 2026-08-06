package nfs

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// recordingDest logs the sequence of destination operations so a test can assert
// ORDER, not just final content. The bugs this file exists to prevent are
// sequencing bugs — they leave correct bytes behind and corrupt only on a crash
// or a concurrent read, so a test that checks bytes alone cannot see them.
type recordingDest struct {
	data      map[int64][]byte
	events    *[]string
	failWrite error
	failSync  error
}

func (d *recordingDest) WriteAt(p []byte, off int64) (int, error) {
	*d.events = append(*d.events, fmt.Sprintf("write@%d:%d", off, len(p)))
	if d.failWrite != nil {
		return 0, d.failWrite
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	d.data[off] = cp
	return len(p), nil
}

func (d *recordingDest) Sync() error {
	*d.events = append(*d.events, "sync")
	return d.failSync
}

type recordingReleaser struct {
	events *[]string
	total  int64
}

func (r *recordingReleaser) releaseCapacity(delta int64) {
	*r.events = append(*r.events, fmt.Sprintf("release:%d", delta))
	r.total += delta
}

// streamFixture builds a spool entry with `size` real bytes on disk.
func streamFixture(t *testing.T, size int) (*SpoolEntry, *os.File, *[]string) {
	t.Helper()
	s := newTestSpoolStore(t, 1<<30)
	e, err := s.OpenWrite("/clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if _, err := e.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	// O_RDWR, NOT OpenForRead: punching requires write access (F_PUNCHHOLE
	// returns EPERM on a read-only fd). OpenForRead hands back a read-only
	// handle, and using it here is the mistake this fixture exists to avoid.
	src, err := os.OpenFile(e.SpoolFilePath(), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	events := []string{}
	return e, src, &events
}

// THE ORDERING CONTRACT. Every adjacent pair below has an unrecoverable failure
// that the order exists to prevent:
//
//	sync before publish  — else a crash loses bytes we already punched
//	publish before punch — else a live reader is routed at a hole, silently
//	punch before release — else capacity is freed the disk still holds
func TestAdvanceStreamOrdersSyncPublishPunchRelease(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}

	n, err := advanceStream(e, src, dest, rel, 128<<10)
	if err != nil {
		t.Fatalf("advanceStream: %v", err)
	}
	if n <= 0 {
		t.Fatalf("reclaimed %d bytes, want > 0", n)
	}
	if got := e.PunchedEnd(); got != 128<<10 {
		t.Fatalf("punchedEnd = %d, want %d", got, 128<<10)
	}

	seq := strings.Join(*events, ",")
	iWrite := indexOfPrefix(*events, "write@")
	iSync := indexOf(*events, "sync")
	iRelease := indexOfPrefix(*events, "release:")

	if iWrite < 0 || iSync < 0 || iRelease < 0 {
		t.Fatalf("missing an expected operation in %q", seq)
	}
	if !(iWrite < iSync) {
		t.Errorf("dest was synced BEFORE the write (%q) — the fsync must cover the "+
			"bytes it is making durable", seq)
	}
	if !(iSync < iRelease) {
		t.Errorf("capacity released before the dest was synced (%q) — a crash here "+
			"loses the bytes from BOTH sides: dest never had them durably, spool no "+
			"longer holds them", seq)
	}
}

// Publishing must happen BEFORE the punch. This is the pair that corrupts a
// live reader rather than only a crash, so it gets its own test: the dest is
// made to fail at Sync, and the assertion is that the boundary never moved.
func TestAdvanceStreamPublishesNothingWhenSyncFails(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{
		data: map[int64][]byte{}, events: events,
		failSync: errors.New("simulated fsync failure"),
	}
	rel := &recordingReleaser{events: events}

	n, err := advanceStream(e, src, dest, rel, 128<<10)
	if err == nil {
		t.Fatal("advanceStream succeeded despite an fsync failure")
	}
	if n != 0 {
		t.Errorf("reported %d bytes reclaimed on a failed advance, want 0", n)
	}
	if got := e.PunchedEnd(); got != 0 {
		t.Errorf("punchedEnd advanced to %d after a failed fsync — readers are now "+
			"routed at a destination that does not durably hold those bytes", got)
	}
	if rel.total != 0 {
		t.Errorf("released %d bytes of capacity on a failed advance", rel.total)
	}
	// The spool bytes must still be readable — nothing was punched.
	buf := make([]byte, 4096)
	if _, err := src.ReadAt(buf, 0); err != nil {
		t.Errorf("spool prefix unreadable after a failed advance: %v", err)
	}
	if buf[0] != 0 || buf[1] != 1 {
		t.Errorf("spool prefix corrupted after a failed advance (first bytes %d,%d)",
			buf[0], buf[1])
	}
}

// A destination WRITE failure must leave everything untouched.
func TestAdvanceStreamLeavesPrefixIntactWhenWriteFails(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{
		data: map[int64][]byte{}, events: events,
		failWrite: errors.New("simulated dest write failure"),
	}
	rel := &recordingReleaser{events: events}

	if _, err := advanceStream(e, src, dest, rel, 128<<10); err == nil {
		t.Fatal("advanceStream succeeded despite a dest write failure")
	}
	if got := e.PunchedEnd(); got != 0 {
		t.Errorf("punchedEnd advanced to %d after a failed dest write", got)
	}
	if rel.total != 0 {
		t.Errorf("released %d bytes after a failed dest write", rel.total)
	}
	if indexOf(*events, "sync") >= 0 {
		t.Error("dest was synced after its write failed — nothing to make durable")
	}
}

// Capacity released must equal what the FILESYSTEM actually reclaimed, not what
// was requested. punchRange aligns inward, so the two differ; releasing the
// requested amount would drift the budget above true occupancy one partial block
// at a time, until the spool believes it has room the disk does not.
func TestAdvanceStreamReleasesOnlyWhatWasActuallyPunched(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}

	// A deliberately unaligned sealed end: only whole blocks are punchable.
	const sealed = 100<<10 + 777
	n, err := advanceStream(e, src, dest, rel, sealed)
	if err != nil {
		t.Fatalf("advanceStream: %v", err)
	}
	if rel.total != n {
		t.Errorf("released %d but punched %d — the capacity budget must track the "+
			"filesystem, not the request", rel.total, n)
	}
	if n >= sealed {
		t.Errorf("punched %d for an unaligned request of %d; inward alignment must "+
			"reclaim strictly less", n, sealed)
	}
}

// A sealed end that has not advanced is a no-op, not an error. This is the
// common case on every poll of a file that has not accumulated a new chunk.
func TestAdvanceStreamNoOpWhenNothingNewIsSealed(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}

	if _, err := advanceStream(e, src, dest, rel, 64<<10); err != nil {
		t.Fatal(err)
	}
	before := e.PunchedEnd()

	// Same sealed end again, and a retreated one.
	for _, sealed := range []int64{64 << 10, 32 << 10, 0} {
		n, err := advanceStream(e, src, dest, rel, sealed)
		if err != nil {
			t.Errorf("advanceStream(sealed=%d) errored: %v", sealed, err)
		}
		if n != 0 {
			t.Errorf("advanceStream(sealed=%d) reclaimed %d bytes, want 0", sealed, n)
		}
	}
	if after := e.PunchedEnd(); after != before {
		t.Errorf("punchedEnd moved %d -> %d on a no-op/retreating advance — a punch "+
			"must never un-publish", before, after)
	}
}

// The feature must be OFF by default. Streaming changes the write path's
// durability story, and it stays dark until the oversized-file gate test has
// actually run on a machine with room to run it.
func TestStreamDrainIsDisabledByDefault(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	if streamDrainEnabled() {
		t.Error("streaming drain is ON with the env unset — it must be opt-in until " +
			"the oversized-file gate test has run")
	}
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "1")
	if !streamDrainEnabled() {
		t.Error("JM_SPOOL_STREAM_DRAIN=1 did not enable streaming")
	}
	for _, v := range []string{"0", "true", "yes", "on"} {
		t.Setenv("JM_SPOOL_STREAM_DRAIN", v)
		if streamDrainEnabled() {
			t.Errorf("%q enabled streaming; only an explicit \"1\" may", v)
		}
	}
}

// Bytes landing at the destination must be the bytes that were in the spool, at
// the same offsets. Ordering is the subtle risk; this is the blunt one.
func TestAdvanceStreamCopiesTheCorrectBytesToTheCorrectOffset(t *testing.T) {
	e, src, events := streamFixture(t, 256<<10)
	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}

	if _, err := advanceStream(e, src, dest, rel, 64<<10); err != nil {
		t.Fatal(err)
	}
	got, ok := dest.data[0]
	if !ok {
		t.Fatal("destination received nothing at offset 0")
	}
	for i := range got {
		if want := byte(i % 251); got[i] != want {
			t.Fatalf("dest byte %d = %d, want %d", i, got[i], want)
		}
	}
}

func indexOf(ss []string, want string) int {
	for i, s := range ss {
		if s == want {
			return i
		}
	}
	return -1
}

func indexOfPrefix(ss []string, prefix string) int {
	for i, s := range ss {
		if strings.HasPrefix(s, prefix) {
			return i
		}
	}
	return -1
}

// A read-only spool handle must fail LOUDLY rather than silently skipping the
// reclaim.
//
// This is not hypothetical: the first version of this code passed
// SpoolEntry.OpenForRead() here, which returns a read-only fd, and every punch
// died with EPERM. Had punchRange swallowed that error, streaming would have
// published punchedEnd and released capacity while reclaiming NOTHING — the
// budget would drift above true occupancy until the disk filled for real.
func TestAdvanceStreamRejectsAReadOnlySpoolHandle(t *testing.T) {
	e, _, events := streamFixture(t, 256<<10)
	ro, err := e.OpenForRead()
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}

	n, err := advanceStream(e, ro, dest, rel, 64<<10)
	if err == nil {
		t.Fatal("advanceStream succeeded with a READ-ONLY spool handle — punching " +
			"cannot have happened, so capacity would be released for space the " +
			"disk still holds")
	}
	if n != 0 {
		t.Errorf("reported %d bytes reclaimed via a read-only handle", n)
	}
	if rel.total != 0 {
		t.Errorf("released %d bytes of capacity despite reclaiming none", rel.total)
	}
}

// PUBLISH MUST HAPPEN BEFORE PUNCH. This is the pair whose violation corrupts a
// LIVE READER rather than only a crash: between the punch and the publish, a
// reader is still routed at the spool for a range that is already a hole, and
// reads it as zeros with no error.
//
// THIS TEST EXISTS BECAUSE THE FIRST NEUTER OF THIS ORDERING PASSED. Moving
// publishPunchedEnd to after punchRange left every other test green — the
// publish is a state mutation with no trace, and the punch is a syscall the
// tests could not observe. Those assertions were coverage, not guards. The only
// place the invariant is checkable is INSIDE the punch, so that is where this
// looks.
func TestAdvanceStreamPublishesBeforeItPunches(t *testing.T) {
	e, _, events := streamFixture(t, 256<<10)
	src, err := os.OpenFile(e.SpoolFilePath(), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	const sealed = 64 << 10
	var punchedEndAtPunchTime int64 = -1

	orig := punchRangeFn
	t.Cleanup(func() { punchRangeFn = orig })
	punchRangeFn = func(f *os.File, off, end int64) (int64, error) {
		// Sample the published boundary at the instant of punching. If publish
		// has not run yet this is still 0, and the window is open.
		punchedEndAtPunchTime = e.PunchedEnd()
		return orig(f, off, end)
	}

	dest := &recordingDest{data: map[int64][]byte{}, events: events}
	rel := &recordingReleaser{events: events}
	if _, err := advanceStream(e, src, dest, rel, sealed); err != nil {
		t.Fatalf("advanceStream: %v", err)
	}

	if punchedEndAtPunchTime < sealed {
		t.Fatalf("punchedEnd was %d when the punch ran, want >= %d — the punch "+
			"overtook the publish, so between those two statements a reader is "+
			"routed at the spool for a range that is already a hole and gets zeros "+
			"with no error", punchedEndAtPunchTime, sealed)
	}
}
