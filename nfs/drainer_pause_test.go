package nfs

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// setDrainPauseForTest makes paused retries fast and the clock controllable.
func setDrainPauseForTest(t *testing.T, now func() time.Time) {
	t.Helper()
	oldBackoff, oldNow := drainPauseBackoff, drainPauseNow
	drainPauseBackoff = 5 * time.Millisecond
	if now != nil {
		drainPauseNow = now
	}
	t.Cleanup(func() { drainPauseBackoff, drainPauseNow = oldBackoff, oldNow })
}

// inPlaceFailureFixture writes an existing server file, a spool entry that
// rewrites it in place, and cuts the spool source to half its length. The
// in-place drain then truncates the server copy and fails "size mismatch"
// after the truncate on every attempt: the shape of an fsync EIO from a
// refused storage key.
func inPlaceFailureFixture(t *testing.T, cfg DrainerConfig) (spool *SpoolStore, d *Drainer, id int64, dest string, newBytes []byte, restore func()) {
	t.Helper()
	spool, d = newTestDrainer(t, cfg)
	const name = "/proj/F2/edit.bin"
	dest = filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	const size = 256 << 10
	if err := os.WriteFile(dest, patternedPayload(size, 1), 0o644); err != nil {
		t.Fatal(err)
	}
	mirrorSeesBackend(t, spool, name, dest)
	newBytes = patternedPayload(size, 2)
	e := writeSpoolEntry(t, spool, name, newBytes)
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(row.SpoolFile, size/2); err != nil {
		t.Fatal(err)
	}
	restore = func() {
		if err := os.WriteFile(row.SpoolFile, newBytes, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return spool, d, e.ID(), dest, newBytes, restore
}

// F2: a drain that fails AFTER it truncated the server copy is paused, not
// failed. More attempts than the budget allows leave the row ready (spool
// bytes kept, status flagged), and once the cause clears the retry writes
// the exact spool bytes to the server.
func TestF2InPlaceFailureAfterTruncatePausesAndRecovers(t *testing.T) {
	setDrainPauseForTest(t, nil)
	spool, d, id, dest, newBytes, restore := inPlaceFailureFixture(t, DrainerConfig{MaxAttempts: 3})

	// One failure, still inside the attempt budget: the status must already
	// say why (attempts alone would not flag it yet).
	drainAllForTest(t, d)
	st, _ := BuildSpoolStatus(spool, d)
	var seen bool
	for _, v := range st.Entries {
		if strings.HasSuffix(v.Path, "edit.bin") {
			seen = true
			if !v.Stalled || !strings.Contains(v.LastError, serverIncompleteMarker) {
				t.Fatalf("F2: status must flag the paused row with its reason; got stalled=%v last_error=%q", v.Stalled, v.LastError)
			}
		}
	}
	if !seen || st.StalledFiles != 1 {
		t.Fatalf("F2: status lists the paused row as stalled: seen=%v stalled_files=%d", seen, st.StalledFiles)
	}

	for i := 0; i < 7; i++ {
		drainAllForTest(t, d)
	}
	row, err := spool.Meta().Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if row.DrainAttempts < 8 {
		t.Fatalf("precondition: expected 8 failing attempts, got %d", row.DrainAttempts)
	}
	if row.DrainState == metadata.DrainFailed {
		t.Fatalf("F2: row failed after %d attempts although the server copy is incomplete; last_error=%q", row.DrainAttempts, row.LastError)
	}
	if !strings.Contains(row.LastError, serverIncompleteMarker) || !isDrainPaused(row.LastError) {
		t.Fatalf("F2: paused row must say the server copy is incomplete; last_error=%q", row.LastError)
	}
	if fi, err := os.Lstat(dest); err != nil || fi.Size() == int64(len(newBytes)) {
		t.Fatalf("precondition: the failure must leave the server copy truncated (size %v, err %v)", fi.Size(), err)
	}
	if _, err := os.Stat(row.SpoolFile); err != nil {
		t.Fatalf("F2: spool bytes lost while paused: %v", err)
	}
	if !spool.HasPending("proj/F2/edit.bin") && !spool.HasPending("/proj/F2/edit.bin") {
		t.Fatal("F2: the paused row must keep its spool shadow so this Mac serves the saved bytes")
	}

	restore()
	drainAllForTest(t, d)
	row, _ = spool.Meta().Get(id)
	if row.DrainState != metadata.DrainDone {
		t.Fatalf("F2: retry after the cause cleared: state=%s last_error=%q", row.DrainState, row.LastError)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(newBytes) {
		t.Fatal("F2: server copy after the retry is not the spool bytes")
	}
}

// F2 time cap: a paused row is retried for drainPauseCap and then marked
// failed with a reason that says what is wrong. The spool file is kept for
// RetryFailed.
func TestF2PauseTimeCapFailsLoudly(t *testing.T) {
	clock := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	setDrainPauseForTest(t, func() time.Time { return clock })
	spool, d, id, _, _, _ := inPlaceFailureFixture(t, DrainerConfig{MaxAttempts: 3})

	for i := 0; i < 4; i++ {
		drainAllForTest(t, d)
	}
	row, _ := spool.Meta().Get(id)
	if row.DrainState == metadata.DrainFailed || !isDrainPaused(row.LastError) {
		t.Fatalf("precondition: row paused before the cap: state=%s last_error=%q", row.DrainState, row.LastError)
	}
	since, ok := drainPausedAt(row.LastError)
	if !ok || !since.Equal(clock) {
		t.Fatalf("pause start not persisted: %v %v (last_error=%q)", since, ok, row.LastError)
	}

	clock = clock.Add(drainPauseCap - time.Minute)
	// Model the same elapsed time for SQL eligibility as the pause clock.
	if _, err := spool.Meta().Owner().DB().Exec("UPDATE spool_entries SET retry_after_ns=0 WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	drainAllForTest(t, d)
	row, _ = spool.Meta().Get(id)
	if row.DrainState == metadata.DrainFailed {
		t.Fatalf("F2: failed before the cap: last_error=%q", row.LastError)
	}

	clock = clock.Add(2 * time.Minute)
	// Model the same elapsed time for SQL eligibility as the pause clock.
	if _, err := spool.Meta().Owner().DB().Exec("UPDATE spool_entries SET retry_after_ns=0 WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	drainAllForTest(t, d)
	row, _ = spool.Meta().Get(id)
	if row.DrainState != metadata.DrainFailed {
		t.Fatalf("F2: the cap was not honoured: state=%s last_error=%q", row.DrainState, row.LastError)
	}
	if isDrainPaused(row.LastError) || !strings.Contains(row.LastError, "server copy incomplete for") {
		t.Fatalf("F2: capped failure reason must name the incomplete server copy and not look paused: %q", row.LastError)
	}
	if _, err := os.Stat(row.SpoolFile); err != nil {
		t.Fatalf("F2: spool file must be kept after the cap: %v", err)
	}
}

// A NEW name (stage install) never changed the server copy, so its failures
// still spend the ordinary attempt budget.
func TestF2StageFailureStillUsesAttemptBudget(t *testing.T) {
	setDrainPauseForTest(t, nil)
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 3})
	const name = "/proj/F2/new.bin"
	e := writeSpoolEntry(t, spool, name, patternedPayload(64<<10, 3))
	row, _ := spool.Meta().Get(e.ID())
	if err := os.Truncate(row.SpoolFile, 32<<10); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		drainAllForTest(t, d)
	}
	row, _ = spool.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainFailed || isDrainPaused(row.LastError) {
		t.Fatalf("stage drain must keep the attempt budget: state=%s last_error=%q", row.DrainState, row.LastError)
	}
	if _, err := os.Lstat(filepath.Join(d.fuseRoot, name)); !os.IsNotExist(err) {
		t.Fatalf("a failed new-name drain must not publish the name: %v", err)
	}
}
