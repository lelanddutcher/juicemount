package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
	"github.com/lelanddutcher/juicemount/metadata"
)

const (
	drainMacA = "AAAAAAAA-0000-4000-8000-00000000000A"
	drainMacB = "BBBBBBBB-0000-4000-8000-00000000000B"
)

func drainOnMac(t *testing.T, uuid string) {
	t.Helper()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return uuid, nil }))
}

func drainNoFingerprint(t *testing.T) {
	t.Helper()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "", deviceid.ErrUnavailable }))
}

// reopenDrainer constructs another drainer over the same spool root and spool
// database: the same files as seen by the next start, on whichever Mac the
// fingerprint source currently names.
func reopenDrainer(t *testing.T, first *Drainer) *Drainer {
	t.Helper()
	d, err := NewDrainer(first.spool, DrainerConfig{FuseRoot: first.fuseRoot,
		BackoffBase: 5 * time.Millisecond, PollFallback: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("reopen drainer: %v", err)
	}
	d.SetOnDrainCommit(first.onDrainCommit)
	return d
}

func drainCount(t *testing.T, d *Drainer) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return d.DrainOnceForTest(ctx)
}

func stageFor(t *testing.T, d *Drainer, inst string, rowID int64, seq int) string {
	t.Helper()
	dir := filepath.Join(d.fuseRoot, "proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour).UnixNano()
	p := filepath.Join(dir, fmt.Sprintf("%s%s-%d-%x-%d-clip.mov", drainStagePrefix, inst, rowID, old, seq))
	if err := os.WriteFile(p, []byte("in flight"), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(p string) bool { _, err := os.Lstat(p); return err == nil }

func readTrim(t *testing.T, p string) string {
	t.Helper()
	b, _ := os.ReadFile(p)
	return strings.TrimSpace(string(b))
}

// A spool root and database copied to another Mac carry the first Mac's
// instance ID and pending rows. The second Mac takes a new ID, so its boot
// sweep never deletes the first Mac's in-flight stages, while still sweeping
// its own.
func TestDrainInstanceIDCloneNeverSweepsOtherMacStages(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	e := writeSpoolEntry(t, spool, "/proj/clip.mov", []byte("pending"))
	idA, _ := a.instanceID()
	theirs := stageFor(t, a, idA, e.ID(), 1)

	drainOnMac(t, drainMacB)
	b := reopenDrainer(t, a)
	idB, persisted := b.instanceID()
	if !persisted {
		t.Fatal("Mac B instance ID not persisted; sweep would be disabled rather than safe")
	}
	ours := stageFor(t, b, idB, e.ID(), 2)
	n, err := b.SweepDrainStages(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !exists(theirs) {
		t.Fatal("H8: Mac B's boot sweep deleted Mac A's in-flight stage")
	}
	if idB == idA {
		t.Fatalf("clone kept the copied spool instance ID %q", idA)
	}
	if exists(ours) || n != 1 {
		t.Fatalf("Mac B did not sweep its own orphan: removed=%d", n)
	}
	if got := readTrim(t, filepath.Join(spool.root, drainInstanceFile)); got != idB {
		t.Fatalf("rotated ID not persisted: %q", got)
	}
}

// Rows copied from another Mac are that Mac's pending writes. They are held:
// never drained or deleted automatically, still counted as pending, and only
// released or discarded by an explicit decision. Rows written on this Mac
// afterwards drain normally.
func TestDrainCloneHoldsCopiedRows(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	copied := writeSpoolEntry(t, spool, "/proj/copied.mov", []byte("Mac A's pending bytes"))

	drainOnMac(t, drainMacB)
	b := reopenDrainer(t, a)
	if h, _ := spool.Meta().CloneHold(copied.ID()); h != metadata.SpoolCloneHoldReason {
		t.Fatalf("copied row not held: hold=%q", h)
	}
	if n := drainCount(t, b); n != 0 {
		t.Fatalf("H8: Mac B drained %d row(s) copied from Mac A", n)
	}
	if _, err := os.Lstat(filepath.Join(b.fuseRoot, "proj/copied.mov")); err == nil {
		t.Fatal("H8: a copied row was published")
	}
	if files, _, _ := spool.Meta().PendingStats(); files != 1 {
		t.Fatalf("held row not counted as pending (Quit guard, /spool): %d", files)
	}
	if rows, _ := b.CopiedSpoolRows(); len(rows) != 1 || rows[0].ID != copied.ID() {
		t.Fatalf("CopiedSpoolRows = %+v", rows)
	}

	// A save made on Mac B after the copy drains normally.
	writeSpoolEntry(t, spool, "/proj/new-on-b.mov", []byte("Mac B"))
	if n := drainCount(t, b); n != 1 {
		t.Fatalf("Mac B's own row did not drain: %d", n)
	}

	// Reopening on Mac B again does not release the hold.
	b2 := reopenDrainer(t, b)
	if n := drainCount(t, b2); n != 0 {
		t.Fatalf("hold lost across restart: drained %d", n)
	}

	// The user's decision: upload.
	if n, err := b2.ResolveCopiedSpoolRows(true); err != nil || n != 1 {
		t.Fatalf("upload decision = %d, %v", n, err)
	}
	if n := drainCount(t, b2); n != 1 {
		t.Fatalf("released row did not drain: %d", n)
	}
}

func TestDrainCloneDiscardDecisionDeletesOnlyHeldRows(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	copied := writeSpoolEntry(t, spool, "/proj/copied.mov", []byte("Mac A"))
	row, _ := spool.Meta().Get(copied.ID())
	drainOnMac(t, drainMacB)
	b := reopenDrainer(t, a)
	mine := writeSpoolEntry(t, spool, "/proj/mine.mov", []byte("Mac B"))
	if n, err := b.ResolveCopiedSpoolRows(false); err != nil || n != 1 {
		t.Fatalf("discard decision = %d, %v", n, err)
	}
	if r, _ := spool.Meta().Get(copied.ID()); r != nil {
		t.Fatal("discarded row still present")
	}
	if exists(row.SpoolFile) {
		t.Fatal("discarded row's spool file left behind")
	}
	if r, _ := spool.Meta().Get(mine.ID()); r == nil {
		t.Fatal("discard deleted a row written on this Mac")
	}
}

func TestDrainInstanceIDSameMacKeepsIDAndRows(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	e := writeSpoolEntry(t, spool, "/proj/clip.mov", []byte("pending"))
	idA, _ := a.instanceID()
	orphan := stageFor(t, a, idA, e.ID(), 1)
	b := reopenDrainer(t, a) // restart on the same Mac
	if id, _ := b.instanceID(); id != idA {
		t.Fatalf("same Mac rotated the spool ID: %q -> %q", idA, id)
	}
	if h, _ := spool.Meta().CloneHold(e.ID()); h != "" {
		t.Fatalf("same Mac held its own row: %q", h)
	}
	if n, err := b.SweepDrainStages(time.Now()); err != nil || n != 1 || exists(orphan) {
		t.Fatalf("own orphan not swept on the same Mac: removed=%d err=%v", n, err)
	}
	if n := drainCount(t, b); n != 1 {
		t.Fatalf("same Mac's row did not drain: %d", n)
	}
}

// A spool from a build without the binding has only .drain-instance-id. It
// keeps that ID (its stages stay recognisable), holds nothing, and is bound
// to this Mac once.
func TestDrainInstanceIDUpgradeAdoptsOnce(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	e := writeSpoolEntry(t, spool, "/proj/clip.mov", []byte("pending"))
	const legacy = "0123456789abcdef"
	if err := os.WriteFile(filepath.Join(spool.root, drainInstanceFile), []byte(legacy+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(spool.root, drainInstanceDeviceFile)); err != nil {
		t.Fatal(err)
	}
	orphan := stageFor(t, a, legacy, e.ID(), 1)
	b := reopenDrainer(t, a)
	if id, ok := b.instanceID(); id != legacy || !ok {
		t.Fatalf("upgrade rotated or lost the legacy ID: %q ok=%v", id, ok)
	}
	if h, _ := spool.Meta().CloneHold(e.ID()); h != "" {
		t.Fatalf("upgrade held a row: %q", h)
	}
	if n, err := b.SweepDrainStages(time.Now()); err != nil || n != 1 || exists(orphan) {
		t.Fatalf("legacy orphan not swept after upgrade: removed=%d err=%v", n, err)
	}
	if got := readTrim(t, filepath.Join(spool.root, drainInstanceDeviceFile)); got != deviceid.Digest(drainMacA) {
		t.Fatalf("upgrade did not bind the spool ID: %q", got)
	}
	// Once bound, a copy onto another Mac rotates like any clone.
	drainOnMac(t, drainMacB)
	if c, _ := loadDrainInstanceID(spool.root); c == legacy {
		t.Fatal("adopted spool ID copied to another Mac was kept")
	}
}

// With no fingerprint nothing can be compared: the ID is kept, no row is
// held, nothing is rotated or deleted.
func TestDrainInstanceIDFingerprintUnavailableKeepsEverything(t *testing.T) {
	drainOnMac(t, drainMacA)
	spool, a := newTestDrainer(t, DrainerConfig{})
	e := writeSpoolEntry(t, spool, "/proj/clip.mov", []byte("pending"))
	idA, _ := a.instanceID()
	drainNoFingerprint(t)
	b := reopenDrainer(t, a)
	if id, ok := b.instanceID(); id != idA || !ok {
		t.Fatalf("unverifiable start rotated the spool ID: %q -> %q ok=%v", idA, id, ok)
	}
	if h, _ := spool.Meta().CloneHold(e.ID()); h != "" {
		t.Fatalf("unverifiable start held a row: %q", h)
	}
	if got := readTrim(t, filepath.Join(spool.root, drainInstanceDeviceFile)); got != deviceid.Digest(drainMacA) {
		t.Fatalf("binding changed: %q", got)
	}
}
