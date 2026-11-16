package nfs

import (
	"os"
	"testing"
	"time"
)

// B1 (laptop runs 2026-11-16): a slow in-place operation abroad published
// the row with its finish time, a later second than the true mtime JuiceFS
// stamped at its start, so the Mac saw the mtime step back once the true
// value arrived. The row now carries the operation's start second.
func TestOwnSlowTruncateStampsItsStart(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(8192, 113))
	var inside time.Time
	jfs.handler.testAfterInPlaceMutation = func(_ string, op string) {
		if op == "truncate" {
			inside = time.Now()
			// The sync runs on past the next second, as abroad.
			time.Sleep(time.Until(inside.Truncate(time.Second).Add(time.Second + 30*time.Millisecond)))
		}
	}
	// Start late in a second so the wait past its end stays short (the
	// harness times a FUSE operation out after about a second).
	if now := time.Now(); now.Nanosecond() < 850e6 {
		time.Sleep(time.Duration(850e6-now.Nanosecond()) * time.Nanosecond)
	}
	f, err := jfs.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if inside.IsZero() {
		t.Fatal("test hook did not run")
	}
	e := store.LookupByPath(path)
	if e == nil {
		t.Fatal("no row")
	}
	if e.Mtime.Unix() > inside.Unix() {
		t.Fatalf("row mtime second %d is after the operation's start second %d", e.Mtime.Unix(), inside.Unix())
	}
}
