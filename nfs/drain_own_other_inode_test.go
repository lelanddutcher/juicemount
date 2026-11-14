package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// R2 (laptop run 2026-11-14, roam-lan-d30a): after a 30 s link drop a small
// save was published by the direct path, but the name then showed another
// inode than the one juicefs answered, holding exactly the saved bytes. The
// completion's identity check paused the row ("changed before it was
// opened"), and the retry saw a claimed base whose inode no longer matched
// the name and published "f0062 (conflict ...).bin" beside the file's own
// copy. The fake below reproduces that view: juicefs answers an inode the
// name does not show.
func ownOtherInodeFixture(t *testing.T, foreign []byte) (*SpoolStore, *Drainer, *SpoolEntry, string, []byte) {
	t.Helper()
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	fake := withFakeCreateFiles(t, d)
	const name = "/proj/w/f0062.bin"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := patternedPayload(256<<10, 62)
	e := writeSpoolEntry(t, spool, name, payload)
	first := true
	fake.after = func(_ int, _ jfsNewFile, c *jfsCreated) {
		if !first || c.err != nil {
			return
		}
		first = false
		c.ino += 1 << 30 // the answer names an inode the name does not show
		if foreign != nil {
			// and another writer's bytes take the name meanwhile
			if err := os.Remove(dest); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(dest, foreign, 0o644); err != nil {
				t.Error(err)
			}
		}
	}
	return spool, d, e, dest, payload
}

func drainUntilSettled(t *testing.T, spool *SpoolStore, d *Drainer, e *SpoolEntry) *metadata.SpoolRow {
	t.Helper()
	probeDrainLoop(d)
	return mustRow(t, spool, e.ID())
}

// The name holds exactly this save's bytes under another inode: the retry
// adopts it. One file, no conflict copy.
func TestDrainOwnBytesUnderOtherInodeAdopted(t *testing.T) {
	spool, d, e, dest, payload := ownOtherInodeFixture(t, nil)
	row := drainUntilSettled(t, spool, d, e)
	if row.DrainState != metadata.DrainDone {
		t.Fatalf("row %s (%q), want done", row.DrainState, row.LastError)
	}
	assertOnlyOwnFile(t, spool, e, dest, payload)
}

// Another writer's different bytes at the name still get a conflict copy,
// and its file is untouched.
func TestDrainForeignBytesUnderOtherInodeStillConflict(t *testing.T) {
	foreign := bytes.Repeat([]byte("theirs"), 1000)
	spool, d, e, dest, _ := ownOtherInodeFixture(t, foreign)
	row := drainUntilSettled(t, spool, d, e)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, foreign) {
		t.Fatalf("the other writer's file was replaced (%d bytes)", len(got))
	}
	if !strings.Contains(row.LastError, conflictCopyMarker) {
		t.Fatalf("no conflict copy for another writer's bytes: state %s err %q", row.DrainState, row.LastError)
	}
}
