package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A new file's install rename can land on the server and still report an
// error: the link drops with the reply in flight, or juicefs retries a
// transaction that had already committed and answers EEXIST. The name then
// holds this row's own bytes. The drain must finish the row with no conflict
// copy of itself. Remote lab (roam-lab.sh drop30, 2026-11-13): an 8 MiB file
// drained across a 30 s link drop left "w00009 (conflict ...).bin" beside
// its own w00009.bin.
func TestDrainInstallRenameLandedButReportedError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"reply lost (EIO)", syscall.EIO},
		{"juicefs retry after commit (EEXIST)", os.ErrExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
			name := "/proj/shot/clip.mov"
			dest := filepath.Join(d.fuseRoot, name)
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("roam"), 2<<20/4) // 2 MiB: the stage path
			e := writeSpoolEntry(t, spool, name, payload)
			lied := 0
			orig := drainRenameNoReplace
			drainRenameNoReplace = func(a, b string) error {
				err := orig(a, b)
				if err == nil && b == dest && lied == 0 {
					lied++
					return tc.err // committed, reply says otherwise
				}
				return err
			}
			t.Cleanup(func() { drainRenameNoReplace = orig })
			probeDrainLoop(d)
			if lied != 1 {
				t.Fatalf("the install rename ran %d lying times, want 1", lied)
			}
			row, err := spool.Meta().Get(e.ID())
			if err != nil {
				t.Fatal(err)
			}
			if row.DrainState != metadata.DrainDone {
				t.Fatalf("row state %s (%q), want done", row.DrainState, row.LastError)
			}
			got, err := os.ReadFile(dest)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("destination bytes wrong (%d bytes, %v)", len(got), err)
			}
			ents, err := os.ReadDir(filepath.Dir(dest))
			if err != nil {
				t.Fatal(err)
			}
			for _, ent := range ents {
				if ent.Name() != filepath.Base(dest) {
					if strings.Contains(ent.Name(), "conflict") {
						t.Fatalf("published a conflict copy of its own file: %q", ent.Name())
					}
					t.Fatalf("left %q beside the destination", ent.Name())
				}
			}
		})
	}
}

// Control: a file that another writer put at the free name while this row
// drained still gets a conflict copy (the in-memory identity is this row's
// stage, not any file at the name).
func TestDrainInstallForeignNameStillConflicts(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	name := "/proj/shot/other.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	e := writeSpoolEntry(t, spool, name, bytes.Repeat([]byte("mine"), 2<<20/4))
	orig := drainRenameNoReplace
	drainRenameNoReplace = func(a, b string) error {
		if b == dest {
			if _, err := os.Lstat(dest); os.IsNotExist(err) {
				if werr := os.WriteFile(dest, []byte("someone else's"), 0o644); werr != nil {
					t.Error(werr)
				}
			}
		}
		return orig(a, b)
	}
	t.Cleanup(func() { drainRenameNoReplace = orig })
	probeDrainLoop(d)
	if got, _ := os.ReadFile(dest); string(got) != "someone else's" {
		t.Fatalf("the foreign file was replaced: %q", got)
	}
	row, _ := spool.Meta().Get(e.ID())
	if !strings.Contains(row.LastError, conflictCopyMarker) {
		t.Fatalf("no conflict copy for a foreign file: state %s err %q", row.DrainState, row.LastError)
	}
}
