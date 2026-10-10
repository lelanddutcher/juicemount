package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// L4B-1: an online CREATE of a NEW name now pays a backend Lstat through
// nfsLstatGate. With the gate saturated (FUSE wedge: 24 leaked Lstats),
// every create blocks for drainBaseProbeTimeout.
func TestL4BCreateBlocksOnSaturatedLstatGate(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	for i := 0; i < cap(nfsLstatGate); i++ {
		nfsLstatGate <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(nfsLstatGate); i++ {
			<-nfsLstatGate
		}
	}()
	for _, offline := range []bool{false, true} {
		was := pin.IsOffline()
		pin.SetOffline(offline)
		var durs []time.Duration
		for i := 0; i < 6; i++ {
			start := time.Now()
			e, err := spool.OpenWrite("Card/A001/C" + strconv.Itoa(i) + map[bool]string{true: "off", false: "on"}[offline] + ".MP4")
			if err != nil {
				t.Fatal(err)
			}
			durs = append(durs, time.Since(start))
			e.ReleaseHandle()
		}
		pin.SetOffline(was)
		t.Logf("offline=%v per-create: %v", offline, durs)
	}
}

// L4B-2: count backend Lstats per new-file create when the gate is healthy.
func TestL4BCreateLstatCount(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	const n = 200
	var tot time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		e, err := spool.OpenWrite("Card/B/C" + strconv.Itoa(i) + ".MP4")
		if err != nil {
			t.Fatal(err)
		}
		tot += time.Since(start)
		e.ReleaseHandle()
	}
	t.Logf("avg create (local APFS as fake FUSE, ~0 latency): %v", tot/n)
}

// L4B-3: Premiere-style atomic save: write a temp name, rename it over the
// existing project file. Handler emulation: MigrateForRename, then the FUSE
// rename fails ENOENT (temp is spool-only) so the handler removes the stale
// FUSE dest. Must land as the new project file, no conflict copy.
func TestL4BTempThenRenameSaveNoConflict(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const tmp, final = "Film/ProjA/Proj-tmp123.prproj", "Film/ProjA/Proj.prproj"
	dest := writeBackendFile(t, d, final, []byte("v1 project"))
	mirrorSeesBackend(t, spool, final, dest)
	ours := []byte("v2 project saved via temp+rename")
	writeSpoolEntry(t, spool, tmp, ours)
	if _, _, err := spool.MigrateForRename(tmp, final); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(d.fuseRoot, tmp), dest); err != nil {
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		_ = os.Remove(dest)
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, ours) {
		t.Fatalf("save did not land: %q", got)
	}
	if _, err := os.Stat(conflictCopyOf(d, final)); err == nil {
		t.Fatal("own temp+rename save became a conflict copy")
	}
}

// L4B-4: user renames a pending save (present base), then saves AGAIN at the
// new name before the first drain. The rename changes ctime (JuiceFS); the
// second save reuses/creates a row whose base must still allow in place.
func TestL4BRenameThenSaveAgainNoConflict(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const from, to = "Film/ProjA/a.prproj", "Film/ProjA/b.prproj"
	src := writeBackendFile(t, d, from, []byte("v1"))
	mirrorSeesBackend(t, spool, from, src)
	writeSpoolEntry(t, spool, from, []byte("v2"))
	if _, _, err := spool.MigrateForRename(from, to); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(d.fuseRoot, to)
	if err := os.Rename(src, dest); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	_ = os.Chmod(dest, 0o644)
	drainOnce(t, d)
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("rename+save lost: %q", got)
	}
	if _, err := os.Stat(conflictCopyOf(d, to)); err == nil {
		t.Fatal("own rename+save became a conflict copy")
	}
}

// L4B-5: online probe timed out, mirror has nothing -> UNKNOWN -> legacy
// in-place. Another Mac created the name meanwhile (keyspace lag): what does
// the drain do?
func TestL4BTimeoutUnknownOverwritesForeignNewFile(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/new.mov"
	for i := 0; i < cap(nfsLstatGate); i++ {
		nfsLstatGate <- struct{}{}
	}
	ours := []byte("mac A create while FUSE slow")
	e := writeSpoolEntry(t, spool, name, ours)
	for i := 0; i < cap(nfsLstatGate); i++ {
		<-nfsLstatGate
	}
	b, _ := d.rowDrainBase(rowOf(t, spool, e))
	t.Logf("recorded base kind: %v", b.Kind)
	theirs := []byte("MAC B created it")
	dest := writeBackendFile(t, d, name, theirs)
	drainOnce(t, d)
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, theirs) {
		t.Fatalf("other Mac's new file overwritten (base=%v): %q", b.Kind, got)
	}
}

var _ = metadata.SpoolBaseUnknown
