package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The stall dump must not live under /tmp.
//
// WHY: the dump is written while the mount is wedging — i.e. in the minutes
// before the machine may panic or be force-restarted. macOS clears /tmp on
// boot, so the single moment those stack traces matter is the moment they are
// guaranteed to be gone. On 2026-08-05 twelve dumps were written while NFS
// reads sat in-flight for 592s; the box panicked and all twelve evaporated,
// taking the only direct evidence of where the read was blocked.
func TestStallDumpDirSurvivesReboot(t *testing.T) {
	if strings.HasPrefix(inflightDumpDir, "/tmp/") || strings.HasPrefix(inflightDumpDir, "/private/tmp/") {
		t.Fatalf("stall dumps go to %q, which macOS clears on boot — a crash erases the "+
			"evidence it was written to capture", inflightDumpDir)
	}
	if !strings.Contains(inflightDumpDir, "Logs") {
		t.Errorf("dump dir %q is not beside the app log, where an operator actually looks",
			inflightDumpDir)
	}
}

// Persisting across reboots means dumps accumulate, so the directory must stay
// bounded — this system already fights for boot-disk space (the spool and the
// JuiceFS cache share the SSD), and an unbounded debug directory is its own
// failure mode.
func TestStallDumpsArePruned(t *testing.T) {
	dir := t.TempDir()
	total := maxStallDumps + 2
	for i := 0; i < total; i++ {
		fn := filepath.Join(dir, fmt.Sprintf("inflight_stall_%d.txt", i))
		if err := os.WriteFile(fn, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		mt := time.Now().Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(fn, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	// An unrelated file must survive — pruning is scoped to our own dumps, not
	// to whatever else shares the directory.
	other := filepath.Join(dir, "juicemount.log")
	if err := os.WriteFile(other, []byte("log"), 0o644); err != nil {
		t.Fatal(err)
	}

	pruneStallDumps(dir)

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var dumps []string
	var sawOther bool
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "inflight_stall_") {
			dumps = append(dumps, e.Name())
		}
		if e.Name() == "juicemount.log" {
			sawOther = true
		}
	}
	if len(dumps) != maxStallDumps {
		t.Errorf("after pruning %d dumps remain, want %d", len(dumps), maxStallDumps)
	}
	if !sawOther {
		t.Error("pruning deleted an unrelated file — it must only remove its own dumps")
	}
	// The NEWEST must be kept: a stall is diagnosed from the most recent
	// capture, so discarding those while keeping stale ones is worse than not
	// pruning at all.
	oldest := fmt.Sprintf("inflight_stall_%d.txt", 0)
	newest := fmt.Sprintf("inflight_stall_%d.txt", total-1)
	for _, d := range dumps {
		if d == oldest {
			t.Error("pruning kept the OLDEST dump and discarded newer ones")
		}
	}
	found := false
	for _, d := range dumps {
		if d == newest {
			found = true
		}
	}
	if !found {
		t.Error("pruning discarded the NEWEST dump — that is the one a stall is diagnosed from")
	}
}
