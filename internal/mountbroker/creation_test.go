package mountbroker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func testSpec() Spec {
	return Spec{MountPath: "/Volumes/owned", Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef", Endpoint: "127.0.0.1:11049", ExportPath: "/jm-0123456789abcdef0123456789abcdef", MetadataPath: "/private/owned/meta.db"}
}
func testSnapshot(entries ...mounttable.MountEntry) mounttable.Snapshot {
	return mounttable.Snapshot{Version: 1, BootID: "boot-a", Entries: append([]mounttable.MountEntry{}, entries...)}
}
func testEntry() mounttable.MountEntry {
	s := testSpec()
	return mounttable.MountEntry{Path: s.MountPath, Source: s.Source, Type: "nfs", FSID: [2]int32{12, 34}}
}
func testCreator(t *testing.T) Creator {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return Creator{Directory: directory}
}
func readRecord(t *testing.T, c Creator) *Record {
	t.Helper()
	j, err := openJournal(c.Directory, testSpec().MountPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	r, err := j.read()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCreationPersistsIntentBeforeCommandAndObservedReceiptAfter(t *testing.T) {
	c := testCreator(t)
	observations, commands := 0, 0
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		observations++
		if observations == 1 {
			return testSnapshot(), nil
		}
		return testSnapshot(testEntry()), nil
	}
	result := c.Create(context.Background(), testSpec(), func() error {
		commands++
		// Another creator must not start or even inspect while this callback owns the lock.
		other := c
		other.Observe = func(context.Context) (mounttable.Snapshot, error) {
			t.Fatal("concurrent observation")
			return mounttable.Snapshot{}, nil
		}
		if r := other.Create(context.Background(), testSpec(), func() error { t.Fatal("concurrent mount"); return nil }); r.Err == nil {
			t.Fatal("parallel creator entered")
		}
		paths, _ := filepath.Glob(filepath.Join(c.Directory, "*.json"))
		if len(paths) != 1 {
			t.Fatal("intent missing before command")
		}
		data, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		var record Record
		if err := json.Unmarshal(data, &record); err != nil || record.State != InFlight || record.Mount != nil || record.Spec != testSpec() {
			t.Fatalf("bad durable intent: %s %v", data, err)
		}
		return nil
	})
	if result.Failure() != nil || !result.Attempted || commands != 1 || observations != 2 {
		t.Fatalf("result=%+v commands=%d observations=%d", result, commands, observations)
	}
	rec := readRecord(t, c)
	if rec.State != Observed || rec.Mount == nil || *rec.Mount != testEntry() || rec.OperationID != result.OperationID {
		t.Fatalf("receipt=%+v", rec)
	}
	// Even a successful old observation is not permission for a fresh generation/replay.
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		t.Fatal("replayed receipt inspected")
		return mounttable.Snapshot{}, nil
	}
	if r := c.Create(context.Background(), testSpec(), func() error { t.Fatal("replayed mount"); return nil }); r.State != InFlight || r.Attempted || r.Err == nil {
		t.Fatalf("replay=%+v", r)
	}
}

func TestCreationRefusesUnknownForeignAndDescendantBeforeCommand(t *testing.T) {
	descendant := testEntry()
	descendant.Path += "/foreign"
	caseAlias := testEntry()
	caseAlias.Path = strings.ToUpper(caseAlias.Path)
	for name, snapshot := range map[string]mounttable.Snapshot{"unknown": {}, "foreign": testSnapshot(testEntry()), "descendant": testSnapshot(descendant), "case-alias": testSnapshot(caseAlias)} {
		t.Run(name, func(t *testing.T) {
			c := testCreator(t)
			c.Observe = func(context.Context) (mounttable.Snapshot, error) { return snapshot, nil }
			r := c.Create(context.Background(), testSpec(), func() error { t.Fatal("mount ran"); return nil })
			if r.Err == nil || r.Attempted {
				t.Fatalf("result=%+v", r)
			}
			if readRecord(t, c) != nil {
				t.Fatal("refusal published intent")
			}
		})
	}
}

func TestCreationAmbiguousCommandOutcomeRetainsIntentAcrossReopen(t *testing.T) {
	foreign := testEntry()
	foreign.Source = "foreign:/export"
	changedBoot := testSnapshot(testEntry())
	changedBoot.BootID = "boot-b"
	zeroFSID := testEntry()
	zeroFSID.FSID = [2]int32{}
	for name, snapshot := range map[string]mounttable.Snapshot{"absent": testSnapshot(), "foreign": testSnapshot(foreign), "duplicate": testSnapshot(testEntry(), testEntry()), "boot-change": changedBoot, "zero-fsid": testSnapshot(zeroFSID), "query-failed": {}} {
		t.Run(name, func(t *testing.T) {
			c := testCreator(t)
			n := 0
			c.Observe = func(context.Context) (mounttable.Snapshot, error) {
				n++
				if n == 1 {
					return testSnapshot(), nil
				}
				return snapshot, nil
			}
			r := c.Create(context.Background(), testSpec(), func() error { return errors.New("command outcome uncertain") })
			if r.State != Unknown || !r.Attempted || r.Err == nil {
				t.Fatalf("result=%+v", r)
			}
			if rec := readRecord(t, c); rec.State != InFlight || rec.Mount != nil {
				t.Fatalf("lost intent %+v", rec)
			}
			if next := c.Create(context.Background(), testSpec(), func() error { t.Fatal("replayed command"); return nil }); next.Attempted || next.Err == nil {
				t.Fatal("unsettled attempt replayed")
			}
		})
	}
}

func TestCreationDurabilityFailuresNeverBecomeSuccess(t *testing.T) {
	for _, point := range []string{"intent-file", "intent-dir", "receipt-file", "receipt-dir"} {
		t.Run(point, func(t *testing.T) {
			c := testCreator(t)
			n := 0
			c.Observe = func(context.Context) (mounttable.Snapshot, error) {
				n++
				if n == 1 {
					return testSnapshot(), nil
				}
				return testSnapshot(testEntry()), nil
			}
			fileSyncs, dirSyncs, commands := 0, 0, 0
			c.syncFile = func(f *os.File) error {
				fileSyncs++
				if point == "intent-file" && fileSyncs == 1 || point == "receipt-file" && fileSyncs == 2 {
					return errors.New("injected file fsync")
				}
				return f.Sync()
			}
			c.syncDir = func(f *os.File) error {
				dirSyncs++
				if point == "intent-dir" && dirSyncs == 1 || point == "receipt-dir" && dirSyncs == 2 {
					return errors.New("injected directory fsync")
				}
				return f.Sync()
			}
			r := c.Create(context.Background(), testSpec(), func() error { commands++; return nil })
			if r.Err == nil || r.State != Unknown {
				t.Fatalf("false durable success %+v", r)
			}
			if strings.HasPrefix(point, "intent") && commands != 0 {
				t.Fatal("command before durable intent")
			}
			if strings.HasPrefix(point, "receipt") && (!r.Attempted || commands != 1) {
				t.Fatal("lost custody of attempted mount")
			}
		})
	}
}

func TestCreationRejectsInRootSymlinkLockBeforeEffects(t *testing.T) {
	c := testCreator(t)
	j, err := openJournal(c.Directory, testSpec().MountPath)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(c.Directory, j.name+".lock")
	j.close()
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("unexpected-target", lock); err != nil {
		t.Fatal(err)
	}
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		t.Fatal("symlink reached observation")
		return mounttable.Snapshot{}, nil
	}
	result := c.Create(context.Background(), testSpec(), func() error { t.Fatal("symlink admitted mount"); return nil })
	if result.Err == nil || result.Attempted {
		t.Fatalf("symlink admitted: %+v", result)
	}
	if _, err := os.Lstat(filepath.Join(c.Directory, "unexpected-target")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target created: %v", err)
	}
}

func TestCreationRejectsCorruptSymlinkHardlinkAndInsecureJournal(t *testing.T) {
	for _, kind := range []string{"corrupt", "trailing", "oversized", "symlink", "hardlink", "insecure"} {
		t.Run(kind, func(t *testing.T) {
			c := testCreator(t)
			j, err := openJournal(c.Directory, testSpec().MountPath)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(c.Directory, j.name)
			j.close()
			switch kind {
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), name); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				other := filepath.Join(t.TempDir(), "outside")
				os.WriteFile(other, []byte("{}"), 0600)
				if err := os.Link(other, name); err != nil {
					t.Fatal(err)
				}
			case "insecure":
				os.Chmod(c.Directory, 0755)
			default:
				data := "{"
				if kind == "trailing" {
					data = "{} {}"
				}
				if kind == "oversized" {
					data = strings.Repeat(" ", 70<<10)
				}
				if err := os.WriteFile(name, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			c.Observe = func(context.Context) (mounttable.Snapshot, error) {
				t.Fatal("invalid journal reached observation")
				return mounttable.Snapshot{}, nil
			}
			r := c.Create(context.Background(), testSpec(), func() error { t.Fatal("invalid journal mount"); return nil })
			if r.Err == nil || r.Attempted {
				t.Fatalf("result=%+v", r)
			}
		})
	}
}

// Parent kills only its own test child after the callback proves the intent is
// on disk. Reopening must refuse replay; the mount callback is a stand-in.
func TestCreationCrashRetainsIntent(t *testing.T) {
	if directory := os.Getenv("JM_CREATION_CRASH_CHILD"); directory != "" {
		c := Creator{Directory: directory, Observe: func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }}
		c.Create(context.Background(), testSpec(), func() error {
			if err := os.WriteFile(filepath.Join(directory, "entered"), []byte("ready"), 0600); err != nil {
				panic(err)
			}
			select {}
		})
		os.Exit(9)
	}
	c := testCreator(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCreationCrashRetainsIntent$", "-test.count=1")
	cmd.Env = append(os.Environ(), "JM_CREATION_CRASH_CHILD="+c.Directory)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(c.Directory, "entered")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owned child did not reach mount barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		t.Fatal("crashed intent replayed")
		return mounttable.Snapshot{}, nil
	}
	r := c.Create(context.Background(), testSpec(), func() error { t.Fatal("crashed intent ran command"); return nil })
	if r.State != InFlight || r.Attempted || r.Record == nil || r.Record.Mount != nil {
		t.Fatalf("reopened %+v", r)
	}
}

func TestIncompleteSnapshotNeverProvesAbsenceOrAdmitsCreation(t *testing.T) {
	for _, snapshot := range []mounttable.Snapshot{
		{Version: 1, BootID: "boot"},
		testSnapshot(mounttable.MountEntry{Path: "relative", Source: "s", Type: "nfs"}),
		testSnapshot(mounttable.MountEntry{Path: "/other", Source: "s"}),
		testSnapshot(mounttable.MountEntry{Path: "/other", Type: "nfs"}),
	} {
		if ObservedAbsent(snapshot, testSpec().MountPath) {
			t.Fatal("incomplete snapshot reported absence")
		}
		c := testCreator(t)
		c.Observe = func(context.Context) (mounttable.Snapshot, error) { return snapshot, nil }
		if r := c.Create(context.Background(), testSpec(), func() error { t.Fatal("incomplete snapshot admitted command"); return nil }); r.Err == nil {
			t.Fatal("incomplete snapshot accepted")
		}
	}
}
