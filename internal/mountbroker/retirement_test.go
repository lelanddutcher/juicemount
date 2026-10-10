package mountbroker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

// Read the already-fsynced intent without attempting a second per-path flock
// while CreateBound's callback still owns that flock.
func intentDuringCallback(t *testing.T, c Creator) Record {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(c.Directory, "*.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("intent file count=%d err=%v", len(paths), err)
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	var record Record
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func observedOperation(t *testing.T, c *Creator) Record {
	t.Helper()
	mounted := false
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		if mounted {
			return testSnapshot(testEntry()), nil
		}
		return testSnapshot(), nil
	}
	result := c.CreateBound(context.Background(), testSpec(), func(intent Record) error {
		if intent.State != InFlight || intent.Mount != nil || intent.OperationID == "" || intent.BootID != "boot-a" {
			t.Fatalf("callback did not receive exact durable intent: %+v", intent)
		}
		if row := intentDuringCallback(t, *c); row.OperationID != intent.OperationID || row.State != InFlight {
			t.Fatalf("intent not durable before callback: %+v", row)
		}
		mounted = true
		return nil
	})
	if err := result.Failure(); err != nil {
		t.Fatal(err)
	}
	return *result.Record
}

func TestRetirementAllowsOnlyNewDurableIntentAfterExactSettlement(t *testing.T) {
	c := testCreator(t)
	first := observedOperation(t, &c)
	settled := 0
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	if err := c.Retire(context.Background(), first, func() error { settled++; return nil }); err != nil {
		t.Fatal(err)
	}
	if settled != 1 || readRecord(t, c).State != Retired {
		t.Fatal("exact operation was not durably retired")
	}
	secondMounted := false
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		if secondMounted {
			return testSnapshot(testEntry()), nil
		}
		return testSnapshot(), nil
	}
	second := c.CreateBound(context.Background(), testSpec(), func(intent Record) error {
		row := intentDuringCallback(t, c)
		if intent.OperationID == first.OperationID || row.State != InFlight || row.OperationID != intent.OperationID {
			t.Fatal("successor effect preceded a new durable intent")
		}
		secondMounted = true
		return nil
	})
	if err := second.Failure(); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := c.Retire(context.Background(), first, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("stale operation entered settlement or erased its successor")
	}
	if row := readRecord(t, c); row.OperationID != second.OperationID || row.State != Observed {
		t.Fatalf("successor journal changed: %+v", row)
	}
}

func TestRetirementRequiresLiveCallbackAndFreshGlobalAbsence(t *testing.T) {
	for _, variant := range []string{"nil-settle", "failed-settle", "snapshot-failed", "boot-changed", "same-path", "descendant", "same-source-elsewhere", "same-fsid-elsewhere", "same-source-case-alias"} {
		t.Run(variant, func(t *testing.T) {
			c := testCreator(t)
			record := observedOperation(t, &c)
			entry := testEntry()
			if variant == "descendant" {
				entry.Path += "/child"
			}
			if variant == "same-source-elsewhere" {
				entry.Path = "/Volumes/other"
				entry.FSID = [2]int32{44, 55}
			}
			if variant == "same-fsid-elsewhere" {
				entry.Path = "/Volumes/other"
				entry.Source = "foreign:/export"
			}
			if variant == "same-source-case-alias" {
				entry.Path = "/VOLUMES/OWNED"
			}
			c.Observe = func(context.Context) (mounttable.Snapshot, error) {
				switch variant {
				case "snapshot-failed":
					return mounttable.Snapshot{}, errors.New("observer unavailable")
				case "boot-changed":
					s := testSnapshot()
					s.BootID = "boot-b"
					return s, nil
				case "same-path", "descendant", "same-source-elsewhere", "same-fsid-elsewhere", "same-source-case-alias":
					return testSnapshot(entry), nil
				default:
					return testSnapshot(), nil
				}
			}
			var settle func() error = func() error { return nil }
			if variant == "nil-settle" {
				settle = nil
			}
			if variant == "failed-settle" {
				settle = func() error { return errors.New("original helper still live") }
			}
			if err := c.Retire(context.Background(), record, settle); err == nil {
				t.Fatal("unsettled or present operation retired")
			}
			if row := readRecord(t, c); row.State != Observed || row.OperationID != record.OperationID {
				t.Fatalf("refusal modified journal: %+v", row)
			}
		})
	}
}

func TestRetirementReconcilesInFlightAndDirectorySyncFailure(t *testing.T) {
	c := testCreator(t)
	calls := 0
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { calls++; return testSnapshot(), nil }
	created := c.CreateBound(context.Background(), testSpec(), func(Record) error { return errors.New("mount outcome uncertain") })
	if created.State != Unknown || !created.Attempted || readRecord(t, c).State != InFlight {
		t.Fatalf("expected retained InFlight intent, got %+v", created)
	}
	expected := *created.Record
	mount := testEntry() // Controller's retained FD identity after command uncertainty.
	expected.Mount = &mount
	dirSyncs := 0
	c.syncDir = func(f *os.File) error {
		dirSyncs++
		if dirSyncs == 1 {
			return errors.New("injected directory sync failure")
		}
		return f.Sync()
	}
	if err := c.Retire(context.Background(), expected, func() error { return nil }); err == nil {
		t.Fatal("directory sync failure reported durable retirement")
	}
	if row := readRecord(t, c); row.State != Retired || row.Mount == nil || *row.Mount != mount {
		t.Fatalf("renamed but unsynced tombstone lost identity: %+v", row)
	}
	if err := c.Retire(context.Background(), expected, func() error { return nil }); err != nil {
		t.Fatalf("same operation could not resync retirement: %v", err)
	}
	if dirSyncs != 2 || readRecord(t, c).State != Retired || calls != 4 {
		t.Fatalf("retry did not reobserve/resync: syncs=%d observes=%d", dirSyncs, calls)
	}
}

func TestRetirementRejectsWrongIdentityBeforeSettleAndHeldLock(t *testing.T) {
	c := testCreator(t)
	record := observedOperation(t, &c)
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	wrong := record
	wrong.OperationID = "ffffffffffffffffffffffffffffffff"
	called := false
	if err := c.Retire(context.Background(), wrong, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("wrong operation entered live callback")
	}
	wrong = record
	wrong.Spec.MetadataPath = "/private/other/meta.db"
	if err := c.Retire(context.Background(), wrong, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("wrong spec entered live callback")
	}
	wrong = record
	wrong.BootID = "different-boot"
	if err := c.Retire(context.Background(), wrong, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("wrong boot entered live callback")
	}
	wrong = record
	altered := *record.Mount
	altered.FSID = [2]int32{99, 100}
	wrong.Mount = &altered
	if err := c.Retire(context.Background(), wrong, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("wrong retained FSID entered live callback")
	}
	wrong = record
	wrong.Mount = nil
	if err := c.Retire(context.Background(), wrong, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("persisted mount identity was omitted")
	}
	j, err := openJournal(c.Directory, record.Spec.MountPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := c.Retire(context.Background(), record, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("held per-path lock admitted retirement")
	}
}

func TestRetirementAcceptsLiveEnrichedIdentityAfterReceiptSyncFailure(t *testing.T) {
	c := testCreator(t)
	mounted := false
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		if mounted {
			return testSnapshot(testEntry()), nil
		}
		return testSnapshot(), nil
	}
	dirSyncs := 0
	c.syncDir = func(f *os.File) error {
		dirSyncs++
		if dirSyncs == 2 {
			return errors.New("receipt directory sync failed")
		}
		return f.Sync()
	}
	result := c.CreateBound(context.Background(), testSpec(), func(Record) error { mounted = true; return nil })
	if result.State != Unknown || !result.Attempted || result.Record == nil || result.Record.Mount != nil {
		t.Fatalf("expected uncertain receipt with original live intent: %+v", result)
	}
	if row := readRecord(t, c); row.State != Observed || row.Mount == nil {
		t.Fatalf("receipt rename not available for reconciliation: %+v", row)
	}
	enriched := *result.Record
	mount := testEntry()
	enriched.Mount = &mount
	mounted = false
	settled := 0
	if err := c.Retire(context.Background(), enriched, func() error { settled++; return nil }); err != nil {
		t.Fatalf("retained identity did not reconcile receipt sync uncertainty: %v", err)
	}
	if settled != 1 || readRecord(t, c).State != Retired {
		t.Fatal("reconciled operation was not durably retired")
	}
}

func TestRetirementFileSyncFailureCanRetrySameOperation(t *testing.T) {
	c := testCreator(t)
	record := observedOperation(t, &c)
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	fileSyncs := 0
	c.syncFile = func(f *os.File) error {
		fileSyncs++
		if fileSyncs == 1 {
			return errors.New("retirement file sync failed")
		}
		return f.Sync()
	}
	if err := c.Retire(context.Background(), record, func() error { return nil }); err == nil {
		t.Fatal("file sync failure reported retirement")
	}
	if row := readRecord(t, c); row.State != Observed {
		t.Fatalf("failed pre-rename write altered record: %+v", row)
	}
	if err := c.Retire(context.Background(), record, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if fileSyncs != 2 || readRecord(t, c).State != Retired {
		t.Fatal("same-operation file sync retry did not settle")
	}
}

func TestRetiredPredecessorStillRefusesOldSourceOrFSIDElsewhere(t *testing.T) {
	for _, kind := range []string{"source", "fsid"} {
		t.Run(kind, func(t *testing.T) {
			c := testCreator(t)
			record := observedOperation(t, &c)
			c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
			if err := c.Retire(context.Background(), record, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			other := testEntry()
			other.Path = "/Volumes/elsewhere"
			if kind == "source" {
				other.FSID = [2]int32{80, 90}
			} else {
				other.Source = "foreign:/export"
			}
			c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(other), nil }
			attempts := 0
			r := c.CreateBound(context.Background(), testSpec(), func(Record) error { attempts++; return nil })
			if r.Err == nil || r.Attempted || attempts != 0 || readRecord(t, c).State != Retired {
				t.Fatalf("old identity elsewhere admitted new effect: %+v attempts=%d", r, attempts)
			}
		})
	}
}

func TestRetiredPredecessorNewIntentSyncFailureStartsNoMount(t *testing.T) {
	for _, point := range []string{"file", "directory"} {
		t.Run(point, func(t *testing.T) {
			c := testCreator(t)
			record := observedOperation(t, &c)
			c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
			if err := c.Retire(context.Background(), record, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if point == "file" {
				c.syncFile = func(*os.File) error { return errors.New("new intent file sync failed") }
			}
			if point == "directory" {
				c.syncDir = func(*os.File) error { return errors.New("new intent directory sync failed") }
			}
			attempts := 0
			r := c.CreateBound(context.Background(), testSpec(), func(Record) error { attempts++; return nil })
			if r.Err == nil || r.Attempted || attempts != 0 {
				t.Fatalf("new mount ran before durable successor intent: %+v attempts=%d", r, attempts)
			}
		})
	}
}

func TestNeverAdmittedIntentCanRetireOnlyThroughLiveSettlement(t *testing.T) {
	c := testCreator(t)
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	result := c.CreateBound(context.Background(), testSpec(), func(Record) error { return errors.New("command returned without mount") })
	if !result.Attempted || result.Record == nil || result.Record.Mount != nil || readRecord(t, c).State != InFlight {
		t.Fatalf("expected uncertain never-admitted intent: %+v", result)
	}
	if err := c.Retire(context.Background(), *result.Record, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	row := readRecord(t, c)
	if row.State != Retired || row.Mount != nil {
		t.Fatalf("never-admitted retirement forged mount: %+v", row)
	}
}

func TestMalformedRetiredOperationCannotAdmitNewEffect(t *testing.T) {
	c := testCreator(t)
	j, err := openJournal(c.Directory, testSpec().MountPath)
	if err != nil {
		t.Fatal(err)
	}
	rec := Record{Version: 1, OperationID: strings.Repeat("z", 32), BootID: "boot-a", Spec: testSpec(), State: Retired}
	if err := j.write(rec); err != nil {
		j.close()
		t.Fatal(err)
	}
	j.close()
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		t.Fatal("malformed retired row reached observer")
		return mounttable.Snapshot{}, nil
	}
	called := false
	r := c.CreateBound(context.Background(), testSpec(), func(Record) error { called = true; return nil })
	if r.Err == nil || r.Attempted || called {
		t.Fatalf("malformed retired operation admitted effect: %+v", r)
	}
}

func TestRetirementFreezesExpectedMountAcrossSettle(t *testing.T) {
	c := testCreator(t)
	record := observedOperation(t, &c)
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	original := *record.Mount
	if err := c.Retire(context.Background(), record, func() error {
		record.Mount.FSID = [2]int32{99, 100}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	row := readRecord(t, c)
	if row.Mount == nil || *row.Mount != original {
		t.Fatalf("callback changed recorded mount identity: %+v", row)
	}
}

func TestRetiredOldBootIgnoresReusedFSIDButNeverOldSource(t *testing.T) {
	c := testCreator(t)
	record := observedOperation(t, &c)
	c.Observe = func(context.Context) (mounttable.Snapshot, error) { return testSnapshot(), nil }
	if err := c.Retire(context.Background(), record, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	foreign := testEntry()
	foreign.Path = "/Volumes/unrelated"
	foreign.Source = "other:/export"
	// Its FSID is reused after reboot, without reviving the old source.
	before := testSnapshot(foreign)
	before.BootID = "boot-b"
	after := testSnapshot(testEntry())
	after.BootID = "boot-b"
	observes := 0
	c.Observe = func(context.Context) (mounttable.Snapshot, error) {
		observes++
		if observes == 1 {
			return before, nil
		}
		return after, nil
	}
	started := false
	r := c.CreateBound(context.Background(), testSpec(), func(intent Record) error {
		started = true
		if intent.BootID != "boot-b" {
			t.Fatalf("successor bound wrong boot: %+v", intent)
		}
		return nil
	})
	if err := r.Failure(); err != nil || !started {
		t.Fatalf("retired cross-boot successor refused reused FSID: %+v", r)
	}
}
