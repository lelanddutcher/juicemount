package nfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestH4RecoveryRefusesLegacyDrainingBeforeReset(t *testing.T) {
	spool, root, db := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	bytes := []byte("filmmaker footage still on this Mac")
	file := dropFile(t, root, "draining-fault", bytes)
	id, err := spool.Meta().Insert("/clip.mov", file)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().MarkReady(id, int64(len(bytes)), nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := spool.Meta().MarkDraining(id); err != nil || !claimed {
		t.Fatalf("mark draining = (%v, %v)", claimed, err)
	}
	before, err := spool.Meta().Get(id)
	if err != nil {
		t.Fatal(err)
	}
	spool.used.Store(17)
	// This trigger distinguishes an attempted reset from the required early
	// custody refusal. The trigger must remain unreachable in this test.
	if _, err := db.Exec(`CREATE TRIGGER h4_recovery_reset_fault BEFORE UPDATE ON spool_entries
		WHEN OLD.drain_state = 'draining' AND NEW.drain_state = 'ready'
		BEGIN SELECT RAISE(ABORT, 'h4 injected reset failure'); END`); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		report, err := spool.RecoverOnBoot(context.Background())
		var pending *metadata.PendingDrainCompletion
		if !errors.As(err, &pending) || pending == nil {
			t.Fatalf("attempt %d recovery error = %v, want PendingDrainCompletion", attempt, err)
		}
		want := metadata.DrainCompletionHold{SpoolID: id, NFSPath: "/clip.mov", SpoolFile: file, Size: int64(len(bytes))}
		if len(pending.Rows) != 1 || pending.Rows[0] != want {
			t.Fatalf("attempt %d pending rows = %+v, want exactly %+v", attempt, pending.Rows, want)
		}
		if report != (RecoveryReport{}) {
			t.Fatalf("attempt %d partial recovery report = %+v", attempt, report)
		}
		row, err := spool.Meta().Get(id)
		if err != nil || !reflect.DeepEqual(row, before) {
			t.Fatalf("attempt %d changed draining row = (%+v, %v), want %+v", attempt, row, err, before)
		}
		got, err := os.ReadFile(file)
		if err != nil || !slices.Equal(got, bytes) {
			t.Fatalf("attempt %d changed spool bytes = (%q, %v)", attempt, got, err)
		}
		if used, _ := spool.Capacity(); used != 17 {
			t.Fatalf("attempt %d changed capacity = %d, want 17", attempt, used)
		}
	}
}

func TestH4DrainingResetSQLFailurePreservesSource(t *testing.T) {
	spool, root, db := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	bytes := []byte("filmmaker footage still on this Mac")
	file := dropFile(t, root, "draining-fault", bytes)
	id, err := spool.Meta().Insert("/clip.mov", file)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().MarkReady(id, int64(len(bytes)), nil); err != nil {
		t.Fatal(err)
	}
	if claimed, err := spool.Meta().MarkDraining(id); err != nil || !claimed {
		t.Fatalf("mark draining = (%v, %v)", claimed, err)
	}
	if _, err := db.Exec(`CREATE TRIGGER h4_recovery_reset_fault BEFORE UPDATE ON spool_entries
		WHEN OLD.drain_state = 'draining' AND NEW.drain_state = 'ready'
		BEGIN SELECT RAISE(ABORT, 'h4 injected reset failure'); END`); err != nil {
		t.Fatal(err)
	}
	// Boot recovery refuses legacy draining rows before attempting a reset.
	// Exercise the SQL transition directly to retain its failure coverage.
	if err := spool.Meta().ResetToReady(id); err == nil || !strings.Contains(err.Error(), "h4 injected reset failure") {
		t.Fatalf("reset error = %v, want injected reset failure", err)
	}
	row, err := spool.Meta().Get(id)
	if err != nil || row.DrainState != metadata.DrainDraining {
		t.Fatalf("row after failed reset = (%+v, %v), want draining", row, err)
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != string(bytes) {
		t.Fatalf("spool bytes after failed reset = (%q, %v)", got, err)
	}
}

func TestH4RecoveryRefusesUnreadableFilesDirectory(t *testing.T) {
	spool, root, _ := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	bytes := []byte("last local copy")
	file := dropFile(t, root, "read-dir-fault", bytes)
	id, err := spool.Meta().Insert("/clip.mov", file)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().MarkReady(id, int64(len(bytes)), nil); err != nil {
		t.Fatal(err)
	}
	before, err := spool.Meta().Get(id)
	if err != nil {
		t.Fatal(err)
	}
	blockedRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(blockedRoot, SpoolFilesSubdir), []byte("block ReadDir"), 0600); err != nil {
		t.Fatal(err)
	}
	spool.root = blockedRoot
	t.Cleanup(func() { spool.root = root })
	if _, err := spool.RecoverOnBoot(context.Background()); err == nil || !strings.Contains(err.Error(), "read spool files dir") {
		t.Fatalf("recovery error = %v, want ReadDir refusal", err)
	}
	row, err := spool.Meta().Get(id)
	if err != nil || !reflect.DeepEqual(row, before) {
		t.Fatalf("row after failed ReadDir = (%+v, %v), want unchanged %+v", row, err, before)
	}
	got, err := os.ReadFile(file)
	if err != nil || !slices.Equal(got, bytes) {
		t.Fatalf("spool bytes after failed ReadDir = (%q, %v)", got, err)
	}
}

func TestH4RecoveryRefusesMissingFilesDirectoryWithRows(t *testing.T) {
	spool, root, _ := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	bytes := []byte("last local copy")
	file := dropFile(t, root, "missing-dir-fault", bytes)
	id, err := spool.Meta().Insert("/clip.mov", file)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().MarkReady(id, int64(len(bytes)), nil); err != nil {
		t.Fatal(err)
	}
	before, err := spool.Meta().Get(id)
	if err != nil {
		t.Fatal(err)
	}
	spool.root = t.TempDir() // valid private root; only its files child is absent
	t.Cleanup(func() { spool.root = root })
	if _, err := spool.RecoverOnBoot(context.Background()); err == nil || !strings.Contains(err.Error(), "missing with 1 recorded rows") {
		t.Fatalf("recovery error = %v, want missing-directory refusal", err)
	}
	row, err := spool.Meta().Get(id)
	if err != nil || !reflect.DeepEqual(row, before) {
		t.Fatalf("row after missing directory = (%+v, %v), want unchanged %+v", row, err, before)
	}
	got, err := os.ReadFile(file)
	if err != nil || !slices.Equal(got, bytes) {
		t.Fatalf("spool bytes after missing directory = (%q, %v)", got, err)
	}
}

func TestH4RecoveryPreservesWritingFileOnHashReadError(t *testing.T) {
	spool, root, _ := newSpoolStoreForRecovery(t)
	t.Cleanup(spool.Stop)
	bytes := []byte("committed bytes before writer close")
	file := dropFile(t, root, "hash-fault", bytes)
	id, err := spool.Meta().Insert("/clip.mov", file)
	if err != nil {
		t.Fatal(err)
	}
	spool.hashFileOnRecovery = func(string) ([]byte, int64, error) {
		return nil, 0, errors.New("injected spool read failure")
	}
	if _, err := spool.RecoverOnBoot(context.Background()); err == nil || !strings.Contains(err.Error(), "injected spool read failure") {
		t.Fatalf("recovery error = %v, want read failure", err)
	}
	row, err := spool.Meta().Get(id)
	if err != nil || row.DrainState != metadata.DrainWriting {
		t.Fatalf("row after hash error = (%+v, %v), want writing", row, err)
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != string(bytes) {
		t.Fatalf("spool bytes after hash error = (%q, %v)", got, err)
	}
}
