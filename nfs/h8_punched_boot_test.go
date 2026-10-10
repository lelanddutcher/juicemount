package nfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestH8PunchedBootRefusesBeforeRecoveryMutationAndListener(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	spool, err := NewSpoolStore(root, 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Stop()
	plain := filepath.Join(root, SpoolFilesSubdir, "plain")
	punched := filepath.Join(root, SpoolFilesSubdir, "punched")
	if err := os.WriteFile(plain, []byte("ordinary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(punched, []byte("\x00\x00\x00\x00tail"), 0o600); err != nil {
		t.Fatal(err)
	}
	plainID, err := spool.Meta().Insert("plain.mov", plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().MarkReady(plainID, 8, nil); err != nil {
		t.Fatal(err)
	}
	punchedID, err := spool.Meta().Insert("punched.mov", punched)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().SetPunchedEnd(punchedID, 4); err != nil {
		t.Fatal(err)
	}
	spool.used.Store(23)

	var pending *metadata.PendingPunchedSpool
	if err := store.CheckSpoolRecovery(); !errors.As(err, &pending) ||
		len(pending.Rows) != 1 || pending.Rows[0].ID != punchedID {
		t.Fatalf("store admission = %v, want punched-row refusal", err)
	}
	if report, err := spool.RecoverOnBoot(context.Background()); !errors.As(err, &pending) ||
		report != (RecoveryReport{}) {
		t.Fatalf("recovery = (%+v, %v), want zero-work refusal", report, err)
	}
	if used, _ := spool.Capacity(); used != 23 {
		t.Fatalf("recovery reset capacity before refusal: %d", used)
	}
	for _, tc := range []struct {
		id   int64
		want metadata.DrainState
	}{
		{plainID, metadata.DrainReady},
		{punchedID, metadata.DrainWriting},
	} {
		row, err := spool.Meta().Get(tc.id)
		if err != nil || row.DrainState != tc.want {
			t.Fatalf("row %d changed by recovery: (%+v, %v)", tc.id, row, err)
		}
	}
	if got, err := os.ReadFile(punched); err != nil || string(got) != "\x00\x00\x00\x00tail" {
		t.Fatalf("punched source changed: (%q, %v)", got, err)
	}

	server := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: root}, store)
	if err := server.Start(); !errors.As(err, &pending) {
		t.Fatalf("direct server start = %v, want punched-row refusal", err)
	}
	if server.IsRunning() || server.listener != nil || server.protocol != nil {
		t.Fatal("direct server start published a listener after refusal")
	}
}

func TestH8AlreadyRunningServerStartDoesNotReclassifyPunchedRowAsCold(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool := metadata.NewSpoolStore(store.DB())
	id, err := spool.Insert("active.mov", filepath.Join(t.TempDir(), "active-source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.SetPunchedEnd(id, 2); err != nil {
		t.Fatal(err)
	}
	server := NewServer(Config{ListenAddr: "127.0.0.1:0"}, store)
	server.running.Store(true)
	if err := server.Start(); err != nil {
		t.Fatalf("idempotent Start refused an already-served punched row: %v", err)
	}
}

func TestH8BootInventoryExcludesNewlyAdmittedStreamStage(t *testing.T) {
	spool, root, _ := newSpoolStoreForRecovery(t)
	if _, err := spool.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, err := spool.Meta().Insert("new.mov", filepath.Join(root, "new-source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().SetPunchedEnd(id, 2); err != nil {
		t.Fatal(err)
	}
	stageRoot := t.TempDir()
	stage, err := streamTempPath(filepath.Join(stageRoot, "new.mov"), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stage, []byte("new writer in progress"), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: stageRoot})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SweepStreamPartials(); err != nil {
		t.Fatalf("new writer was classified as a boot orphan: %v", err)
	}
	if got, err := os.ReadFile(stage); err != nil || string(got) != "new writer in progress" {
		t.Fatalf("new writer stage changed: (%q, %v)", got, err)
	}
}

func TestH8PunchedSweepRefusesAndPreservesAllStages(t *testing.T) {
	spool, root, _ := newSpoolStoreForRecovery(t)
	stageRoot := t.TempDir()
	punchedSource := dropFile(t, root, "punched-source", []byte("\x00\x00tail"))
	punchedID, err := spool.Meta().Insert("punched.mov", punchedSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Meta().SetPunchedEnd(punchedID, 2); err != nil {
		t.Fatal(err)
	}
	punchedStage, err := streamTempPath(filepath.Join(stageRoot, "punched.mov"), punchedID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(punchedStage, []byte("abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	plainSource := dropFile(t, root, "plain-source", []byte("plain"))
	plainID, err := spool.Meta().Insert("plain.mov", plainSource)
	if err != nil {
		t.Fatal(err)
	}
	plainStage, err := streamTempPath(filepath.Join(stageRoot, "plain.mov"), plainID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plainStage, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: stageRoot})
	if err != nil {
		t.Fatal(err)
	}
	var pending *metadata.PendingPunchedSpool
	if err := d.SweepStreamPartials(); !errors.As(err, &pending) {
		t.Fatalf("sweep = %v, want typed refusal", err)
	}
	for _, name := range []string{punchedStage, plainStage} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("sweep removed %s before refusal: %v", name, err)
		}
	}
	rows, err := spool.Meta().ListAll()
	if err != nil {
		t.Fatal(err)
	}
	candidates := streamPartialCandidates(rows, stageRoot)
	if len(candidates) != 1 || candidates[0] != plainStage {
		t.Fatalf("candidate filter = %v, want only unpunched stage", candidates)
	}
	if _, err := os.Stat(punchedStage); err != nil {
		t.Fatalf("candidate filter removed punched stage: %v", err)
	}
	if _, err := os.Stat(plainStage); err != nil {
		t.Fatalf("candidate filter removed unpunched stage: %v", err)
	}
}
