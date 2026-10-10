package nfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestRecoverOnBootRefusesBoundBirthBeforeAnyMutation(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	store.SetSpoolStore(meta)
	root := t.TempDir()
	spool, err := NewSpoolStore(root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Stop()
	bornFile := filepath.Join(root, SpoolFilesSubdir, "born-private")
	const body = "last private source bytes"
	if err := os.WriteFile(bornFile, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	expected, err := store.CaptureMirror("born.mov")
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := metadata.NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	receipt, born, err := store.InsertBoundSpoolBirth(expected, metadata.MakeEntry("born.mov", false, int64(len(body)), time.Now(), 1001), bornFile, nonce)
	if err != nil {
		t.Fatal(err)
	}
	legacyFile := filepath.Join(root, SpoolFilesSubdir, "legacy-ready")
	if err := os.WriteFile(legacyFile, []byte("legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	legacyID, err := meta.Insert("legacy.mov", legacyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.MarkReady(legacyID, 6, nil); err != nil {
		t.Fatal(err)
	}
	spool.used.Store(23)
	for attempt := 0; attempt < 2; attempt++ {
		var pending *metadata.PendingSpoolBirthRecovery
		if err := store.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Births) != 1 || pending.Births[0].BirthID != nonce {
			t.Fatalf("attempt %d preflight=%v, want exact birth refusal", attempt, err)
		}
		report, err := spool.RecoverOnBoot(context.Background())
		if !errors.As(err, &pending) || report != (RecoveryReport{}) {
			t.Fatalf("attempt %d direct recovery=(%+v,%v), want zero-work refusal", attempt, report, err)
		}
		if got, err := os.ReadFile(bornFile); err != nil || string(got) != body {
			t.Fatalf("attempt %d source bytes=(%q,%v)", attempt, got, err)
		}
		row, err := meta.Get(receipt.SpoolID)
		if err != nil || row.DrainState != metadata.DrainWriting || row.SpoolFile != bornFile {
			t.Fatalf("attempt %d birth row=(%+v,%v)", attempt, row, err)
		}
		legacy, err := meta.Get(legacyID)
		if err != nil || legacy.DrainState != metadata.DrainReady {
			t.Fatalf("attempt %d legacy index=(%+v,%v)", attempt, legacy, err)
		}
		if got := store.LookupByPath("born.mov"); got == nil || got.Version != born.Version || got.Size != int64(len(body)) {
			t.Fatalf("attempt %d mirror changed: %+v", attempt, got)
		}
		if used, _ := spool.Capacity(); used != 23 {
			t.Fatalf("attempt %d capacity changed: %d", attempt, used)
		}
	}
}
