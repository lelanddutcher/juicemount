package nativeclient

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNativeProfileRecoveryPreservesPendingIntentAndRefusesCorruption(t *testing.T) {
	path, j, v := nativeJournalTest(t)
	if err := j.CreateIntent(v.intent); err != nil {
		t.Fatal(err)
	}
	want, err := j.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := OpenJournal(path, v.intent.Endpoint, v.intent.LibraryID, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := fresh.RecoverProfile(); err != nil {
		t.Fatal(err)
	}
	got, err := fresh.Usage()
	if err != nil || got != want || got.IntentCount != 1 {
		t.Fatalf("profile recovery replaced reservation: %+v %+v %v", got, want, err)
	}
	leaf := filepath.Join(path, "staging", v.intent.CopyID, "intent.json")
	if err := os.WriteFile(leaf, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := fresh.RecoverProfile(); err == nil {
		t.Fatal("profile recovery accepted corrupt custody record")
	}
}
