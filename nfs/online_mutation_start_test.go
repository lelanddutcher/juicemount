package nfs

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestOnlineMutationStartRefusesBeforeListenAndFastReturn(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: t.TempDir()}, store)
	if _, err := store.DB().Exec(`INSERT INTO online_mutations VALUES
	 ('00112233445566778899aabbccddeeff','truncate','reel.mov','store','inc',1,'backing','mount',8192,0,0,0)`); err != nil {
		t.Fatal(err)
	}
	var pending *metadata.PendingOnlineMutation
	server.running.Store(true)
	if err := server.Start(); !errors.As(err, &pending) {
		t.Fatalf("running fast-return accepted pending intent: %v", err)
	}
	server.running.Store(false)
	if err := server.Start(); !errors.As(err, &pending) {
		t.Fatalf("new listener accepted pending intent: %v", err)
	}
	if server.listener != nil || server.running.Load() {
		t.Fatal("Start published a listener while recovery is pending")
	}
}
