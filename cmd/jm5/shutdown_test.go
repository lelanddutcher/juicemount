package main

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

func TestServerOnlyShutdownQuiescesAcceptedClient(t *testing.T) {
	store, err := openCheckedStore(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := jmnfs.NewServer(jmnfs.Config{ListenAddr: "127.0.0.1:0"}, store)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if !prepareServerOnlyShutdown(server, time.Second) {
		t.Fatal("accepted client did not quiesce")
	}
	if err := server.Start(); err == nil {
		t.Fatal("retired server accepted a restart")
	}
}
