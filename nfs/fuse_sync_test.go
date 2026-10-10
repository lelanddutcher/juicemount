package nfs

import (
	"os"
	"testing"
)

func TestFuseDurableFileSyncPersistsBytes(t *testing.T) {
	path := t.TempDir() + "/checkpoint.bin"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := fuseDurableFile{File: f}
	if _, err := wrapped.Write([]byte("durable checkpoint")); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := wrapped.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("FUSE durability sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "durable checkpoint" {
		t.Fatalf("persisted bytes = %q", got)
	}
}

func TestFuseDurableFileSyncRejectsNil(t *testing.T) {
	if err := (fuseDurableFile{}).Sync(); err == nil {
		t.Fatal("nil fuse durable file sync succeeded")
	}
}
