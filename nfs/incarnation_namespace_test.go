package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestNamespaceRenameRefusesRAMOnlyChildBeforeBackingMutation(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "scene/reel.mov", []byte("original"))
	child := store.LookupByPath("scene/reel.mov")
	if err := store.Delete(child.Path); err != nil {
		t.Fatal(err)
	}
	child.Version = metadata.MirrorToken{}
	store.InsertToCache(child)
	mirror(t, store, root, "scene", true)
	if err := store.Insert(store.LookupByPath("scene")); err != nil {
		t.Fatal(err)
	}
	if err := jfs.Rename("scene", "moved"); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("Rename = %v, want incomplete mirror conflict", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "scene/reel.mov")); err != nil || string(got) != "original" {
		t.Fatalf("preflight changed backing source: bytes=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "moved")); !os.IsNotExist(err) {
		t.Fatalf("preflight created backing destination: %v", err)
	}
	awaitNamespaceIdle(t, &jfs.handler.namespace)
}
