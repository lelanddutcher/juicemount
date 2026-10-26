package catalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func TestDeliveryLifecycleHostSchemaGate(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	library := continuity.LibraryID("lib_11111111111111111111111111111111")
	old, err := CreateDelivery(context.Background(), filepath.Join(dir, "old.sqlite"), library)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.RequireDeliveryLifecycle(context.Background()); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema 7 delivery host admitted: %v", err)
	}
	if old.version != 7 {
		t.Fatalf("schema gate mutated old profile: %d", old.version)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	current, err := CreateDeliveryLifecycle(context.Background(), filepath.Join(dir, "current.sqlite"), library)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.RequireDeliveryLifecycle(context.Background()); err != nil {
		t.Fatalf("schema 8 refused: %v", err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := current.RequireDeliveryLifecycle(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed catalog admitted delivery host: %v", err)
	}
}
