package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
)

func TestOwnerRetirementFailedSyncRetainsFence(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}
	i, _, err := createOwnerIntent(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	sentinel := errors.New("injected directory sync failure")
	l.syncDirectory = func() error { return sentinel }
	if err := l.publishRetired(i, ownerShutdownID(i)); !errors.Is(err, sentinel) {
		t.Fatalf("sync failure lost: %v", err)
	}
	if _, born, err := createOwnerIntent(scope, cfg); err == nil || born {
		t.Fatal("failed durability released successor authority")
	}
	l.syncDirectory = nil
	if err := l.publishRetired(i, ownerShutdownID(i)); err != nil {
		t.Fatal(err)
	}
	l.close()
	if err := confirmOwnerRetired(i); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerRetirementRequiresRecordAndReleasedLifetime(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}
	original, created, err := createOwnerIntent(scope, cfg)
	if err != nil || !created {
		t.Fatalf("first selection: created=%v err=%v", created, err)
	}
	lifetime, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer lifetime.close()
	if err := confirmOwnerRetired(original); err == nil {
		t.Fatal("active owner reported retired")
	}
	if same, born, err := createOwnerIntent(scope, cfg); err != nil || born || same != original {
		t.Fatalf("normal reconnect did not preserve active identity: born=%v err=%v", born, err)
	}
	if err := lifetime.publishRetired(original, ownerShutdownID(original)); err != nil {
		t.Fatal(err)
	}
	if err := confirmOwnerRetired(original); err == nil {
		t.Fatal("retirement record bypassed held lifetime")
	}
	if _, born, err := createOwnerIntent(scope, cfg); err == nil || born {
		t.Fatal("successor admitted before lifetime release")
	}
	lifetime.close()
	if err := confirmOwnerRetired(original); err != nil {
		t.Fatal(err)
	}
	next, born, err := createOwnerIntent(scope, cfg)
	if err != nil || !born || next.Instance == original.Instance || next.ClaimID == original.ClaimID || next.MountID == original.MountID {
		t.Fatalf("settled owner did not admit fresh identity: born=%v err=%v", born, err)
	}
	if _, err := readOwnerRetirement(scope, original); err != nil {
		t.Fatalf("predecessor terminal record lost: %v", err)
	}
	if same, born, err := createOwnerIntent(scope, cfg); err != nil || born || same != next {
		t.Fatal("unlaunched selected successor was replaced")
	}
}

func TestOwnerRetirementRejectsPartialOrForeignReceipt(t *testing.T) {
	for _, kind := range []string{"partial", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			scope := appisolation.Scope{Root: t.TempDir()}
			cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}
			i, _, err := createOwnerIntent(scope, cfg)
			if err != nil {
				t.Fatal(err)
			}
			b := []byte("{")
			if kind == "foreign" {
				other := i
				other.ClaimID = "11111111111111111111111111111111"
				b, err = json.Marshal(ownerRetirement{Version: 1, Intent: other, ShutdownID: ownerShutdownID(other)})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(scope.Root, ownerRetirementLeaf(i)), b, 0600); err != nil {
				t.Fatal(err)
			}
			if err := confirmOwnerRetired(i); err == nil {
				t.Fatal("invalid receipt authorized retirement")
			}
			if _, born, err := createOwnerIntent(scope, cfg); err == nil || born {
				t.Fatal("invalid receipt authorized successor")
			}
			if current, err := readOwnerIntent(scope); err != nil || current != i {
				t.Fatal("failed retirement changed active intent")
			}
		})
	}
}

func TestOwnerRetirementRejectsChangedSelectedIntent(t *testing.T) {
	scope := appisolation.Scope{Root: t.TempDir()}
	cfg := ServerConfig{RedisURL: "redis://never-contacted.invalid:6379"}
	i, _, err := createOwnerIntent(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	foreign, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.atomicRecord(filepath.Base(ownerIntentPath(scope.Root)), foreign); err != nil {
		t.Fatal(err)
	}
	if err := l.publishRetired(i, ownerShutdownID(i)); err == nil {
		t.Fatal("old owner published retirement against a successor selection")
	}
	if _, err := readOwnerRetirement(scope, i); !os.IsNotExist(err) {
		t.Fatal("rejected publication left a retirement record")
	}
}
