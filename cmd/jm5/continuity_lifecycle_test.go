package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
	"github.com/lelanddutcher/juicemount/internal/manager"
)

func TestContinuityEmbeddedSettingsPreservePrecedence(t *testing.T) {
	for _, tc := range []struct{ roots, oldRoots, key, oldKey, env, wantRoots, wantKey string }{
		{"/current", "/old", "current", "old", "env", "/current", "current"},
		{"", "/old", "", "old", "env", "/old", "old"},
		{"", "", "", "", "env", "", "env"},
	} {
		roots, key := resolveContinuityManagerSettings(tc.roots, tc.oldRoots, tc.key, tc.oldKey, tc.env)
		if roots != tc.wantRoots || key != tc.wantKey {
			t.Fatalf("legacy precedence changed: %q %q", roots, key)
		}
	}
}

func TestContinuityEmbeddedPreflightRefusesBeforeStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "never-create.sqlite")
	cfg := manager.ContinuityConfig{CatalogPath: path, LibraryID: "lib_11111111111111111111111111111111", ManagerAdminKey: "synthetic-manager-deployment-key-000000000000"}
	for _, roots := range []string{"", ", ,", "/synthetic-source"} {
		service, err := prepareEmbeddedContinuity(context.Background(), roots, cfg)
		if err == nil || service != nil {
			t.Fatal("invalid explicit preparation succeeded")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("preflight created storage")
		}
	}
	if service, err := prepareEmbeddedContinuity(context.Background(), "", manager.ContinuityConfig{}); err != nil || service != nil {
		t.Fatal("opt-out changed")
	}
}

func TestContinuityEmbeddedStartupCleanupJoinsErrorsInReverseOrder(t *testing.T) {
	initial, terminal := errors.New("startup"), errors.New("catalog close")
	var order []int
	var cleanup cliStartupCleanup
	cleanup.addError(func() error { order = append(order, 1); return terminal })
	cleanup.add(func() { order = append(order, 2) })
	cleanup.addError(func() error { order = append(order, 3); return nil })
	err := cleanup.fail(initial)
	if !errors.Is(err, initial) || !errors.Is(err, terminal) || !reflect.DeepEqual(order, []int{3, 2, 1}) || len(cleanup.steps) != 0 {
		t.Fatalf("cleanup result: %v %v", err, order)
	}
	cleanup.fail(nil)
	if len(order) != 3 {
		t.Fatal("cleanup repeated retired owners")
	}
}

func TestContinuityEmbeddedCloseRetriesOnlyIncomplete(t *testing.T) {
	calls := 0
	terminal := errors.New("terminal close")
	err := closeEmbeddedContinuity(func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.Join(catalog.ErrDiscoveryDraining, context.Canceled)
		}
		return terminal
	})
	if calls != 2 || !errors.Is(err, terminal) {
		t.Fatalf("close did not distinguish terminal: calls=%d err=%v", calls, err)
	}
}
