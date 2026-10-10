package main

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/lelanddutcher/juicemount/nfs"
)

func TestCLIReplacementRecoveryRequiresSpool(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		recovery, spoolEnabled bool
		wantError              bool
	}{
		{"default-off", false, false, false},
		{"ordinary-spool", false, true, false},
		{"recovery-without-spool", true, false, true},
		{"recovery-with-spool", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCLIReplacementRecovery(tc.recovery, tc.spoolEnabled); (err != nil) != tc.wantError {
				t.Fatalf("validation error=%v, want error=%t", err, tc.wantError)
			}
		})
	}
}

func TestCLIReplacementLeaseAttachmentAndRetainedCleanup(t *testing.T) {
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := nfs.NewSpoolStore(filepath.Join(t.TempDir(), "spool"), 1<<20, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Stop()
	drainer, err := nfs.NewDrainer(spool, nfs.DrainerConfig{FuseRoot: t.TempDir(), ReplacementRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	defer drainer.Stop(5 * time.Second)
	if leases, err := newCLIReplacementLeases(false, "", spool, drainer); err != nil || leases != nil || drainer.ProjectLeases() != nil {
		t.Fatalf("disabled recovery created a lease client: %v %v", leases, err)
	}
	for _, tc := range []struct {
		name    string
		spool   *nfs.SpoolStore
		drainer *nfs.Drainer
	}{
		{"nil-spool", nil, drainer},
		{"nil-drainer", spool, nil},
	} {
		if leases, err := newCLIReplacementLeases(true, "redis://127.0.0.1:1/3", tc.spool, tc.drainer); err == nil || leases != nil {
			t.Fatalf("%s admitted: %v %v", tc.name, leases, err)
		}
	}
	// Constructing and attaching does not dial the deliberately absent endpoint.
	leases, err := newCLIReplacementLeases(true, "redis://127.0.0.1:1/3", spool, drainer)
	if err != nil || leases == nil || drainer.ProjectLeases() != leases {
		t.Fatalf("lease client was not attached before Start: %v %v", leases, err)
	}
	var cleanup cliStartupCleanup
	stops := 0
	cleanup.add(func() { spool.Stop() })
	cleanup.add(func() { stops++; leases.Stop() })
	joined := false
	retire := func() error { return nil }
	quiesce := func() bool { return true }
	stopHandler := func() bool {
		if !joined {
			return false
		}
		return drainer.Stop(5 * time.Second)
	}
	rollback := func() error { return nil }
	failed := failCLIStartup(errors.New("listener failed"), retire, quiesce, stopHandler, rollback, &cleanup)
	if !errors.Is(failed, errCLICleanupIncomplete) || stops != 0 || len(cleanup.steps) != 2 {
		t.Fatalf("incomplete handler cleanup retired lease dependencies: %v stops=%d steps=%d", failed, stops, len(cleanup.steps))
	}
	joined = true
	_ = failCLIStartup(errors.New("listener failed"), retire, quiesce, stopHandler, rollback, &cleanup)
	if stops != 1 || len(cleanup.steps) != 0 {
		t.Fatalf("joined handler did not release lease once: stops=%d steps=%d", stops, len(cleanup.steps))
	}
}
