package main

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestOpenCheckedStoreRefusesPersistedPublication(t *testing.T) {
	for _, survivingRow := range []bool{false, true} {
		name := "without spool row"
		if survivingRow {
			name = "with spool row"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata.db")
			store, err := openCheckedStore(path)
			if err != nil {
				t.Fatal(err)
			}
			id := int64(733)
			if survivingRow {
				id, err = metadata.NewSpoolStore(store.DB()).Insert("reel.mov", filepath.Join(t.TempDir(), "reel"))
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation,phase,stage_path,publish_path)
				VALUES (?, 'original-incarnation', 'publishing', '/stage/reel', 'reel.mov')`, id); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			checked, err := openCheckedStore(path)
			if checked != nil {
				checked.Close()
				t.Fatal("published a store with unresolved spool publication")
			}
			var pending *metadata.PendingSpoolPublication
			if !errors.As(err, &pending) || len(pending.Paths) != 1 || pending.Paths[0] != "reel.mov" {
				t.Fatalf("lost typed recovery refusal: %v", err)
			}
			store, err = metadata.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.CheckSpoolRecovery(); !errors.As(err, &pending) {
				t.Fatalf("publication marker changed after refusal: %v", err)
			}
		})
	}
}

func TestRunCLIStartupOrdersRecoverySyncAndPublication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prepareErr bool
		fuseErr    bool
		listenErr  bool
		adopted    bool
		want       []string
	}{
		{name: "recovery refusal", prepareErr: true,
			want: []string{"recover", "retire listener", "quiesce clients", "stop handler", "stop redis", "close store"}},
		{name: "FUSE failure", fuseErr: true,
			want: []string{"recover", "fuse", "retire listener", "quiesce clients", "stop handler", "rollback fuse", "stop spool", "stop redis", "close store"}},
		{name: "listener failure", listenErr: true,
			want: []string{"recover", "fuse", "sync", "listen", "retire listener", "quiesce clients", "stop handler", "rollback fuse", "stop spool", "stop redis", "close store"}},
		{name: "adopted mount listener failure", listenErr: true, adopted: true,
			want: []string{"recover", "fuse", "sync", "listen", "retire listener", "quiesce clients", "stop handler", "stop spool", "stop redis", "close store"}},
		{name: "successful startup",
			want: []string{"recover", "fuse", "sync", "listen", "monitor", "workers"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			var cleanup cliStartupCleanup
			cleanup.add(func() { order = append(order, "close store") })
			cleanup.add(func() { order = append(order, "stop redis") })
			created := false
			err := runCLIStartup(func() error {
				order = append(order, "recover")
				if tc.prepareErr {
					return errors.New("recovery refused")
				}
				cleanup.add(func() { order = append(order, "stop spool") })
				return nil
			}, func() error {
				order = append(order, "fuse")
				created = !tc.adopted
				if tc.fuseErr {
					return errors.New("FUSE refused")
				}
				return nil
			}, func() {
				order = append(order, "sync")
			}, func() error {
				order = append(order, "listen")
				if tc.listenErr {
					return errors.New("listener refused")
				}
				return nil
			}, func() {
				order = append(order, "monitor")
				order = append(order, "workers")
			}, func(err error) error {
				return failCLIStartup(err, func() error {
					order = append(order, "retire listener")
					return nil
				}, func() bool {
					order = append(order, "quiesce clients")
					return true
				}, func() bool {
					order = append(order, "stop handler")
					return true
				}, func() error {
					if created {
						order = append(order, "rollback fuse")
					}
					return nil
				}, &cleanup)
			})
			if (err != nil) != (tc.prepareErr || tc.fuseErr || tc.listenErr) {
				t.Fatalf("startup error = %v", err)
			}
			if !reflect.DeepEqual(order, tc.want) {
				t.Fatalf("startup stages = %v, want %v", order, tc.want)
			}
		})
	}
}

func TestCLIStartupFailureRetainsDependenciesWhenHandlerCannotStop(t *testing.T) {
	var order []string
	var cleanup cliStartupCleanup
	cleanup.add(func() { order = append(order, "close store") })
	cleanup.add(func() { order = append(order, "stop spool") })
	err := failCLIStartup(errors.New("listener refused"), func() error {
		order = append(order, "retire listener")
		return nil
	}, func() bool {
		order = append(order, "quiesce clients")
		return true
	}, func() bool {
		order = append(order, "stop handler")
		return false
	}, func() error {
		order = append(order, "rollback fuse")
		return nil
	}, &cleanup)
	if err == nil || !reflect.DeepEqual(order, []string{"retire listener", "quiesce clients", "stop handler"}) {
		t.Fatalf("blocked handler cleanup = %v, order = %v", err, order)
	}
}

func TestCLIStartupFailureRetainsDependenciesWhenListenerOrClientsRemain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retireErr error
		quiesce   bool
		want      []string
	}{
		{"listener remains", errors.New("close failed"), true, []string{"retire listener"}},
		{"clients remain", nil, false, []string{"retire listener", "quiesce clients"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			var cleanup cliStartupCleanup
			cleanup.add(func() { order = append(order, "close store") })
			err := failCLIStartup(errors.New("startup failed"), func() error {
				order = append(order, "retire listener")
				return tc.retireErr
			}, func() bool {
				order = append(order, "quiesce clients")
				return tc.quiesce
			}, func() bool {
				order = append(order, "stop handler")
				return true
			}, func() error {
				order = append(order, "rollback fuse")
				return nil
			}, &cleanup)
			if err == nil || !reflect.DeepEqual(order, tc.want) {
				t.Fatalf("incomplete cleanup = %v, order = %v, want %v", err, order, tc.want)
			}
		})
	}
}

func TestOpenCheckedStoreQueryFailureCanRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	store, err := openCheckedStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE spool_bindings RENAME COLUMN phase TO phase_missing`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	checked, err := openCheckedStore(path)
	if checked != nil || err == nil {
		if checked != nil {
			checked.Close()
		}
		t.Fatalf("query failure accepted: %v", err)
	}
	var pending *metadata.PendingSpoolPublication
	if errors.As(err, &pending) {
		t.Fatalf("ordinary query failure became permanent marker: %v", err)
	}
	store, err = metadata.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`ALTER TABLE spool_bindings RENAME COLUMN phase_missing TO phase`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	checked, err = openCheckedStore(path)
	if err != nil {
		t.Fatalf("corrected retry failed: %v", err)
	}
	checked.Close()
}
