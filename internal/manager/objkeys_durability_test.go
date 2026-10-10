package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Both failures happen after rename: the new floor may be visible, but it
// cannot authorize a store or desired advance until the directory is synced.
func TestObjKeysFloorDirectoryFailureBlocksIntentAndRetries(t *testing.T) {
	for _, mode := range []string{"parent_open", "parent_sync"} {
		t.Run(mode, func(t *testing.T) {
			r := newObjRig(t)
			name, _ := r.enroll(97)
			storeBefore, err := os.ReadFile(r.statePath)
			if err != nil {
				t.Fatal(err)
			}
			desiredPath := filepath.Join(r.objDir, objKeyDesiredName)
			desiredBefore, err := os.ReadFile(desiredPath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { atomicFloorDirOpenHook, atomicFloorDirSyncHook = nil, nil }()
			injected := 0
			if mode == "parent_open" {
				atomicFloorDirOpenHook = func(dir string) (*os.File, error) {
					if dir == r.objDir {
						injected++
						return nil, errors.New("injected parent open failure")
					}
					return os.Open(dir)
				}
			} else {
				atomicFloorDirSyncHook = func(d *os.File) error {
					if d.Name() == r.objDir {
						injected++
						return errors.New("injected parent sync failure")
					}
					return d.Sync()
				}
			}
			firstErr := r.s.ensureObjKey(name)
			if injected != 1 || firstErr == nil || !strings.Contains(firstErr.Error(), "injected parent") {
				t.Fatalf("floor directory failure not reached once: calls=%d err=%v", injected, firstErr)
			}
			storeAfter, err := os.ReadFile(r.statePath)
			if err != nil {
				t.Fatal(err)
			}
			desiredAfter, err := os.ReadFile(desiredPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(storeBefore, storeAfter) || !bytes.Equal(desiredBefore, desiredAfter) {
				t.Fatal("unconfirmed floor durability advanced principal store or desired.json")
			}
			floorPath := filepath.Join(r.objDir, objKeyGensName)
			floorBytes, err := os.ReadFile(floorPath)
			if err != nil {
				t.Fatal(err)
			}
			var floor struct {
				Gens map[string]int `json:"gens"`
			}
			if err := json.Unmarshal(floorBytes, &floor); err != nil || floor.Gens[name] != 1 || r.s.objk.gens[name] != 1 {
				t.Fatalf("visible/retained floor lost: file=%s err=%v ram=%d", floorBytes, err, r.s.objk.gens[name])
			}
			if strings.Contains(r.s.objk.gensFileBody, name) {
				t.Fatal("unconfirmed floor cached as durable")
			}

			atomicFloorDirOpenHook = nil
			retried := 0
			atomicFloorDirSyncHook = func(d *os.File) error { retried++; return d.Sync() }
			r.s.mu.Lock()
			err = r.s.objk.writeGensFile() // same floor must sync again
			r.s.mu.Unlock()
			atomicFloorDirSyncHook = nil
			if err != nil || retried != 1 || !strings.Contains(r.s.objk.gensFileBody, name) {
				t.Fatalf("same-floor durability retry skipped: syncs=%d err=%v", retried, err)
			}
			r.open() // restart loads the confirmed visible floor from disk
			if r.s.objk.gens[name] != 1 {
				t.Fatalf("restart lost floor: %d", r.s.objk.gens[name])
			}
		})
	}
}

func TestObjKeysUnconfirmedFloorRestartRevalidatesVisibleFile(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(98)
	injected := 0
	atomicFloorDirSyncHook = func(d *os.File) error { injected++; return errors.New("injected parent sync failure") }
	firstErr := r.s.ensureObjKey(name)
	atomicFloorDirSyncHook = nil
	if injected != 1 || firstErr == nil || !strings.Contains(firstErr.Error(), "injected parent sync failure") {
		t.Fatalf("directory sync failure was not observed once: calls=%d err=%v", injected, firstErr)
	}
	r.open() // file may have been renamed; restart must read and sync it
	if r.s.objk.gens[name] != 1 || !strings.Contains(r.s.objk.gensFileBody, name) {
		t.Fatalf("restart did not confirm visible floor: gen=%d body=%q", r.s.objk.gens[name], r.s.objk.gensFileBody)
	}
	_ = r.s.ensureObjKey(name)
	if _, ok := r.desired().has(name + "-2"); !ok {
		t.Fatal("restart reused generation 1")
	}
}
