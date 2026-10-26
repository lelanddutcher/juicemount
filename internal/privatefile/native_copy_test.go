package privatefile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativeCopyTestID() string { return "copy_" + strings.Repeat("a", 32) }

const nativeCopyTestEndpoint = "https://example.test"
const nativeCopyTestLibrary = "library_native_test"

func nativeCopyTestRoot(t *testing.T) (string, *NativeRoot) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "native")
	root, err := CreateNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("native root close: %v", err)
		}
	})
	return path, root
}
func nativeCopyTestControl(t *testing.T, root *NativeRoot) *NativeControl {
	t.Helper()
	control, err := root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := control.Close(); err != nil {
			t.Errorf("native control close: %v", err)
		}
	})
	return control
}
func nativeCopyTestCreate(t *testing.T, control *NativeControl) *NativeCopy {
	t.Helper()
	copy, err := control.CreateCopy(nativeCopyTestID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := copy.Close(); err != nil {
			t.Errorf("native copy close: %v", err)
		}
	})
	return copy
}

func TestNativeCopyCreateOnceAndIndependentOwnerLock(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	if err := copy.WriteIntent([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := root.LockControl(); !errors.Is(err, ErrNativeCopyBusy) {
		t.Fatalf("second control acquired live lock: %v", err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	second := nativeCopyTestControl(t, root)
	if _, err := second.OpenCopy(nativeCopyTestID()); !errors.Is(err, ErrNativeCopyBusy) {
		t.Fatalf("second copy acquired live owner lock: %v", err)
	}
	if err := root.Close(); !errors.Is(err, ErrNativeCopyBusy) {
		t.Fatalf("root closed live owner: %v", err)
	}
	if err := copy.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := second.OpenCopy(nativeCopyTestID())
	if err != nil {
		t.Fatal(err)
	}
	if line, err := reopened.ReadIntent(); err != nil || string(line) != "{}\n" {
		t.Fatalf("reopened immutable intent: %q %v", line, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCopyZeroPartialAndIdentityReopen(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	id, err := copy.CreateZeroPartial()
	if err != nil || id.Size != 0 {
		t.Fatalf("zero partial: %+v %v", id, err)
	}
	if _, err := copy.CreateZeroPartial(); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second partial create replaced original: %v", err)
	}
	if err := copy.SyncPartial(); err != nil {
		t.Fatal(err)
	}
	if err := copy.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := control.OpenCopy(nativeCopyTestID())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if actual, err := reopened.PartialIdentity(); err != nil || actual != id {
		t.Fatalf("reopened original zero partial: %+v %v", actual, err)
	}
}

func TestNativeCopyRejectsSymlinkHardlinkModeAndInodeSubstitution(t *testing.T) {
	for _, kind := range []string{"symlink_lock", "hardlink_partial", "mode_partial", "inode_lock"} {
		t.Run(kind, func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			control := nativeCopyTestControl(t, root)
			copy := nativeCopyTestCreate(t, control)
			if _, err := copy.CreateZeroPartial(); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(path, "staging", nativeCopyTestID())
			switch kind {
			case "symlink_lock":
				if err := os.Rename(filepath.Join(dir, "owner.lock"), filepath.Join(dir, "old.lock")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("old.lock", filepath.Join(dir, "owner.lock")); err != nil {
					t.Fatal(err)
				}
				if err := copy.Revalidate(); err == nil {
					t.Fatal("symlink lock accepted")
				}
			case "hardlink_partial":
				if err := os.Link(filepath.Join(dir, "payload.partial"), filepath.Join(dir, "duplicate")); err != nil {
					t.Fatal(err)
				}
				if _, err := copy.PartialIdentity(); err == nil {
					t.Fatal("hardlinked partial accepted")
				}
			case "mode_partial":
				if err := os.Chmod(filepath.Join(dir, "payload.partial"), 0644); err != nil {
					t.Fatal(err)
				}
				if _, err := copy.PartialIdentity(); err == nil {
					t.Fatal("nonprivate partial accepted")
				}
			case "inode_lock":
				if err := os.Rename(filepath.Join(dir, "owner.lock"), filepath.Join(dir, "old.lock")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "owner.lock"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				if err := copy.Revalidate(); err == nil {
					t.Fatal("replacement lock inode accepted")
				}
			}
		})
	}
}

func TestNativeCopyWriteSyncAndCloseUncertaintyRetainsLeaf(t *testing.T) {
	for _, kind := range []string{"sync", "close", "directory"} {
		t.Run(kind, func(t *testing.T) {
			_, root := nativeCopyTestRoot(t)
			control := nativeCopyTestControl(t, root)
			copy := nativeCopyTestCreate(t, control)
			fault := errors.New("injected write fault")
			copy.writeOps = &nativeWriteOps{}
			switch kind {
			case "sync":
				copy.writeOps.syncFile = func(*os.File) error { return fault }
			case "close":
				copy.writeOps.closeFile = func(f *os.File) error { return errors.Join(f.Close(), fault) }
			case "directory":
				copy.writeOps.syncDir = func(*os.File) error { return fault }
			}
			if err := copy.WriteIntent([]byte("{}\n")); !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
				t.Fatalf("write uncertainty lost: %v", err)
			}
			copy.writeOps = nil
			if err := copy.WriteIntent([]byte("{}\n")); !errors.Is(err, os.ErrExist) {
				t.Fatalf("uncertain leaf replaced: %v", err)
			}
			if line, err := copy.ReadIntent(); err != nil || string(line) != "{}\n" {
				t.Fatalf("retained uncertain bytes: %q %v", line, err)
			}
			if err := copy.SyncIntent(); err != nil {
				t.Fatalf("exact durable retry: %v", err)
			}
		})
	}
}

func TestNativeCopyControlLockReplacementRevokesExistingOwners(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	other, err := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Errorf("other native root close: %v", err)
		}
	})
	control := nativeCopyTestControl(t, root)
	copy := nativeCopyTestCreate(t, control)
	oldLock := filepath.Join(path, "control.lock")
	if err := os.Rename(oldLock, filepath.Join(path, "old.control.lock")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldLock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(oldLock); err != nil {
			t.Errorf("remove substituted control: %v", err)
			return
		}
		if err := os.Rename(filepath.Join(path, "old.control.lock"), oldLock); err != nil {
			t.Errorf("restore original control: %v", err)
		}
	})
	if _, err := control.CopyIDs(); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("held control accepted replacement path: %v", err)
	}
	if err := copy.WriteIntent([]byte("{}\n")); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("copy record mutation accepted replacement control: %v", err)
	}
	if _, err := copy.CreateZeroPartial(); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("partial mutation accepted replacement control: %v", err)
	}
	if _, err := other.LockControl(); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("separate preexisting opener adopted replacement control: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "staging", nativeCopyTestID(), "intent.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record appeared after refusal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "staging", nativeCopyTestID(), "payload.partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial appeared after refusal: %v", err)
	}
}

func TestNativeRootStrictCreateOpenAndMissingState(t *testing.T) {
	for _, lost := range []string{"initialized.json", "accounting.sqlite", "staging", "copies", "control.lock", "unknown"} {
		t.Run(lost, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "native")
			if _, err := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary); err == nil {
				t.Fatal("Open initialized missing root")
			}
			root, err := CreateNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := CreateNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary); !errors.Is(err, os.ErrExist) {
				t.Fatalf("Create adopted existing root: %v", err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			if lost == "unknown" {
				if err := os.WriteFile(filepath.Join(path, "stray"), []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if lost == "staging" || lost == "copies" {
				if err := os.Remove(filepath.Join(path, lost)); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(filepath.Join(path, lost)); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary); err == nil {
				t.Fatalf("Open accepted lost %s", lost)
			}
			if lost != "unknown" {
				if _, err := os.Lstat(filepath.Join(path, lost)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Open recreated %s: %v", lost, err)
				}
			}
		})
	}
}

func TestNativeRootInterruptedInitializationRemainsUnopenable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(path, "staging"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary); err == nil {
		t.Fatal("partial root opened without final marker")
	}
	if _, err := CreateNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary); !errors.Is(err, os.ErrExist) {
		t.Fatalf("Create adopted incomplete root: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(path, "initialized.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed Open published readiness marker: %v", err)
	}
}

func TestNativeRootPostOpenUnknownRootAndCopiesRefuseAdmission(t *testing.T) {
	for _, scope := range []string{"root", "copies"} {
		t.Run(scope, func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			parent := path
			if scope == "copies" {
				parent = filepath.Join(path, "copies")
			}
			stray := filepath.Join(parent, "unexpected")
			if err := os.WriteFile(stray, []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Remove(stray); err != nil {
					t.Errorf("remove stray: %v", err)
				}
			})
			if _, err := root.LockControl(); !errors.Is(err, ErrNativeCopyChanged) {
				t.Fatalf("post-open unknown %s accepted: %v", scope, err)
			}
		})
	}
}

func TestNativeRootCloseDisposesOriginalHandlesAfterNamespaceLoss(t *testing.T) {
	for _, lost := range []string{"control", "root"} {
		t.Run(lost, func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			if lost == "control" {
				if err := os.Rename(filepath.Join(path, "control.lock"), filepath.Join(path, "old.control.lock")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "control.lock"), []byte("replacement"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "replacement"), []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := root.Close(); err != nil {
				t.Fatalf("original descriptor disposal: %v", err)
			}
			if _, err := root.root.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("original root descriptor remains live: %v", err)
			}
			if lost == "control" {
				if data, err := os.ReadFile(filepath.Join(path, "control.lock")); err != nil || string(data) != "replacement" {
					t.Fatalf("replacement control touched: %q %v", data, err)
				}
			} else if data, err := os.ReadFile(filepath.Join(path, "replacement")); err != nil || string(data) != "untouched" {
				t.Fatalf("replacement root touched: %q %v", data, err)
			}
		})
	}
}

func TestNativeCopyStagingBarrierRetryBeforeIntentReady(t *testing.T) {
	_, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	if err := control.ReserveIntent(nativeCopyTestID(), strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	fault := errors.New("injected staging parent sync failure")
	control.stagingSync = func(*os.File) error { return fault }
	if copy, err := control.ResumePendingCopy(nativeCopyTestID()); copy != nil || !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
		if copy != nil {
			copy.Close()
		}
		t.Fatalf("post-Mkdirat parent barrier uncertainty lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root.path, "staging", nativeCopyTestID())); err != nil {
		t.Fatalf("original uncertain directory missing: %v", err)
	}
	control.stagingSync = nil
	copy, err := control.ResumePendingCopy(nativeCopyTestID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := copy.Close(); err != nil {
			t.Errorf("copy close: %v", err)
		}
	})
	if err := copy.WriteIntent([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	control.stagingSync = func(*os.File) error { return fault }
	if err := copy.SyncNamespace(); !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) {
		t.Fatalf("staging barrier uncertainty lost: %v", err)
	}
	rows, err := control.InventoryEntries()
	if err != nil || len(rows) != 1 || rows[0].Phase != NativeIntentPending {
		t.Fatalf("parent-barrier failure acknowledged pending row: %+v %v", rows, err)
	}
	control.stagingSync = nil
	if err := copy.SyncNamespace(); err != nil {
		t.Fatalf("same original namespace did not re-sync: %v", err)
	}
	if err := control.IntentReady(nativeCopyTestID(), copy.DirectoryIdentity(), copy.LockIdentity()); err != nil {
		t.Fatalf("acknowledge after parent barrier: %v", err)
	}
}

func TestNativeRootEnumerationPreservesOtherObserverCursor(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	all, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 5 {
		t.Fatalf("incomplete root fixture: %d", len(all))
	}
	if _, err := root.root.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	first, err := root.root.Readdirnames(1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first observer read: %v %v", first, err)
	}
	if err := root.rootEntries(); err != nil {
		t.Fatal(err)
	}
	rest, err := root.root.Readdirnames(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest)+len(first) != len(all) {
		t.Fatalf("enumeration consumed another observer cursor: first=%d rest=%d total=%d", len(first), len(rest), len(all))
	}
}
