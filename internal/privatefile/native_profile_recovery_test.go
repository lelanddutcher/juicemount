package privatefile

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNativeProfileRecoveryRepeatsEachBarrier(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run([]string{"marker", "root", "parent"}[failAt], func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			control := nativeCopyTestControl(t, root)
			before, err := os.ReadFile(filepath.Join(path, "initialized.json"))
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("injected profile initialization barrier failure")
			calls := 0
			err = control.syncProfileInitialization(func(f *os.File) error {
				calls++
				if calls-1 == failAt {
					return fault
				}
				return f.Sync()
			})
			if !errors.Is(err, fault) || !errors.Is(err, ErrNativeCopyUncertain) || calls != failAt+1 {
				t.Fatalf("profile barrier failure was not preserved: calls=%d err=%v", calls, err)
			}
			if err := control.Close(); err != nil {
				t.Fatal(err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			fresh, err := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := fresh.Close(); err != nil {
					t.Error(err)
				}
			}()
			owner, err := fresh.LockControl()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			var names []string
			if err := owner.syncProfileInitialization(func(f *os.File) error {
				names = append(names, filepath.Base(f.Name()))
				return f.Sync()
			}); err != nil {
				t.Fatal(err)
			}
			want := []string{filepath.Base(fresh.markerFile.Name()), filepath.Base(fresh.root.Name()), filepath.Base(filepath.Dir(path))}
			if !reflect.DeepEqual(names, want) {
				t.Fatalf("reopened profile skipped exact barriers: got=%v want=%v", names, want)
			}
			after, err := os.ReadFile(filepath.Join(path, "initialized.json"))
			if err != nil || string(after) != string(before) {
				t.Fatal("profile recovery changed initialization identity")
			}
		})
	}
}

func TestNativeProfileRecoveryRefusesChangedNamespace(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	control := nativeCopyTestControl(t, root)
	if err := os.Rename(path, path+"-preserved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	called := false
	err := control.syncProfileInitialization(func(*os.File) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("recovery synced replacement profile namespace")
	}
}

func TestNativeProfileRecoveryRefusesReparentDuringBarrier(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "owner")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "native")
	root, err := CreateNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	}()
	control, err := root.LockControl()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := control.Close(); err != nil {
			t.Error(err)
		}
	}()
	calls := 0
	err = control.syncProfileInitialization(func(f *os.File) error {
		calls++
		if calls == 3 {
			if err := os.Rename(parent, parent+"-old"); err != nil {
				return err
			}
			if err := os.Mkdir(parent, 0700); err != nil {
				return err
			}
			if err := os.Rename(filepath.Join(parent+"-old", "native"), path); err != nil {
				return err
			}
		}
		return f.Sync()
	})
	if calls != 3 || !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("recovery acknowledged detached parent barrier: calls=%d err=%v", calls, err)
	}
	if err := root.Revalidate(); err != nil {
		t.Fatalf("fixture did not preserve exact root identity: %v", err)
	}
}
