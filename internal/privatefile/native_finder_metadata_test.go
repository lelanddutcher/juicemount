package privatefile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestNativeFinderMetadataIsBoundedAndNotCustody(t *testing.T) {
	for _, kind := range []string{"owned", "unknown", "symlink", "hardlink", "directory", "writable", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			copies := filepath.Join(path, "copies")
			leaf := filepath.Join(copies, finderMetadataName)
			switch kind {
			case "unknown":
				leaf = filepath.Join(copies, "desktop.ini")
				fallthrough
			case "owned", "writable", "oversize":
				if err := os.WriteFile(leaf, []byte("Finder metadata"), 0644); err != nil {
					t.Fatal(err)
				}
				if kind != "unknown" {
					if err := os.Chmod(leaf, 0644); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "writable" {
					if err := os.Chmod(leaf, 0666); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "oversize" {
					if err := os.Truncate(leaf, finderCopiesMetadataMaximum+1); err != nil {
						t.Fatal(err)
					}
				}
			case "symlink":
				if err := os.Symlink("outside", leaf); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				outside := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(outside, []byte("linked"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(outside, leaf); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(leaf, 0700); err != nil {
					t.Fatal(err)
				}
			}
			err := root.Revalidate()
			if kind == "owned" {
				if err != nil {
					t.Fatalf("bounded owned Finder metadata refused: %v", err)
				}
				opened, openErr := OpenNativeRoot(path, nativeCopyTestEndpoint, nativeCopyTestLibrary)
				if openErr != nil {
					t.Fatal(openErr)
				}
				if closeErr := opened.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			} else if !errors.Is(err, ErrNativeCopyChanged) {
				t.Fatalf("unsafe or foreign metadata accepted: %s: %v", kind, err)
			}
		})
	}
	foreign := Identity{Mode: unix.S_IFREG | 0644, UID: uint32(os.Geteuid()) + 1, Size: 1}
	if validFinderMetadata(foreign, 1, finderCopiesMetadataMaximum) {
		t.Fatal("foreign owner accepted")
	}
}

func TestNativeFinderMetadataNeverEmptiesUnverifiedTargetOrStaging(t *testing.T) {
	path, root := nativeCopyTestRoot(t)
	id := nativeCopyTestID()
	if err := os.Mkdir(filepath.Join(path, "copies", id), 0700); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(path, "copies", id, finderMetadataName)
	if err := os.WriteFile(leaf, []byte("Finder"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(leaf, 0644); err != nil {
		t.Fatal(err)
	}
	control := nativeCopyTestControl(t, root)
	_, names, err := control.TargetDirectory(id)
	if err != nil || len(names) != 1 || names[0] != finderMetadataName {
		t.Fatalf("target metadata was hidden from pending-state guard: %v %v", names, err)
	}
	if _, err := control.EnsureTargetDirectory(id); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("Finder metadata made a pending target appear empty: %v", err)
	}
	stagingLeaf := filepath.Join(path, "staging", finderMetadataName)
	if err := os.WriteFile(stagingLeaf, []byte("foreign staging metadata"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := control.CopyIDs(); !errors.Is(err, ErrNativeCopyChanged) {
		t.Fatalf("private staging metadata admitted: %v", err)
	}
}

func TestNativeFinderMetadataUnsafeTargetRefused(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "writable", "oversize", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			path, root := nativeCopyTestRoot(t)
			id := nativeCopyTestID()
			target := filepath.Join(path, "copies", id)
			if err := os.Mkdir(target, 0700); err != nil {
				t.Fatal(err)
			}
			leaf := filepath.Join(target, finderMetadataName)
			switch kind {
			case "symlink":
				if err := os.Symlink("foreign", leaf); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				outside := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(outside, []byte("linked"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(outside, leaf); err != nil {
					t.Fatal(err)
				}
			default:
				if kind == "unknown" {
					leaf = filepath.Join(target, "unlisted")
				}
				if err := os.WriteFile(leaf, []byte("metadata"), 0644); err != nil {
					t.Fatal(err)
				}
				if kind == "writable" {
					if err := os.Chmod(leaf, 0666); err != nil {
						t.Fatal(err)
					}
				} else if kind == "oversize" {
					if err := os.Truncate(leaf, finderTargetMetadataMaximum+1); err != nil {
						t.Fatal(err)
					}
				}
			}
			control := nativeCopyTestControl(t, root)
			_, names, err := control.TargetDirectory(id)
			if kind == "unknown" {
				if err != nil || len(names) != 1 || names[0] != "unlisted" {
					t.Fatalf("unexpected name was hidden: %v %v", names, err)
				}
			} else if !errors.Is(err, ErrNativeCopyChanged) {
				t.Fatalf("unsafe Finder target metadata accepted: %v", err)
			}
		})
	}
}
