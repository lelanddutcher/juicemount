package nfs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestRenameKnownConflictPreservesQueuedUploads(t *testing.T) {
	for _, scenario := range []string{"nonempty-directory", "directory-over-file", "file-over-directory"} {
		for _, localOnly := range []bool{false, true} {
			lane := "versioned"
			if localOnly {
				lane = "local-only"
			}
			t.Run(scenario+"/"+lane, func(t *testing.T) {
				jfs, spool, _, root := newSpoolWiredHandlerNoDrain(t)
				store := jfs.handler.store
				put := func(name string, directory bool, body string) {
					t.Helper()
					full := filepath.Join(root, name)
					var err error
					if directory {
						err = os.MkdirAll(full, 0700)
					} else {
						err = os.WriteFile(full, []byte(body), 0600)
					}
					if err != nil {
						t.Fatal(err)
					}
					info, err := os.Lstat(full)
					if err != nil {
						t.Fatal(err)
					}
					e := metadata.MakeEntry(name, directory, info.Size(), info.ModTime(), info.Sys().(*syscall.Stat_t).Ino)
					e.LocalOnly = localOnly
					if err := store.Insert(e); err != nil {
						t.Fatal(err)
					}
				}
				sourceDir := scenario != "file-over-directory"
				put("source", sourceDir, "source backing")
				put("destination", scenario != "directory-over-file", "destination backing")
				sourcePath, destinationPath := "source", "destination"
				if sourceDir {
					sourcePath = "source/clip.mov"
					put(sourcePath, false, "source backing")
				}
				if scenario != "directory-over-file" {
					destinationPath = "destination/winner.mov"
					put(destinationPath, false, "destination backing")
				}
				paths := []string{sourcePath, destinationPath}
				before := make(map[string]*metadata.SpoolRow)
				mirror := make(map[string]metadata.Entry)
				private := make(map[string][]byte)
				backing := make(map[string][]byte)
				for _, name := range paths {
					w, err := spool.OpenWrite(name)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := w.WriteAt([]byte("queued "+name), 0); err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					row, err := spool.Meta().LookupByPath(name)
					if err != nil {
						t.Fatal(err)
					}
					before[name] = row
					mirror[name] = *store.LookupByPath(name)
					private[name], err = os.ReadFile(row.SpoolFile)
					if err != nil {
						t.Fatal(err)
					}
					backing[name], err = os.ReadFile(filepath.Join(root, name))
					if err != nil {
						t.Fatal(err)
					}
				}
				want := error(syscall.ENOTEMPTY)
				if scenario == "directory-over-file" {
					want = syscall.ENOTDIR
				} else if scenario == "file-over-directory" {
					want = syscall.EISDIR
				}
				if err := jfs.Rename("source", "destination"); !errors.Is(err, want) {
					t.Fatalf("Rename = %v, want %v", err, want)
				}
				for _, name := range paths {
					row, err := spool.Meta().Get(before[name].ID)
					if err != nil || !reflect.DeepEqual(row, before[name]) {
						t.Errorf("refusal changed upload row for %s: before=%+v after=%+v err=%v", name, before[name], row, err)
					}
					active, ok := spool.LookupActive(name)
					if !ok || active.ID() != before[name].ID || active.NFSPath() != name {
						t.Errorf("refusal changed upload index for %s", name)
					}
					if e := store.LookupByPath(name); e == nil || !reflect.DeepEqual(*e, mirror[name]) {
						t.Errorf("refusal changed mirrored identity for %s", name)
					}
					got, err := os.ReadFile(before[name].SpoolFile)
					if err != nil || !bytes.Equal(got, private[name]) {
						t.Errorf("refusal changed private bytes for %s: %v", name, err)
					}
					got, err = os.ReadFile(filepath.Join(root, name))
					if err != nil || !bytes.Equal(got, backing[name]) {
						t.Errorf("refusal changed backing bytes for %s: %v", name, err)
					}
				}
				if sourceDir {
					if _, ok := spool.LookupActive("destination/clip.mov"); ok {
						t.Error("refusal created a migrated upload index")
					}
				}
				awaitNamespaceIdle(t, &jfs.handler.namespace)
			})
		}
	}
}
