package nfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// This private acceptance overlay manually supplies the writer activation
// which is deliberately not wired in production yet. It exercises the real
// OpenWrite READY continuation, not a replacement implementation of it.
func TestBoundReadyContinuationKeepsPrivateIdentity(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "same_private_inode"
		if replace {
			name = "replaced_private_inode"
		}
		t.Run(name, func(t *testing.T) {
			store, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := metadata.InitSpoolSchema(store.DB()); err != nil {
				t.Fatal(err)
			}
			meta := metadata.NewSpoolStore(store.DB())
			store.SetSpoolStore(meta)
			spool, err := NewSpoolStore(t.TempDir(), 0, meta)
			if err != nil {
				t.Fatal(err)
			}
			defer spool.Stop()
			const path = "._take.mov"
			file := filepath.Join(spool.Root(), SpoolFilesSubdir, "bound-private")
			fd, err := os.OpenFile(file, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer fd.Close()
			originalInfo, err := fd.Stat()
			if err != nil {
				t.Fatal(err)
			}
			expected, err := store.CaptureMirror(path)
			if err != nil {
				t.Fatal(err)
			}
			nonce, err := metadata.NewSpoolCustodyID()
			if err != nil {
				t.Fatal(err)
			}
			receipt, born, err := store.InsertBoundSpoolBirth(expected, metadata.MakeEntry(path, false, 0, time.Now(), 551), file, nonce)
			if err != nil {
				t.Fatal(err)
			}
			entry := &SpoolEntry{id: receipt.SpoolID, nfsPath: path, spoolFile: file, file: fd, store: spool, refcount: 1}
			entry.SetInode(born.Inode)
			entry.lastWrite.Store(time.Now().UnixNano())
			spool.index.Insert(path, entry)
			const body = "original-complete-body"
			if n, err := entry.WriteAt([]byte(body), 0); err != nil || n != len(body) {
				t.Fatalf("seed=(%d,%v)", n, err)
			}
			if err := entry.Close(); err != nil {
				t.Fatal(err)
			}
			row, err := meta.Get(receipt.SpoolID)
			if err != nil || row.DrainState != metadata.DrainReady || row.PunchedEnd != 0 || row.Size != int64(len(body)) {
				t.Fatalf("not exact ready row: %+v %v", row, err)
			}
			var savedIdentity, savedNonce string
			if err := store.DB().QueryRow(`SELECT birth_identity,birth_id FROM spool_bindings WHERE spool_id=?`, receipt.SpoolID).Scan(&savedIdentity, &savedNonce); err != nil {
				t.Fatal(err)
			}
			if savedIdentity != receipt.FileIdentity || savedNonce != nonce {
				t.Fatal("birth identity precondition failed")
			}
			retained := file + ".retained"
			if replace {
				if err := os.Rename(file, retained); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(file, []byte("unrelated-private-body"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := spool.OpenWrite(path)
			if !replace {
				if err != nil || reopened != entry || reopened.ID() != receipt.SpoolID {
					t.Fatalf("same-source continuation=(%p,%v)", reopened, err)
				}
				got, statErr := reopened.file.Stat()
				if statErr != nil || !os.SameFile(originalInfo, got) {
					t.Fatalf("same-source identity changed: %v", statErr)
				}
				if b, err := os.ReadFile(file); err != nil || string(b) != body {
					t.Fatalf("same-source body=(%q,%v)", b, err)
				}
				reopened.ReleaseHandle()
				return
			}
			if reopened != nil {
				defer reopened.ReleaseHandle()
				got, statErr := reopened.file.Stat()
				if statErr != nil {
					t.Fatal(statErr)
				}
				t.Errorf("bound READY continuation exposed file: old_identity=%s same_original_inode=%v error=%v", savedIdentity, os.SameFile(originalInfo, got), err)
				if n, writeErr := reopened.WriteAt([]byte("NEW"), 0); writeErr != nil || n != 3 {
					t.Fatalf("real accepted write=(%d,%v)", n, writeErr)
				}
				b, readErr := os.ReadFile(file)
				t.Logf("substituted source after accepted write=%q err=%v", b, readErr)
			} else if err == nil {
				t.Error("refusal must return a non-nil error")
			}
			row, rowErr := meta.Get(receipt.SpoolID)
			if rowErr != nil || row.DrainState != metadata.DrainReady {
				t.Errorf("substitution changed READY row: %+v %v", row, rowErr)
			}
			if b, readErr := os.ReadFile(retained); readErr != nil || string(b) != body {
				t.Fatalf("retained predecessor body=(%q,%v)", b, readErr)
			}
			if got := store.LookupByPath(path); got == nil || got.Version != born.Version {
				t.Fatalf("mirror changed unexpectedly: %+v", got)
			}
		})
	}
}
