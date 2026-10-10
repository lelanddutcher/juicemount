package nfs

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type legacyRenameFS struct {
	*mockFS
	renames int
}

func (f *legacyRenameFS) Rename(oldpath, newpath string) error {
	f.renames++
	if oldpath != "f.bin/from" || newpath != "f.bin/to" {
		return os.ErrInvalid
	}
	return nil
}

func TestRenameGenericFilesystemFallback(t *testing.T) {
	fs := &legacyRenameFS{mockFS: &mockFS{lstatInfo: &mockFileInfo{name: "f.bin", mode: os.ModeDir, dir: true}}}
	h := &mockHandler{fs: fs}
	var body bytes.Buffer
	if err := xdr.Write(&body, DirOpArg{Handle: []byte{1}, Filename: []byte("from")}); err != nil {
		t.Fatal(err)
	}
	if err := xdr.Write(&body, DirOpArg{Handle: []byte{1}, Filename: []byte("to")}); err != nil {
		t.Fatal(err)
	}
	w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Body: &body}}
	if err := onRename(context.Background(), w, h); err != nil {
		t.Fatalf("generic rename failed: %v", err)
	}
	if fs.renames != 1 {
		t.Fatalf("generic Rename calls=%d, want 1", fs.renames)
	}
}
