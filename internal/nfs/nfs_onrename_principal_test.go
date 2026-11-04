package nfs

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type principalRenameFS struct {
	*mockFS
	principals []string
}

func (f *principalRenameFS) SetMutationPrincipal(p string) { f.principals = append(f.principals, p) }
func (f *principalRenameFS) Rename(string, string) error   { return nil }

func authSys(t *testing.T, stamp, uid uint32) rpc.Auth {
	t.Helper()
	var body bytes.Buffer
	if err := xdr.Write(&body, rpc.AuthUnix{Stamp: stamp, Uid: uid, Gid: 20}); err != nil {
		t.Fatal(err)
	}
	return rpc.Auth{Flavor: authSysFlavor, Body: body.Bytes()}
}

// A JUKEBOX resend carries a new XID, so the filesystem matches it to the
// first transmission by its contents and sender. The sender must not change
// with the AUTH_SYS stamp, and must change with the uid.
func TestRenameScopesRetryToCredential(t *testing.T) {
	fs := &principalRenameFS{mockFS: &mockFS{lstatInfo: &mockFileInfo{name: "d", mode: os.ModeDir, dir: true}}}
	h := &mockHandler{fs: fs}
	send := func(cred rpc.Auth) {
		var body bytes.Buffer
		for _, name := range []string{"from", "to"} {
			if err := xdr.Write(&body, DirOpArg{Handle: []byte{1}, Filename: []byte(name)}); err != nil {
				t.Fatal(err)
			}
		}
		w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Header: rpc.Header{Cred: cred}, Body: &body}}
		if err := onRename(context.Background(), w, h); err != nil {
			t.Fatal(err)
		}
	}
	send(authSys(t, 0, 501))
	send(authSys(t, 7, 501))
	send(authSys(t, 0, 502))
	if len(fs.principals) != 3 {
		t.Fatalf("principals = %q", fs.principals)
	}
	if fs.principals[0] != fs.principals[1] {
		t.Errorf("stamp changed the principal: %q vs %q", fs.principals[0], fs.principals[1])
	}
	if fs.principals[0] == fs.principals[2] {
		t.Errorf("different uid shares a principal: %q", fs.principals[0])
	}
}
