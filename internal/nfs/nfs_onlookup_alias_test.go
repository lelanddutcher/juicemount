package nfs

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"os"
	"syscall"
	"testing"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type aliasLookupFS struct {
	*mockFS
	exact bool
	calls int
	alias *LookupAlias
	err   error
	stats []string
}

func (f *aliasLookupFS) Lstat(name string) (os.FileInfo, error) {
	f.stats = append(f.stats, name)
	if name == "f.bin" {
		return &mockFileInfo{name: name, dir: true, mode: os.ModeDir | 0755}, nil
	}
	if f.exact {
		return &mockFileInfo{name: name, size: 17, mode: 0644}, nil
	}
	return nil, os.ErrNotExist
}
func (f *aliasLookupFS) StatCacheOnly(string) (os.FileInfo, bool) { return nil, f.exact }
func (f *aliasLookupFS) ResolveLookupAlias(parent, name string) (*LookupAlias, error) {
	f.calls++
	if parent != "f.bin" {
		panic("wrong parent")
	}
	return f.alias, f.err
}

type aliasLookupHandler struct {
	mockHandler
	paths [][]string
}

func (h *aliasLookupHandler) ToHandle(_ billy.Filesystem, p []string) []byte {
	h.paths = append(h.paths, append([]string(nil), p...))
	return []byte{7}
}

func TestLookupCanonicalAliasWireAndExactFastPath(t *testing.T) {
	for _, names := range [][2]string{{"cafe\u0301.mov", "café.mov"}, {"K", "K"}} {
		for _, exact := range []bool{false, true} {
			t.Run(names[0]+"/"+map[bool]string{false: "alias", true: "exact"}[exact], func(t *testing.T) {
				alias := &LookupAlias{Name: names[0], Handle: []byte{9, 8, 7}, Info: &mockFileInfo{name: names[0], size: 81, mode: 0644}}
				fs := &aliasLookupFS{mockFS: &mockFS{}, exact: exact, alias: alias}
				h := &aliasLookupHandler{mockHandler: mockHandler{fs: fs}}
				var wire bytes.Buffer
				w := &response{responded: true, writer: &wire, req: &request{Body: xdrBody(t, DirOpArg{Handle: []byte{1}, Filename: []byte(names[1])})}}
				if err := onLookup(context.Background(), w, h); err != nil {
					t.Fatal(err)
				}
				if exact {
					if fs.calls != 0 || len(h.paths) != 1 || h.paths[0][1] != names[1] {
						t.Fatal("exact identity did not win")
					}
				} else {
					if fs.calls != 1 || len(h.paths) != 0 {
						t.Fatal("alias used ordinary handle fallback")
					}
					// Decode the actual NFS reply independently; do not produce the
					// expected bytes with the response helper being tested.
					r := bytes.NewReader(wire.Bytes())
					var status, hasChild, hasParent uint32
					if err := xdr.Read(r, &status); err != nil || status != 0 {
						t.Fatalf("status=%d err=%v", status, err)
					}
					var handle []byte
					err := xdr.Read(r, &handle)
					if err != nil || !bytes.Equal(handle, []byte{9, 8, 7}) {
						t.Fatalf("stored handle=%x err=%v", handle, err)
					}
					if err := xdr.Read(r, &hasChild); err != nil || hasChild != 1 {
						t.Fatalf("child attrs=%d err=%v", hasChild, err)
					}
					var attrs FileAttribute
					if err := xdr.Read(r, &attrs); err != nil {
						t.Fatal(err)
					}
					storedID := fnv.New64()
					_, _ = storedID.Write([]byte("f.bin/" + names[0]))
					if attrs.Fileid != storedID.Sum64() || attrs.Filesize != 81 || attrs.FileMode != 0644 || attrs.Type != FileTypeRegular {
						t.Fatalf("attrs not bound to stored name/body: %+v", attrs)
					}
					if err := xdr.Read(r, &hasParent); err != nil || hasParent != 0 || r.Len() != 0 {
						t.Fatalf("optional parent attrs=%d remaining=%d err=%v", hasParent, r.Len(), err)
					}
					if len(fs.stats) != 1 || fs.stats[0] != "f.bin" {
						t.Fatalf("alias response performed Lstat after original parent check: %q", fs.stats)
					}
				}
			})
		}
	}
}

func TestLookupCanonicalAliasRefusals(t *testing.T) {
	for _, name := range []string{"ascii", string([]byte{0xff}), "é/child", "é\x00"} {
		fs := &aliasLookupFS{mockFS: &mockFS{}}
		h := &mockHandler{fs: fs}
		w := &response{responded: true, writer: new(bytes.Buffer), req: &request{Body: xdrBody(t, DirOpArg{Handle: []byte{1}, Filename: []byte(name)})}}
		if err := onLookup(context.Background(), w, h); !errors.Is(err, os.ErrNotExist) || fs.calls != 0 {
			t.Fatalf("%q: err=%v calls=%d", name, err, fs.calls)
		}
	}
	for _, refusal := range []error{os.ErrNotExist, syscall.EIO} {
		fs := &aliasLookupFS{mockFS: &mockFS{}, err: refusal}
		w := &response{responded: true, writer: new(bytes.Buffer), req: &request{Body: xdrBody(t, DirOpArg{Handle: []byte{1}, Filename: []byte("café.mov")})}}
		if err := onLookup(context.Background(), w, &mockHandler{fs: fs}); !errors.Is(err, refusal) {
			t.Fatalf("lost refusal: %v", err)
		}
	}
}
