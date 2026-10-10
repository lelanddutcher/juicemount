package nfs

// H-8 option B: a write refused by a read-only JuiceFS backend (EROFS) must
// reach the macOS client as NFS3ERR_ROFS (30), never as the generic
// NFS3ERR_IO (Finder -36 "I/O error") or NFS3ERR_ACCES (Finder -5000).

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// leaseRefusal has the shape a lease-refusal sentinel would have: an
// *os.PathError around EROFS (what os.* returns from a FUSE call), wrapped
// once more with %w.
func leaseRefusal(op string) error {
	return fmt.Errorf("project lease not held: %w", &os.PathError{Op: op, Path: "proj/a.mov", Err: syscall.EROFS})
}

func TestNFSStatusErrorFromEROFS(t *testing.T) {
	cases := map[string]struct {
		in   error
		want NFSStatus
	}{
		"bare errno":        {syscall.EROFS, NFSStatusROFS},
		"path error":        {&os.PathError{Op: "mkdir", Path: "/p", Err: syscall.EROFS}, NFSStatusROFS},
		"sentinel wrapped":  {leaseRefusal("create"), NFSStatusROFS},
		"link error":        {&os.LinkError{Op: "rename", Old: "a", New: "b", Err: syscall.EROFS}, NFSStatusROFS},
		"enospc unchanged":  {syscall.ENOSPC, NFSStatusNoSPC},
		"generic unchanged": {errors.New("boom"), NFSStatusIO},
		"eacces unchanged":  {syscall.EACCES, NFSStatusAccess},
	}
	for name, tc := range cases {
		var statusErr *NFSStatusError
		if !errors.As(nfsStatusErrorFrom(tc.in), &statusErr) {
			t.Fatalf("%s: not an *NFSStatusError", name)
		}
		if statusErr.NFSStatus != tc.want {
			t.Errorf("%s: status=%v, want %v", name, statusErr.NFSStatus, tc.want)
		}
	}
}

func TestWriteRefusalStatus(t *testing.T) {
	cases := map[string]struct {
		in       error
		fallback NFSStatus
		want     NFSStatus
	}{
		"bare errno":       {syscall.EROFS, NFSStatusAccess, NFSStatusROFS},
		"sentinel wrapped": {leaseRefusal("symlink"), NFSStatusAccess, NFSStatusROFS},
		// attrs.Apply already wraps its failure in an *NFSStatusError; the
		// read-only cause underneath must still win over the outer status.
		"inside status error": {&NFSStatusError{NFSStatusIO, leaseRefusal("chmod")}, NFSStatusIO, NFSStatusROFS},
		"fallback access":     {errors.New("boom"), NFSStatusAccess, NFSStatusAccess},
		"fallback io":         {syscall.EIO, NFSStatusIO, NFSStatusIO},
		"fallback serverfail": {errors.New("x"), NFSStatusServerFault, NFSStatusServerFault},
	}
	for name, tc := range cases {
		got := writeRefusalStatus(tc.in, tc.fallback)
		if got.NFSStatus != tc.want {
			t.Errorf("%s: status=%v, want %v", name, got.NFSStatus, tc.want)
		}
		if !errors.Is(got, tc.in) {
			t.Errorf("%s: lost the original error", name)
		}
	}
	if got, _ := removeErrStatus(leaseRefusal("remove")); got == nil || got.NFSStatus != NFSStatusROFS {
		t.Errorf("removeErrStatus(EROFS) = %v, want NFSStatusROFS", got)
	}
	if got := renameErrStatus(leaseRefusal("rename")); got != NFSStatusROFS {
		t.Errorf("renameErrStatus(EROFS) = %v, want NFSStatusROFS", got)
	}
}

// --- per-op handler test -------------------------------------------------

// erofsFS is a writable billy filesystem whose every mutation is refused with
// EROFS. "f.bin" (the path mockHandler.FromHandle returns) is a directory;
// every other path is absent, so each handler's pre-checks pass and the
// mutation itself is reached.
type erofsFS struct {
	*mockFS
	file      *trackFile
	commitErr error
}

func (f *erofsFS) stat(name string) (os.FileInfo, error) {
	if name == "f.bin" {
		return &mockFileInfo{name: "f.bin", mode: os.ModeDir | 0o755, dir: true}, nil
	}
	return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
}
func (f *erofsFS) Stat(name string) (os.FileInfo, error)    { return f.stat(name) }
func (f *erofsFS) Lstat(name string) (os.FileInfo, error)   { return f.stat(name) }
func (f *erofsFS) StatCacheOnly(string) (os.FileInfo, bool) { return nil, false }
func (f *erofsFS) Create(string) (billy.File, error)        { return nil, leaseRefusal("create") }
func (f *erofsFS) MkdirAll(string, os.FileMode) error       { return leaseRefusal("mkdir") }
func (f *erofsFS) Symlink(string, string) error             { return leaseRefusal("symlink") }
func (f *erofsFS) Remove(string) error                      { return leaseRefusal("remove") }
func (f *erofsFS) Rename(string, string) error              { return leaseRefusal("rename") }
func (f *erofsFS) CommitFile(string) error                  { return f.commitErr }
func (f *erofsFS) OpenFile(string, int, os.FileMode) (billy.File, error) {
	if f.file != nil {
		return f.file, nil
	}
	return nil, leaseRefusal("open")
}

// erofsChanger refuses every attribute and special-file change with EROFS.
type erofsChanger struct{}

func (erofsChanger) Chmod(string, os.FileMode) error            { return leaseRefusal("chmod") }
func (erofsChanger) Lchown(string, int, int) error              { return leaseRefusal("lchown") }
func (erofsChanger) Chown(string, int, int) error               { return leaseRefusal("chown") }
func (erofsChanger) Chtimes(string, time.Time, time.Time) error { return leaseRefusal("chtimes") }
func (erofsChanger) Mknod(string, uint32, uint32, uint32) error { return leaseRefusal("mknod") }
func (erofsChanger) Mkfifo(string, uint32) error                { return leaseRefusal("mkfifo") }
func (erofsChanger) Socket(string) error                        { return leaseRefusal("socket") }
func (erofsChanger) Link(string, string) error                  { return leaseRefusal("link") }

type erofsHandler struct{ mockHandler }

func (erofsHandler) Change(billy.Filesystem) billy.Change { return erofsChanger{} }

var _ UnixChange = erofsChanger{}

func xdrBody(t *testing.T, parts ...any) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	for _, p := range parts {
		if err := xdr.Write(&b, p); err != nil {
			t.Fatalf("encode %T: %v", p, err)
		}
	}
	return &b
}

// emptySattr is a sattr3 that sets nothing (six FALSE / DONT_CHANGE arms).
var emptySattr = [6]uint32{}

func TestWriteHandlersReturnROFSOnEROFS(t *testing.T) {
	dirop := func(name string) DirOpArg { return DirOpArg{Handle: []byte{1}, Filename: []byte(name)} }
	payload := []byte("data")
	mode := uint32(0o644)

	cases := []struct {
		name string
		op   func(context.Context, *response, Handler) error
		body func(*testing.T) *bytes.Buffer
		file *trackFile
		// commitErr is what the durability barrier (Committer) returns.
		commitErr error
	}{
		{"CREATE", onCreate, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("new.mov"), uint32(createModeUnchecked), emptySattr)
		}, nil, nil},
		{"CREATE exclusive", onCreate, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("new.mov"), uint32(createModeExclusive), [8]byte{})
		}, nil, nil},
		{"MKDIR", onMkdir, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("newdir"), emptySattr)
		}, nil, nil},
		{"SYMLINK", onSymlink, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("ln"), emptySattr, []byte("target"))
		}, nil, nil},
		{"MKNOD fifo", onMknod, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("fifo"), uint32(FTYPE_NF3FIFO), emptySattr)
		}, nil, nil},
		{"MKNOD sock", onMknod, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("sock"), uint32(FTYPE_NF3SOCK), emptySattr)
		}, nil, nil},
		{"LINK", onLink, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("hard"), emptySattr, []byte("f.bin/src"))
		}, nil, nil},
		{"REMOVE", onRemove, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("old.mov"))
		}, nil, nil},
		{"RMDIR", onRmDir, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("olddir"))
		}, nil, nil},
		{"RENAME", onRename, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("from"), dirop("to"))
		}, nil, nil},
		{"WRITE open", onWrite, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, writeArgs{Handle: []byte{1}, Count: 4, How: uint32(unstable), Data: payload})
		}, nil, nil},
		{"WRITE writeat", onWrite, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, writeArgs{Handle: []byte{1}, Count: 4, How: uint32(unstable), Data: payload})
		}, &trackFile{name: "f.bin", writeAtErr: leaseRefusal("write")}, nil},
		{"WRITE close", onWrite, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, writeArgs{Handle: []byte{1}, Count: 4, How: uint32(unstable), Data: payload})
		}, &trackFile{name: "f.bin", closeErr: leaseRefusal("close")}, nil},
		{"WRITE filesync barrier", onWrite, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, writeArgs{Handle: []byte{1}, Count: 4, How: uint32(fileSync), Data: payload})
		}, &trackFile{name: "f.bin"}, leaseRefusal("fsync")},
		{"SETATTR", onSetAttr, func(t *testing.T) *bytes.Buffer {
			// handle, sattr3{mode=0644}, guard=FALSE. The handle is 4 bytes
			// because xdr.ReadOpaque does not consume opaque padding.
			return xdrBody(t, []byte{1, 2, 3, 4}, uint32(1), mode, [5]uint32{}, uint32(0))
		}, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsys := &erofsFS{mockFS: &mockFS{}, file: tc.file, commitErr: tc.commitErr}
			h := &erofsHandler{mockHandler{fs: fsys}}
			w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Body: tc.body(t)}}
			err := tc.op(context.Background(), w, h)
			if err == nil {
				t.Fatal("handler succeeded; want an EROFS refusal")
			}
			// Assert on the wire: the status word the client decodes.
			formatted := w.errorFmt(err)
			if formatted.Code() != ResponseCodeSuccess {
				t.Fatalf("reply is RPC-level code %d, not an NFS status (err %v)", formatted.Code(), err)
			}
			wire, mErr := formatted.MarshalBinary()
			if mErr != nil || len(wire) < 4 {
				t.Fatalf("marshal: %v (%d bytes)", mErr, len(wire))
			}
			got := NFSStatus(binary.BigEndian.Uint32(wire[:4]))
			if got != NFSStatusROFS {
				t.Fatalf("%s reply status = %v (%d), want NFS3ERR_ROFS (30); err: %v", tc.name, got, uint32(got), err)
			}
			if !errors.Is(err, syscall.EROFS) {
				t.Fatalf("%s lost the EROFS cause: %v", tc.name, err)
			}
		})
	}
}

// --- follow-up refusals: the mutation succeeds, a later step is refused ---

// followUpFS lets every namespace mutation succeed and records what it
// created, so the handler moves on to the step after it: CREATE's Close, or
// attrs.Apply for LINK, SYMLINK and MKNOD. Created paths then Lstat as an
// 0600 regular file, so a sattr3 asking for 0644 forces a Chmod.
type followUpFS struct {
	*mockFS
	created map[string]bool
}

func (f *followUpFS) stat(name string) (os.FileInfo, error) {
	if name == "f.bin" {
		return &mockFileInfo{name: "f.bin", mode: os.ModeDir | 0o755, dir: true}, nil
	}
	if f.created[name] {
		return &mockFileInfo{name: name, mode: 0o600}, nil
	}
	return nil, &os.PathError{Op: "stat", Path: name, Err: syscall.ENOENT}
}
func (f *followUpFS) Stat(name string) (os.FileInfo, error)    { return f.stat(name) }
func (f *followUpFS) Lstat(name string) (os.FileInfo, error)   { return f.stat(name) }
func (f *followUpFS) StatCacheOnly(string) (os.FileInfo, bool) { return nil, false }
func (f *followUpFS) Create(name string) (billy.File, error) {
	f.created[name] = true
	return &trackFile{name: name, closeErr: leaseRefusal("close")}, nil
}
func (f *followUpFS) Symlink(_, link string) error { f.created[link] = true; return nil }

// followUpChanger creates special files and links successfully but refuses
// every attribute change with EROFS.
type followUpChanger struct{ fs *followUpFS }

func (c followUpChanger) Chmod(string, os.FileMode) error            { return leaseRefusal("chmod") }
func (c followUpChanger) Lchown(string, int, int) error              { return leaseRefusal("lchown") }
func (c followUpChanger) Chown(string, int, int) error               { return leaseRefusal("chown") }
func (c followUpChanger) Chtimes(string, time.Time, time.Time) error { return leaseRefusal("chtimes") }
func (c followUpChanger) Mknod(p string, _, _, _ uint32) error       { c.fs.created[p] = true; return nil }
func (c followUpChanger) Mkfifo(p string, _ uint32) error            { c.fs.created[p] = true; return nil }
func (c followUpChanger) Socket(p string) error                      { c.fs.created[p] = true; return nil }
func (c followUpChanger) Link(_, link string) error                  { c.fs.created[link] = true; return nil }

type followUpHandler struct {
	mockHandler
	fs *followUpFS
}

func (h *followUpHandler) Change(billy.Filesystem) billy.Change { return followUpChanger{fs: h.fs} }

var _ UnixChange = followUpChanger{}

func TestWriteHandlersReturnROFSOnFollowUpEROFS(t *testing.T) {
	dirop := func(name string) DirOpArg { return DirOpArg{Handle: []byte{1}, Filename: []byte(name)} }
	// sattr3 with only mode=0644 set.
	chmodSattr := []uint32{1, 0o644, 0, 0, 0, 0, 0}

	cases := []struct {
		name    string
		op      func(context.Context, *response, Handler) error
		body    func(*testing.T) *bytes.Buffer
		created string
	}{
		{"CREATE close", onCreate, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("new.mov"), uint32(createModeUnchecked), emptySattr)
		}, "f.bin/new.mov"},
		{"LINK attrs", onLink, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("hard"), [7]uint32(chmodSattr), []byte("f.bin/src"))
		}, "f.bin/hard"},
		{"SYMLINK attrs", onSymlink, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("ln"), [7]uint32(chmodSattr), []byte("target"))
		}, "f.bin/ln"},
		{"MKNOD blk attrs", onMknod, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("blk"), uint32(FTYPE_NF3BLK), [7]uint32(chmodSattr), uint32(1), uint32(2))
		}, "f.bin/blk"},
		{"MKNOD sock attrs", onMknod, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("sock"), uint32(FTYPE_NF3SOCK), [7]uint32(chmodSattr))
		}, "f.bin/sock"},
		{"MKNOD fifo attrs", onMknod, func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop("fifo"), uint32(FTYPE_NF3FIFO), [7]uint32(chmodSattr))
		}, "f.bin/fifo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsys := &followUpFS{mockFS: &mockFS{}, created: map[string]bool{}}
			h := &followUpHandler{mockHandler: mockHandler{fs: fsys}, fs: fsys}
			w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Body: tc.body(t)}}
			err := tc.op(context.Background(), w, h)
			if !fsys.created[tc.created] {
				t.Fatalf("mutation never reached %q (created %v); the test does not exercise the follow-up step", tc.created, fsys.created)
			}
			if err == nil {
				t.Fatal("handler succeeded; want the follow-up EROFS refusal")
			}
			wire, mErr := w.errorFmt(err).MarshalBinary()
			if mErr != nil || len(wire) < 4 {
				t.Fatalf("marshal: %v (%d bytes)", mErr, len(wire))
			}
			if got := NFSStatus(binary.BigEndian.Uint32(wire[:4])); got != NFSStatusROFS {
				t.Fatalf("%s reply status = %v (%d), want NFS3ERR_ROFS (30); err: %v", tc.name, got, uint32(got), err)
			}
		})
	}
}
