package nfs

// Phase-1 BUG 2 + BUG 4 (docs/LAUNCH_PLAN.md): error paths in the NFS
// library dropped successfully-opened billy.Files without Close, and raw
// (non-NFSStatusError) errors escaped to the RPC error formatter, producing
// an accept_stat=SYSTEM_ERR reply with no NFS body — which macOS surfaces as
// EBADRPC "RPC struct is bad" (errno 72).
//
// In JuiceMount, a dropped write handle leaks a spool entry refcount (the
// idle sweeper then never finalizes the entry — the 43-stuck-entries
// incident), so every OpenFile-for-write in this package MUST be Closed on
// every path, success or failure.
//
// The two leak sites:
//   - SetFileAttributes.Apply (file.go): SetSize arm leaked fp when
//     Truncate failed or the size was out of range, and returned the raw
//     error.
//   - onWrite (nfs_onwrite.go): leaked the file when WriteAt (or the
//     legacy Seek fallback) failed.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// --- mock billy plumbing -------------------------------------------------

type mockFileInfo struct {
	name string
	size int64
	mode os.FileMode
	dir  bool
}

func (m *mockFileInfo) Name() string       { return m.name }
func (m *mockFileInfo) Size() int64        { return m.size }
func (m *mockFileInfo) Mode() os.FileMode  { return m.mode }
func (m *mockFileInfo) ModTime() time.Time { return time.Unix(1700000000, 0) }
func (m *mockFileInfo) IsDir() bool        { return m.dir }
func (m *mockFileInfo) Sys() any           { return nil }

// trackFile records Close calls and lets tests inject Truncate/WriteAt
// failures — the same failure shapes a spool-backed billy.File produces.
type trackFile struct {
	name       string
	closeCalls int
	truncErr   error
	writeAtErr error
}

func (f *trackFile) Name() string { return f.name }
func (f *trackFile) Write(p []byte) (int, error) {
	if f.writeAtErr != nil {
		return 0, f.writeAtErr
	}
	return len(p), nil
}
func (f *trackFile) Read(p []byte) (int, error)                { return 0, nil }
func (f *trackFile) ReadAt(p []byte, off int64) (int, error)   { return 0, nil }
func (f *trackFile) Seek(off int64, whence int) (int64, error) { return 0, nil }
func (f *trackFile) Close() error                              { f.closeCalls++; return nil }
func (f *trackFile) Lock() error                               { return nil }
func (f *trackFile) Unlock() error                             { return nil }
func (f *trackFile) Truncate(size int64) error                 { return f.truncErr }
func (f *trackFile) WriteAt(p []byte, off int64) (n int, e error) {
	if f.writeAtErr != nil {
		return 0, f.writeAtErr
	}
	return len(p), nil
}

// mockFS hands out a fixed file from OpenFile and a fixed Lstat result.
type mockFS struct {
	file      *trackFile
	lstatInfo os.FileInfo
	openErr   error
}

func (m *mockFS) Create(filename string) (billy.File, error) { return m.OpenFile(filename, 0, 0) }
func (m *mockFS) Open(filename string) (billy.File, error)   { return m.OpenFile(filename, 0, 0) }
func (m *mockFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	return m.file, nil
}
func (m *mockFS) Stat(filename string) (os.FileInfo, error)  { return m.lstatInfo, nil }
func (m *mockFS) Lstat(filename string) (os.FileInfo, error) { return m.lstatInfo, nil }
func (m *mockFS) Rename(oldpath, newpath string) error       { return nil }
func (m *mockFS) Remove(filename string) error               { return nil }
func (m *mockFS) Join(elem ...string) string {
	out := ""
	for i, e := range elem {
		if i > 0 {
			out += "/"
		}
		out += e
	}
	return out
}
func (m *mockFS) TempFile(dir, prefix string) (billy.File, error)  { return nil, os.ErrInvalid }
func (m *mockFS) ReadDir(path string) ([]os.FileInfo, error)       { return nil, nil }
func (m *mockFS) MkdirAll(filename string, perm os.FileMode) error { return nil }
func (m *mockFS) Symlink(target, link string) error                { return os.ErrInvalid }
func (m *mockFS) Readlink(link string) (string, error)             { return "", os.ErrInvalid }
func (m *mockFS) Chroot(path string) (billy.Filesystem, error)     { return nil, os.ErrInvalid }
func (m *mockFS) Root() string                                     { return "/" }

// erroringChanger fails Chmod/Chtimes with a RAW error (the shape an
// unexpected backend failure produces).
type erroringChanger struct{ err error }

func (c *erroringChanger) Chmod(name string, mode os.FileMode) error         { return c.err }
func (c *erroringChanger) Lchown(name string, uid, gid int) error            { return c.err }
func (c *erroringChanger) Chown(name string, uid, gid int) error             { return c.err }
func (c *erroringChanger) Chtimes(name string, atime, mtime time.Time) error { return c.err }

// mockHandler satisfies Handler for driving onWrite directly.
type mockHandler struct {
	fs billy.Filesystem
}

func (h *mockHandler) Mount(ctx context.Context, conn net.Conn, req MountRequest) (MountStatus, billy.Filesystem, []AuthFlavor) {
	return MountStatusOk, h.fs, nil
}
func (h *mockHandler) Change(billy.Filesystem) billy.Change { return nil }
func (h *mockHandler) FSStat(ctx context.Context, f billy.Filesystem, s *FSStat) error {
	return nil
}
func (h *mockHandler) ToHandle(f billy.Filesystem, path []string) []byte { return []byte{1} }
func (h *mockHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	return h.fs, []string{"f.bin"}, nil
}
func (h *mockHandler) InvalidateHandle(billy.Filesystem, []byte) error { return nil }
func (h *mockHandler) HandleLimit() int                                { return 100 }

var _ fs.FileInfo = (*mockFileInfo)(nil)
var _ billy.Filesystem = (*mockFS)(nil)
var _ Handler = (*mockHandler)(nil)

// --- Apply: SetSize arm --------------------------------------------------

// TestApplySetSizeClosesHandleOnTruncateError: the opened write handle MUST
// be closed when Truncate fails. A dropped handle leaks the JuiceMount
// spool-entry refcount → the entry is never finalized or drained.
func TestApplySetSizeClosesHandleOnTruncateError(t *testing.T) {
	file := &trackFile{name: "f.bin", truncErr: fmt.Errorf("spoolWriteFile.Truncate: not supported")}
	fsys := &mockFS{file: file, lstatInfo: &mockFileInfo{name: "f.bin", size: 10, mode: 0o644}}

	size := uint64(4096)
	attrs := &SetFileAttributes{SetSize: &size}
	err := attrs.Apply(nil, fsys, "f.bin")
	if err == nil {
		t.Fatal("expected an error from the failing Truncate")
	}
	if file.closeCalls == 0 {
		t.Fatal("LEAK: opened write handle was not Closed after Truncate failed (spool refcount leak)")
	}

	// And the error must be reply-encodable as a proper NFS status — see
	// the formatter assertion helper below.
	assertWellFormedSetattrError(t, err)
}

// TestApplySetSizeClosesHandleOnOversize: the out-of-range size guard runs
// AFTER OpenFile succeeded — it must close the handle too.
func TestApplySetSizeClosesHandleOnOversize(t *testing.T) {
	file := &trackFile{name: "f.bin"}
	fsys := &mockFS{file: file, lstatInfo: &mockFileInfo{name: "f.bin", size: 10, mode: 0o644}}

	size := uint64(1) << 63 // > math.MaxInt64
	attrs := &SetFileAttributes{SetSize: &size}
	err := attrs.Apply(nil, fsys, "f.bin")
	if err == nil {
		t.Fatal("expected NFSStatusInval for out-of-range size")
	}
	if file.closeCalls == 0 {
		t.Fatal("LEAK: opened write handle was not Closed on the oversize guard path")
	}
	assertWellFormedSetattrError(t, err)
}

// TestApplyErrorsAreAlwaysReplyEncodable: every failure mode of Apply must
// surface as an error the SETATTR error formatter can encode as a status +
// wcc_data body. A raw error falls through to ResponseCodeSystemError — an
// RPC-level SYSTEM_ERR with NO NFS body — which macOS reports as EBADRPC
// "RPC struct is bad" (the fio rc=72 failure).
func TestApplyErrorsAreAlwaysReplyEncodable(t *testing.T) {
	rawErr := errors.New("backend exploded (raw, unwrapped)")
	mode := uint32(0o755)
	now := time.Now()
	size := uint64(4096)

	cases := []struct {
		name    string
		attrs   *SetFileAttributes
		changer billy.Change
		file    *trackFile
	}{
		{
			name:    "chmod error",
			attrs:   &SetFileAttributes{SetMode: &mode},
			changer: &erroringChanger{err: rawErr},
			file:    &trackFile{name: "f.bin"},
		},
		{
			name:    "chtimes error",
			attrs:   &SetFileAttributes{SetAtime: &now, SetMtime: &now},
			changer: &erroringChanger{err: rawErr},
			file:    &trackFile{name: "f.bin"},
		},
		{
			name:  "truncate error",
			attrs: &SetFileAttributes{SetSize: &size},
			file:  &trackFile{name: "f.bin", truncErr: rawErr},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsys := &mockFS{file: tc.file, lstatInfo: &mockFileInfo{name: "f.bin", size: 10, mode: 0o644}}
			err := tc.attrs.Apply(tc.changer, fsys, "f.bin")
			if err == nil {
				t.Fatal("expected error")
			}
			assertWellFormedSetattrError(t, err)
		})
	}
}

// assertWellFormedSetattrError checks the property BUG 4 violated: the error
// must format (via the SETATTR wcc_data error formatter) into a SUCCESSFUL
// RPC reply carrying an NFS status + wcc_data — never an RPC-level
// SYSTEM_ERR.
func assertWellFormedSetattrError(t *testing.T, err error) {
	t.Helper()
	rpcErr := wccDataErrorFormatter(err)
	if rpcErr.Code() != ResponseCodeSuccess {
		t.Fatalf("error %q formats to RPC accept_stat=%d (SYSTEM_ERR class) — macOS surfaces this as "+
			"EBADRPC 'RPC struct is bad'; want ResponseCodeSuccess with an NFS status body", err, rpcErr.Code())
	}
	body, mErr := rpcErr.MarshalBinary()
	if mErr != nil {
		t.Fatalf("marshal: %v", mErr)
	}
	// 4-byte NFS status + 8-byte wcc_data (pre=FALSE, post=FALSE), per RFC
	// 1813: every SETATTR reply, success or failure, carries obj_wcc.
	if len(body) != 12 {
		t.Fatalf("SETATTR error body = %d bytes, want 12 (status + wcc_data)", len(body))
	}
}

// --- onWrite -------------------------------------------------------------

// TestOnWriteClosesHandleWhenWriteAtFails drives the real onWrite handler
// with an XDR-encoded WRITE request against a file whose WriteAt fails (the
// shape ErrSpoolFull produces). The opened handle must be Closed on the
// error path or the spool entry refcount leaks.
func TestOnWriteClosesHandleWhenWriteAtFails(t *testing.T) {
	file := &trackFile{name: "f.bin", writeAtErr: errors.New("spool: capacity full")}
	fsys := &mockFS{file: file, lstatInfo: &mockFileInfo{name: "f.bin", size: 0, mode: 0o644}}
	handler := &mockHandler{fs: fsys}

	payload := []byte("0123456789abcdef")
	var body bytes.Buffer
	if err := xdr.Write(&body, writeArgs{
		Handle: []byte{1},
		Offset: 0,
		Count:  uint32(len(payload)),
		How:    uint32(unstable),
		Data:   payload,
	}); err != nil {
		t.Fatalf("encode write args: %v", err)
	}

	w := &response{req: &request{Body: bytes.NewReader(body.Bytes())}}
	err := onWrite(context.Background(), w, handler)
	if err == nil {
		t.Fatal("expected onWrite to fail when WriteAt errors")
	}
	if file.closeCalls == 0 {
		t.Fatal("LEAK: onWrite dropped the opened handle without Close after a WriteAt failure " +
			"(this is the spool refcount leak — entry stuck in `writing` forever)")
	}
	var statusErr *NFSStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("onWrite error %T is not an NFSStatusError — reply would be RPC-level garbage", err)
	}
}
