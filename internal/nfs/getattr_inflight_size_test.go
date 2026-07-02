package nfs

// Integration regression for #85/#65/#38: after a large in-flight WRITE, the
// GETATTR reply — and the WRITE/COMMIT reply post-op attrs — must carry the
// FULL written high-water, not a truncated (~4 MiB / one-block) contiguous
// prefix. In production the spool shadow's FileInfo.Size() drives all three
// (onGetAttr → Lstat; onWrite → WriteWcc(tryStat); onCommit → WritePostOpAttrs
// (tryStat)). This test drives the REAL onWrite/onCommit/onGetAttr handlers
// against a filesystem whose Lstat mirrors the written high-water (the property
// the spool shadow now guarantees) and decodes the encoded fattr3 Filesize.

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// sizeTrackingFile records the written high-water (max off+len) the way a spool
// entry's writtenEnd does, so Lstat can report it during an in-flight copy.
type sizeTrackingFile struct {
	name string
	mu   sync.Mutex
	hw   int64 // written high-water
}

func (f *sizeTrackingFile) Name() string                      { return f.name }
func (f *sizeTrackingFile) Read(p []byte) (int, error)        { return 0, nil }
func (f *sizeTrackingFile) ReadAt([]byte, int64) (int, error) { return 0, nil }
func (f *sizeTrackingFile) Seek(int64, int) (int64, error)    { return 0, nil }
func (f *sizeTrackingFile) Close() error                      { return nil }
func (f *sizeTrackingFile) Lock() error                       { return nil }
func (f *sizeTrackingFile) Unlock() error                     { return nil }
func (f *sizeTrackingFile) Write(p []byte) (int, error)       { return len(p), nil }
func (f *sizeTrackingFile) Truncate(size int64) error {
	f.mu.Lock()
	f.hw = size // authoritative shrink/grow mirrors spool.Truncate(writtenEnd=size)
	f.mu.Unlock()
	return nil
}
func (f *sizeTrackingFile) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	f.mu.Lock()
	if end > f.hw {
		f.hw = end
	}
	f.mu.Unlock()
	return len(p), nil
}
func (f *sizeTrackingFile) highWater() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hw
}

// liveSizeInfo is the FileInfo returned by Lstat: Size() reflects the live
// written high-water, exactly like the spool shadow's spoolFileInfoForEntry.
type liveSizeInfo struct {
	name string
	f    *sizeTrackingFile
}

func (i *liveSizeInfo) Name() string       { return i.name }
func (i *liveSizeInfo) Size() int64        { return i.f.highWater() }
func (i *liveSizeInfo) Mode() os.FileMode  { return 0o644 }
func (i *liveSizeInfo) ModTime() time.Time { return time.Unix(1700000000, 0) }
func (i *liveSizeInfo) IsDir() bool        { return false }
func (i *liveSizeInfo) Sys() any           { return nil }

// sizeTrackingFS: OpenFile hands out the shared tracking file; Lstat/Stat
// report its live high-water. CommitFile is a no-op durability barrier so the
// onWrite(FILE_SYNC)/onCommit paths exercise the Committer branch.
type sizeTrackingFS struct{ file *sizeTrackingFile }

func (m *sizeTrackingFS) Create(filename string) (billy.File, error) { return m.file, nil }
func (m *sizeTrackingFS) Open(filename string) (billy.File, error)   { return m.file, nil }
func (m *sizeTrackingFS) OpenFile(string, int, os.FileMode) (billy.File, error) {
	return m.file, nil
}
func (m *sizeTrackingFS) Stat(string) (os.FileInfo, error) {
	return &liveSizeInfo{name: m.file.name, f: m.file}, nil
}
func (m *sizeTrackingFS) Lstat(string) (os.FileInfo, error) {
	return &liveSizeInfo{name: m.file.name, f: m.file}, nil
}
func (m *sizeTrackingFS) Rename(string, string) error { return nil }
func (m *sizeTrackingFS) Remove(string) error         { return nil }
func (m *sizeTrackingFS) Join(elem ...string) string {
	out := ""
	for i, e := range elem {
		if i > 0 {
			out += "/"
		}
		out += e
	}
	return out
}
func (m *sizeTrackingFS) TempFile(string, string) (billy.File, error) { return nil, os.ErrInvalid }
func (m *sizeTrackingFS) ReadDir(string) ([]os.FileInfo, error)       { return nil, nil }
func (m *sizeTrackingFS) MkdirAll(string, os.FileMode) error          { return nil }
func (m *sizeTrackingFS) Symlink(string, string) error                { return os.ErrInvalid }
func (m *sizeTrackingFS) Readlink(string) (string, error)             { return "", os.ErrInvalid }
func (m *sizeTrackingFS) Chroot(string) (billy.Filesystem, error)     { return nil, os.ErrInvalid }
func (m *sizeTrackingFS) Root() string                                { return "/" }

// CommitFile satisfies the Committer interface (fsync durability barrier).
func (m *sizeTrackingFS) CommitFile(string) error { return nil }

var (
	_ billy.Filesystem = (*sizeTrackingFS)(nil)
	_ Committer        = (*sizeTrackingFS)(nil)
)

// driveWrite XDR-encodes a WRITE and runs the real onWrite, returning the
// captured reply body (post-RPC-header) for post-op attr decoding.
func driveWrite(t *testing.T, h Handler, off uint64, data []byte, how uint32) []byte {
	t.Helper()
	var body bytes.Buffer
	if err := xdr.Write(&body, writeArgs{
		Handle: []byte{1},
		Offset: off,
		Count:  uint32(len(data)),
		How:    how,
		Data:   data,
	}); err != nil {
		t.Fatalf("encode write args: %v", err)
	}
	w := newTestResponse(body.Bytes())
	if err := onWrite(context.Background(), w, h); err != nil {
		t.Fatalf("onWrite(off=%d): %v", off, err)
	}
	return replyBodyAfterRPCHeader(t, w.writer.Bytes())
}

// newTestResponse builds a response backed by an in-memory writer and a minimal
// conn/Server, so handlers that emit w.Server.ID (onWrite/onCommit verifier) or
// the RPC reply header don't nil-panic when driven directly in a unit test.
func newTestResponse(reqBody []byte) *response {
	return &response{
		conn:   &conn{Server: &Server{}},
		writer: &bytes.Buffer{},
		req:    &request{Body: bytes.NewReader(reqBody)},
	}
}

// replyBodyAfterRPCHeader strips the fixed 24-byte accepted-reply RPC framing
// (xid 4 + msgType 4 + reply/MsgAccepted 4 + AuthNull verf 8 + acceptStat 4)
// written by response.writeHeader, leaving the NFS procedure result body.
func replyBodyAfterRPCHeader(t *testing.T, full []byte) []byte {
	t.Helper()
	const rpcAcceptedHeaderLen = 24
	if len(full) < rpcAcceptedHeaderLen {
		t.Fatalf("reply too short: %d bytes", len(full))
	}
	return full[rpcAcceptedHeaderLen:]
}

// decodePostOpFilesizeWcc reads the Filesize from a reply whose body is
// status(uint32) + pre-op-present(uint32, 0 or wcc_attr) + post_op_attr. Both
// the WRITE reply (WriteWcc, pre=nil) and the COMMIT reply (an explicit
// uint32(0) "no pre-op cache data" then WritePostOpAttrs) share this shape: a
// leading zero-or-wcc pre-op field precedes the post_op_attr's present flag.
// Filesize sits 20 bytes into the fattr3 (5 uint32 fields: type/mode/nlink/uid/
// gid) after the present flag.
func decodePostOpFilesizeWcc(t *testing.T, b []byte) uint64 {
	t.Helper()
	r := bytes.NewReader(b)
	skipU32(t, r) // NFS status
	// pre_op field: WRITE emits a pre_op_attr (present flag; wcc_attr if set),
	// COMMIT emits a bare uint32(0). Either way a present flag of 1 is followed
	// by a 24-byte wcc_attr (size(8)+mtime(8)+ctime(8)); 0 by nothing.
	if readU32(t, r) == 1 {
		skipN(t, r, 24)
	}
	return readFattr3Filesize(t, r)
}

// decodeGetAttrFilesize reads the GETATTR reply body: status(uint32) then a
// bare fattr3 (no post_op present flag).
func decodeGetAttrFilesize(t *testing.T, b []byte) uint64 {
	t.Helper()
	r := bytes.NewReader(b)
	skipU32(t, r) // NFS status
	return readFattr3FilesizeBare(t, r)
}

// readFattr3Filesize consumes the post_op_attr present flag then the fattr3.
func readFattr3Filesize(t *testing.T, r *bytes.Reader) uint64 {
	t.Helper()
	if readU32(t, r) != 1 {
		t.Fatal("post_op_attr present flag = 0 (attributes absent) — post-op attrs must carry the size")
	}
	return readFattr3FilesizeBare(t, r)
}

// readFattr3FilesizeBare reads a fattr3 and returns Filesize. Layout: type(4),
// mode(4), nlink(4), uid(4), gid(4) = 20 bytes, then size(8).
func readFattr3FilesizeBare(t *testing.T, r *bytes.Reader) uint64 {
	t.Helper()
	skipN(t, r, 20)
	return readU64(t, r)
}

func readU32(t *testing.T, r *bytes.Reader) uint32 {
	t.Helper()
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		t.Fatalf("readU32: %v", err)
	}
	return binary.BigEndian.Uint32(b[:])
}
func readU64(t *testing.T, r *bytes.Reader) uint64 {
	t.Helper()
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		t.Fatalf("readU64: %v", err)
	}
	return binary.BigEndian.Uint64(b[:])
}
func skipU32(t *testing.T, r *bytes.Reader) { readU32(t, r) }
func skipN(t *testing.T, r *bytes.Reader, n int) {
	t.Helper()
	if _, err := r.Seek(int64(n), io.SeekCurrent); err != nil {
		t.Fatalf("skipN(%d): %v", n, err)
	}
}

// TestGetAttrFullSizeDuringInflightWrite: drive onWrite RPCs totaling >4 MiB
// (no finalize/drain), then onGetAttr — the reported Filesize must equal the
// total written. Also asserts the onWrite and onCommit reply post-op attrs
// carry the full high-water (the values that seed the macOS attr cache).
func TestGetAttrFullSizeDuringInflightWrite(t *testing.T) {
	file := &sizeTrackingFile{name: "big.mov"}
	fsys := &sizeTrackingFS{file: file}
	handler := &mockHandler{fs: fsys}

	const chunk = 1 << 20 // 1 MiB
	const nchunks = 6
	const total = chunk * nchunks
	payload := bytes.Repeat([]byte{0x7E}, chunk)

	var lastWriteReply []byte
	for i := 0; i < nchunks; i++ {
		lastWriteReply = driveWrite(t, handler, uint64(i)*chunk, payload, uint32(unstable))
	}

	// The final WRITE reply's post-op attrs must carry the full high-water —
	// this is what rides out to the macOS attr cache after each WRITE.
	if got := decodePostOpFilesizeWcc(t, lastWriteReply); got != total {
		t.Fatalf("onWrite post-op Filesize = %d, want %d (full high-water; a truncated value seeds a stale macOS attr cache)", got, total)
	}

	// GETATTR before any finalize/drain must report the full written total.
	{
		var body bytes.Buffer
		if err := xdr.Write(&body, []byte{1} /* handle opaque */); err != nil {
			t.Fatalf("encode getattr handle: %v", err)
		}
		w := newTestResponse(body.Bytes())
		if err := onGetAttr(context.Background(), w, handler); err != nil {
			t.Fatalf("onGetAttr: %v", err)
		}
		reply := replyBodyAfterRPCHeader(t, w.writer.Bytes())
		if got := decodeGetAttrFilesize(t, reply); got != total {
			t.Fatalf("onGetAttr Filesize = %d, want %d (in-flight GETATTR must report the full size, not a ~4 MiB block prefix)", got, total)
		}
	}

	// COMMIT reply post-op attrs must also carry the full high-water.
	{
		var body bytes.Buffer
		if err := xdr.Write(&body, []byte{1}); err != nil {
			t.Fatalf("encode commit handle: %v", err)
		}
		w := newTestResponse(body.Bytes())
		if err := onCommit(context.Background(), w, handler); err != nil {
			t.Fatalf("onCommit: %v", err)
		}
		reply := replyBodyAfterRPCHeader(t, w.writer.Bytes())
		if got := decodePostOpFilesizeWcc(t, reply); got != total {
			t.Fatalf("onCommit post-op Filesize = %d, want %d (full high-water)", got, total)
		}
	}
}
