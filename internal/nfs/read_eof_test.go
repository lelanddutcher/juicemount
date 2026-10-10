package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"testing"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// These handles deliberately return (n,nil) at an exact boundary. Only a
// trusted complete body or descriptor size may turn that into protocol EOF.
type eofTestFile struct {
	trackFile
	body      []byte
	cached    int64
	short     bool
	shortEOF  bool
	emptyEOF  bool
	readCalls int
}

func (f *eofTestFile) CachedInfo() os.FileInfo { return &mockFileInfo{name: "f.bin", size: f.cached} }
func (f *eofTestFile) ReadAt(p []byte, off int64) (int, error) {
	f.readCalls++
	if f.emptyEOF {
		return 0, io.EOF
	}
	if off >= int64(len(f.body)) {
		return 0, io.EOF
	}
	if f.short && len(p) > 1 {
		p = p[:1]
	}
	n := copy(p, f.body[off:])
	if f.shortEOF {
		return n, io.EOF
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestReadEOFRetriesPrematureEmptyRead(t *testing.T) {
	file := &eofCompleteFile{&eofTestFile{body: []byte("body"), cached: 4, emptyEOF: true}}
	fs := &eofTestFS{file: file, size: 4}
	var args bytes.Buffer
	if err := xdr.Write(&args, nfsReadArgs{Handle: []byte{1}, Count: 4}); err != nil {
		t.Fatal(err)
	}
	w := newTestResponse(args.Bytes())
	err := onRead(context.Background(), w, &mockHandler{fs: fs})
	var status *NFSStatusError
	if !errors.As(err, &status) || status.NFSStatus != NFSStatusJukebox || w.writer.Len() != 0 {
		t.Fatalf("premature empty read=%v, reply bytes=%d; want retry without success reply", err, w.writer.Len())
	}
	if file.closeCalls != 1 {
		t.Fatalf("Close calls=%d", file.closeCalls)
	}
}

type eofCompleteFile struct{ *eofTestFile }

func (f *eofCompleteFile) CompleteCachedSize() (int64, bool, error) {
	return int64(len(f.body)), true, nil
}

type eofDescriptorFile struct{ *eofTestFile }

func (f *eofDescriptorFile) AuthoritativeSize() (int64, error) { return int64(len(f.body)), nil }

type eofLiveFile struct{ *eofTestFile }

func (f *eofLiveFile) LiveSize() (int64, bool) { return int64(len(f.body)), true }

type eofTestFS struct {
	billy.Filesystem
	file billy.File
	size int64
}

func (f *eofTestFS) Open(string) (billy.File, error) { return f.file, nil }
func (f *eofTestFS) Join(...string) string           { return "f.bin" }
func (f *eofTestFS) Stat(string) (os.FileInfo, error) {
	return &mockFileInfo{name: "f.bin", size: f.size}, nil
}

func TestReadEOFTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		off          uint64
		count        uint32
		cached, stat int64
		size, want   int
		eof          uint32
		short        bool
	}{
		{"complete-small-exact", "complete", 0, 4, 4, 4, 4, 4, 1, false},
		{"complete-overrides-high-mirror", "complete", 0, 65536, 999999, 999999, 4, 4, 1, false},
		{"complete-zero-at-end", "complete", 4, 0, 4, 4, 4, 0, 1, false},
		{"complete-large-offset", "complete", math.MaxUint64, 65536, 4, 4, 4, 0, 1, false},
		{"complete-small-large-offset", "complete", math.MaxUint64, 1, 4, 4, 4, 0, 1, false},
		{"complete-short-prefix", "complete", 0, 4, 4, 4, 4, 1, 0, true},
		{"complete-premature-eof", "complete", 0, 4, 4, 4, 4, 1, 0, true},
		{"descriptor-oversized", "descriptor", 0, 65536, 2, 2, 4, 4, 1, false},
		{"descriptor-past-end", "descriptor", 8, 65536, 2, 2, 4, 0, 1, false},
		{"live-empty", "live", 0, 65536, 0, 0, 0, 0, 1, false},
		{"mirror-exact-is-not-proof", "mirror", 0, 65536, 65536, 65536, 131072, 65536, 0, false},
		{"stat-clamp-cannot-hide-bytes", "mirror", 0, 65536, 2, 2, 4, 4, 1, false},
		{"stat-zero-cannot-hide-bytes", "mirror", 4, 65536, 4, 4, 8, 4, 1, false},
		{"stat-stale-low-full-request", "mirror", 0, 65536, 2, 2, 65537, 65536, 0, false},
		{"stat-stale-zero-full-request", "mirror", 0, 65536, 0, 0, 65537, 65536, 0, false},
		{"stat-short-prefix-is-not-eof", "mirror", 0, 65536, 0, 0, 8, 1, 0, true},
		{"stat-exact-end-read-proves-eof", "mirror", 4, 65536, 4, 4, 4, 0, 1, false},
		{"stat-past-end-read-proves-eof", "mirror", 8, 65536, 4, 4, 4, 0, 1, false},
		{"stat-empty-read-proves-eof", "mirror", 0, 65536, 0, 0, 0, 0, 1, false},
		{"stat-zero-count-is-not-proof", "mirror", 4, 0, 4, 4, 4, 0, 0, false},
		{"stat-zero-count-large-offset", "mirror", math.MaxUint64, 0, 4, 4, 4, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &eofTestFile{body: bytes.Repeat([]byte{'x'}, tc.size), cached: tc.cached, short: tc.short, shortEOF: tc.name == "complete-premature-eof"}
			var file billy.File = base
			switch tc.source {
			case "complete":
				file = &eofCompleteFile{base}
			case "descriptor":
				file = &eofDescriptorFile{base}
			case "live":
				file = &eofLiveFile{base}
			}
			fs := &eofTestFS{file: file, size: tc.stat}
			var args bytes.Buffer
			if err := xdr.Write(&args, nfsReadArgs{Handle: []byte{1}, Offset: tc.off, Count: tc.count}); err != nil {
				t.Fatal(err)
			}
			w := newTestResponse(args.Bytes())
			if err := onRead(context.Background(), w, &mockHandler{fs: fs}); err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(replyBodyAfterRPCHeader(t, w.writer.Bytes()))
			if status := readU32(t, r); status != 0 {
				t.Fatalf("status=%d", status)
			}
			if readU32(t, r) == 1 {
				skipN(t, r, 84)
			}
			var got nfsReadResponse
			if err := xdr.Read(r, &got); err != nil {
				t.Fatal(err)
			}
			if got.Count != uint32(tc.want) || got.EOF != tc.eof || !bytes.Equal(got.Data, bytes.Repeat([]byte{'x'}, tc.want)) || r.Len() != 0 {
				t.Fatalf("READ count=%d EOF=%d bytes=%d trailing=%d; want count=%d EOF=%d", got.Count, got.EOF, len(got.Data), r.Len(), tc.want, tc.eof)
			}
			if tc.count == 0 && base.readCalls != 0 {
				t.Fatalf("zero-count request performed %d reads", base.readCalls)
			}
			if base.closeCalls != 1 {
				t.Fatalf("Close calls=%d", base.closeCalls)
			}
		})
	}
}

// Untrusted size hints cannot mask an unrepresentable ReaderAt offset, and
// the incomplete-reader backstop must still refuse a stale-low size hint.
type eofIncompleteFile struct{ *eofTestFile }

func (*eofIncompleteFile) IncompleteAt(int64) bool { return true }

func TestReadEOFUntrustedSizeRefusals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		offset     uint64
		count      uint32
		incomplete bool
		status     NFSStatus
	}{
		{"incomplete-stale-zero", 0, CheckRead + 1, true, NFSStatusJukebox},
		{"large-offset-small", math.MaxInt64 + 1, 1, false, NFSStatusInval},
		{"large-offset-wide", math.MaxUint64, CheckRead + 1, false, NFSStatusInval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &eofTestFile{body: []byte("body"), cached: 0}
			var file billy.File = base
			if tc.incomplete {
				file = &eofIncompleteFile{base}
			}
			var args bytes.Buffer
			if err := xdr.Write(&args, nfsReadArgs{Handle: []byte{1}, Offset: tc.offset, Count: tc.count}); err != nil {
				t.Fatal(err)
			}
			w := newTestResponse(args.Bytes())
			err := onRead(context.Background(), w, &mockHandler{fs: &eofTestFS{file: file, size: 0}})
			var status *NFSStatusError
			if !errors.As(err, &status) || status.NFSStatus != tc.status || base.readCalls != 0 || base.closeCalls != 1 || w.writer.Len() != 0 {
				t.Fatalf("err=%v reads=%d closes=%d body=%d", err, base.readCalls, base.closeCalls, w.writer.Len())
			}
		})
	}
}
