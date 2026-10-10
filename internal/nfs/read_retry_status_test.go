package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	billy "github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type retryReadFile struct {
	*eofTestFile
	n   int
	err error
}

func (f *retryReadFile) ReadAt(p []byte, _ int64) (int, error) {
	copy(p, []byte("body"))
	return f.n, f.err
}

type retryAdmissionFile struct {
	*eofTestFile
	beginErr, checkErr error
}

func (f *retryAdmissionFile) BeginReadAdmission() (func() error, func(), error) {
	if f.beginErr != nil {
		return nil, nil, f.beginErr
	}
	return func() error { return f.checkErr }, nil, nil
}

type retryPreparationFile struct {
	*eofTestFile
	err error
}

func (f *retryPreparationFile) PrepareResidentRead(uint64, uint32) (*ResidentRead, error) {
	return nil, f.err
}

type retryOpenFS struct {
	*eofTestFS
	err error
}

func (f *retryOpenFS) Open(string) (billy.File, error) { return nil, f.err }

type retrySizeFile struct{ *eofTestFile }

func (f *retrySizeFile) AuthoritativeSize() (int64, error) { return 0, syscall.EAGAIN }

type retryStatFS struct{ *eofTestFS }

func (f *retryStatFS) Stat(string) (os.FileInfo, error) { return nil, syscall.EAGAIN }

// Exercise the READ handler and its actual RPC error formatter. A failed
// subread may have copied bytes into a private buffer, but must serialize
// only READ3resfail (status plus absent post-op attrs), never a success body.
func TestReadRetryStatusOnWire(t *testing.T) {
	for _, tc := range []struct {
		name   string
		makeFS func() billy.Filesystem
		want   NFSStatus
		juke   int64
	}{
		{"plain data EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retryReadFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, n: 0, err: syscall.EAGAIN}, size: 4}
		}, NFSStatusJukebox, 1},
		{"wrapped partial EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retryReadFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, n: 2, err: fmt.Errorf("mapping: %w", syscall.EAGAIN)}, size: 4}
		}, NFSStatusJukebox, 1},
		{"open EAGAIN", func() billy.Filesystem { return &retryOpenFS{eofTestFS: &eofTestFS{}, err: syscall.EAGAIN} }, NFSStatusJukebox, 1},
		{"open spool incomplete remains IO", func() billy.Filesystem { return &retryOpenFS{eofTestFS: &eofTestFS{}, err: pin.ErrSpoolIncomplete} }, NFSStatusIO, 0},
		{"admission EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retryAdmissionFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, beginErr: fmt.Errorf("admission: %w", syscall.EAGAIN)}, size: 4}
		}, NFSStatusJukebox, 1},
		{"preparation EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retryPreparationFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, err: syscall.EAGAIN}, size: 4}
		}, NFSStatusJukebox, 1},
		{"size EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retrySizeFile{&eofTestFile{}}}
		}, NFSStatusJukebox, 1},
		{"stat EAGAIN", func() billy.Filesystem {
			return &retryStatFS{&eofTestFS{file: &eofTestFile{}}}
		}, NFSStatusJukebox, 1},
		{"final admission EAGAIN", func() billy.Filesystem {
			return &eofTestFS{file: &retryAdmissionFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, checkErr: syscall.EAGAIN}, size: 4}
		}, NFSStatusJukebox, 1},
		{"genuine EIO", func() billy.Filesystem {
			return &eofTestFS{file: &retryReadFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, n: 0, err: syscall.EIO}, size: 4}
		}, NFSStatusIO, 0},
		{"data ENOENT stays IO", func() billy.Filesystem {
			return &eofTestFS{file: &retryReadFile{eofTestFile: &eofTestFile{body: []byte("body"), cached: 4}, n: 0, err: syscall.ENOENT}, size: 4}
		}, NFSStatusIO, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var args bytes.Buffer
			count := uint32(4)
			if tc.name == "size EAGAIN" || tc.name == "stat EAGAIN" {
				count = CheckRead + 1
			}
			if err := xdr.Write(&args, nfsReadArgs{Handle: []byte{1}, Count: count}); err != nil {
				t.Fatal(err)
			}
			w := newTestResponse(args.Bytes())
			w.req.Header.Prog, w.req.Header.Proc = nfsServiceID, uint32(NFSProcedureRead)
			key := inflightOpName(w.req)
			before := snapshotJukebox()[key]
			err := onRead(context.Background(), w, &mockHandler{fs: tc.makeFS()})
			var status *NFSStatusError
			if !errors.As(err, &status) || status.NFSStatus != tc.want || w.writer.Len() != 0 {
				t.Fatalf("handler err=%v body=%d want=%v", err, w.writer.Len(), tc.want)
			}
			if err := w.conn.err(context.Background(), w, err); err != nil {
				t.Fatal(err)
			}
			if got := snapshotJukebox()[key] - before; got != tc.juke {
				t.Fatalf("JUKEBOX delta=%d want=%d", got, tc.juke)
			}
			body := replyBodyAfterRPCHeader(t, w.writer.Bytes())
			if len(body) != 8 || binary.BigEndian.Uint32(body[:4]) != uint32(tc.want) || binary.BigEndian.Uint32(body[4:]) != 0 {
				t.Fatalf("READ3resfail=%x", body)
			}
		})
	}
}
