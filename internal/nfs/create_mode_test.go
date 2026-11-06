package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"syscall"
	"testing"

	billy "github.com/go-git/go-billy/v5"
)

// modeFS records which entry point each CREATE, REMOVE and RMDIR reached.
type modeFS struct {
	*mockFS
	createErr error
	hows      []CreateHow
	calls     []string
}

func (f *modeFS) StatCacheOnly(string) (os.FileInfo, bool) { return nil, false }
func (f *modeFS) Create(string) (billy.File, error) {
	f.calls = append(f.calls, "Create")
	return &trackFile{name: "new.mov"}, nil
}
func (f *modeFS) CreateMode(name string, how CreateHow) (billy.File, error) {
	f.calls = append(f.calls, "CreateMode")
	f.hows = append(f.hows, how)
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &trackFile{name: name}, nil
}
func (f *modeFS) Remove(string) error { f.calls = append(f.calls, "Remove"); return nil }
func (f *modeFS) RemoveDirectory(string) error {
	f.calls = append(f.calls, "RemoveDirectory")
	return nil
}

func newModeFS() *modeFS {
	return &modeFS{mockFS: &mockFS{lstatInfo: &mockFileInfo{name: "f.bin", mode: os.ModeDir | 0o755, dir: true}}}
}

func wireStatus(t *testing.T, w *response, err error) NFSStatus {
	t.Helper()
	if err == nil {
		return NFSStatusOk
	}
	formatted := w.errorFmt(err)
	wire, mErr := formatted.MarshalBinary()
	if formatted.Code() != ResponseCodeSuccess || mErr != nil || len(wire) < 4 {
		t.Fatalf("reply is not an NFS status: %v (%v)", err, mErr)
	}
	return NFSStatus(binary.BigEndian.Uint32(wire[:4]))
}

// CREATE hands the filesystem its mode and verifier, and a name the
// filesystem finds taken is NFS3ERR_EXIST under GUARDED and EXCLUSIVE
// (RFC 1813 section 3.3.8).
func TestCreatePassesModeAndVerifier(t *testing.T) {
	dirop := DirOpArg{Handle: []byte{1}, Filename: []byte("new.mov")}
	verf := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	cases := []struct {
		name      string
		body      func(*testing.T) *bytes.Buffer
		want      CreateHow
		takenWant NFSStatus
	}{
		{"unchecked", func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop, uint32(CreateUnchecked), emptySattr)
		}, CreateHow{Mode: CreateUnchecked}, NFSStatusAccess},
		{"guarded", func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop, uint32(CreateGuarded), emptySattr)
		}, CreateHow{Mode: CreateGuarded}, NFSStatusExist},
		{"exclusive", func(t *testing.T) *bytes.Buffer {
			return xdrBody(t, dirop, uint32(CreateExclusive), verf)
		}, CreateHow{Mode: CreateExclusive, Verifier: verf}, NFSStatusExist},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, taken := range []bool{false, true} {
				fs := newModeFS()
				want := NFSStatusOk
				if taken {
					fs.createErr = &os.PathError{Op: "create", Path: "f.bin/new.mov", Err: syscall.EEXIST}
					want = tc.takenWant
				}
				w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Body: tc.body(t)}}
				err := onCreate(context.Background(), w, &mockHandler{fs: fs})
				if len(fs.hows) != 1 || fs.hows[0] != tc.want {
					t.Fatalf("taken=%v: CreateMode got %+v, want %+v (calls %q)", taken, fs.hows, tc.want, fs.calls)
				}
				if got := wireStatus(t, w, err); got != want {
					t.Fatalf("taken=%v: status = %v, want %v (err %v)", taken, got, want, err)
				}
				if taken && !errors.Is(err, os.ErrExist) {
					t.Fatalf("lost the EEXIST cause: %v", err)
				}
			}
		})
	}
}

// RMDIR reaches the filesystem as RMDIR, so its remembered late outcome
// cannot answer a REMOVE of the same name, or the reverse.
func TestRmdirReachesRemoveDirectory(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   func(context.Context, *response, Handler) error
		want string
	}{
		{"REMOVE", onRemove, "Remove"},
		{"RMDIR", onRmDir, "RemoveDirectory"},
	} {
		fs := newModeFS()
		w := &response{conn: &conn{Server: &Server{}}, writer: &bytes.Buffer{}, req: &request{Body: xdrBody(t, DirOpArg{Handle: []byte{1}, Filename: []byte("x")})}}
		if err := tc.op(context.Background(), w, &mockHandler{fs: fs}); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(fs.calls) != 1 || fs.calls[0] != tc.want {
			t.Fatalf("%s reached %q, want %s", tc.name, fs.calls, tc.want)
		}
	}
}
