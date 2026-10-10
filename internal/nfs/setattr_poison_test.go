package nfs

import (
	"encoding/binary"
	"os"
	"syscall"
	"testing"
)

type poisonStatFS struct {
	mockFS
	err error
}

func (f *poisonStatFS) Lstat(string) (os.FileInfo, error) { return nil, f.err }

func TestApplyPoisonedLstatReturnsIO(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status NFSStatus
	}{
		{"poison", syscall.EIO, NFSStatusIO},
		{"absent", os.ErrNotExist, NFSStatusNoEnt},
		{"permission", os.ErrPermission, NFSStatusAccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size := uint64(0)
			err := (&SetFileAttributes{SetSize: &size}).Apply(nil, &poisonStatFS{err: tc.err}, "take.mov")
			if err == nil {
				t.Fatal("SETATTR acknowledged failed precondition")
			}
			assertWellFormedSetattrError(t, err)
			body, marshalErr := wccDataErrorFormatter(err).MarshalBinary()
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if got := binary.BigEndian.Uint32(body[:4]); got != uint32(tc.status) {
				t.Fatalf("SETATTR status=%d, want %d", got, tc.status)
			}
		})
	}
}
