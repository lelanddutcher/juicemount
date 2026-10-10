package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestSpoolReadAcknowledgedBoundaryErrors(t *testing.T) {
	for _, tail := range []string{"incomplete", "logical-eof", "short-stage", "missing-stage"} {
		for _, method := range []string{"ReadAt", "Read"} {
			t.Run(tail+"/"+method, func(t *testing.T) {
				s := newTestSpoolStore(t, 0)
				e, err := s.OpenWrite("/boundary.mov")
				if err != nil {
					t.Fatal(err)
				}
				defer e.CloseAndDelete("fixture cleanup")
				prefix := []byte("prefix")
				if _, err := e.WriteAt(prefix, 0); err != nil {
					t.Fatal(err)
				}
				if tail == "incomplete" {
					if _, err := e.WriteAt([]byte("suffix"), 64); err != nil {
						t.Fatal(err)
					}
				}
				stage := filepath.Join(t.TempDir(), "stage")
				data := prefix
				if tail == "short-stage" {
					data = prefix[:5]
				}
				if tail != "missing-stage" {
					if err := os.WriteFile(stage, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				e.SetStreamDest(stage)
				e.publishPunchedEnd(6)
				r := &spoolReadFile{name: e.NFSPath(), entry: e}
				defer r.Close()
				buf := bytes.Repeat([]byte{0xEE}, 4)
				var n int
				if method == "Read" {
					if _, err := r.Seek(4, io.SeekStart); err != nil {
						t.Fatal(err)
					}
					n, err = r.Read(buf)
				} else {
					n, err = r.ReadAt(buf, 4)
				}
				wantN := 2
				var wantErr error
				switch tail {
				case "incomplete":
					wantErr = pin.ErrSpoolIncomplete
				case "logical-eof":
					if method == "ReadAt" {
						wantErr = io.EOF
					}
				case "short-stage":
					wantN, wantErr = 1, io.ErrUnexpectedEOF
				case "missing-stage":
					wantN, wantErr = 0, pin.ErrSpoolDrained
				}
				if n != wantN || !errors.Is(err, wantErr) || !bytes.Equal(buf[:n], prefix[4:4+n]) {
					t.Fatalf("n=%d err=%v bytes=%x; want n=%d err=%v", n, err, buf[:n], wantN, wantErr)
				}
				if method == "Read" {
					if pos, _ := r.Seek(0, io.SeekCurrent); pos != 4+int64(n) {
						t.Errorf("cursor=%d want=%d", pos, 4+n)
					}
				}
			})
		}
	}
}
