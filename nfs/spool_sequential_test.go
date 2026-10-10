package nfs

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// Sequential consumers must not bypass the same hole admission used by READ
// RPCs. A suffix write creates a real unfilled range, not an all-zero fixture.
func TestSpoolSequentialReadPreservesIncompleteAdmission(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, err := s.OpenWrite("/sequential-hole.mov")
	if err != nil {
		t.Fatal(err)
	}
	defer e.CloseAndDelete("fixture cleanup")
	prefix, suffix := []byte("prefix"), []byte("suffix")
	if _, err := e.WriteAt(prefix, 0); err != nil {
		t.Fatal(err)
	}
	const suffixOffset = 4096
	if _, err := e.WriteAt(suffix, suffixOffset); err != nil {
		t.Fatal(err)
	}
	r := &spoolReadFile{name: e.NFSPath(), entry: e}
	defer r.Close()
	buf := make([]byte, 64)
	n, err := r.Read(buf)
	if err != nil || n != len(prefix) || !bytes.Equal(buf[:n], prefix) {
		t.Errorf("sequential prefix crossed into unwritten hole: n=%d err=%v", n, err)
	}
	if _, err := r.Seek(int64(len(prefix)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := r.ReadAt(buf, int64(len(prefix))); n != 0 || !errors.Is(err, pin.ErrSpoolIncomplete) {
		t.Fatalf("invalid ReadAt reference: n=%d err=%v", n, err)
	}
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, pin.ErrSpoolIncomplete) {
		t.Errorf("sequential hole became bytes: n=%d err=%v", n, err)
	}
	if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != int64(len(prefix)) {
		t.Errorf("refused read advanced position: %d %v", pos, err)
	}
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Errorf("zero-byte read entered incomplete range: n=%d err=%v", n, err)
	}
	// Filling the same hole must resume the existing sequential reader exactly
	// where admission stopped, without a close/reopen or a zero-filled prefix.
	fill := bytes.Repeat([]byte{0x71}, suffixOffset-len(prefix))
	if _, err := e.WriteAt(fill, int64(len(prefix))); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seek(int64(len(prefix)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(buf); err != nil || n != len(buf) || !bytes.Equal(buf, fill[:len(buf)]) {
		t.Errorf("filled hole did not resume: n=%d err=%v", n, err)
	}
	if _, err := r.Seek(suffixOffset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Read(buf); err != nil || n != len(suffix) || !bytes.Equal(buf[:n], suffix) {
		t.Errorf("short final read: n=%d err=%v", n, err)
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Errorf("read after final bytes: n=%d err=%v", n, err)
	}
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Errorf("zero-byte read at EOF: n=%d err=%v", n, err)
	}
}
