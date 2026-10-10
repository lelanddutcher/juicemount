//go:build darwin

package nfs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Exercise a real local hole punch through the production stream advance.
// The stage is a private ordinary file, not a mounted JuiceFS durability test.
func TestSpoolSequentialReadUsesPunchedStage(t *testing.T) {
	e, src, _ := streamFixture(t, 256<<10)
	defer e.CloseAndDelete("fixture cleanup")
	stage, err := os.OpenFile(filepath.Join(t.TempDir(), "stage"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	e.SetStreamDest(stage.Name())
	const prefix = 64 << 10
	want := make([]byte, prefix)
	if _, err := src.ReadAt(want, 0); err != nil {
		t.Fatal(err)
	}
	punched, err := advanceStream(e, src, stage, nil, spoolMetaDurability{meta: e.store.meta}, destReadbackVerifier{f: stage}, prefix)
	if err != nil || punched != prefix {
		t.Fatalf("actual stream punch: n=%d err=%v", punched, err)
	}
	zero := make([]byte, prefix)
	if _, err := src.ReadAt(zero, 0); err != nil || !bytes.Equal(zero, make([]byte, prefix)) {
		t.Fatalf("fixture is not hole-punched: %v", err)
	}
	r := &spoolReadFile{name: e.NFSPath(), entry: e}
	defer r.Close()
	got := make([]byte, 4096)
	if n, err := r.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, want[:len(got)]) {
		t.Fatalf("invalid ReadAt stage reference: n=%d err=%v", n, err)
	}
	for i := 0; i < 2; i++ {
		if n, err := r.Read(got); err != nil || n != len(got) || !bytes.Equal(got, want[i*len(got):(i+1)*len(got)]) {
			t.Errorf("sequential read served punched zeros: read=%d n=%d err=%v", i, n, err)
		}
	}
	if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != int64(2*len(got)) {
		t.Errorf("stage position: %d %v", pos, err)
	}
	if _, err := r.Seek(prefix, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := r.ReadAt(want[:len(got)], prefix); err != nil || n != len(got) {
		t.Fatalf("tail reference: %d %v", n, err)
	}
	if n, err := r.Read(got); err != nil || n != len(got) || !bytes.Equal(got, want[:len(got)]) {
		t.Errorf("local tail: %d %v", n, err)
	}
}
