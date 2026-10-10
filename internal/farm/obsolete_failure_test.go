package farm

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func audioOnlyProbe(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ffprobe")
	data := `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"audio","codec_name":"mp3","channels":2,"sample_rate":"48000"}],"format":{"format_name":"mp3","duration":"1.0","size":"64"}}'
`
	if err := os.WriteFile(p, []byte(data), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func failedRowFixture(t *testing.T, store *derivatives.Store, source, kind string) (uint64, string) {
	t.Helper()
	fi, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	inode := uint64(st.Ino)
	hash, err := SampleHash(source, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: kind, Status: "failed", Producer: "linux-farm", Version: 1,
		Hash: &hash,
	}); err != nil {
		t.Fatal(err)
	}
	return inode, hash
}

func assertAbsentRow(t *testing.T, store *derivatives.Store, inode uint64, kind string) {
	t.Helper()
	rows, err := store.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == kind {
			if row.Status != "absent" || row.BlobRelPath != nil {
				t.Fatalf("%s row = %+v, want blobless absent tombstone", kind, row)
			}
			return
		}
	}
	t.Fatalf("missing %s row", kind)
}

func TestProcessTombstonesFailedPreviewsWhenCurrentProbeFindsNoVideo(t *testing.T) {
	mount := t.TempDir()
	source := filepath.Join(mount, "audio.mp3")
	if err := os.WriteFile(source, make([]byte, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inode, _ := failedRowFixture(t, store, source, "thumbnail")
	_, _ = failedRowFixture(t, store, source, "filmstrip")
	res := Process(store, source, Options{
		Producer: "linux-farm", Version: 2, Mount: mount,
		Blobs: true, Filmstrip: true, FFprobeBin: audioOnlyProbe(t), RegenerateFresh: true,
	})
	if res.Err != nil || res.BlobErr != nil {
		t.Fatalf("audio-only repair failed: err=%v blob_err=%v", res.Err, res.BlobErr)
	}
	assertAbsentRow(t, store, inode, "thumbnail")
	assertAbsentRow(t, store, inode, "filmstrip")
}

func TestGenerateProxyTombstonesFailedRowWhenCurrentProbeFindsNoVideo(t *testing.T) {
	mount := t.TempDir()
	source := filepath.Join(mount, "audio.mp3")
	if err := os.WriteFile(source, make([]byte, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	inode, _ := failedRowFixture(t, store, source, "proxy")
	res := GenerateProxy(store, source, Options{
		Producer: "linux-farm", Version: 2, Mount: mount,
		ProxyVCodec: "libx264", FFprobeBin: audioOnlyProbe(t),
	})
	if res.Err != nil || res.Wrote {
		t.Fatalf("audio-only proxy repair = %+v", res)
	}
	assertAbsentRow(t, store, inode, "proxy")
}
