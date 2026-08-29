package farm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func TestProxyReceiptPreventsReencodeAfterIndexCommitFailure(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.mov")
	if err := os.WriteFile(source, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(dir, "ffprobe")
	probeScript := `#!/bin/sh
case "$*" in
  *"/dev/fd/3"*) echo h264 ;;
  *) echo '{"streams":[{"codec_type":"video","codec_name":"prores","width":1920,"height":1080,"r_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"10","size":"65536"}}' ;;
esac
`
	if err := os.WriteFile(probe, []byte(probeScript), 0o755); err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(dir, "encode-count")
	ffmpeg := filepath.Join(dir, "ffmpeg")
	ffmpegScript := "#!/bin/sh\nfor last do :; done\nprintf 'committed-proxy-bytes' > \"$last\"\nprintf '1\\n' >> " + counter + "\n"
	if err := os.WriteFile(ffmpeg, []byte(ffmpegScript), 0o755); err != nil {
		t.Fatal(err)
	}

	brokenStore, err := derivatives.Open(filepath.Join(dir, "broken.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := brokenStore.Close(); err != nil {
		t.Fatal(err)
	}
	opt := Options{
		Mount: dir, FFprobeBin: probe, FFmpegBin: ffmpeg,
		ProxyVCodec: "libx264", Producer: "receipt-test", Version: 1,
	}
	first := GenerateProxy(brokenStore, source, opt)
	if first.Err == nil || !first.Wrote {
		t.Fatalf("first result = %+v, want committed blob plus private-index error", first)
	}

	repairedStore, err := derivatives.Open(filepath.Join(dir, "repaired.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repairedStore.Close()
	second := GenerateProxy(repairedStore, source, opt)
	if second.Err != nil || !second.SkippedFresh || second.Wrote {
		t.Fatalf("receipt recovery result = %+v, want skipped committed proxy", second)
	}
	raw, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(strings.Fields(string(raw))); got != 1 {
		t.Fatalf("encoder executions = %d, want 1", got)
	}
	rows, err := repairedStore.Manifest(second.Inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == "proxy" && row.Status == "ready" && row.Codec != nil && *row.Codec == "h264" {
			return
		}
	}
	t.Fatalf("recovered store has no ready H.264 proxy row: %+v", rows)
}

func TestProxyReceiptCannotVouchForDifferentFinalBytes(t *testing.T) {
	dir := t.TempDir()
	inode := uint64(42)
	derivDir, err := derivDirFor(dir, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer derivDir.Close()
	if err := derivatives.WriteFileAt(derivDir, "proxy.mp4", []byte("old-final"), 0o644); err != nil {
		t.Fatal(err)
	}
	receipt := proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode,
		SourceHash: "0011223344556677", SourceSize: 100,
		BlobHash: "8899aabbccddeeff", BlobSize: int64(len("new-final")),
		Codec: "h264", CodecString: "avc1.640028", WrittenAt: 1,
	}
	if err := writeProxyCommitReceipt(derivDir, receipt); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "source.mov")
	if err := os.WriteFile(source, make([]byte, receipt.SourceSize), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	store, err := derivatives.Open(filepath.Join(dir, "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	found, err := recoverProxyCommitReceipt(store, fi, inode, receipt.SourceHash, Options{Mount: dir})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("receipt for different bytes vouched for the old final blob")
	}
}
