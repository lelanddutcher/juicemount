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

func TestGenerateProxyEncodesOnLocalScratchThenPublishesOnce(t *testing.T) {
	root := t.TempDir()
	mount := filepath.Join(root, "mount")
	scratch := filepath.Join(root, "scratch")
	tools := filepath.Join(root, "tools")
	for _, dir := range []string{mount, scratch, tools} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(mount, "source.mov")
	if err := os.WriteFile(source, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(tools, "ffprobe")
	probeScript := `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video","codec_name":"prores","width":1920,"height":1080,"r_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"10","size":"65536"}}'
`
	if err := os.WriteFile(probe, []byte(probeScript), 0o755); err != nil {
		t.Fatal(err)
	}
	ffmpeg := filepath.Join(tools, "ffmpeg")
	ffmpegScript := `#!/bin/sh
for last do :; done
printf '%s' "$last" > "$(dirname "$0")/ffmpeg-output-path"
printf 'first-' > "$last"
printf 'rewrite-' >> "$last"
printf 'complete' >> "$last"
`
	if err := os.WriteFile(ffmpeg, []byte(ffmpegScript), 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := derivatives.Open(filepath.Join(root, "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	res := GenerateProxy(store, source, Options{
		Mount: mount, EncodeScratchDir: scratch,
		FFprobeBin: probe, FFmpegBin: ffmpeg,
		ProxyVCodec: "libx264", Producer: "linux-farm", Version: 1,
	})
	if res.Err != nil || !res.Wrote || res.SkippedFresh {
		t.Fatalf("proxy result = %+v", res)
	}
	encodedAt, err := os.ReadFile(filepath.Join(tools, "ffmpeg-output-path"))
	if err != nil {
		t.Fatal(err)
	}
	resolvedScratch, err := filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(encodedAt), resolvedScratch+string(os.PathSeparator)) {
		t.Fatalf("ffmpeg output %q was not node-local scratch %q", encodedAt, scratch)
	}
	final, err := os.ReadFile(filepath.Join(mount, derivatives.DerivBlobRel(res.Inode, "proxy.mp4")))
	if err != nil {
		t.Fatal(err)
	}
	if string(final) != "first-rewrite-complete" {
		t.Fatalf("published proxy = %q", final)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("local scratch leaked files: %v", entries)
	}
}

func TestCreateEncodeScratchRejectsSharedMount(t *testing.T) {
	mount := t.TempDir()
	scratch := filepath.Join(mount, "scratch")
	if err := os.Mkdir(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	if f, err := createEncodeScratch(Options{Mount: mount, EncodeScratchDir: scratch}, "proxy"); err == nil {
		f.Close()
		os.Remove(f.Name())
		t.Fatal("accepted proxy scratch inside shared mount")
	}
}
