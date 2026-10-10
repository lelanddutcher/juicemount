package farm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func proxyMetadataFakeProbe(t *testing.T, codec string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ffprobe")
	script := fmt.Sprintf(`#!/bin/sh
case "$*" in
  *"-show_data"*) cat <<'OUTPUT'
%s
OUTPUT
    ;;
  *"/dev/fd/3"*) echo %s ;;
  *) echo '{"streams":[{"codec_type":"video","codec_name":"h264","width":320,"height":180,"r_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"1","size":"39"}}' ;;
esac
`, proxyOutputProbeFixture(codec), codec)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func proxyMetadataSentinelEncoder(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	bin, marker := filepath.Join(dir, "ffmpeg"), filepath.Join(dir, "ffmpeg-invoked")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf invoked > '"+marker+"'\nexit 89\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

func proxyMetadataAssertNoEncode(t *testing.T, marker string) {
	t.Helper()
	if _, err := os.Lstat(marker); !os.IsNotExist(err) {
		t.Fatalf("metadata repair attempted encode: marker stat=%v", err)
	}
}

func proxyMetadataRow(t *testing.T, store *derivatives.Store, inode uint64) derivatives.DerivRow {
	t.Helper()
	rows, err := store.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == "proxy" {
			return row
		}
	}
	t.Fatal("proxy row missing")
	return derivatives.DerivRow{}
}

func TestProxyFreshRepairsOldTokenAndFutureSidecarWithoutEncoding(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "h264")
	defer store.Close()
	source := filepath.Join(mount, "source.mp4")
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	blobPath := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	blobInfo, err := os.Stat(blobPath)
	if err != nil {
		t.Fatal(err)
	}
	blobHash, err := SampleHash(blobPath, blobInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	old := "avc1.640028"
	oldFuture := time.Now().Unix() + 60
	row := proxyMetadataRow(t, store, inode)
	row.CodecString = &old
	row.UpdatedAt = oldFuture
	if err := store.PutDeriv(inode, row); err != nil {
		t.Fatal(err)
	}
	if err := WriteManifestSidecar(store, mount, inode); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(mount, derivatives.DerivBlobRel(inode, "manifest.json"))
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var sc ManifestSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	for i := range sc.Derivatives {
		if sc.Derivatives[i].Kind == "proxy" {
			sc.Derivatives[i].CodecString = &old
			sc.Derivatives[i].UpdatedAt = oldFuture + 30
		}
	}
	raw, err = json.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeProxyCommitReceipt(dir, proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode, SourceHash: hash, SourceSize: size,
		BlobHash: blobHash, BlobSize: blobInfo.Size(), Codec: "h264", CodecString: old,
	}); err != nil {
		t.Fatal(err)
	}
	opt := Options{Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataFakeProbe(t, "h264")}
	res := GenerateProxy(store, source, opt)
	if res.Err != nil || !res.SkippedFresh || res.Wrote {
		t.Fatalf("fresh repair=%+v", res)
	}
	want := "avc1.64000c"
	got := proxyMetadataRow(t, store, inode)
	if got.UpdatedAt <= oldFuture {
		t.Fatal("metadata repair did not advance change timestamp")
	}
	changes, err := store.ListChangedSince(oldFuture, 100)
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, change := range changes {
		if change.Inode == inode && change.Kind == "proxy" && change.Status == "ready" && change.UpdatedAt == got.UpdatedAt {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("repaired proxy absent from changes feed after cursor %d: %+v", oldFuture, changes)
	}
	if got.CodecString == nil || *got.CodecString != want {
		t.Fatalf("repaired row=%+v want token %s", got, want)
	}
	repeated := GenerateProxy(store, source, opt)
	if repeated.Err != nil || !repeated.SkippedFresh || repeated.Wrote {
		t.Fatalf("identical repair retry=%+v", repeated)
	}
	if unchanged := proxyMetadataRow(t, store, inode); unchanged.UpdatedAt != got.UpdatedAt {
		t.Fatalf("identical metadata advanced changes-feed cursor: first=%d second=%d", got.UpdatedAt, unchanged.UpdatedAt)
	}
	receipt, err := readProxyCommitReceipt(dir)
	if err != nil || receipt.CodecString != old {
		t.Fatalf("repaired receipt=%+v err=%v", receipt, err)
	}
	raw, err = os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &sc); err != nil {
		t.Fatal(err)
	}
	for _, row := range sc.Derivatives {
		if row.Kind == "proxy" {
			if row.CodecString == nil || *row.CodecString != want {
				t.Fatalf("future-dated sidecar restored wrong token: %+v", row)
			}
			return
		}
	}
	t.Fatal("repaired sidecar lost proxy row")
}

func TestProxyFreshRowOnlyRepairDoesNotCreateReceipt(t *testing.T) {
	store, mount, inode, _, _ := seedFreshProxy(t, "hevc")
	defer store.Close()
	res := GenerateProxy(store, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", PreserveExistingProxy: true,
		FFprobeBin: proxyMetadataFakeProbe(t, "hevc"),
	})
	if res.Err != nil || !res.SkippedFresh || res.Wrote {
		t.Fatalf("preserved HEVC repair=%+v", res)
	}
	row := proxyMetadataRow(t, store, inode)
	if row.Codec == nil || *row.Codec != "hevc" || row.CodecString == nil || *row.CodecString == "avc1.640028" {
		t.Fatalf("HEVC bytes were downgraded: %+v", row)
	}
	if _, err := os.Lstat(filepath.Join(mount, derivatives.DerivBlobRel(inode, proxyReceiptName))); !os.IsNotExist(err) {
		t.Fatalf("row-only repair manufactured receipt: %v", err)
	}
}

func TestProxyReceiptRecoveryRepairsOldTokenWithoutEncoding(t *testing.T) {
	seed, mount, inode, hash, size := seedFreshProxy(t, "h264")
	defer seed.Close()
	blobPath := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	bi, err := os.Stat(blobPath)
	if err != nil {
		t.Fatal(err)
	}
	blobHash, err := SampleHash(blobPath, bi.Size())
	if err != nil {
		t.Fatal(err)
	}
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := writeProxyCommitReceipt(dir, proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode, SourceHash: hash, SourceSize: size,
		BlobHash: blobHash, BlobSize: bi.Size(), Codec: "h264", CodecString: "avc1.640028",
	}); err != nil {
		t.Fatal(err)
	}
	fresh := freshStore(t)
	res := GenerateProxy(fresh, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataFakeProbe(t, "h264"),
	})
	if res.Err != nil || !res.SkippedFresh || res.Wrote {
		t.Fatalf("receipt recovery=%+v", res)
	}
	row := proxyMetadataRow(t, fresh, inode)
	if row.CodecString == nil || *row.CodecString != "avc1.64000c" {
		t.Fatalf("recovered token=%+v", row)
	}
	receipt, err := readProxyCommitReceipt(dir)
	if err != nil || receipt.CodecString != "avc1.640028" {
		t.Fatalf("recovered receipt=%+v err=%v", receipt, err)
	}
}

func proxyMetadataActionProbe(t *testing.T, action string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ffprobe")
	script := fmt.Sprintf(`#!/bin/sh
case "$*" in
  *"-show_data"*) %s ;;
  *"/dev/fd/3"*) echo h264 ;;
  *) echo '{"streams":[{"codec_type":"video","codec_name":"h264","width":320,"height":180,"r_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"1","size":"39"}}' ;;
esac
`, action)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestProxyFreshMetadataProbeFailureKeepsBytesAndDoesNotEncode(t *testing.T) {
	store, mount, inode, _, _ := seedFreshProxy(t, "h264")
	defer store.Close()
	blob := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	before, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	rowBefore := proxyMetadataRow(t, store, inode)
	encoder, marker := proxyMetadataSentinelEncoder(t)
	res := GenerateProxy(store, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataActionProbe(t, "exit 92"), FFmpegBin: encoder,
	})
	if res.Err == nil || res.Wrote || res.SkippedFresh {
		t.Fatalf("qualified fresh probe failure=%+v", res)
	}
	proxyMetadataAssertNoEncode(t, marker)
	after, err := os.ReadFile(blob)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("fresh probe failure changed blob: %v", err)
	}
	rowAfter := proxyMetadataRow(t, store, inode)
	if rowAfter.CodecString != nil || rowAfter.UpdatedAt != rowBefore.UpdatedAt {
		t.Fatalf("failed metadata probe published row: before=%+v after=%+v", rowBefore, rowAfter)
	}
}

func TestProxyReceiptBoundProbeFailureKeepsBytesAndDoesNotEncode(t *testing.T) {
	seed, mount, inode, hash, size := seedFreshProxy(t, "h264")
	defer seed.Close()
	blob := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	before, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	blobHash, err := SampleHash(blob, int64(len(before)))
	if err != nil {
		t.Fatal(err)
	}
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := writeProxyCommitReceipt(dir, proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode, SourceHash: hash, SourceSize: size,
		BlobHash: blobHash, BlobSize: int64(len(before)), Codec: "h264", CodecString: "avc1.640028",
	}); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(mount, derivatives.DerivBlobRel(inode, proxyReceiptName))
	receiptBefore, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	fresh := freshStore(t) // no ready row: recovery must reach the bound receipt
	encoder, marker := proxyMetadataSentinelEncoder(t)
	res := GenerateProxy(fresh, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataActionProbe(t, "exit 92"), FFmpegBin: encoder,
	})
	if res.Err == nil || res.Wrote || res.SkippedFresh {
		t.Fatalf("bound receipt probe failure=%+v", res)
	}
	proxyMetadataAssertNoEncode(t, marker)
	after, err := os.ReadFile(blob)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("receipt probe failure changed blob: %v", err)
	}
	receiptAfter, err := os.ReadFile(receiptPath)
	if err != nil || !bytes.Equal(receiptBefore, receiptAfter) {
		t.Fatalf("receipt probe failure rewrote receipt: %v", err)
	}
	rows, err := fresh.Manifest(inode)
	if err != nil || len(rows) != 0 {
		t.Fatalf("receipt probe failure published rows=%+v err=%v", rows, err)
	}
}

func TestProxyFreshRejectsNamedBlobReplacementDuringProbe(t *testing.T) {
	store, mount, inode, _, _ := seedFreshProxy(t, "h264")
	defer store.Close()
	blob := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	rowBefore := proxyMetadataRow(t, store, inode)
	encoder, marker := proxyMetadataSentinelEncoder(t)
	action := fmt.Sprintf("printf replacement > %q; mv %q %q; cat <<'OUTPUT'\n%s\nOUTPUT\n", blob+".swap", blob+".swap", blob, proxyOutputProbeFixture("h264"))
	res := GenerateProxy(store, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataActionProbe(t, action), FFmpegBin: encoder,
	})
	if res.Err == nil || res.Wrote || res.SkippedFresh {
		t.Fatalf("named blob replacement was accepted: %+v", res)
	}
	proxyMetadataAssertNoEncode(t, marker)
	if current, err := os.ReadFile(blob); err != nil || string(current) != "replacement" {
		t.Fatalf("replacement fixture did not run: bytes=%q err=%v", current, err)
	}
	rowAfter := proxyMetadataRow(t, store, inode)
	if rowAfter.CodecString != nil || rowAfter.UpdatedAt != rowBefore.UpdatedAt {
		t.Fatalf("replaced name received metadata: %+v", rowAfter)
	}
}

func TestProxyFreshPreservesMismatchedHistoricalReceipt(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "h264")
	defer store.Close()
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := writeProxyCommitReceipt(dir, proxyCommitReceipt{
		Version: proxyReceiptVersion, Inode: inode, SourceHash: hash, SourceSize: size,
		BlobHash: "0011223344556677", BlobSize: 999, Codec: "h264", CodecString: "avc1.640028",
	}); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(mount, derivatives.DerivBlobRel(inode, proxyReceiptName))
	before, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	encoder, marker := proxyMetadataSentinelEncoder(t)
	res := GenerateProxy(store, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataFakeProbe(t, "h264"), FFmpegBin: encoder,
	})
	if res.Err != nil || !res.SkippedFresh || res.Wrote {
		t.Fatalf("row-only repair with historical receipt=%+v", res)
	}
	proxyMetadataAssertNoEncode(t, marker)
	after, err := os.ReadFile(receiptPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("historical receipt was rewritten: %v", err)
	}
	row := proxyMetadataRow(t, store, inode)
	if row.CodecString == nil || *row.CodecString != "avc1.64000c" {
		t.Fatalf("actual blob metadata not repaired: %+v", row)
	}
}

func TestProxyFreshRefusesUnadvanceableMetadataCursor(t *testing.T) {
	store, mount, inode, _, _ := seedFreshProxy(t, "h264")
	defer store.Close()
	const maxInt64 = int64(1<<63 - 1)
	old := "avc1.640028"
	row := proxyMetadataRow(t, store, inode)
	row.CodecString, row.UpdatedAt = &old, maxInt64
	if err := store.PutDeriv(inode, row); err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	before, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	encoder, marker := proxyMetadataSentinelEncoder(t)
	res := GenerateProxy(store, filepath.Join(mount, "source.mp4"), Options{
		Mount: mount, ProxyVCodec: "libx264", FFprobeBin: proxyMetadataFakeProbe(t, "h264"), FFmpegBin: encoder,
	})
	if res.Err == nil || res.Wrote || res.SkippedFresh {
		t.Fatalf("unadvanceable metadata cursor=%+v", res)
	}
	proxyMetadataAssertNoEncode(t, marker)
	after, err := os.ReadFile(blob)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("cursor failure changed blob: %v", err)
	}
	unchanged := proxyMetadataRow(t, store, inode)
	if unchanged.CodecString == nil || *unchanged.CodecString != old || unchanged.UpdatedAt != maxInt64 {
		t.Fatalf("cursor failure published metadata: %+v", unchanged)
	}
}
