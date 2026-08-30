package farm

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func seedFreshProxy(t *testing.T, codec string) (*derivatives.Store, string, uint64, string, int64) {
	t.Helper()
	mount := t.TempDir()
	source := filepath.Join(mount, "source.mp4")
	if err := os.WriteFile(source, []byte("stable source bytes for proxy freshness"), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("source stat has no syscall.Stat_t")
	}
	inode := uint64(st.Ino)
	hash, err := SampleHash(source, info.Size())
	if err != nil {
		t.Fatal(err)
	}

	store := freshStore(t)
	if err := store.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	rel := "proxy.mp4"
	mediaType := "video/mp4"
	size := info.Size()
	proxyBytes := []byte("proxy bytes")
	blobSize := int64(len(proxyBytes))
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mediaType, Codec: &codec,
		SourceSize: &size, BlobSize: &blobSize,
	}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(mount, ".juicemount", "derivatives", fmt.Sprint(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, rel), proxyBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	return store, mount, inode, hash, size
}

func fakeFFprobeCodec(t *testing.T, codec string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "ffprobe")
	if err := os.WriteFile(p, []byte("#!/bin/sh\nprintf '%s\\n' "+codec+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProxyFreshRequiresMatchingCodecAndBlob(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "hevc")
	hevc := Options{Mount: mount, ProxyVCodec: "hevc_vaapi"}
	if !proxyFresh(store, inode, hash, size, hevc) {
		t.Fatal("matching HEVC source row and blob should skip")
	}

	if proxyFresh(store, inode, hash, size, Options{Mount: mount, ProxyVCodec: "libx264"}) {
		t.Fatal("ordinary H.264 request must retain exact-codec semantics")
	}
	if !proxyFresh(store, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "libx264", PreserveHEVCOnFallback: true,
		FFprobeBin: fakeFFprobeCodec(t, "hevc"),
	}) {
		t.Fatal("CPU fallback must preserve a current, better HEVC result")
	}
	if proxyFresh(store, inode, hash, size, Options{Mount: mount, ProxyVCodec: "hevc_vaapi", RegenerateFresh: true}) {
		t.Fatal("explicit regeneration must bypass the proxy freshness gate")
	}
	if proxyFresh(store, inode, "edited-source", size, hevc) {
		t.Fatal("changed source hash must regenerate")
	}
	if proxyFresh(store, inode, hash, size+1, hevc) {
		t.Fatal("changed source size must regenerate")
	}

	if err := os.Remove(filepath.Join(mount, derivatives.DerivBlobRel(inode, "proxy.mp4"))); err != nil {
		t.Fatal(err)
	}
	if proxyFresh(store, inode, hash, size, hevc) {
		t.Fatal("a ready database row must not skip when the proxy bytes are missing")
	}
}

func TestProxyFreshFallbackVerifiesSharedBlobCodec(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "hevc")
	if proxyFresh(store, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "libx264", PreserveHEVCOnFallback: true,
		FFprobeBin: fakeFFprobeCodec(t, "h264"),
	}) {
		t.Fatal("a sidecar HEVC label must not preserve bytes that probe as H.264")
	}
}

func TestProxyFreshReconcilesGPUProxyAcrossWorkerStores(t *testing.T) {
	gpu, mount, inode, hash, size := seedFreshProxy(t, "hevc")
	if err := WriteManifestSidecar(gpu, mount, inode); err != nil {
		t.Fatal(err)
	}

	// A metadata-only NAS worker has its own SQLite cache. Its sidecar write must
	// merge, rather than erase, the GPU's proxy row.
	nas := freshStore(t)
	if err := nas.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	if err := nas.PutDeriv(inode, derivatives.DerivRow{
		Kind: "transcript", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, SourceSize: &size,
	}); err != nil {
		t.Fatal(err)
	}
	if err := WriteManifestSidecar(nas, mount, inode); err != nil {
		t.Fatal(err)
	}

	if proxyFreshIndexed(nas, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "libx264", PreserveHEVCOnFallback: true,
		FFprobeBin: fakeFFprobeCodec(t, "hevc"),
	}) {
		t.Fatal("the NAS private cache unexpectedly knew the GPU row before reconcile")
	}
	if !proxyFresh(nas, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "libx264", PreserveHEVCOnFallback: true,
		FFprobeBin: fakeFFprobeCodec(t, "hevc"),
	}) {
		t.Fatal("CPU fallback did not reconcile and preserve the shared GPU HEVC proxy")
	}
}

func TestSidecarMergeUsesSharedBlobOverClockSkew(t *testing.T) {
	gpu, mount, inode, hash, size := seedFreshProxy(t, "hevc")
	if err := WriteManifestSidecar(gpu, mount, inode); err != nil {
		t.Fatal(err)
	}

	nas := freshStore(t)
	if err := nas.PutSource(inode, &hash); err != nil {
		t.Fatal(err)
	}
	rel, mediaType, h264 := "proxy.mp4", "video/mp4", "h264"
	wrongSize := int64(99999)
	if err := nas.PutDeriv(inode, derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mediaType, Codec: &h264,
		SourceSize: &size, BlobSize: &wrongSize, UpdatedAt: nowUnix() + 60,
	}); err != nil {
		t.Fatal(err)
	}
	if err := WriteManifestSidecar(nas, mount, inode); err != nil {
		t.Fatal(err)
	}

	consumer := freshStore(t)
	if found, err := ReconcileOneSidecar(consumer, mount, inode); err != nil || !found {
		t.Fatalf("reconcile merged sidecar: found=%v err=%v", found, err)
	}
	rows, err := consumer.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Kind == "proxy" {
			if row.Codec == nil || *row.Codec != "hevc" {
				t.Fatalf("newer stale H.264 cache row beat the HEVC bytes: %+v", row)
			}
			return
		}
	}
	t.Fatal("merged sidecar lost the proxy row")
}

func TestProxyFreshDoesNotTreatH264AsHEVC(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "h264")
	if proxyFresh(store, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "hevc_vaapi", PreserveHEVCOnFallback: true,
	}) {
		t.Fatal("an explicit HEVC run must retain exact-codec semantics")
	}
}

func TestProxyFreshQueuePreservesCurrentH264InsteadOfPromoting(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "h264")
	if !proxyFresh(store, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "hevc_vaapi", PreserveExistingProxy: true,
		FFprobeBin: fakeFFprobeCodec(t, "h264"),
	}) {
		t.Fatal("automatic HEVC sweep must preserve a current, proven H.264 proxy")
	}
	if proxyFresh(store, inode, hash, size, Options{
		Mount: mount, ProxyVCodec: "hevc_vaapi", PreserveExistingProxy: true,
		FFprobeBin: fakeFFprobeCodec(t, "hevc"),
	}) {
		t.Fatal("cross-codec preservation trusted a row whose bytes probe differently")
	}
}

func TestGenerateProxySkipsBeforeProbeWhenCurrent(t *testing.T) {
	store, mount, inode, _, _ := seedFreshProxy(t, "hevc")
	source := filepath.Join(mount, "source.mp4")
	res := GenerateProxy(store, source, Options{
		Mount: mount, ProxyVCodec: "hevc_vaapi", FFprobeBin: "/definitely/not/ffprobe",
	})
	if res.Err != nil {
		t.Fatalf("current proxy reached ffprobe instead of skipping: %v", res.Err)
	}
	if res.Inode != inode || !res.SkippedFresh || res.Wrote {
		t.Fatalf("skip result = %+v, want inode=%d skipped=true wrote=false", res, inode)
	}
}

func TestGenerateProxySkipsAudioWithAttachedCoverArt(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "audio-with-cover-art")
	if err := os.WriteFile(source, []byte("representative audio bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(dir, "ffprobe")
	probeScript := `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video","codec_name":"mjpeg","width":1200,"height":1200,"disposition":{"attached_pic":1}},{"codec_type":"audio","codec_name":"mp3","channels":2,"sample_rate":"44100"}],"format":{"format_name":"mp3","duration":"180","size":"26"}}'
`
	if err := os.WriteFile(probe, []byte(probeScript), 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "ffmpeg-invoked")
	ffmpeg := filepath.Join(dir, "ffmpeg")
	if err := os.WriteFile(ffmpeg, []byte("#!/bin/sh\nprintf invoked > '"+marker+"'\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	res := GenerateProxy(nil, source, Options{
		Mount: dir, FFprobeBin: probe, FFmpegBin: ffmpeg,
	})
	if res.Err != nil || res.Wrote || res.SkippedFresh {
		t.Fatalf("cover-art audio proxy result = %+v, want clean no-op", res)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("cover-art audio reached ffmpeg; marker stat error = %v", err)
	}
}
