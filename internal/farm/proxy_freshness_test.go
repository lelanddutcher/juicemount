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
	if err := store.PutDeriv(inode, derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mediaType, Codec: &codec,
		SourceSize: &size,
	}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(mount, ".juicemount", "derivatives", fmt.Sprint(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, rel), []byte("proxy bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	return store, mount, inode, hash, size
}

func TestProxyFreshRequiresMatchingCodecAndBlob(t *testing.T) {
	store, mount, inode, hash, size := seedFreshProxy(t, "hevc")
	hevc := Options{Mount: mount, ProxyVCodec: "hevc_vaapi"}
	if !proxyFresh(store, inode, hash, size, hevc) {
		t.Fatal("matching HEVC source row and blob should skip")
	}

	if proxyFresh(store, inode, hash, size, Options{Mount: mount, ProxyVCodec: "libx264"}) {
		t.Fatal("HEVC row must not satisfy an H.264 request")
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
