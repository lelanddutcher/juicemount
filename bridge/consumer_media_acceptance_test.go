//go:build consumer_acceptance

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

func consumerMediaSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func consumerMediaPinned(t *testing.T, name string) string {
	t.Helper()
	path, want := os.Getenv(name), os.Getenv(name+"_SHA256")
	if !filepath.IsAbs(path) || len(want) != 64 {
		t.Fatalf("%s requires absolute pinned path and SHA-256", name)
	}
	for _, r := range want {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("%s SHA-256 must be lowercase hex", name)
		}
	}
	got, err := consumerMediaSHA(path)
	if err != nil || got != want {
		t.Fatalf("%s pinned SHA-256 got=%s want=%s err=%v", name, got, want, err)
	}
	return path
}

func consumerMediaCopy(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

type consumerMediaLog struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	overflow bool
}

func (l *consumerMediaLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := len(p)
	const maxLog = 64 << 10
	if available := maxLog - l.buf.Len(); available < n {
		l.overflow = true
		if available > 0 {
			_, _ = l.buf.Write(p[:available])
		}
		return n, nil
	}
	_, _ = l.buf.Write(p)
	return n, nil
}

func (l *consumerMediaLog) Snapshot() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String(), l.overflow
}

func consumerMediaChild(t *testing.T, probe, base, mode, fixture string) {
	t.Helper()
	cmd := exec.Command(probe, base, mode)
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append(os.Environ(), "JM_CONSUMER_MEDIA_FIXTURE="+fixture)
	var output consumerMediaLog
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	forced := false
	timedOut := false
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		timedOut = true
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case err = <-done:
		case <-time.After(3 * time.Second):
			forced = true
			_ = cmd.Process.Kill()
			err = <-done
		}
	}
	log, overflow := output.Snapshot()
	digest := sha256.Sum256([]byte(log))
	t.Logf("consumer child mode=%s pid=%d original_wait=%v timed_out=%v forced_kill=%v log_overflow=%v log_bytes=%d log_sha256=%x\n%s", mode, cmd.Process.Pid, err, timedOut, forced, overflow, len(log), digest, log)
	if err != nil || timedOut || forced || overflow || !strings.Contains(log, "PASS "+mode+":") {
		t.Fatalf("consumer %s failed: original_wait=%v timed_out=%v forced_kill=%v log_overflow=%v", mode, err, timedOut, forced, overflow)
	}
}

func TestConsumerProxyMediaProductionClient(t *testing.T) {
	probe := consumerMediaPinned(t, "JM_CONSUMER_PROBE")
	sourceFixture := consumerMediaPinned(t, "JM_CONSUMER_MEDIA_SOURCE")
	ffmpeg := consumerMediaPinned(t, "JM_CONSUMER_FFMPEG")
	ffprobe := consumerMediaPinned(t, "JM_CONSUMER_FFPROBE")
	fixtureInfo, err := os.Stat(sourceFixture)
	if err != nil || !fixtureInfo.Mode().IsRegular() || fixtureInfo.Size() != 223153 {
		t.Fatalf("source fixture must be the retained 223153-byte real media: info=%v err=%v", fixtureInfo, err)
	}
	restore := seedControlPlane(t)
	defer restore()
	globalMu.Lock()
	oldSpool, oldDrainer, oldRC, oldPrefetcher, oldThumb := globalSpool, globalDrainer, globalRC, globalPrefetcher, globalThumbCache
	oldAtomic := globalDrainerAtomic.Load()
	globalSpool, globalDrainer, globalRC, globalPrefetcher, globalThumbCache = nil, nil, nil, nil, nil
	globalDrainerAtomic.Store(nil)
	fuseRoot, mirror, provider := globalFUSEPath, globalStore, globalDerivStore
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalSpool, globalDrainer, globalRC, globalPrefetcher, globalThumbCache = oldSpool, oldDrainer, oldRC, oldPrefetcher, oldThumb
		globalDrainerAtomic.Store(oldAtomic)
		globalMu.Unlock()
	}()
	if mirror == nil || provider == nil || fuseRoot == "" {
		t.Fatal("seeded control plane absent")
	}
	project := filepath.Join(fuseRoot, "Project_Foo")
	if err := os.MkdirAll(project, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(project, "consumer-media.mov")
	consumerMediaCopy(t, sourceFixture, source)
	sourceSHA, err := consumerMediaSHA(source)
	if err != nil || sourceSHA != os.Getenv("JM_CONSUMER_MEDIA_SOURCE_SHA256") {
		t.Fatalf("source copy changed bytes: %s %v", sourceSHA, err)
	}
	fi, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("fixture source has no filesystem inode")
	}
	inode := uint64(stat.Ino)
	sourceHash, err := farm.SampleHash(source, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	displayPath := "/Volumes/zpool/Project_Foo/consumer-media.mov"
	if err := mirror.Insert(&metadata.Entry{Path: "Project_Foo/consumer-media.mov", Name: "consumer-media.mov", ParentPath: "Project_Foo",
		Size: fi.Size(), Mtime: fi.ModTime(), MtimeNsecKnown: true, Inode: inode, Mode: fi.Mode()}); err != nil {
		t.Fatal(err)
	}
	if known, _ := provider.Known(inode); known {
		t.Fatal("provider derivative store was preseeded for media inode")
	}
	farmRoot := t.TempDir()
	scratch := filepath.Join(farmRoot, "scratch")
	if err := os.Mkdir(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	farmStore, err := derivatives.Open(filepath.Join(farmRoot, "farm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer farmStore.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	result := farm.GenerateProxy(farmStore, source, farm.Options{Context: ctx, Mount: fuseRoot, EncodeScratchDir: scratch,
		FFmpegBin: ffmpeg, FFprobeBin: ffprobe, ProxyVCodec: "libx264", Producer: "linux-farm", Version: 1})
	if result.Err != nil || !result.Wrote || result.SkippedFresh || result.Inode != inode || result.Hash != sourceHash {
		t.Fatalf("actual farm encode failed: %+v", result)
	}
	if known, _ := provider.Known(inode); known {
		t.Fatal("farm SQLite bypassed provider on-miss reconcile")
	}
	proxyPath := filepath.Join(fuseRoot, derivatives.DerivBlobRel(inode, "proxy.mp4"))
	proxyInfo, err := os.Stat(proxyPath)
	if err != nil || proxyInfo.Size() <= 0 {
		t.Fatalf("proxy stat=%v err=%v", proxyInfo, err)
	}
	proxySHA, err := consumerMediaSHA(proxyPath)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := farmStore.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	const exactToken = "avc1.64000c, mp4a.40.2"
	ready := false
	for _, row := range rows {
		if row.Kind == "proxy" && row.Status == "ready" && row.Hash != nil && *row.Hash == sourceHash &&
			row.Codec != nil && *row.Codec == "h264" && row.CodecString != nil && *row.CodecString == exactToken &&
			row.BlobSize != nil && *row.BlobSize == proxyInfo.Size() {
			ready = true
		}
	}
	if !ready {
		t.Fatalf("farm did not publish byte-bound exact codec metadata: %+v", rows)
	}
	desc := struct {
		Inode       uint64 `json:"inode"`
		SourceHash  string `json:"sourceHash"`
		SourceSize  int64  `json:"sourceSize"`
		SourceMtime int64  `json:"sourceMtime"`
		DisplayPath string `json:"displayPath"`
		Mount       string `json:"mount"`
		ProxySHA256 string `json:"proxySHA256"`
		ProxySize   int64  `json:"proxySize"`
		CodecString string `json:"codecString"`
	}{inode, sourceHash, fi.Size(), fi.ModTime().Unix(), displayPath, fuseRoot, proxySHA, proxyInfo.Size(), exactToken}
	fixtureJSON, err := json.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(farmRoot, "consumer-fixture.json")
	f, err := os.OpenFile(fixturePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write(fixtureJSON); err != nil || n != len(fixtureJSON) {
		f.Close()
		t.Fatalf("descriptor write %d/%d err=%v", n, len(fixtureJSON), err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var blobRequests []string
	s := metrics.NewServer("127.0.0.1:0", metrics.NewRegistry())
	s.ExtraRoutes = map[string]http.HandlerFunc{
		"/whoami": handleWhoamiHTTP, "/lookup": handleLookupHTTP,
		"/derivatives": handleDerivativesHTTP, "/derivatives/changes": handleDerivativesChangesHTTP,
		"/blob": func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			blobRequests = append(blobRequests, fmt.Sprintf("%s %s", r.Method, r.Header.Get("Range")))
			mu.Unlock()
			handleBlobHTTP(w, r)
		},
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	base := "http://" + s.Addr()
	consumerMediaChild(t, probe, base, "proxy-media", fixturePath)
	mu.Lock()
	beforeRanges := len(blobRequests)
	mu.Unlock()
	if beforeRanges != 0 {
		t.Fatalf("client.blob(proxy) issued %d HTTP blob requests", beforeRanges)
	}
	if known, hash := provider.Known(inode); !known || hash == nil || *hash != sourceHash {
		t.Fatal("production /derivatives did not reconcile sidecar on miss")
	}
	consumerMediaChild(t, probe, base, "proxy-ranges", fixturePath)
	mu.Lock()
	requests := append([]string(nil), blobRequests...)
	mu.Unlock()
	if len(requests) != 5 {
		t.Fatalf("raw range/full transport issued %d /blob requests, want 5: %v", len(requests), requests)
	}
	t.Logf("actual consumer media inode=%d source_sha256=%s blob_sha256=%s blob_bytes=%d raw_blob_requests=%v", inode, sourceSHA, proxySHA, proxyInfo.Size(), requests)
}
