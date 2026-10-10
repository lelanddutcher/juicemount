//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

type fallbackMediaLog struct {
	mu       sync.Mutex
	text     strings.Builder
	overflow bool
}

func (b *fallbackMediaLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	const limit = 128 << 10
	if room := limit - b.text.Len(); room < n {
		b.overflow = true
		if room > 0 {
			_, _ = b.text.Write(p[:room])
		}
	} else {
		_, _ = b.text.Write(p)
	}
	return n, nil
}

func (b *fallbackMediaLog) snapshot() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.String(), b.overflow
}

type fallbackMediaProcess struct {
	cmd    *exec.Cmd
	done   chan error
	log    fallbackMediaLog
	joined bool
	root   string
	role   string
	retain *bool
}

func (p *fallbackMediaProcess) join(t *testing.T) {
	t.Helper()
	if p.joined {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Errorf("signal owned worker pid=%d: %v", p.cmd.Process.Pid, err)
	}
	var waitErr error
	forced := false
	select {
	case waitErr = <-p.done:
	case <-time.After(10 * time.Second):
		forced = true
		_ = p.cmd.Process.Kill()
		waitErr = <-p.done
	}
	p.joined = true
	log, overflow := p.log.snapshot()
	h := sha256.Sum256([]byte(log))
	t.Logf("farm worker pid=%d original_wait=%v forced=%v log_overflow=%v log_bytes=%d log_sha256=%x\n%s", p.cmd.Process.Pid, waitErr, forced, overflow, len(log), h, log)
	if forced || waitErr != nil || !strings.Contains(log, "QUIESCENT fallback-"+p.role+"\n") {
		*p.retain = true
		t.Errorf("INCOMPLETE_MEDIA_CUSTODY: worker pid=%d may have unjoined media; retain root=%s", p.cmd.Process.Pid, p.root)
	}
	if waitErr != nil || overflow {
		t.Errorf("worker pid=%d did not join cleanly: wait=%v overflow=%v", p.cmd.Process.Pid, waitErr, overflow)
	}
}

func fallbackMediaStart(t *testing.T, role, root, toolDir string, retain *bool) *fallbackMediaProcess {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &fallbackMediaProcess{done: make(chan error, 1), root: root, role: role, retain: retain}
	p.cmd = exec.Command(self, "-test.run=^TestRunQueueRenderFailureToCPUMedia$", "-test.timeout=150s")
	p.cmd.WaitDelay = 2 * time.Second
	p.cmd.Env = append(os.Environ(), "JM_FARM_FALLBACK_CHILD="+role, "JM_FARM_FALLBACK_ROOT="+root,
		"JM_FARM_WATCH=0", "PATH="+toolDir+string(os.PathListSeparator)+"/usr/bin:/bin")
	p.cmd.Stdout, p.cmd.Stderr = &p.log, &p.log
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { p.join(t) })
	return p
}

func fallbackMediaWorkerID(t *testing.T, ctx context.Context, rdb *redis.Client, name string) string {
	t.Helper()
	ids, err := rdb.SMembers(ctx, farmqueue.WorkerIndexKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		raw, err := rdb.Get(ctx, farmqueue.WorkerPrefix+id).Bytes()
		if err != nil {
			continue
		}
		var w farmqueue.Worker
		if json.Unmarshal(raw, &w) == nil && w.Name == name {
			return id
		}
	}
	return ""
}

func fallbackMediaCheckProcess(t *testing.T, p *fallbackMediaProcess, phase string) {
	t.Helper()
	select {
	case err := <-p.done:
		p.joined = true
		log, overflow := p.log.snapshot()
		h := sha256.Sum256([]byte(log))
		if err != nil || !strings.Contains(log, "QUIESCENT fallback-"+p.role+"\n") {
			*p.retain = true
			t.Logf("INCOMPLETE_MEDIA_CUSTODY: early worker exit; retain root=%s", p.root)
		}
		t.Fatalf("worker pid=%d exited before %s: original_wait=%v overflow=%v log_bytes=%d log_sha256=%x\n%s", p.cmd.Process.Pid, phase, err, overflow, len(log), h, log)
	default:
	}
}

func TestRunQueueRenderFailureToCPUMedia(t *testing.T) {
	if role := os.Getenv("JM_FARM_FALLBACK_CHILD"); role != "" {
		if role != "render" && role != "cpu" {
			t.Fatalf("unknown worker role %q", role)
		}
		ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
		for name, want := range map[string]string{"ffmpeg": ffmpeg, "ffprobe": ffprobe} {
			got, err := exec.LookPath(name)
			if err != nil || got != want {
				t.Fatalf("child PATH selected %s=%q want=%q err=%v", name, got, want, err)
			}
		}
		root := os.Getenv("JM_FARM_FALLBACK_ROOT")
		if !filepath.IsAbs(root) {
			t.Fatal("child root must be owned absolute path")
		}
		cfg := queueConfig{meta: os.Getenv("JM_FARM_TEST_REDIS_URL"), mount: filepath.Join(root, "mount"),
			dbPath: filepath.Join(root, role+".db"), encodeScratch: filepath.Join(root, "scratch-"+role),
			name: "fallback-media-" + role, producer: "linux-farm", role: farmqueue.QueueClassServer,
			kinds: []string{farmqueue.KindProxy}, vcodec: "libx264", crf: 21, preset: "fast", conc: 1, pConc: 1, version: 1, verbose: true}
		if role == "render" {
			cfg.role = farmqueue.QueueClassRender
			cfg.vcodec = "h264_vaapi"
			cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
				// Test-only admission fixture. Real ffmpeg/ffprobe and failure remain
				// unmodified; this profile is not a real hardware qualification.
				return workerProfile{Role: farmqueue.QueueClassRender, Encoders: []string{"h264_vaapi"}, Decoders: []string{"h264_vaapi"},
					Capabilities: []string{"vaapi", "encoder:h264_vaapi", "decoder:h264_vaapi", "decoder:h264_vaapi:profile:high", "decoder:h264_vaapi:pixfmt:yuv420p"},
					DecodeLimits: map[string]farmqueue.VideoDecodeLimit{"h264_vaapi": {MaxWidth: 4096, MaxHeight: 4096}},
					Benchmarks:   farmqueue.WorkerBenchmarks{EncodeFPS: 120, DecodeFPS: 120, ProbedAt: time.Now().UTC().Format(time.RFC3339)}}, nil
			}
		} else {
			cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
				return workerProfile{Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}, nil
			}
		}
		code := runQueue(cfg)
		fmt.Printf("QUIESCENT fallback-%s\n", role) // runQueue defers have joined its work.
		os.Exit(code)
	}
	if os.Getenv("JM_FARM_FALLBACK_MEDIA_TEST") != "1" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" || os.Getenv("JM_FARM_TEST_REDIS_URL") == "" {
		t.Skip("requires explicit real-media fallback opt-in and isolated flushable Redis")
	}
	ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
	if filepath.Dir(ffmpeg) != filepath.Dir(ffprobe) {
		t.Fatal("pinned ffmpeg and ffprobe must share PATH directory")
	}
	sourceFixture, sourceWant := os.Getenv("JM_FARM_FALLBACK_SOURCE"), os.Getenv("JM_FARM_FALLBACK_SOURCE_SHA256")
	if !filepath.IsAbs(sourceFixture) || len(sourceWant) != 64 || cpuMediaSHA(t, sourceFixture) != sourceWant {
		t.Fatal("retained real source path/hash absent or changed")
	}
	fixtureInfo, err := os.Stat(sourceFixture)
	if err != nil || !fixtureInfo.Mode().IsRegular() || fixtureInfo.Size() != 223153 {
		t.Fatalf("retained 223153-byte source: info=%v err=%v", fixtureInfo, err)
	}
	opt, err := redis.ParseURL(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 130*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	q, err := farmqueue.Open(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	root, err := os.MkdirTemp(os.TempDir(), "farm-fallback-")
	if err != nil {
		t.Fatal(err)
	}
	retain := false
	t.Cleanup(func() {
		// Registered before worker cleanups: every direct worker is joined first.
		if retain {
			t.Logf("INCOMPLETE_MEDIA_CUSTODY: retained fixture %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	mount := filepath.Join(root, "mount")
	for _, dir := range []string{mount, filepath.Join(root, "scratch-render"), filepath.Join(root, "scratch-cpu")} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(mount, "source.mov")
	in, err := os.Open(sourceFixture)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(source, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		in.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	inErr := in.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || inErr != nil {
		t.Fatalf("source copy: %v %v %v %v", copyErr, syncErr, closeErr, inErr)
	}
	if got := cpuMediaSHA(t, source); got != sourceWant {
		t.Fatalf("copied source SHA=%s want=%s", got, sourceWant)
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := sourceInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("source lacks inode stat")
	}
	inode := uint64(st.Ino)
	sourceHash, err := farm.SampleHash(source, sourceInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	sourceProbe := cpuMediaProbeFile(t, ffprobe, source)
	if sourceProbe.Streams[0].CodecType != "video" || sourceProbe.Streams[0].CodecName != "h264" || sourceProbe.Streams[0].PixFmt != "yuv420p" || sourceProbe.Streams[0].ReadFrames != "24" ||
		sourceProbe.Streams[0].Width != 320 || sourceProbe.Streams[0].Height != 180 || sourceProbe.Streams[1].CodecType != "audio" || sourceProbe.Streams[1].CodecName != "pcm_s16le" ||
		sourceProbe.Streams[1].SampleRate != "48000" || sourceProbe.Streams[1].Channels != 2 {
		t.Fatalf("pinned source stream shape changed: %+v", sourceProbe.Streams)
	}
	derivDir := filepath.Join(mount, derivatives.DerivDirRel(inode))
	proxyPath := filepath.Join(derivDir, "proxy.mp4")
	if _, err := os.Lstat(proxyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("proxy preexisted: %v", err)
	}
	for _, db := range []string{"render.db", "cpu.db"} {
		if _, err := os.Lstat(filepath.Join(root, db)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s preexisted: %v", db, err)
		}
	}
	job := farmqueue.NewJob(mount, []string{farmqueue.KindProxy}, "linux-farm")
	job.ID = "fallback-real-media-single"
	job.QueueClass = farmqueue.QueueClassRender
	job.SelectedBackend = "h264_vaapi"
	job.VCodec = "h264_vaapi"
	job.RequiredCapabilities = []string{"encoder:h264_vaapi", "decoder:h264_vaapi", "decoder:h264_vaapi:profile:high", "decoder:h264_vaapi:pixfmt:yuv420p"}
	job.SourceVideoCodec = "h264"
	job.SourceVideoProfile = "high"
	job.SourceBitDepth = 8
	job.SourcePixelFormat = "yuv420p"
	job.SourceVideoWidth = 320
	job.SourceVideoHeight = 180
	job.RetryTargets = []string{source}
	job.ShardIndex = 1
	job.ShardCount = 1
	job.PlanOnly = false
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	refreshPermit := func() {
		t.Helper()
		if err := q.PublishStoragePermit(ctx, farmqueue.StoragePermit{Source: "fallback-media-fixture-not-NAS-proof", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
			t.Fatal(err)
		}
	}
	refreshPermit()
	render := fallbackMediaStart(t, "render", root, filepath.Dir(ffmpeg), &retain)
	var queued map[string]string
	var payload farmqueue.Job
	deadline := time.Now().Add(85 * time.Second)
	nextPermit := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if time.Now().After(nextPermit) {
			refreshPermit()
			nextPermit = time.Now().Add(5 * time.Second)
		}
		fallbackMediaCheckProcess(t, render, "terminal CPU route")
		queued, err = rdb.HGetAll(ctx, farmqueue.JobHashPrefix+job.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		if queued["status"] == farmqueue.StatusFailed || queued["status"] == farmqueue.StatusBlocked {
			t.Fatalf("render terminal failure: %v\n%s", queued, func() string { v, _ := render.log.snapshot(); return v }())
		}
		if queued["status"] == farmqueue.StatusQueued && queued["queue_class"] == farmqueue.QueueClassCPU && queued["hardware_failures"] == "2" {
			if err := json.Unmarshal([]byte(queued["payload"]), &payload); err != nil {
				t.Fatal(err)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if payload.ID == "" {
		v, _ := render.log.snapshot()
		t.Fatalf("two actual render failures did not publish terminal CPU route: status=%v\n%s", queued, v)
	}
	if payload.ID != job.ID || payload.Path != mount || !reflect.DeepEqual(payload.Kinds, []string{farmqueue.KindProxy}) || payload.QueueClass != farmqueue.QueueClassCPU ||
		payload.SelectedBackend != "libx264" || payload.VCodec != "libx264" || !payload.CPUFallbackLocked || payload.SelectedWorker != "" ||
		!reflect.DeepEqual(payload.RequiredCapabilities, []string{"cpu"}) || !reflect.DeepEqual(payload.RetryTargets, []string{source}) ||
		payload.ProcessedOffset != 0 || payload.HardwareFailures != 2 || payload.Attempts != 2 || queued["processed"] != "0" || queued["failed"] != "0" ||
		!strings.Contains(queued["error"], "render backend failed after 1 hardware retries") {
		t.Fatalf("terminal same-ID CPU payload/status mismatch: payload=%+v status=%v", payload, queued)
	}
	if exists, err := rdb.HExists(ctx, farmqueue.JobHashPrefix+job.ID, "cpu_fallback_published").Result(); err != nil || exists {
		t.Fatalf("unexpected subset-publication receipt: exists=%v err=%v", exists, err)
	}
	queueRaw, err := rdb.LRange(ctx, farmqueue.QueueKeyFor(&payload), 0, -1).Result()
	if err != nil || len(queueRaw) != 1 || queueRaw[0] != queued["payload"] {
		t.Fatalf("terminal CPU queue entries=%d err=%v", len(queueRaw), err)
	}
	ids, err := rdb.ZRange(ctx, farmqueue.JobIndexKey, 0, -1).Result()
	if err != nil || len(ids) != 1 || ids[0] != job.ID {
		t.Fatalf("fallback published synthetic child: IDs=%v err=%v", ids, err)
	}
	renderID := fallbackMediaWorkerID(t, ctx, rdb, "fallback-media-render")
	if renderID == "" {
		t.Fatal("render heartbeat identity absent")
	}
	if n, err := rdb.LLen(ctx, farmqueue.ProcessingPrefix+renderID).Result(); err != nil || n != 0 {
		t.Fatalf("render processing remains: %d %v", n, err)
	}
	if n, err := rdb.ZCard(ctx, farmqueue.LeaseIndexKey).Result(); err != nil || n != 0 {
		t.Fatalf("render lease remains: %d %v", n, err)
	}
	if got := cpuMediaSHA(t, source); got != sourceWant {
		t.Fatalf("hardware failure changed source bytes: %s", got)
	}
	if _, err := os.Lstat(proxyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hardware failure published proxy: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(derivDir, ".proxy-commit.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hardware failure published receipt: %v", err)
	}
	// Let the same render process re-admit after terminal demotion; the locked
	// CPU payload must remain on its lane while a verified-looking worker lives.
	deadline = time.Now().Add(8 * time.Second)
	readmitted := false
	for time.Now().Before(deadline) {
		if time.Now().After(nextPermit) {
			refreshPermit()
			nextPermit = time.Now().Add(5 * time.Second)
		}
		raw, err := rdb.Get(ctx, farmqueue.WorkerPrefix+renderID).Bytes()
		if err == nil {
			var w farmqueue.Worker
			currentLog, logOverflow := render.log.snapshot()
			if json.Unmarshal(raw, &w) == nil && !w.AdmissionBlocked && !logOverflow && strings.Count(currentLog, "render accelerator re-admitted") >= 3 {
				readmitted = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	renderLog, overflow := render.log.snapshot()
	if !readmitted || overflow || strings.Count(renderLog, "render accelerator re-admitted") < 3 || strings.Count(renderLog, "ffmpeg proxy") < 2 || !strings.Contains(renderLog, "h264_vaapi") ||
		!(strings.Contains(renderLog, "Unknown encoder") || strings.Contains(renderLog, "Unrecognized hwaccel: vaapi") || strings.Contains(renderLog, "Device creation failed") || strings.Contains(renderLog, "No VA display") || strings.Contains(renderLog, "No device available")) {
		t.Fatalf("real backend-unavailable execution/re-admission proof absent; overflow=%v\n%s", overflow, renderLog)
	}
	if raw, err := rdb.HGet(ctx, farmqueue.JobHashPrefix+job.ID, "payload").Result(); err != nil || raw != queued["payload"] {
		t.Fatalf("locked CPU route promoted after render re-admission: %v", err)
	}
	cpu := fallbackMediaStart(t, "cpu", root, filepath.Dir(ffmpeg), &retain)
	deadline = time.Now().Add(50 * time.Second)
	var done map[string]string
	for time.Now().Before(deadline) {
		fallbackMediaCheckProcess(t, cpu, "CPU completion")
		done, err = rdb.HGetAll(ctx, farmqueue.JobHashPrefix+job.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		if done["status"] == farmqueue.StatusDone {
			break
		}
		if done["status"] == farmqueue.StatusFailed || done["status"] == farmqueue.StatusBlocked {
			v, _ := cpu.log.snapshot()
			t.Fatalf("CPU failed: %v\n%s", done, v)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if done["status"] != farmqueue.StatusDone || done["processed"] != "1" || done["failed"] != "0" || done["id"] != job.ID || done["worker"] != "fallback-media-cpu" ||
		done["queue_class"] != farmqueue.QueueClassCPU || done["backend"] != "libx264" || done["hardware_failures"] != "2" {
		v, _ := cpu.log.snapshot()
		t.Fatalf("same-ID CPU completion invalid: %v\n%s", done, v)
	}
	if v, _ := cpu.log.snapshot(); !strings.Contains(v, "proxy=true skipped=false") {
		t.Fatalf("CPU job did not encode real output\n%s", v)
	}
	cpuID := fallbackMediaWorkerID(t, ctx, rdb, "fallback-media-cpu")
	if cpuID == "" {
		t.Fatal("CPU heartbeat identity absent")
	}
	deadline = time.Now().Add(10 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		a, e1 := rdb.LLen(ctx, farmqueue.ProcessingPrefix+renderID).Result()
		b, e2 := rdb.LLen(ctx, farmqueue.ProcessingPrefix+cpuID).Result()
		c, e3 := rdb.ZCard(ctx, farmqueue.LeaseIndexKey).Result()
		if e1 != nil || e2 != nil || e3 != nil {
			t.Fatalf("custody reads: %v %v %v", e1, e2, e3)
		}
		if a == 0 && b == 0 && c == 0 {
			settled = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !settled {
		t.Fatal("render/CPU processing or lease custody remained after completion")
	}
	depth, err := q.QueueDepth(ctx)
	if err != nil || depth != 0 {
		t.Fatalf("queue not empty: %d %v", depth, err)
	}
	cpu.join(t)
	render.join(t)
	proxyInfo, err := os.Stat(proxyPath)
	if err != nil || !proxyInfo.Mode().IsRegular() || proxyInfo.Size() <= 0 {
		t.Fatalf("proxy output stat=%v err=%v", proxyInfo, err)
	}
	proxySHA := cpuMediaSHA(t, proxyPath)
	store, err := derivatives.Open(filepath.Join(root, "cpu.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	known, hash := store.Known(inode)
	if !known || hash == nil || *hash != sourceHash {
		t.Fatalf("CPU source row hash=%v known=%v", hash, known)
	}
	rows, err := store.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	var row *derivatives.DerivRow
	for i := range rows {
		if rows[i].Kind == "proxy" {
			row = &rows[i]
		}
	}
	if row == nil || row.Status != "ready" || row.Producer != "linux-farm" || row.Hash == nil || *row.Hash != sourceHash || row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" ||
		row.MediaType == nil || *row.MediaType != "video/mp4" || row.Codec == nil || *row.Codec != "h264" || row.CodecString == nil || *row.CodecString != "avc1.64000c, mp4a.40.2" ||
		row.SourceSize == nil || *row.SourceSize != sourceInfo.Size() || row.SourceMtime == nil || *row.SourceMtime != sourceInfo.ModTime().Unix() || row.BlobSize == nil || *row.BlobSize != proxyInfo.Size() {
		t.Fatalf("CPU ready row=%+v", row)
	}
	manifestRaw, err := os.ReadFile(filepath.Join(derivDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest farm.ManifestSidecar
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil || manifest.Inode != inode || manifest.SourceHash == nil || *manifest.SourceHash != sourceHash {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	matched := false
	for _, d := range manifest.Derivatives {
		if d.Kind == "proxy" && d.Status == row.Status && d.Producer == row.Producer && d.Version == row.Version && reflect.DeepEqual(d.Codec, row.Codec) && reflect.DeepEqual(d.Hash, row.Hash) && reflect.DeepEqual(d.BlobRelPath, row.BlobRelPath) && reflect.DeepEqual(d.MediaType, row.MediaType) && reflect.DeepEqual(d.CodecString, row.CodecString) && reflect.DeepEqual(d.SourceSize, row.SourceSize) && reflect.DeepEqual(d.SourceMtime, row.SourceMtime) && reflect.DeepEqual(d.BlobSize, row.BlobSize) {
			matched = true
		}
	}
	if !matched {
		t.Fatal("manifest omitted exact CPU ready row")
	}
	receiptRaw, err := os.ReadFile(filepath.Join(derivDir, ".proxy-commit.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Version     int    `json:"version"`
		Inode       uint64 `json:"inode"`
		SourceHash  string `json:"source_hash"`
		SourceSize  int64  `json:"source_size"`
		BlobHash    string `json:"blob_hash"`
		BlobSize    int64  `json:"blob_size"`
		Codec       string `json:"codec"`
		CodecString string `json:"codec_string"`
	}
	if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	blobSample, err := farm.SampleHash(proxyPath, proxyInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 1 || receipt.Inode != inode || receipt.SourceHash != sourceHash || receipt.SourceSize != sourceInfo.Size() || receipt.BlobHash != blobSample || receipt.BlobSize != proxyInfo.Size() || receipt.Codec != "h264" || receipt.CodecString != *row.CodecString {
		t.Fatalf("byte-bound receipt=%+v", receipt)
	}
	probe := cpuMediaProbeFile(t, ffprobe, proxyPath)
	if probe.Streams[0].CodecType != "video" || probe.Streams[0].CodecName != "h264" || probe.Streams[0].PixFmt != "yuv420p" || probe.Streams[0].Width != 320 || probe.Streams[0].Height != 180 || probe.Streams[0].ReadFrames != "24" ||
		probe.Streams[1].CodecType != "audio" || probe.Streams[1].CodecName != "aac" || probe.Streams[1].SampleRate != "48000" || probe.Streams[1].Channels != 2 {
		t.Fatalf("CPU proxy media shape: %+v", probe.Streams)
	}
	cpuMediaRun(t, ffmpeg, "-v", "error", "-xerror", "-i", proxyPath, "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
	cpuMediaFaststart(t, proxyPath)
	if cpuMediaSHA(t, source) != sourceWant {
		t.Fatal("source bytes changed during fallback")
	}
	t.Logf("real failure→CPU same-id=%s inode=%d source_sample=%s source_sha256=%s proxy_sha256=%s proxy_bytes=%d H264_profile=%s level=%d render_pid=%d cpu_pid=%d", job.ID, inode, sourceHash, sourceWant, proxySHA, proxyInfo.Size(), probe.Streams[0].Profile, probe.Streams[0].Level, render.cmd.Process.Pid, cpu.cmd.Process.Pid)
	if dir := os.Getenv("JM_FARM_FALLBACK_EVIDENCE_DIR"); dir != "" {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("evidence dir=%q info=%v err=%v", dir, info, err)
		}
		copyEvidence := func(name string, body []byte) {
			t.Helper()
			f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := f.Write(body); err != nil || n != len(body) {
				f.Close()
				t.Fatalf("evidence %s write=%d/%d err=%v", name, n, len(body), err)
			}
			if err := f.Sync(); err != nil {
				f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for name, path := range map[string]string{"source.mov": source, "proxy.mp4": proxyPath} {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copyEvidence(name, b)
		}
		for name, value := range map[string]any{"queued-cpu.json": queued, "completed-job.json": done, "ready-row.json": row} {
			body, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			copyEvidence(name, body)
		}
		copyEvidence("source.ffprobe.json", sourceProbe.Raw)
		copyEvidence("proxy.ffprobe.json", probe.Raw)
		copyEvidence("manifest.json", manifestRaw)
		copyEvidence(".proxy-commit.json", receiptRaw)
	}
}
