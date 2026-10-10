//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

type cpuMediaProcess struct {
	cmd    *exec.Cmd
	done   chan error
	output lockedBuffer
	joined bool
}

func (p *cpuMediaProcess) stop(t *testing.T) {
	t.Helper()
	if p.joined {
		return
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal owned worker %d: %v", p.cmd.Process.Pid, err)
	}
	select {
	case err := <-p.done:
		p.joined = true
		out := p.output.String()
		digest := sha256.Sum256([]byte(out))
		t.Logf("worker pid=%d original wait=%v log bytes=%d sha256=%x\n%s", p.cmd.Process.Pid, err, len(out), digest, out)
		if err != nil {
			t.Fatalf("worker %d did not stop cleanly", p.cmd.Process.Pid)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("worker %d did not stop", p.cmd.Process.Pid)
	}
}

func cpuMediaRun(t *testing.T, bin string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var output lockedBuffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		body := []byte(output.String())
		if err != nil {
			t.Fatalf("media command %s original wait=%v output=%s", filepath.Base(bin), err, body)
		}
		logged := body
		if len(logged) > 4096 {
			logged = logged[:4096]
		}
		t.Logf("media pid=%d executable=%s argv=%v original wait=0 output_bytes=%d output_prefix=%q", cmd.Process.Pid, bin, args, len(body), logged)
		return body
	case <-time.After(25 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGTERM)
		var err error
		forced := false
		select {
		case err = <-done:
		case <-time.After(3 * time.Second):
			forced = true
			_ = cmd.Process.Kill()
			err = <-done
		}
		t.Fatalf("INCOMPLETE_MEDIA_CUSTODY: media command %s timed out; forced_kill=%v original wait=%v output=%s", filepath.Base(bin), forced, err, output.String())
		return nil
	}
}

func cpuMediaSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cpuMediaPinnedTool(t *testing.T, name string) string {
	t.Helper()
	path := os.Getenv("JM_FARM_CPU_MEDIA_" + name)
	want := strings.ToLower(os.Getenv("JM_FARM_CPU_MEDIA_" + name + "_SHA256"))
	if !filepath.IsAbs(path) || len(want) != 64 {
		t.Fatalf("%s needs an absolute pinned binary and SHA-256", name)
	}
	for _, c := range want {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("%s SHA-256 is not lowercase hex", name)
		}
	}
	if got := cpuMediaSHA(t, path); got != want {
		t.Fatalf("%s binary SHA-256=%s, want %s", name, got, want)
	}
	return path
}

type cpuMediaProbe struct {
	Raw     []byte `json:"-"`
	Streams []struct {
		CodecType  string `json:"codec_type"`
		CodecName  string `json:"codec_name"`
		PixFmt     string `json:"pix_fmt"`
		Profile    string `json:"profile"`
		Level      int    `json:"level"`
		Width      int    `json:"width"`
		Height     int    `json:"height"`
		NBFrames   string `json:"nb_frames"`
		ReadFrames string `json:"nb_read_frames"`
		SampleRate string `json:"sample_rate"`
		Channels   int    `json:"channels"`
	} `json:"streams"`
}

func cpuMediaProbeFile(t *testing.T, ffprobe, path string) cpuMediaProbe {
	t.Helper()
	raw := cpuMediaRun(t, ffprobe, "-v", "error", "-count_frames", "-show_entries",
		"stream=codec_type,codec_name,pix_fmt,profile,level,width,height,nb_frames,nb_read_frames,sample_rate,channels", "-of", "json", path)
	var p cpuMediaProbe
	if err := json.Unmarshal(raw, &p); err != nil || len(p.Streams) != 2 {
		t.Fatalf("ffprobe streams=%d err=%v raw=%s", len(p.Streams), err, raw)
	}
	p.Raw = raw
	return p
}

func cpuMediaFaststart(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	moov, mdat := -1, -1
	for pos := 0; pos < len(b); {
		if len(b)-pos < 8 {
			t.Fatalf("truncated MP4 atom at %d", pos)
		}
		size := uint64(binary.BigEndian.Uint32(b[pos : pos+4]))
		header := uint64(8)
		if size == 1 {
			if len(b)-pos < 16 {
				t.Fatalf("truncated extended MP4 atom at %d", pos)
			}
			size, header = binary.BigEndian.Uint64(b[pos+8:pos+16]), 16
		} else if size == 0 {
			size = uint64(len(b) - pos)
		}
		if size < header || size > uint64(len(b)-pos) {
			t.Fatalf("invalid MP4 atom boundary at %d size=%d", pos, size)
		}
		switch string(b[pos+4 : pos+8]) {
		case "moov":
			moov = pos
		case "mdat":
			mdat = pos
		}
		pos += int(size)
	}
	if moov < 0 || mdat < 0 || moov >= mdat {
		t.Fatalf("MP4 is not faststart: moov=%d mdat=%d", moov, mdat)
	}
}

func TestRunQueueCPUMediaFreshWorker(t *testing.T) {
	if os.Getenv("JM_FARM_CPU_MEDIA_CHILD") == "1" {
		ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
		for name, pinned := range map[string]string{"ffmpeg": ffmpeg, "ffprobe": ffprobe} {
			got, err := exec.LookPath(name)
			if err != nil || got != pinned {
				t.Fatalf("child PATH selected %s=%q, want pinned %q: %v", name, got, pinned, err)
			}
		}
		root, generation := os.Getenv("JM_FARM_CPU_MEDIA_ROOT"), os.Getenv("JM_FARM_CPU_MEDIA_GENERATION")
		cfg := queueConfig{meta: os.Getenv("JM_FARM_TEST_REDIS_URL"), mount: filepath.Join(root, "mount"),
			dbPath: filepath.Join(root, "worker"+generation+".db"), encodeScratch: filepath.Join(root, "scratch"+generation),
			name: "cpu-media-worker-" + generation, producer: "linux-farm", role: farmqueue.QueueClassServer,
			kinds: []string{farmqueue.KindProxy}, vcodec: "libx264", crf: 21, preset: "fast",
			conc: 1, pConc: 1, version: 1, verbose: true}
		cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
			return workerProfile{Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}, nil
		}
		os.Exit(runQueue(cfg))
	}
	if os.Getenv("JM_FARM_CPU_MEDIA_TEST") != "1" || os.Getenv("JM_FARM_TEST_REDIS_URL") == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("requires JM_FARM_CPU_MEDIA_TEST=1 and an isolated flushable Redis")
	}
	ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
	if filepath.Dir(ffmpeg) != filepath.Dir(ffprobe) {
		t.Fatal("pinned ffmpeg and ffprobe must share one tool directory")
	}
	metaURL := os.Getenv("JM_FARM_TEST_REDIS_URL")
	opt, err := redis.ParseURL(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	q, err := farmqueue.Open(metaURL)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	root := t.TempDir()
	mount, source := filepath.Join(root, "mount"), filepath.Join(root, "mount", "source.mov")
	for _, dir := range []string{mount, filepath.Join(root, "scratch1"), filepath.Join(root, "scratch2")} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cpuMediaRun(t, ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=24:duration=1",
		"-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000:duration=1",
		"-t", "1", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "pcm_s16le", "-ac", "2", source)
	sourceInfo, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := sourceInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("source has no inode stat")
	}
	inode := uint64(stat.Ino)
	sourceHash, err := farm.SampleHash(source, sourceInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	sourceSHA := cpuMediaSHA(t, source)
	sourceProbe := cpuMediaProbeFile(t, ffprobe, source)
	if sourceProbe.Streams[0].CodecType != "video" || sourceProbe.Streams[0].CodecName != "h264" || sourceProbe.Streams[0].ReadFrames != "24" ||
		sourceProbe.Streams[0].PixFmt != "yuv420p" || sourceProbe.Streams[0].Width != 320 || sourceProbe.Streams[0].Height != 180 ||
		sourceProbe.Streams[1].CodecType != "audio" || sourceProbe.Streams[1].CodecName != "pcm_s16le" ||
		sourceProbe.Streams[1].SampleRate != "48000" || sourceProbe.Streams[1].Channels != 2 {
		t.Fatalf("source does not prove H.264 8-bit/PCM routing fixture: %+v", sourceProbe.Streams)
	}
	derivDir := filepath.Join(mount, derivatives.DerivDirRel(inode))
	proxyPath := filepath.Join(derivDir, "proxy.mp4")
	if _, err := os.Lstat(proxyPath); !os.IsNotExist(err) {
		t.Fatalf("proxy existed before first run: %v", err)
	}
	checkNoReady := func(dbPath string) {
		t.Helper()
		store, err := derivatives.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := store.Manifest(inode)
		if closeErr := store.Close(); err == nil {
			err = closeErr
		}
		if err != nil || len(rows) != 0 {
			t.Fatalf("preexisting derivative rows=%d err=%v", len(rows), err)
		}
	}
	checkNoReady(filepath.Join(root, "worker1.db"))
	startWorker := func(generation string) *cpuMediaProcess {
		t.Helper()
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		p := &cpuMediaProcess{done: make(chan error, 1)}
		p.cmd = exec.Command(executable, "-test.run=^TestRunQueueCPUMediaFreshWorker$", "-test.timeout=100s")
		p.cmd.Env = append(os.Environ(), "JM_FARM_CPU_MEDIA_CHILD=1", "JM_FARM_CPU_MEDIA_ROOT="+root,
			"JM_FARM_CPU_MEDIA_GENERATION="+generation, "JM_FARM_WATCH=0",
			"PATH="+filepath.Dir(ffmpeg)+string(os.PathListSeparator)+"/usr/bin:/bin")
		p.cmd.Stdout, p.cmd.Stderr = &p.output, &p.output
		if err := p.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		go func() { p.done <- p.cmd.Wait() }()
		t.Cleanup(func() {
			if !p.joined {
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
				var err error
				forced := false
				select {
				case err = <-p.done:
				case <-time.After(3 * time.Second):
					forced = true
					_ = p.cmd.Process.Kill()
					err = <-p.done
				}
				p.joined = true
				if forced {
					t.Errorf("INCOMPLETE_MEDIA_CUSTODY: worker pid=%d did not join after TERM; killed exact child, original wait=%v\n%s", p.cmd.Process.Pid, err, p.output.String())
				} else {
					t.Logf("cleanup worker pid=%d TERM original wait=%v\n%s", p.cmd.Process.Pid, err, p.output.String())
				}
			}
		})
		return p
	}
	workerID := func(name string) string {
		t.Helper()
		ids, err := rdb.SMembers(ctx, farmqueue.WorkerIndexKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			b, err := rdb.Get(ctx, farmqueue.WorkerPrefix+id).Bytes()
			if err != nil {
				continue
			}
			var w farmqueue.Worker
			if json.Unmarshal(b, &w) == nil && w.Name == name {
				return id
			}
		}
		return ""
	}
	waitJob := func(parentID string, p *cpuMediaProcess) (string, map[string]string) {
		t.Helper()
		deadline := time.Now().Add(55 * time.Second)
		for time.Now().Before(deadline) {
			parent, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+parentID).Result()
			if err != nil {
				t.Fatal(err)
			}
			if parent["status"] == farmqueue.StatusDispatched {
				ids, err := rdb.ZRange(ctx, farmqueue.JobIndexKey, 0, -1).Result()
				if err != nil {
					t.Fatal(err)
				}
				for _, id := range ids {
					m, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+id).Result()
					if err != nil {
						t.Fatal(err)
					}
					if m["parent_id"] != parentID {
						continue
					}
					if m["status"] == farmqueue.StatusFailed || m["status"] == farmqueue.StatusBlocked {
						t.Fatalf("child %s terminal failure: %v\n%s", id, m, p.output.String())
					}
					if m["status"] == farmqueue.StatusDone {
						// Ordinary child status hashes retain display fields, not the queued payload.
						return id, m
					}
				}
			}
			select {
			case err := <-p.done:
				p.joined = true
				t.Fatalf("worker exited before child completed: %v\n%s", err, p.output.String())
			default:
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("parent %s child did not complete\n%s", parentID, p.output.String())
		return "", nil
	}
	waitSettled := func(parentID, childID, workerID string, p *cpuMediaProcess) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			processing, e1 := rdb.LLen(ctx, farmqueue.ProcessingPrefix+workerID).Result()
			leases, e2 := rdb.ZCard(ctx, farmqueue.LeaseIndexKey).Result()
			if e1 != nil || e2 != nil {
				t.Fatalf("claim cleanup query: processing=%v lease=%v", e1, e2)
			}
			if processing == 0 && leases == 0 {
				ids, err := rdb.ZRange(ctx, farmqueue.JobIndexKey, 0, -1).Result()
				if err != nil {
					t.Fatal(err)
				}
				children := 0
				for _, id := range ids {
					m, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+id).Result()
					if err != nil {
						t.Fatal(err)
					}
					if m["parent_id"] == parentID {
						children++
						if id != childID {
							t.Fatalf("unexpected child %q for %s", id, parentID)
						}
					}
				}
				if children != 1 {
					t.Fatalf("parent %s published %d children, want one", parentID, children)
				}
				return
			}
			select {
			case err := <-p.done:
				p.joined = true
				t.Fatalf("worker exited before claim cleanup: %v\n%s", err, p.output.String())
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("parent %s child %s claim cleanup not complete\n%s", parentID, childID, p.output.String())
	}
	checkRoute := func(parentID, childID, workerName string, status map[string]string) {
		t.Helper()
		if status["id"] != childID || status["parent_id"] != parentID || status["path"] != mount ||
			status["kinds"] != farmqueue.KindProxy || status["producer"] != "linux-farm" ||
			status["status"] != farmqueue.StatusDone || status["queue_class"] != farmqueue.QueueClassCPU ||
			status["backend"] != "libx264" || status["target_worker"] != "" || status["worker"] != workerName ||
			status["processed"] != "1" || status["failed"] != "0" ||
			!strings.Contains(status["error"], "no verified hardware decode+encode path is online for h264") {
			t.Fatalf("source-aware CPU child status=%v", status)
		}
	}
	enqueue := func(id string) {
		t.Helper()
		job := farmqueue.NewJob(mount, []string{farmqueue.KindProxy}, "linux-farm")
		job.ID = id
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	}
	enqueue("cpu-media-parent-one")
	firstWorker := startWorker("1")
	firstChild, firstStatus := waitJob("cpu-media-parent-one", firstWorker)
	firstID := workerID("cpu-media-worker-1")
	if firstID == "" {
		t.Fatal("first worker heartbeat identity absent")
	}
	checkRoute("cpu-media-parent-one", firstChild, "cpu-media-worker-1", firstStatus)
	if !strings.Contains(firstWorker.output.String(), "proxy=true skipped=false") {
		t.Fatalf("first worker did not report a real proxy write\n%s", firstWorker.output.String())
	}
	waitSettled("cpu-media-parent-one", firstChild, firstID, firstWorker)
	firstWorker.stop(t)
	if n, err := rdb.LLen(ctx, farmqueue.ProcessingPrefix+firstID).Result(); err != nil || n != 0 {
		t.Fatalf("first worker processing receipt remains: depth=%d err=%v", n, err)
	}
	if _, err := rdb.ZScore(ctx, farmqueue.LeaseIndexKey, firstChild).Result(); err != redis.Nil {
		t.Fatalf("first child lease remains: %v", err)
	}
	proxyInfo, err := os.Stat(proxyPath)
	if err != nil || !proxyInfo.Mode().IsRegular() || proxyInfo.Size() <= 0 {
		t.Fatalf("proxy blob stat=%v err=%v", proxyInfo, err)
	}
	firstFD, err := os.Open(proxyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer firstFD.Close()
	firstSHA := cpuMediaSHA(t, proxyPath)
	checkRows := func(dbPath string) derivatives.DerivRow {
		t.Helper()
		store, err := derivatives.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		known, hash := store.Known(inode)
		if !known || hash == nil || *hash != sourceHash {
			t.Fatalf("source row known=%v hash=%v", known, hash)
		}
		rows, err := store.Manifest(inode)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.Kind == "proxy" {
				if row.Status != "ready" || row.Producer != "linux-farm" || row.Hash == nil || *row.Hash != sourceHash ||
					row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" || row.MediaType == nil || *row.MediaType != "video/mp4" ||
					row.Codec == nil || *row.Codec != "h264" || row.CodecString == nil || *row.CodecString != "avc1.64000c, mp4a.40.2" ||
					row.SourceSize == nil || *row.SourceSize != sourceInfo.Size() || row.SourceMtime == nil || *row.SourceMtime != sourceInfo.ModTime().Unix() ||
					row.BlobSize == nil || *row.BlobSize != proxyInfo.Size() {
					t.Fatalf("invalid ready proxy row: %+v", row)
				}
				return row
			}
		}
		t.Fatal("ready proxy row missing")
		return derivatives.DerivRow{}
	}
	firstRow := checkRows(filepath.Join(root, "worker1.db"))
	manifestRaw, err := os.ReadFile(filepath.Join(derivDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest farm.ManifestSidecar
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil || manifest.Inode != inode || manifest.SourceHash == nil || *manifest.SourceHash != sourceHash {
		t.Fatalf("manifest source identity err=%v manifest=%+v", err, manifest)
	}
	manifestProxy := false
	for _, row := range manifest.Derivatives {
		if row.Kind == "proxy" && row.Status == firstRow.Status && row.Producer == firstRow.Producer &&
			reflect.DeepEqual(row.Hash, firstRow.Hash) && reflect.DeepEqual(row.BlobRelPath, firstRow.BlobRelPath) &&
			reflect.DeepEqual(row.MediaType, firstRow.MediaType) && reflect.DeepEqual(row.SourceSize, firstRow.SourceSize) &&
			reflect.DeepEqual(row.SourceMtime, firstRow.SourceMtime) && reflect.DeepEqual(row.BlobSize, firstRow.BlobSize) &&
			reflect.DeepEqual(row.Codec, firstRow.Codec) && reflect.DeepEqual(row.CodecString, firstRow.CodecString) {
			manifestProxy = true
		}
	}
	if !manifestProxy {
		t.Fatal("manifest omitted byte-bound ready proxy row")
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
	receiptRaw, err := os.ReadFile(filepath.Join(derivDir, ".proxy-commit.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(receiptRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	blobSample, err := farm.SampleHash(proxyPath, proxyInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 1 || receipt.Inode != inode || receipt.SourceHash != sourceHash || receipt.SourceSize != sourceInfo.Size() ||
		receipt.BlobHash != blobSample || receipt.BlobSize != proxyInfo.Size() || receipt.Codec != "h264" || receipt.CodecString != *firstRow.CodecString {
		t.Fatalf("proxy commit receipt does not bind source/blob: %+v", receipt)
	}
	proxyProbe := cpuMediaProbeFile(t, ffprobe, proxyPath)
	if proxyProbe.Streams[0].CodecType != "video" || proxyProbe.Streams[0].CodecName != "h264" || proxyProbe.Streams[0].PixFmt != "yuv420p" ||
		proxyProbe.Streams[0].Width != 320 || proxyProbe.Streams[0].Height != 180 || proxyProbe.Streams[0].ReadFrames != "24" ||
		proxyProbe.Streams[1].CodecType != "audio" || proxyProbe.Streams[1].CodecName != "aac" ||
		proxyProbe.Streams[1].SampleRate != "48000" || proxyProbe.Streams[1].Channels != 2 {
		t.Fatalf("proxy stream contract: %+v", proxyProbe.Streams)
	}
	t.Logf("actual proxy H.264 profile=%q level=%d declared_codec_string=%q (avcC token and actual AAC-LC metadata asserted)",
		proxyProbe.Streams[0].Profile, proxyProbe.Streams[0].Level, *firstRow.CodecString)
	cpuMediaRun(t, ffmpeg, "-v", "error", "-xerror", "-i", proxyPath, "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
	cpuMediaFaststart(t, proxyPath)
	if _, err := os.Lstat(filepath.Join(root, "worker2.db")); !os.IsNotExist(err) {
		t.Fatalf("second worker SQLite already exists: %v", err)
	}
	// Emulate metadata left by an older producer, including a future-dated
	// sidecar. Only owned fixture metadata changes; keep the actual proxy held.
	oldToken := "avc1.640028, mp4a.40.2"
	for i := range manifest.Derivatives {
		if manifest.Derivatives[i].Kind == "proxy" {
			manifest.Derivatives[i].CodecString = &oldToken
			manifest.Derivatives[i].UpdatedAt = time.Now().Unix() + 60
		}
	}
	oldManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(derivDir, "manifest.json"), oldManifest, 0644); err != nil {
		t.Fatal(err)
	}
	var historicalReceipt map[string]json.RawMessage
	if err := json.Unmarshal(receiptRaw, &historicalReceipt); err != nil {
		t.Fatal(err)
	}
	historicalReceipt["codec_string"], err = json.Marshal(oldToken)
	if err != nil {
		t.Fatal(err)
	}
	historicalReceiptRaw, err := json.Marshal(historicalReceipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(derivDir, ".proxy-commit.json"), historicalReceiptRaw, 0644); err != nil {
		t.Fatal(err)
	}
	enqueue("cpu-media-parent-two")
	secondWorker := startWorker("2")
	secondChild, secondStatus := waitJob("cpu-media-parent-two", secondWorker)
	secondID := workerID("cpu-media-worker-2")
	if secondID == "" || secondID == firstID || secondWorker.cmd.Process.Pid == firstWorker.cmd.Process.Pid {
		t.Fatalf("second worker was not a fresh process/identity: first=%d/%s second=%d/%s",
			firstWorker.cmd.Process.Pid, firstID, secondWorker.cmd.Process.Pid, secondID)
	}
	checkRoute("cpu-media-parent-two", secondChild, "cpu-media-worker-2", secondStatus)
	if secondChild == firstChild || !strings.Contains(secondWorker.output.String(), "proxy=false skipped=true") {
		t.Fatalf("fresh worker did not skip current proxy: child=%s status=%v\n%s", secondChild, secondStatus, secondWorker.output.String())
	}
	waitSettled("cpu-media-parent-two", secondChild, secondID, secondWorker)
	secondWorker.stop(t)
	if n, err := rdb.LLen(ctx, farmqueue.ProcessingPrefix+secondID).Result(); err != nil || n != 0 {
		t.Fatalf("second worker processing receipt remains: depth=%d err=%v", n, err)
	}
	if _, err := rdb.ZScore(ctx, farmqueue.LeaseIndexKey, secondChild).Result(); err != redis.Nil {
		t.Fatalf("second child lease remains: %v", err)
	}
	_ = checkRows(filepath.Join(root, "worker2.db"))
	manifestRaw, err = os.ReadFile(filepath.Join(derivDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifestProxy = false
	for _, row := range manifest.Derivatives {
		if row.Kind == "proxy" && row.CodecString != nil && *row.CodecString == *firstRow.CodecString {
			manifestProxy = true
		}
	}
	if !manifestProxy {
		t.Fatal("fresh worker did not repair historical sidecar token")
	}
	receiptAfter, err := os.ReadFile(filepath.Join(derivDir, ".proxy-commit.json"))
	if err != nil || string(receiptAfter) != string(historicalReceiptRaw) {
		t.Fatal("repair rewrote historical byte-authority receipt")
	}
	currentInfo, err := os.Stat(proxyPath)
	if err != nil || !os.SameFile(proxyInfo, currentInfo) || currentInfo.Size() != proxyInfo.Size() ||
		!currentInfo.ModTime().Equal(proxyInfo.ModTime()) || cpuMediaSHA(t, proxyPath) != firstSHA {
		t.Fatalf("fresh worker replaced proxy: sameFile=%v oldSize=%d newInfo=%v err=%v",
			err == nil && os.SameFile(proxyInfo, currentInfo), proxyInfo.Size(), currentInfo, err)
	}
	heldInfo, err := firstFD.Stat()
	if err != nil || !os.SameFile(heldInfo, currentInfo) {
		t.Fatalf("held pre-restart FD points to replaced blob: err=%v", err)
	}
	if n, err := q.QueueDepth(ctx); err != nil || n != 0 {
		t.Fatalf("work remains claimable: depth=%d err=%v", n, err)
	}
	t.Logf("real CPU proxy source inode=%d source_sample=%s source_full_sha256=%s blob_full_sha256=%s blob_bytes=%d worker_ids=%s,%s", inode, sourceHash, sourceSHA, firstSHA, proxyInfo.Size(), firstID, secondID)
	if dir := os.Getenv("JM_FARM_CPU_MEDIA_EVIDENCE_DIR"); dir != "" {
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			t.Fatalf("configured evidence directory unavailable: %v err=%v", st, err)
		}
		copyEvidence := func(name string, body []byte) {
			t.Helper()
			out, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := out.Write(body); err != nil || n != len(body) {
				_ = out.Close()
				t.Fatalf("evidence %s write=%d/%d err=%v", name, n, len(body), err)
			}
			if err := out.Sync(); err != nil {
				_ = out.Close()
				t.Fatal(err)
			}
			if err := out.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for name, src := range map[string]string{"source.mov": source, "proxy.mp4": proxyPath} {
			body, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			copyEvidence(name, body)
		}
		copyEvidence("source.ffprobe.json", sourceProbe.Raw)
		copyEvidence("proxy.ffprobe.json", proxyProbe.Raw)
	}
}
