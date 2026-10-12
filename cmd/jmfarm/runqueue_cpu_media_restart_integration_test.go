//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
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

// Interrupt an unpublished real-media generation after observed scratch growth.
// The restart phase may be encode, mux or probe; it is not distinguished. Root
// owns Redis and the soft runtime deadline. A global Go timeout would abandon
// children, so -timeout=0 is mandatory; an unjoined child retains its owner.
const restartMediaSelector = "^TestRunQueueCPUMediaInterruptedRestart$"
const restartMediaWorkerName = "cpu-media-interrupted-restart"
const restartMediaJobID = "cpu-media-interrupted-same-job"

type restartMediaChild struct {
	cmd           *exec.Cmd
	done          chan struct{}
	waitErr       error
	log           *os.File
	joined        bool
	stopRequested bool
	label         string
}

func restartMediaRecord(t *testing.T, root, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(root, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, e1 := f.Write(append(raw, '\n'))
	e2 := f.Sync()
	e3 := f.Close()
	if e1 != nil || e2 != nil || e3 != nil {
		t.Fatalf("evidence write: %v %v %v", e1, e2, e3)
	}
}

func restartMediaStart(t *testing.T, root, label string, argv, env []string) *restartMediaChild {
	t.Helper()
	log, err := os.OpenFile(filepath.Join(root, label+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	p := &restartMediaChild{cmd: exec.Command(argv[0], argv[1:]...), done: make(chan struct{}), log: log, label: label}
	p.cmd.Env = env
	p.cmd.Stdout = log
	p.cmd.Stderr = log
	if err = p.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	go func() { p.waitErr = p.cmd.Wait(); close(p.done) }()
	// Never kill, abandon or erase a failed fixture. Unknown stop retains this
	// original Cmd/Wait and log. Root must also retain the Go owner.
	t.Cleanup(func() {
		if p.joined {
			return
		}
		select {
		case <-p.done:
		default:
			if !p.stopRequested {
				p.stopRequested = true
				_ = p.cmd.Process.Signal(syscall.SIGTERM)
			}
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				t.Errorf("HOLD_UNJOINED: %s pid=%d; original owner/log retained, no forced kill", label, p.cmd.Process.Pid)
				<-p.done
			}
		}
		p.joined = true
		_ = p.log.Close()
		t.Logf("failed-case cleanup original wait %s pid=%d err=%v; evidence retained", label, p.cmd.Process.Pid, p.waitErr)
	})
	return p
}

func restartMediaWait(t *testing.T, root string, p *restartMediaChild, timeout time.Duration, quiescent bool) {
	t.Helper()
	exceeded := false
	select {
	case <-p.done:
	case <-time.After(timeout):
		exceeded = true
		// Never resolve an unknown restart outcome with another action.
		t.Errorf("HOLD_UNJOINED: %s pid=%d; retaining original wait", p.label, p.cmd.Process.Pid)
		<-p.done
	}
	p.joined = true
	if err := p.log.Close(); err != nil {
		t.Fatal(err)
	}
	restartMediaRecord(t, root, p.label+"-wait", map[string]any{"pid": p.cmd.Process.Pid, "exit": p.cmd.ProcessState.ExitCode(), "error": fmt.Sprint(p.waitErr), "log_sha256": cpuMediaSHA(t, filepath.Join(root, p.label+".log"))})
	if exceeded {
		t.Fatal("phase deadline exceeded; late original join retained, acceptance stopped")
	}
	if p.waitErr != nil {
		t.Fatalf("%s original wait: %v", p.label, p.waitErr)
	}
	if quiescent {
		raw, err := os.ReadFile(filepath.Join(root, p.label+".log"))
		if err != nil || !strings.Contains(string(raw), "QUIESCENT interrupted-restart\n") {
			t.Fatalf("%s lacks joined runQueue return: %v", p.label, err)
		}
	}
}

func restartMediaAlive(t *testing.T, p *restartMediaChild) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("worker %s exited prematurely: %v", p.label, p.waitErr)
	default:
	}
}

func restartMediaPrivateDir(t *testing.T, path string) {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		t.Fatal("evidence path must be absolute/canonical")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		t.Fatal("evidence path has symlink or is missing")
	}
	for q := path; ; q = filepath.Dir(q) {
		st, err := os.Lstat(q)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("invalid evidence ancestor %s", q)
		}
		native, ok := st.Sys().(*syscall.Stat_t)
		if !ok || (native.Uid != 0 && native.Uid != uint32(os.Geteuid())) || st.Mode().Perm()&0022 != 0 {
			t.Fatalf("unsafe evidence ancestor %s", q)
		}
		if q == path && (native.Uid != uint32(os.Geteuid()) || st.Mode().Perm() != 0700) {
			t.Fatal("evidence directory must be owned0700")
		}
		if filepath.Dir(q) == q {
			break
		}
	}
}

func restartMediaCapacity(t *testing.T, path string) {
	t.Helper()
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		t.Fatal(err)
	}
	if uint64(s.Bavail)*uint64(s.Bsize) < 40805695488 {
		t.Fatal("primary free space below40,805,695,488 bytes")
	}
}

func restartMediaAbsent(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Fatalf("INCONCLUSIVE_PREMATURE_PUBLICATION: %s exists or cannot be observed: %v", p, err)
		}
	}
}

// Reuse only subprocess-free helpers: pinned hashes, streamed SHA256 and MP4
// atom validation. cpuMediaRun/stop have forced-timeout cleanup, so are unused.
func TestRunQueueCPUMediaInterruptedRestart(t *testing.T) {
	if os.Getenv("JM_FARM_CPU_MEDIA_RESTART_CHILD") == "1" {
		ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
		for name, want := range map[string]string{"ffmpeg": ffmpeg, "ffprobe": ffprobe} {
			got, err := exec.LookPath(name)
			if err != nil || got != want {
				t.Fatalf("child tool binding %s", name)
			}
		}
		root, generation := os.Getenv("JM_FARM_CPU_MEDIA_RESTART_ROOT"), os.Getenv("JM_FARM_CPU_MEDIA_RESTART_GENERATION")
		if generation != "1" && generation != "2" {
			t.Fatal("unknown worker generation")
		}
		farm.SetProxyThreads(2)
		farm.SetFFmpegThreads(2)
		cfg := queueConfig{meta: os.Getenv("JM_FARM_TEST_REDIS_URL"), mount: filepath.Join(root, "library"),
			dbPath: filepath.Join(root, "worker"+generation+".db"), encodeScratch: filepath.Join(root, "scratch"+generation),
			name: restartMediaWorkerName, producer: "linux-farm", role: farmqueue.QueueClassServer,
			kinds: []string{farmqueue.KindProxy}, vcodec: "libx264", crf: 21, preset: "slow", conc: 1, pConc: 1, version: 1, verbose: true}
		cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
			return workerProfile{Role: farmqueue.QueueClassServer, Capabilities: []string{"cpu", "metadata"}}, nil
		}
		code := runQueue(cfg)
		// Production cancellation kills its owned media process group, then
		// CombinedOutput joins before runQueue's defers finish. This marker is
		// emitted only after that return, not by test cleanup.
		fmt.Println("QUIESCENT interrupted-restart")
		os.Exit(code)
	}
	if os.Getenv("JM_FARM_CPU_MEDIA_RESTART_TEST") != "1" {
		t.Skip("requires explicit interrupted real-media admission")
	}
	if os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" || os.Getenv("JM_FARM_CPU_MEDIA_RESTART_LOCAL_FS") != "1" {
		t.Fatal("requires owned Redis and root-admitted ordinary local filesystem")
	}
	timeout, err := time.ParseDuration(flag.Lookup("test.timeout").Value.String())
	if err != nil || timeout != 0 {
		t.Fatal("requires -timeout=0; root retains owner on soft timeout")
	}
	ffmpeg, ffprobe := cpuMediaPinnedTool(t, "FFMPEG"), cpuMediaPinnedTool(t, "FFPROBE")
	if filepath.Dir(ffmpeg) != filepath.Dir(ffprobe) {
		t.Fatal("pinned tools must share directory")
	}
	base := os.Getenv("JM_FARM_CPU_MEDIA_RESTART_EVIDENCE_DIR")
	restartMediaPrivateDir(t, base)
	restartMediaCapacity(t, base)
	root := filepath.Join(base, "interrupted-restart")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal("exclusive attempt directory: ", err)
	}
	// No RemoveAll, TempDir, HOME changes, FUSE/NFS operations or CACHE paths.
	for _, name := range []string{"library", "scratch1", "scratch2"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	env := os.Environ()
	ctx, cancel := context.WithTimeout(context.Background(), 165*time.Second)
	defer cancel()
	opt, err := redis.ParseURL(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal("invalid Redis admission")
	}
	host, _, err := net.SplitHostPort(opt.Addr)
	if err != nil || host != "127.0.0.1" || opt.Network != "tcp" {
		t.Fatal("Redis must be exact IPv4 loopback TCP")
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	info, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal("Redis identity unavailable")
	}
	identity := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			identity[k] = v
		}
	}
	pid := os.Getenv("JM_FARM_TEST_REDIS_PID")
	runID := os.Getenv("JM_FARM_TEST_REDIS_RUN_ID")
	if pid == "" || runID == "" || identity["process_id"] != pid || identity["run_id"] != runID {
		t.Fatal("owned Redis PID/run_id mismatch")
	}
	if n, err := rdb.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Fatal("dedicated Redis DB is not empty; never flushing")
	}
	if ok, err := rdb.SetNX(ctx, "jm:private-cpu-restart", root, 0).Result(); err != nil || !ok {
		t.Fatal("private Redis marker not exclusive")
	}
	q, err := farmqueue.Open(os.Getenv("JM_FARM_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal("queue open failed")
	}
	defer q.Close()
	restartMediaRecord(t, root, "admission", map[string]any{"redis_pid": pid, "redis_run_id": runID, "ffmpeg_sha256": cpuMediaSHA(t, ffmpeg), "ffprobe_sha256": cpuMediaSHA(t, ffprobe), "local_fs_root_admission": true, "global_test_timeout_disabled": true})
	run := func(label, bin string, args ...string) []byte {
		t.Helper()
		p := restartMediaStart(t, root, label, append([]string{bin}, args...), env)
		restartMediaWait(t, root, p, 25*time.Second, false)
		raw, err := os.ReadFile(filepath.Join(root, label+".log"))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	source := filepath.Join(root, "library", "source.mov")
	run("source-generate", ffmpeg, "-n", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=24:duration=24", "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000:duration=24", "-t", "24", "-threads", "2", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "pcm_s16le", "-ac", "2", source)
	sourceInfo, err := os.Stat(source)
	if err != nil || !sourceInfo.Mode().IsRegular() || sourceInfo.Size() <= 0 || sourceInfo.Size() > 96<<20 {
		t.Fatal("source size/identity outside bound")
	}
	native, ok := sourceInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("source inode unavailable")
	}
	inode := uint64(native.Ino)
	sourceHash, err := farm.SampleHash(source, sourceInfo.Size())
	if err != nil {
		t.Fatal(err)
	}
	sourceSHA := cpuMediaSHA(t, source)
	derivDir := filepath.Join(root, "library", derivatives.DerivDirRel(inode))
	proxy := filepath.Join(derivDir, "proxy.mp4")
	receiptPath := filepath.Join(derivDir, ".proxy-commit.json")
	restartMediaAbsent(t, proxy, receiptPath)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	startWorker := func(generation string) *restartMediaChild {
		t.Helper()
		restartMediaAbsent(t, filepath.Join(root, "worker"+generation+".db"))
		childEnv := append(append([]string{}, env...), "JM_FARM_CPU_MEDIA_RESTART_CHILD=1", "JM_FARM_CPU_MEDIA_RESTART_ROOT="+root, "JM_FARM_CPU_MEDIA_RESTART_GENERATION="+generation, "JM_FARM_WATCH=0", "PATH="+filepath.Dir(ffmpeg)+string(os.PathListSeparator)+"/usr/bin:/bin")
		return restartMediaStart(t, root, "worker"+generation, []string{executable, "-test.run=" + restartMediaSelector, "-test.timeout=0"}, childEnv)
	}
	worker := func() farmqueue.Worker {
		t.Helper()
		ids, err := rdb.SMembers(ctx, farmqueue.WorkerIndexKey).Result()
		if err != nil {
			t.Fatal(err)
		}
		var found farmqueue.Worker
		for _, id := range ids {
			raw, err := rdb.Get(ctx, farmqueue.WorkerPrefix+id).Bytes()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			var w farmqueue.Worker
			if err := json.Unmarshal(raw, &w); err != nil {
				t.Fatal(err)
			}
			if w.Name == restartMediaWorkerName {
				if found.ID != "" {
					t.Fatal("multiple live generations")
				}
				found = w
			}
		}
		return found
	}
	job := farmqueue.NewJob(filepath.Join(root, "library"), []string{farmqueue.KindProxy}, "linux-farm")
	job.ID = restartMediaJobID
	job.QueueClass = farmqueue.QueueClassCPU
	job.RequiredCapabilities = []string{"cpu"}
	job.SelectedBackend = "libx264"
	job.VCodec = "libx264"
	job.CPUFallbackLocked = true
	job.RetryTargets = []string{source}
	job.ShardCount = 1
	job.ShardIndex = 1
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	first := startWorker("1")
	var firstWorker farmqueue.Worker
	var scratchInfo os.FileInfo
	var scratchPath, claimRaw string
	active := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		restartMediaAlive(t, first)
		restartMediaCapacity(t, base)
		restartMediaAbsent(t, proxy, receiptPath)
		firstWorker = worker()
		status, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+job.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] == farmqueue.StatusDone || status["status"] == farmqueue.StatusFailed {
			t.Fatal("INCONCLUSIVE: job finished before active-media admission")
		}
		entries, err := os.ReadDir(filepath.Join(root, "scratch1"))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > 1 {
			t.Fatal("unexpected scratch writers")
		}
		if len(entries) == 1 {
			path := filepath.Join(root, "scratch1", entries[0].Name())
			now, err := os.Lstat(path)
			if err != nil || !now.Mode().IsRegular() || !strings.HasPrefix(entries[0].Name(), "jmfarm-proxy-") || now.Size() > 256<<20 {
				t.Fatal("invalid real scratch observation")
			}
			if scratchInfo != nil && (!os.SameFile(scratchInfo, now) || scratchPath != path) {
				t.Fatal("scratch inode replaced before restart")
			}
			if scratchInfo != nil && scratchInfo.Size() >= 256<<10 && now.Size() > scratchInfo.Size() && firstWorker.ID != "" && firstWorker.CurrentJob == job.ID && status["status"] == farmqueue.StatusRunning {
				raws, err := rdb.LRange(ctx, farmqueue.ProcessingPrefix+firstWorker.ID, 0, -1).Result()
				if err != nil || len(raws) != 1 {
					t.Fatal("exact active processing receipt absent")
				}
				var claimed farmqueue.Job
				if err := json.Unmarshal([]byte(raws[0]), &claimed); err != nil || claimed.ID != job.ID || !reflect.DeepEqual(claimed.RetryTargets, []string{source}) {
					t.Fatal("active claim identity mismatch")
				}
				claimRaw = raws[0]
				restartMediaRecord(t, root, "active-media", map[string]any{"worker": firstWorker, "status": status, "claim_raw": claimRaw, "scratch_path": path, "inode": now.Sys().(*syscall.Stat_t).Ino, "bytes_before": scratchInfo.Size(), "bytes_after": now.Size(), "observed_unix_nano": time.Now().UnixNano()})
				active = true
				break
			}
			scratchInfo = now
			scratchPath = path
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !active {
		t.Fatal("INCONCLUSIVE: no growing real media scratch under exact running claim before publication")
	}
	restartMediaAbsent(t, proxy, receiptPath)
	restartMediaAlive(t, first)
	// Sole restart request. From dispatch onward cleanup may only retain/join;
	// it cannot send another stop on a receipt-write or acknowledgement failure.
	first.stopRequested = true
	command, err := q.RequestWorkerRestart(ctx, restartMediaWorkerName, "isolated-media-acceptance")
	if err != nil {
		t.Errorf("HOLD_RESTART_UNKNOWN: retaining original worker")
		<-first.done
		t.Fatal("restart request failed without retry")
	}
	restartMediaRecord(t, root, "restart-request", command)
	restartMediaWait(t, root, first, 15*time.Second, true)
	ack, found, err := q.WorkerCommandStatusByID(ctx, command.ID)
	if err != nil || !found || ack.ID != command.ID || ack.Name != restartMediaWorkerName || ack.Action != farmqueue.WorkerActionRestart || ack.State != "acknowledged" || ack.WorkerID != firstWorker.ID {
		t.Fatalf("restart acknowledgement not bound to original worker: %+v err=%v", ack, err)
	}
	restartMediaRecord(t, root, "restart-ack", ack)
	restartMediaAbsent(t, proxy, receiptPath)
	files, err := os.ReadDir(filepath.Join(root, "scratch1"))
	if err != nil || len(files) != 0 {
		t.Fatal("first worker returned with scratch still present")
	}
	raws, err := rdb.LRange(ctx, farmqueue.ProcessingPrefix+firstWorker.ID, 0, -1).Result()
	if err != nil || !reflect.DeepEqual(raws, []string{claimRaw}) {
		t.Fatal("interrupted claim was lost/replaced")
	}
	status, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+job.ID).Result()
	if err != nil || status["status"] != farmqueue.StatusRunning {
		t.Fatal("interrupted claim published terminal outcome")
	}
	restartMediaRecord(t, root, "interrupted-custody", map[string]any{"claim_raw": raws, "status": status, "no_blob": true, "no_scratch": true})
	// No DEL/EXPIRE, fabricated heartbeat or manual reaper. Observe ordinary
	// liveness expiry before launching a fresh generation.
	deadline = time.Now().Add(35 * time.Second)
	for {
		n, err := rdb.Exists(ctx, farmqueue.WorkerPrefix+firstWorker.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original heartbeat did not expire naturally")
		}
		time.Sleep(100 * time.Millisecond)
	}
	raws, err = rdb.LRange(ctx, farmqueue.ProcessingPrefix+firstWorker.ID, 0, -1).Result()
	if err != nil || !reflect.DeepEqual(raws, []string{claimRaw}) {
		t.Fatal("claim changed before fresh worker recovery")
	}
	restartMediaRecord(t, root, "natural-heartbeat-expiry", map[string]any{"old_worker": firstWorker.ID, "claim_raw": raws, "observed_unix_nano": time.Now().UnixNano()})
	second := startWorker("2")
	var secondWorker farmqueue.Worker
	deadline = time.Now().Add(70 * time.Second)
	for {
		restartMediaAlive(t, second)
		restartMediaCapacity(t, base)
		status, err = rdb.HGetAll(ctx, farmqueue.JobHashPrefix+job.ID).Result()
		if err != nil {
			t.Fatal(err)
		}
		secondWorker = worker()
		if status["status"] == farmqueue.StatusDone {
			break
		}
		if status["status"] == farmqueue.StatusFailed || status["status"] == farmqueue.StatusBlocked {
			t.Fatalf("recovered job failed: %v", status)
		}
		if time.Now().After(deadline) {
			t.Fatal("same job did not recover and finish within phase bound")
		}
		entries, err := os.ReadDir(filepath.Join(root, "scratch2"))
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, e := range entries {
			st, err := e.Info()
			if err != nil || !st.Mode().IsRegular() {
				t.Fatal("invalid recovery scratch")
			}
			total += st.Size()
		}
		if total > 256<<20 {
			t.Fatal("recovery scratch budget exceeded")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if secondWorker.ID == "" || secondWorker.ID == firstWorker.ID || second.cmd.Process.Pid == first.cmd.Process.Pid || status["id"] != job.ID || status["processed"] != "1" || status["failed"] != "0" || status["worker"] != restartMediaWorkerName {
		t.Fatal("same-job fresh-worker completion identity/counts invalid")
	}
	// Wait for ACK, not merely a done display hash.
	deadline = time.Now().Add(8 * time.Second)
	for {
		a, e1 := rdb.LLen(ctx, farmqueue.ProcessingPrefix+firstWorker.ID).Result()
		b, e2 := rdb.LLen(ctx, farmqueue.ProcessingPrefix+secondWorker.ID).Result()
		n, e3 := rdb.ZCard(ctx, farmqueue.LeaseIndexKey).Result()
		if e1 != nil || e2 != nil || e3 != nil {
			t.Fatal("claim settlement query failed")
		}
		if a == 0 && b == 0 && n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claim/lease not settled")
		}
		time.Sleep(50 * time.Millisecond)
	}
	ids, err := rdb.ZRange(ctx, farmqueue.JobIndexKey, 0, -1).Result()
	if err != nil || !reflect.DeepEqual(ids, []string{job.ID}) {
		t.Fatal("unexpected job/child publication")
	}
	depth, err := rdb.LLen(ctx, farmqueue.QueueKeyFor(&job)).Result()
	if err != nil || depth != 0 {
		t.Fatal("ready queue not drained")
	}
	// Ordinary TERM of the second idle worker is retirement, not the restart
	// under test. No forced kill or counting failed cleanup as acceptance.
	second.stopRequested = true
	if err := second.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	restartMediaWait(t, root, second, 10*time.Second, true)
	logs, err := os.ReadFile(filepath.Join(root, "worker2.log"))
	if err != nil || !strings.Contains(string(logs), "recovered 1 abandoned job(s)") || strings.Count(string(logs), "proxy=true skipped=false") != 1 {
		t.Fatal("missing one actual recovered media publication")
	}
	firstLog, err := os.ReadFile(filepath.Join(root, "worker1.log"))
	if err != nil || strings.Contains(string(firstLog), "proxy=true skipped=false") {
		t.Fatal("first generation published before interruption")
	}
	if cpuMediaSHA(t, source) != sourceSHA {
		t.Fatal("source bytes changed")
	}
	output, err := os.Stat(proxy)
	if err != nil || !output.Mode().IsRegular() || output.Size() <= 0 || output.Size() > 96<<20 {
		t.Fatal("invalid bounded final proxy")
	}
	store, err := derivatives.Open(filepath.Join(root, "worker2.db"))
	if err != nil {
		t.Fatal(err)
	}
	known, knownHash := store.Known(inode)
	rows, err := store.Manifest(inode)
	closeErr := store.Close()
	if err != nil || closeErr != nil || !known || knownHash == nil || *knownHash != sourceHash {
		t.Fatal("source-bound store identity missing")
	}
	var row derivatives.DerivRow
	count := 0
	for _, v := range rows {
		if v.Kind == "proxy" {
			row = v
			count++
		}
	}
	if count != 1 || row.Status != "ready" || row.Producer != "linux-farm" || row.Hash == nil || *row.Hash != sourceHash || row.SourceSize == nil || *row.SourceSize != sourceInfo.Size() || row.SourceMtime == nil || *row.SourceMtime != sourceInfo.ModTime().Unix() || row.BlobSize == nil || *row.BlobSize != output.Size() || row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" || row.Codec == nil || *row.Codec != "h264" || row.CodecString == nil || row.MediaType == nil || *row.MediaType != "video/mp4" {
		t.Fatal("final ready row is not source/blob bound")
	}
	var manifest farm.ManifestSidecar
	raw, err := os.ReadFile(filepath.Join(derivDir, "manifest.json"))
	if err != nil || json.Unmarshal(raw, &manifest) != nil || manifest.Inode != inode || manifest.SourceHash == nil || *manifest.SourceHash != sourceHash {
		t.Fatal("final manifest identity invalid")
	}
	// Provenance is intentionally local-only (json:"-"), not a manifest field.
	wireRow, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	match := 0
	for _, v := range manifest.Derivatives {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		if v.Kind == "proxy" && string(b) == string(wireRow) {
			match++
		}
	}
	if match != 1 {
		t.Fatal("manifest ready proxy differs from SQLite wire fields")
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
	raw, err = os.ReadFile(receiptPath)
	if err != nil || json.Unmarshal(raw, &receipt) != nil {
		t.Fatal("proxy commit receipt unreadable")
	}
	blobHash, err := farm.SampleHash(proxy, output.Size())
	if err != nil || receipt.Version != 1 || receipt.Inode != inode || receipt.SourceHash != sourceHash || receipt.SourceSize != sourceInfo.Size() || receipt.BlobHash != blobHash || receipt.BlobSize != output.Size() || receipt.Codec != "h264" || receipt.CodecString != *row.CodecString {
		t.Fatal("commit receipt does not bind actual output")
	}
	probeRaw := run("proxy-probe", ffprobe, "-v", "error", "-count_frames", "-show_entries", "stream=codec_type,codec_name,pix_fmt,profile,level,width,height,nb_frames,nb_read_frames,sample_rate,channels", "-of", "json", proxy)
	var probe cpuMediaProbe
	if json.Unmarshal(probeRaw, &probe) != nil || len(probe.Streams) != 2 {
		t.Fatal("final probe invalid")
	}
	video, audio := probe.Streams[0], probe.Streams[1]
	if video.CodecType != "video" || video.CodecName != "h264" || video.PixFmt != "yuv420p" || video.Width != 1280 || video.Height != 720 || video.ReadFrames != "576" || video.Profile != "High" || audio.CodecType != "audio" || audio.CodecName != "aac" || audio.Profile != "LC" || audio.SampleRate != "48000" || audio.Channels != 2 {
		t.Fatal("final complete-media stream contract failed")
	}
	if *row.CodecString != fmt.Sprintf("avc1.6400%02x, mp4a.40.2", video.Level) {
		t.Fatal("declared codec differs from actual High-profile/level/AAC-LC")
	}
	run("proxy-full-decode", ffmpeg, "-v", "error", "-xerror", "-i", proxy, "-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-")
	cpuMediaFaststart(t, proxy)
	if cpuMediaSHA(t, ffmpeg) != strings.ToLower(os.Getenv("JM_FARM_CPU_MEDIA_FFMPEG_SHA256")) || cpuMediaSHA(t, ffprobe) != strings.ToLower(os.Getenv("JM_FARM_CPU_MEDIA_FFPROBE_SHA256")) {
		t.Fatal("tool bytes changed")
	}
	restartMediaRecord(t, root, "result", map[string]any{"state": "PASS_INTERRUPTED_CPU_MEDIA_RECOVERY_BOUNDED", "job_id": job.ID, "restart_command_id": command.ID, "worker_before": firstWorker.ID, "worker_after": secondWorker.ID, "source_sha256": sourceSHA, "proxy_sha256": cpuMediaSHA(t, proxy), "status": status, "row": row, "receipt": receipt, "all_original_children_joined": true, "limits": []string{"isolated CPU/local filesystem only", "no GPU/deployed queue/Redis restart/mount acceptance", "media join follows production CombinedOutput and quiescent worker return", "restart interrupted an unpublished real-media job after scratch growth; encode/mux/probe phase is not distinguished"}})
}
