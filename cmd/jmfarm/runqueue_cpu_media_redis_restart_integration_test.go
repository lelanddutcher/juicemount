//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

const mediaRedisSHA = "8aa8ae0f231e47dc12a70e121ddcf3d299d3db090480e688cbf1774c52e7c92f"

type mediaRedis struct {
	cmd                              *exec.Cmd
	done                             chan struct{}
	waitErr                          error
	log                              *os.File
	control                          *redis.Client
	root, socket, port, runID, label string
	ready, shutdownSent, joined      bool
}

func mediaRedisInfo(raw string) map[string]string {
	v := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		if k, val, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			v[k] = val
		}
	}
	return v
}
func (r *mediaRedis) identity(ctx context.Context) (map[string]string, error) {
	raw, e := r.control.Info(ctx, "server").Result()
	if e != nil {
		return nil, e
	}
	v := mediaRedisInfo(raw)
	if v["process_id"] != strconv.Itoa(r.cmd.Process.Pid) || v["tcp_port"] != r.port || len(v["run_id"]) != 40 || (r.runID != "" && v["run_id"] != r.runID) {
		return nil, errors.New("owned Unix Redis PID/run_id/tcp_port mismatch")
	}
	return v, nil
}
func mediaRedisPersistence(t *testing.T, ctx context.Context, c *redis.Client) map[string]string {
	t.Helper()
	raw, e := c.Info(ctx, "persistence").Result()
	if e != nil {
		t.Fatal(e)
	}
	v := mediaRedisInfo(raw)
	for k, want := range map[string]string{"loading": "0", "async_loading": "0", "aof_enabled": "1", "aof_rewrite_in_progress": "0", "aof_rewrite_scheduled": "0", "aof_pending_rewrite": "0", "aof_last_bgrewrite_status": "ok", "aof_last_write_status": "ok", "aof_rewrites_consecutive_failures": "0", "aof_delayed_fsync": "0", "rdb_bgsave_in_progress": "0", "rdb_saves": "0"} {
		if v[k] != want {
			t.Fatalf("persistence gate %s=%q want%q", k, v[k], want)
		}
	}
	return v
}
func mediaRedisLogHealthy(t *testing.T, raw []byte, replay bool) {
	t.Helper()
	s := strings.ToLower(string(raw))
	for _, line := range strings.Split(s, "\n") {
		if (strings.Contains(line, "fsync") && (strings.Contains(line, "fail") || strings.Contains(line, "error") || strings.Contains(line, "too long"))) ||
			(strings.Contains(line, "aof") && (strings.Contains(line, "error") || strings.Contains(line, "short write") || strings.Contains(line, "truncat") || strings.Contains(line, "corrupt"))) {
			t.Fatal("Redis persistence warning; original log retained")
		}
	}
	if replay && !strings.Contains(string(raw), "DB loaded from append only file:") {
		t.Fatal("actual append-only replay marker absent")
	}
}
func (r *mediaRedis) hold(t *testing.T, why string) {
	t.Helper()
	t.Errorf("HOLD_REDIS %s pid=%d: %s; original Cmd/Wait/log retained; no signal/retry", r.label, r.cmd.Process.Pid, why)
	<-r.done
}
func (r *mediaRedis) stop(t *testing.T) {
	t.Helper()
	if r.joined {
		return
	}
	select {
	case <-r.done:
		// An unexpected exit is joined and recorded but cannot become acceptance.
		if !r.shutdownSent {
			t.Error("Redis exited without owned ordinary shutdown")
		}
	default:
		if !r.ready {
			r.hold(t, "startup identity never admitted")
		} else if !r.shutdownSent {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, e := r.identity(ctx)
			cancel()
			if e != nil {
				r.hold(t, "shutdown identity unavailable")
			} else {
				r.shutdownSent = true
				ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
				// This control client has MaxRetries=-1. NOSAVE still flushes/fsyncs AOF.
				e = r.control.Do(ctx, "SHUTDOWN", "NOSAVE").Err()
				cancel()
				restartMediaRecord(t, r.root, r.label+"-shutdown", map[string]any{"pid": r.cmd.Process.Pid, "run_id": r.runID, "command": "SHUTDOWN NOSAVE", "result": fmt.Sprint(e), "no_retry": true})
			}
		}
	}
	late := false
	select {
	case <-r.done:
	case <-time.After(10 * time.Second):
		late = true
		r.hold(t, "ordinary shutdown did not join within bound")
	}
	r.joined = true
	_ = r.control.Close()
	if e := r.log.Sync(); e != nil {
		t.Error(e)
	}
	if e := r.log.Close(); e != nil {
		t.Error(e)
	}
	raw, e := os.ReadFile(filepath.Join(r.root, r.label+".log"))
	if e != nil {
		t.Fatal(e)
	}
	restartMediaRecord(t, r.root, r.label+"-wait", map[string]any{"pid": r.cmd.Process.Pid, "run_id": r.runID, "exit": r.cmd.ProcessState.ExitCode(), "error": fmt.Sprint(r.waitErr), "late": late, "log_sha256": cpuMediaSHA(t, filepath.Join(r.root, r.label+".log"))})
	mediaRedisLogHealthy(t, raw, r.label == "redis2")
	if late || r.waitErr != nil || !r.shutdownSent || t.Failed() {
		t.Fatal("Redis original retirement failed")
	}
	if !strings.Contains(string(raw), "Calling fsync() on the AOF file.") {
		t.Fatal("ordinary shutdown AOF fsync step absent")
	}
	if _, e := os.Lstat(r.socket); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("original Redis Unix socket remains after join")
	}
}

func mediaRedisStart(t *testing.T, root, socket, port, binary, config, label string) *mediaRedis {
	t.Helper()
	if _, e := os.Lstat(socket); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("private Redis socket already exists")
	}
	log, e := os.OpenFile(filepath.Join(root, label+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	r := &mediaRedis{cmd: exec.Command(binary, config), done: make(chan struct{}), log: log, root: root, socket: socket, port: port, label: label}
	r.cmd.Stdout = log
	r.cmd.Stderr = log
	r.cmd.Env = os.Environ()
	r.control = redis.NewClient(&redis.Options{Network: "unix", Addr: socket, MaxRetries: -1, DialTimeout: 750 * time.Millisecond, ReadTimeout: 750 * time.Millisecond, WriteTimeout: 750 * time.Millisecond, PoolSize: 1})
	if e := r.cmd.Start(); e != nil {
		log.Close()
		r.control.Close()
		t.Fatal(e)
	}
	go func() { r.waitErr = r.cmd.Wait(); close(r.done) }()
	// Media children are registered later, so their original waits retire first.
	// No TERM fallback exists for uncertain Redis startup or shutdown.
	t.Cleanup(func() {
		if !r.joined {
			r.stop(t)
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-r.done:
			t.Fatalf("Redis startup exited: %v", r.waitErr)
		default:
		}
		if st, e := os.Lstat(socket); e == nil {
			ns, ok := st.Sys().(*syscall.Stat_t)
			if !ok || st.Mode()&os.ModeSocket == 0 || st.Mode().Perm() != 0700 || ns.Uid != uint32(os.Geteuid()) {
				t.Fatal("private Redis socket ownership/type/mode mismatch")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			v, e := r.identity(ctx)
			cancel()
			if e == nil {
				r.runID = v["run_id"]
				r.ready = true
				break
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			t.Fatal(e)
		}
		if time.Now().After(deadline) {
			t.Fatal("Redis private readiness exceeded bound; retain owner")
		}
		time.Sleep(50 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := map[string]string{}
	for _, key := range []string{"appendonly", "appendfsync", "save", "dir", "port", "unixsocket", "aof-load-truncated", "auto-aof-rewrite-percentage"} {
		v, e := r.control.ConfigGet(ctx, key).Result()
		if e != nil {
			t.Fatal(e)
		}
		cfg[key] = v[key]
	}
	for k, want := range map[string]string{"appendonly": "yes", "appendfsync": "everysec", "save": "", "dir": filepath.Join(root, "data"), "port": port, "unixsocket": socket, "aof-load-truncated": "no", "auto-aof-rewrite-percentage": "0"} {
		if cfg[k] != want {
			t.Fatalf("Redis config binding %s=%q want%q", k, cfg[k], want)
		}
	}
	persistence := mediaRedisPersistence(t, ctx, r.control)
	raw, e := os.ReadFile(filepath.Join(root, label+".log"))
	if e != nil {
		t.Fatal(e)
	}
	mediaRedisLogHealthy(t, raw, label == "redis2")
	restartMediaRecord(t, root, label+"-ready", map[string]any{"pid": r.cmd.Process.Pid, "run_id": r.runID, "port": port, "socket": socket, "config": cfg, "persistence": persistence, "binary_sha256": cpuMediaSHA(t, binary), "config_sha256": cpuMediaSHA(t, config)})
	return r
}

type mediaRedisAOFMember struct {
	Name, Type, SHA256 string
	Size               int64
}

// Call only after an original Redis Wait: the manifest and its members are
// stable. A base.rdb referenced by the AOF manifest is valid AOF persistence.
func mediaRedisAOF(t *testing.T, root, label string) {
	t.Helper()
	data := filepath.Join(root, "data")
	if _, e := os.Lstat(filepath.Join(data, "dump.rdb")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("standalone RDB fallback forbidden")
	}
	dir := filepath.Join(data, "appendonlydir")
	manifest := filepath.Join(dir, "appendonly.aof.manifest")
	st, e := os.Lstat(manifest)
	if e != nil || !st.Mode().IsRegular() || st.Size() > 64<<10 {
		t.Fatal("AOF manifest absent/nonregular/oversized")
	}
	raw, e := os.ReadFile(manifest)
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{"appendonly.aof.manifest": true}
	var members []mediaRedisAOFMember
	var total int64
	bases, incr := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		f := strings.Fields(line)
		if len(f) != 6 || f[0] != "file" || f[2] != "seq" || f[4] != "type" || filepath.Base(f[1]) != f[1] || strings.ContainsAny(f[1], "/\\") || seen[f[1]] {
			t.Fatal("unexpected AOF manifest record")
		}
		n, e := strconv.Atoi(f[3])
		if e != nil || n < 1 {
			t.Fatal("invalid AOF sequence")
		}
		if f[5] == "b" {
			bases++
		} else if f[5] == "i" {
			incr++
		} else {
			t.Fatal("unexpected AOF history/rewrite member")
		}
		seen[f[1]] = true
		p := filepath.Join(dir, f[1])
		st, e := os.Lstat(p)
		if e != nil || !st.Mode().IsRegular() || st.Size() < 0 {
			t.Fatal("AOF member missing/nonregular")
		}
		total += st.Size()
		if total > 16<<20 || len(members) >= 32 {
			t.Fatal("unexpected AOF evidence size")
		}
		members = append(members, mediaRedisAOFMember{Name: f[1], Type: f[5], Size: st.Size(), SHA256: cpuMediaSHA(t, p)})
	}
	if bases != 1 || incr < 1 || total == 0 {
		t.Fatal("AOF base/increment evidence absent")
	}
	entries, e := os.ReadDir(dir)
	if e != nil || len(entries) != len(seen) {
		t.Fatal("AOF directory has unbound members")
	}
	for _, e := range entries {
		if !seen[e.Name()] {
			t.Fatal("unbound AOF artifact")
		}
	}
	// Redis reopens its last incremental AOF for append. Preserve the stopped
	// bytes before a new generation starts, not just hashes of mutable inputs.
	snapshot := filepath.Join(root, label+"-aof-snapshot")
	if e := os.Mkdir(snapshot, 0700); e != nil {
		t.Fatal("exclusive stopped AOF snapshot: ", e)
	}
	copyMember := func(name string, size int64, expectedSHA string) {
		t.Helper()
		in, e := os.Open(filepath.Join(dir, name))
		if e != nil {
			t.Fatal(e)
		}
		outPath := filepath.Join(snapshot, name)
		out, e := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			in.Close()
			t.Fatal(e)
		}
		n, copyErr := io.Copy(out, io.LimitReader(in, size+1))
		syncErr := out.Sync()
		closeErr := out.Close()
		readCloseErr := in.Close()
		if n != size || copyErr != nil || syncErr != nil || closeErr != nil || readCloseErr != nil {
			t.Fatalf("stopped AOF snapshot copy %s: bytes=%d/%d errors=%v/%v/%v/%v", name, n, size, copyErr, syncErr, closeErr, readCloseErr)
		}
		if cpuMediaSHA(t, outPath) != expectedSHA {
			t.Fatal("stopped AOF snapshot hash mismatch: ", name)
		}
	}
	manifestSHA := fmt.Sprintf("%x", sha256.Sum256(raw))
	copyMember("appendonly.aof.manifest", int64(len(raw)), manifestSHA)
	for _, member := range members {
		copyMember(member.Name, member.Size, member.SHA256)
	}
	restartMediaRecord(t, root, label+"-aof", map[string]any{"manifest": string(raw), "manifest_sha256": manifestSHA, "members": members, "total_bytes": total, "standalone_rdb_absent": true, "snapshot_directory": snapshot, "snapshot_hashes_verified": true})
}

type mediaRedisSnapshot struct {
	Raw                 []string
	Status              map[string]string
	Processing, Workers []string
	Jobs, Leases        []redis.Z
	Ready               []string
	Marker, Heartbeat   string
	ExpiryMS, TTLMS     int64
	Ack                 farmqueue.WorkerCommandStatus
	AckFields           map[string]string
}

func mediaRedisSnapshotAt(t *testing.T, ctx context.Context, rdb *redis.Client, q *farmqueue.Client, point restartMediaCheckpoint) mediaRedisSnapshot {
	t.Helper()
	var s mediaRedisSnapshot
	var e error
	s.Raw, e = rdb.LRange(ctx, farmqueue.ProcessingPrefix+point.WorkerID, 0, -1).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Status, e = rdb.HGetAll(ctx, farmqueue.JobHashPrefix+point.Job.ID).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Processing, e = rdb.SMembers(ctx, farmqueue.ProcessingIndexKey).Result()
	if e != nil {
		t.Fatal(e)
	}
	sort.Strings(s.Processing)
	s.Workers, e = rdb.SMembers(ctx, farmqueue.WorkerIndexKey).Result()
	if e != nil {
		t.Fatal(e)
	}
	sort.Strings(s.Workers)
	s.Jobs, e = rdb.ZRangeWithScores(ctx, farmqueue.JobIndexKey, 0, -1).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Leases, e = rdb.ZRangeWithScores(ctx, farmqueue.LeaseIndexKey, 0, -1).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Ready, e = rdb.LRange(ctx, farmqueue.QueueKeyFor(&point.Job), 0, -1).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Marker, e = rdb.Get(ctx, "jm:private-cpu-restart").Result()
	if e != nil {
		t.Fatal(e)
	}
	s.Heartbeat, e = rdb.Get(ctx, farmqueue.WorkerPrefix+point.WorkerID).Result()
	if e != nil {
		t.Fatal("HOLD heartbeat expired/unavailable during checkpoint")
	}
	s.ExpiryMS, e = rdb.Do(ctx, "PEXPIRETIME", farmqueue.WorkerPrefix+point.WorkerID).Int64()
	if e != nil {
		t.Fatal(e)
	}
	ttl, e := rdb.PTTL(ctx, farmqueue.WorkerPrefix+point.WorkerID).Result()
	if e != nil {
		t.Fatal(e)
	}
	s.TTLMS = ttl.Milliseconds()
	var found bool
	s.Ack, found, e = q.WorkerCommandStatusByID(ctx, point.Command.ID)
	if e != nil || !found {
		t.Fatal("original queue client acknowledgement unavailable")
	}
	s.AckFields, e = rdb.HGetAll(ctx, farmqueue.WorkerCommandStatusPrefix+point.Command.ID).Result()
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(s.Raw, []string{point.ClaimRaw}) || !reflect.DeepEqual(s.Status, point.Status) || !reflect.DeepEqual(s.Processing, []string{point.WorkerID}) || !reflect.DeepEqual(s.Workers, []string{point.WorkerID}) || len(s.Jobs) != 1 || s.Jobs[0].Member != point.Job.ID || len(s.Leases) != 1 || s.Leases[0].Member != point.Job.ID || s.Leases[0].Score <= 0 || len(s.Ready) != 0 || s.Marker != point.Root || s.TTLMS <= 0 || s.ExpiryMS <= 0 || s.Ack != point.Ack {
		t.Fatal("HOLD exact direct/untracked custody or positive heartbeat changed")
	}
	var w farmqueue.Worker
	if json.Unmarshal([]byte(s.Heartbeat), &w) != nil || w.ID != point.WorkerID {
		t.Fatal("heartbeat identity mismatch")
	}
	return s
}

func TestRunQueueCPUMediaRedisRestart(t *testing.T) {
	if os.Getenv("JM_FARM_CPU_MEDIA_REDIS_RESTART_TEST") != "1" {
		t.Skip("requires owned AOF Redis restart acceptance")
	}
	if os.Getenv("JM_FARM_CPU_MEDIA_RESTART_CHILD") != "" {
		t.Fatal("Redis owner cannot run as media child")
	}
	duration, e := time.ParseDuration(flag.Lookup("test.timeout").Value.String())
	if e != nil || duration != 0 {
		t.Fatal("Redis owner requires -timeout=0")
	}
	if os.Getenv("JM_FARM_CPU_MEDIA_RESTART_TEST") != "1" || os.Getenv("JM_FARM_CPU_MEDIA_RESTART_LOCAL_FS") != "1" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Fatal("explicit media/local/empty Redis admission missing")
	}
	base := os.Getenv("JM_FARM_CPU_MEDIA_REDIS_RESTART_ROOT")
	restartMediaPrivateDir(t, base)
	restartMediaCapacity(t, base)
	socket := filepath.Join(base, "r.sock")
	if len([]byte(socket)) > 103 {
		t.Fatal("private Unix socket exceeds portable bound")
	}
	root := filepath.Join(base, "redis-aof")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal("exclusive Redis attempt directory: ", e)
	}
	data := filepath.Join(root, "data")
	if e := os.Mkdir(data, 0700); e != nil {
		t.Fatal(e)
	}
	binary := cpuMediaPinnedTool(t, "REDIS")
	if os.Getenv("JM_FARM_CPU_MEDIA_REDIS_SHA256") != mediaRedisSHA {
		t.Fatal("Redis binary is not admitted7.4.11")
	}
	// Select once, then use only private Unix readiness. If either Redis cannot
	// bind this exact TCP endpoint it must exit; never probe/substitute a port.
	l, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	_, port, e := net.SplitHostPort(l.Addr().String())
	if e != nil {
		l.Close()
		t.Fatal(e)
	}
	if e := l.Close(); e != nil {
		t.Fatal(e)
	}
	config := filepath.Join(root, "redis.conf")
	content := fmt.Sprintf("bind 127.0.0.1\nprotected-mode yes\nport %s\nunixsocket %s\nunixsocketperm 700\ndir %s\ndaemonize no\nlogfile \"\"\nloglevel notice\nsave \"\"\nappendonly yes\nappendfsync everysec\nappenddirname appendonlydir\nappendfilename appendonly.aof\naof-load-truncated no\nauto-aof-rewrite-percentage 0\n", port, strconv.Quote(socket), strconv.Quote(data))
	f, e := os.OpenFile(config, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteString(content)
	if e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = f.Sync(); e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	configSHA := cpuMediaSHA(t, config)
	first := mediaRedisStart(t, root, socket, port, binary, config, "redis1")
	t.Setenv("JM_FARM_TEST_REDIS_URL", "redis://127.0.0.1:"+port+"/0")
	t.Setenv("JM_FARM_TEST_REDIS_PID", strconv.Itoa(first.cmd.Process.Pid))
	t.Setenv("JM_FARM_TEST_REDIS_RUN_ID", first.runID)
	var second *mediaRedis
	calls := 0
	mediaRoot := runCPUMediaInterruptedRestart(t, func(ctx context.Context, point restartMediaCheckpoint, rdb *redis.Client, q *farmqueue.Client) {
		calls++
		if calls != 1 || second != nil {
			t.Fatal("checkpoint invoked more than once")
		}
		before := mediaRedisSnapshotAt(t, ctx, rdb, q, point)
		p := mediaRedisPersistence(t, ctx, first.control)
		restartMediaRecord(t, root, "custody-before", map[string]any{"snapshot": before, "persistence": p, "old_pid": first.cmd.Process.Pid, "old_run_id": first.runID})
		first.stop(t)
		mediaRedisAOF(t, root, "redis1-stopped")
		if cpuMediaSHA(t, config) != configSHA || cpuMediaSHA(t, binary) != mediaRedisSHA {
			t.Fatal("Redis config/binary changed")
		}
		second = mediaRedisStart(t, root, socket, port, binary, config, "redis2")
		if second.cmd.Process.Pid == first.cmd.Process.Pid || second.runID == first.runID {
			t.Fatal("Redis generation identity unchanged")
		}
		// Same original TCP client must reconnect after Unix identity admits the
		// new server; no TCP readiness request can reach an unowned listener.
		raw, e := rdb.Info(ctx, "server").Result()
		if e != nil {
			t.Fatal(e)
		}
		tcp := mediaRedisInfo(raw)
		if tcp["process_id"] != strconv.Itoa(second.cmd.Process.Pid) || tcp["run_id"] != second.runID || tcp["tcp_port"] != port {
			t.Fatal("original TCP client reached wrong generation")
		}
		after := mediaRedisSnapshotAt(t, ctx, rdb, q, point)
		if after.TTLMS >= before.TTLMS {
			t.Fatal("heartbeat TTL failed to decrease across restart")
		}
		beforeComparable, afterComparable := before, after
		beforeComparable.TTLMS = 0
		afterComparable.TTLMS = 0
		if !reflect.DeepEqual(beforeComparable, afterComparable) {
			t.Fatal("AOF replay changed durable custody, scores, heartbeat expiry or acknowledgement")
		}
		restartMediaRecord(t, root, "custody-after", map[string]any{"snapshot": after, "persistence": mediaRedisPersistence(t, ctx, second.control), "new_pid": second.cmd.Process.Pid, "new_run_id": second.runID, "original_tcp_client_reconnected": true, "original_queue_client_reconnected": true})
	})
	if calls != 1 || second == nil {
		t.Fatal("Redis checkpoint did not execute")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	persistence := mediaRedisPersistence(t, ctx, second.control)
	cancel()
	restartMediaRecord(t, root, "redis2-final-persistence", persistence)
	second.stop(t)
	mediaRedisAOF(t, root, "redis2-stopped")
	if cpuMediaSHA(t, config) != configSHA || cpuMediaSHA(t, binary) != mediaRedisSHA {
		t.Fatal("final Redis config/binary changed")
	}
	restartMediaRecord(t, root, "result", map[string]any{"state": "PASS_ORDERLY_AOF_REDIS_MEDIA_RECOVERY_BOUNDED", "media_result": filepath.Join(mediaRoot, "result.json"), "media_result_sha256": cpuMediaSHA(t, filepath.Join(mediaRoot, "result.json")), "redis_before": first.cmd.Process.Pid, "redis_after": second.cmd.Process.Pid, "old_run_id": first.runID, "new_run_id": second.runID, "config_sha256": configSHA, "redis_sha256": mediaRedisSHA, "tcp_port": port, "all_original_children_joined": true, "limits": []string{"one direct/untracked CPU job; no tracked planner or deployed queue acceptance", "orderly SHUTDOWN NOSAVE with AOF/everysec; no crash or power-loss durability", "no GPU, FUSE/NFS, physical Link or UI acceptance"}})
}
