//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type replacementProcessChild struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
}

func launchReplacementProcess(t *testing.T, root, raw, role, cut string) *replacementProcessChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, role+".log")
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestReplacementProcessHelper$", "-test.count=1", "-test.timeout=90s", "-test.v")
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "JM_REPLACEMENT_PROCESS_") {
			cmd.Env = append(cmd.Env, item)
		}
	}
	cmd.Env = append(cmd.Env, "JM_REPLACEMENT_PROCESS_ROOT="+root, "JM_REPLACEMENT_PROCESS_REDIS="+raw, "JM_REPLACEMENT_PROCESS_ROLE="+role, "JM_REPLACEMENT_PROCESS_CUT="+cut)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	child := &replacementProcessChild{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() { child.err = cmd.Wait(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = cmd.Process.Kill()
			<-child.done
		}
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	})
	return child
}

func (c *replacementProcessChild) diagnostic(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(c.logPath)
	if err != nil {
		return err.Error()
	}
	if len(data) > 24000 {
		data = data[len(data)-24000:]
	}
	return string(data)
}

// SIGKILL prevents the seed race detector from reporting a final exit status.
// Inspect its complete transcript after the original Wait, then retain it in
// the parent test log so the outer evidence includes both process generations.
func checkReplacementProcessTranscript(t *testing.T, c *replacementProcessChild, role string) {
	t.Helper()
	data, err := os.ReadFile(c.logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("child=%s pid=%d transcript_sha256=%s bytes=%d\n%s", role, c.cmd.Process.Pid, replacementProcessHash(data), len(data), data)
	for _, marker := range []string{"WARNING: DATA RACE", "panic:", "fatal error:", "--- FAIL:", "test timed out"} {
		if bytes.Contains(data, []byte(marker)) {
			t.Fatalf("%s child transcript contains %q", role, marker)
		}
	}
}

func awaitReplacementProcessReady(t *testing.T, root string, child *replacementProcessChild, cut string) replacementProcessReady {
	t.Helper()
	deadline := time.NewTimer(45 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var ready replacementProcessReady
		data, err := os.ReadFile(filepath.Join(root, "ready.json"))
		_, complete := os.Stat(filepath.Join(root, "ready.done"))
		if err == nil && complete == nil && json.Unmarshal(data, &ready) == nil {
			phase, length := metadata.ReplacementPublicationIntent, uint64(1<<20)
			if cut == replacementProcessCommittedRename {
				phase, length = metadata.ReplacementPublicationArmed, uint64(len(replacementProcessBody()))
			}
			if ready.PID != child.cmd.Process.Pid || ready.Cut != cut || ready.Phase != phase || ready.StageLength != length {
				t.Fatalf("invalid child boundary: %+v", ready)
			}
			select {
			case <-child.done:
				t.Fatalf("seed exited before kill: %v\n%s", child.err, child.diagnostic(t))
			default:
			}
			return ready
		}
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-child.done:
			t.Fatalf("seed exited before boundary: %v\n%s", child.err, child.diagnostic(t))
		case <-deadline.C:
			t.Fatalf("seed never reached requested crash boundary\n%s", child.diagnostic(t))
		case <-tick.C:
		}
	}
}

func replacementProcessRunID(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "run_id:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "run_id:"))
		}
	}
	t.Fatal("Redis run_id missing")
	return ""
}

// Read-only SQL and raw object bytes independently establish the durable cut.
// No parent SDK, metadata.Store, drainer or lease client survives the restart.
func verifyReplacementProcessCut(t *testing.T, root string, rdb *redis.Client, ready replacementProcessReady) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.db"), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	index := metadata.NewSpoolStore(db)
	row, err := index.Get(ready.RowID)
	if err != nil || row == nil {
		t.Fatalf("durable row: %+v %v", row, err)
	}
	publication, err := index.ReplacementPublication(row)
	committed := ready.Cut == replacementProcessCommittedRename
	phase := metadata.ReplacementPublicationIntent
	if committed {
		phase = metadata.ReplacementPublicationArmed
	}
	if err != nil || publication == nil || publication.Phase != phase || publication.StagePath != ready.StagePath || publication.OperationID != ready.OperationID || row.DrainState != metadata.DrainReady || ready.CapacityUsed != row.Size {
		t.Fatalf("durable publication/accepted charge: %+v %v", publication, err)
	}
	if committed {
		saved, err := replaceguard.Decode([]byte(publication.Record))
		if err != nil || saved.Phase != "prepared" || saved.OperationID != ready.OperationID || saved.Stage.Inode != ready.StageInode || saved.Stage.Name != path.Base(ready.StagePath) || saved.Stage.Length != uint64(row.Size) || saved.Proxy != ready.ProxyIncarnation || saved.Owner != "mac-test" || saved.Epoch == 0 || saved.Destination.Inode != 3 || saved.Destination.Name != "clip.mov" {
			t.Fatalf("local prepared authority: %+v %v", saved, err)
		}
		saved.Phase = "committed"
		remote, err := rdb.Get(ctx, replaceguard.Key(ready.StageInode)).Bytes()
		if err != nil || string(remote) != replacementRecordBytes(saved) || replacementProcessHash(remote) != ready.ServerRecordSHA256 {
			t.Fatalf("server committed/local armed split differs: %v", err)
		}
	} else if publication.Record != "" || ready.ServerRecordSHA256 != "" {
		t.Fatal("first-chunk intent acquired a remote record")
	}
	witness, err := index.ReplacementWitness(row)
	if err != nil || witness == nil || witness.OperationID != ready.OperationID || row.SpoolFile != ready.SpoolPath {
		t.Fatalf("durable witness: %v", err)
	}
	if filepath.Dir(row.SpoolFile) != filepath.Join(root, "spool", SpoolFilesSubdir) {
		t.Fatal("save escaped private spool root")
	}
	body := replacementProcessBody()
	actual, err := os.ReadFile(row.SpoolFile)
	if err != nil || !bytes.Equal(actual, body) || replacementProcessHash(actual) != ready.BodySHA256 || row.Size != int64(len(body)) {
		t.Fatalf("accepted local image changed: %v", err)
	}
	expectedSHA := replacementProcessHash(body)
	if fmt.Sprintf("%x", witness.Image.SHA256) != expectedSHA {
		t.Fatal("durable image digest differs")
	}
	if !committed {
		attr, err := rdb.Get(ctx, "i3").Bytes()
		if err != nil || replacementProcessHash(attr) != ready.OriginalAttrSHA256 {
			t.Fatalf("original attrs changed: %v", err)
		}
		graph, err := rdb.LRange(ctx, "c3_0", 0, -1).Result()
		if err != nil || replacementProcessGraphHash(graph) != ready.OriginalGraphSHA256 {
			t.Fatalf("original graph changed: %v", err)
		}
		entry, err := rdb.HGet(ctx, "d4", "clip.mov").Bytes()
		if err != nil || replacementProcessHash(entry) != ready.OriginalDentrySHA256 || len(entry) != 9 || binary.BigEndian.Uint64(entry[1:]) != 3 {
			t.Fatalf("original namespace changed: %v", err)
		}
		original, err := os.ReadFile(filepath.Join(root, "objects/chunks/0/0/77_0_65536"))
		if err != nil || !bytes.Equal(original, bytes.Repeat([]byte{'B'}, 65536)) {
			t.Fatalf("original server object changed: %v", err)
		}
	}
	opBirth, err := rdb.Get(ctx, replaceguard.BirthOperationKey(ready.OperationID)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	inodeBirth, err := rdb.Get(ctx, replaceguard.BirthInodeKey(ready.StageInode)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	born, err := replaceguard.DecodeBirth(opBirth)
	if err != nil || !bytes.Equal(opBirth, inodeBirth) || replacementProcessHash(opBirth) != ready.OperationBirthSHA256 || replacementProcessHash(inodeBirth) != ready.InodeBirthSHA256 || born.Stage.Inode != ready.StageInode || born.OperationID != ready.OperationID || born.Stage.Name != path.Base(ready.StagePath) || born.Proxy != ready.ProxyIncarnation || born.Owner != "mac-test" || born.BackendUUID != replacementProcessUUID || born.Database != 3 {
		t.Fatalf("reciprocal birth changed: %v", err)
	}
	if committed {
		verifyReplacementCommittedRemote(t, root, rdb, ready)
		return
	}
	if count, err := rdb.Exists(ctx, replaceguard.Key(ready.StageInode)).Result(); err != nil || count != 0 {
		t.Fatalf("stage armed before cut: count=%d err=%v", count, err)
	}
	stageAttr, err := rdb.Get(ctx, fmt.Sprintf("i%d", ready.StageInode)).Bytes()
	if err != nil || len(stageAttr) < 71 {
		t.Fatalf("stage attrs: %v", err)
	}
	var stage jfsmeta.Attr
	stage.Unmarshal(stageAttr)
	if stage.Length != 1<<20 || stage.Parent != 4 || stage.Typ != jfsmeta.TypeFile || stage.Nlink != 1 {
		t.Fatalf("stage shape: %+v", stage)
	}
	named, err := rdb.HGet(ctx, "d4", path.Base(ready.StagePath)).Bytes()
	if err != nil || len(named) != 9 || binary.BigEndian.Uint64(named[1:]) != ready.StageInode {
		t.Fatalf("stage name changed: %v", err)
	}
	slices, err := rdb.LRange(ctx, fmt.Sprintf("c%d_0", ready.StageInode), 0, -1).Result()
	if err != nil || len(slices) != 1 || len(slices[0]) != 24 {
		t.Fatalf("first stage chunk not committed: %d %v", len(slices), err)
	}
	slice := []byte(slices[0])
	id := binary.BigEndian.Uint64(slice[4:12])
	size := binary.BigEndian.Uint32(slice[12:16])
	if binary.BigEndian.Uint32(slice[:4]) != 0 || id <= 100 || size != 1<<20 || binary.BigEndian.Uint32(slice[16:20]) != 0 || binary.BigEndian.Uint32(slice[20:]) != 1<<20 {
		t.Fatal("unexpected first-chunk graph")
	}
	chunkPath := filepath.Join(root, "objects", fmt.Sprintf("chunks/%d/%d/%d_0_%d", id/1000000, id/1000, id, size))
	prefix, err := os.ReadFile(chunkPath)
	if err != nil || !bytes.Equal(prefix, body[:1<<20]) {
		t.Fatalf("acknowledged stage object bytes absent: %v", err)
	}
}

// This fixture deliberately uploads less than the configured 4 MiB object
// block. Verify the actual raw graph and full object independently of the SDK.
func verifyReplacementCommittedRemote(t *testing.T, root string, rdb *redis.Client, ready replacementProcessReady) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := replacementProcessBody()
	raw, err := rdb.Get(ctx, replaceguard.Key(ready.StageInode)).Bytes()
	if err != nil || replacementProcessHash(raw) != ready.ServerRecordSHA256 {
		t.Fatalf("committed receipt changed: %v", err)
	}
	record, err := replaceguard.Decode(raw)
	if err != nil || record.Phase != "committed" || record.Stage.Inode != ready.StageInode || record.Stage.Length != uint64(len(body)) || record.Proxy != ready.ProxyIncarnation || record.OperationID != ready.OperationID {
		t.Fatalf("committed receipt identity: %+v %v", record, err)
	}
	entry, err := rdb.HGet(ctx, "d4", "clip.mov").Bytes()
	if err != nil || len(entry) != 9 || entry[0] != jfsmeta.TypeFile || binary.BigEndian.Uint64(entry[1:]) != ready.StageInode {
		t.Fatalf("published destination: %v", err)
	}
	if exists, err := rdb.HExists(ctx, "d4", path.Base(ready.StagePath)).Result(); err != nil || exists {
		t.Fatalf("published stage name still exists: %v", err)
	}
	attrRaw, err := rdb.Get(ctx, fmt.Sprintf("i%d", ready.StageInode)).Bytes()
	if err != nil || len(attrRaw) < 71 {
		t.Fatalf("published attrs: %v", err)
	}
	var attr jfsmeta.Attr
	attr.Unmarshal(attrRaw)
	if attr.Parent != 4 || attr.Typ != jfsmeta.TypeFile || attr.Nlink != 1 || attr.Length != uint64(len(body)) || attr.Mtime*1_000_000_000+int64(attr.Mtimensec) != record.Stage.MtimeNS {
		t.Fatalf("published inode shape: %+v", attr)
	}
	formatBytes, err := rdb.Get(ctx, "setting").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var format jfsmeta.Format
	if err := json.Unmarshal(formatBytes, &format); err != nil || format.UUID != replacementProcessUUID || format.Compression != "none" || format.BlockSize != 4096 || len(body) >= format.BlockSize<<10 {
		t.Fatalf("unexpected object format: %+v %v", format, err)
	}
	graph, err := rdb.LRange(ctx, fmt.Sprintf("c%d_0", ready.StageInode), 0, -1).Result()
	if err != nil || len(graph) != 1 || len(graph[0]) != 24 {
		t.Fatalf("unexpected full-image graph: %d %v", len(graph), err)
	}
	slice := []byte(graph[0])
	id, size := binary.BigEndian.Uint64(slice[4:12]), binary.BigEndian.Uint32(slice[12:16])
	if binary.BigEndian.Uint32(slice[:4]) != 0 || id <= 100 || size != uint32(len(body)) || binary.BigEndian.Uint32(slice[16:20]) != 0 || binary.BigEndian.Uint32(slice[20:]) != uint32(len(body)) {
		t.Fatal("full-image graph does not cover exact saved bytes")
	}
	objectPath := filepath.Join(root, "objects", fmt.Sprintf("chunks/%d/%d/%d_0_%d", id/1000000, id/1000, id, size))
	object, err := os.ReadFile(objectPath)
	if err != nil || !bytes.Equal(object, body) || replacementProcessHash(object) != ready.BodySHA256 {
		t.Fatalf("committed full remote bytes: %v", err)
	}
	return replacementProcessGraphHash(graph)
}

func TestReplacementProcessCrashRecovery(t *testing.T) {
	testReplacementProcessCrashRecovery(t, replacementProcessFirstChunk)
}

func TestReplacementProcessCommittedCrashRecovery(t *testing.T) {
	testReplacementProcessCrashRecovery(t, replacementProcessCommittedRename)
}

func testReplacementProcessCrashRecovery(t *testing.T, cut string) {
	t.Helper()
	root := t.TempDir()
	raw := redistestfixture.Start(t)
	options, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	options.ContextTimeoutEnabled = true
	options.MaxRetries = -1
	rdb := redis.NewClient(options)
	t.Cleanup(func() { _ = rdb.Close() })
	runID := replacementProcessRunID(t, rdb)
	child := launchReplacementProcess(t, root, raw, "seed", cut)
	ready := awaitReplacementProcessReady(t, root, child, cut)
	verifyReplacementProcessCut(t, root, rdb, ready)
	var committedGraph string
	if cut == replacementProcessCommittedRename {
		committedGraph = verifyReplacementCommittedRemote(t, root, rdb, ready)
	}
	dirs := map[string]os.FileInfo{}
	for _, name := range []string{".", "objects", "spool", "state"} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		dirs[name] = info
	}
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.done:
	case <-time.After(10 * time.Second):
		t.Fatal("original seed Wait did not join")
	}
	var exited *exec.ExitError
	if !errors.As(child.err, &exited) {
		t.Fatalf("seed exit was not a signal: %v", child.err)
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("seed exit=%v", child.err)
	}
	checkReplacementProcessTranscript(t, child, "seed")
	verifyReplacementProcessCut(t, root, rdb, ready)
	if replacementProcessRunID(t, rdb) != runID {
		t.Fatal("Redis restarted")
	}
	for name, before := range dirs {
		now, err := os.Stat(filepath.Join(root, name))
		if err != nil || !os.SameFile(before, now) {
			t.Fatalf("retained directory changed %s: %v", name, err)
		}
	}
	proxyURL, err := url.Parse(ready.ProxyURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", proxyURL.Host, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("old proxy still listens after original process exit")
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		value, err := rdb.Get(ctx, respproxy.SingletonKey).Result()
		cancel()
		if err == redis.Nil {
			break
		}
		if err != nil || value != ready.ProxyIncarnation {
			t.Fatalf("unexpected singleton before restart: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("old singleton never expired")
		}
		time.Sleep(20 * time.Millisecond)
	}
	next := launchReplacementProcess(t, root, raw, "resume", cut)
	select {
	case <-next.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("recovery process did not join\n%s", next.diagnostic(t))
	}
	if next.err != nil {
		t.Fatalf("fresh process failed: %v\n%s", next.err, next.diagnostic(t))
	}
	checkReplacementProcessTranscript(t, next, "resume")
	var recovered replacementProcessRecovered
	data, err := os.ReadFile(filepath.Join(root, "recovered.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &recovered); err != nil {
		t.Fatal(err)
	}
	wantRenames := 1
	if cut == replacementProcessCommittedRename {
		wantRenames = 0
		if recovered.MutationCalls != 0 || recovered.ServerRecordSHA256 != ready.ServerRecordSHA256 || verifyReplacementCommittedRemote(t, root, rdb, ready) != committedGraph {
			t.Fatalf("committed recovery repeated mutation or changed historical receipt/graph: %+v", recovered)
		}
	}
	if recovered.PID != next.cmd.Process.Pid || recovered.PID == ready.PID || recovered.RowID != ready.RowID || recovered.Phase != "cleaned" || recovered.Renames != wantRenames || recovered.ByteCount != len(replacementProcessBody()) || recovered.CapacityUsed != ready.CapacityUsed-int64(recovered.ByteCount) || recovered.StageInode != ready.StageInode || recovered.ProxyIncarnation == ready.ProxyIncarnation || recovered.BodySHA256 != ready.BodySHA256 || recovered.OperationBirthSHA256 != ready.OperationBirthSHA256 || recovered.InodeBirthSHA256 != ready.InodeBirthSHA256 {
		t.Fatalf("recovery receipt: %+v", recovered)
	}
	if replacementProcessRunID(t, rdb) != runID {
		t.Fatal("Redis generation changed during recovery")
	}
	if _, err := os.Lstat(ready.SpoolPath); !os.IsNotExist(err) {
		t.Fatalf("private image was not retired: %v", err)
	}
	t.Logf("seed_pid=%d SIGKILL/original-Wait joined; recovery_pid=%d exit=0; same Redis/run/root; cut=%s; recovery_renames=%d mutation_calls=%d; inode=%d; full offline NFS bytes=%d; phase=cleaned/refund=%d", ready.PID, recovered.PID, cut, recovered.Renames, recovered.MutationCalls, recovered.StageInode, recovered.ByteCount, recovered.ByteCount)
}
