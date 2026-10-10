//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

const replacementProcessUUID = "12345678-1234-abcd-5678-123456789abc"
const replacementProcessPath = "media/project/clip.mov"
const replacementProcessFirstChunk = "first-chunk"
const replacementProcessCommittedRename = "committed-rename"

func replacementProcessBody() []byte {
	return bytes.Repeat([]byte("process-recovery-save-"), 160003)
}

type replacementProcessReady struct {
	Cut                  string `json:"cut"`
	PID                  int    `json:"pid"`
	RowID                int64  `json:"row_id"`
	SpoolPath            string `json:"spool_path"`
	OperationID          string `json:"operation_id"`
	Phase                string `json:"phase"`
	StagePath            string `json:"stage_path"`
	StageInode           uint64 `json:"stage_inode"`
	StageLength          uint64 `json:"stage_length"`
	ProxyURL             string `json:"proxy_url"`
	ProxyIncarnation     string `json:"proxy_incarnation"`
	OriginalAttrSHA256   string `json:"original_attr_sha256"`
	OriginalGraphSHA256  string `json:"original_graph_sha256"`
	OriginalDentrySHA256 string `json:"original_dentry_sha256"`
	OperationBirthSHA256 string `json:"operation_birth_sha256"`
	InodeBirthSHA256     string `json:"inode_birth_sha256"`
	BodySHA256           string `json:"body_sha256"`
	CapacityUsed         int64  `json:"capacity_used"`
	ServerRecordSHA256   string `json:"server_record_sha256"`
}

type replacementProcessRecovered struct {
	PID                  int    `json:"pid"`
	RowID                int64  `json:"row_id"`
	Phase                string `json:"phase"`
	Renames              int    `json:"renames"`
	ByteCount            int    `json:"byte_count"`
	CapacityUsed         int64  `json:"capacity_used"`
	ProxyURL             string `json:"proxy_url"`
	ProxyIncarnation     string `json:"proxy_incarnation"`
	OperationBirthSHA256 string `json:"operation_birth_sha256"`
	InodeBirthSHA256     string `json:"inode_birth_sha256"`
	BodySHA256           string `json:"body_sha256"`
	StageInode           uint64 `json:"stage_inode"`
	MutationCalls        int    `json:"mutation_calls"`
	ServerRecordSHA256   string `json:"server_record_sha256"`
}

func replacementProcessHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// The Redis list contains arbitrary binary slice records. Hash each raw
// element with an explicit boundary, without a UTF-8 or JSON conversion.
func replacementProcessGraphHash(graph []string) string {
	h := sha256.New()
	var length [8]byte
	for _, item := range graph {
		binary.BigEndian.PutUint64(length[:], uint64(len(item)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(item))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func replacementProcessOriginalHashes(t *testing.T, x *boundCacheFixture) (string, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attr, err := x.rdb.Get(ctx, "i3").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	graph, err := x.rdb.LRange(ctx, "c3_0", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	dentry, err := x.rdb.HGet(ctx, "d4", "clip.mov").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return replacementProcessHash(attr), replacementProcessGraphHash(graph), replacementProcessHash(dentry)
}

func replacementProcessReceipt(t *testing.T, root, name string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(root, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = dir.Sync(); err != nil {
		_ = dir.Close()
		t.Fatal(err)
	}
	if err = dir.Close(); err != nil {
		t.Fatal(err)
	}
}

func replacementProcessBirthHashes(t *testing.T, x *boundCacheFixture, operation string, inode uint64) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	op, err := x.rdb.Get(ctx, replaceguard.BirthOperationKey(operation)).Bytes()
	if err != nil {
		t.Fatalf("operation birth: %v", err)
	}
	in, err := x.rdb.Get(ctx, replaceguard.BirthInodeKey(inode)).Bytes()
	if err != nil {
		t.Fatalf("inode birth: %v", err)
	}
	return replacementProcessHash(op), replacementProcessHash(in)
}

// TestReplacementProcessHelper is inert in ordinary package runs. Its caller
// owns the Redis server and must kill and join the seed child after ready.json.
func TestReplacementProcessHelper(t *testing.T) {
	root := os.Getenv("JM_REPLACEMENT_PROCESS_ROOT")
	if root == "" {
		return
	}
	raw := os.Getenv("JM_REPLACEMENT_PROCESS_REDIS")
	if !filepath.IsAbs(root) || raw == "" {
		t.Fatal("process fixture requires absolute root and owned logical Redis URL")
	}
	cacheRoot := filepath.Join(root, "cache", replacementProcessUUID)
	if err := os.MkdirAll(cacheRoot, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_CACHE_DIR", cacheRoot)
	switch os.Getenv("JM_REPLACEMENT_PROCESS_ROLE") {
	case "seed":
		cut := os.Getenv("JM_REPLACEMENT_PROCESS_CUT")
		if cut == "" {
			cut = replacementProcessFirstChunk
		}
		if cut != replacementProcessFirstChunk && cut != replacementProcessCommittedRename {
			t.Fatal("unknown process cut")
		}
		replacementProcessSeed(t, root, raw, cut)
	case "resume":
		replacementProcessResume(t, root, raw)
	case "refuse":
		replacementProcessRefuse(t, root, raw)
	case "grant-cut", "grant-resume":
		replacementProcessRefuse(t, root, raw)
	default:
		t.Fatal("process role must be seed, resume or refuse")
	}
}

type replacementProcessBlockIO struct {
	replacementPublishIO
	cut         string
	ready       func(replacementPublishFile, int64)
	readyRename func()
	writes      int
}

func (b *replacementProcessBlockIO) Create(name string, mode os.FileMode) (replacementPublishFile, error) {
	f, err := b.replacementPublishIO.Create(name, mode)
	if err != nil {
		return nil, err
	}
	if b.cut == replacementProcessCommittedRename {
		return f, nil
	}
	return &replacementProcessBlockFile{replacementPublishFile: f, owner: b}, nil
}

func (b *replacementProcessBlockIO) Rename(source, destination string) error {
	err := b.replacementPublishIO.Rename(source, destination)
	if err == nil && b.cut == replacementProcessCommittedRename {
		b.readyRename()
		select {} // Commit succeeded, but the caller has not received its result.
	}
	return err
}

type replacementProcessBlockFile struct {
	replacementPublishFile
	owner *replacementProcessBlockIO
}

func (f *replacementProcessBlockFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.replacementPublishFile.WriteAt(p, off)
	if err == nil && n > 0 {
		f.owner.writes++
		if f.owner.writes == 1 {
			if off != 0 || n != 1<<20 {
				panic(fmt.Sprintf("first process chunk off=%d bytes=%d", off, n))
			}
			if err := f.replacementPublishFile.Sync(); err != nil {
				panic(fmt.Sprintf("stage Sync: %v", err))
			}
			f.owner.ready(f.replacementPublishFile, int64(n))
			select {} // Parent kills this exact process before WriteAt returns.
		}
	}
	return n, err
}

func replacementProcessSeed(t *testing.T, root, raw, cut string) {
	t.Helper()
	for _, name := range []string{"state", "fuse", "spool", "objects"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	x := seedBoundCacheFixture(t, &boundCacheFixture{
		raw: raw, volumeUUID: replacementProcessUUID,
		db: filepath.Join(root, "metadata.db"), state: filepath.Join(root, "state"),
		fuseRoot: filepath.Join(root, "fuse"), want: bytes.Repeat([]byte{'B'}, 65536),
	})
	body := replacementProcessBody()
	x, spool, _, row, w, _ := seedReplacementNestedFixtureBody(t, x, body, filepath.Join(root, "spool"))
	// The reusable seeder attached a default drainer. It has not started,
	// but its idle sweeper must join before attaching the opted-in drainer.
	x.jfs.handler.spoolSweeperStop()
	// The original graph's chunk 77 must have real object bytes. The base
	// metadata/cache fixture alone only seeds its resident encoded cache block.
	oldObject := filepath.Join(root, "objects", "chunks", "0", "0", "77_0_65536")
	if err := os.MkdirAll(filepath.Dir(oldObject), 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(oldObject, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.Write(x.want); err != nil || n != len(x.want) {
		_ = file.Close()
		t.Fatalf("seed original object bytes=%d err=%v", n, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: x.fuseRoot, ReplacementRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	rt := newReplacementRuntime(t, x, filepath.Join(root, "objects"), true)
	lease, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	lease.Start()
	t.Cleanup(lease.Stop)
	d.SetProjectLeases(lease)
	x.jfs.handler.SetSpool(spool, d)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	backend := &replacementSDKIO{fs: rt.fs, m: rt.m, w: *w, ctx: jfsmeta.Background(), parent: 4}
	originalAttr, originalGraph, originalDentry := replacementProcessOriginalHashes(t, x)
	block := &replacementProcessBlockIO{replacementPublishIO: backend, cut: cut}
	writeReady := func(publication *metadata.SpoolReplacementPublication, stage replaceguard.File, serverHash string) {
		opBirth, inodeBirth := replacementProcessBirthHashes(t, x, w.OperationID, stage.Inode)
		used, _ := spool.Capacity()
		replacementProcessReceipt(t, root, "ready.json", replacementProcessReady{
			Cut: cut,
			PID: os.Getpid(), RowID: row.ID, SpoolPath: row.SpoolFile, OperationID: w.OperationID,
			Phase: publication.Phase, StagePath: publication.StagePath, StageInode: stage.Inode,
			StageLength: stage.Length, ProxyURL: rt.url, ProxyIncarnation: rt.proxy.Incarnation(),
			OriginalAttrSHA256: originalAttr, OriginalGraphSHA256: originalGraph,
			OriginalDentrySHA256: originalDentry, OperationBirthSHA256: opBirth,
			InodeBirthSHA256: inodeBirth, BodySHA256: replacementProcessHash(body), CapacityUsed: used,
			ServerRecordSHA256: serverHash,
		})
		replacementProcessReceipt(t, root, "ready.done", struct{}{})
	}
	block.ready = func(f replacementPublishFile, n int64) {
		publication, err := spool.Meta().ReplacementPublication(row)
		if err != nil || publication == nil || publication.Phase != metadata.ReplacementPublicationIntent {
			panic(fmt.Sprintf("pre-arm intent: %+v %v", publication, err))
		}
		stage, err := f.Stat()
		if err != nil || stage.Length != uint64(n) || publication.StagePath != path.Join("media/project", stage.Name) {
			panic(fmt.Sprintf("stage after sync: %+v %v", stage, err))
		}
		writeReady(publication, stage, "")
	}
	block.readyRename = func() {
		publication, err := spool.Meta().ReplacementPublication(row)
		if err != nil || publication == nil || publication.Phase != metadata.ReplacementPublicationArmed {
			panic(fmt.Sprintf("post-rename armed journal: %+v %v", publication, err))
		}
		prepared, err := replaceguard.Decode([]byte(publication.Record))
		if err != nil || prepared.Phase != "prepared" || prepared.OperationID != w.OperationID || prepared.Stage.Length != uint64(len(body)) {
			panic(fmt.Sprintf("post-rename prepared record: %+v %v", prepared, err))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		serverBytes, err := x.rdb.Get(ctx, replaceguard.Key(prepared.Stage.Inode)).Bytes()
		if err != nil {
			panic(err)
		}
		server, err := replaceguard.Decode(serverBytes)
		committed := prepared
		committed.Phase = "committed"
		if err != nil || server.Phase != "committed" || replacementRecordBytes(committed) != string(serverBytes) {
			panic(fmt.Sprintf("post-rename committed server record: %+v %v", server, err))
		}
		destination, err := backend.Stat(path.Base(row.NFSPath))
		if err != nil || destination.Inode != prepared.Stage.Inode || destination.Length != uint64(len(body)) {
			panic(fmt.Sprintf("post-rename destination: %+v %v", destination, err))
		}
		writeReady(publication, prepared.Stage, replacementProcessHash(serverBytes))
	}
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		return x.jfs.handler.resumeQueuedReplacement(ctx, d, id, func(got metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
			if got.OperationID != w.OperationID {
				return nil, nil, metadata.ErrMirrorConflict
			}
			return block, func() {}, nil
		})
	}
	d.Start()
	select {} // An unexpected worker return must not clean up generation one.
}

// A historical committed receipt may only be reconciled locally. Count and
// reject every stage I/O or namespace mutation the SDK adapter could attempt.
type replacementProcessReadOnlyIO struct {
	replacementPublishIO
	checkCalls    int
	checkFailures int
	mutationCalls int
}

func (b *replacementProcessReadOnlyIO) Check() error {
	b.checkCalls++
	err := b.replacementPublishIO.Check()
	if err != nil {
		b.checkFailures++
	}
	return err
}

func (b *replacementProcessReadOnlyIO) Open(string, bool) (replacementPublishFile, error) {
	b.mutationCalls++
	return nil, metadata.ErrMirrorConflict
}
func (b *replacementProcessReadOnlyIO) Create(string, os.FileMode) (replacementPublishFile, error) {
	b.mutationCalls++
	return nil, metadata.ErrMirrorConflict
}
func (b *replacementProcessReadOnlyIO) OpenOwned(string, replaceguard.File) (replacementPublishFile, error) {
	b.mutationCalls++
	return nil, metadata.ErrMirrorConflict
}
func (b *replacementProcessReadOnlyIO) Rename(string, string) error {
	b.mutationCalls++
	return metadata.ErrMirrorConflict
}

func replacementProcessResume(t *testing.T, root, raw string) {
	t.Helper()
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	store, err := metadata.Open(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(filepath.Join(root, "spool"), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	store.SetSpoolStore(spool.Meta())
	if _, err := spool.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	row, err := spool.Meta().LookupByPath(replacementProcessPath)
	if err != nil || row == nil {
		t.Fatalf("persisted row: %+v %v", row, err)
	}
	w, err := spool.Meta().ReplacementWitness(row)
	if err != nil || w == nil {
		t.Fatalf("persisted witness: %+v %v", w, err)
	}
	publication, err := spool.Meta().ReplacementPublication(row)
	if err != nil || publication == nil || publication.OperationID != w.OperationID {
		t.Fatalf("persisted replacement journal: %+v %v", publication, err)
	}
	var cut string
	var prepared replaceguard.Record
	switch publication.Phase {
	case metadata.ReplacementPublicationIntent:
		cut = replacementProcessFirstChunk
	case metadata.ReplacementPublicationArmed:
		cut = replacementProcessCommittedRename
		prepared, err = replaceguard.Decode([]byte(publication.Record))
		if err != nil || prepared.Phase != "prepared" || prepared.OperationID != w.OperationID ||
			prepared.Stage.Length != uint64(len(replacementProcessBody())) {
			t.Fatalf("persisted prepared record: %+v %v", prepared, err)
		}
	default:
		t.Fatalf("unexpected persisted process phase %q", publication.Phase)
	}
	x := &boundCacheFixture{raw: raw, volumeUUID: replacementProcessUUID, db: filepath.Join(root, "metadata.db"),
		state: filepath.Join(root, "state"), fuseRoot: filepath.Join(root, "fuse"),
		want: bytes.Repeat([]byte{'B'}, 65536), store: store}
	x.rc, err = metadata.NewRedisClientDeferred(raw, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.rc.Stop)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	opts.ContextTimeoutEnabled, opts.MaxRetries = true, -1
	x.rdb = redis.NewClient(opts)
	t.Cleanup(func() { _ = x.rdb.Close() })
	x.paths, err = metadata.OpenCachePathEligibility(store)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, x.fuseRoot)
	x.jfs = &juiceFS{handler: h}
	x.session, err = cache.NewSession(cache.SessionConfig{Store: store, LogicalURL: raw, Options: opts,
		StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil },
		OnReader: h.SetCacheReader, OnQualified: x.rc.SetBackendQualified, OnStatus: h.SetCacheBackendStatus})
	if err != nil {
		t.Fatal(err)
	}
	h.SetBoundCacheSession(x.session, x.rc, x.paths)
	h.SetCacheReader(x.session.Reader())
	t.Cleanup(func() { h.StopHandler(); x.session.Stop() })
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: x.fuseRoot, ReplacementRecovery: true})
	if err != nil {
		t.Fatal(err)
	}
	h.SetSpool(spool, d)
	rt := newReplacementRuntime(t, x, filepath.Join(root, "objects"), true)
	lease, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	lease.Start()
	d.SetProjectLeases(lease)
	// Runs before the runtime's registered cleanup. A failed join must not
	// retire the SDK/proxy beneath a still-running queue worker.
	t.Cleanup(func() {
		if !h.StopHandler() {
			fmt.Fprintln(os.Stderr, "process resume handler did not join; retaining dependencies")
			os.Exit(2)
		}
		lease.Stop()
	})
	x.session.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	backend := &replacementSDKIO{fs: rt.fs, m: rt.m, w: *w, ctx: jfsmeta.Background(), parent: 4}
	var stage replaceguard.File
	var serverHash string
	if cut == replacementProcessFirstChunk {
		stage, err = backend.Stat(path.Base(publication.StagePath))
		if err != nil || stage.Length != 1<<20 {
			t.Fatalf("reopened partial stage: %+v %v", stage, err)
		}
	} else {
		stage = prepared.Stage
		destination, err := backend.Stat(path.Base(row.NFSPath))
		if err != nil || destination.Inode != stage.Inode || destination.Length != uint64(len(replacementProcessBody())) {
			t.Fatalf("reopened committed destination: %+v %v", destination, err)
		}
		if _, err := backend.Stat(path.Base(publication.StagePath)); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("committed stage still named: %v", err)
		}
		ctxRecord, cancelRecord := context.WithTimeout(context.Background(), 5*time.Second)
		serverBytes, err := x.rdb.Get(ctxRecord, replaceguard.Key(stage.Inode)).Bytes()
		cancelRecord()
		if err != nil {
			t.Fatal(err)
		}
		committed := prepared
		committed.Phase = "committed"
		if replacementRecordBytes(committed) != string(serverBytes) {
			t.Fatal("reopened committed receipt differs from saved prepared evidence")
		}
		serverHash = replacementProcessHash(serverBytes)
	}
	opBirth, inodeBirth := replacementProcessBirthHashes(t, x, w.OperationID, stage.Inode)
	ctxBirth, cancelBirth := context.WithTimeout(context.Background(), 5*time.Second)
	bornBytes, err := x.rdb.Get(ctxBirth, replaceguard.BirthOperationKey(w.OperationID)).Bytes()
	cancelBirth()
	if err != nil {
		t.Fatal(err)
	}
	born, err := replaceguard.DecodeBirth(bornBytes)
	if err != nil || born.Owner != "mac-test" || born.Epoch == 0 || born.Stage.Inode != stage.Inode {
		t.Fatalf("persisted stage birth: %+v %v", born, err)
	}
	readyData, err := os.ReadFile(filepath.Join(root, "ready.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ready replacementProcessReady
	if err := json.Unmarshal(readyData, &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Cut != cut || ready.ServerRecordSHA256 != serverHash ||
		ready.PID == os.Getpid() || ready.OperationID != w.OperationID || ready.RowID != row.ID ||
		ready.StagePath != publication.StagePath || ready.StageInode != stage.Inode ||
		ready.OperationBirthSHA256 != opBirth || ready.InodeBirthSHA256 != inodeBirth ||
		ready.ProxyIncarnation != born.Proxy || ready.ProxyIncarnation == rt.proxy.Incarnation() ||
		ready.StageLength != stage.Length ||
		ready.BodySHA256 != replacementProcessHash(replacementProcessBody()) {
		t.Fatal("process identity, persisted stage or birth changed")
	}
	if cut == replacementProcessFirstChunk {
		originalAttr, originalGraph, originalDentry := replacementProcessOriginalHashes(t, x)
		if ready.OriginalAttrSHA256 != originalAttr || ready.OriginalGraphSHA256 != originalGraph ||
			ready.OriginalDentrySHA256 != originalDentry {
			t.Fatal("original graph changed before resumed upload")
		}
	}
	grant, err := lease.EnsureForDrain(projectOf(row.NFSPath))
	if err != nil || !grant.held || grant.epoch != born.Epoch || grant.foreignBefore != 0 {
		t.Fatalf("recovered same-owner lease: %+v %v", grant, err)
	}
	readOnly := &replacementProcessReadOnlyIO{replacementPublishIO: backend}
	done := make(chan error, 8)
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		err := h.resumeQueuedReplacement(ctx, d, id, func(got metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
			if got.OperationID != w.OperationID {
				return nil, nil, metadata.ErrMirrorConflict
			}
			if cut == replacementProcessCommittedRename {
				return readOnly, func() {}, nil
			}
			return backend, func() {}, nil
		})
		select {
		case done <- err:
		default:
		}
		return err
	}
	d.Start()
	awaitReplacementQueueCleaned(t, &replacementResumeFixture{x: x, spool: spool, d: d, row: row}, done)
	if !d.Stop(5 * time.Second) {
		t.Fatal("replacement drainer did not join")
	}
	lease.Stop()
	wantRenames := 1
	if cut == replacementProcessCommittedRename {
		wantRenames = 0
	}
	if backend.renames != wantRenames || readOnly.mutationCalls != 0 {
		t.Fatalf("resume SDK mutations: renames=%d calls=%d", backend.renames, readOnly.mutationCalls)
	}
	used, _ := spool.Capacity()
	if used != ready.CapacityUsed-row.Size {
		t.Fatalf("capacity %d != %d", used, ready.CapacityUsed-row.Size)
	}
	if _, err := os.Lstat(row.SpoolFile); !os.IsNotExist(err) {
		t.Fatalf("private source remains: %v", err)
	}
	finalBirth, finalInodeBirth := replacementProcessBirthHashes(t, x, w.OperationID, stage.Inode)
	if finalBirth != opBirth || finalInodeBirth != inodeBirth {
		t.Fatal("birth changed during resumed publication")
	}
	ctxReceipt, cancelReceipt := context.WithTimeout(context.Background(), 5*time.Second)
	recordBytes, err := x.rdb.Get(ctxReceipt, replaceguard.Key(stage.Inode)).Bytes()
	cancelReceipt()
	if err != nil {
		t.Fatal(err)
	}
	record, err := replaceguard.Decode(recordBytes)
	wantProxy := rt.proxy.Incarnation()
	if cut == replacementProcessCommittedRename {
		wantProxy = prepared.Proxy
	}
	if err != nil || record.Phase != "committed" || record.Owner != born.Owner ||
		record.Epoch != born.Epoch || record.Proxy != wantProxy || record.Stage.Inode != stage.Inode ||
		(cut == replacementProcessCommittedRename && replacementProcessHash(recordBytes) != serverHash) {
		t.Fatalf("current committed authority: %+v %v", record, err)
	}
	assertReplacementOfflineNFS(t, x, spool, row, replacementProcessBody(), used, false)
	replacementProcessReceipt(t, root, "recovered.json", replacementProcessRecovered{
		PID: os.Getpid(), RowID: row.ID, Phase: metadata.ReplacementPublicationCleaned,
		Renames: backend.renames, ByteCount: len(replacementProcessBody()), CapacityUsed: used,
		ProxyURL: rt.url, ProxyIncarnation: rt.proxy.Incarnation(),
		OperationBirthSHA256: finalBirth, InodeBirthSHA256: finalInodeBirth,
		BodySHA256: replacementProcessHash(replacementProcessBody()), StageInode: stage.Inode,
		MutationCalls: readOnly.mutationCalls, ServerRecordSHA256: replacementProcessHash(recordBytes),
	})
}
