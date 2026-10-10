//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type replacementProcessRefused struct {
	AdapterCheckFailures int               `json:"adapter_check_failures"`
	PeerBefore           map[string]string `json:"peer_before"`
	AuthorityAfter       map[string]string `json:"authority_after"`
	Case                 string            `json:"case"`
	PID                  int               `json:"pid"`
	RowID                int64             `json:"row_id"`
	OperationID          string            `json:"operation_id"`
	AcceptedEpoch        uint64            `json:"accepted_epoch"`
	Phase                string            `json:"phase"`
	Revision             int64             `json:"revision"`
	ProxyURL             string            `json:"proxy_url"`
	ProxyIncarnation     string            `json:"proxy_incarnation"`
	StageInode           uint64            `json:"stage_inode"`
	StageLength          uint64            `json:"stage_length"`
	OriginalAttrSHA256   string            `json:"original_attr_sha256"`
	OriginalGraphSHA256  string            `json:"original_graph_sha256"`
	OriginalDentrySHA256 string            `json:"original_dentry_sha256"`
	OperationBirthSHA256 string            `json:"operation_birth_sha256"`
	InodeBirthSHA256     string            `json:"inode_birth_sha256"`
	BodySHA256           string            `json:"body_sha256"`
	CapacityUsed         int64             `json:"capacity_used"`
	OldEpoch             uint64            `json:"old_epoch"`
	FreeEpoch            uint64            `json:"free_epoch"`
	PeerEpoch            uint64            `json:"peer_epoch"`
	ReleasedEpoch        uint64            `json:"released_epoch"`
	OwnerEpoch           uint64            `json:"owner_epoch"`
	OwnerHeld            bool              `json:"owner_held"`
	OwnerHeldBy          string            `json:"owner_held_by"`
	ForeignBefore        uint64            `json:"foreign_before"`
	Attempts             int               `json:"attempts"`
	FactoryCalls         int               `json:"factory_calls"`
	AdapterChecks        int               `json:"adapter_checks"`
	Renames              int               `json:"renames"`
	MutationCalls        int               `json:"mutation_calls"`
	ServerRecordCount    int64             `json:"server_record_count"`
	DispositionState     string            `json:"disposition_state"`
}

type replacementProcessGrantReady struct {
	PID              int    `json:"pid"`
	RowID            int64  `json:"row_id"`
	OperationID      string `json:"operation_id"`
	ProxyIncarnation string `json:"proxy_incarnation"`
	Epoch            uint64 `json:"epoch"`
	ForeignBefore    uint64 `json:"foreign_before"`
}

type replacementProcessAuthorityDurable struct {
	Path, SpoolFile, ImageSHA256 string
	Size                         int64
	Publication                  metadata.SpoolReplacementPublication
	Witness                      metadata.SpoolReplacementWitness
}

func replacementProcessAuthoritySnapshot(t *testing.T, root string, id int64) replacementProcessAuthorityDurable {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.db"), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	index := metadata.NewSpoolStore(db)
	row, err := index.Get(id)
	if err != nil || row == nil {
		t.Fatalf("read-only row: %+v %v", row, err)
	}
	p, err := index.ReplacementPublication(row)
	if err != nil || p == nil {
		t.Fatalf("read-only publication: %+v %v", p, err)
	}
	w, err := index.ReplacementWitness(row)
	if err != nil || w == nil {
		t.Fatalf("read-only witness: %+v %v", w, err)
	}
	return replacementProcessAuthorityDurable{Path: row.NFSPath, SpoolFile: row.SpoolFile,
		ImageSHA256: fmt.Sprintf("%x", row.SHA256), Size: row.Size, Publication: *p, Witness: *w}
}

func replacementProcessAuthorityDisposition(t *testing.T, root string, id int64) *metadata.ReplacementDisposition {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.db"), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d, err := metadata.NewSpoolStore(db).ReplacementDisposition(id)
	if err != nil || d == nil {
		t.Fatalf("read-only replacement disposition: %+v %v", d, err)
	}
	return d
}

// The parent writes a plan, never authority. The child derives publication
// authority exclusively from the reopened journal, witness and proxy replies.
func replacementProcessAuthorityPlan(t *testing.T, root string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "authority-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Case string `json:"case"`
	}
	if err := json.Unmarshal(b, &p); err != nil || (p.Case != "peer-held" && p.Case != "foreign-history" && p.Case != "peer-release-fresh" && p.Case != "grant-unobserved") {
		t.Fatalf("authority plan: %+v %v", p, err)
	}
	return p.Case
}

func replacementProcessPeerStatus(t *testing.T, peer *redis.Client) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	v, err := peer.Do(ctx, "JM.LEASE", "STATUS", "path:media/project").Result()
	a, ok := v.([]any)
	if err != nil || !ok || len(a) != 30 {
		t.Fatalf("authenticated peer STATUS: %T %v", v, err)
	}
	out := make(map[string]string, len(a)/2)
	for i := 0; i < len(a); i += 2 {
		key, ok := a[i].(string)
		if !ok {
			t.Fatalf("peer STATUS key %T", a[i])
		}
		out[key] = fmt.Sprint(a[i+1])
	}
	if out["proj"] != "4" {
		t.Fatalf("peer STATUS project: %v", out)
	}
	return out
}

func replacementProcessStatusEpoch(t *testing.T, state map[string]string, holder, status string, epoch uint64) {
	t.Helper()
	got, err := strconv.ParseUint(state["epoch"], 10, 64)
	if err != nil || got != epoch || state["holder"] != holder || state["state"] != status {
		t.Fatalf("lease status: %v, epoch error %v", state, err)
	}
}

func replacementProcessAssertRefusedAuthority(t *testing.T, which string, before, after map[string]string) {
	t.Helper()
	replacementProcessStatusEpoch(t, before, "mac-peer", "held", 3)
	if which == "peer-held" {
		replacementProcessStatusEpoch(t, after, "mac-peer", "held", 3)
		for _, key := range []string{"holder", "kind", "host", "epoch", "mode", "granted_ms", "checkout_until_ms"} {
			if before[key] == "" || before[key] != after[key] {
				t.Fatalf("peer tenure changed %s: %q -> %q", key, before[key], after[key])
			}
		}
	} else {
		replacementProcessStatusEpoch(t, after, "mac-test", "held", 5)
	}
}

func replacementProcessStageGraph(t *testing.T, rdb *redis.Client, ready replacementProcessReady) [3]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	attr, err := rdb.Get(ctx, fmt.Sprintf("i%d", ready.StageInode)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	dentry, err := rdb.HGet(ctx, "d4", path.Base(ready.StagePath)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := rdb.LRange(ctx, fmt.Sprintf("c%d_0", ready.StageInode), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return [3]string{replacementProcessHash(attr), replacementProcessHash(dentry), replacementProcessGraphHash(chunks)}
}

func replacementProcessRefuse(t *testing.T, root, raw string) {
	t.Helper()
	which := replacementProcessAuthorityPlan(t, root)
	role := os.Getenv("JM_REPLACEMENT_PROCESS_ROLE")
	if which == "grant-unobserved" && role != "grant-cut" && role != "grant-resume" {
		t.Fatal("missing-observation fixture requires a grant process role")
	}
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
		t.Fatalf("reopened row: %+v %v", row, err)
	}
	w, err := spool.Meta().ReplacementWitness(row)
	if err != nil || w == nil {
		t.Fatalf("reopened witness: %+v %v", w, err)
	}
	publication, err := spool.Meta().ReplacementPublication(row)
	if err != nil || publication == nil || publication.Phase != metadata.ReplacementPublicationIntent || publication.Revision != 2 || publication.OperationID != w.OperationID || publication.Record != "" {
		t.Fatalf("reopened intent: %+v %v", publication, err)
	}
	image, err := os.ReadFile(row.SpoolFile)
	if err != nil || !bytes.Equal(image, replacementProcessBody()) {
		t.Fatalf("private image: %v", err)
	}
	x := &boundCacheFixture{raw: raw, volumeUUID: replacementProcessUUID, db: filepath.Join(root, "metadata.db"), state: filepath.Join(root, "state"), fuseRoot: filepath.Join(root, "fuse"), want: bytes.Repeat([]byte{'B'}, 65536), store: store}
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
	// Only the proxy/engine starts here. No recovered owner client or queue may
	// heartbeat until the real sweep and peer tenure have completed.
	rt := newReplacementRuntimeWithSweep(t, x, filepath.Join(root, "objects"), true, 50*time.Millisecond)
	peerURL, err := url.Parse(rt.url)
	if err != nil {
		t.Fatal(err)
	}
	peerURL.User = url.UserPassword("mac-peer", "mac-secret")
	peerOpts, err := redisurl.Parse(peerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	peerOpts.Protocol, peerOpts.DisableIdentity, peerOpts.ContextTimeoutEnabled, peerOpts.MaxRetries = 2, true, true, 1
	peer := redis.NewClient(peerOpts)
	t.Cleanup(func() { _ = peer.Close() })
	var peerHeld map[string]string
	var peerEpoch, releasedEpoch uint64
	if role == "grant-resume" {
		peerHeld = replacementProcessPeerStatus(t, peer)
		replacementProcessStatusEpoch(t, peerHeld, "mac-test", "held", 5)
		peerEpoch, releasedEpoch = 3, 4
	} else {
		old := replacementProcessPeerStatus(t, peer)
		replacementProcessStatusEpoch(t, old, "mac-test", "held", 1)
		var free map[string]string
		until := time.Now().Add(10 * time.Second)
		for time.Now().Before(until) {
			free = replacementProcessPeerStatus(t, peer)
			if free["state"] == "free" {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		replacementProcessStatusEpoch(t, free, "", "free", 2)
		ctxPeer, cancelPeer := context.WithTimeout(context.Background(), 3*time.Second)
		grantRaw, err := peer.Do(ctxPeer, "JM.LEASE", "ACQUIRE", "path:media/project", "checkout", "60").Result()
		cancelPeer()
		grant, ok := grantRaw.([]any)
		if err != nil || !ok || len(grant) != 5 || grant[0] != "GRANTED" {
			t.Fatalf("peer checkout: %v %v", grantRaw, err)
		}
		peerEpoch, ok = respUint(grant[1])
		if !ok || peerEpoch != 3 {
			t.Fatalf("peer checkout epoch: %v", grantRaw)
		}
		peerHeld = replacementProcessPeerStatus(t, peer)
		replacementProcessStatusEpoch(t, peerHeld, "mac-peer", "held", 3)
	}
	if which == "foreign-history" || role == "grant-cut" {
		ctxRelease, cancelRelease := context.WithTimeout(context.Background(), 3*time.Second)
		releaseRaw, err := peer.Do(ctxRelease, "JM.LEASE", "RELEASE", "path:media/project", "3").Result()
		cancelRelease()
		release, ok := releaseRaw.([]any)
		if err != nil || !ok || len(release) != 2 || release[0] != "RELEASED" {
			t.Fatalf("peer release: %v %v", releaseRaw, err)
		}
		releasedEpoch, ok = respUint(release[1])
		if !ok || releasedEpoch != 4 {
			t.Fatalf("peer release epoch: %v", releaseRaw)
		}
		replacementProcessStatusEpoch(t, replacementProcessPeerStatus(t, peer), "", "free", 4)
	}
	// Keep the authenticated peer transport open only for STATUS; it sends no
	// heartbeat. In particular, no old-owner RELEASE is sent.
	lease, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Stop)
	if role == "grant-resume" {
		ctxRenew, cancelRenew := context.WithTimeout(context.Background(), 3*time.Second)
		reply, err := lease.rdb.Do(ctxRenew, "JM.LEASE", "ACQUIRE", "path:media/project", "auto").Result()
		cancelRenew()
		parts, ok := reply.([]any)
		if err != nil || !ok || len(parts) != 5 || parts[0] != "RENEWED" {
			t.Fatalf("new process did not renew epoch: %v %v", reply, err)
		}
		epoch, epochOK := respUint(parts[1])
		foreign, foreignOK := respUint(parts[4])
		if !epochOK || !foreignOK || epoch != 5 || foreign != 0 {
			t.Fatalf("new process renewal must lose local foreign observation: %v", reply)
		}
	}
	lease.Start()
	d.SetProjectLeases(lease)
	t.Cleanup(func() {
		if !h.StopHandler() {
			fmt.Fprintln(os.Stderr, "authority refusal handler did not join; retaining dependencies")
			os.Exit(2)
		}
		lease.Stop()
	})
	x.session.SetOnline(true)
	ctxQualify, cancelQualify := context.WithTimeout(context.Background(), 30*time.Second)
	err = x.session.Qualify(ctxQualify)
	cancelQualify()
	if err != nil {
		t.Fatal(err)
	}
	backend := &replacementSDKIO{fs: rt.fs, m: rt.m, w: *w, ctx: jfsmeta.Background(), parent: 4}
	stage, err := backend.Stat(path.Base(publication.StagePath))
	if err != nil || stage.Length != 1<<20 {
		t.Fatalf("reopened stage: %+v %v", stage, err)
	}
	readyData, err := os.ReadFile(filepath.Join(root, "ready.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ready replacementProcessReady
	if err := json.Unmarshal(readyData, &ready); err != nil {
		t.Fatal(err)
	}
	opBirth, inodeBirth := replacementProcessBirthHashes(t, x, w.OperationID, stage.Inode)
	ctxBirth, cancelBirth := context.WithTimeout(context.Background(), 3*time.Second)
	birthBytes, err := x.rdb.Get(ctxBirth, replaceguard.BirthOperationKey(w.OperationID)).Bytes()
	cancelBirth()
	if err != nil {
		t.Fatal(err)
	}
	born, err := replaceguard.DecodeBirth(birthBytes)
	if err != nil || born.Epoch != 1 || born.Owner != "mac-test" || born.Proxy != ready.ProxyIncarnation {
		t.Fatalf("reopened birth authority: %+v %v", born, err)
	}
	attr, graph, dentry := replacementProcessOriginalHashes(t, x)
	if ready.PID == os.Getpid() || ready.RowID != row.ID || ready.OperationID != w.OperationID || ready.StagePath != publication.StagePath || ready.StageInode != stage.Inode || ready.StageLength != stage.Length || ready.ProxyIncarnation == rt.proxy.Incarnation() || ready.OperationBirthSHA256 != opBirth || ready.InodeBirthSHA256 != inodeBirth || ready.OriginalAttrSHA256 != attr || ready.OriginalGraphSHA256 != graph || ready.OriginalDentrySHA256 != dentry || ready.BodySHA256 != replacementProcessHash(image) {
		t.Fatal("reopened publication or original graph differs from killed generation")
	}
	ctxRecord, cancelRecord := context.WithTimeout(context.Background(), 3*time.Second)
	records, err := x.rdb.Exists(ctxRecord, replaceguard.Key(stage.Inode)).Result()
	cancelRecord()
	if err != nil || records != 0 {
		t.Fatalf("unexpected ARM record: %d %v", records, err)
	}
	decision, err := lease.EnsureForDrain(projectOf(row.NFSPath))
	if err != nil {
		t.Fatal(err)
	}
	if which == "peer-held" && (decision.held || decision.heldBy != "mac-peer@peer-host" || decision.epoch != 3) {
		t.Fatalf("peer-held recovered decision: %+v", decision)
	}
	if which == "foreign-history" && (!decision.held || decision.epoch != 5 || decision.foreignBefore != 5 || !decision.taints(w.AcceptedEpoch)) {
		t.Fatalf("foreign-history recovered decision: %+v accepted=%d", decision, w.AcceptedEpoch)
	}
	if role == "grant-cut" {
		if !decision.held || decision.epoch != 5 || decision.foreignBefore != 5 {
			t.Fatalf("cut must follow actual foreign owner grant: %+v", decision)
		}
		if disposition, err := spool.Meta().ReplacementDisposition(row.ID); err != nil || disposition != nil {
			t.Fatalf("grant cut already observed locally: %+v %v", disposition, err)
		}
		replacementProcessReceipt(t, root, "grant-ready.json", replacementProcessGrantReady{
			PID: os.Getpid(), RowID: row.ID, OperationID: w.OperationID,
			ProxyIncarnation: rt.proxy.Incarnation(), Epoch: decision.epoch, ForeignBefore: decision.foreignBefore,
		})
		replacementProcessReceipt(t, root, "grant-ready.done", struct{}{})
		select {} // Parent SIGKILLs this exact process before any local refusal CAS.
	}
	if role == "grant-resume" {
		if !decision.held || decision.epoch != 5 || decision.foreignBefore != 0 {
			t.Fatalf("fresh process unexpectedly inherited local foreign observation: %+v", decision)
		}
		if disposition, err := spool.Meta().ReplacementDisposition(row.ID); err != nil || disposition != nil {
			t.Fatalf("missing-observation cut was locally classified: %+v %v", disposition, err)
		}
	}
	readOnly := &replacementProcessReadOnlyIO{replacementPublishIO: backend}
	done := make(chan error, 8)
	attempts := 0
	factoryCalls := 0
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		err := h.resumeQueuedReplacement(ctx, d, id, func(got metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
			factoryCalls++
			if got.OperationID != w.OperationID {
				return nil, nil, metadata.ErrMirrorConflict
			}
			return readOnly, func() {}, nil
		})
		attempts++
		select {
		case done <- err:
		default:
		}
		return err
	}
	d.Start()
	select {
	case err := <-done:
		if !errors.Is(err, metadata.ErrMirrorConflict) {
			t.Fatalf("authority refusal callback: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("authority refusal callback did not complete")
	}
	if !d.Stop(5 * time.Second) {
		t.Fatal("authority refusal drainer did not join")
	}
	if which == "peer-release-fresh" {
		lease.Stop() // Join the old client before the peer releases its lease.
		waiting, err := spool.Meta().ReplacementDisposition(row.ID)
		if err != nil || waiting == nil || waiting.State != metadata.ReplacementWaitingWriter || waiting.ObservedEpoch != 3 {
			t.Fatalf("first callback did not durably observe peer: %+v %v", waiting, err)
		}
		ctxRelease, cancelRelease := context.WithTimeout(context.Background(), 3*time.Second)
		reply, err := peer.Do(ctxRelease, "JM.LEASE", "RELEASE", "path:media/project", "3").Result()
		cancelRelease()
		parts, ok := reply.([]any)
		if err != nil || !ok || len(parts) != 2 || parts[0] != "RELEASED" {
			t.Fatalf("peer release after wait: %v %v", reply, err)
		}
		releasedEpoch, ok = respUint(parts[1])
		if !ok || releasedEpoch != 4 {
			t.Fatalf("peer release epoch: %v", reply)
		}
		if n, err := spool.Meta().ReleaseProjectHold(projectOf(row.NFSPath)); err != nil || n != 1 {
			t.Fatalf("release schedule hold: %d %v", n, err)
		}
		still, err := spool.Meta().ReplacementDisposition(row.ID)
		if err != nil || !reflect.DeepEqual(still, waiting) {
			t.Fatalf("schedule release lost peer evidence: %+v %v", still, err)
		}
		first, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(first.Stop)
		first.Start()
		firstGrant, err := first.EnsureForDrain(projectOf(row.NFSPath))
		if err != nil || !firstGrant.held || firstGrant.epoch != 5 || firstGrant.foreignBefore != 5 {
			t.Fatalf("owner grant after peer: %+v %v", firstGrant, err)
		}
		first.Stop() // Stop retains the actual lease; no owner RELEASE is sent.
		fresh, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: rt.url, Spool: spool})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(fresh.Stop)
		ctxFresh, cancelFresh := context.WithTimeout(context.Background(), 3*time.Second)
		freshReply, err := fresh.rdb.Do(ctxFresh, "JM.LEASE", "ACQUIRE", "path:media/project", "auto").Result()
		cancelFresh()
		freshParts, ok := freshReply.([]any)
		if err != nil || !ok || len(freshParts) != 5 || freshParts[0] != "RENEWED" {
			t.Fatalf("new owner client did not renew held epoch: %v %v", freshReply, err)
		}
		freshEpoch, epochOK := respUint(freshParts[1])
		foreignFlag, flagOK := respUint(freshParts[4])
		if !epochOK || !flagOK || freshEpoch != 5 || foreignFlag != 0 {
			t.Fatalf("new owner raw renewal retained history: %v", freshReply)
		}
		fresh.Start()
		decision, err = fresh.EnsureForDrain(projectOf(row.NFSPath))
		if err != nil || !decision.held || decision.epoch != 5 || decision.foreignBefore != 0 {
			t.Fatalf("fresh owner renewal must lack local foreign history: %+v %v", decision, err)
		}
		next, err := NewDrainer(spool, DrainerConfig{FuseRoot: x.fuseRoot, ReplacementRecovery: true})
		if err != nil {
			t.Fatal(err)
		}
		h.SetSpool(spool, next)
		next.SetProjectLeases(fresh)
		next.replacementDispatch.resume = func(ctx context.Context, id int64) error {
			err := h.resumeQueuedReplacement(ctx, next, id, func(got metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				factoryCalls++
				if got.OperationID != w.OperationID {
					return nil, nil, metadata.ErrMirrorConflict
				}
				return readOnly, func() {}, nil
			})
			attempts++
			select {
			case done <- err:
			default:
			}
			return err
		}
		t.Cleanup(func() {
			if !next.Stop(5 * time.Second) {
				t.Error("fresh drainer cleanup did not join")
			}
		})
		next.Start()
		select {
		case err := <-done:
			if !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatalf("fresh owner refusal: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("fresh owner callback did not complete")
		}
		if !next.Stop(5 * time.Second) {
			t.Fatal("fresh owner drainer did not join")
		}
		fresh.Stop()
		d = next
	}
	authorityAfter := replacementProcessPeerStatus(t, peer)
	if role == "grant-resume" {
		replacementProcessStatusEpoch(t, authorityAfter, "mac-test", "held", 5)
	} else {
		replacementProcessAssertRefusedAuthority(t, which, peerHeld, authorityAfter)
	}
	lease.Stop()
	wantAttempts := 1
	if which == "peer-release-fresh" {
		wantAttempts = 2
	}
	if attempts != wantAttempts || factoryCalls != wantAttempts || readOnly.checkCalls != wantAttempts || readOnly.checkFailures != 0 || backend.renames != 0 || readOnly.mutationCalls != 0 {
		t.Fatalf("refusal attempts=%d factory=%d checks=%d check_failures=%d renames=%d SDK mutations=%d", attempts, factoryCalls, readOnly.checkCalls, readOnly.checkFailures, backend.renames, readOnly.mutationCalls)
	}
	disposition, err := spool.Meta().ReplacementDisposition(row.ID)
	wantState := metadata.ReplacementWaitingWriter
	if which != "peer-held" {
		wantState = metadata.ReplacementWriterChanged
	}
	if err != nil || disposition == nil || disposition.State != wantState || disposition.ObservedEpoch <= w.AcceptedEpoch {
		t.Fatalf("durable refusal disposition: %+v %v", disposition, err)
	}
	after, err := spool.Meta().ReplacementPublication(row)
	if err != nil || !reflect.DeepEqual(publication, after) {
		t.Fatalf("publication mutated: %+v %v", after, err)
	}
	if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, image) {
		t.Fatalf("private image changed: %v", err)
	}
	used, _ := spool.Capacity()
	if used != ready.CapacityUsed {
		t.Fatalf("capacity changed: %d != %d", used, ready.CapacityUsed)
	}
	finalBirth, finalInodeBirth := replacementProcessBirthHashes(t, x, w.OperationID, stage.Inode)
	if finalBirth != opBirth || finalInodeBirth != inodeBirth {
		t.Fatal("birth changed after refusal")
	}
	attr2, graph2, dentry2 := replacementProcessOriginalHashes(t, x)
	if attr2 != attr || graph2 != graph || dentry2 != dentry {
		t.Fatal("original graph changed after refusal")
	}
	ctxFinal, cancelFinal := context.WithTimeout(context.Background(), 3*time.Second)
	records, err = x.rdb.Exists(ctxFinal, replaceguard.Key(stage.Inode)).Result()
	cancelFinal()
	if err != nil || records != 0 {
		t.Fatalf("refusal ARM record: %d %v", records, err)
	}
	statusView, err := BuildSpoolStatus(spool, d)
	if err != nil {
		t.Fatal(err)
	}
	statusJSON, err := json.Marshal(statusView)
	if err != nil {
		t.Fatal(err)
	}
	if len(statusJSON) > 16<<10 {
		t.Fatalf("replacement status too large: %d", len(statusJSON))
	}
	t.Logf("replacement_spool_status=%s", statusJSON)
	if which == "peer-held" && (statusView.ReplacementWaiting != 1 || statusView.ReplacementConflicts != 0) {
		t.Fatalf("waiting status: %+v", statusView)
	}
	if which != "peer-held" && (statusView.ReplacementConflicts != 1 || statusView.ReplacementWaiting != 0) {
		t.Fatalf("conflict status: %+v", statusView)
	}
	foundStatus := false
	for _, entry := range statusView.Entries {
		if entry.Path == row.NFSPath {
			foundStatus = true
			if entry.ReplacementState != wantState || entry.DrainState != string(metadata.DrainReady) || entry.Stalled || entry.LastError == "" {
				t.Fatalf("replacement status entry: %+v", entry)
			}
		}
	}
	if !foundStatus {
		t.Fatal("replacement status omitted retained save")
	}
	assertReplacementOfflineNFSRoute(t, x, spool, row, image, used, true, true)
	replacementProcessReceipt(t, root, "refused.json", replacementProcessRefused{
		Case: which, PID: os.Getpid(), RowID: row.ID, OperationID: w.OperationID,
		PeerBefore: peerHeld, AuthorityAfter: authorityAfter,
		AcceptedEpoch: w.AcceptedEpoch,
		Phase:         after.Phase, Revision: after.Revision, ProxyURL: rt.url, ProxyIncarnation: rt.proxy.Incarnation(),
		StageInode: stage.Inode, StageLength: stage.Length,
		OriginalAttrSHA256: attr2, OriginalGraphSHA256: graph2, OriginalDentrySHA256: dentry2,
		OperationBirthSHA256: finalBirth, InodeBirthSHA256: finalInodeBirth,
		BodySHA256: replacementProcessHash(image), CapacityUsed: used,
		OldEpoch: 1, FreeEpoch: 2, PeerEpoch: peerEpoch, ReleasedEpoch: releasedEpoch,
		OwnerEpoch: decision.epoch, OwnerHeld: decision.held, OwnerHeldBy: decision.heldBy,
		ForeignBefore: decision.foreignBefore, Attempts: attempts, FactoryCalls: factoryCalls,
		AdapterChecks: readOnly.checkCalls, AdapterCheckFailures: readOnly.checkFailures, Renames: backend.renames,
		MutationCalls: readOnly.mutationCalls, ServerRecordCount: records,
		DispositionState: disposition.State,
	})
}

func TestReplacementProcessAuthorityChanges(t *testing.T) {
	for _, which := range []string{"peer-held", "foreign-history", "peer-release-fresh"} {
		t.Run(which, func(t *testing.T) { testReplacementProcessAuthorityChange(t, which) })
	}
}

func replacementProcessAssertNoDisposition(t *testing.T, root string, id int64) {
	t.Helper()
	uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.db"), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d, err := metadata.NewSpoolStore(db).ReplacementDisposition(id)
	if err != nil || d != nil {
		t.Fatalf("grant cut acquired local refusal authority: %+v %v", d, err)
	}
}

func replacementProcessWaitSingletonGone(t *testing.T, rdb *redis.Client, old string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		value, err := rdb.Get(ctx, respproxy.SingletonKey).Result()
		cancel()
		if err == redis.Nil {
			return
		}
		if err != nil || value != old || time.Now().After(deadline) {
			t.Fatalf("old singleton %q: %q %v", old, value, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The first recovery process sees the foreign grant but dies before any local
// disposition CAS. A third process can then renew epoch 5 with foreign_since=0;
// only durable server evidence can classify that otherwise missed tenure.
func TestReplacementProcessMissingForeignObservation(t *testing.T) {
	root := t.TempDir()
	raw := redistestfixture.Start(t)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	opts.ContextTimeoutEnabled, opts.MaxRetries = true, -1
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	runID := replacementProcessRunID(t, rdb)
	seed := launchReplacementProcess(t, root, raw, "seed", replacementProcessFirstChunk)
	ready := awaitReplacementProcessReady(t, root, seed, replacementProcessFirstChunk)
	verifyReplacementProcessCut(t, root, rdb, ready)
	initial := replacementProcessAuthoritySnapshot(t, root, ready.RowID)
	stageGraph := replacementProcessStageGraph(t, rdb, ready)
	if err := seed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-seed.done:
	case <-time.After(10 * time.Second):
		t.Fatal("seed SIGKILL did not join")
	}
	var exited *exec.ExitError
	if !errors.As(seed.err, &exited) {
		t.Fatalf("seed did not exit by signal: %v", seed.err)
	}
	waitStatus, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("seed exit: %v", seed.err)
	}
	checkReplacementProcessTranscript(t, seed, "seed")
	replacementProcessWaitSingletonGone(t, rdb, ready.ProxyIncarnation)
	ctxCfg, cancelCfg := context.WithTimeout(context.Background(), 3*time.Second)
	err = rdb.HSet(ctxCfg, respproxy.ConfigKey, "orphan_s", "5").Err()
	cancelCfg()
	if err != nil {
		t.Fatal(err)
	}
	replacementProcessReceipt(t, root, "authority-plan.json", struct {
		Case string `json:"case"`
	}{"grant-unobserved"})
	grant := launchReplacementProcess(t, root, raw, "grant-cut", replacementProcessFirstChunk)
	var cut replacementProcessGrantReady
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(filepath.Join(root, "grant-ready.done")); err == nil {
			data, err := os.ReadFile(filepath.Join(root, "grant-ready.json"))
			if err != nil || json.Unmarshal(data, &cut) != nil {
				t.Fatalf("grant-ready receipt: %v", err)
			}
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-grant.done:
			t.Fatalf("grant child exited before cut: %v\n%s", grant.err, grant.diagnostic(t))
		case <-deadline.C:
			t.Fatalf("grant child did not reach cut: %s", grant.diagnostic(t))
		case <-tick.C:
		}
	}
	if cut.PID != grant.cmd.Process.Pid || cut.PID == ready.PID || cut.RowID != ready.RowID || cut.OperationID != ready.OperationID || cut.Epoch != 5 || cut.ForeignBefore != 5 || cut.ProxyIncarnation == ready.ProxyIncarnation {
		t.Fatalf("grant cut identity: %+v", cut)
	}
	replacementProcessAssertNoDisposition(t, root, ready.RowID)
	verifyReplacementProcessCut(t, root, rdb, ready)
	if after := replacementProcessStageGraph(t, rdb, ready); after != stageGraph {
		t.Fatalf("grant cut mutated stage graph: %v != %v", after, stageGraph)
	}
	if err := grant.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-grant.done:
	case <-time.After(10 * time.Second):
		t.Fatal("grant SIGKILL did not join")
	}
	if !errors.As(grant.err, &exited) {
		t.Fatalf("grant did not exit by signal: %v", grant.err)
	}
	waitStatus, ok = exited.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("grant exit: %v", grant.err)
	}
	checkReplacementProcessTranscript(t, grant, "grant-cut")
	replacementProcessAssertNoDisposition(t, root, ready.RowID)
	replacementProcessWaitSingletonGone(t, rdb, cut.ProxyIncarnation)
	ctxCfg, cancelCfg = context.WithTimeout(context.Background(), 3*time.Second)
	err = rdb.HSet(ctxCfg, respproxy.ConfigKey, "orphan_s", "30").Err()
	cancelCfg()
	if err != nil {
		t.Fatal(err)
	}
	resume := launchReplacementProcess(t, root, raw, "grant-resume", replacementProcessFirstChunk)
	select {
	case <-resume.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("third process did not join: %s", resume.diagnostic(t))
	}
	if resume.err != nil {
		t.Fatalf("third process: %v\n%s", resume.err, resume.diagnostic(t))
	}
	checkReplacementProcessTranscript(t, resume, "grant-resume")
	data, err := os.ReadFile(filepath.Join(root, "refused.json"))
	if err != nil {
		t.Fatal(err)
	}
	var refusal replacementProcessRefused
	if err := json.Unmarshal(data, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Case != "grant-unobserved" || refusal.PID != resume.cmd.Process.Pid || refusal.PID == cut.PID || refusal.RowID != ready.RowID || refusal.OperationID != ready.OperationID || refusal.OwnerEpoch != 5 || !refusal.OwnerHeld || refusal.ForeignBefore != 0 || refusal.DispositionState != metadata.ReplacementWriterChanged || refusal.Attempts != 1 || refusal.FactoryCalls != 1 || refusal.AdapterChecks != 1 || refusal.AdapterCheckFailures != 0 || refusal.MutationCalls != 0 || refusal.Renames != 0 || refusal.ServerRecordCount != 0 || refusal.BodySHA256 != ready.BodySHA256 || refusal.CapacityUsed != ready.CapacityUsed {
		t.Fatalf("missing-observation refusal: %+v", refusal)
	}
	if after := replacementProcessAuthoritySnapshot(t, root, ready.RowID); !reflect.DeepEqual(after, initial) {
		t.Fatal("accepted image or journal changed across both crashes")
	}
	if after := replacementProcessStageGraph(t, rdb, ready); after != stageGraph {
		t.Fatalf("stage graph changed: %v != %v", after, stageGraph)
	}
	if replacementProcessRunID(t, rdb) != runID {
		t.Fatal("Redis generation changed")
	}
	t.Logf("seed_pid=%d grant_pid=%d resume_pid=%d, two SIGKILL/Wait boundaries, fresh RENEWED(5,0), persisted writer_changed, SDK mutations=0", ready.PID, cut.PID, refusal.PID)
}

func testReplacementProcessAuthorityChange(t *testing.T, which string) {
	t.Helper()
	root := t.TempDir()
	raw := redistestfixture.Start(t)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	opts.ContextTimeoutEnabled, opts.MaxRetries = true, -1
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	runID := replacementProcessRunID(t, rdb)
	seed := launchReplacementProcess(t, root, raw, "seed", replacementProcessFirstChunk)
	ready := awaitReplacementProcessReady(t, root, seed, replacementProcessFirstChunk)
	verifyReplacementProcessCut(t, root, rdb, ready)
	durable := replacementProcessAuthoritySnapshot(t, root, ready.RowID)
	stageGraph := replacementProcessStageGraph(t, rdb, ready)
	dirs := map[string]os.FileInfo{}
	for _, name := range []string{".", "objects", "spool", "state"} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		dirs[name] = info
	}
	if err := seed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-seed.done:
	case <-time.After(10 * time.Second):
		t.Fatal("killed seed Wait did not join")
	}
	var exited *exec.ExitError
	if !errors.As(seed.err, &exited) {
		t.Fatalf("seed not signaled: %v", seed.err)
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("seed exit: %v", seed.err)
	}
	checkReplacementProcessTranscript(t, seed, "seed")
	verifyReplacementProcessCut(t, root, rdb, ready)
	u, err := url.Parse(ready.ProxyURL)
	if err != nil {
		t.Fatal(err)
	}
	if conn, err := net.DialTimeout("tcp", u.Host, 200*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("killed proxy still listening")
	}
	until := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		v, err := rdb.Get(ctx, respproxy.SingletonKey).Result()
		cancel()
		if err == redis.Nil {
			break
		}
		if err != nil || v != ready.ProxyIncarnation {
			t.Fatalf("old singleton: %q %v", v, err)
		}
		if time.Now().After(until) {
			t.Fatal("old singleton did not expire")
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctxCfg, cancelCfg := context.WithTimeout(context.Background(), 3*time.Second)
	// Leave time for the fresh SDK constructor to finish before the first
	// authenticated STATUS. The loaded orphan window is still test-bounded.
	err = rdb.HSet(ctxCfg, respproxy.ConfigKey, "orphan_s", "5").Err()
	cancelCfg()
	if err != nil {
		t.Fatal(err)
	}
	replacementProcessReceipt(t, root, "authority-plan.json", struct {
		Case string `json:"case"`
	}{which})
	child := launchReplacementProcess(t, root, raw, "refuse", replacementProcessFirstChunk)
	select {
	case <-child.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("refusal child did not join\n%s", child.diagnostic(t))
	}
	if child.err != nil {
		t.Fatalf("refusal child: %v\n%s", child.err, child.diagnostic(t))
	}
	checkReplacementProcessTranscript(t, child, "refuse")
	data, err := os.ReadFile(filepath.Join(root, "refused.json"))
	if err != nil {
		t.Fatal(err)
	}
	var refusal replacementProcessRefused
	if err := json.Unmarshal(data, &refusal); err != nil {
		t.Fatal(err)
	}
	wantAttempts := 1
	if which == "peer-release-fresh" {
		wantAttempts = 2
	}
	if refusal.Case != which || refusal.PID != child.cmd.Process.Pid || refusal.PID == ready.PID || refusal.RowID != ready.RowID || refusal.OperationID != ready.OperationID || refusal.AcceptedEpoch != durable.Witness.AcceptedEpoch || refusal.Phase != metadata.ReplacementPublicationIntent || refusal.Revision != 2 || refusal.ProxyURL == "" || refusal.ProxyIncarnation == "" || refusal.ProxyIncarnation == ready.ProxyIncarnation || refusal.StageInode != ready.StageInode || refusal.StageLength != ready.StageLength || refusal.OriginalAttrSHA256 != ready.OriginalAttrSHA256 || refusal.OriginalGraphSHA256 != ready.OriginalGraphSHA256 || refusal.OriginalDentrySHA256 != ready.OriginalDentrySHA256 || refusal.OperationBirthSHA256 != ready.OperationBirthSHA256 || refusal.InodeBirthSHA256 != ready.InodeBirthSHA256 || refusal.BodySHA256 != ready.BodySHA256 || refusal.CapacityUsed != ready.CapacityUsed || refusal.OldEpoch != 1 || refusal.FreeEpoch != 2 || refusal.PeerEpoch != 3 || refusal.Attempts != wantAttempts || refusal.FactoryCalls != wantAttempts || refusal.AdapterChecks != wantAttempts || refusal.AdapterCheckFailures != 0 || refusal.Renames != 0 || refusal.MutationCalls != 0 || refusal.ServerRecordCount != 0 {
		t.Fatalf("refusal receipt: %+v", refusal)
	}
	if which == "peer-held" && (refusal.ReleasedEpoch != 0 || refusal.OwnerHeld || refusal.OwnerEpoch != 3 || refusal.OwnerHeldBy != "mac-peer@peer-host" || refusal.ForeignBefore != 0) {
		t.Fatalf("peer-held receipt: %+v", refusal)
	}
	if which == "foreign-history" && (refusal.ReleasedEpoch != 4 || !refusal.OwnerHeld || refusal.OwnerEpoch != 5 || refusal.ForeignBefore != 5 || refusal.OwnerHeldBy != "") {
		t.Fatalf("foreign-history receipt: %+v", refusal)
	}
	if which == "peer-release-fresh" && (refusal.ReleasedEpoch != 4 || !refusal.OwnerHeld || refusal.OwnerEpoch != 5 || refusal.ForeignBefore != 0 || refusal.OwnerHeldBy != "") {
		t.Fatalf("fresh owner receipt: %+v", refusal)
	}
	if refusal.DispositionState != map[string]string{"peer-held": metadata.ReplacementWaitingWriter, "foreign-history": metadata.ReplacementWriterChanged, "peer-release-fresh": metadata.ReplacementWriterChanged}[which] {
		t.Fatalf("refusal disposition receipt: %+v", refusal)
	}
	storedDisposition := replacementProcessAuthorityDisposition(t, root, ready.RowID)
	if storedDisposition.State != refusal.DispositionState || storedDisposition.OperationID != ready.OperationID || storedDisposition.ObservedEpoch <= durable.Witness.AcceptedEpoch || storedDisposition.JournalRevision != durable.Publication.Revision {
		t.Fatalf("independent durable disposition: %+v", storedDisposition)
	}
	verifyReplacementProcessCut(t, root, rdb, ready)
	replacementProcessAssertRefusedAuthority(t, which, refusal.PeerBefore, refusal.AuthorityAfter)
	if after := replacementProcessStageGraph(t, rdb, ready); after != stageGraph {
		t.Fatalf("partial stage graph changed: %v != %v", after, stageGraph)
	}
	t.Logf("unchanged partial stage attr/dentry/chunks SHA256: %v", stageGraph)
	if after := replacementProcessAuthoritySnapshot(t, root, ready.RowID); !reflect.DeepEqual(after, durable) {
		t.Fatal("accepted image, witness or publication intent changed across authority refusal")
	}
	if replacementProcessRunID(t, rdb) != runID {
		t.Fatal("Redis generation changed")
	}
	for name, before := range dirs {
		now, err := os.Stat(filepath.Join(root, name))
		if err != nil || !os.SameFile(before, now) {
			t.Fatalf("retained directory %s: %v", name, err)
		}
	}
	t.Logf("case=%s seed_pid=%d SIGKILL/Wait refusal_pid=%d exit=0; epoch sequence 1->2->3->%d->%d; new proxy=%s; queued conflict, mutations=0, unchanged image=%d bytes", which, ready.PID, refusal.PID, refusal.ReleasedEpoch, refusal.OwnerEpoch, refusal.ProxyIncarnation, len(replacementProcessBody()))
}
