package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

type heldOwnerSnapshot struct {
	once             sync.Once
	entered, release chan struct{}
}

func (h *heldOwnerSnapshot) OwnerSnapshot() health.FUSEOwnerSnapshot {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return health.FUSEOwnerSnapshot{MountPath: "/new/physical", Attempted: true}
}

func TestVolumeOwnerStatusDoesNotWaitForPhysicalSnapshot(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "selected-config"
	o.manager = health.NewFUSEManager(health.FUSEConfig{MountPoint: "/original/physical"})
	o.snapshot = health.FUSEOwnerSnapshot{MountPath: "/original/physical"}
	held := &heldOwnerSnapshot{entered: make(chan struct{}), release: make(chan struct{})}
	const id = "1234567890abcdef1234567890abcdef"
	workRelease := make(chan struct{})
	var workOnce sync.Once
	if _, err := o.begin("mount", id, o.config, func() (volumeOwnerReceipt, error) {
		<-workRelease
		return volumeOwnerReceipt{MountPath: "/original/physical"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		workOnce.Do(func() { close(workRelease) })
		o.mu.Lock()
		op := o.ops[id]
		o.mu.Unlock()
		select {
		case <-op.done:
		case <-time.After(time.Second):
			t.Error("original work not joined")
		}
	}()
	o.status = held
	producerDone := make(chan struct{})
	go func() { o.publishSnapshot(held, true); close(producerDone) }()
	var snapshotRelease sync.Once
	defer func() {
		snapshotRelease.Do(func() { close(held.release) })
		select {
		case <-producerDone:
		case <-time.After(time.Second):
			t.Error("snapshot producer not joined")
		}
	}()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot producer did not enter")
	}
	readDone := make(chan error, 1)
	go func() {
		r, err := o.inspect(o.instance, o.config)
		if err == nil && r.MountPath != "/original/physical" {
			err = fmt.Errorf("published snapshot changed before physical producer returned")
		}
		if err == nil {
			r, err = o.query(o.instance, id, o.config)
			if err == nil && r.OperationState != "pending" {
				err = fmt.Errorf("pending original operation disappeared")
			}
		}
		readDone <- err
	}()
	readJoined := false
	defer func() {
		snapshotRelease.Do(func() { close(held.release) })
		if !readJoined {
			select {
			case <-readDone:
			case <-time.After(time.Second):
				t.Error("status reader not joined")
			}
		}
	}()
	select {
	case err := <-readDone:
		readJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("inspect/query blocked on the physical snapshot producer")
	}
}

func TestVolumeOwnerSocketReconnectQueriesOriginalPendingOperation(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "selected-config"
	const id = "fedcba9876543210fedcba9876543210"
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	if _, err := o.begin("mount", id, o.config, func() (volumeOwnerReceipt, error) {
		close(entered)
		<-release
		return volumeOwnerReceipt{Instance: o.instance, Build: version.Commit,
			IsolationRoot: o.root, ConfigDigest: o.config, MountPath: "/original"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		once.Do(func() { close(release) })
		o.mu.Lock()
		op := o.ops[id]
		o.mu.Unlock()
		select {
		case <-op.done:
		case <-time.After(time.Second):
			t.Error("original work not joined")
		}
	}()
	// The private test TMPDIR is supplied by the runner; keep the Unix path
	// short without falling back to a shared system socket directory.
	directory, err := os.MkdirTemp(os.TempDir(), "vo-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "o.sock")
	server, err := servicecontrol.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(o.dispatch) }()
	defer func() {
		_ = server.Close()
		select {
		case <-served:
		case <-time.After(time.Second):
			t.Error("owner listener not joined")
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("original owner work did not enter")
	}
	query := func() volumeOwnerReceipt {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "query", Strings: []string{o.instance, id, o.config}})
		if err != nil || response.Error != "" {
			t.Fatalf("reconnect query failed: %v %s", err, response.Error)
		}
		var r volumeOwnerReceipt
		if err := json.Unmarshal([]byte(response.Text), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := query(); r.OperationState != "pending" {
		t.Fatal("lost client implied original work canceled")
	}
	once.Do(func() { close(release) })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r := query(); r.OperationState == "complete" {
			if r.MountPath != "/original" {
				t.Fatal("replacement result observed")
			}
			return
		}
	}
	t.Fatal("original result not queryable after reconnect")
}

func TestVolumeOwnerDeduplicatesOriginalOperation(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		o.mu.Lock()
		op := o.ops[id]
		o.mu.Unlock()
		if op != nil {
			select {
			case <-op.done:
			case <-time.After(time.Second):
				t.Error("original operation not joined")
			}
		}
	}()
	work := func() (volumeOwnerReceipt, error) {
		close(entered)
		<-release
		return volumeOwnerReceipt{MountPath: "/owned/original"}, nil
	}
	if r, err := o.begin("mount", id, "same-config", work); err != nil || r.OperationState != "pending" {
		t.Fatalf("original operation was not retained: %v %+v", err, r)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("original operation did not start")
	}
	var duplicate atomic.Int32
	if r, err := o.begin("mount", id, "same-config", func() (volumeOwnerReceipt, error) {
		duplicate.Add(1)
		return volumeOwnerReceipt{}, nil
	}); err != nil || r.OperationState != "pending" {
		t.Fatal("reconnected caller did not observe the original pending ID")
	}
	if _, err := o.begin("mount", id, "different-config", work); err == nil {
		t.Fatal("same ID rebound to another configuration")
	}
	releaseOnce.Do(func() { close(release) })
	// The original operation owns completion independently of client replies.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		r, err := o.query(o.instance, id, "")
		if err != nil {
			t.Fatal(err)
		}
		if r.OperationState == "complete" {
			if r.MountPath != "/owned/original" || duplicate.Load() != 0 {
				t.Fatal("replacement result appeared")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("original operation did not settle")
}

func TestVolumeOwnerSelectedInstanceAndConfig(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "original-request-digest"
	if _, err := o.inspect("wrong-instance", o.config); err == nil {
		t.Fatal("foreign instance accepted")
	}
	if _, err := o.inspect(o.instance, "wrong-config"); err == nil {
		t.Fatal("different requested config accepted")
	}
	if r, err := o.inspect(o.instance, o.config); err != nil || r.Instance != o.instance || r.ConfigDigest != o.config {
		t.Fatalf("selected original identity unavailable: %v %+v", err, r)
	}
	if _, err := o.query(o.instance, "unknown", o.config); err == nil {
		t.Fatal("unknown operation accepted")
	}
}

func TestVolumeOwnerMutationRequiresSelectedInstanceAndConfig(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	o.config = "requested-digest"
	for _, strings := range [][]string{
		{"", "0123456789abcdef0123456789abcdef", o.config},
		{o.instance, "0123456789abcdef0123456789abcdef", ""},
		{"foreign-instance", "0123456789abcdef0123456789abcdef", o.config},
		{o.instance, "0123456789abcdef0123456789abcdef", "foreign-config"},
	} {
		resp := o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "mount", Strings: strings})
		if resp.Error == "" || len(o.ops) != 0 {
			t.Fatal("unselected caller admitted a mount operation")
		}
	}
}

type heldOwnerManager struct {
	entered   chan struct{}
	release   chan struct{}
	stops     atomic.Int32
	monitors  atomic.Int32
	stopState health.FUSEStopState
	status    ownerSnapshotSource
}

func (m *heldOwnerManager) Mount() error {
	close(m.entered)
	<-m.release
	return nil
}
func (m *heldOwnerManager) StartMonitor() { m.monitors.Add(1) }
func (m *heldOwnerManager) Stop() health.FUSEStopResult {
	m.stops.Add(1)
	return health.FUSEStopResult{State: m.stopState}
}
func (m *heldOwnerManager) OwnerSnapshot() health.FUSEOwnerSnapshot {
	if m.status != nil {
		return m.status.OwnerSnapshot()
	}
	return health.FUSEOwnerSnapshot{MountPath: "/late/physical"}
}

func TestVolumeOwnerStopAdmittedDuringHeldMount(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &heldOwnerManager{entered: make(chan struct{}), release: make(chan struct{}), stopState: health.FUSEInFlight}
	o.manager, o.status = m, m
	o.config = "original-config"
	o.snapshot = health.FUSEOwnerSnapshot{MountPath: "/original/physical"}
	var released atomic.Int32
	o.releaseLink = func() { released.Add(1) }
	mountDone := make(chan error, 1)
	go func() { _, err := o.mount(); mountDone <- err }()
	var once sync.Once
	mountJoined := false
	defer func() {
		once.Do(func() { close(m.release) })
		if !mountJoined {
			select {
			case <-mountDone:
			case <-time.After(time.Second):
				t.Error("original mount not joined")
			}
		}
	}()
	select {
	case <-m.entered:
	case <-time.After(time.Second):
		t.Fatal("mount did not enter")
	}
	stopDone := make(chan error, 1)
	go func() { _, err := o.stop(); stopDone <- err }()
	stopJoined := false
	defer func() {
		once.Do(func() { close(m.release) })
		if !stopJoined {
			select {
			case <-stopDone:
			case <-time.After(time.Second):
				t.Error("original Stop not joined")
			}
		}
	}()
	select {
	case err := <-stopDone:
		stopJoined = true
		if err == nil || m.stops.Load() != 1 || released.Load() != 0 || o.manager != m {
			t.Fatalf("incomplete Stop lost original resources: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Stop waited behind held Mount")
	}
	once.Do(func() { close(m.release) })
	select {
	case err := <-mountDone:
		mountJoined = true
		if err == nil {
			t.Fatal("late Mount succeeded after Stop admission")
		}
	case <-time.After(time.Second):
		t.Fatal("mount not joined")
	}
	if m.monitors.Load() != 0 || released.Load() != 0 || o.snapshot.MountPath != "/original/physical" {
		t.Fatal("late Mount published or released original resources")
	}
	m.stopState = health.FUSEStopped
	if _, err := o.stop(); err != nil || released.Load() != 1 || o.manager != nil {
		t.Fatalf("completed original Stop did not release Link once: %v", err)
	}
}

func TestVolumeOwnerStopDoesNotWaitForHeldSnapshot(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	held := &heldOwnerSnapshot{entered: make(chan struct{}), release: make(chan struct{})}
	m := &heldOwnerManager{stopState: health.FUSEInFlight, status: held}
	o.manager, o.status = m, m
	o.config = "original-config"
	var released atomic.Int32
	o.releaseLink = func() { released.Add(1) }
	producerDone := make(chan struct{})
	go func() { o.publishSnapshot(m, false); close(producerDone) }()
	var once sync.Once
	defer func() {
		once.Do(func() { close(held.release) })
		select {
		case <-producerDone:
		case <-time.After(time.Second):
			t.Error("snapshot producer not joined")
		}
	}()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot not entered")
	}
	done := make(chan error, 1)
	go func() { _, err := o.stop(); done <- err }()
	stopJoined := false
	defer func() {
		once.Do(func() { close(held.release) })
		if !stopJoined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original Stop not joined")
			}
		}
	}()
	select {
	case err := <-done:
		stopJoined = true
		if err == nil || released.Load() != 0 || o.manager != m {
			t.Fatalf("incomplete Stop lost resources: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Stop waited for physical snapshot")
	}
	once.Do(func() { close(held.release) })
}

func TestVolumeOwnerRetirementRejectsLateSnapshot(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	held := &heldOwnerSnapshot{entered: make(chan struct{}), release: make(chan struct{})}
	m := &heldOwnerManager{stopState: health.FUSEStopped, status: held}
	o.manager, o.status = m, m
	o.snapshot = health.FUSEOwnerSnapshot{MountPath: "/original/physical"}
	done := make(chan struct{})
	go func() { o.publishSnapshot(m, true); close(done) }()
	var once sync.Once
	joined := false
	defer func() {
		once.Do(func() { close(held.release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("late snapshot worker not joined")
			}
		}
	}()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not enter")
	}
	if _, err := o.stop(); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(held.release) })
	select {
	case <-done:
		joined = true
	case <-time.After(time.Second):
		t.Fatal("late snapshot not joined")
	}
	if o.snapshot.MountPath != "/original/physical" || o.version != 0 || !o.retired {
		t.Fatal("retired owner accepted a late manager snapshot")
	}
}

func TestVolumeOwnerStopAdmissionFencesBeforeWorkerRuns(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const stopID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	release := make(chan struct{})
	defer func() {
		close(release)
		o.mu.Lock()
		var originals []*volumeOwnerOperation
		for _, op := range o.ops {
			originals = append(originals, op)
		}
		o.mu.Unlock()
		for _, op := range originals {
			select {
			case <-op.done:
			case <-time.After(time.Second):
				t.Error("original admitted operation not joined")
			}
		}
	}()
	work := func() (volumeOwnerReceipt, error) {
		<-release // deliberately never calls stop(); admission itself must fence.
		return volumeOwnerReceipt{}, nil
	}
	if _, err := o.begin("stop", stopID, "selected-config", work); err != nil {
		t.Fatal(err)
	}
	for i, method := range []string{"mount", "claim"} {
		id := fmt.Sprintf("%032x", i+1)
		if _, err := o.begin(method, id, `{"RedisURL":"selected"}`+"\x000\x001", func() (volumeOwnerReceipt, error) {
			return volumeOwnerReceipt{}, nil
		}); err == nil {
			t.Fatalf("new %s admitted after accepted Stop but before its worker", method)
		}
	}
	if r, err := o.begin("stop", stopID, "selected-config", work); err != nil || r.OperationState != "pending" {
		t.Fatalf("original Stop no longer queryable: %+v %v", r, err)
	}
}

func TestVolumeOwnerMountResultRejectsStopDuringSnapshot(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	held := &heldOwnerSnapshot{entered: make(chan struct{}), release: make(chan struct{})}
	m := &heldOwnerManager{entered: make(chan struct{}), release: make(chan struct{}), stopState: health.FUSEStopped, status: held}
	close(m.release)
	o.manager, o.status = m, m
	o.snapshot = health.FUSEOwnerSnapshot{MountPath: "/original/physical"}
	done := make(chan error, 1)
	go func() { _, err := o.mount(); done <- err }()
	var once sync.Once
	joined := false
	defer func() {
		once.Do(func() { close(held.release) })
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original Mount not joined")
			}
		}
	}()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("post-Mount snapshot did not enter")
	}
	if _, err := o.stop(); err == nil {
		t.Fatal("owner Stop completed before original Mount worker joined")
	}
	once.Do(func() { close(held.release) })
	select {
	case err := <-done:
		joined = true
		if err == nil {
			t.Fatal("Mount reported usable success after Stop during snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("original Mount did not settle")
	}
	if o.snapshot.MountPath != "/original/physical" {
		t.Fatal("late Mount replaced stopped publication")
	}
	if _, err := o.stop(); err != nil {
		t.Fatalf("same-manager Stop retry did not settle: %v", err)
	}
}

func TestVolumeOwnerSelectedStopWhileClaimPending(t *testing.T) {
	o, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const claimID = "abcdefabcdefabcdefabcdefabcdefab"
	const stopID = "12341234123412341234123412341234"
	canonicalBytes, err := json.Marshal(canonicalServerConfig(ServerConfig{RedisURL: "redis://selected"}))
	if err != nil {
		t.Fatal(err)
	}
	canonical := string(canonicalBytes)
	digest, err := servingConfigDigest(ServerConfig{RedisURL: "redis://selected"})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	if _, err := o.begin("claim", claimID, canonical+"\x000\x001", func() (volumeOwnerReceipt, error) {
		<-release
		return volumeOwnerReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		once.Do(func() { close(release) })
		o.mu.Lock()
		op := o.ops[claimID]
		o.mu.Unlock()
		select {
		case <-op.done:
		case <-time.After(time.Second):
			t.Error("original claim not joined")
		}
	}()
	request := func(selected string) servicecontrol.Response {
		return o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "stop", Strings: []string{o.instance, stopID, selected}})
	}
	if response := request("wrong"); response.Error == "" {
		t.Fatal("foreign pending claim selected")
	}
	if o.stopRequested {
		t.Fatal("unselected Stop set admission fence")
	}
	if response := request(digest); response.Error != "" {
		t.Fatalf("selected pending claim Stop refused: %s", response.Error)
	}
	// The selected Stop has its own original ID; without a published manager it
	// retains the latch and returns an incomplete result rather than detaching.
	o.mu.Lock()
	stopOp := o.ops[stopID]
	o.mu.Unlock()
	if stopOp == nil {
		t.Fatal("selected Stop had no retained original operation")
	}
	select {
	case <-stopOp.done:
	case <-time.After(time.Second):
		t.Fatal("selected Stop did not settle")
	}
	if stopOp.err == nil || !o.stopRequested || o.retired {
		t.Fatal("pending claim Stop falsely completed")
	}
	once.Do(func() { close(release) })
	if op := waitClaimIdentityOperation(t, o, claimID); op.err != nil {
		t.Fatalf("held original claim did not settle: %v", op.err)
	}
	const retryID = "56785678567856785678567856785678"
	if response := o.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "stop", Strings: []string{o.instance, retryID, digest}}); response.Error != "" {
		t.Fatalf("settled empty owner Stop refused: %s", response.Error)
	}
	if op := waitClaimIdentityOperation(t, o, retryID); op.err != nil || !o.retired {
		t.Fatalf("settled empty owner did not retire: %v", op.err)
	}
}
