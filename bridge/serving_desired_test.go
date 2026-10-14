//go:build !serviceclient

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func confirmedServingFixture(t *testing.T) (ServerConfig, volumeOwnerIntent, uint64) {
	t.Helper()
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db")}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = owner; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	return cfg, owner, revision
}

func TestReplacementRecoveryServingDesiredReplay(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := h8bSecretConfig(base)
	cfg.ReplacementRecovery = true
	cfg.ObjectAccessKey, cfg.ObjectSecretKey = "object-access", "object-secret"
	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(scope.Root, servingDesiredLeaf))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{h8bRedisSecret, h8bLinkSecret, "object-access", "object-secret"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("serving-desired persisted a secret")
		}
	}
	replayed, err := readServingDesired()
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Config.ReplacementRecovery || replayed.Config != persistableServerConfig(cfg) {
		t.Fatalf("replayed config changed opt-in or redaction: %+v", replayed.Config)
	}
	before, err := servingConfigDigest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	after, err := servingConfigDigest(replayed.Config)
	if err != nil || before != after {
		t.Fatalf("replay digest changed: before=%s after=%s error=%v", before, after, err)
	}
}

func TestReplacementRecoveryReadsLegacyStoppedDesired(t *testing.T) {
	servingDesiredFixture(t)
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	rootJSON, err := json.Marshal(scope.Root)
	if err != nil {
		t.Fatal(err)
	}
	// A literal pre-option record: config has no replacement_recovery member.
	// The previous build may be superseded only because it stopped with no owner.
	const oldBuild = "synthetic-previous-build"
	raw := `{"version":1,"build":"` + oldBuild + `","root":` + string(rootJSON) +
		`,"revision":1,"desired":"stopped","config":` + historicalCanonicalRecoveryOffConfig +
		`,"owner":{"version":0,"build":"","root":"","instance":"","config_digest":"","claim_id":"","mount_id":"","startup_probe_id":""},"nfs_phase":"none"}`
	path := filepath.Join(scope.Root, servingDesiredLeaf)
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	legacy, err := readServingDesired()
	if err != nil || legacy.Build != oldBuild || legacy.Revision != 1 || legacy.Config.ReplacementRecovery || !legacy.Config.SpoolEnable {
		t.Fatalf("legacy stopped record unavailable: %+v %v", legacy, err)
	}
	revision, err := beginServingDesired(legacy.Config)
	if err != nil || revision != 2 {
		t.Fatalf("legacy stopped record could not be superseded: revision=%d error=%v", revision, err)
	}
	current, err := readServingDesired()
	if err != nil || current.Revision != 2 || current.Config.ReplacementRecovery {
		t.Fatalf("upgrade changed recovery opt-in: %+v %v", current, err)
	}
}

func TestRecoveryWorkerAdmitsOneConfirmedStart(t *testing.T) {
	cfg, owner, revision := confirmedServingFixture(t)
	oldObserve, oldInspect, oldStart := observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine
	t.Cleanup(func() {
		observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine = oldObserve, oldInspect, oldStart
	})
	observations, inspections, starts := 0, 0, 0
	observePriorServingNFS = func(context.Context, servingDesiredRecord) (bool, error) { observations++; return true, nil }
	inspectRecoveryOwner = func(got ServerConfig) (volumeOwnerIntent, error) {
		inspections++
		if got != cfg {
			t.Error("recovery config changed")
		}
		return owner, nil
	}
	startRecoveryEngine = func(got servingDesiredRecord) error {
		starts++
		if got.Revision != revision || got.Desired != "recovering" || got.Owner != owner {
			t.Error("recovery admission changed identity")
		}
		return confirmServingDesired(got.Revision)
	}
	resumeConfirmedServingOnce(context.Background())
	if observations != 2 || inspections != 1 || starts != 1 {
		t.Fatalf("recovery calls: observation=%d inspect=%d start=%d", observations, inspections, starts)
	}
	if got, err := readServingDesired(); err != nil || got.Desired != "running" || got.Revision != revision || recoveryStatus() != "resumed" {
		t.Fatalf("recovery did not confirm: %+v %v status=%s", got, err, recoveryStatus())
	}
}

func TestRecoveryWorkerHoldsPendingAndUnknownNFS(t *testing.T) {
	_, _, revision := confirmedServingFixture(t)
	oldObserve, oldInspect, oldStart := observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine
	t.Cleanup(func() {
		observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine = oldObserve, oldInspect, oldStart
	})
	observations := 0
	observePriorServingNFS = func(context.Context, servingDesiredRecord) (bool, error) {
		observations++
		return false, errors.New("unknown inventory")
	}
	inspectRecoveryOwner = func(ServerConfig) (volumeOwnerIntent, error) {
		t.Error("uncertain NFS inspected owner")
		return volumeOwnerIntent{}, nil
	}
	startRecoveryEngine = func(servingDesiredRecord) error { t.Error("uncertain NFS started engine"); return nil }
	if err := markServingNFSPending(revision); err != nil {
		t.Fatal(err)
	}
	resumeConfirmedServingOnce(context.Background())
	if observations != 0 || recoveryStatus() != "held-nfs-admission-uncertain" {
		t.Fatalf("pending admission observed: calls=%d status=%s", observations, recoveryStatus())
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.NFSPhase = "none"; return nil }); err != nil {
		t.Fatal(err)
	}
	resumeConfirmedServingOnce(context.Background())
	if observations != 1 || recoveryStatus() != "held-nfs-observation-unknown" {
		t.Fatalf("unknown inventory admitted: calls=%d status=%s", observations, recoveryStatus())
	}
}

func TestRecoveryWorkerCannotCrossDurableStop(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db")}
	revision, err := beginServingDesired(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = owner; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	oldObserve, oldInspect, oldStart := observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine
	observePriorServingNFS = func(context.Context, servingDesiredRecord) (bool, error) {
		close(entered)
		<-release
		return true, nil
	}
	inspectRecoveryOwner = func(ServerConfig) (volumeOwnerIntent, error) {
		t.Error("stopped worker inspected owner")
		return owner, nil
	}
	startRecoveryEngine = func(servingDesiredRecord) error { t.Error("stopped worker started engine"); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		<-done
		observePriorServingNFS, inspectRecoveryOwner, startRecoveryEngine = oldObserve, oldInspect, oldStart
	})
	go func() { resumeConfirmedServingOnce(ctx); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not observe")
	}
	globalLifecycleMu.Lock()
	_, err = stopServingDesired()
	globalLifecycleMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopped worker did not join")
	}
	if r, err := readServingDesired(); err != nil || r.Desired != "stopped" {
		t.Fatalf("Stop supersession: %+v %v", r, err)
	}
}

func servingDesiredFixture(t *testing.T) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	oldPath, oldMode := selectedOwnerServiceSocketPath, servingProcessMode.Load()
	selectedOwnerServiceSocketPath = func() (string, error) { return filepath.Join(base, "control.sock"), nil }
	servingProcessMode.Store(true)
	t.Cleanup(func() {
		selectedOwnerServiceSocketPath = oldPath
		servingProcessMode.Store(oldMode)
		servingDesiredMu.Lock()
		servingDesiredActiveRevision = 0
		servingDesiredMu.Unlock()
	})
	for _, key := range []string{"JM_APP_ISOLATION", "JM_APP_ISOLATION_ROOT"} {
		old, present := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	return base
}

func TestDesiredServingStartRequiresBoundOwnerAndStopSupersedes(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db"), MountPoint: filepath.Join(base, "volume")}
	revision, err := beginServingDesired(cfg)
	if err != nil || revision != 1 {
		t.Fatalf("pending Start: %d %v", revision, err)
	}
	if err := confirmServingDesired(revision); err == nil {
		t.Fatal("unbound pending Start confirmed")
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = owner; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	if r, err := readServingDesired(); err != nil || r.Desired != "running" || r.Config != cfg || r.Owner != owner {
		t.Fatalf("confirmed intent: %+v %v", r, err)
	}
	stopRevision, err := stopServingDesired()
	if err != nil || stopRevision != revision+1 {
		t.Fatalf("durable Stop: %d %v", stopRevision, err)
	}
	if err := markServingNFSPending(revision); err == nil {
		t.Fatal("late NFS admission crossed Stop")
	}
	if _, err := admitServingRecovery(servingDesiredRecord{Revision: revision, Config: cfg, Owner: owner}); err == nil {
		t.Fatal("stopped revision recovered")
	}
	raw, _ := json.Marshal(cfg)
	resp := dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: "start-automatic", Strings: []string{string(raw)}})
	if resp.Error != "" || resp.Text != "automatic_start_superseded" {
		t.Fatalf("automatic Start overrode Stop: %+v", resp)
	}
}

func TestHTTPStopAcknowledgesOnlyAfterDurablePublication(t *testing.T) {
	servingDesiredFixture(t)
	stopInProgress.Store(false)
	globalLifecycleMu.Lock()
	locked := true
	type result struct {
		code int
		body string
	}
	done := make(chan result, 2)
	var posts sync.WaitGroup
	posts.Add(2)
	t.Cleanup(func() {
		if locked {
			globalLifecycleMu.Unlock()
		}
		posts.Wait()
		stopHTTPFlights.Wait()
		stopInProgress.Store(false)
	})
	post := func() {
		defer posts.Done()
		req := httptest.NewRequest(http.MethodPost, "/stop", nil)
		req.Header.Set(localControlHeader, "1")
		w := httptest.NewRecorder()
		handleStopHTTP(w, req)
		done <- result{w.Code, w.Body.String()}
	}
	go post()
	go post()
	select {
	case got := <-done:
		globalLifecycleMu.Unlock()
		locked = false
		t.Fatalf("HTTP acknowledged before lifecycle publication: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	globalLifecycleMu.Unlock()
	locked = false
	for i := 0; i < 2; i++ {
		select {
		case got := <-done:
			if got.code != http.StatusOK || !strings.Contains(got.body, `"stopping":true`) {
				t.Fatalf("stop response: %+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("HTTP stop did not finish")
		}
	}
	if r, err := readServingDesired(); err != nil || r.Desired != "stopped" {
		t.Fatalf("HTTP ack lacked stopped intent: %+v %v", r, err)
	}
}

func rewriteDesiredBuild(t *testing.T, build string) {
	t.Helper()
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(scope.Root, servingDesiredLeaf)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r servingDesiredRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	r.Build = build
	out, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// H-13: after an app update the desired record still names the previous build.
// A durable Stop that owns nothing must not block the new build's Start; a
// record that owns a running mount must still require the exact build.
func TestDesiredRecordFromPreviousBuild(t *testing.T) {
	const previous = "66dc696101379e3ab835c7f92f161093ea832940"
	t.Run("stopped record is superseded by Start", func(t *testing.T) {
		servingDesiredFixture(t)
		if _, err := stopServingDesired(); err != nil {
			t.Fatal(err)
		}
		rewriteDesiredBuild(t, previous)
		revision, err := beginServingDesired(ServerConfig{})
		if err != nil {
			t.Fatalf("Start after an update was refused: %v", err)
		}
		r, err := readServingDesired()
		if err != nil || r.Build != version.Commit || r.Revision != revision || r.Desired != "pending" {
			t.Fatalf("restamped record = %+v err=%v", r, err)
		}
	})
	t.Run("running record from another build stays refused", func(t *testing.T) {
		confirmedServingFixture(t)
		rewriteDesiredBuild(t, previous)
		if _, err := readServingDesired(); err == nil {
			t.Fatal("a running record from another build was accepted")
		}
	})
}

// This fixture uses only private files and fake native inventory callbacks.
// It creates/retire journals through mountbroker without mounting anything.
func legacyRetiredServingFixture(t *testing.T) (appisolation.Scope, servingDesiredRecord, *mounttable.Snapshot) {
	t.Helper()
	root := controlIsolationFixture(t)
	scope := appisolation.Scope{Root: root}
	cfg := ServerConfig{DBPath: filepath.Join(root, "metadata.db"), MountPoint: filepath.Join(root, "volume")}
	for _, path := range []string{cfg.MountPoint, filepath.Join(root, "mount-transactions")} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := intendedOwner(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	owner.Build = "historical-retired-fixture"
	put := func(path string, value any) {
		t.Helper()
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	put(ownerIntentPath(root), owner)
	put(filepath.Join(root, ownerRetirementLeaf(owner)), ownerRetirement{Version: 1, Intent: owner, ShutdownID: ownerShutdownID(owner)})
	if err := os.WriteFile(filepath.Join(root, ownerLifetimeLeaf), nil, 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := &mounttable.Snapshot{Version: 1, BootID: "legacy-upgrade-boot", Entries: []mounttable.MountEntry{}}
	spec := mountbroker.Spec{MountPath: cfg.MountPoint, Source: "127.0.0.1:/jm-0123456789abcdef0123456789abcdef", Endpoint: "127.0.0.1:11049", ExportPath: "/jm-0123456789abcdef0123456789abcdef", MetadataPath: cfg.DBPath}
	creator := mountbroker.Creator{Directory: filepath.Join(root, "mount-transactions"), Observe: func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }}
	result := creator.CreateBound(context.Background(), spec, func(mountbroker.Record) error {
		snapshot.Entries = []mounttable.MountEntry{{Path: cfg.MountPoint, Source: spec.Source, Type: "nfs", FSID: [2]int32{21, 22}}}
		return nil
	})
	if err := result.Failure(); err != nil {
		t.Fatal(err)
	}
	snapshot.Entries = []mounttable.MountEntry{}
	if err := creator.Retire(context.Background(), *result.Record, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	r := servingDesiredRecord{Version: 1, Build: owner.Build, Root: root, Revision: 8, Desired: "stopped", Config: cfg, Owner: owner, NFSPhase: "bound", NFSRecord: result.Record}
	put(filepath.Join(root, servingDesiredLeaf), r)
	oldObserve := observeOwnedNFS
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) { return *snapshot, nil }
	t.Cleanup(func() { observeOwnedNFS = oldObserve })
	return scope, r, snapshot
}

func TestRetiredLegacyServingStartPreservesEvidenceAndSelectsSuccessor(t *testing.T) {
	scope, old, _ := legacyRetiredServingFixture(t)
	before, err := os.ReadFile(filepath.Join(scope.Root, servingDesiredLeaf))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := readServingDesired(); err != nil || got.Build != old.Build || got.Owner != old.Owner {
		t.Fatalf("retired predecessor: %+v %v", got, err)
	}
	if got, err := readOwnerIntent(scope); err == nil || got != (volumeOwnerIntent{}) {
		t.Fatal("historical owner passed active current-build reader")
	}
	if _, err := beginServingDesired(old.Config); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(scope.Root, retiredServingDesiredLeaf(old.Owner))
	if got, err := os.ReadFile(archive); err != nil || !bytes.Equal(got, before) {
		t.Fatal("exact predecessor was not preserved")
	}
	current, err := readServingDesired()
	if err != nil || current.Build != version.Commit || current.Desired != "pending" || current.Owner != (volumeOwnerIntent{}) {
		t.Fatalf("fresh admission: %+v %v", current, err)
	}
	next, born, err := createOwnerIntent(scope, old.Config)
	if err != nil || !born || next.Build != version.Commit || next.Instance == old.Owner.Instance {
		t.Fatalf("successor: %+v %v %v", next, born, err)
	}
	if got, err := os.ReadFile(archive); err != nil || !bytes.Equal(got, before) {
		t.Fatal("successor changed immutable predecessor")
	}
	if _, err := readOwnerRetirementForBuild(scope, old.Owner, old.Build); err != nil {
		t.Fatal("predecessor terminal receipt lost:", err)
	}
	if selected, err := readOwnerIntent(scope); err != nil || selected != next {
		t.Fatal("successor not selected")
	}
}

func TestRetiredLegacyServingRejectsIncompleteProof(t *testing.T) {
	for _, kind := range []string{"running", "pending", "recovering", "nfs-pending", "none-with-record", "missing-retirement", "foreign-retirement", "held-lifetime", "unknown-inventory", "mounted", "other-boot", "wrong-nfs-operation", "changed-config", "changed-owner-build"} {
		t.Run(kind, func(t *testing.T) {
			scope, r, snapshot := legacyRetiredServingFixture(t)
			switch kind {
			case "running", "pending", "recovering":
				r.Desired = kind
			case "nfs-pending":
				r.NFSPhase, r.NFSRecord = "pending", nil
			case "none-with-record":
				r.NFSPhase = "none"
			case "missing-retirement":
				if err := os.Remove(filepath.Join(scope.Root, ownerRetirementLeaf(r.Owner))); err != nil {
					t.Fatal(err)
				}
			case "foreign-retirement":
				if err := os.WriteFile(filepath.Join(scope.Root, ownerRetirementLeaf(r.Owner)), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "held-lifetime":
				l, err := acquireOwnerLifetime(scope)
				if err != nil {
					t.Fatal(err)
				}
				defer l.close()
			case "unknown-inventory":
				snapshot.Entries = nil
			case "mounted":
				snapshot.Entries = []mounttable.MountEntry{{Path: r.Config.MountPoint, Source: "foreign:/", Type: "nfs", FSID: [2]int32{90, 91}}}
			case "other-boot":
				snapshot.BootID = "other"
			case "wrong-nfs-operation":
				r.NFSRecord.OperationID = strings.Repeat("f", 32)
			case "changed-config":
				r.Config.DBPath += ".changed"
			case "changed-owner-build":
				r.Owner.Build = "different"
			}
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(scope.Root, servingDesiredLeaf)
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readServingDesired(); err == nil {
				t.Fatal("incomplete predecessor admitted")
			}
			if _, err := beginServingDesired(r.Config); err == nil {
				t.Fatal("incomplete predecessor superseded")
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, b) {
				t.Fatal("refusal changed desired evidence")
			}
		})
	}
}

func TestRetiredLegacyServingRepeatedStopAndPreBirthRetry(t *testing.T) {
	scope, old, _ := legacyRetiredServingFixture(t)
	for n := 1; n <= 2; n++ {
		if rev, err := stopServingDesired(); err != nil || rev != old.Revision+uint64(n) {
			t.Fatalf("Stop revision=%d err=%v", rev, err)
		}
		got, err := readServingDesired()
		if err != nil || got.Build != old.Build || got.Owner != old.Owner {
			t.Fatal("Stop relabelled historical owner")
		}
	}
	prior, err := os.ReadFile(filepath.Join(scope.Root, servingDesiredLeaf))
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 2; n++ {
		if _, err := beginServingDesired(old.Config); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(scope.Root, retiredServingDesiredLeaf(old.Owner))); err != nil || !bytes.Equal(got, prior) {
		t.Fatal("pre-birth retry changed predecessor archive")
	}
	if _, born, err := createOwnerIntent(scope, old.Config); err != nil || !born {
		t.Fatalf("retry successor born=%v err=%v", born, err)
	}
}

func TestRetiredLegacySuccessorRequiresFreshConfigAndArchive(t *testing.T) {
	scope, old, _ := legacyRetiredServingFixture(t)
	if _, born, err := createOwnerIntent(scope, old.Config); err == nil || born {
		t.Fatal("historical owner replaced without fresh Start")
	}
	if _, err := beginServingDesired(old.Config); err != nil {
		t.Fatal(err)
	}
	changed := old.Config
	changed.DBPath += ".different"
	if _, born, err := createOwnerIntent(scope, changed); err == nil || born {
		t.Fatal("unbound fresh configuration replaced historical owner")
	}
	archive := filepath.Join(scope.Root, retiredServingDesiredLeaf(old.Owner))
	if err := os.Rename(archive, archive+".retained"); err != nil {
		t.Fatal(err)
	}
	if _, born, err := createOwnerIntent(scope, old.Config); err == nil || born {
		t.Fatal("missing archive replaced historical owner")
	}
}

func TestRetiredLegacyArchiveConflictPreservesOriginal(t *testing.T) {
	scope, old, _ := legacyRetiredServingFixture(t)
	path := filepath.Join(scope.Root, servingDesiredLeaf)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(scope.Root, retiredServingDesiredLeaf(old.Owner))
	if err := os.WriteFile(archive, []byte("retained-conflict"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := beginServingDesired(old.Config); err == nil {
		t.Fatal("archive conflict overwritten")
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, before) {
		t.Fatal("archive failure changed predecessor")
	}
	if got, err := os.ReadFile(archive); err != nil || string(got) != "retained-conflict" {
		t.Fatal("conflicting archive was replaced")
	}
}

func TestRetiredServingUpgradeGuardRetainsCallerFence(t *testing.T) {
	scope, old, _ := legacyRetiredServingFixture(t)
	l, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	for n := 0; n < 2; n++ {
		got, err := validateRetiredServingForUpgrade(l, old.Build)
		if err != nil || got.Build != old.Build || got.Owner != old.Owner || got.Revision != old.Revision {
			t.Fatalf("held-fence proof lost original: %+v %v", got, err)
		}
	}
	if second, err := acquireOwnerLifetime(scope); err == nil {
		second.close()
		t.Fatal("upgrade guard released caller fence")
	}
	if _, err := validateRetiredServingForUpgrade(l, "wrong-build"); err == nil {
		t.Fatal("wrong expected build admitted")
	}
}
