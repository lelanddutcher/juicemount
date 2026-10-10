//go:build !serviceclient

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

type shutdownOwnerManager struct {
	stops                       atomic.Int32
	failFirst                   bool
	pendingGate, pendingEntered chan struct{}
}

func (*shutdownOwnerManager) Mount() error  { panic("shutdown cannot mount") }
func (*shutdownOwnerManager) StartMonitor() { panic("shutdown cannot start monitor") }
func (*shutdownOwnerManager) OwnerSnapshot() health.FUSEOwnerSnapshot {
	return health.FUSEOwnerSnapshot{MountPath: "/original/fuse"}
}
func (m *shutdownOwnerManager) Stop() health.FUSEStopResult {
	count := m.stops.Add(1)
	if count == 1 && m.pendingGate != nil {
		close(m.pendingEntered)
		<-m.pendingGate
	}
	if count == 1 && m.failFirst {
		return health.FUSEStopResult{State: health.FUSEInFlight}
	}
	return health.FUSEStopResult{State: health.FUSEStopped}
}

// The real serving dispatcher must recover the exact old owner after an idle
// service restart. This fixture has a real private control socket and owner
// operation dispatcher, but no FUSE mount, Link, or child process.
func TestShutdownIdleRestartRetiresExactOwnerAndReusesStop(t *testing.T) {
	for _, mode := range []string{"new-stop", "completed-original-stop", "failed-original-stop", "pending-original-stop", "already-retired"} {
		t.Run(mode, func(t *testing.T) {
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
			intent, _, err := createOwnerIntentUnfenced(scope, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := updateServingDesired(revision, func(r *servingDesiredRecord) error {
				r.Owner = intent
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := confirmServingDesired(revision); err != nil {
				t.Fatal(err)
			}
			lifetime, err := acquireOwnerLifetime(scope)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lifetime.close)
			manager := &shutdownOwnerManager{failFirst: mode == "failed-original-stop"}
			var pendingRelease sync.Once
			if mode == "pending-original-stop" {
				manager.pendingGate, manager.pendingEntered = make(chan struct{}), make(chan struct{})
				t.Cleanup(func() { pendingRelease.Do(func() { close(manager.pendingGate) }) })
			}
			owner := &volumeOwner{instance: intent.Instance, root: scope.Root,
				config: intent.ConfigDigest, intentBound: true, intent: intent,
				manager: manager, status: manager, ops: make(map[string]*volumeOwnerOperation)}
			if mode == "pending-original-stop" {
				t.Cleanup(func() {
					pendingRelease.Do(func() { close(manager.pendingGate) })
					if op := owner.ops["0123456789abcdef0123456789abcdef"]; op != nil {
						select {
						case <-op.done:
						case <-time.After(time.Second):
							t.Error("pending original Stop not joined")
						}
					}
				})
			}
			if mode != "new-stop" {
				const originalID = "0123456789abcdef0123456789abcdef"
				if _, err := owner.begin("stop", originalID, intent.ConfigDigest, owner.stop); err != nil {
					t.Fatal(err)
				}
				if mode == "pending-original-stop" {
					select {
					case <-manager.pendingEntered:
					case <-time.After(time.Second):
						t.Fatal("original Stop did not enter")
					}
				} else {
					select {
					case <-owner.ops[originalID].done:
					case <-time.After(time.Second):
						t.Fatal("original Stop did not join")
					}
				}
				if manager.stops.Load() != 1 {
					t.Fatal("original Stop did not complete")
				}
				if mode == "failed-original-stop" && owner.ops[originalID].err == nil {
					t.Fatal("historical Stop did not fail semantically")
				}
				if mode == "already-retired" {
					owner.terminalRequested = true
					if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
						t.Fatal(err)
					}
					lifetime.close()
				}
			}
			path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
				resp := owner.dispatch(req)
				if mode == "pending-original-stop" && req.Method == "inspect" {
					pendingRelease.Do(func() { close(manager.pendingGate) })
				}
				if req.Method == "shutdown" && resp.Error == "" {
					if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
						return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: err.Error()}
					}
					lifetime.close()
				}
				return resp
			})
			oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
			shutdownOwnerSocketPath = func() (string, error) { return path, nil }
			var observations atomic.Int32
			observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) {
				observations.Add(1)
				return true, nil
			}
			t.Cleanup(func() { shutdownOwnerSocketPath = oldPath; observePriorServingShutdownNFS = oldObserve })
			globalMu.Lock()
			if globalOwner != nil || globalFUSE != nil || globalServer != nil {
				globalMu.Unlock()
				t.Fatal("requires idle engine")
			}
			globalOwnerStopID = ""
			globalMu.Unlock()
			resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
				Build: version.Commit, Method: "shutdown"})
			if resp.Error != "" || resp.Int != 1 {
				t.Fatalf("idle Shutdown: %+v", resp)
			}
			wantStops := int32(1)
			if mode == "failed-original-stop" {
				wantStops = 2
			}
			if observations.Load() != 1 || manager.stops.Load() != wantStops {
				t.Fatalf("NFS gate=%d physical Stops=%d", observations.Load(), manager.stops.Load())
			}
			if _, err := readOwnerRetirement(scope, intent); err != nil {
				t.Fatalf("original terminal receipt: %v", err)
			}
			if got, err := readOwnerIntent(scope); err != nil || got != intent {
				t.Fatalf("intent replaced: %+v %v", got, err)
			}
			if got, err := readServingDesired(); err != nil || got.Desired != "stopped" {
				t.Fatalf("durable Stop missing: %+v %v", got, err)
			}
		})
	}
}

func TestShutdownIdleRestartRefusesUnknownPriorNFS(t *testing.T) {
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
	intent, _, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateServingDesired(revision, func(r *servingDesiredRecord) error { r.Owner = intent; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := confirmServingDesired(revision); err != nil {
		t.Fatal(err)
	}
	oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
	shutdownOwnerSocketPath = func() (string, error) { t.Error("unknown NFS inspected original owner"); return "", os.ErrNotExist }
	observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) { return false, nil }
	t.Cleanup(func() { shutdownOwnerSocketPath = oldPath; observePriorServingShutdownNFS = oldObserve })
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if resp.Error != "" || resp.Int != 0 {
		t.Fatalf("unknown prior NFS admitted: %+v", resp)
	}
	if _, err := readOwnerIntent(scope); err != nil {
		t.Fatalf("original owner intent lost: %v", err)
	}
}

func TestShutdownFreshNeverOwnedServingNamespace(t *testing.T) {
	servingDesiredFixture(t)
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if resp.Error != "" || resp.Int != 1 {
		t.Fatalf("fresh never-owned Shutdown: %+v", resp)
	}
}

// A failed Start can retain the original owner in the current engine before
// desired.Owner is published. Shutdown must still use that owner's ordinary
// Stop and terminal control operations.
func TestShutdownCurrentEngineRetiresFailedPreclaimOwner(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db"), RedisURL: "redis://:fixture-secret@127.0.0.1:58512/0"}
	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if desired, err := readServingDesired(); err != nil || desired.Owner != (volumeOwnerIntent{}) {
		t.Fatalf("pre-claim desired state unexpectedly owns selected intent: %+v %v", desired, err)
	}
	lifetime, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lifetime.close)
	owner := &volumeOwner{instance: intent.Instance, root: intent.Root, claimDigest: intent.ConfigDigest,
		intentBound: true, intent: intent, ops: make(map[string]*volumeOwnerOperation)}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	claim := owner.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "claim", Strings: []string{intent.Instance, intent.ClaimID, string(raw)}, Ints: []int{-1, 1}})
	if claim.Error != "" {
		t.Fatalf("selected credential claim refused before original operation: %s", claim.Error)
	}
	if op := waitClaimIdentityOperation(t, owner, intent.ClaimID); op.err == nil {
		t.Fatal("invalid cache budget did not settle failed claim")
	}
	path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
		resp := owner.dispatch(req)
		if req.Method == "shutdown" && resp.Error == "" {
			if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
				return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: err.Error()}
			}
			lifetime.close()
		}
		return resp
	})
	globalMu.Lock()
	if globalOwner != nil || globalFUSE != nil || globalServer != nil || globalStopPending {
		globalMu.Unlock()
		t.Fatal("fixture requires idle serving engine")
	}
	globalOwner = &ownerFacade{intent: intent, client: &volumeOwnerClient{
		path: path, instance: intent.Instance, digest: intent.ConfigDigest, root: intent.Root}}
	globalOwnerStopID = ""
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalOwner, globalOwnerStopID, globalStopPending = nil, "", false
		globalMu.Unlock()
	})
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if resp.Error != "" || resp.Int != 1 {
		t.Fatalf("ordinary Shutdown of failed pre-claim owner: %+v", resp)
	}
	if op := waitClaimIdentityOperation(t, owner, ownerShutdownID(intent)); op.err != nil || !owner.terminalRequested || !owner.retired {
		t.Fatalf("original owner terminal operation missing: %v", op.err)
	}
	if _, err := readOwnerRetirement(scope, intent); err != nil {
		t.Fatalf("exact terminal publication missing: %v", err)
	}
	if desired, err := readServingDesired(); err != nil || desired.Desired != "stopped" || desired.Owner != (volumeOwnerIntent{}) {
		t.Fatalf("pre-claim durable Shutdown changed owner binding: %+v %v", desired, err)
	}
}

// Selection may outlive the engine before desired.Owner is written. A new
// engine has no facade, but Shutdown must use the one durable selected intent.
func TestShutdownReplacementRetiresFailedPreclaimOwner(t *testing.T) {
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db"), RedisURL: "redis://:fixture-secret@127.0.0.1:58512/0"}
	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	lifetime, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lifetime.close)
	owner := &volumeOwner{instance: intent.Instance, root: intent.Root, claimDigest: intent.ConfigDigest,
		intentBound: true, intent: intent, ops: make(map[string]*volumeOwnerOperation)}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	claim := owner.dispatch(servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
		Method: "claim", Strings: []string{intent.Instance, intent.ClaimID, string(raw)}, Ints: []int{-1, 1}})
	if claim.Error != "" {
		t.Fatalf("selected credential claim refused before original operation: %s", claim.Error)
	}
	if op := waitClaimIdentityOperation(t, owner, intent.ClaimID); op.err == nil {
		t.Fatal("invalid cache budget did not settle failed claim")
	}
	var stops, terminals, claims, mounts, attaches atomic.Int32
	path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
		switch req.Method {
		case "stop":
			stops.Add(1)
		case "shutdown":
			terminals.Add(1)
		case "claim":
			claims.Add(1)
		case "mount":
			mounts.Add(1)
		case "attach":
			attaches.Add(1)
		}
		resp := owner.dispatch(req)
		if req.Method == "shutdown" && resp.Error == "" {
			if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
				return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: err.Error()}
			}
			lifetime.close()
		}
		return resp
	})
	oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
	oldSelect := selectServingOwner
	selectServingOwner = func(ServerConfig) (*ownerFacade, error) { t.Fatal("Shutdown selected a new owner"); return nil, nil }
	t.Cleanup(func() { selectServingOwner = oldSelect })
	shutdownOwnerSocketPath = func() (string, error) { return path, nil }
	var observations atomic.Int32
	observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) {
		observations.Add(1)
		return true, nil
	}
	t.Cleanup(func() { shutdownOwnerSocketPath, observePriorServingShutdownNFS = oldPath, oldObserve })
	globalMu.Lock()
	if globalOwner != nil || globalFUSE != nil || globalServer != nil || globalStopPending {
		globalMu.Unlock()
		t.Fatal("replacement fixture requires idle engine")
	}
	globalOwnerStopID = ""
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalOwner, globalOwnerStopID, globalStopPending = nil, "", false
		globalMu.Unlock()
	})
	resp := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if resp.Error != "" || resp.Int != 1 {
		t.Fatalf("replacement Shutdown of exact pre-claim owner: %+v", resp)
	}
	if observations.Load() != 1 || stops.Load() != 1 || terminals.Load() != 1 || claims.Load() != 0 || mounts.Load() != 0 || attaches.Load() != 0 {
		t.Fatalf("unexpected owner calls: NFS=%d Stop=%d terminal=%d claim=%d mount=%d attach=%d", observations.Load(), stops.Load(), terminals.Load(), claims.Load(), mounts.Load(), attaches.Load())
	}
	if op := waitClaimIdentityOperation(t, owner, ownerShutdownID(intent)); op.err != nil || !owner.terminalRequested || !owner.retired {
		t.Fatalf("original owner terminal operation missing: %v", op.err)
	}
	if _, err := readOwnerRetirement(scope, intent); err != nil {
		t.Fatalf("exact terminal publication missing: %v", err)
	}
	if desired, err := readServingDesired(); err != nil || desired.Desired != "stopped" || desired.Owner != (volumeOwnerIntent{}) {
		t.Fatalf("replacement Shutdown changed pre-claim binding: %+v %v", desired, err)
	}
}

func preclaimRecoveryFixture(t *testing.T) (volumeOwnerIntent, *volumeOwner, *ownerLifetime, string) {
	t.Helper()
	base := servingDesiredFixture(t)
	cfg := ServerConfig{DBPath: filepath.Join(base, "metadata.db")}
	if _, err := beginServingDesired(cfg); err != nil {
		t.Fatal(err)
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		t.Fatal(err)
	}
	intent, _, err := createOwnerIntentUnfenced(scope, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stopServingDesired(); err != nil {
		t.Fatal(err)
	}
	lifetime, err := acquireOwnerLifetime(scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lifetime.close)
	owner := &volumeOwner{instance: intent.Instance, root: intent.Root, claimDigest: intent.ConfigDigest,
		intentBound: true, intent: intent, ops: make(map[string]*volumeOwnerOperation)}
	path := operationReplyServer(t, owner.dispatch)
	return intent, owner, lifetime, path
}

func TestPreclaimRecoveryRefusesUncertainCustody(t *testing.T) {
	for _, mode := range []string{"foreign-instance", "unknown-nfs", "changed-intent", "changed-desired", "absent-owner", "untrusted-intent", "conflicting-stop", "wrong-build", "wrong-root", "config-mismatch", "nfs-phase", "nfs-record"} {
		t.Run(mode, func(t *testing.T) {
			intent, owner, _, path := preclaimRecoveryFixture(t)
			oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
			shutdownOwnerSocketPath = func() (string, error) { return path, nil }
			var observations atomic.Int32
			observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) {
				observations.Add(1)
				return mode != "unknown-nfs", nil
			}
			t.Cleanup(func() { shutdownOwnerSocketPath, observePriorServingShutdownNFS = oldPath, oldObserve })
			switch mode {
			case "foreign-instance":
				owner.instance = "ffffffffffffffffffffffffffffffff"
			case "changed-intent":
				observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) {
					observations.Add(1)
					changed := intent
					changed.Instance = "ffffffffffffffffffffffffffffffff"
					data, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(ownerIntentPath(intent.Root), data, 0600); err != nil {
						t.Fatal(err)
					}
					return true, nil
				}
			case "changed-desired":
				observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) {
					observations.Add(1)
					changed, err := readServingDesired()
					if err != nil {
						t.Fatal(err)
					}
					changed.Revision++
					if err := writeServingDesired(changed); err != nil {
						t.Fatal(err)
					}
					return true, nil
				}
			case "absent-owner":
				shutdownOwnerSocketPath = func() (string, error) { return filepath.Join(intent.Root, "absent.sock"), nil }
			case "untrusted-intent":
				if err := os.Chmod(ownerIntentPath(intent.Root), 0644); err != nil {
					t.Fatal(err)
				}
			case "wrong-build", "wrong-root":
				changed := intent
				if mode == "wrong-build" {
					changed.Build = "foreign-build"
				} else {
					changed.Root = filepath.Join(intent.Root, "foreign")
				}
				data, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(ownerIntentPath(intent.Root), data, 0600); err != nil {
					t.Fatal(err)
				}
			case "config-mismatch", "nfs-phase", "nfs-record":
				changed, err := readServingDesired()
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "config-mismatch":
					changed.Config.RedisURL = "redis://foreign"
				case "nfs-phase":
					changed.NFSPhase = "pending"
				case "nfs-record":
					changed.NFSRecord = &mountbroker.Record{}
				}
				if err := writeServingDesired(changed); err != nil {
					t.Fatal(err)
				}
			case "conflicting-stop":
				for _, id := range []string{"11111111111111111111111111111111", "22222222222222222222222222222222"} {
					op := &volumeOwnerOperation{method: "stop", payload: intent.ConfigDigest, done: make(chan struct{})}
					close(op.done)
					op.result = volumeOwnerReceipt{Operation: id, OperationState: "complete"}
					owner.ops[id] = op
				}
			}
			if _, err := recoverServingShutdownOwner(); err == nil {
				t.Fatal("uncertain or foreign pre-claim owner authorized Shutdown")
			}
			if owner.terminalRequested || owner.retired {
				t.Fatal("recovery mutated original owner before custody proof")
			}
			wantObservation := int32(1)
			if mode == "wrong-build" || mode == "wrong-root" || mode == "config-mismatch" || mode == "nfs-phase" || mode == "nfs-record" || mode == "untrusted-intent" {
				wantObservation = 0
			}
			if observations.Load() != wantObservation {
				t.Fatalf("unexpected NFS proof count %d, want %d", observations.Load(), wantObservation)
			}
		})
	}
}

func TestShutdownReplacementWaitsForLatePreclaimManager(t *testing.T) {
	intent, owner, lifetime, _ := preclaimRecoveryFixture(t)
	desired, err := readServingDesired()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(canonicalServerConfig(desired.Config))
	if err != nil {
		t.Fatal(err)
	}
	manager := &shutdownOwnerManager{}
	release := make(chan struct{})
	var once sync.Once
	if _, err := owner.begin("claim", intent.ClaimID, string(raw)+"\x000\x001", func() (volumeOwnerReceipt, error) {
		<-release
		owner.mu.Lock()
		owner.config, owner.manager, owner.status = intent.ConfigDigest, manager, manager
		owner.mu.Unlock()
		return volumeOwnerReceipt{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		waitClaimIdentityOperation(t, owner, intent.ClaimID)
	})
	path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
		resp := owner.dispatch(req)
		if req.Method == "shutdown" && resp.Error == "" {
			if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
				return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: err.Error()}
			}
			lifetime.close()
		}
		return resp
	})
	oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
	shutdownOwnerSocketPath = func() (string, error) { return path, nil }
	observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) { return true, nil }
	t.Cleanup(func() { shutdownOwnerSocketPath, observePriorServingShutdownNFS = oldPath, oldObserve })
	t.Cleanup(func() {
		globalMu.Lock()
		globalOwner, globalOwnerStopID, globalStopPending = nil, "", false
		globalMu.Unlock()
	})
	first := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if first.Error != "" || first.Int != 0 || owner.retired || owner.terminalRequested || manager.stops.Load() != 0 {
		t.Fatalf("pending claim falsely retired: %+v", first)
	}
	if _, err := readOwnerRetirement(appisolation.Scope{Root: intent.Root}, intent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending claim published terminal record: %v", err)
	}
	once.Do(func() { close(release) })
	if op := waitClaimIdentityOperation(t, owner, intent.ClaimID); op.err != nil {
		t.Fatalf("late original claim did not settle: %v", op.err)
	}
	second := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "shutdown"})
	if second.Error != "" || second.Int != 1 || manager.stops.Load() != 1 || !owner.retired || !owner.terminalRequested {
		t.Fatalf("late manager was not physically stopped once: %+v count=%d", second, manager.stops.Load())
	}
}

func TestPreclaimRecoveryRejoinsExactTerminalWithoutSocket(t *testing.T) {
	intent, _, lifetime, _ := preclaimRecoveryFixture(t)
	if err := lifetime.publishRetired(intent, ownerShutdownID(intent)); err != nil {
		t.Fatal(err)
	}
	lifetime.close()
	oldPath, oldObserve := shutdownOwnerSocketPath, observePriorServingShutdownNFS
	shutdownOwnerSocketPath = func() (string, error) { return filepath.Join(intent.Root, "absent.sock"), nil }
	observePriorServingShutdownNFS = func(context.Context, servingDesiredRecord) (bool, error) { return true, nil }
	t.Cleanup(func() { shutdownOwnerSocketPath, observePriorServingShutdownNFS = oldPath, oldObserve })
	f, err := recoverServingShutdownOwner()
	if err != nil || f == nil || f.intent != intent {
		t.Fatalf("exact terminal owner not reconciled: facade=%v err=%v", f, err)
	}
}
