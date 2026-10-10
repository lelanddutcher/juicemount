package main

import (
	"context"
	"encoding/json"
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

type socketHeldOwnerStop struct {
	entered, release chan struct{}
	once             sync.Once
	calls            atomic.Int32
}

func (*socketHeldOwnerStop) Mount() error  { return nil }
func (*socketHeldOwnerStop) StartMonitor() {}
func (*socketHeldOwnerStop) OwnerSnapshot() health.FUSEOwnerSnapshot {
	return health.FUSEOwnerSnapshot{MountPath: "/retained/original"}
}
func (m *socketHeldOwnerStop) Stop() health.FUSEStopResult {
	m.calls.Add(1)
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return health.FUSEStopResult{State: health.FUSEInFlight, Detail: "held original child"}
}

// The real dispatcher accepts Stop over a real Unix connection. Its pending
// reply is deliberately withheld, then the caller disconnects. Reconnection
// must observe that same operation, including its eventual incomplete result.
func TestVolumeOwnerAcceptedSocketStopSurvivesLostReply(t *testing.T) {
	owner, err := newVolumeOwner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager := &socketHeldOwnerStop{entered: make(chan struct{}), release: make(chan struct{})}
	owner.config = "selected-config"
	owner.manager, owner.status = manager, manager
	owner.snapshot = manager.OwnerSnapshot()
	root, err := os.MkdirTemp(os.TempDir(), "vos-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "s")
	server, err := servicecontrol.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	const id = "abcdef0123456789abcdef0123456789"
	accepted, replyRelease := make(chan struct{}), make(chan struct{})
	handlerDone := make(chan struct{})
	var acceptedOnce, releaseOnce sync.Once
	serveDone := make(chan error, 1)
	callDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	callJoined := false
	var original *volumeOwnerOperation
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(manager.release); close(replyRelease) })
		if !callJoined {
			select {
			case <-callDone:
			case <-time.After(time.Second):
				t.Error("original disconnected client not joined")
			}
		}
		op := original
		if op == nil {
			owner.mu.Lock()
			op = owner.ops[id]
			owner.mu.Unlock()
		}
		if op != nil {
			select {
			case <-op.done:
			case <-time.After(time.Second):
				t.Error("original accepted Stop not joined")
			}
		}
		select {
		case <-accepted:
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Error("original accepted handler not joined")
			}
		default:
		}
		_ = server.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("original socket server not joined")
		}
	}()
	go func() {
		serveDone <- server.Serve(func(req servicecontrol.Request) servicecontrol.Response {
			response := owner.dispatch(req)
			if req.Method == "stop" {
				acceptedOnce.Do(func() { close(accepted) })
				<-replyRelease
				close(handlerDone)
			}
			return response
		})
	}()
	request := func(method string) servicecontrol.Request {
		return servicecontrol.Request{Version: servicecontrol.ProtocolVersion, Build: version.Commit,
			Method: method, Strings: []string{owner.instance, id, owner.config}}
	}
	go func() {
		_, err := servicecontrol.Call(ctx, path, request("stop"))
		callDone <- err
	}()
	for _, ready := range []<-chan struct{}{accepted, manager.entered} {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("actual dispatcher did not accept the original Stop")
		}
	}
	owner.mu.Lock()
	original = owner.ops[id]
	owner.mu.Unlock()
	if original == nil {
		t.Fatal("accepted Stop did not retain its original operation")
	}
	cancel()
	select {
	case err := <-callDone:
		callJoined = true
		if err == nil {
			t.Fatal("withheld reply unexpectedly reached disconnected caller")
		}
	case <-time.After(time.Second):
		t.Fatal("original caller did not disconnect")
	}
	query := func() (volumeOwnerReceipt, string) {
		queryCtx, queryCancel := context.WithTimeout(context.Background(), time.Second)
		defer queryCancel()
		response, err := servicecontrol.Call(queryCtx, path, request("query"))
		if err != nil {
			t.Fatal(err)
		}
		var receipt volumeOwnerReceipt
		if err := json.Unmarshal([]byte(response.Text), &receipt); err != nil {
			t.Fatal(err)
		}
		return receipt, response.Error
	}
	if receipt, appErr := query(); appErr != "" || receipt.Operation != id || receipt.OperationState != "pending" {
		t.Fatalf("disconnect lost original pending operation: %+v %s", receipt, appErr)
	}
	releaseOnce.Do(func() { close(manager.release); close(replyRelease) })
	select {
	case <-original.done:
	case <-time.After(time.Second):
		t.Fatal("original Stop did not publish its bounded incomplete result")
	}
	receipt, appErr := query()
	if appErr == "" || receipt.OperationState != "error" || receipt.StopResult == "" || receipt.MountPath != "/retained/original" {
		t.Fatalf("original incomplete Stop result lost: %+v %s", receipt, appErr)
	}
	// Query/reuse of the original ID may not execute Stop again.
	owner.dispatch(request("stop"))
	owner.mu.Lock()
	retained := owner.manager == manager && !owner.retired
	owner.mu.Unlock()
	if !retained || manager.calls.Load() != 1 {
		t.Fatalf("lost reply replaced resources or repeated Stop: retained=%v calls=%d", retained, manager.calls.Load())
	}
}
