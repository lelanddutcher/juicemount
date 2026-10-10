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

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// Exercise the client over the real control transport, without creating Link,
// FUSE, or an owner child. Every listener and handler is joined before cleanup.
func operationReplyServer(t *testing.T, dispatch func(servicecontrol.Request) servicecontrol.Response) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "voc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	s, err := servicecontrol.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(dispatch) }()
	t.Cleanup(func() {
		_ = s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.WaitClosed(ctx); err != nil {
			t.Errorf("control handlers not joined: %v", err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("control listener not joined")
		}
	})
	return path
}

func TestOwnerCallbackLoopJoinsHeldRecovery(t *testing.T) {
	intent := volumeOwnerIntent{Instance: "selected", ConfigDigest: "config", Root: "root"}
	r := volumeOwnerReceipt{Instance: intent.Instance, Build: version.Commit,
		ConfigDigest: intent.ConfigDigest, IsolationRoot: intent.Root, RemountVersion: 1}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	path := operationReplyServer(t, func(servicecontrol.Request) servicecontrol.Response {
		return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(data)}
	})
	f := &ownerFacade{intent: intent, client: &volumeOwnerClient{path: path}}
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	var calls atomic.Int32
	loop := startOwnerCallbackLoop(f, func() bool {
		calls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		return true
	}, func(string) {})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		if !loop.stopWithin(time.Second) {
			t.Error("original callback loop not joined")
		}
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("recovery callback never entered")
	}
	if loop.stopWithin(5 * time.Millisecond) {
		t.Fatal("shutdown claimed join while recovery callback remained held")
	}
	releaseOnce.Do(func() { close(release) })
	if !loop.stopWithin(time.Second) {
		t.Fatal("released callback failed to join")
	}
	if calls.Load() != 1 {
		t.Fatalf("stopped callback replayed: %d calls", calls.Load())
	}
}

func TestSelectedOwnerOperationValidatesImmediateReceipt(t *testing.T) {
	for _, field := range []string{"valid", "instance", "build", "config", "root", "operation"} {
		t.Run(field, func(t *testing.T) {
			intent := volumeOwnerIntent{Instance: "selected", ConfigDigest: "config", Root: "root"}
			const id = "0123456789abcdef0123456789abcdef"
			r := volumeOwnerReceipt{Instance: intent.Instance, Build: version.Commit,
				ConfigDigest: intent.ConfigDigest, IsolationRoot: intent.Root, Operation: id, OperationState: "complete"}
			switch field {
			case "instance":
				r.Instance = "foreign"
			case "build":
				r.Build = "foreign"
			case "config":
				r.ConfigDigest = "foreign"
			case "root":
				r.IsolationRoot = "foreign"
			case "operation":
				r.Operation = "foreign"
			}
			data, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var mutations atomic.Int32
			path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
				if req.Method == "query" {
					return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: "unknown original volume owner operation"}
				}
				mutations.Add(1)
				return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(data)}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = selectedOwnerOperation(ctx, path, intent, "mount", id, nil, []int{1})
			if mutations.Load() != 1 {
				t.Fatal("did not exercise immediate mutation reply exactly once")
			}
			if field == "valid" && err != nil {
				t.Fatalf("valid immediate completion refused: %v", err)
			}
			if field != "valid" && err == nil {
				t.Fatalf("immediate completion accepted foreign %s", field)
			}
		})
	}
}

func TestSelectedOwnerOperationPreservesSemanticRejection(t *testing.T) {
	intent := volumeOwnerIntent{Instance: "selected", ConfigDigest: "config", Root: "root"}
	const id = "0123456789abcdef0123456789abcdef"
	r := volumeOwnerReceipt{Instance: intent.Instance, Build: version.Commit,
		ConfigDigest: intent.ConfigDigest, IsolationRoot: intent.Root, Operation: id, OperationState: "complete"}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var queries atomic.Int32
	path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
		if req.Method == "query" {
			if queries.Add(1) == 1 {
				return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: "unknown original volume owner operation"}
			}
			return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(data)}
		}
		return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: "volume owner operation ID rebound"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := selectedOwnerOperation(ctx, path, intent, "mount", id, nil, []int{0}); err == nil {
		t.Fatal("rejected new payload was replaced by old operation success")
	}
	if queries.Load() != 1 {
		t.Fatal("semantic rejection was treated as lost transport reply")
	}
}

func TestSelectedOwnerOperationReconnectUsesOriginal(t *testing.T) {
	intent := volumeOwnerIntent{Instance: "selected", ConfigDigest: "config", Root: "root"}
	const id = "0123456789abcdef0123456789abcdef"
	r := volumeOwnerReceipt{Instance: intent.Instance, Build: version.Commit,
		ConfigDigest: intent.ConfigDigest, IsolationRoot: intent.Root, Operation: id, OperationState: "complete",
		MountPath: "/original-generation"}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var queries, mutations atomic.Int32
	path := operationReplyServer(t, func(req servicecontrol.Request) servicecontrol.Response {
		if req.Method == "query" {
			queries.Add(1)
			return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Text: string(data)}
		}
		mutations.Add(1)
		return servicecontrol.Response{Version: servicecontrol.ProtocolVersion, Error: "must retain original activation"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := selectedOwnerOperation(ctx, path, intent, "mount", id, nil, []int{0})
	if err != nil || got.MountPath != r.MountPath || queries.Load() != 1 || mutations.Load() != 0 {
		t.Fatalf("reconnect did not reuse exact original activation: receipt=%+v err=%v queries=%d mutations=%d", got, err, queries.Load(), mutations.Load())
	}
}
