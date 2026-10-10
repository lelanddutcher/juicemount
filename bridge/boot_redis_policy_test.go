package main

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// This is an integration wiring guard: a helper-only policy test would still
// pass if one of the production serial/parallel branches passed false here.
func TestRedisClientForBootProductionWiring(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "cbridge.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var start, publicStart, automaticStart *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			switch fn.Name.Name {
			case "nfsServerStartSerialized":
				start = fn
			case "NFSServerStart":
				publicStart = fn
			case "NFSServerStartAutomatic":
				automaticStart = fn
			}
		}
	}
	if start == nil || publicStart == nil || automaticStart == nil {
		t.Fatal("production startup function or wrapper missing")
	}
	callCount := func(node ast.Node, name string) int {
		count := 0
		ast.Inspect(node, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
					count++
				}
			}
			return true
		})
		return count
	}
	boundPolicy, sharedBranches := false, false
	ast.Inspect(start.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "redisClientForBoot" && len(call.Args) == 5 {
				policy, ok := call.Args[3].(*ast.Ident)
				boundPolicy = ok && policy.Name == "bootUserOffline"
			}
		}
		if branch, ok := n.(*ast.IfStmt); ok && branch.Else != nil &&
			callCount(branch.Body, "connectBootRedis") == 1 && callCount(branch.Else, "connectBootRedis") == 1 {
			sharedBranches = true
		}
		return true
	})
	if !boundPolicy || !sharedBranches || callCount(start.Body, "redisClientForBoot") != 1 ||
		callCount(publicStart.Body, "nfsServerStartSerialized") != 1 ||
		callCount(automaticStart.Body, "nfsServerStartSerialized") != 1 {
		t.Fatal("serial and parallel startup must share persisted-offline Redis policy")
	}
}

func TestRedisClientForBootSkipsConnectedConstructorWhenProbeDeferred(t *testing.T) {
	want := &metadata.RedisClient{}
	connectedCalls := 0
	deferredCalls := 0

	got, err := redisClientForBootWith(
		"redis://example.invalid:6379/1",
		nil,
		false,
		false,
		5,
		func(string, *metadata.Store, int) (*metadata.RedisClient, error) {
			connectedCalls++
			return nil, errors.New("must not dial")
		},
		func(string, *metadata.Store) (*metadata.RedisClient, error) {
			deferredCalls++
			return want, nil
		},
	)

	if err != nil {
		t.Fatalf("redisClientForBootWith returned error: %v", err)
	}
	if got != want {
		t.Fatalf("client = %p, want deferred client %p", got, want)
	}
	if connectedCalls != 0 {
		t.Fatalf("connected constructor called %d times after the probe chose offline; want 0", connectedCalls)
	}
	if deferredCalls != 1 {
		t.Fatalf("deferred constructor called %d times, want 1", deferredCalls)
	}
}

func TestRedisClientForBootFallsBackAfterOnlineConnectFailure(t *testing.T) {
	want := &metadata.RedisClient{}
	wantErr := errors.New("redis ping timed out")
	connectedCalls := 0
	deferredCalls := 0

	got, err := redisClientForBootWith(
		"redis://example.invalid:6379/1",
		nil,
		true,
		false,
		5,
		func(string, *metadata.Store, int) (*metadata.RedisClient, error) {
			connectedCalls++
			return nil, wantErr
		},
		func(string, *metadata.Store) (*metadata.RedisClient, error) {
			deferredCalls++
			return want, nil
		},
	)

	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want original connect error %v", err, wantErr)
	}
	if got != want {
		t.Fatalf("client = %p, want deferred client %p", got, want)
	}
	if connectedCalls != 1 || deferredCalls != 1 {
		t.Fatalf("constructor calls = connected:%d deferred:%d, want 1 and 1", connectedCalls, deferredCalls)
	}
}

func TestRedisClientForBootPersistedOfflineDefersEvenWhenTCPReachable(t *testing.T) {
	want := &metadata.RedisClient{}
	connectedCalls, deferredCalls := 0, 0
	got, err := redisClientForBootWith(
		"redis://example.invalid:6379/1", nil, true, true, 5,
		func(string, *metadata.Store, int) (*metadata.RedisClient, error) {
			connectedCalls++
			return nil, errors.New("protocol PING must not run")
		},
		func(string, *metadata.Store) (*metadata.RedisClient, error) {
			deferredCalls++
			return want, nil
		},
	)
	if err != nil || got != want || connectedCalls != 0 || deferredCalls != 1 {
		t.Fatalf("persisted offline with TCP reachable = (%p, %v, connected %d, deferred %d); want deferred once without PING",
			got, err, connectedCalls, deferredCalls)
	}
}

func TestRedisClientForBootOnlineUsesConnectedConstructor(t *testing.T) {
	want := &metadata.RedisClient{}
	connectedCalls, deferredCalls := 0, 0
	got, err := redisClientForBootWith(
		"redis://example.invalid:6379/1", nil, true, false, 5,
		func(_ string, _ *metadata.Store, attempts int) (*metadata.RedisClient, error) {
			connectedCalls++
			if attempts != 5 {
				t.Fatalf("attempts = %d, want 5", attempts)
			}
			return want, nil
		},
		func(string, *metadata.Store) (*metadata.RedisClient, error) {
			deferredCalls++
			return nil, errors.New("must not defer online success")
		},
	)
	if err != nil || got != want || connectedCalls != 1 || deferredCalls != 0 {
		t.Fatalf("online constructor = (%p, %v, connected %d, deferred %d); want connected once",
			got, err, connectedCalls, deferredCalls)
	}
}

func TestRedisClientForBootPersistedOfflinePropagatesDeferredError(t *testing.T) {
	wantErr := errors.New("deferred constructor refused")
	connectedCalls, deferredCalls := 0, 0
	got, err := redisClientForBootWith(
		"redis://example.invalid:6379/1", nil, true, true, 5,
		func(string, *metadata.Store, int) (*metadata.RedisClient, error) {
			connectedCalls++
			return nil, errors.New("must not dial")
		},
		func(string, *metadata.Store) (*metadata.RedisClient, error) {
			deferredCalls++
			return nil, wantErr
		},
	)
	if got != nil || !errors.Is(err, wantErr) || connectedCalls != 0 || deferredCalls != 1 {
		t.Fatalf("offline constructor failure = (%p, %v, connected %d, deferred %d); want original error",
			got, err, connectedCalls, deferredCalls)
	}
}
