//go:build serviceclient

package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

//go:embed cbridge.go
var clientExportSource string

// installControlFake also clears every JM_ variable for the test. The desktop
// start path refuses any JM_ override outside servingGUIEnvAllowed before it
// forwards anything (a GUI-only knob would silently not reach the launchd
// service). A developer or CI shell that exports, for example,
// JM_ENABLE_MEMBUF_SERVE=0 would otherwise turn every forwarded start into a
// local refusal, and a forwarding test would measure the shell, not the client.
func installControlFake(t *testing.T, call func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error)) {
	t.Helper()
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "JM_") {
			t.Setenv(key, "") // servingConfiguration ignores empty values
		}
	}
	oldCall, oldPath := callServingProcess, servingSocketPath
	callServingProcess, servingSocketPath = call, func() (string, error) { return "/private/fixture/control.sock", nil }
	pendingNetworkCost.valid = false
	t.Cleanup(func() { callServingProcess, servingSocketPath = oldCall, oldPath; pendingNetworkCost.valid = false })
}

func TestServingClientForwardsEveryExportWithoutOwningEngine(t *testing.T) {
	type operation struct {
		export, method string
		args           []string
		ints           []int
	}
	ops := []operation{
		{"NFSServerLinkTest", "link-test", []string{"{"}, nil}, {"NFSServerStart", "start", []string{"{"}, nil},
		{"NFSServerStartAutomatic", "start-automatic", []string{"{"}, nil}, {"NFSServerAutoStartEligible", "auto-start-eligible", nil, nil},
		{"NFSServerStop", "stop", nil, nil}, {"NFSServerStopMount", "stop-mount", nil, nil},
		{"NFSServerShutdown", "shutdown", nil, nil}, {"NFSServerShutdownForced", "shutdown-forced", nil, nil},
		{"NFSServerIsRunning", "is-running", nil, nil}, {"NFSServerStats", "stats", nil, nil},
		{"NFSServerMetrics", "metrics", nil, nil}, {"NFSServerSyncNow", "sync", nil, nil},
		{"NFSServerSearch", "search", []string{"needle", "folder"}, []int{17}},
		{"NFSServerPin", "pin", []string{"/fixture/file"}, nil}, {"NFSServerUnpin", "unpin", []string{"/fixture/file"}, nil},
		{"NFSServerCacheStatus", "cache-status", nil, nil}, {"NFSServerSetOffline", "set-offline", nil, []int{1}},
		{"NFSServerIsOffline", "is-offline", nil, nil}, {"NFSServerSetNetworkCost", "network-cost", nil, []int{1, 0}},
		{"NFSServerLog", "log", []string{"fixture"}, nil},
	}
	covered := map[string]bool{"NFSServerFreeString": true}
	var calls []servicecontrol.Request
	installControlFake(t, func(ctx context.Context, path string, req servicecontrol.Request) (servicecontrol.Response, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("unbounded GUI call")
		}
		calls = append(calls, req)
		return servicecontrol.Response{Version: 1, Text: "sentinel", Int: 37}, nil
	})
	for _, op := range ops {
		covered[op.export] = true
		before := len(calls)
		req := servicecontrol.Request{Version: 1, Build: version.Commit, Method: op.method, Strings: op.args, Ints: op.ints}
		dispatchServingRequest(req) // Calls the actual C export through its Go adapter.
		if len(calls) != before+1 || !reflect.DeepEqual(calls[len(calls)-1], req) {
			t.Fatalf("%s did not forward exactly once: %+v", op.export, calls[before:])
		}
	}
	f, err := parser.ParseFile(token.NewFileSet(), "cbridge.go", clientExportSource, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Doc == nil {
			continue
		}
		for _, comment := range fn.Doc.List {
			if strings.HasPrefix(comment.Text, "//export ") && !covered[fn.Name.Name] {
				t.Errorf("C export %s missing forwarding coverage", fn.Name.Name)
			}
		}
	}
	if NFSServerUsesServingService() != 1 {
		t.Fatal("desktop archive did not identify client-only role")
	}
	if globalServer != nil || globalFUSE != nil || globalStore != nil {
		t.Fatal("desktop acquired storage")
	}
}

func TestServingClientAmbiguousFailureNeverFallsBackOrRetries(t *testing.T) {
	calls := 0
	installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
		calls++
		return servicecontrol.Response{}, errors.New("reply lost after request write")
	})
	for _, method := range []string{"start", "start-automatic", "shutdown", "stop", "stop-mount", "shutdown-forced", "is-offline"} {
		before := calls
		req := servicecontrol.Request{Version: 1, Build: version.Commit, Method: method}
		if method == "start" || method == "start-automatic" {
			req.Strings = []string{"{}"}
		}
		got := dispatchServingRequest(req)
		if calls == before {
			t.Fatalf("%s was never forwarded (refused locally?): %+v", method, got)
		}
		if calls != before+1 {
			t.Fatalf("%s retried", method)
		}
		if (method == "start" || method == "start-automatic") && !strings.HasPrefix(got.Text, "error:") {
			t.Fatalf("start lost error: %+v", got)
		}
		if method == "is-offline" {
			if got.Int != -1 {
				t.Fatal("unknown became online")
			}
		} else if got.Int != 0 {
			t.Fatal("unknown became successful teardown")
		}
	}
	if globalServer != nil || globalStore != nil || globalFUSE != nil {
		t.Fatal("client failure acquired local engine")
	}
}

func TestServingClientRetainsNetworkCostBeforeServiceStarts(t *testing.T) {
	for _, startMethod := range []string{"start", "start-automatic"} {
		t.Run(startMethod, func(t *testing.T) {
			var methods []string
			available := false
			installControlFake(t, func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
				methods = append(methods, req.Method)
				if !available {
					return servicecontrol.Response{}, errors.New("not started")
				}
				if req.Method == "network-cost" && !reflect.DeepEqual(req.Ints, []int{1, 0}) {
					t.Error("cost lost")
				}
				return servicecontrol.Response{Version: 1}, nil
			})
			forwardNetworkCost(1, 0)
			available = true
			dispatchServingRequest(servicecontrol.Request{Version: 1, Build: version.Commit, Method: startMethod, Strings: []string{"{"}})
			if !reflect.DeepEqual(methods, []string{"network-cost", "network-cost", startMethod}) {
				t.Fatalf("startup order: %v", methods)
			}
		})
	}
}

func TestServingClientConfigurationPreservesExplicitCustodySettings(t *testing.T) {
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "JM_") {
			t.Setenv(key, "")
		}
	}
	old := resolveServingJuiceFS
	resolveServingJuiceFS = func() string { return "/custom/install/juicefs" }
	t.Cleanup(func() { resolveServingJuiceFS = old })
	t.Setenv("JM_SPOOL_DIR", "/custom/spool")
	t.Setenv("JM_SPOOL_SIZE_GB", "77")
	t.Setenv("JM_SPOOL_ENABLE", "1")
	raw, err := servingConfiguration(`{"redis_url":"redis://fixture","spool_size_gb":0}`)
	if err != nil {
		t.Fatal(err)
	}
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.JuiceFSBin != "/custom/install/juicefs" || cfg.SpoolDir != "/custom/spool" || cfg.SpoolSizeGB != 77 || !cfg.SpoolEnable || cfg.RedisURL != "redis://fixture" {
		t.Fatalf("lost config: %+v", cfg)
	}
	t.Setenv("JM_UNSUPPORTED_OVERRIDE", "must-not-be-logged")
	_, err = servingConfiguration(`{}`)
	if err == nil || !strings.Contains(err.Error(), "JM_UNSUPPORTED_OVERRIDE") || strings.Contains(err.Error(), "must-not-be-logged") {
		t.Fatalf("unsupported override should fail without values: %v", err)
	}
}

func TestServingClientConfigurationPreservesNativePreviewChoice(t *testing.T) {
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "JM_") {
			t.Setenv(key, "")
		}
	}
	old := resolveServingJuiceFS
	resolveServingJuiceFS = func() string { return "/custom/install/juicefs" }
	t.Cleanup(func() { resolveServingJuiceFS = old })
	for _, enabled := range []bool{false, true} {
		input, err := json.Marshal(ServerConfig{RedisURL: "redis://fixture", NativePreviewWarm: enabled})
		if err != nil {
			t.Fatal(err)
		}
		forwarded, err := servingConfiguration(string(input))
		if err != nil {
			t.Fatal(err)
		}
		var got ServerConfig
		if err := json.Unmarshal([]byte(forwarded), &got); err != nil {
			t.Fatal(err)
		}
		if got.NativePreviewWarm != enabled {
			t.Fatalf("forwarded native preview choice=%t, want %t", got.NativePreviewWarm, enabled)
		}
	}
}
