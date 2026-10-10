package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

// One-shot proxy mode has no worker role admission; its explicit GPU encoder
// must reach both the transcode options and governor unchanged.
func TestOneShotHardwareCodecWiring(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	isFlagCodec := func(expr ast.Expr) bool {
		star, ok := expr.(*ast.StarExpr)
		if !ok {
			return false
		}
		id, ok := star.X.(*ast.Ident)
		return ok && id.Name == "vcodec"
	}
	var mainBody *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "main" {
			mainBody = fn.Body
			break
		}
	}
	if mainBody == nil {
		t.Fatal("main function missing")
	}
	optionsCodec, governorCodec := false, false
	ast.Inspect(mainBody, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if isFlagCodec(lhs) {
					t.Error("one-shot vcodec flag is rewritten before options/governor")
				}
			}
		case *ast.CompositeLit:
			selector, ok := n.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Options" {
				break
			}
			for _, elt := range n.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "ProxyVCodec" {
					optionsCodec = isFlagCodec(kv.Value)
				}
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "NewGovernor" && len(n.Args) > 1 {
				governorCodec = isFlagCodec(n.Args[1])
			}
		}
		return true
	})
	if !optionsCodec || !governorCodec {
		t.Fatalf("one-shot codec wiring: options=%v governor=%v", optionsCodec, governorCodec)
	}
}

func TestEffectiveVCodecByRole(t *testing.T) {
	for _, role := range []string{farmqueue.QueueClassCPU, farmqueue.QueueClassServer, farmqueue.QueueClassRender} {
		for _, codec := range []string{"libx264", "libx265", "h264_nvenc", "h264_qsv", "h264_vaapi", "hevc_nvenc", "hevc_qsv", "hevc_vaapi"} {
			want := codec
			if role != farmqueue.QueueClassRender && codec != "libx264" && codec != "libx265" {
				want = "libx264"
			}
			if got := effectiveVCodec(role, codec); got != want {
				t.Errorf("role=%s codec=%s: got %s, want %s", role, codec, got, want)
			}
		}
	}
}

func TestResolvedJobVCodecPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, role, defaultCodec, jobCodec, requested, effective string
	}{
		{"default clamp", "server", "hevc_qsv", "", "hevc_qsv", "libx264"},
		{"job clamp", "server", "libx265", "h264_nvenc", "h264_nvenc", "libx264"},
		{"software override", "server", "hevc_vaapi", "libx265", "libx265", "libx265"},
		{"render override", "render", "libx264", "hevc_vaapi", "hevc_vaapi", "hevc_vaapi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested, effective := resolvedJobVCodec(tc.role, tc.defaultCodec, tc.jobCodec)
			if requested != tc.requested || effective != tc.effective {
				t.Fatalf("resolved %s -> %s, want %s -> %s", requested, effective, tc.requested, tc.effective)
			}
		})
	}
}

func TestWorkerEffectiveSettingsVCodec(t *testing.T) {
	cfg := queueConfig{vcodec: "hevc_vaapi"}
	if got := workerEffectiveSettings(cfg, farmqueue.QueueClassServer)["vcodec"]; got != "libx264" {
		t.Fatalf("server heartbeat codec = %q", got)
	}
	if got := workerEffectiveSettings(cfg, farmqueue.QueueClassRender)["vcodec"]; got != "hevc_vaapi" {
		t.Fatalf("render heartbeat codec = %q", got)
	}
}

func TestHardwareBoundCPUClaimRerouteGuard(t *testing.T) {
	worker := farmqueue.Worker{ID: "cpu-1", Role: farmqueue.QueueClassServer}
	for _, job := range []farmqueue.Job{
		{SelectedBackend: "hevc_vaapi", RequiredCapabilities: []string{"cpu"}},
		{SelectedBackend: "libx264", RequiredCapabilities: []string{"encoder:hevc_vaapi"}},
		{SelectedBackend: "vulkan", RequiredCapabilities: []string{"cpu"}},
	} {
		calls := 0
		rerouted, err := rerouteHardwareBoundClaim(context.Background(), worker, job, func(_ context.Context, reason string) error {
			calls++
			if !strings.Contains(reason, "hardware-bound") || !strings.Contains(reason, "cpu-1") {
				t.Fatalf("reroute reason = %q", reason)
			}
			return nil
		})
		if err != nil || !rerouted || calls != 1 {
			t.Fatalf("job %+v: rerouted=%v calls=%d err=%v", job, rerouted, calls, err)
		}
	}
	tracked := farmqueue.Job{SelectedBackend: "hevc_vaapi", TrackedEpoch: "epoch"}
	called := false
	rerouted, err := rerouteHardwareBoundClaim(context.Background(), worker, tracked, func(context.Context, string) error {
		called = true
		return nil
	})
	if rerouted || err == nil || called {
		t.Fatalf("tracked bound claim: rerouted=%v called=%v err=%v", rerouted, called, err)
	}
	for _, job := range []farmqueue.Job{
		{SelectedBackend: "libx264", RequiredCapabilities: []string{"cpu"}},
		{SelectedBackend: "cpu-decode", RequiredCapabilities: []string{"cpu"}},
	} {
		rerouted, err := rerouteHardwareBoundClaim(context.Background(), worker, job, func(context.Context, string) error {
			t.Fatal("valid CPU claim rerouted")
			return nil
		})
		if err != nil || rerouted {
			t.Fatalf("valid CPU job %+v: rerouted=%v err=%v", job, rerouted, err)
		}
	}
}
