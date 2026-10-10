package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestBuildProxyPlanRoutesPerSourceBeforeRenderQueue(t *testing.T) {
	parent := farmqueue.Job{
		ID: "proxy-parent", Path: "/jfs/incoming", Kinds: []string{farmqueue.KindProxy},
		Producer: "manager", PlanOnly: true, QueueClass: farmqueue.QueueClassServer,
		RequiredCapabilities: []string{"metadata"},
	}
	tracks := map[string]*farm.VideoTrack{
		"/jfs/incoming/a-h264.mp4":   {Codec: "h264", Profile: "High", PixFmt: "yuv420p", BitDepth: 8, Width: 1920, Height: 1080},
		"/jfs/incoming/b-h264.mp4":   {Codec: "h264", Profile: "High", PixFmt: "yuv420p", BitDepth: 8, Width: 1920, Height: 1080},
		"/jfs/incoming/c-av1.mp4":    {Codec: "av1", Profile: "Main", PixFmt: "yuv420p10le", BitDepth: 10, Width: 3840, Height: 2160},
		"/jfs/incoming/d-prores.mov": {Codec: "prores", Profile: "HQ", PixFmt: "yuv422p10le", BitDepth: 10, Width: 4096, Height: 2160},
		"/jfs/incoming/e-audio.wav":  nil,
	}
	probe := func(path string) (*farm.VideoTrack, error) { return tracks[path], nil }
	route := func(_ context.Context, job *farmqueue.Job) {
		if job.SourceVideoCodec != "h264" {
			job.QueueClass = farmqueue.QueueClassCPU
			job.VCodec = "libx264"
			job.SelectedBackend = "libx264"
			job.RequiredCapabilities = []string{"cpu"}
			job.RoutingReason = "no verified hardware decode+encode path is online for " + job.SourceVideoCodec
			return
		}
		job.QueueClass = farmqueue.QueueClassRender
		job.VCodec = "hevc_vaapi"
		job.SelectedBackend = "hevc_vaapi"
		job.SelectedWorker = "gpu"
		job.RequiredCapabilities = []string{
			"encoder:hevc_vaapi", "decoder:h264_vaapi", "decoder:h264_vaapi:profile:high",
			"decoder:h264_vaapi:pixfmt:yuv420p", "worker:gpu-1",
		}
	}
	targets := []string{
		"/jfs/incoming/e-audio.wav", "/jfs/incoming/d-prores.mov", "/jfs/incoming/c-av1.mp4",
		"/jfs/incoming/b-h264.mp4", "/jfs/incoming/a-h264.mp4",
	}
	children, err := buildProxyPlan(t.Context(), route, parent, targets, 2, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 3 {
		t.Fatalf("children=%d, want one H.264 render batch plus AV1 and ProRes CPU batches: %+v", len(children), children)
	}

	var render, av1, prores *farmqueue.Job
	for i := range children {
		child := &children[i]
		if child.ShardIndex < 1 || child.ShardCount != 3 || child.PlanOnly || child.ParentID != parent.ID {
			t.Fatalf("invalid bounded child: %+v", child)
		}
		switch child.SourceVideoCodec {
		case "h264":
			render = child
		case "av1":
			av1 = child
		case "prores":
			prores = child
		}
	}
	if render == nil || render.QueueClass != farmqueue.QueueClassRender ||
		!reflect.DeepEqual(render.RetryTargets, []string{"/jfs/incoming/a-h264.mp4", "/jfs/incoming/b-h264.mp4"}) ||
		!reflect.DeepEqual(render.RequiredCapabilities, []string{"encoder:hevc_vaapi", "decoder:h264_vaapi", "decoder:h264_vaapi:profile:high", "decoder:h264_vaapi:pixfmt:yuv420p"}) {
		t.Fatalf("render child=%+v", render)
	}
	for codec, child := range map[string]*farmqueue.Job{"av1": av1, "prores": prores} {
		if child == nil || child.QueueClass != farmqueue.QueueClassCPU ||
			child.SelectedBackend != "libx264" || !strings.Contains(strings.ToLower(child.RoutingReason), codec) {
			t.Fatalf("%s fallback child=%+v", codec, child)
		}
	}
	if av1.CPUFallbackLocked {
		t.Fatal("temporarily unavailable AV1 hardware route was locked away from future promotion")
	}
	if !prores.CPUFallbackLocked {
		t.Fatal("unsupported ProRes source was left in a hardware promotion loop")
	}
}

func TestBuildProxyPlanIDsDoNotChangeWithWorkerAvailability(t *testing.T) {
	parent := farmqueue.Job{ID: "stable-parent", Path: "/jfs", Kinds: []string{farmqueue.KindProxy}, PlanOnly: true}
	targets := []string{"/jfs/a.mp4", "/jfs/b.mp4"}
	probe := func(string) (*farm.VideoTrack, error) {
		return &farm.VideoTrack{Codec: "h264", Profile: "High", PixFmt: "yuv420p", BitDepth: 8, Width: 1920, Height: 1080}, nil
	}
	renderRoute := func(_ context.Context, job *farmqueue.Job) {
		job.QueueClass = farmqueue.QueueClassRender
		job.SelectedBackend = "hevc_vaapi"
		job.RequiredCapabilities = []string{"encoder:hevc_vaapi", "decoder:h264_vaapi", "worker:gpu"}
	}
	cpuRoute := func(_ context.Context, job *farmqueue.Job) {
		job.QueueClass = farmqueue.QueueClassCPU
		job.SelectedBackend = "libx264"
		job.RequiredCapabilities = []string{"cpu"}
		job.RoutingReason = "no verified hardware decode+encode path is online for h264 profile high"
	}
	withGPU, err := buildProxyPlan(t.Context(), renderRoute, parent, targets, 2, probe)
	if err != nil {
		t.Fatal(err)
	}
	withoutGPU, err := buildProxyPlan(t.Context(), cpuRoute, parent, targets, 2, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(withGPU) != 1 || len(withoutGPU) != 1 || withGPU[0].ID != withoutGPU[0].ID {
		t.Fatalf("route change altered deterministic child identity: gpu=%+v cpu=%+v", withGPU, withoutGPU)
	}
	if withGPU[0].QueueClass != farmqueue.QueueClassRender || withoutGPU[0].QueueClass != farmqueue.QueueClassCPU {
		t.Fatalf("routes were not applied independently: gpu=%+v cpu=%+v", withGPU[0], withoutGPU[0])
	}
}

func TestBuildProxyPlanProbeFailureIsScopedToOneSource(t *testing.T) {
	parent := farmqueue.Job{ID: "probe-parent", Path: "/jfs", Kinds: []string{farmqueue.KindProxy}, PlanOnly: true}
	probe := func(path string) (*farm.VideoTrack, error) {
		if strings.Contains(path, "bad") {
			return nil, errors.New("truncated header")
		}
		return &farm.VideoTrack{Codec: "h264", Profile: "High", PixFmt: "yuv420p", BitDepth: 8}, nil
	}
	route := func(_ context.Context, job *farmqueue.Job) {
		job.QueueClass = farmqueue.QueueClassRender
		job.SelectedBackend = "hevc_vaapi"
		job.RequiredCapabilities = []string{"encoder:hevc_vaapi", "decoder:h264_vaapi"}
	}
	children, err := buildProxyPlan(t.Context(), route, parent, []string{"/jfs/good.mp4", "/jfs/bad.mov"}, 4, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 {
		t.Fatalf("probe failure contaminated compatible batch: %+v", children)
	}
	for _, child := range children {
		if strings.Contains(child.RetryTargets[0], "bad") &&
			(child.QueueClass != farmqueue.QueueClassCPU || !child.CPUFallbackLocked || !strings.Contains(child.RoutingReason, "truncated header")) {
			t.Fatalf("probe failure child=%+v", child)
		}
	}
}
