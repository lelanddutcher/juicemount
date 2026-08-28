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

func TestDerivativePlanSeparatesMetadataFromVerifiedPreviewDecode(t *testing.T) {
	parent := farmqueue.Job{
		ID: "deriv-parent", Path: "/jfs/incoming", Kinds: []string{farmqueue.KindDerivatives},
		Producer: "manager", PlanOnly: true, QueueClass: farmqueue.QueueClassServer,
	}
	targets := []string{"/jfs/incoming/prores.mov", "/jfs/incoming/audio.wav", "/jfs/incoming/h264.mp4", "/jfs/incoming/broken.mov"}
	probe := func(path string) (*farm.VideoTrack, error) {
		switch {
		case strings.HasSuffix(path, "audio.wav"):
			return nil, nil
		case strings.HasSuffix(path, "h264.mp4"):
			return &farm.VideoTrack{Codec: "h264", PixFmt: "yuv420p", BitDepth: 8}, nil
		case strings.HasSuffix(path, "prores.mov"):
			return &farm.VideoTrack{Codec: "prores", PixFmt: "yuv422p10le", BitDepth: 10}, nil
		default:
			return nil, errors.New("truncated header")
		}
	}
	route := func(_ context.Context, job *farmqueue.Job) {
		if job.SourceVideoCodec == "h264" {
			job.QueueClass = farmqueue.QueueClassRender
			job.SelectedBackend = "h264_vaapi"
			job.SelectedWorker = "ephemeral-render-id"
			job.RequiredCapabilities = []string{"decoder:h264_vaapi", "worker:ephemeral-render-id"}
			return
		}
		routePreviewCPU(job, "no compatible decoder")
	}

	children, err := buildDerivativePlan(context.Background(), route, parent, targets, probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 4 { // one metadata shard + three video/error previews; audio has no preview
		t.Fatalf("children=%d, want 4: %+v", len(children), children)
	}
	ids := map[string]bool{}
	var metadata, render, cpu int
	for _, child := range children {
		if ids[child.ID] || child.ParentID != parent.ID || child.PlanOnly || child.ShardCount != len(children) || child.ShardIndex < 1 {
			t.Fatalf("bad child identity/bounds: %+v", child)
		}
		ids[child.ID] = true
		switch child.DerivativePass {
		case farmqueue.DerivativePassMetadata:
			metadata++
			if child.QueueClass != farmqueue.QueueClassServer || child.SelectedBackend != "server-metadata" ||
				!reflect.DeepEqual(child.RequiredCapabilities, []string{"metadata"}) || len(child.RetryTargets) != len(targets) {
				t.Fatalf("metadata child=%+v", child)
			}
		case farmqueue.DerivativePassPreviews:
			if len(child.RetryTargets) != 1 {
				t.Fatalf("preview child is not one-file bounded: %+v", child)
			}
			if child.QueueClass == farmqueue.QueueClassRender {
				render++
				if child.SelectedBackend != "h264_vaapi" || child.SelectedWorker != "" ||
					!reflect.DeepEqual(child.RequiredCapabilities, []string{"decoder:h264_vaapi"}) {
					t.Fatalf("render preview retained a CPU path or exact worker pin: %+v", child)
				}
			} else {
				cpu++
				if child.QueueClass != farmqueue.QueueClassCPU || child.SelectedBackend != "cpu-decode" || child.RoutingReason == "" {
					t.Fatalf("CPU preview fallback is not explicit: %+v", child)
				}
			}
		default:
			t.Fatalf("unknown derivative pass: %+v", child)
		}
	}
	if metadata != 1 || render != 1 || cpu != 2 {
		t.Fatalf("planned lanes metadata=%d render=%d cpu=%d", metadata, render, cpu)
	}

	// Device availability may change during durable-parent replay, but child IDs
	// remain source/pass based so the Redis transaction cannot duplicate work.
	offlineRoute := func(_ context.Context, job *farmqueue.Job) { routePreviewCPU(job, "GPU offline") }
	replayed, err := buildDerivativePlan(context.Background(), offlineRoute, parent, targets, probe)
	if err != nil {
		t.Fatal(err)
	}
	replayedIDs := map[string]bool{}
	for _, child := range replayed {
		replayedIDs[child.ID] = true
	}
	if !reflect.DeepEqual(ids, replayedIDs) {
		t.Fatalf("replay IDs changed with route availability: first=%v replay=%v", ids, replayedIDs)
	}
}

func TestPreviewLiveAdmissionFallsStraightToVisibleCPU(t *testing.T) {
	worker := farmqueue.Worker{Role: farmqueue.QueueClassRender}
	job := farmqueue.Job{Kinds: []string{farmqueue.KindDerivatives}, DerivativePass: farmqueue.DerivativePassPreviews}
	requeue, cpu, reason := renderFailureDisposition(worker, job, fmtError(errRenderIncompatible))
	if !requeue || !cpu || !strings.Contains(reason, "explicit CPU decode") {
		t.Fatalf("incompatible preview disposition=%v/%v/%q", requeue, cpu, reason)
	}
}

func TestDerivativePlanCarriesMeasuredProfileAdmission(t *testing.T) {
	parent := farmqueue.Job{
		ID: "profile-parent", Path: "/jfs/incoming", Kinds: []string{farmqueue.KindDerivatives},
		Producer: "manager", PlanOnly: true, QueueClass: farmqueue.QueueClassServer,
	}
	route := func(_ context.Context, job *farmqueue.Job) {
		job.QueueClass = farmqueue.QueueClassRender
		job.SelectedBackend = "h264_vaapi"
		job.SelectedWorker = "intel"
		job.RequiredCapabilities = append(
			farmqueue.DecoderRequirements("h264_vaapi", job.SourceVideoCodec, job.SourceVideoProfile),
			"worker:intel",
		)
	}
	children, err := buildDerivativePlan(context.Background(), route, parent, []string{"/jfs/incoming/main.mp4"}, func(string) (*farm.VideoTrack, error) {
		return &farm.VideoTrack{Codec: "h264", Profile: "Main", PixFmt: "yuv420p", BitDepth: 8}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 2 {
		t.Fatalf("children=%d, want metadata + preview", len(children))
	}
	preview := children[1]
	want := []string{"decoder:h264_vaapi", "decoder:h264_vaapi:profile:main"}
	if preview.SourceVideoProfile != "main" || preview.SelectedWorker != "" ||
		!reflect.DeepEqual(preview.RequiredCapabilities, want) {
		t.Fatalf("profile-aware preview=%+v, want capabilities %v", preview, want)
	}
}

func fmtError(cause error) error { return errors.Join(errors.New("preview admission"), cause) }
