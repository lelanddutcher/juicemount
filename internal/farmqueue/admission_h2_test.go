package farmqueue

import (
	"context"
	"testing"
	"time"
)

func TestAdmissionH2BlockedWorkerCannotClaimOrRoute(t *testing.T) {
	w := Worker{ID: "render-1", Role: QueueClassRender, AdmissionBlocked: true,
		Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi"},
		TranscriptBackends: []string{"vulkan"}, Capabilities: []string{"decoder:h264_vaapi"}}
	if keys := WorkerQueueKeys(w); len(keys) != 0 {
		t.Fatalf("blocked worker has claim lanes: %v", keys)
	}
	if WorkerSupports(w, []string{"encoder:hevc_vaapi"}) || workerCanClaimJob(w, Job{}) {
		t.Fatal("blocked worker passed eligibility")
	}
	if _, _, ok := preferredHardwareWorker([]Worker{w}); ok {
		t.Fatal("blocked encoder selected")
	}
	if _, _, ok := preferredTranscriptWorker([]Worker{w}); ok {
		t.Fatal("blocked transcript backend selected")
	}
	if _, _, ok := preferredHardwareDecoderWorkerForSourceWithLoad([]Worker{w}, "h264", "", "", 0, 0, nil); ok {
		t.Fatal("blocked decoder selected")
	}
	var client *Client
	if _, ok, err := client.ClaimForWorker(context.Background(), time.Second, w); ok || err != nil {
		t.Fatalf("blocked claim touched queue: ok=%v err=%v", ok, err)
	}
}

func TestAdmissionH2RenderLaneVerdictRequiresMeasuredProbeAndRate(t *testing.T) {
	base := Worker{ID: "render", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"},
		Decoders: []string{"h264_vaapi"}, TranscriptBackends: []string{"vulkan"},
		Benchmarks: WorkerBenchmarks{ProbedAt: nowISO(), EncodeFPS: 20, DecodeFPS: 18, TranscriptXReal: 2}}
	for _, tc := range []struct {
		name   string
		mutate func(*Worker)
		kind   string
		decode bool
		want   bool
	}{
		{name: "all measured", kind: KindProxy, want: true},
		{name: "proxy decode path measured", kind: KindProxy, decode: true, want: true},
		{name: "proxy decode rate missing", kind: KindProxy, decode: true, mutate: func(w *Worker) { w.Benchmarks.DecodeFPS = 0 }},
		{name: "proxy encode rate missing", kind: KindProxy, mutate: func(w *Worker) { w.Benchmarks.EncodeFPS = 0 }},
		{name: "decoder inventory missing", kind: KindDerivatives, mutate: func(w *Worker) { w.Decoders = nil }},
		{name: "decoder rate missing", kind: KindDerivatives, mutate: func(w *Worker) { w.Benchmarks.DecodeFPS = 0 }},
		{name: "transcript cpu only", kind: KindTranscript, mutate: func(w *Worker) { w.TranscriptBackends = []string{"cpu"} }},
		{name: "transcript rate missing", kind: KindTranscript, mutate: func(w *Worker) { w.Benchmarks.TranscriptXReal = 0 }},
		{name: "probe missing", kind: KindProxy, mutate: func(w *Worker) { w.Benchmarks.ProbedAt = "" }},
		{name: "blocked", kind: KindProxy, mutate: func(w *Worker) { w.AdmissionBlocked = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := base
			if tc.mutate != nil {
				tc.mutate(&worker)
			}
			got := renderLaneProfile(worker, tc.kind, tc.decode).eligible
			if got != tc.want {
				t.Fatalf("render lane eligible=%v; want %v", got, tc.want)
			}
		})
	}

	worker := base
	worker.Benchmarks.DecodeFPS = 0
	if keys := WorkerQueueKeys(worker); h2ContainsString(keys, classQueue(KindDerivatives, QueueClassRender)) {
		t.Fatalf("unmeasured decoder exposed claim lane: %v", keys)
	}
	job := Job{QueueClass: QueueClassRender, Kinds: []string{KindProxy}, RequiredCapabilities: []string{"encoder:hevc_vaapi", "decoder:h264_vaapi"}}
	if workerCanClaimJob(worker, job) {
		t.Fatal("source-aware proxy accepted without measured decode rate")
	}
}

func h2ContainsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
