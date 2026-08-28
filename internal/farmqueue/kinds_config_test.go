package farmqueue

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestQueueKeyFor pins the routing rule: single explicit kind → dedicated
// queue; multi-kind / all / empty → catch-all.
func TestQueueKeyFor(t *testing.T) {
	cases := []struct {
		kinds []string
		want  string
	}{
		{[]string{"transcript"}, QueueKey + ":transcript"},
		{[]string{"proxy"}, QueueKey + ":proxy"},
		{[]string{"derivatives"}, QueueKey + ":derivatives"},
		{[]string{"all"}, QueueKey},
		{[]string{}, QueueKey},
		{nil, QueueKey},
		{[]string{"proxy", "transcript"}, QueueKey}, // multi-kind → catch-all
		{[]string{"not-a-kind"}, QueueKey},
		{[]string{"proxy"}, classQueue("proxy", QueueClassRender)},
	}
	for _, tc := range cases {
		job := &Job{Kinds: tc.kinds}
		if len(tc.kinds) == 1 && tc.kinds[0] == "proxy" && tc.want == classQueue("proxy", QueueClassRender) {
			job.QueueClass = QueueClassRender
		}
		got := QueueKeyFor(job)
		if got != tc.want {
			t.Errorf("QueueKeyFor(%v) = %q, want %q", tc.kinds, got, tc.want)
		}
	}
}

func TestWorkerQueueKeysAreDisjointByRole(t *testing.T) {
	server := WorkerQueueKeys(Worker{Role: QueueClassServer})
	render := WorkerQueueKeys(Worker{
		Role: QueueClassRender, Encoders: []string{"hevc_vaapi"}, Decoders: []string{"h264_vaapi", "hevc_vaapi"},
		TranscriptBackends: []string{"cpu", "vulkan"},
	})
	serverSet := map[string]bool{}
	for _, key := range server {
		serverSet[key] = true
	}
	for _, key := range render {
		if serverSet[key] {
			t.Fatalf("server/render both drain %q; accelerator jobs could race CPU", key)
		}
	}
	if !serverSet[classQueue(KindDerivatives, QueueClassServer)] ||
		!serverSet[classQueue(KindDerivatives, QueueClassCPU)] ||
		!serverSet[classQueue(KindProxy, QueueClassServer)] ||
		!serverSet[classQueue(KindTranscript, QueueClassServer)] ||
		!serverSet[classQueue(KindProxy, QueueClassCPU)] || !serverSet[QueueKey] {
		t.Fatalf("server queues = %v, want planning, CPU fallback, and legacy catch-all lanes", server)
	}
	if len(render) != 3 || render[0] != classQueue(KindDerivatives, QueueClassRender) ||
		render[1] != classQueue(KindProxy, QueueClassRender) || render[2] != classQueue(KindTranscript, QueueClassRender) {
		t.Fatalf("render queues = %v", render)
	}
}

func TestInitialCompositeKindsRouteThroughServerPlanner(t *testing.T) {
	for _, kind := range []string{KindDerivatives, KindProxy, KindTranscript} {
		job := Job{ID: "parent", Path: "/jfs/library", Kinds: []string{kind}}
		(&Client{}).RouteInitialJob(t.Context(), &job)
		if !job.PlanOnly || job.QueueClass != QueueClassServer || job.SelectedBackend != "server-dispatch" {
			t.Fatalf("initial %s route = %+v, want server planner", kind, job)
		}
		if !WorkerSupports(Worker{Role: QueueClassServer}, job.RequiredCapabilities) {
			t.Fatalf("server cannot satisfy %s planner capabilities %v", kind, job.RequiredCapabilities)
		}
	}
	preview := Job{ID: "preview", Path: "/jfs/library", Kinds: []string{KindDerivatives}, DerivativePass: DerivativePassPreviews}
	if shouldPlanOnServer(preview) {
		t.Fatal("bounded derivative execution pass was routed back through the planner")
	}

	child := Job{ID: "child", Path: "/jfs/library", Kinds: []string{KindProxy}, ShardIndex: 1, ShardCount: 2}
	if shouldPlanOnServer(child) {
		t.Fatal("bounded execution child was routed back through the planner")
	}
	fallback := Job{ID: "fallback", Path: "/jfs/library", Kinds: []string{KindProxy}, RetryTargets: []string{"clip.mov"}}
	if shouldPlanOnServer(fallback) {
		t.Fatal("exact retry subset was routed back through the planner")
	}
}

func TestPreviewDecoderSelectionUsesMeasuredDecodeAndCodec(t *testing.T) {
	workers := []Worker{
		{ID: "slow-h264", Role: QueueClassRender, Decoders: []string{"h264_vaapi"}, Benchmarks: WorkerBenchmarks{DecodeFPS: 90, AccessMBps: 500}},
		{ID: "fast-h264", Role: QueueClassRender, Decoders: []string{"h264_qsv"}, Benchmarks: WorkerBenchmarks{DecodeFPS: 240, AccessMBps: 500}},
		{ID: "hevc-only", Role: QueueClassRender, Decoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{DecodeFPS: 500, AccessMBps: 500}},
	}
	selected, decoder, ok := preferredHardwareDecoderWorkerWithLoad(workers, "h264", "", nil)
	if !ok || selected.ID != "fast-h264" || decoder != "h264_qsv" {
		t.Fatalf("H.264 decoder selection=%q/%q/%v", selected.ID, decoder, ok)
	}
	if _, _, ok := preferredHardwareDecoderWorkerWithLoad(workers, "prores", "", nil); ok {
		t.Fatal("unverified ProRes decoder was admitted")
	}
}

func TestPreviewDecoderSelectionRequiresMeasuredSourceProfile(t *testing.T) {
	intel := Worker{
		ID: "intel", Role: QueueClassRender, Decoders: []string{"h264_vaapi"},
		Capabilities: []string{
			"decoder:h264_vaapi:profile:main",
			"decoder:h264_vaapi:profile:high",
		},
	}
	if _, _, ok := preferredHardwareDecoderWorkerWithLoad([]Worker{intel}, "h264", "Baseline", nil); ok {
		t.Fatal("H.264 Baseline was admitted without a successful profile probe")
	}
	selected, decoder, ok := preferredHardwareDecoderWorkerWithLoad([]Worker{intel}, "h264", "Main", nil)
	if !ok || selected.ID != intel.ID || decoder != "h264_vaapi" {
		t.Fatalf("H.264 Main selection=%q/%q/%v, want measured Intel decoder", selected.ID, decoder, ok)
	}

	job := Job{
		Kinds: []string{KindDerivatives}, DerivativePass: DerivativePassPreviews,
		SourceVideoCodec: "h264", SourceVideoProfile: "Baseline",
	}
	routeJobWithWorkers(&job, []Worker{intel}, nil)
	if job.QueueClass != QueueClassCPU || job.SelectedBackend != "cpu-decode" ||
		!strings.Contains(job.RoutingReason, "profile baseline") {
		t.Fatalf("Baseline route=%+v, want visible CPU fallback", job)
	}
}

func TestNormalizeVideoProfile(t *testing.T) {
	tests := map[string]string{
		"Baseline": "baseline", "Constrained Baseline": "constrained_baseline", "Main": "main",
		"High": "high", "High 10": "high", "Extended": "extended",
	}
	for input, want := range tests {
		if got := NormalizeVideoProfile("h264", input); got != want {
			t.Errorf("NormalizeVideoProfile(h264, %q)=%q, want %q", input, got, want)
		}
	}
	if got := NormalizeVideoProfile("hevc", "Main 10"); got != "main10" {
		t.Fatalf("NormalizeVideoProfile(hevc, Main 10)=%q", got)
	}
}

func TestHardwarePreferenceIsHEVCThenObservedSpeed(t *testing.T) {
	workers := []Worker{
		{ID: "fast-h264", Role: QueueClassRender, Encoders: []string{"h264_nvenc"}, Benchmarks: WorkerBenchmarks{EncodeFPS: 400}},
		{ID: "hevc", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{EncodeFPS: 120}},
	}
	if got, ok := preferredHardwareEncoder(workers); !ok || got != "hevc_vaapi" {
		t.Fatalf("preferred encoder = %q/%v, want HEVC hardware", got, ok)
	}
	if selected, _, ok := preferredHardwareWorker(workers); !ok || selected.ID != "hevc" {
		t.Fatalf("selected worker = %q/%v, want HEVC worker", selected.ID, ok)
	}
	workers = []Worker{
		{ID: "server", Role: QueueClassServer, Encoders: []string{"hevc_vaapi"}},
		{ID: "render", Role: QueueClassRender, Encoders: []string{"h264_qsv"}},
	}
	if got, ok := preferredHardwareEncoder(workers); !ok || got != "h264_qsv" {
		t.Fatalf("preferred encoder = %q/%v, want eligible render encoder", got, ok)
	}
	workers = []Worker{
		{ID: "slow", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"}, Benchmarks: WorkerBenchmarks{EncodeFPS: 80}},
		{ID: "fast", Role: QueueClassRender, Encoders: []string{"hevc_qsv"}, Benchmarks: WorkerBenchmarks{EncodeFPS: 220}},
	}
	if selected, _, ok := preferredHardwareWorker(workers); !ok || selected.ID != "fast" {
		t.Fatalf("same-codec selection = %q/%v, want fastest observed worker", selected.ID, ok)
	}
}

func TestHardwarePreferenceBalancesMeasuredQueueWithinCodecFamily(t *testing.T) {
	workers := []Worker{
		{ID: "busy-fast", Role: QueueClassRender, CurrentJob: "running", Encoders: []string{"hevc_qsv"},
			Benchmarks: WorkerBenchmarks{EncodeFPS: 300, DecodeFPS: 300, AccessMBps: 500}},
		{ID: "idle-balanced", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"},
			Benchmarks: WorkerBenchmarks{EncodeFPS: 180, DecodeFPS: 180, AccessMBps: 500}},
	}
	selected, _, ok := preferredHardwareWorkerWithLoad(workers, map[string]int{"busy-fast": 3})
	if !ok || selected.ID != "idle-balanced" {
		t.Fatalf("load-aware selection = %q/%v, want idle-balanced", selected.ID, ok)
	}

	// HEVC preference is an invariant, not a soft score: even a heavily loaded
	// HEVC node remains the target while it is healthy; H.264 is only the next
	// hardware family when no verified HEVC encoder is online.
	workers = []Worker{
		{ID: "busy-hevc", Role: QueueClassRender, CurrentJob: "running", Encoders: []string{"hevc_vaapi"},
			Benchmarks: WorkerBenchmarks{EncodeFPS: 100, DecodeFPS: 100, AccessMBps: 100}},
		{ID: "idle-h264", Role: QueueClassRender, Encoders: []string{"h264_nvenc"},
			Benchmarks: WorkerBenchmarks{EncodeFPS: 500, DecodeFPS: 500, AccessMBps: 1000}},
	}
	selected, encoder, ok := preferredHardwareWorkerWithLoad(workers, map[string]int{"busy-hevc": 20})
	if !ok || selected.ID != "busy-hevc" || encoder != "hevc_vaapi" {
		t.Fatalf("codec priority selection = %q/%q/%v, want busy-hevc/hevc_vaapi", selected.ID, encoder, ok)
	}
}

func TestVideoCostUsesDecodeAndMountMeasurements(t *testing.T) {
	encodeOnlyFast := Worker{ID: "decode-bound", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 500, DecodeFPS: 40, AccessMBps: 80}}
	balanced := Worker{ID: "balanced", Role: QueueClassRender, Encoders: []string{"hevc_qsv"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 180, DecodeFPS: 180, AccessMBps: 500}}
	selected, _, ok := preferredHardwareWorkerWithLoad([]Worker{encodeOnlyFast, balanced}, nil)
	if !ok || selected.ID != "balanced" {
		t.Fatalf("measured pipeline selection = %q/%v, want balanced", selected.ID, ok)
	}
}

func TestVideoCostPenalizesObservedFailures(t *testing.T) {
	reliable := Worker{ID: "reliable", Role: QueueClassRender, Encoders: []string{"hevc_vaapi"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 200, DecodeFPS: 200, AccessMBps: 500,
			ProxyFilesProcessed: 20}}
	flaky := Worker{ID: "flaky", Role: QueueClassRender, Encoders: []string{"hevc_qsv"},
		Benchmarks: WorkerBenchmarks{EncodeFPS: 220, DecodeFPS: 220, AccessMBps: 500,
			ProxyFilesProcessed: 15, ProxyFilesFailed: 5}}
	selected, _, ok := preferredHardwareWorkerWithLoad([]Worker{flaky, reliable}, nil)
	if !ok || selected.ID != reliable.ID {
		t.Fatalf("reliability-aware selection = %q/%v, want reliable", selected.ID, ok)
	}
}

func TestTranscriptPreferenceUsesObservedSpeed(t *testing.T) {
	workers := []Worker{
		{ID: "slow", Role: QueueClassRender, TranscriptBackends: []string{"cpu", "vulkan"}, Benchmarks: WorkerBenchmarks{TranscriptXReal: 2}},
		{ID: "fast", Role: QueueClassRender, TranscriptBackends: []string{"cpu", "cuda"}, Benchmarks: WorkerBenchmarks{TranscriptXReal: 7}},
	}
	selected, backend, ok := preferredTranscriptWorker(workers)
	if !ok || selected.ID != "fast" || backend != "cuda" {
		t.Fatalf("selected transcript worker/backend = %q/%q/%v", selected.ID, backend, ok)
	}
}

func TestTranscriptPreferenceBalancesQueuedWork(t *testing.T) {
	workers := []Worker{
		{ID: "busy", Role: QueueClassRender, TranscriptBackends: []string{"cuda"},
			Benchmarks: WorkerBenchmarks{TranscriptXReal: 8, AccessMBps: 500}},
		{ID: "idle", Role: QueueClassRender, TranscriptBackends: []string{"vulkan"},
			Benchmarks: WorkerBenchmarks{TranscriptXReal: 4, AccessMBps: 500}},
	}
	selected, backend, ok := preferredTranscriptWorkerWithLoad(workers, map[string]int{"busy": 4})
	if !ok || selected.ID != "idle" || backend != "vulkan" {
		t.Fatalf("load-aware transcript selection = %q/%q/%v, want idle/vulkan", selected.ID, backend, ok)
	}
}

func TestQueueKeysForKinds(t *testing.T) {
	cases := []struct {
		name  string
		kinds []string
		want  []string
	}{
		{
			name:  "specific kinds",
			kinds: []string{KindTranscript, KindProxy, KindTranscript},
			want:  []string{QueueKey + ":transcript", QueueKey + ":proxy", QueueKey},
		},
		{
			name:  "all expands to every dedicated queue",
			kinds: []string{KindAll},
			want:  []string{QueueKey + ":derivatives", QueueKey + ":proxy", QueueKey + ":transcript", QueueKey},
		},
		{
			name:  "empty defaults to generic worker",
			kinds: nil,
			want:  []string{QueueKey + ":derivatives", QueueKey + ":proxy", QueueKey + ":transcript", QueueKey},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := QueueKeysForKinds(tc.kinds)
			if len(got) != len(tc.want) {
				t.Fatalf("keys = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("keys[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestFarmConfigRoundTrip ensures the config doc marshals/unmarshals cleanly
// and that unknown fields on the wire don't break decoding (forward compat).
func TestFarmConfigRoundTrip(t *testing.T) {
	in := FarmConfig{
		Revision: 7,
		Defaults: map[string]any{"crf": float64(21), "preset": "slow"},
		Overrides: map[string]map[string]any{
			"b70-gpu": {"transcript_device": "vulkan", "model": "whisper.cpp/large-v3"},
		},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an older/newer manager adding a field we don't know about.
	poked := append([]byte(nil), raw...)
	poked = append(poked[:len(poked)-1], []byte(`,"future_field":{"x":1}}`)...)
	var out FarmConfig
	if err := json.Unmarshal(poked, &out); err != nil {
		t.Fatalf("decode with unknown field: %v", err)
	}
	if out.Revision != 7 {
		t.Errorf("revision = %d, want 7", out.Revision)
	}
	patch := out.Overrides["b70-gpu"]
	if patch == nil || patch["transcript_device"] != "vulkan" {
		t.Errorf("override patch lost: %#v", patch)
	}
}

// TestWorkerHeartbeatAdditiveFields verifies the enriched heartbeat still
// decodes with an OLD reader's struct (unknown fields ignored).
type oldWorker struct {
	ID         string `json:"id"`
	StartedAt  string `json:"started_at"`
	LastSeen   string `json:"last_seen"`
	CurrentJob string `json:"current_job,omitempty"`
}

func TestWorkerHeartbeatAdditiveFields(t *testing.T) {
	w := Worker{
		ID:             "w1",
		BuildVersion:   "0.5.0",
		BuildCommit:    "0123456789abcdef",
		Name:           "b70-gpu",
		ConfigRevision: 3,
		PendingRestart: []string{"nice"},
		Capabilities:   []string{"vulkan", "vaapi"},
		Effective:      map[string]string{"crf": "21"},
	}
	raw, _ := json.Marshal(w)
	var legacy oldWorker
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.ID != "w1" {
		t.Errorf("legacy decode lost id")
	}
	var current Worker
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatal(err)
	}
	if current.BuildVersion != "0.5.0" || current.BuildCommit != "0123456789abcdef" {
		t.Fatalf("build provenance lost: version=%q commit=%q", current.BuildVersion, current.BuildCommit)
	}
}
