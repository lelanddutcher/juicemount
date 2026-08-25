package farmqueue

import (
	"encoding/json"
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
	}
	for _, tc := range cases {
		got := QueueKeyFor(&Job{Kinds: tc.kinds})
		if got != tc.want {
			t.Errorf("QueueKeyFor(%v) = %q, want %q", tc.kinds, got, tc.want)
		}
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
}
