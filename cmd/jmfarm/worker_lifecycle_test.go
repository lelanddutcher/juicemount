package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestAdmissionCancellationRequiresSignal(t *testing.T) {
	signalCtx, stop := context.WithCancel(context.Background())
	defer stop()
	if admissionCanceledBySignal(signalCtx, context.Canceled) {
		t.Fatal("probe cancellation without a signal was treated as a clean stop")
	}
	stop()
	if !admissionCanceledBySignal(signalCtx, context.Canceled) {
		t.Fatal("signal cancellation during the probe was treated as admission failure")
	}
	if admissionCanceledBySignal(signalCtx, errors.New("render hardware probe failed")) {
		t.Fatal("hardware admission failure was treated as a clean stop")
	}
}

func TestMissingFarmConfigClearsNodePauseAndDrain(t *testing.T) {
	var paused, drain atomic.Bool
	config := &farmqueue.FarmConfig{
		Defaults: map[string]any{"paused": true},
		Overrides: map[string]map[string]any{
			"render-node": {"drain": true},
		},
	}
	setNodePauseDrain(config, "render-node", &paused, &drain)
	if !paused.Load() || !drain.Load() {
		t.Fatalf("configured controls: paused=%v drain=%v", paused.Load(), drain.Load())
	}
	setNodePauseDrain(nil, "render-node", &paused, &drain)
	if paused.Load() || drain.Load() {
		t.Fatalf("controls stayed latched after config deletion: paused=%v drain=%v", paused.Load(), drain.Load())
	}
	setNodePauseDrain(config, "render-node", &paused, &drain)
	if !paused.Load() || !drain.Load() {
		t.Fatalf("recreated controls were not applied: paused=%v drain=%v", paused.Load(), drain.Load())
	}
}

type fakeConfigSource struct {
	fc  *farmqueue.FarmConfig
	err error
}

func (f *fakeConfigSource) GetConfig(context.Context) (*farmqueue.FarmConfig, error) {
	return f.fc, f.err
}

// Both config polls in runQueue go through refreshNodeControls, so a nil-config
// guard reintroduced there (the original B-3 shape) fails here.
func TestRefreshNodeControlsClearsOnDeletedConfigKeepsOnError(t *testing.T) {
	var paused, drain atomic.Bool
	src := &fakeConfigSource{fc: &farmqueue.FarmConfig{
		Overrides: map[string]map[string]any{"render-node": {"paused": true, "drain": true}},
	}}
	if _, err := refreshNodeControls(context.Background(), src, "render-node", &paused, &drain); err != nil || !paused.Load() || !drain.Load() {
		t.Fatalf("configured controls not applied: err=%v paused=%v drain=%v", err, paused.Load(), drain.Load())
	}
	src.fc, src.err = nil, errors.New("redis unreachable")
	if _, err := refreshNodeControls(context.Background(), src, "render-node", &paused, &drain); err == nil || !paused.Load() || !drain.Load() {
		t.Fatalf("read error must keep last-known controls: err=%v paused=%v drain=%v", err, paused.Load(), drain.Load())
	}
	src.err = nil
	fc, err := refreshNodeControls(context.Background(), src, "render-node", &paused, &drain)
	if err != nil || fc != nil {
		t.Fatalf("deleted config: fc=%v err=%v", fc, err)
	}
	if paused.Load() || drain.Load() {
		t.Fatalf("controls stayed latched after config deletion: paused=%v drain=%v", paused.Load(), drain.Load())
	}
}
