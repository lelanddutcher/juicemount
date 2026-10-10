package main

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestRenderH2SlowFailedRecoveredProbe(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cfg := queueConfig{role: farmqueue.QueueClassRender, tDevice: "vulkan"}
	cfg.probeProfile = func(ctx context.Context, _ queueConfig, _ *derivatives.Store) (workerProfile, error) {
		close(started)
		select {
		case <-release:
			return workerProfile{}, errRenderProbeUnavailable
		case <-ctx.Done():
			return workerProfile{}, ctx.Err()
		}
	}
	w := farmqueue.Worker{ID: "stable-id", Role: farmqueue.QueueClassRender}
	blockRenderWorker(&w, "accelerator probe in progress")
	run := startRenderProbe(context.Background(), cfg, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow probe never started")
	}
	if !w.AdmissionBlocked || w.Role != farmqueue.QueueClassRender || w.ID != "stable-id" || len(farmqueue.WorkerQueueKeys(w)) != 0 {
		t.Fatalf("slow probe exposed admission: %+v", w)
	}
	close(release)
	result := <-run.result
	run.cancel()
	if !errors.Is(result.err, errRenderProbeUnavailable) {
		t.Fatalf("failed probe: %v", result.err)
	}
	blockRenderWorker(&w, result.err.Error())
	if !w.AdmissionBlocked || w.Benchmarks.ProbeError == "" {
		t.Fatalf("failure not visible: %+v", w)
	}
	cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
		return workerProfile{Role: farmqueue.QueueClassRender, Encoders: []string{"hevc_vaapi"},
			Benchmarks: farmqueue.WorkerBenchmarks{EncodeFPS: 24, ProbedAt: time.Now().UTC().Format(time.RFC3339)}}, nil
	}
	run = startRenderProbe(context.Background(), cfg, nil)
	result = <-run.result
	run.cancel()
	if result.err != nil {
		t.Fatal(result.err)
	}
	installRenderProfile(&w, result.profile)
	if w.AdmissionBlocked || w.ID != "stable-id" || len(farmqueue.WorkerQueueKeys(w)) == 0 {
		t.Fatalf("recovered profile not admitted: %+v", w)
	}
}

func TestRenderH2StaleProbeAndPostFailureWithdrawal(t *testing.T) {
	started := make(chan struct{})
	cfg := queueConfig{role: farmqueue.QueueClassRender, wModel: "old-model"}
	cfg.probeProfile = func(ctx context.Context, _ queueConfig, _ *derivatives.Store) (workerProfile, error) {
		close(started)
		<-ctx.Done()
		return workerProfile{}, ctx.Err()
	}
	run := startRenderProbe(context.Background(), cfg, nil)
	<-started
	cfg.wModel = "new-model"
	run.cancel()
	result := <-run.result
	if result.configKey == renderProbeConfigKey(cfg) || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("stale probe could be installed: %+v", result)
	}
	w := farmqueue.Worker{ID: "stable-id", Role: farmqueue.QueueClassRender}
	installRenderProfile(&w, workerProfile{Encoders: []string{"hevc_vaapi"}})
	job := farmqueue.Job{QueueClass: farmqueue.QueueClassRender}
	requeue, _, _ := renderFailureDisposition(w, job, errRenderExecution)
	if !requeue {
		t.Fatal("hardware execution failure did not request durable requeue")
	}
	// The queue loop applies this withdrawal only after requeue succeeds.
	blockRenderWorker(&w, "accelerator execution failed")
	if !w.AdmissionBlocked || len(w.Encoders) != 0 || len(farmqueue.WorkerQueueKeys(w)) != 0 {
		t.Fatalf("failed hardware remained claimable: %+v", w)
	}
	if requeue, _, _ := renderFailureDisposition(w, job, errRenderIncompatible); !requeue {
		t.Fatal("source incompatibility lost its CPU fallback")
	}
}

func advanceUntil(t *testing.T, a *renderAdmission, cfg queueConfig, w *farmqueue.Worker, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !done() {
		if err := a.advance(context.Background(), cfg, w); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("render admission transition timed out")
		}
		runtime.Gosched()
	}
}

func TestRenderH2AutoRenderLoopLifecycle(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	probes := 0
	cfg := queueConfig{role: "auto", wModel: "first"}
	cfg.probeProfile = func(ctx context.Context, got queueConfig, _ *derivatives.Store) (workerProfile, error) {
		if got.role != farmqueue.QueueClassRender {
			return workerProfile{}, errors.New("recovery probe lost render role")
		}
		probes++ // probes are serial; the loop joins each result before starting another
		if probes == 1 {
			close(started)
			select {
			case <-release:
				return workerProfile{}, errRenderProbeUnavailable
			case <-ctx.Done():
				return workerProfile{}, ctx.Err()
			}
		}
		return workerProfile{Role: farmqueue.QueueClassRender, Encoders: []string{"hevc_vaapi"}}, nil
	}
	w := farmqueue.Worker{ID: "auto-render-id", Role: farmqueue.QueueClassRender,
		Encoders: []string{"hevc_vaapi"}}
	a := &renderAdmission{}
	defer a.close()
	if err := a.advance(context.Background(), cfg, &w); err != nil {
		t.Fatal(err)
	}
	if !w.AdmissionBlocked {
		t.Fatal("auto-admitted render did not enter pinned verification")
	}
	var heartbeats []farmqueue.Worker
	heartbeat := func() { heartbeats = append(heartbeats, w) }
	heartbeat() // the real loop publishes before start
	a.start(context.Background(), cfg, nil, w)
	<-started
	for i := 0; i < 3; i++ {
		if err := a.advance(context.Background(), cfg, &w); err != nil {
			t.Fatal(err)
		}
		heartbeat()
	}
	for _, beat := range heartbeats {
		if !beat.AdmissionBlocked || beat.Role != farmqueue.QueueClassRender || beat.ID != "auto-render-id" {
			t.Fatalf("slow probe heartbeat lost identity or admission block: %+v", beat)
		}
	}
	close(release)
	advanceUntil(t, a, cfg, &w, func() bool { return a.probe == nil })
	if !w.AdmissionBlocked || w.Benchmarks.ProbeError == "" {
		t.Fatalf("failed probe lost visible blocked state: %+v", w)
	}
	a.nextProbe = time.Time{} // advance the retry timer without wall-clock sleep
	heartbeat()
	a.start(context.Background(), cfg, nil, w)
	advanceUntil(t, a, cfg, &w, func() bool { return !w.AdmissionBlocked })
	if w.ID != "auto-render-id" || w.Role != farmqueue.QueueClassRender || probes != 2 {
		t.Fatalf("auto render was not re-admitted from measured probe: worker=%+v probes=%d", w, probes)
	}
}

func TestRenderH2StaleLoopResultAndJoin(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cfg := queueConfig{role: farmqueue.QueueClassRender, wModel: "old"}
	cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
		close(started)
		<-release // deliberate late completion after cancellation
		return workerProfile{Role: farmqueue.QueueClassRender, Encoders: []string{"hevc_vaapi"}}, nil
	}
	w := farmqueue.Worker{ID: "same-id", Role: farmqueue.QueueClassRender, AdmissionBlocked: true}
	a := &renderAdmission{}
	a.start(context.Background(), cfg, nil, w)
	<-started
	newCfg := cfg
	newCfg.wModel = "new"
	newCfg.probeProfile = func(ctx context.Context, _ queueConfig, _ *derivatives.Store) (workerProfile, error) {
		<-ctx.Done()
		return workerProfile{}, ctx.Err()
	}
	if err := a.advance(context.Background(), newCfg, &w); err != nil {
		t.Fatal(err)
	}
	close(release)
	advanceUntil(t, a, newCfg, &w, func() bool { return a.probe == nil })
	if !w.AdmissionBlocked || len(w.Encoders) != 0 {
		t.Fatalf("stale result installed: %+v", w)
	}
	a.start(context.Background(), newCfg, nil, w)
	a.close() // cancellation joins the in-flight probe before store teardown
	if a.probe != nil {
		t.Fatal("probe not joined")
	}
}

func TestRenderH2DurableRequeueBeforeWithdrawalAndExitStatus(t *testing.T) {
	w := farmqueue.Worker{ID: "render-id", Role: farmqueue.QueueClassRender, Encoders: []string{"hevc_vaapi"}}
	a := &renderAdmission{}
	requeueErr := errors.New("durable requeue failed")
	if err := a.afterRequeue(&w, errRenderExecution, func() error {
		if w.AdmissionBlocked {
			t.Fatal("worker withdrew before requeue")
		}
		return requeueErr
	}); !errors.Is(err, requeueErr) || w.AdmissionBlocked {
		t.Fatalf("failed requeue changed admission: err=%v worker=%+v", err, w)
	}
	if err := a.afterRequeue(&w, errRenderExecution, func() error {
		if w.AdmissionBlocked {
			t.Fatal("worker withdrew before successful requeue")
		}
		return nil
	}); err != nil || !w.AdmissionBlocked {
		t.Fatalf("successful requeue did not withdraw hardware: err=%v worker=%+v", err, w)
	}
	if code := runQueue(queueConfig{}); code != 2 {
		t.Fatalf("invalid queue arguments exit=%d", code)
	}
	if code := runQueue(queueConfig{meta: "unused", mount: "/unused", role: "bad"}); code != 2 {
		t.Fatalf("invalid role exit=%d", code)
	}
}
