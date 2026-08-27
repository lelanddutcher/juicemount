package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestPassFailureRejectsPartialSuccess(t *testing.T) {
	if err := passFailure(20, 0); err != nil {
		t.Fatalf("clean pass returned %v", err)
	}
	err := passFailure(20, 1)
	if err == nil || !strings.Contains(err.Error(), "1 of 21") {
		t.Fatalf("partial failure was not surfaced: %v", err)
	}
}

func TestRenderFailureRetriesHardwareBeforeCPUFallback(t *testing.T) {
	worker := farmqueue.Worker{Role: farmqueue.QueueClassRender}
	job := farmqueue.Job{Kinds: []string{farmqueue.KindProxy}, Attempts: 0}
	runErr := errors.New("vaapi admission failed")

	requeue, cpu, reason := renderFailureDisposition(worker, job, runErr)
	if !requeue || cpu || !strings.Contains(reason, "verified hardware worker") {
		t.Fatalf("first failure = requeue %v cpu %v reason %q", requeue, cpu, reason)
	}

	job.Attempts = renderHardwareRetries
	requeue, cpu, reason = renderFailureDisposition(worker, job, runErr)
	if !requeue || !cpu || !strings.Contains(reason, "explicit CPU/H.264 fallback") {
		t.Fatalf("repeated failure = requeue %v cpu %v reason %q", requeue, cpu, reason)
	}

	worker.Role = farmqueue.QueueClassServer
	if requeue, _, _ := renderFailureDisposition(worker, job, runErr); requeue {
		t.Fatal("server failure entered render retry policy")
	}
	worker.Role = farmqueue.QueueClassRender
	if requeue, _, _ := renderFailureDisposition(worker, job, nil); requeue {
		t.Fatal("successful render entered failure retry policy")
	}
}

func TestCollectRetryTargetsStaysInsideOriginalScope(t *testing.T) {
	mount := t.TempDir()
	scope := filepath.Join(mount, "incoming")
	if err := os.MkdirAll(scope, 0o755); err != nil {
		t.Fatal(err)
	}
	failed := filepath.Join(scope, "failed.mp4")
	if err := os.WriteFile(failed, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := collectRetryTargets(mount, scope, []string{failed, failed}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != failed {
		t.Fatalf("retry targets = %v, want only %q", got, failed)
	}

	outside := filepath.Join(mount, "outside.mp4")
	if err := os.WriteFile(outside, []byte("media"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := collectRetryTargets(mount, scope, []string{outside}, 0); err == nil {
		t.Fatal("retry target outside the original directory scope was accepted")
	}
	if _, err := collectRetryTargets(mount, failed, []string{outside}, 0); err == nil {
		t.Fatal("retry target different from an original file scope was accepted")
	}
}
