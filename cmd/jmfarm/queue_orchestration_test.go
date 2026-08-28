package main

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestOnlyServerWorkerRunsDiscovery(t *testing.T) {
	t.Setenv("JM_FARM_WATCH", "1")
	if !workerRunsDiscovery(farmqueue.Worker{Role: farmqueue.QueueClassServer}) {
		t.Fatal("server worker should own recursive discovery")
	}
	if workerRunsDiscovery(farmqueue.Worker{Role: farmqueue.QueueClassRender}) {
		t.Fatal("render worker must not own recursive discovery")
	}
	t.Setenv("JM_FARM_WATCH", "0")
	if workerRunsDiscovery(farmqueue.Worker{Role: farmqueue.QueueClassServer}) {
		t.Fatal("watch kill switch did not disable server discovery")
	}
}

func TestNoMediaJobDoesNotPolluteBenchmarks(t *testing.T) {
	w := farmqueue.Worker{}
	recordCompletedJobBenchmark(&w, farmqueue.Job{Kinds: []string{farmqueue.KindProxy}}, 0, 0, time.Millisecond)
	if w.Benchmarks.JobsCompleted != 0 || w.Benchmarks.LastJobSeconds != 0 || w.Benchmarks.ProxyJobsCompleted != 0 {
		t.Fatalf("no-op job changed benchmarks: %+v", w.Benchmarks)
	}

	recordCompletedJobBenchmark(&w, farmqueue.Job{Kinds: []string{farmqueue.KindProxy}}, 1, 0, 2*time.Second)
	if w.Benchmarks.JobsCompleted != 1 || w.Benchmarks.ProxyJobsCompleted != 1 || w.Benchmarks.LastProxySeconds != 2 ||
		w.Benchmarks.FilesProcessed != 1 || w.Benchmarks.ProxyFilesProcessed != 1 {
		t.Fatalf("real proxy job not recorded: %+v", w.Benchmarks)
	}
	recordCompletedJobBenchmark(&w, farmqueue.Job{Kinds: []string{farmqueue.KindTranscript}}, 0, 1, 3*time.Second)
	if w.Benchmarks.JobsCompleted != 2 || w.Benchmarks.TranscriptJobsCompleted != 1 || w.Benchmarks.LastTranscriptSeconds != 3 ||
		w.Benchmarks.FilesFailed != 1 || w.Benchmarks.TranscriptFilesFailed != 1 {
		t.Fatalf("failed transcript sample not recorded: %+v", w.Benchmarks)
	}
}

func TestTargetShardSizeBoundsLeaseWithoutDiscardingParallelism(t *testing.T) {
	proxy := farmqueue.Job{Kinds: []string{farmqueue.KindProxy}}
	if got := targetShardSize(proxy); got != 4 {
		t.Fatalf("proxy shard size = %d, want 4", got)
	}
	proxy.ProxyWorkers = 32
	if got := targetShardSize(proxy); got != 8 {
		t.Fatalf("proxy shard cap = %d, want 8", got)
	}
	if got := targetShardSize(farmqueue.Job{Kinds: []string{farmqueue.KindTranscript}}); got != 1 {
		t.Fatalf("transcript shard size = %d, want 1", got)
	}
	if got := targetShardSize(farmqueue.Job{Kinds: []string{farmqueue.KindDerivatives}}); got != 32 {
		t.Fatalf("derivative shard size = %d, want 32", got)
	}
	if got := targetShardSize(farmqueue.Job{Kinds: []string{farmqueue.KindProxy}, ShardIndex: 1, ShardCount: 5}); got != 0 {
		t.Fatalf("existing shard was split recursively: %d", got)
	}
	if got := targetShardSize(farmqueue.Job{Kinds: []string{farmqueue.KindProxy, farmqueue.KindDerivatives}}); got != 0 {
		t.Fatalf("multi-kind compatibility job was split: %d", got)
	}
}

func TestPlanOnlyParentDispatchesEvenOneTarget(t *testing.T) {
	job := farmqueue.Job{Kinds: []string{farmqueue.KindTranscript}, PlanOnly: true}
	if got := targetShardSize(job); got != 1 {
		t.Fatalf("planner transcript shard size=%d, want one", got)
	}
}
