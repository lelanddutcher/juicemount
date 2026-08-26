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
