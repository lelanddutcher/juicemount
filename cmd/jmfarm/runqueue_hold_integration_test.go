//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	"github.com/redis/go-redis/v9"
)

// The hardware profile and ffprobe are deterministic fixtures. This exercises
// the actual worker loop and process restart, not hardware encoding acceptance.
func TestRunQueueAmbiguousFallbackDoesNotStarveAfterRestart(t *testing.T) {
	if os.Getenv("JM_FARM_HOLD_CHILD") == "1" {
		root := os.Getenv("JM_FARM_HOLD_ROOT")
		cfg := queueConfig{meta: os.Getenv("JM_FARM_TEST_REDIS_URL"), mount: root,
			dbPath: filepath.Join(root, "worker.db"), name: "hold-worker", producer: "hold-test",
			role: farmqueue.QueueClassRender, kinds: []string{farmqueue.KindProxy, farmqueue.KindTranscript},
			vcodec: "hevc_vaapi", tDevice: "vulkan", conc: 1, pConc: 1, version: 1}
		cfg.probeProfile = func(context.Context, queueConfig, *derivatives.Store) (workerProfile, error) {
			return workerProfile{Role: farmqueue.QueueClassRender,
				Capabilities: []string{"encoder:hevc_vaapi", "transcript:vulkan"},
				Encoders:     []string{"hevc_vaapi"}, TranscriptBackends: []string{"vulkan"},
				Benchmarks: farmqueue.WorkerBenchmarks{EncodeFPS: 120, TranscriptXReal: 2, ProbedAt: time.Now().UTC().Format(time.RFC3339)}}, nil
		}
		os.Exit(runQueue(cfg))
	}
	meta := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if meta == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Skip("requires isolated owned Redis")
	}
	opt, err := redis.ParseURL(meta)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	q, err := farmqueue.Open(meta)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	root := t.TempDir()
	empty := filepath.Join(root, "empty")
	tools := filepath.Join(root, "tools")
	for _, dir := range []string{empty, tools} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	media := filepath.Join(root, "clip.mov")
	if err := os.WriteFile(media, []byte("owned synthetic video"), 0600); err != nil {
		t.Fatal(err)
	}
	probe := `#!/bin/sh
printf '%s\n' '{"streams":[{"codec_type":"video","codec_name":"prores","width":1920,"height":1080,"r_frame_rate":"24/1","avg_frame_rate":"24/1"}],"format":{"format_name":"mov","duration":"10","size":"20"}}'
`
	if err := os.WriteFile(filepath.Join(tools, "ffprobe"), []byte(probe), 0700); err != nil {
		t.Fatal(err)
	}
	children := map[string]map[string]string{}
	for i := 0; i < 2; i++ {
		job := farmqueue.Job{ID: fmt.Sprintf("hold-parent-%d", i), Path: root, Kinds: []string{farmqueue.KindProxy}, Producer: "test",
			QueueClass: farmqueue.QueueClassRender, SelectedBackend: "hevc_vaapi", VCodec: "hevc_vaapi",
			RequiredCapabilities: []string{"encoder:hevc_vaapi"}, RetryTargets: []string{media}, Attempts: i + 10}
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		old := job
		old.ID = job.ID + "-cpu"
		old.ParentID = job.ID
		old.RetryTargets = []string{filepath.Join(root, "historical-other.mov")}
		raw, _ := json.Marshal(old)
		fields := map[string]string{"id": old.ID, "status": "done", "payload": string(raw), "processed": "1", "backend": "hevc_vaapi", "queue_class": "render"}
		if err := rdb.HSet(ctx, farmqueue.JobHashPrefix+old.ID, fields).Err(); err != nil {
			t.Fatal(err)
		}
		children[old.ID] = fields
	}
	enqueueTranscript := func(id string) {
		t.Helper()
		if err := q.Enqueue(ctx, farmqueue.Job{ID: id, Path: empty, Kinds: []string{farmqueue.KindTranscript}, Producer: "test",
			QueueClass: farmqueue.QueueClassRender, SelectedBackend: "vulkan", RequiredCapabilities: []string{"transcript:vulkan"}}); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]map[string]string{}
	for generation := 0; generation < 2; generation++ {
		transcript := fmt.Sprintf("hold-transcript-%d", generation)
		enqueueTranscript(transcript)
		if err := q.PublishStoragePermit(ctx, farmqueue.StoragePermit{Source: "fixture", AvailableBytes: 2, RequiredBytes: 1}); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(executable, "-test.run=^TestRunQueueAmbiguousFallbackDoesNotStarveAfterRestart$", "-test.timeout=35s")
		cmd.Env = append(os.Environ(), "JM_FARM_HOLD_CHILD=1", "JM_FARM_HOLD_ROOT="+root, "JM_FARM_WATCH=0", "PATH="+tools+string(os.PathListSeparator)+os.Getenv("PATH"))
		var output lockedBuffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		running := true
		// Every started process is joined through its original Wait, even on failure.
		func() {
			defer func() {
				if running {
					_ = cmd.Process.Kill()
					waitErr := <-done
					t.Logf("hold child pid=%d forced wait=%v\n%s", cmd.Process.Pid, waitErr, output.String())
				}
			}()
			deadline := time.Now().Add(12 * time.Second)
			for {
				status, err := rdb.HGet(ctx, farmqueue.JobHashPrefix+transcript, "status").Result()
				if err != nil {
					t.Fatal(err)
				}
				if status == farmqueue.StatusDone {
					break
				}
				select {
				case err := <-done:
					running = false
					t.Fatalf("hold worker exited before transcript: %v\n%s", err, output.String())
				default:
				}
				if time.Now().After(deadline) {
					for _, id := range []string{"hold-parent-0", "hold-parent-1", transcript} {
						fields, readErr := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+id).Result()
						t.Logf("timeout job %s fields=%v err=%v", id, fields, readErr)
					}
					t.Fatalf("transcript did not finish status=%s\n%s", status, output.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			for i := 0; i < 2; i++ {
				id := fmt.Sprintf("hold-parent-%d", i)
				fields, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+id).Result()
				if err != nil || fields["status"] != farmqueue.StatusBlocked || fields["held_raw"] == "" || fields["payload"] != fields["held_raw"] {
					t.Fatalf("parent not durably held %s: %v %v", id, fields, err)
				}
				if generation == 0 {
					held[id] = fields
				} else if !reflect.DeepEqual(held[id], fields) {
					t.Fatalf("restart changed held parent %s", id)
				}
				if ttl, err := rdb.PTTL(ctx, farmqueue.JobHashPrefix+id).Result(); err != nil || ttl != -1 {
					t.Fatalf("hold TTL=%v err=%v", ttl, err)
				}
			}
			for id, want := range children {
				got, err := rdb.HGetAll(ctx, farmqueue.JobHashPrefix+id).Result()
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("child changed %s: %v %v", id, got, err)
				}
			}
			if n, err := q.QueueDepth(ctx); err != nil || n != 0 {
				t.Fatalf("remaining claimable work=%d %v", n, err)
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				running = false
				if err != nil {
					t.Fatalf("worker stop: %v\n%s", err, output.String())
				}
			case <-time.After(8 * time.Second):
				t.Fatal("worker did not stop")
			}
			t.Logf("hold child generation=%d pid=%d original wait=0\n%s", generation, cmd.Process.Pid, output.String())
		}()
	}
}
