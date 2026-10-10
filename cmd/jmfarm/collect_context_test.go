package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

func TestRunJobCollectionStopsOnContextCancellation(t *testing.T) {
	t.Run("walk stops before later entries", func(t *testing.T) {
		root := t.TempDir()
		first := filepath.Join(root, "first.mp4")
		second := filepath.Join(root, "second.mp4")
		for _, path := range []string{first, second} {
			if err := os.WriteFile(path, []byte("media"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		rootInfo, err := os.Stat(root)
		if err != nil {
			t.Fatal(err)
		}
		firstInfo, err := os.Stat(first)
		if err != nil {
			t.Fatal(err)
		}
		secondInfo, err := os.Stat(second)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		processed := make([]string, 0, 2)
		walk := func(root string, visit filepath.WalkFunc) error {
			for _, entry := range []struct {
				path string
				info os.FileInfo
			}{{root, rootInfo}, {first, firstInfo}, {second, secondInfo}} {
				if err := visit(entry.path, entry.info, nil); err != nil {
					return err
				}
				if !entry.info.IsDir() {
					processed = append(processed, entry.path)
					if entry.path == first {
						cancel()
					}
				}
			}
			return nil
		}

		targets, err := collectTargetsWithWalkContext(ctx, root, "", 0, 0, walk)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("collect returned %v; want context.Canceled", err)
		}
		if len(processed) != 1 || processed[0] != first {
			t.Fatalf("processed entries = %v; want only %q", processed, first)
		}
		if targets != nil {
			t.Fatalf("partial targets = %v; want nil", targets)
		}
	})

	t.Run("injected collector cancellation skips planning", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stages := make([]string, 0, 2)
		// The job scope must be a real directory under the worker mount: the
		// queue path is validated against the mount BEFORE collection, so a
		// bare "/media" is rejected for the wrong reason and never reaches
		// the cancellation this subtest exists to prove.
		mount := t.TempDir()
		scope := filepath.Join(mount, "media")
		if err := os.Mkdir(scope, 0o755); err != nil {
			t.Fatal(err)
		}
		cfg := queueConfig{
			mount:    mount,
			progress: func(stage string, _ int) { stages = append(stages, stage) },
			collectTargets: func(context.Context, string, string, int, int64) ([]string, error) {
				cancel()
				return []string{filepath.Join(scope, "first.mp4")}, nil
			},
		}
		job := farmqueue.NewJob(scope, []string{farmqueue.KindDerivatives}, "context-cancel-test")
		_, _, _, err := runJob(ctx, nil, nil, cfg, farmqueue.Worker{}, job, nil)
		if err != context.Canceled {
			t.Fatalf("runJob returned %v; want context.Canceled", err)
		}
		if len(stages) != 1 || stages[0] != "discovering" {
			t.Fatalf("progress stages = %v; want only discovering", stages)
		}
	})
}
