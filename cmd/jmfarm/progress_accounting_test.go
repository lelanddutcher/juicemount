package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
)

func TestRunPassesCountsPartialBlobFailure(t *testing.T) {
	dir := t.TempDir()
	store, err := derivatives.Open(filepath.Join(dir, "derivatives.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	good := filepath.Join(dir, "good.mov")
	bad := filepath.Join(dir, "bad.mov")
	fresh := filepath.Join(dir, "fresh.mov")
	statusPath := filepath.Join(dir, "farm-status.json")
	processed, failed, failedTargets, details := runPasses(passOpts{
		mode: "derivatives", effConc: 2, status: statusPath,
		producer: "test", target: dir, store: store,
		process: func(_ *derivatives.Store, path string, _ farm.Options) farm.Result {
			if path == bad {
				return farm.Result{Path: path, Inode: 2, BlobErr: errors.New("waveform decode unavailable")}
			}
			if path == fresh {
				return farm.Result{Path: path, Inode: 3, SkippedFresh: true}
			}
			return farm.Result{Path: path, Inode: 1, ThumbWrote: true}
		},
	}, []string{good, bad, fresh})

	if processed != 2 || failed != 1 {
		t.Fatalf("counts = processed %d failed %d, want 2/1", processed, failed)
	}
	if len(failedTargets) != 1 || failedTargets[0] != bad {
		t.Fatalf("failed targets = %v, want %q", failedTargets, bad)
	}
	if len(details) != 1 || !strings.Contains(details[0], "waveform decode unavailable") {
		t.Fatalf("failure details = %v", details)
	}

	raw, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatal(err)
	}
	var status farm.FarmStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}
	if status.LastSweep.Processed != 2 || status.LastSweep.Failed != 1 {
		t.Fatalf("status sweep = %+v, want processed=2 failed=1", status.LastSweep)
	}
	if status.InProgress != nil {
		t.Fatalf("terminal status retained in-progress block: %+v", status.InProgress)
	}
}
