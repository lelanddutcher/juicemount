//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

type stageRecoveryReport struct {
	Prior []mutationauthority.MediaStage
	Next  mutationauthority.MediaStage
	Lease mutationauthority.Lease
}

func runStageSuccessor(t *testing.T, ctx context.Context, configPath, state string) (stageRecoveryReport, func()) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKStageRecoveryChild$")
	cmd.Env = append(os.Environ(), "JM_STAGE_RECOVERY_CONFIG="+configPath, "JM_STAGE_RECOVERY_STATE="+state)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		_ = in.Close()
		_ = cmd.Process.Kill()
		err := cmd.Wait()
		if ctx.Err() != nil || stderr.Len() != 0 {
			t.Fatal("successor lifecycle/diagnostic failure", err, ctx.Err(), stderr.Len())
		}
	}
	t.Cleanup(stop)
	var report stageRecoveryReport
	decoder := json.NewDecoder(out)
	if err := decoder.Decode(&report); err != nil {
		stop()
		remainder, _ := io.ReadAll(decoder.Buffered())
		t.Fatalf("successor did not admit a new stage: %v diagnostic=%s", err, remainder)
	}
	return report, stop
}

func TestSDKStageRecoveryChild(t *testing.T) {
	configPath := os.Getenv("JM_STAGE_RECOVERY_CONFIG")
	if configPath == "" {
		return
	}
	cfg, err := ReadSDKConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(os.Getenv("JM_STAGE_RECOVERY_STATE"), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	before, err := j.StageRecoveryCandidates(ctx, "volume")
	if err != nil || len(before) == 0 {
		t.Fatal("missing old stages", err)
	}
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range before {
		if err := j.RequireRetainedStage(ctx, s.Request); err != nil {
			t.Fatal(err)
		}
	}
	l, err := j.Claim(ctx, "volume", "recovered-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("new owner payload")
	spec := MediaStageRequest{Volume: "volume", Session: l.Session, Epoch: l.Epoch, ID: "recovered-next", FileID: "new-incarnation", ExpectedRevision: "parent@3,absent", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("new immutable custody"))}
	next, err := r.StageMedia(ctx, spec, bytes.NewReader(payload))
	if err != nil || !next.Closed {
		t.Fatal("new stage failed", err)
	}
	for _, old := range before {
		got, err := j.InspectStage(ctx, "volume", old.Request.ID)
		if err != nil || mutationauthority.StageSnapshotDigest(got) != mutationauthority.StageSnapshotDigest(old) || old.Inode == next.Inode || old.Intent.Path == next.Intent.Path {
			t.Fatal("old stage altered/adopted", err)
		}
		op, err := j.Inspect(ctx, "volume", old.Request.ID)
		if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
			t.Fatal("retention manufactured a receipt", err)
		}
		for _, priorSlice := range old.Slices {
			for _, newSlice := range next.Slices {
				if priorSlice.ID == newSlice.ID {
					t.Fatal("reused old slice")
				}
			}
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(stageRecoveryReport{before, next, l}); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
