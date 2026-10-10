//go:build sdkacceptance

package mutationgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationbackend"
)

type mixedRecoveryReport struct {
	Custody CustodyReceipt
	Stage   mutationauthority.MediaStage
	Epoch   int64
}

func TestGatewayMixedStageRecovery(t *testing.T) {
	f, cfg, path := sdkGatewayFixture(t)
	var first mixedRecoveryReport
	for _, mode := range []string{"seed", "recover"} {
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(f.Context, exe, "-test.run=^TestGatewayMixedStageChild$")
		cmd.Env = append(os.Environ(), "JM_MIXED_RECOVERY_CONFIG="+path, "JM_MIXED_RECOVERY_STATE="+cfg.Directory, "JM_MIXED_RECOVERY_MODE="+mode)
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
		waited := false
		stop := func() {
			if waited {
				return
			}
			waited = true
			in.Close()
			cmd.Process.Kill()
			cmd.Wait()
		}
		t.Cleanup(stop)
		var report mixedRecoveryReport
		d := json.NewDecoder(out)
		if err := d.Decode(&report); err != nil {
			stop()
			rest, _ := io.ReadAll(d.Buffered())
			t.Fatalf("mixed startup failed: %v %s", err, rest)
		}
		if mode == "seed" {
			first = report
		} else if !reflect.DeepEqual(first.Custody, report.Custody) || !reflect.DeepEqual(first.Stage, report.Stage) || report.Epoch != first.Epoch+1 {
			stop()
			t.Fatal("mixed recovery changed custody/stage or failed successor epoch")
		}
		stop()
		if f.Context.Err() != nil || stderr.Len() != 0 {
			t.Fatal("mixed child diagnostic/lifecycle failure", f.Context.Err(), stderr.Len())
		}
		j, err := mutationauthority.Open(filepath.Join(cfg.Directory, "authority.db"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.Claim(context.Background(), "volume", "unfenced", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
			j.Close()
			t.Fatal("plain reopen admitted", err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGatewayMixedStageChild(t *testing.T) {
	path := os.Getenv("JM_MIXED_RECOVERY_CONFIG")
	if path == "" {
		return
	}
	sdk, err := mutationbackend.ReadSDKConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	g, err := Open(Config{Directory: os.Getenv("JM_MIXED_RECOVERY_STATE"), Volume: "volume", LeaseTTL: time.Minute, SDK: sdk})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	session := strings.Repeat("a", 64) + "." + strings.Repeat("b", 64)
	var report mixedRecoveryReport
	if os.Getenv("JM_MIXED_RECOVERY_MODE") == "seed" {
		l, err := g.journal.Claim(ctx, "volume", session, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		u := testUpload(wireLease(l), "mixed-custody")
		if _, err := g.journal.Execute(ctx, g.request(u.Chunk), stageBackend{g: g, chunk: u.Chunk, data: u.Data}); err != nil {
			t.Fatal(err)
		}
		report.Custody, _, err = g.custody(ctx, u.Chunk)
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte("retained gateway media")
		report.Stage, err = g.sdk.StageMedia(ctx, mutationbackend.MediaStageRequest{Volume: "volume", Session: l.Session, Epoch: l.Epoch, ID: "mixed-media", FileID: "incarnation", ExpectedRevision: "parent@3,absent", Size: int64(len(payload)), SHA256: hash(payload), CustodyArtifactSHA256: report.Custody.ArtifactSHA256}, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		report.Epoch = l.Epoch
	} else {
		report.Stage, err = g.journal.InspectStage(ctx, "volume", "mixed-media")
		if err != nil {
			t.Fatal(err)
		}
		if err := g.journal.RequireRetainedStage(ctx, report.Stage.Request); err != nil {
			t.Fatal(err)
		}
		old := mutationauthority.Lease{Volume: "volume", Session: session, Epoch: report.Stage.Request.Epoch}
		u := testUpload(wireLease(old), "mixed-custody")
		var data []byte
		report.Custody, data, err = g.custody(ctx, u.Chunk)
		if err != nil || !bytes.Equal(data, u.Data) {
			t.Fatal("custody changed", err)
		}
		l, err := g.journal.Claim(ctx, "volume", session, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		report.Epoch = l.Epoch
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
