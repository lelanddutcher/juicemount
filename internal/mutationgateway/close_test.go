package mutationgateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func requireGatewayLocked(t *testing.T, cfg Config) {
	t.Helper()
	other, err := Open(cfg)
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, mutationauthority.ErrLocked) {
		t.Fatalf("live gateway authority released: %v", err)
	}
}

// The held upload crosses Close, including a disconnected client. Shutdown
// joins the admitted HTTP handler before inspecting its full committed bytes.
func exerciseCloseDuringUpload(t *testing.T, cfg Config, g *Gateway, cancelClient bool) (Upload, CustodyReceipt, certificates) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	certs := testCertificates(t)
	a, b, stop := testListener(t, g, certs)
	l, err := a.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	g.stageHook = func(phase string) {
		if phase == "prepared" {
			close(started)
			<-release
		}
	}
	requestCtx, disconnect := context.WithCancel(ctx)
	type uploadResult struct {
		receipt CustodyReceipt
		err     error
	}
	done := make(chan uploadResult, 1)
	u := testUpload(l, "held")
	go func() {
		r, err := a.Upload(requestCtx, u)
		done <- uploadResult{r, err}
	}()
	received := false
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		disconnect()
		if !received {
			<-done
		}
		stop()
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("upload never reached its prepared boundary")
	}
	if err := g.Close(); !errors.Is(err, mutationauthority.ErrInFlight) {
		t.Fatalf("close during upload: %v", err)
	}
	requireGatewayLocked(t, cfg)
	_, err = b.Claim(ctx)
	requireHTTP(t, err, "503")
	_, err = a.Upload(ctx, testUpload(l, "after-close"))
	requireHTTP(t, err, "503")
	_, err = a.Status(ctx, u.Chunk)
	requireHTTP(t, err, "503")
	_, err = a.Renew(ctx, l)
	requireHTTP(t, err, "503")
	if err := g.Close(); !errors.Is(err, mutationauthority.ErrInFlight) {
		t.Fatalf("repeat close during upload: %v", err)
	}
	var result uploadResult
	if cancelClient {
		disconnect()
		result = <-done
		received = true
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("disconnect outcome: %v", result.err)
		}
	}
	close(release)
	if !received {
		result = <-done
		received = true
		if result.err != nil {
			t.Fatalf("admitted upload lost its response: %v", result.err)
		}
	}
	stop() // Shutdown waits for the server-side upload even after disconnect.
	r, data, err := g.custody(ctx, u.Chunk)
	if err != nil || !bytes.Equal(data, u.Data) {
		t.Fatalf("admitted custody lost: %+v %q %v", r, data, err)
	}
	if !cancelClient && result.receipt != r {
		t.Fatalf("response differs from committed custody: %+v != %+v", result.receipt, r)
	}
	if leave, err := g.enter(); !errors.Is(err, mutationauthority.ErrClosed) {
		if leave != nil {
			leave()
		}
		t.Fatalf("drained gateway reopened admission: %v", err)
	}
	return u, r, certs
}

func TestCloseAndSuccessionWaitForAdmittedUpload(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(fmt.Sprintf("client_canceled=%t", cancelClient), func(t *testing.T) {
			cfg := testConfig(t)
			g := testGateway(t, cfg)
			u, r, certs := exerciseCloseDuringUpload(t, cfg, g, cancelClient)
			for range 2 {
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
			}
			next := testGateway(t, cfg)
			a, b, _ := testListener(t, next, certs)
			ctx := context.Background()
			l, err := b.Claim(ctx)
			if err != nil || l.Epoch != u.Chunk.Epoch+1 {
				t.Fatalf("successor claim: %+v %v", l, err)
			}
			r2, err := a.Status(ctx, u.Chunk)
			if err != nil || r2 != r {
				t.Fatalf("reopened custody changed: %+v %v", r2, err)
			}
			data, err := a.Readback(ctx, r2)
			if err != nil || !bytes.Equal(data, u.Data) {
				t.Fatalf("reopened bytes changed: %q %v", data, err)
			}
		})
	}
}

func TestCloseRetainsPinnedAuthorityUntilProcessExit(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestGatewayPinnedCloseChild$")
	cmd.Env = append(os.Environ(), "JM_GATEWAY_PINNED_CLOSE_STATE="+cfg.Directory)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	var r CustodyReceipt
	if err := json.NewDecoder(stdout).Decode(&r); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		waited = true
		t.Fatalf("pinned child handshake: %v; %s", err, stderr.String())
	}
	requireGatewayLocked(t, cfg)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err == nil || ctx.Err() != nil {
		t.Fatalf("owned child termination: %v (context %v)", err, ctx.Err())
	}
	next := testGateway(t, cfg)
	r2, data, err := next.custody(context.Background(), r.Chunk)
	u := testUpload(Lease{Session: r.Chunk.Session, Epoch: r.Chunk.Epoch}, "held")
	if err != nil || r2 != r || r.Kind != format || !bytes.Equal(data, u.Data) {
		t.Fatalf("process successor lost exact custody: %+v %q %v", r2, data, err)
	}
	l, err := next.journal.Claim(context.Background(), cfg.Volume, "successor", time.Minute)
	if err != nil || l.Epoch != r.Chunk.Epoch+1 {
		t.Fatalf("process successor claim: %+v %v", l, err)
	}
}

// A pinned journal has no test-only unlock. Only the parent can kill this
// owned process, reap it, and prove that a successor can open the same state.
func TestGatewayPinnedCloseChild(t *testing.T) {
	dir := os.Getenv("JM_GATEWAY_PINNED_CLOSE_STATE")
	if dir == "" {
		return
	}
	cfg := Config{Directory: dir, Volume: "volume", LeaseTTL: time.Minute}
	g, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.journal.BeginBackendProcess(); err != nil {
		t.Fatal(err)
	}
	u, r, certs := exerciseCloseDuringUpload(t, cfg, g, true)
	for range 2 {
		if err := g.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
			t.Fatalf("pinned close: %v", err)
		}
	}
	a, b, stop := testListener(t, g, certs)
	ctx := context.Background()
	_, err = b.Claim(ctx)
	requireHTTP(t, err, "503")
	_, err = a.Upload(ctx, u)
	requireHTTP(t, err, "503")
	_, err = a.Status(ctx, u.Chunk)
	requireHTTP(t, err, "503")
	stop()
	requireGatewayLocked(t, cfg)
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
