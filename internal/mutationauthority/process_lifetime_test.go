package mutationauthority

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Pins intentionally have no test-only unlock. Each case runs in a real child
// process so cleanup exercises the same process-exit boundary as production.
func TestProcessLifetimeRetainsAuthorityUntilExit(t *testing.T) {
	for _, mode := range []string{"pin", "record", "canceled-record"} {
		t.Run(mode, func(t *testing.T) {
			private := filepath.Join(t.TempDir(), "private")
			if err := os.Mkdir(private, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(private, "authority.db")
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestProcessLifetimeChild$")
			cmd.Env = append(os.Environ(), "JM_LIFETIME_PATH="+path, "JM_LIFETIME_MODE="+mode)
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
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if err != nil || line != "retained\n" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				waited = true
				t.Fatalf("child handshake: %q %v; %s", line, err, stderr.String())
			}
			other, err := Open(path)
			if other != nil {
				_ = other.Close()
			}
			if !errors.Is(err, ErrLocked) {
				t.Fatalf("Close and GC released live process authority: %v", err)
			}
			// Abrupt exit proves release is an OS lifetime event, without calling
			// another journal or database cleanup method in the child.
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			waited = true
			if err == nil || ctx.Err() != nil {
				t.Fatalf("expected owned child termination, got %v (context %v)", err, ctx.Err())
			}
			next, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			rows, err := next.BackendProcesses(context.Background(), "volume")
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "record" {
				want = 1
			}
			if len(rows) != want {
				t.Fatalf("durable identity count %d; want %d", len(rows), want)
			}
			if want == 1 {
				if rows[0] != (BackendProcess{1, "volume", "jmgw_" + strings.Repeat("a", 64), strings.Repeat("b", 40)}) {
					t.Fatalf("lost immutable process identity: %+v", rows[0])
				}
				if err := next.RequireOnlyVolume(context.Background(), "foreign"); !errors.Is(err, ErrInvalid) {
					t.Fatalf("foreign volume adopted backend journal: %v", err)
				}
			}
		})
	}
}

func TestProcessLifetimeChild(t *testing.T) {
	path := os.Getenv("JM_LIFETIME_PATH")
	if path == "" {
		return // Child entry point; parent owns the actual case assertions.
	}
	if err := preparePinnedChild(path, os.Getenv("JM_LIFETIME_MODE")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// preparePinnedChild returned without retaining a journal reference. Force
	// several GC cycles before announcing the parent's competing open attempt.
	for range 3 {
		runtime.GC()
		runtime.Gosched()
	}
	fmt.Fprintln(os.Stdout, "retained")
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func preparePinnedChild(path, mode string) error {
	j, err := Open(path)
	if err != nil {
		return err
	}
	if err := j.PinProcessLifetime(); err != nil {
		return err
	}
	if err := j.PinProcessLifetime(); err != nil {
		return fmt.Errorf("idempotent pin: %w", err)
	}
	if err := j.BeginBackendProcess(); err != nil {
		return err
	}
	if err := j.BeginBackendProcess(); !errors.Is(err, ErrProcessLifetime) {
		return fmt.Errorf("second backend initialization allowed: %v", err)
	}
	if mode != "pin" {
		ctx := context.Background()
		if mode == "canceled-record" {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			ctx = canceled
		}
		_, err := j.RecordBackendProcess(ctx, "volume", "jmgw_"+strings.Repeat("a", 64), strings.Repeat("b", 40))
		if mode == "record" && err != nil || mode == "canceled-record" && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("record outcome: %v", err)
		}
		if _, err := j.RecordBackendProcess(context.Background(), "volume", "jmgw_"+strings.Repeat("c", 64), strings.Repeat("b", 40)); !errors.Is(err, ErrInvalid) {
			return fmt.Errorf("second identity attempt allowed: %v", err)
		}
	}
	if err := j.Close(); !errors.Is(err, ErrProcessLifetime) {
		return fmt.Errorf("pinned Close: %v", err)
	}
	return nil
}

func TestBackendIdentityRequiresProcessPin(t *testing.T) {
	j, _ := testJournal(t)
	if _, err := j.RecordBackendProcess(context.Background(), "volume", "jmgw_"+strings.Repeat("a", 64), strings.Repeat("b", 40)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unpinned identity accepted: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.PinProcessLifetime(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed journal pinned: %v", err)
	}
}
