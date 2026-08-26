// Package mounttable provides a bounded, single-flight wrapper around the
// platform mount-table command. On macOS, mount(8) calls getfsstat(2), which
// can remain trapped in the kernel when any FUSE/NFS entry is wedged. The
// standard exec.CommandContext path still waits for that uninterruptible child
// after cancellation, so callers need to stop waiting without starting an
// unbounded procession of replacement commands.
package mounttable

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

var queryGate = make(chan struct{}, 1)

// Output returns mount(8)'s stdout or an error before ctx expires. At most one
// mount query exists at a time. If a timed-out child remains uninterruptible,
// the gate stays held until it actually exits, so later health ticks fail
// boundedly instead of leaking more stuck processes and goroutines.
func Output(ctx context.Context) ([]byte, error) {
	return output(ctx, queryGate, func() *exec.Cmd { return exec.Command("mount") })
}

func output(ctx context.Context, gate chan struct{}, command func() *exec.Cmd) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("mount table: nil context")
	}
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("mount table: previous query still running: %w", ctx.Err())
	}

	cmd := command()
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		<-gate
		return nil, fmt.Errorf("mount table: start: %w", err)
	}
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		<-gate
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return nil, fmt.Errorf("mount table: wait: %w", err)
		}
		return append([]byte(nil), stdout.Bytes()...), nil
	case <-ctx.Done():
		// Kill is best-effort. If the process is in an uninterruptible kernel
		// wait it will exit only after that syscall unwinds; the waiter above
		// deliberately owns the gate until then.
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("mount table: query timed out: %w", ctx.Err())
	}
}
