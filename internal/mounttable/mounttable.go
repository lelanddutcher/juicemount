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

func output(ctx context.Context, gate chan struct{}, command func() *exec.Cmd) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("mount table: nil context")
	}
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("mount table: previous query still running (%s): %w", blockedObservation(), ctx.Err())
	}

	cmd := command()
	op := beginObservation(ctx, gate, "legacy mount", cmd.Path)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Start(); err != nil {
		finishObservation(op, gate, err, false)
		return nil, fmt.Errorf("mount table: start: %w", err)
	}
	launchedObservation(op, cmd)
	done := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		finishObservation(op, gate, err, true)
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
		timeoutObservation(op, ctx.Err())
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("mount table: query timed out: %w", ctx.Err())
	}
}
