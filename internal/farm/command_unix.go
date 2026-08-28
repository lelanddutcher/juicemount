//go:build darwin || linux

package farm

import (
	"context"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// commandContext puts each media tool in its own process group. Cancelling the
// queue job then kills the whole tool tree (not just a wrapper process), which
// is required for a bounded container shutdown and a recoverable durable lease.
func commandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			if err == syscall.ESRCH {
				return os.ErrProcessDone
			}
			return err
		}
		return nil
	}
	// Also bound Wait if an unusual child escapes the process group while
	// retaining stdout/stderr. Normal ffmpeg/ffprobe/whisper exits immediately.
	cmd.WaitDelay = 2 * time.Second
	return cmd
}
