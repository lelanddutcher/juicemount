//go:build !darwin

package mounttable

import (
	"context"
	"os/exec"
)

// Output returns mount(8)'s stdout or an error before ctx expires. At most one
// mount query exists at a time. If a timed-out child remains uninterruptible,
// the gate stays held until it actually exits, so later health ticks fail
// boundedly instead of leaking more stuck processes and goroutines.
//
// Darwin uses getfsstat(MNT_NOWAIT) instead. On macOS, mount(8) can itself
// enter an uninterruptible kernel wait when any NFS/FUSE mount is wedged.
func Output(ctx context.Context) ([]byte, error) {
	return output(ctx, queryGate, func() *exec.Cmd { return exec.Command("mount") })
}
