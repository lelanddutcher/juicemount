//go:build darwin

package mounttable

import (
	"context"
	"os/exec"
)

// Output returns mount(8)'s stdout without ever issuing getfsstat from the
// JuiceMount process itself.
//
// MNT_NOWAIT is only a cache-refresh policy; it is not a syscall deadline. A
// live RC reproduced unix.Getfsstat(..., MNT_NOWAIT) parking the calling
// thread in an uninterruptible kernel wait while a half-established macFUSE
// mount existed. Because that call was in-process, the app and its control
// plane became unresponsive even though every caller carried a context.
//
// Put the syscall in a disposable, single-flight child instead. output returns
// when ctx expires without waiting for an unkillable child, and keeps the gate
// held until that child really exits so health ticks cannot create a process
// leak storm. One stuck helper is recoverable at reboot; a stuck app is not.
func Output(ctx context.Context) ([]byte, error) {
	return output(ctx, queryGate, darwinMountCommand)
}

func darwinMountCommand() *exec.Cmd {
	return exec.Command("/sbin/mount")
}
