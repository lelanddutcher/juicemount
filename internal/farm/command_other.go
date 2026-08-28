//go:build !darwin && !linux

package farm

import (
	"context"
	"os/exec"
)

func commandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}
