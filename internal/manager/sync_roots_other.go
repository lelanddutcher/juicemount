//go:build !linux

package manager

import (
	"errors"
	"os"
)

// openRootBeneath is Linux-only (openat2). juicefsConfinesFileRoots is
// false off Linux, so this is never reached there.
func openRootBeneath(anchor, rel string, create bool) (*os.File, error) {
	return nil, errors.New("confined sync roots need Linux")
}
