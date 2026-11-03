//go:build !linux && !darwin

package smartfolders

import (
	"errors"
	"io/fs"
)

type renameOp int

const (
	renameNoReplace renameOp = iota
	renameExchange
)

// Without a no-replace / exchange rename the OS backend refuses: a plain
// rename could destroy a user's entry.
func renameAt(int, string, string, renameOp) error { return errors.ErrUnsupported }

func dirHandleOp(int, string, uint64, fs.FileMode) (Info, error) {
	return Info{}, errors.ErrUnsupported
}
