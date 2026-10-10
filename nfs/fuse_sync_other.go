//go:build !darwin

package nfs

import (
	"errors"
	"os"
)

func syncFUSEFile(f *os.File) error {
	if f == nil {
		return errors.New("sync fuse file: nil file")
	}
	return f.Sync()
}
