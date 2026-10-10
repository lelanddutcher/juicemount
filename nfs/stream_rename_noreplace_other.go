//go:build !darwin && !linux

package nfs

import "fmt"

func renameStreamNoReplace(oldPath, newPath string) error {
	return fmt.Errorf("atomic no-replace stream rename is unsupported on this platform")
}
