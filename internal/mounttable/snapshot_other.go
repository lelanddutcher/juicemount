//go:build !darwin

package mounttable

import "errors"

// CaptureSnapshot is a macOS-only kernel observer. Unsupported platforms must
// never fabricate an empty snapshot that could be mistaken for mount absence.
func CaptureSnapshot() (Snapshot, error) {
	return Snapshot{}, errors.New("mount snapshot: kernel observer requires macOS")
}
