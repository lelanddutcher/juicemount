// engineprobe build only (see build.sh): hungys/go-lz4 needs cgo. The probe
// refuses a compressed volume, so these are never called.
package lz4

import "errors"

var errNoCgo = errors.New("lz4: not in the engineprobe build (no cgo)")

func CompressBound(size int) int                   { return size + size/255 + 16 }
func CompressDefault(src, dst []byte) (int, error) { return 0, errNoCgo }
func DecompressSafe(src, dst []byte) (int, error)  { return 0, errNoCgo }
