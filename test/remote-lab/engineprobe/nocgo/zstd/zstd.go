// engineprobe build only (see build.sh): DataDog/zstd needs cgo. The probe
// refuses a compressed volume, so these are never called.
package zstd

import "errors"

var errNoCgo = errors.New("zstd: not in the engineprobe build (no cgo)")

func CompressBound(l int) int                                  { return l + 64 }
func CompressLevel(dst, src []byte, level int) ([]byte, error) { return nil, errNoCgo }
func Decompress(dst, src []byte) ([]byte, error)               { return nil, errNoCgo }
