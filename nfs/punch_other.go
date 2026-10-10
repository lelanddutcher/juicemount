//go:build !darwin

package nfs

import "os"

// Prefix punching is a Darwin/APFS facility here (F_PUNCHHOLE). The client is
// macOS-only — macFUSE — but package nfs is compiled on other platforms by
// `go build ./...` and by anything cross-checking the tree, so the primitive
// needs a portable shape.
//
// The stub reports UNSUPPORTED rather than pretending to succeed. That routes
// the streaming drain to its whole-file fallback, which is correct behaviour
// everywhere: without the ability to reclaim a drained prefix there is nothing
// to gain from streaming, and claiming success would silently skip the reclaim
// while the caller released capacity it never got back.

func punchRange(f *os.File, off, end int64) (int64, error) { return 0, nil }

func punchSupported(f *os.File) bool { return false }
