//go:build !darwin

package fuse

import "syscall"

func renameCapabilities() uint32 { return 0 }

// Linux FUSE already uses the same renameat2 flags as JuiceFS metadata.
func normalizeRenameFlags(flags uint32) (uint32, syscall.Errno) { return flags, 0 }
