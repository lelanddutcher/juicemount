//go:build !unix

package smartfolders

import "io/fs"

// Without inode numbers nothing the OS backend created could be told apart
// from a user's entry, so OpenOSFS refuses on these systems.
func inodeOf(fs.FileInfo) uint64 { return 0 }

const osfsSupported = false
