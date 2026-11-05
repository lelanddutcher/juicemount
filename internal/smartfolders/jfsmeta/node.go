package jfsmeta

import "github.com/lelanddutcher/juicemount/internal/smartfolders"

// Node is one inode as the continuity binder reads it: its number (the
// file's identity in the volume), kind and link count.
type Node struct {
	Ino    uint64
	Type   smartfolders.FileType
	Nlink  uint32
	Parent uint64 // 0 when the inode has several parents (hard links)
	// InTrash: the only parent is a JuiceFS trash directory, so the
	// file was deleted and is kept only for the trash's retention.
	InTrash bool
}
