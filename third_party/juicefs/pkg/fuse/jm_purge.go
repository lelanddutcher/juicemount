package fuse

// JuiceMount patch 0031. macFUSE's own FUSE open flags (its fuse_kernel.h,
// the purge_attr and purge_ubc fields of libfuse's fuse_file_info on macOS):
// the kernel drops the file's attributes and its unified buffer cache pages
// before the open returns. macFUSE does not act on a missing
// FOPEN_KEEP_CACHE, which is all upstream sends for a changed file. Only
// darwin opens set these bits.
const (
	jmFopenPurgeAttr = 1 << 30
	jmFopenPurgeUBC  = 1 << 31

	jmDarwinPurgeOnOpen = jmFopenPurgeAttr | jmFopenPurgeUBC
)
