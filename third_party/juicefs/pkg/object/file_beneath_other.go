//go:build !linux

/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

// newFileOps off Linux keeps the upstream path-based behaviour. Root
// confinement needs openat2 (file_beneath_linux.go); the macOS app bundles
// juicefs for mounting and does not run sync.
func newFileOps(root string) fileOps { return pathOps{} }
