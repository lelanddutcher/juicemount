/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package object

import (
	"errors"
	"syscall"
)

// JMProjectFenced is a backend that can keep every write inside one
// project folder (patch 0017). depth is the number of key components that
// name the project folder; the project of a key is its first depth
// components. A write whose path, followed through symlinks, would leave
// that project is refused with ErrOutsideProject. depth <= 0 means the
// backend's root is the project or a folder inside it.
type JMProjectFenced interface {
	JMSetProjectDepth(depth int) error
}

// ErrOutsideProject is returned (inside an *os.PathError) when a write
// would resolve into another project. It unwraps to EXDEV.
var ErrOutsideProject error = &outsideProjectError{}

type outsideProjectError struct{}

func (*outsideProjectError) Error() string {
	return "refused: path resolves outside its project folder (symlink or ..)"
}
func (*outsideProjectError) Unwrap() error { return syscall.EXDEV }

// JMUnwrapPrefix returns the backend under a WithPrefix wrapper and the
// prefix, or s itself and "".
func JMUnwrapPrefix(s ObjectStorage) (ObjectStorage, string) {
	if p, ok := s.(*withPrefix); ok {
		return p.os, p.prefix
	}
	return s, ""
}

// JMSetProjectDepth fences the file:// backend. Only the Linux backend,
// which resolves every path beneath its root, supports it.
func (d *filestore) JMSetProjectDepth(depth int) error {
	f, ok := d.ops.(interface{ setProjectDepth(int) })
	if !ok {
		return errors.New("refused: this file storage cannot keep writes inside a project folder (Linux only)")
	}
	f.setProjectDepth(depth)
	return nil
}
