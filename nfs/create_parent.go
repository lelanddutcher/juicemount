package nfs

import (
	"os"
	"path"

	"golang.org/x/sys/unix"

	"github.com/lelanddutcher/juicemount/metadata"
)

// createParentChain keeps the directory generation used by O_EXCL alive.
// Path checks detect replacement before mirror publication; openat ensures a
// replacement at the pathname cannot redirect the backing CREATE itself.
type createParentChain struct {
	root       string
	generation string
	dirs       []*os.File
}

func (c *createParentChain) close() {
	for i := len(c.dirs) - 1; i >= 0; i-- {
		_ = c.dirs[i].Close()
	}
}

func (c *createParentChain) verify() error {
	for _, dir := range c.dirs {
		if err := verifyCreatedFileMount(dir, c.root, c.generation); err != nil {
			return err
		}
	}
	return nil
}

func (c *createParentChain) create(name, fullPath string) (*os.File, error) {
	parent := c.dirs[len(c.dirs)-1]
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o666)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), fullPath), nil
}

func (jfs *juiceFS) openCreateParent(filename, generation string, op *namespaceOperation) (_ *createParentChain, err error) {
	c := &createParentChain{root: jfs.handler.fusePath, generation: generation}
	defer func() {
		if err != nil {
			c.close()
		}
	}()
	root, err := os.Open(c.root)
	if err != nil {
		return nil, err
	}
	c.dirs = append(c.dirs, root)
	if err := c.verify(); err != nil {
		return nil, err
	}
	parent := path.Dir(filename)
	if parent == "." {
		return c, nil
	}
	var ancestors []string
	for current := parent; current != "."; current = path.Dir(current) {
		ancestors = append(ancestors, current)
	}
	for i := len(ancestors) - 1; i >= 0; i-- {
		name := ancestors[i]
		expected, epoch, err := jfs.handler.store.CaptureDirectoryForFUSE(name)
		if err != nil {
			return nil, err
		}
		parentFD := c.dirs[len(c.dirs)-1]
		fd, err := unix.Openat(int(parentFD.Fd()), path.Base(name), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		dir := os.NewFile(uintptr(fd), jfs.fullPath(name))
		c.dirs = append(c.dirs, dir)
		if err := c.verify(); err != nil {
			return nil, err
		}
		info, err := dir.Stat()
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, metadata.ErrMirrorConflict
		}
		op.checkpoint("confirm-parent", name)
		if err := c.verify(); err != nil {
			return nil, err
		}
		if expected.Incarnation == "" {
			entry := metadata.MakeEntry(name, true, info.Size(), info.ModTime(), jfs.handler.nextSyntheticInode())
			if _, err := jfs.handler.store.ConfirmAbsentDirectoryFromFUSE(expected, epoch, entry); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}
