package privatefile

import (
	"errors"
	"os"
	"path/filepath"
)

// SyncProfileInitialization finishes the final initialization barriers for an
// already validated profile. It creates nothing and keeps the control lock
// through the barriers; readable marker bytes alone are not an acknowledgement.
func (c *NativeControl) SyncProfileInitialization() error {
	return c.syncProfileInitialization(func(f *os.File) error { return f.Sync() })
}

func (c *NativeControl) syncProfileInitialization(syncFile func(*os.File) error) (resultErr error) {
	if c == nil || c.closed || c.root == nil {
		return ErrNativeCopyChanged
	}
	r := c.root
	if err := c.check(); err != nil {
		return err
	}
	if err := r.revalidateInventory(); err != nil {
		return err
	}
	parent, err := walkAbsoluteDirectory(filepath.Dir(r.path))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, parent.Close()) }()
	parentID, err := privateDir(parent)
	if err != nil {
		return err
	}
	child, err := openDirectoryAt(parent, filepath.Base(r.path))
	if err != nil {
		return err
	}
	id, identityErr := privateDir(child)
	closeErr := child.Close()
	if identityErr != nil || closeErr != nil || id != r.rootID {
		return errors.Join(identityErr, closeErr, ErrNativeCopyChanged)
	}
	// Creation had completed all earlier barriers before writing this marker.
	// Repeat the potentially interrupted marker/root barriers and the namespace
	// barrier on the exact existing parent; never reinitialize the inventory.
	for _, f := range []*os.File{r.markerFile, r.root, parent} {
		if err := syncFile(f); err != nil {
			return errors.Join(err, ErrNativeCopyUncertain)
		}
	}
	// The root may still have its original inode after being moved into a new
	// parent. Prove that the directory just synced is still its actual parent.
	freshParent, err := walkAbsoluteDirectory(filepath.Dir(r.path))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, freshParent.Close()) }()
	if id, err := privateDir(freshParent); err != nil || id != parentID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	freshChild, err := openDirectoryAt(freshParent, filepath.Base(r.path))
	if err != nil {
		return err
	}
	id, identityErr = privateDir(freshChild)
	closeErr = freshChild.Close()
	if identityErr != nil || closeErr != nil || id != r.rootID {
		return errors.Join(identityErr, closeErr, ErrNativeCopyChanged)
	}
	if err := c.check(); err != nil {
		return err
	}
	return r.revalidateInventory()
}
