package manager

import (
	"fmt"

	"github.com/lelanddutcher/juicemount/internal/smartfolders"
)

// osBackend is a TEST backend: a temp directory as the volume, through
// smartfolders.OSFS. Production never uses it (smart folders run only on
// the JuiceFS metadata backend).
type osBackend struct {
	root       string
	mountCheck func(string) error
}

func (b *osBackend) acquire() (smartfolders.FS, func(), error) {
	if err := b.mountCheck(b.root); err != nil {
		return nil, nil, fmt.Errorf("volume not mounted at %s: %w", b.root, err)
	}
	fsys, err := smartfolders.OpenOSFS(b.root)
	if err != nil {
		return nil, nil, err
	}
	return fsys, func() { _ = fsys.Close() }, nil
}

func (b *osBackend) lookup() (smartfolders.FS, func(), error) { return b.acquire() }
func (b *osBackend) close() error                             { return nil }
func (b *osBackend) describe() string                         { return "the FUSE mount at " + b.root }
