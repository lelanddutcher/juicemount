package nfs

import (
	"errors"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/testguard"
)

// refuseFUSEContentRead enforces the acceptance harness's zero-byte remote
// content budget. Callers must consult every authoritative local serving path
// first, then call this immediately before an operation that can enter FUSE.
//
// JuiceFS does not expose a pre-read "this range is local" primitive, and one
// logical pread may fetch a whole backend block. Refusing the FUSE fallthrough
// is therefore the only in-process guard that can fail before remote bytes are
// consumed. The joined offline error preserves the existing NFS NXIO/JUKEBOX
// mapping while retaining a test-distinguishable cause.
func refuseFUSEContentRead(enabled bool) error {
	if !enabled {
		return nil
	}
	return errors.Join(pin.ErrOfflineNotAvailable, testguard.ErrFUSEContentReadRefused)
}
