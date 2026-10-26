package privatefile

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

const finderMetadataName = ".DS_Store"

// At most 4096 target directories can be charged. Their 7 KiB allowance
// plus the single 1 MiB copies/ leaf totals 29 MiB of extra physical bytes.
// The copies/ cap permits a larger Finder index as the number of named copies
// grows; a larger file fails closed. The existing 96 MiB logical custody
// reservation and separate 64 MiB inventory/sidecar cap remain unchanged.
// Finder bytes are not custody records and never consume or replace a slot.
// The observed copies/ leaf is 6148 bytes.
const finderCopiesMetadataMaximum int64 = 1 << 20
const finderTargetMetadataMaximum int64 = 7 << 10

// Finder may create one .DS_Store while displaying an exposed copy. It is
// never a custody record, copy ID, or target. All other unknown names still
// fail closed, including Finder metadata in the private staging namespace.
func withoutFinderMetadata(dir *os.File, names []string, maximum int64) ([]string, error) {
	var found bool
	for _, name := range names {
		if name == finderMetadataName {
			found = true
			break
		}
	}
	if !found {
		return names, nil
	}
	first, err := OpenAt(dir, finderMetadataName, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	firstID, firstLinks, firstErr := identity(first)
	firstErr = errors.Join(firstErr, first.Close())
	if firstErr != nil || !validFinderMetadata(firstID, firstLinks, maximum) {
		return nil, errors.Join(firstErr, ErrNativeCopyChanged)
	}
	// Reopen the same live leaf before using this enumeration. The metadata
	// file contributes no authority, but replacement must not hide an unsafe
	// object under its allowed name.
	second, err := OpenAt(dir, finderMetadataName, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	secondID, secondLinks, secondErr := identity(second)
	secondErr = errors.Join(secondErr, second.Close())
	if secondErr != nil || secondID != firstID || !validFinderMetadata(secondID, secondLinks, maximum) {
		return nil, errors.Join(secondErr, ErrNativeCopyChanged)
	}
	filtered := make([]string, 0, len(names)-1)
	for _, name := range names {
		if name != finderMetadataName {
			filtered = append(filtered, name)
		}
	}
	return filtered, nil
}

func validFinderMetadata(id Identity, links uint64, maximum int64) bool {
	mode := id.Mode & 07777
	return id.Mode&unix.S_IFMT == unix.S_IFREG &&
		(mode == 0600 || mode == 0644) &&
		id.UID == uint32(os.Geteuid()) && links == 1 &&
		id.Size >= 0 && id.Size <= maximum
}
