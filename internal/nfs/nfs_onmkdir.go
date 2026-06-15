package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

const (
	mkdirDefaultMode = 755
)

func onMkdir(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	attrs, err := ReadSetFileAttributes(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}
	if string(obj.Filename) == "." || string(obj.Filename) == ".." {
		return &NFSStatusError{NFSStatusExist, os.ErrExist}
	}

	newFolder := append(path, string(obj.Filename))
	newFolderPath := fs.Join(newFolder...)
	// Existence + parent checks resolve from the metadata cache with NO FUSE
	// round-trip (matches onCreate's cache-only checks). Two wins: (1) no
	// ~800ms FUSE existence-stat per dir on a deep-tree copy; (2) — critically
	// for offline — a FUSE Stat of the parent OFFLINE returns
	// ErrOfflineNotAvailable, which the old code mapped to NFSStatusAccess and
	// aborted an offline folder copy with Finder "the device disappeared". A
	// cache MISS now just proceeds: MkdirAll is the real arbiter (offline it
	// records the dir in the cache; online it creates it on FUSE). Reject only
	// on a POSITIVE cache hit that contradicts the mkdir. Filesystems without
	// the cache-only stat keep the old FUSE path with the FUSE-timeout guard.
	if cs, ok := fs.(cacheStater); ok {
		if s, found := cs.StatCacheOnly(newFolderPath); found && s.IsDir() {
			return &NFSStatusError{NFSStatusExist, nil}
		}
		if s, found := cs.StatCacheOnly(fs.Join(path...)); found && !s.IsDir() {
			return &NFSStatusError{NFSStatusNotDir, nil}
		}
	} else if s, err := fs.Stat(newFolderPath); err == nil {
		if s.IsDir() {
			return &NFSStatusError{NFSStatusExist, nil}
		}
	} else {
		// FUSE-stat TIMEOUT verifying the parent must NOT fail the mkdir (it
		// would JUKEBOX→retry-storm to "error 100060"); the parent exists by
		// construction. Proceed on an ambiguous timeout. See onCreate.
		if s, err := fs.Stat(fs.Join(path...)); err != nil {
			if !errors.Is(err, ErrFUSETimeout) {
				return &NFSStatusError{NFSStatusAccess, err}
			}
		} else if !s.IsDir() {
			return &NFSStatusError{NFSStatusNotDir, nil}
		}
	}

	if err := fs.MkdirAll(newFolderPath, attrs.Mode(mkdirDefaultMode)); err != nil {
		return &NFSStatusError{NFSStatusAccess, err}
	}

	fp := userHandle.ToHandle(fs, newFolder)
	changer := userHandle.Change(fs)
	if changer != nil {
		if err := attrs.Apply(changer, fs, newFolderPath); err != nil {
			// Non-fatal. OFFLINE the dir is only in the metadata cache (lazy
			// dir-creation), so attrs.Apply has nothing on FUSE to touch; ONLINE
			// JuiceFS commonly rejects attrs on directories anyway (same class
			// as the ._ sidecar attrs.Apply, handled non-fatally in onCreate).
			// The dir was created either way — don't fail the mkdir over attrs.
			if !pin.IsOffline() {
				Log.Warnf("onMkdir: attrs.Apply failed (non-fatal): %v", err)
			}
		}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// "handle follows"
	if err := xdr.Write(writer, uint32(1)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := xdr.Write(writer, fp); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, newFolder)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, nil, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
