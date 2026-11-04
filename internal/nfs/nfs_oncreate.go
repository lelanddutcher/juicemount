package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// createmode3, RFC 1813 section 3.3.8.
const (
	CreateUnchecked = 0
	CreateGuarded   = 1
	CreateExclusive = 2
)

// CreateHow is a CREATE's createhow3: its mode and, for EXCLUSIVE, the
// client's verifier.
type CreateHow struct {
	Mode     uint32
	Verifier [8]byte
}

// ModeCreator is implemented by a filesystem that serves the CREATE mode
// itself (RFC 1813 section 3.3.8). GUARDED must refuse a name that already
// exists with os.ErrExist, which is answered NFS3ERR_EXIST. EXCLUSIVE must too,
// unless the file there is the one an EXCLUSIVE create with the same verifier
// made: that request is its resend and succeeds. UNCHECKED must not replace an
// existing file with a new object: the reply's handle is the existing file's.
// A filesystem without it gets Create for every mode.
type ModeCreator interface {
	CreateMode(path string, how CreateHow) (billy.File, error)
}

// cacheStater is implemented by the filesystem (nfs.juiceFS) to expose a
// cache-only stat that never touches FUSE. The guarded-CREATE existence/parent
// checks use it so a deep-tree copy of tens of thousands of new files doesn't
// pay an 800ms FUSE existence-stat per file — the cost that tripped the
// soft-mount timeout ("error 100060") on recursive copies (2026-06-14).
type cacheStater interface {
	StatCacheOnly(path string) (os.FileInfo, bool)
}

func onCreate(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	err := xdr.Read(w.req.Body, &obj)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	how, err := xdr.ReadUint32(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	var attrs *SetFileAttributes
	createHow := CreateHow{Mode: how}
	if how == CreateUnchecked || how == CreateGuarded {
		sattr, err := ReadSetFileAttributes(w.req.Body)
		if err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		attrs = sattr
	} else if how == CreateExclusive {
		// [JM5] macOS uses exclusive mode for atomic file creation (O_EXCL).
		// It is served here rather than refused NOTSUPP, which made macOS
		// retry every create as unchecked. The createverf3 verifier is passed
		// to a ModeCreator, which tells a resend of a create it already made
		// (same verifier) from a new one.
		if err := xdr.Read(w.req.Body, &createHow.Verifier); err != nil {
			return &NFSStatusError{NFSStatusInval, err}
		}
		// No attrs to apply for exclusive creates
	} else {
		// invalid
		return &NFSStatusError{NFSStatusNotSupp, os.ErrInvalid}
	}

	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, nil}
	}

	newFile := append(path, string(obj.Filename))
	newFilePath := fs.Join(newFile...)

	// [JM5] Existence check: only reject if the entry is a DIRECTORY. Use the
	// CACHE-ONLY stat (no FUSE round-trip): a brand-new file is absent from the
	// cache, so the common copy case proceeds without an 800ms FUSE existence-
	// stat — the dominant cost that made a recursive deep-tree Finder copy (tens
	// of thousands of new files → that many cache-miss stats through the
	// nfsLstatGate) saturate the metadata path and trip the soft-mount timeout
	// ("error 100060"), while a flat large-file copy sailed through (2026-06-14,
	// root-caused via a faithful Finder reproduction). fs.Create is the real
	// arbiter for any conflict. Filesystems without the cache-only stat fall
	// back to the original FUSE Stat.
	if cs, ok := fs.(cacheStater); ok {
		if s, found := cs.StatCacheOnly(newFilePath); found && s.IsDir() {
			return &NFSStatusError{NFSStatusExist, nil}
		}
	} else if s, err := fs.Stat(newFilePath); err == nil && s.IsDir() {
		return &NFSStatusError{NFSStatusExist, nil}
	}
	// Verify the parent directory — also cache-only. Finder MKDIRs the parent
	// before any file in it, so it is in the cache; if the cache says it's not a
	// dir, reject. A cache MISS is treated as "assume it exists" (proceed) — the
	// parent was just created locally; fs.Create returns ENOENT if it's truly
	// gone. The old FUSE-Stat fallback (with the ErrFUSETimeout proceed guard)
	// is kept for filesystems without the cache-only stat.
	if cs, ok := fs.(cacheStater); ok {
		if s, found := cs.StatCacheOnly(fs.Join(path...)); found && !s.IsDir() {
			return &NFSStatusError{NFSStatusNotDir, nil}
		}
	} else if s, err := fs.Stat(fs.Join(path...)); err != nil {
		if !errors.Is(err, ErrFUSETimeout) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
	} else if !s.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}

	if scoped, ok := fs.(MutationPrincipalSetter); ok {
		scoped.SetMutationPrincipal(requestPrincipal(w.req))
	}
	var file billy.File
	if creator, ok := fs.(ModeCreator); ok {
		file, err = creator.CreateMode(newFilePath, createHow)
	} else {
		file, err = fs.Create(newFilePath)
	}
	if err != nil {
		Log.Errorf("Error Creating: %v", err)
		// A CREATE against a full write spool (nfs.ErrSpoolFull wraps
		// syscall.ENOSPC) must surface as NFS3ERR_NOSPC ("disk full"),
		// not EACCES. Everything else keeps the legacy Access mapping.
		if errors.Is(err, syscall.ENOSPC) {
			return &NFSStatusError{NFSStatusNoSPC, err}
		}
		// GUARDED or EXCLUSIVE found the name taken.
		if how != CreateUnchecked && errors.Is(err, os.ErrExist) {
			return &NFSStatusError{NFSStatusExist, err}
		}
		return writeRefusalStatus(err, NFSStatusAccess)
	}
	if err := file.Close(); err != nil {
		Log.Errorf("Error Creating: %v", err)
		return writeRefusalStatus(err, NFSStatusAccess)
	}

	fp := userHandle.ToHandle(fs, newFile)
	changer := userHandle.Change(fs)
	if attrs != nil {
		if err := attrs.Apply(changer, fs, newFilePath); err != nil {
			// [JM5] Non-fatal: attribute application can fail for ._resource
			// fork files that don't persist on JuiceFS. Log but don't fail
			// the create — the file was already created successfully.
			Log.Warnf("onCreate: attrs.Apply failed (non-fatal): %v", err)
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
	if err := WritePostOpAttrs(writer, tryStat(fs, []string{file.Name()})); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	// dir_wcc (we don't include pre_op_attr)
	if err := xdr.Write(writer, uint32(0)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WritePostOpAttrs(writer, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
