package privatefile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

func nativeTargetName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\\x00") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// TargetIDs is a namespace enumeration, not authority to expose any entry.
// The journal compares every ID with the charged inventory and custody chain.
func (c *NativeControl) TargetIDs() (_ []string, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	dir, err := openDirectoryAt(c.root.root, "copies")
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if id, err := privateDir(dir); err != nil || id != c.root.copiesID {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	names, err = withoutFinderMetadata(dir, names, finderCopiesMetadataMaximum)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if !nativeCopyID(name) {
			return nil, ErrNativeCopyChanged
		}
	}
	sort.Strings(names)
	return names, nil
}

func (c *NativeControl) TargetDirectory(id string) (_ Identity, _ []string, resultErr error) {
	if !nativeCopyID(id) {
		return Identity{}, nil, ErrNativeCopyChanged
	}
	if err := c.check(); err != nil {
		return Identity{}, nil, err
	}
	dir, err := openDirectoryAt(c.root.copies, id)
	if err != nil {
		return Identity{}, nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	identity, err := privateDir(dir)
	if err != nil {
		return Identity{}, nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return Identity{}, nil, err
	}
	// Validate Finder's one metadata leaf, but return it to the caller. A
	// target is not empty before exposure merely because its only leaf is
	// Finder metadata; the journal may ignore it only for a verified final.
	if _, err := withoutFinderMetadata(dir, names, finderTargetMetadataMaximum); err != nil {
		return Identity{}, nil, err
	}
	for _, name := range names {
		if !nativeTargetName(name) {
			return Identity{}, nil, ErrNativeCopyChanged
		}
	}
	sort.Strings(names)
	return identity, names, nil
}

func (c *NativeControl) TargetFileIdentity(id, name string) (_ Identity, resultErr error) {
	if !nativeCopyID(id) || !nativeTargetName(name) {
		return Identity{}, ErrNativeCopyChanged
	}
	if err := c.check(); err != nil {
		return Identity{}, err
	}
	dir, err := openDirectoryAt(c.root.copies, id)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if _, err := privateDir(dir); err != nil {
		return Identity{}, err
	}
	f, err := OpenAt(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	return privateRegular(f)
}

// EnsureTargetDirectory creates or revalidates only the charged copy's empty
// target. The caller must have already matched the exact Intent/selection.
// Creation is not a Prepared record; an interrupted empty directory remains
// visible to explicit same-Intent recovery and blocks unrelated admission.
func (c *NativeControl) EnsureTargetDirectory(id string) (_ Identity, resultErr error) {
	if !nativeCopyID(id) {
		return Identity{}, ErrNativeCopyChanged
	}
	if err := c.check(); err != nil {
		return Identity{}, err
	}
	if err := unix.Mkdirat(int(c.root.copies.Fd()), id, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return Identity{}, err
	}
	dir, err := openDirectoryAt(c.root.copies, id)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	identity, err := privateDir(dir)
	if err != nil {
		return Identity{}, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return Identity{}, err
	}
	if len(names) != 0 {
		return Identity{}, ErrNativeCopyChanged
	}
	if err := dir.Sync(); err != nil {
		return Identity{}, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := c.root.copies.Sync(); err != nil {
		return Identity{}, errors.Join(err, ErrNativeCopyUncertain)
	}
	if fresh, err := openDirectoryAt(c.root.copies, id); err != nil {
		return Identity{}, err
	} else {
		check, checkErr := privateDir(fresh)
		closeErr := fresh.Close()
		if checkErr != nil || closeErr != nil || check != identity {
			return Identity{}, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
		}
	}
	return identity, nil
}

// HashTarget is the post-rename verification of the only expected named leaf.
// It does not return a path or descriptor and cannot establish current rights.
func (n *NativeCopy) HashTarget(ctx context.Context, name string, expectedDir Identity, maximum int64) (fileID Identity, digest string, resultErr error) {
	if ctx == nil || n == nil || !nativeTargetName(name) || name == finderMetadataName || maximum < 0 || maximum > nativeReceiveMaximum {
		return Identity{}, "", ErrNativeCopyChanged
	}
	if err := n.Revalidate(); err != nil {
		return Identity{}, "", err
	}
	dir, err := openDirectoryAt(n.root.copies, n.id)
	if err != nil {
		return Identity{}, "", err
	}
	defer func() {
		if e := dir.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			fileID = Identity{}
			digest = ""
		}
	}()
	if current, err := privateDir(dir); err != nil || current != expectedDir {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := dir.Readdirnames(-1)
	if err == nil {
		names, err = withoutFinderMetadata(dir, names, finderTargetMetadataMaximum)
	}
	if err != nil || len(names) != 1 || names[0] != name {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	f, err := OpenAt(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, "", err
	}
	defer func() {
		if e := f.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			fileID = Identity{}
			digest = ""
		}
	}()
	fileID, err = privateRegular(f)
	if err != nil || fileID.Size > maximum {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	h := sha256.New()
	buffer := make([]byte, 32768)
	for at := int64(0); at < fileID.Size; {
		if err := ctx.Err(); err != nil {
			return Identity{}, "", err
		}
		want := min(int64(len(buffer)), fileID.Size-at)
		read, err := f.ReadAt(buffer[:int(want)], at)
		if err != nil || int64(read) != want {
			return Identity{}, "", errors.Join(err, io.ErrUnexpectedEOF)
		}
		_, _ = h.Write(buffer[:read])
		at += want
	}
	if after, err := privateRegular(f); err != nil || after != fileID {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	current, err := OpenAt(dir, name, os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, "", err
	}
	currentID, currentErr := privateRegular(current)
	closeErr := current.Close()
	if currentErr != nil || closeErr != nil || currentID != fileID {
		return Identity{}, "", errors.Join(currentErr, closeErr, ErrNativeCopyChanged)
	}
	if after, err := privateDir(dir); err != nil || after != expectedDir {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	live, err := openDirectoryAt(n.root.copies, n.id)
	if err != nil {
		return Identity{}, "", errors.Join(err, ErrNativeCopyChanged)
	}
	liveDir, liveErr := privateDir(live)
	liveNames, namesErr := live.Readdirnames(-1)
	if namesErr == nil {
		liveNames, namesErr = withoutFinderMetadata(live, liveNames, finderTargetMetadataMaximum)
	}
	var liveFileID Identity
	if liveErr == nil && namesErr == nil && liveDir == expectedDir && len(liveNames) == 1 && liveNames[0] == name {
		currentFile, openErr := OpenAt(live, name, os.O_RDONLY, 0)
		if openErr == nil {
			liveFileID, openErr = privateRegular(currentFile)
			openErr = errors.Join(openErr, currentFile.Close())
		}
		liveErr = errors.Join(liveErr, openErr)
	}
	liveErr = errors.Join(liveErr, namesErr, live.Close())
	if liveErr != nil || liveDir != expectedDir || len(liveNames) != 1 || liveNames[0] != name || liveFileID != fileID {
		return Identity{}, "", errors.Join(liveErr, ErrNativeCopyChanged)
	}
	if err := n.Revalidate(); err != nil {
		return Identity{}, "", err
	}
	return fileID, hex.EncodeToString(h.Sum(nil)), nil
}

// ExposeNoReplace is the sole local rename syscall for a fully checked
// receiver permit. The bool reports whether rename happened even when a later
// directory barrier fails. No second rename follows an uncertain result.
func (n *NativeCopy) ExposeNoReplace(ctx context.Context, target Identity, name string, file Identity) (exposed bool, resultErr error) {
	if ctx == nil || !nativeTargetName(name) || n == nil {
		return false, ErrNativeCopyChanged
	}
	control, err := n.root.lockCustody()
	if err != nil {
		return false, err
	}
	defer func() {
		if e := control.Close(); e != nil {
			resultErr = errors.Join(resultErr, e, ErrNativeCopyUncertain)
		}
	}()
	if err := n.Revalidate(); err != nil {
		return false, err
	}
	if current, err := n.PartialIdentity(); err != nil || current != file {
		return false, errors.Join(err, ErrNativeCopyChanged)
	}
	dir, err := openDirectoryAt(n.root.copies, n.id)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := dir.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
			if exposed {
				resultErr = errors.Join(resultErr, ErrNativeCopyUncertain)
			}
		}
	}()
	if actual, err := privateDir(dir); err != nil || actual != target {
		return false, errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := dir.Readdirnames(-1)
	if err != nil || len(names) != 0 {
		return false, errors.Join(err, ErrNativeCopyChanged)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := renameNoReplace(int(n.dir.Fd()), "payload.partial", int(dir.Fd()), name); err != nil {
		return false, err
	}
	exposed = true
	if err := dir.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	if err := n.dir.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	if err := n.root.copies.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	return exposed, resultErr
}

// SyncTargetBarriers repeats the original post-rename directory barriers for
// explicit recovery. It cannot rename or repair a missing/substituted target.
func (n *NativeCopy) SyncTargetBarriers(target Identity) (resultErr error) {
	if n == nil {
		return ErrNativeCopyChanged
	}
	control, err := n.root.lockCustody()
	if err != nil {
		return err
	}
	defer func() {
		if e := control.Close(); e != nil {
			resultErr = errors.Join(resultErr, e, ErrNativeCopyUncertain)
		}
	}()
	if err := n.Revalidate(); err != nil {
		return err
	}
	dir, err := openDirectoryAt(n.root.copies, n.id)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if actual, err := privateDir(dir); err != nil || actual != target {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	if err := dir.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	if err := n.dir.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	if err := n.root.copies.Sync(); err != nil {
		resultErr = errors.Join(resultErr, err, ErrNativeCopyUncertain)
	}
	return resultErr
}
