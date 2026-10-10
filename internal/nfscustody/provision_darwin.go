//go:build darwin && cgo

package nfscustody

/*
#cgo CFLAGS: -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0
#include <sys/acl.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <membership.h>
#include <string.h>
// Return 0 for safe ACLs, 1 for any leaf ACL or ancestry mutation grant to
// another principal, and -1 when lookup or principal mapping is ambiguous.
static int jm_acl_unsafe(int fd, int leaf) {
    filesec_t fsec = filesec_init();
    if (!fsec) return -1;
    struct stat st;
    int valid = 0;
    if (fstatx_np(fd, &st, fsec) != 0 || filesec_query_property(fsec, FILESEC_ACL, &valid) != 0) {
        filesec_free(fsec); return -1;
    }
    if (!valid) { filesec_free(fsec); return 0; }
    acl_t a = NULL;
    if (filesec_get_property(fsec, FILESEC_ACL, &a) != 0 || !a) { filesec_free(fsec); return -1; }
    filesec_free(fsec);
    if (acl_valid(a) != 0) { acl_free(a); return -1; }
    acl_entry_t entry;
    int id = ACL_FIRST_ENTRY;
    int unsafe = 0;
    int rc;
    while ((rc = acl_get_entry(a, id, &entry)) == 0) {
        id = ACL_NEXT_ENTRY;
        if (leaf) { unsafe = 1; break; }
        acl_tag_t tag;
        if (acl_get_tag_type(entry, &tag) != 0) { acl_free(a); return -1; }
        if (tag == ACL_EXTENDED_ALLOW) {
            acl_permset_t perms;
            if (acl_get_permset(entry, &perms) != 0) { acl_free(a); return -1; }
            acl_perm_t mutation[] = {ACL_ADD_FILE, ACL_ADD_SUBDIRECTORY, ACL_DELETE, ACL_DELETE_CHILD,
                ACL_WRITE_ATTRIBUTES, ACL_WRITE_EXTATTRIBUTES, ACL_WRITE_SECURITY, ACL_CHANGE_OWNER};
            for (unsigned i = 0; i < sizeof(mutation)/sizeof(mutation[0]); ++i) {
                int granted = acl_get_perm_np(perms, mutation[i]);
                if (granted < 0) { acl_free(a); return -1; }
                if (granted > 0) { unsafe = 1; break; }
            }
            if (unsafe) {
                uuid_t owner;
                void *qualifier = acl_get_qualifier(entry);
                if (!qualifier) { acl_free(a); return -1; }
                int mapped = mbr_uid_to_uuid(st.st_uid, owner);
                int is_owner = mapped == 0 && memcmp(qualifier, owner, sizeof(uuid_t)) == 0;
                int freed = acl_free(qualifier);
                if (mapped != 0 || freed != 0) { acl_free(a); return -1; }
                if (!is_owner) break;
                unsafe = 0;
            }
            continue;
        }
        if (tag != ACL_EXTENDED_DENY) { acl_free(a); return -1; }
    }
    if (!unsafe && rc == -1 && errno != EINVAL) { acl_free(a); return -1; }
    if (acl_free(a) != 0) return -1;
    return unsafe;
}
*/
import "C"

import (
	"errors"
	"io"
	"os"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"golang.org/x/sys/unix"
)

func provisionACL(fd int, leaf bool) error {
	which := C.int(0)
	if leaf {
		which = 1
	}
	state := C.jm_acl_unsafe(C.int(fd), which)
	if state < 0 {
		return errors.New("directory ACL could not be validated")
	}
	if state > 0 {
		return errors.New("directory has extended ACL granting or carrying unexpected access")
	}
	return nil
}
func rootDirectoryEmpty(fd int) error {
	dup, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(dup), "privileged exact NFS directory")
	names, readErr := f.Readdirnames(1)
	closeErr := f.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(names) > 0 {
		return errors.New("NFS directory is not empty")
	}
	return nil
}
func rootProvisionSnapshot(req provisionRequest) error {
	snapshot, err := mounttable.CaptureSnapshot()
	if err != nil {
		return err
	}
	if snapshot.Version != 1 || snapshot.BootID != req.Boot || snapshot.Entries == nil {
		return errors.New("provision mount snapshot incomplete or boot changed")
	}
	for _, e := range snapshot.Entries {
		if e.Path == req.Path || strings.HasPrefix(e.Path, req.Path+"/") {
			return errors.New("provision target or descendant is mounted")
		}
		if (e.Path == "/Volumes" || e.Path == "/" || strings.HasPrefix("/Volumes/", e.Path+"/")) && e.Type != "apfs" && e.Type != "hfs" {
			return errors.New("provision ancestry is not a local filesystem")
		}
	}
	return nil
}
func nativeProvision(req provisionRequest) (provisionReceipt, bool, error) {
	result := provisionReceipt{Request: req}
	if !req.valid() {
		return result, false, errors.New("invalid provision request")
	}
	if err := rootProvisionSnapshot(req); err != nil {
		return result, false, err
	}
	parent, err := unix.Open("/Volumes", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return result, false, err
	}
	defer unix.Close(parent)
	if err := unix.Flock(parent, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return result, false, errors.New("/Volumes provision lock busy or unsupported")
	}
	defer unix.Flock(parent, unix.LOCK_UN)
	var parentStat, namedParent unix.Stat_t
	var parentFS unix.Statfs_t
	if err := unix.Fstat(parent, &parentStat); err != nil {
		return result, false, err
	}
	if err := unix.Lstat("/Volumes", &namedParent); err != nil {
		return result, false, err
	}
	if err := unix.Fstatfs(parent, &parentFS); err != nil {
		return result, false, err
	}
	if parentStat.Dev != namedParent.Dev || parentStat.Ino != namedParent.Ino || parentStat.Uid != 0 || parentStat.Mode&unix.S_IFMT != unix.S_IFDIR || parentStat.Mode&0022 != 0 {
		return result, false, errors.New("/Volumes parent identity or permissions unsafe")
	}
	if uint64(parentStat.Dev) != req.ExpectedParentDev || parentStat.Ino != req.ExpectedParentIno {
		return result, false, errors.New("/Volumes parent changed after authorization")
	}
	if fsType := unix.ByteSliceToString(parentFS.Fstypename[:]); fsType != "apfs" && fsType != "hfs" {
		return result, false, errors.New("/Volumes parent is not local")
	}
	if err := provisionACL(parent, false); err != nil {
		return result, false, err
	}
	result.ParentDev = uint64(parentStat.Dev)
	result.ParentIno = parentStat.Ino
	leafName := strings.TrimPrefix(req.Path, "/Volumes/")
	var before unix.Stat_t
	err = unix.Fstatat(parent, leafName, &before, unix.AT_SYMLINK_NOFOLLOW)
	if req.Mode == "create" {
		if err == nil {
			return result, false, errors.New("create target already exists")
		}
		if !errors.Is(err, unix.ENOENT) {
			return result, false, err
		}
	} else {
		if err != nil {
			return result, false, err
		}
		if before.Mode&unix.S_IFMT != unix.S_IFDIR || before.Uid != 0 {
			return result, false, errors.New("migration target must be root-owned directory")
		}
		if uint64(before.Dev) != req.ExpectedLeafDev || before.Ino != req.ExpectedLeafIno {
			return result, false, errors.New("migration target changed after authorization")
		}
	}
	effect := false
	if req.Mode == "create" {
		if err := unix.Mkdirat(parent, leafName, 0700); err != nil {
			return result, false, err
		}
		effect = true
	}
	leaf, err := unix.Openat(parent, leafName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return result, effect, err
	}
	defer unix.Close(leaf)
	var st, namedLeaf unix.Stat_t
	var leafFS unix.Statfs_t
	if err := unix.Fstat(leaf, &st); err != nil {
		return result, effect, err
	}
	result.LeafDev = uint64(st.Dev)
	result.LeafIno = st.Ino
	result.BeforeUID = st.Uid
	result.BeforeGID = st.Gid
	result.BeforeMode = uint32(st.Mode & 07777)
	if err := unix.Fstatat(parent, leafName, &namedLeaf, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return result, effect, err
	}
	if err := unix.Fstatfs(leaf, &leafFS); err != nil {
		return result, effect, err
	}
	if st.Dev != namedLeaf.Dev || st.Ino != namedLeaf.Ino || leafFS.Fsid != parentFS.Fsid || st.Uid != 0 || st.Mode&unix.S_IFMT != unix.S_IFDIR {
		return result, effect, errors.New("provision leaf identity or filesystem changed")
	}
	if req.Mode == "migrate" && (st.Dev != before.Dev || st.Ino != before.Ino) {
		return result, effect, errors.New("migration leaf replaced")
	}
	if err := rootDirectoryEmpty(leaf); err != nil {
		return result, effect, err
	}
	if err := provisionACL(leaf, true); err != nil {
		return result, effect, err
	}
	if err := rootProvisionSnapshot(req); err != nil {
		return result, effect, err
	}
	if err := unix.Lstat("/Volumes", &namedParent); err != nil {
		return result, effect, err
	}
	if namedParent.Dev != parentStat.Dev || namedParent.Ino != parentStat.Ino {
		return result, effect, errors.New("/Volumes parent changed before mutation")
	}
	if err := unix.Fstatat(parent, leafName, &namedLeaf, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return result, effect, err
	}
	if err := unix.Fstat(leaf, &st); err != nil {
		return result, effect, err
	}
	if st.Dev != namedLeaf.Dev || st.Ino != namedLeaf.Ino || st.Uid != 0 {
		return result, effect, errors.New("provision leaf changed before mutation")
	}
	result.BeforeUID = st.Uid
	result.BeforeGID = st.Gid
	result.BeforeMode = uint32(st.Mode & 07777)
	// Every potentially mutating syscall follows all refusal checks. A failed
	// fchown may still have had an effect, so mark the operation uncertain.
	effect = true
	if err := unix.Fchown(leaf, int(req.UID), int(req.GID)); err != nil {
		return result, effect, err
	}
	if err := unix.Fchmod(leaf, 0700); err != nil {
		return result, effect, err
	}
	if err := unix.Fstat(leaf, &st); err != nil {
		return result, effect, err
	}
	if err := unix.Fstatat(parent, leafName, &namedLeaf, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return result, effect, err
	}
	if st.Dev != namedLeaf.Dev || st.Ino != namedLeaf.Ino || st.Uid != req.UID || st.Gid != req.GID || st.Mode&0777 != 0700 {
		return result, effect, errors.New("provision postcondition mismatch")
	}
	if err := provisionACL(leaf, true); err != nil {
		return result, effect, err
	}
	if err := rootDirectoryEmpty(leaf); err != nil {
		return result, effect, err
	}
	if err := unix.Fsync(leaf); err != nil {
		return result, effect, err
	}
	if err := unix.Fsync(parent); err != nil {
		return result, effect, err
	}
	if err := rootProvisionSnapshot(req); err != nil {
		return result, effect, err
	}
	if err := unix.Lstat("/Volumes", &namedParent); err != nil {
		return result, effect, err
	}
	if namedParent.Dev != parentStat.Dev || namedParent.Ino != parentStat.Ino {
		return result, effect, errors.New("/Volumes parent rebound during provision")
	}
	result.LeafDev = uint64(st.Dev)
	result.LeafIno = st.Ino
	result.AfterUID = st.Uid
	result.AfterGID = st.Gid
	result.AfterMode = uint32(st.Mode & 07777)
	if req.Mode == "create" {
		result.State = "created"
	} else {
		result.State = "migrated"
	}
	return result, effect, nil
}
