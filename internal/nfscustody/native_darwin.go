//go:build darwin && cgo

package nfscustody

/*
#cgo CFLAGS: -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0
#include <sys/mount.h>
#include <errno.h>
static int jm_nfs_funmount_available(void) {
    if (__builtin_available(macOS 16.0, *)) return 1;
    return 0;
}
static int jm_nfs_funmount_fd(int fd) {
    if (__builtin_available(macOS 16.0, *)) return funmount(fd, 0);
    errno = ENOTSUP;
    return -1;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type nativeRoot struct {
	fd     int
	before unix.Statfs_t
	closed bool
}

func nativeCapability() bool         { return C.jm_nfs_funmount_available() != 0 }
func (r *nativeRoot) FSID() [2]int32 { return r.before.Fsid.Val }
func (r *nativeRoot) Close() error {
	if r.closed {
		return errors.New("NFS descriptor close already attempted")
	}
	r.closed = true
	return unix.Close(r.fd)
}
func sameMount(a, b unix.Statfs_t) bool {
	return a.Fsid == b.Fsid && a.Flags == b.Flags && a.Owner == b.Owner &&
		a.Fstypename == b.Fstypename && a.Mntonname == b.Mntonname && a.Mntfromname == b.Mntfromname
}
func (r *nativeRoot) Detach() error {
	var current unix.Statfs_t
	if err := unix.Fstatfs(r.fd, &current); err != nil || !sameMount(r.before, current) {
		return errors.New("retained NFS descriptor identity changed")
	}
	if rc, err := C.jm_nfs_funmount_fd(C.int(r.fd)); rc != 0 {
		return fmt.Errorf("descriptor NFS detach: %w", err)
	}
	return nil
}
func openNativeRoot(path, source string) (rootCustody, error) {
	if !nativeCapability() {
		return nil, ErrUnsupported
	}
	if path == "" || path == "/" || !filepath.IsAbs(path) || filepath.Clean(path) != path || source == "" {
		return nil, errors.New("invalid NFS root identity")
	}
	fd, err := unix.Open(path, unix.O_EVTONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	admitted := false
	defer func() {
		if !admitted {
			_ = unix.Close(fd)
		}
	}()
	var fs, again unix.Statfs_t
	var st unix.Stat_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return nil, err
	}
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || unix.ByteSliceToString(fs.Fstypename[:]) != "nfs" ||
		fs.Owner != uint32(os.Getuid()) || unix.ByteSliceToString(fs.Mntonname[:]) != path ||
		unix.ByteSliceToString(fs.Mntfromname[:]) != source || fs.Fsid == (unix.Fsid{}) {
		return nil, errors.New("root is not the exact current-user NFS mount")
	}
	if err := unix.Fstatfs(fd, &again); err != nil || !sameMount(fs, again) {
		return nil, errors.New("NFS root changed during admission")
	}
	admitted = true
	return &nativeRoot{fd: fd, before: fs}, nil
}
