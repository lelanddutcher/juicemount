//go:build darwin && cgo

package fusecustody

/*
#cgo CFLAGS: -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0
#include <sys/mount.h>
#include <errno.h>
static int jm_funmount_available(void) {
    if (__builtin_available(macOS 16.0, *)) return 1;
    return 0;
}
static int jm_funmount_fd(int fd) {
    if (__builtin_available(macOS 16.0, *)) return funmount(fd, 0);
    errno = ENOTSUP;
    return -1;
}
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type nativeRoot struct {
	fd       int
	before   unix.Statfs_t
	uuid     string
	readonly bool
}

func nativeCapability() bool { return C.jm_funmount_available() != 0 }

func (r *nativeRoot) UUID() string   { return r.uuid }
func (r *nativeRoot) ReadOnly() bool { return r.readonly }
func (r *nativeRoot) FSID() [2]int32 { return r.before.Fsid.Val }
func (r *nativeRoot) Close() error   { return unix.Close(r.fd) }

func sameMount(a, b unix.Statfs_t) bool {
	return a.Fsid == b.Fsid && a.Flags == b.Flags && a.Owner == b.Owner &&
		a.Fstypename == b.Fstypename && a.Mntonname == b.Mntonname && a.Mntfromname == b.Mntfromname
}

func (r *nativeRoot) Detach() error {
	var current unix.Statfs_t
	if err := unix.Fstatfs(r.fd, &current); err != nil || !sameMount(r.before, current) {
		return errors.New("retained FUSE descriptor identity changed")
	}
	if result, err := C.jm_funmount_fd(C.int(r.fd)); result != 0 {
		return fmt.Errorf("descriptor FUSE detach: %w", err)
	}
	return nil
}

type mountConfig struct {
	Format struct {
		UUID string `json:"UUID"`
	} `json:"Format"`
	Meta struct {
		MountPoint string `json:"MountPoint"`
		ReadOnly   *bool  `json:"ReadOnly"`
	} `json:"Meta"`
	Pid  int `json:"Pid"`
	PPid int `json:"PPid"`
}

func openNativeRoot(root string, child, parent int, source string) (rootCustody, error) {
	if !nativeCapability() {
		return nil, ErrUnsupported
	}
	if root == "" || root == "/" || !filepath.IsAbs(root) || filepath.Clean(root) != root || source == "" {
		return nil, errors.New("invalid FUSE custody path or source")
	}
	fd, err := unix.Open(root, unix.O_EVTONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	admitted := false
	defer func() {
		if !admitted {
			_ = unix.Close(fd)
		}
	}()
	var fs unix.Statfs_t
	var st unix.Stat_t
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return nil, err
	}
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || unix.ByteSliceToString(fs.Fstypename[:]) != "macfuse" ||
		fs.Owner != uint32(os.Getuid()) || unix.ByteSliceToString(fs.Mntonname[:]) != root ||
		unix.ByteSliceToString(fs.Mntfromname[:]) != source || fs.Fsid == (unix.Fsid{}) {
		return nil, errors.New("root is not the selected owned macFUSE mount")
	}
	configFD, err := unix.Openat(fd, ".config", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	configFile := os.NewFile(uintptr(configFD), "retained-fuse-config")
	defer configFile.Close()
	var cfgfs unix.Statfs_t
	var cfgst unix.Stat_t
	if err := unix.Fstatfs(configFD, &cfgfs); err != nil {
		return nil, err
	}
	if err := unix.Fstat(configFD, &cfgst); err != nil {
		return nil, err
	}
	if cfgst.Mode&unix.S_IFMT != unix.S_IFREG || fs.Fsid != cfgfs.Fsid {
		return nil, errors.New(".config is not a regular file on retained FUSE mount")
	}
	raw, err := io.ReadAll(io.LimitReader(configFile, (1<<20)+1))
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return nil, errors.New("retained .config unavailable or oversized")
	}
	var config mountConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, errors.New("retained .config type invalid")
	}
	if config.Pid != child || config.PPid != parent || config.Meta.MountPoint != root ||
		config.Meta.ReadOnly == nil || !CanonicalUUID(config.Format.UUID) {
		return nil, errors.New("retained .config does not identify original child and mount")
	}
	var after unix.Statfs_t
	if err := unix.Fstatfs(fd, &after); err != nil || !sameMount(fs, after) {
		return nil, errors.New("retained mount changed during .config admission")
	}
	admitted = true
	return &nativeRoot{fd: fd, before: fs, uuid: config.Format.UUID, readonly: *config.Meta.ReadOnly}, nil
}
