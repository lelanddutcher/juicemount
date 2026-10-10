package nfs

import (
	"os"
	"path"
	"path/filepath"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// replacementFUSEIO retains the exact parent and requalifies the full ancestry
// against the mounted root before effects. It never creates parent directories.
type replacementFUSEIO struct {
	root    string
	witness metadata.SpoolReplacementWitness
	parent  *os.File
	dev     uint64
	inode   uint64
}

func openReplacementFUSE(root string, w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
	b := &replacementFUSEIO{root: root, witness: w}
	parent, err := b.openParent()
	if err != nil {
		return nil, nil, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(int(parent.Fd()), &st); err != nil {
		_ = parent.Close()
		return nil, nil, err
	}
	b.parent, b.dev, b.inode = parent, uint64(st.Dev), st.Ino
	return b, func() { _ = parent.Close() }, nil
}
func (b *replacementFUSEIO) openParent() (*os.File, error) {
	fd, err := unix.Open(b.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	current := os.NewFile(uintptr(fd), b.root)
	if err := pin.FUSEIdentityCheckFD(current.Fd()); err != nil {
		_ = current.Close()
		return nil, err
	}
	var root unix.Stat_t
	if err := unix.Fstat(fd, &root); err != nil {
		_ = current.Close()
		return nil, err
	}
	if root.Ino != 1 {
		_ = current.Close()
		return nil, metadata.ErrMirrorConflict
	}
	for _, node := range b.witness.Destination.Chain[:len(b.witness.Destination.Chain)-1] {
		next, err := birthOpenDirectoryAt(current, path.Base(node.Path))
		_ = current.Close()
		if err != nil {
			return nil, err
		}
		current = next
		var st unix.Stat_t
		if err := unix.Fstat(int(current.Fd()), &st); err != nil {
			_ = current.Close()
			return nil, err
		}
		if st.Ino != node.Inode || st.Dev != root.Dev {
			_ = current.Close()
			return nil, metadata.ErrMirrorConflict
		}
	}
	return current, nil
}
func (b *replacementFUSEIO) Check() error {
	if err := pin.FUSEIdentityCheckFD(b.parent.Fd()); err != nil {
		return err
	}
	p, err := b.openParent()
	if err != nil {
		return err
	}
	defer p.Close()
	var st unix.Stat_t
	if err := unix.Fstat(int(p.Fd()), &st); err != nil {
		return err
	}
	if uint64(st.Dev) != b.dev || st.Ino != b.inode {
		return metadata.ErrMirrorConflict
	}
	return nil
}
func replacementSimpleName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name
}
func (b *replacementFUSEIO) Open(name string, create bool) (replacementPublishFile, error) {
	if !replacementSimpleName(name) {
		return nil, metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return nil, err
	}
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
	if create {
		flags = unix.O_RDWR | unix.O_CREAT | unix.O_EXCL | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
	}
	fd, err := unix.Openat(int(b.parent.Fd()), name, flags, 0600)
	if err != nil {
		return nil, err
	}
	f := &replacementFUSEFile{File: os.NewFile(uintptr(fd), name), parent: b, name: name}
	if _, err := f.Stat(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// Create uses the intended saved permissions at the birth transaction. A
// process umask may narrow them; refuse that result rather than relaxing a
// born stage with a later chmod.
func (b *replacementFUSEIO) Create(name string, mode os.FileMode) (replacementPublishFile, error) {
	if !replacementSimpleName(name) {
		return nil, metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(b.parent.Fd()), name,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	f := &replacementFUSEFile{File: os.NewFile(uintptr(fd), name), parent: b, name: name}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		_ = f.Close()
		return nil, err
	}
	got, err := f.Stat()
	named, nameErr := b.Stat(name)
	checkErr := b.Check()
	if err != nil || nameErr != nil || checkErr != nil || got != named || uint32(st.Mode)&07777 != uint32(mode.Perm()) {
		_ = f.Close()
		return nil, metadata.ErrMirrorConflict
	}
	return f, nil
}

// OpenOwned reopens only the exact currently qualified stage. It never creates
// or truncates and checks descriptor and named identity after the open.
func (b *replacementFUSEIO) OpenOwned(name string, expected replaceguard.File) (replacementPublishFile, error) {
	if !replacementSimpleName(name) || expected.Name != name || expected.Parent != b.inode || expected.Inode <= 1 {
		return nil, metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(b.parent.Fd()), name,
		unix.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := &replacementFUSEFile{File: os.NewFile(uintptr(fd), name), parent: b, name: name}
	descriptor, err := f.Stat()
	named, nameErr := b.Stat(name)
	checkErr := b.Check()
	if err != nil || nameErr != nil || checkErr != nil || descriptor != expected || named != expected {
		_ = f.Close()
		return nil, metadata.ErrMirrorConflict
	}
	return f, nil
}
func (b *replacementFUSEIO) statRecord(name string, st unix.Stat_t) (replaceguard.File, error) {
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Size < 0 || uint64(st.Dev) != b.dev {
		return replaceguard.File{}, metadata.ErrMirrorConflict
	}
	return replaceguard.File{Name: name, Parent: b.inode, Inode: st.Ino, Length: uint64(st.Size), MtimeNS: st.Mtim.Nano(), CtimeNS: st.Ctim.Nano()}, nil
}
func (b *replacementFUSEIO) Stat(name string) (replaceguard.File, error) {
	if !replacementSimpleName(name) {
		return replaceguard.File{}, metadata.ErrMirrorConflict
	}
	var st unix.Stat_t
	if err := unix.Fstatat(int(b.parent.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return replaceguard.File{}, err
	}
	return b.statRecord(name, st)
}
func (b *replacementFUSEIO) Rename(source, destination string) error {
	if !replacementSimpleName(source) || !replacementSimpleName(destination) {
		return metadata.ErrMirrorConflict
	}
	if err := b.Check(); err != nil {
		return err
	}
	return unix.Renameat(int(b.parent.Fd()), source, int(b.parent.Fd()), destination)
}

type replacementFUSEFile struct {
	*os.File
	parent *replacementFUSEIO
	name   string
}

func (f *replacementFUSEFile) Stat() (replaceguard.File, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return replaceguard.File{}, err
	}
	return f.parent.statRecord(f.name, st)
}
func (f *replacementFUSEFile) SetMtime(ns int64) error { return replacementFDSetMtime(int(f.Fd()), ns) }

func (f *replacementFUSEFile) Sync() error                    { return (fuseDurableFile{File: f.File}).Sync() }
func (f *replacementFUSEFile) SetMode(mode os.FileMode) error { return f.File.Chmod(mode.Perm()) }
