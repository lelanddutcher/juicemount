package derivatives

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// Descriptor-relative writes into the derivative tree.
//
// WHY THIS EXISTS. Deleting the unanchored OPEN helper (openguard.go) closed the
// read side and left the write side with the identical shape, because
// DerivBlobDir(mount, inode) still handed out a joined absolute path that every
// generator re-resolved BY NAME. A guard that validates a path and then lets the
// write re-resolve it is check-then-use: the attacker swaps the <inode>
// component during the ffprobe+encode window, which is seconds to minutes.
//
// So the joined-path primitive is gone too, and writers hold a DESCRIPTOR for
// the validated directory. openat/renameat against that descriptor cannot be
// redirected by anything that happens to the path afterwards.

// tmpSeq makes temp/stage names unique within a process.
var tmpSeq uint64

// OpenDirUnder walks root/rel with O_NOFOLLOW at every component and returns the
// directory's descriptor. The caller owns it and must Close it.
func OpenDirUnder(root, rel string) (*os.File, error) {
	clean := trimRel(rel)
	if clean == "" {
		return nil, fmt.Errorf("derivatives: empty relative dir under %q", root)
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("derivatives: open root %q: %w", root, err)
	}
	for _, part := range splitRel(clean) {
		next, oerr := unix.Openat(fd, part,
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if oerr != nil {
			return nil, fmt.Errorf("derivatives: walk %q under %q: %w", clean, root, oerr)
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), filepath.Join(root, clean)), nil
}

// WriteFileAt atomically writes one file INSIDE dir, addressing every step
// through dir's descriptor: a temp file is created with O_EXCL|O_NOFOLLOW,
// written, fsync'd, then renameat'd onto the final name. No step resolves a
// path, so no step can be redirected.
func WriteFileAt(dir *os.File, name string, data []byte, perm os.FileMode) error {
	if err := validLeaf(name); err != nil {
		return err
	}
	dfd := int(dir.Fd())
	// UNIQUE per writer. A fixed ".tmp-<name>" let two concurrent writers for the
	// same asset collide: the second unlinks the first's temp, the first keeps
	// writing to a now-unlinked inode, and its renameat then fails with ENOENT —
	// so one writer's update vanishes with only a returned error to show for it.
	// The farm's three passes are sequential today, but WriteManifestSidecar is
	// called from all three and correctness here should not rest on that.
	tmp := fmt.Sprintf(".tmp-%d-%d-%s", os.Getpid(), atomic.AddUint64(&tmpSeq, 1), name)
	fd, err := unix.Openat(dfd, tmp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(perm))
	if err != nil {
		return fmt.Errorf("derivatives: create temp %q: %w", tmp, err)
	}
	f := os.NewFile(uintptr(fd), tmp)
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = unix.Unlinkat(dfd, tmp, 0)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = unix.Unlinkat(dfd, tmp, 0)
		return err
	}
	if err := f.Close(); err != nil {
		_ = unix.Unlinkat(dfd, tmp, 0)
		return err
	}
	if err := unix.Renameat(dfd, tmp, dfd, name); err != nil {
		_ = unix.Unlinkat(dfd, tmp, 0)
		return fmt.Errorf("derivatives: commit %q: %w", name, err)
	}
	return nil
}

// StageNameAt creates an EMPTY file exclusively under dir and returns the name
// plus the absolute path an external process (ffmpeg) must be given, because a
// subprocess takes a path and not a descriptor.
//
// HONEST LIMITATION, stated rather than papered over: the subprocess resolves
// that path by NAME, so this is not as strong as WriteFileAt. What it does buy
// is that the name is created by us with O_EXCL|O_NOFOLLOW inside the walked
// directory, so it cannot already BE a symlink and cannot be replaced by one
// without first unlinking a file the attacker does not own. Closing the residual
// window needs the subprocess to accept a descriptor (/proc/self/fd on Linux,
// /dev/fd on Darwin) — a change to the encode path that must be validated on a
// real farm host before it ships, so it is deliberately not done blind here.
func StageNameAt(dir *os.File, name string) (stagedName, absPath string, err error) {
	if err := validLeaf(name); err != nil {
		return "", "", err
	}
	dfd := int(dir.Fd())
	// The staged name keeps the ORIGINAL EXTENSION last, because ffmpeg selects
	// its muxer from it — ".stage-1234-7-proxy.mp4" still ends in ".mp4".
	staged := fmt.Sprintf(".stage-%d-%d-%s", os.Getpid(), atomic.AddUint64(&tmpSeq, 1), name)
	if err := unix.Unlinkat(dfd, staged, 0); err != nil && err != unix.ENOENT {
		return "", "", fmt.Errorf("derivatives: clear stage %q: %w", staged, err)
	}
	fd, err := unix.Openat(dfd, staged,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o644)
	if err != nil {
		return "", "", fmt.Errorf("derivatives: stage %q: %w", staged, err)
	}
	unix.Close(fd)
	return staged, filepath.Join(dir.Name(), staged), nil
}

// CommitStagedAt renames a staged name onto its final name through dir's
// descriptor, so the COMMIT itself cannot be redirected even though the
// subprocess wrote by path.
//
// IT REFUSES AN EMPTY STAGED FILE, and that check is the point rather than a
// nicety. StageNameAt creates the output file BEFORE the generator runs, so a
// generator that returns nil WITHOUT writing anything — Waveform does exactly
// that when the source turns out to have no audio — would otherwise publish the
// 0-byte placeholder as a `ready` derivative. That inverts the project's
// fail-closed rule: a missing blob 404s and the reader regenerates locally,
// whereas a 0-byte blob is served with 200 and looks like a real answer. A
// staging design must never be able to commit bytes the generator did not write.
func CommitStagedAt(dir *os.File, stagedName, finalName string) error {
	if err := validLeaf(finalName); err != nil {
		return err
	}
	dfd := int(dir.Fd())
	var st unix.Stat_t
	if err := unix.Fstatat(dfd, stagedName, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("derivatives: staged %q vanished before commit: %w", stagedName, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Unlinkat(dfd, stagedName, 0)
		return fmt.Errorf("derivatives: staged %q is not a regular file", stagedName)
	}
	if st.Size == 0 {
		_ = unix.Unlinkat(dfd, stagedName, 0)
		return fmt.Errorf("derivatives: refusing to publish an empty %q — the generator wrote nothing", finalName)
	}
	if err := unix.Renameat(dfd, stagedName, dfd, finalName); err != nil {
		_ = unix.Unlinkat(dfd, stagedName, 0)
		return fmt.Errorf("derivatives: commit %q: %w", finalName, err)
	}
	return nil
}

// DiscardStagedAt removes a staged file after a failed generation.
func DiscardStagedAt(dir *os.File, stagedName string) {
	_ = unix.Unlinkat(int(dir.Fd()), stagedName, 0)
}

// validLeaf refuses anything that is not a single flat filename — the derivative
// tree holds only reserved flat names, and a separator here would re-introduce
// exactly the multi-component resolution these helpers exist to avoid.
func validLeaf(name string) error {
	if name == "" || name == "." || name == ".." ||
		filepath.Base(name) != name || filepath.IsAbs(name) {
		return fmt.Errorf("derivatives: %q is not a flat filename", name)
	}
	return nil
}
