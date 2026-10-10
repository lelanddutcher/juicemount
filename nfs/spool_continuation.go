package nfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

type pendingBoundContinuation struct {
	path string
	file string
	fd   *os.File
}

// qualifyBoundContinuation pins local root/files and checks the exact birth
// image by FD and directory entry. current is the already-open active FD; nil
// opens a READY image through the pinned directory. No FUSE or SQL I/O occurs.
func (s *SpoolStore) qualifyBoundContinuation(c metadata.SpoolContinuationSnapshot, current *os.File) (qualified *os.File, retErr error) {
	if c.Legacy || c.BirthID == "" || c.BirthIdentity == "" {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	size := int64(-1)
	if c.State == string(metadata.DrainReady) {
		size = c.Size
	}
	return s.qualifySpoolImage(c.File, c.BirthIdentity, size, current)
}

// qualifySpoolImage verifies an existing authority's identity; it never creates
// one. size<0 permits a live bound writer whose coverage is checked separately.
func (s *SpoolStore) qualifySpoolImage(file, identity string, size int64, current *os.File) (qualified *os.File, retErr error) {
	if identity == "" || !filepath.IsAbs(s.root) || filepath.Clean(s.root) != s.root ||
		!filepath.IsAbs(file) || filepath.Clean(file) != file || filepath.Dir(file) != filepath.Join(s.root, SpoolFilesSubdir) {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	base := filepath.Base(file)
	if base == "." || base == ".." {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	rootFD, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("bound root: %w: %v", metadata.ErrSpoolContinuationConflict, err)
	}
	root := os.NewFile(uintptr(rootFD), s.root)
	defer root.Close()
	files, err := birthOpenDirectoryAt(root, SpoolFilesSubdir)
	if err != nil {
		return nil, fmt.Errorf("bound files directory: %w: %v", metadata.ErrSpoolContinuationConflict, err)
	}
	defer files.Close()
	if current == nil {
		fd, err := unix.Openat(int(files.Fd()), base, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("bound image: %w: %v", metadata.ErrSpoolContinuationConflict, err)
		}
		qualified = os.NewFile(uintptr(fd), file)
		opened := qualified
		defer func() {
			if retErr != nil {
				_ = opened.Close()
			}
		}()
	} else {
		qualified = current
	}
	info, err := qualified.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	fdID, fdSize, err := birthFDIdentity(qualified)
	if err != nil || fdID != identity || size >= 0 && fdSize != size {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	leafID, leafSize, err := birthEntryIdentity(files, base)
	if err != nil || leafID != fdID || size >= 0 && leafSize != size {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	rootID, _, err := birthFDIdentity(root)
	if err != nil {
		return nil, err
	}
	filesID, _, err := birthFDIdentity(files)
	if err != nil {
		return nil, err
	}
	currentRootFD, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	currentRoot := os.NewFile(uintptr(currentRootFD), s.root)
	defer currentRoot.Close()
	nowRootID, _, err := birthFDIdentity(currentRoot)
	if err != nil || nowRootID != rootID {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	currentFiles, err := birthOpenDirectoryAt(currentRoot, SpoolFilesSubdir)
	if err != nil {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	defer currentFiles.Close()
	nowFilesID, _, err := birthFDIdentity(currentFiles)
	if err != nil || nowFilesID != filesID {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	nowLeafID, _, err := birthEntryIdentity(currentFiles, base)
	if err != nil || nowLeafID != fdID {
		return nil, metadata.ErrSpoolContinuationConflict
	}
	return qualified, nil
}

// resumeReadyLocked installs an already-open FD after its metadata CAS. Caller
// holds the entry mutex. This callback makes no filesystem or SQL calls.
func (e *SpoolEntry) resumeReadyLocked(f *os.File) {
	e.file = f
	e.closed = false
	e.writing.Store(true)
	e.refcount = 1
	e.committed = false
	e.hasher = nil
	e.hashValid = false
	e.sha256 = nil
	e.writtenExtents = nil
	e.lastWrite.Store(time.Now().UnixNano())
}

func (s *SpoolStore) hasPendingBoundContinuation(id int64) bool {
	s.pendingContinuationMu.Lock()
	_, ok := s.pendingContinuations[id]
	s.pendingContinuationMu.Unlock()
	return ok
}

func (s *SpoolStore) hasPendingBoundContinuationPath(path string) bool {
	s.pendingContinuationMu.Lock()
	_, ok := s.pendingContinuationPaths[path]
	s.pendingContinuationMu.Unlock()
	return ok
}

func (s *SpoolStore) hasPendingBoundContinuationUnder(oldPath, newPath string) bool {
	s.pendingContinuationMu.Lock()
	defer s.pendingContinuationMu.Unlock()
	for path := range s.pendingContinuationPaths {
		if path == oldPath || path == newPath || strings.HasPrefix(path, oldPath+"/") || strings.HasPrefix(path, newPath+"/") {
			return true
		}
	}
	return false
}

func (s *SpoolStore) retainPendingBoundContinuation(id int64, path, file string, f *os.File) {
	s.pendingContinuationMu.Lock()
	if s.pendingContinuations == nil {
		s.pendingContinuations = make(map[int64]pendingBoundContinuation)
	}
	if s.pendingContinuationPaths == nil {
		s.pendingContinuationPaths = make(map[string]int64)
	}
	// The path shard prevents a second unresolved transition for this row.
	s.pendingContinuations[id] = pendingBoundContinuation{path: path, file: file, fd: f}
	s.pendingContinuationPaths[path] = id
	s.pendingContinuationMu.Unlock()
}
