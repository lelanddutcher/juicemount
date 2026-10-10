package cache

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

var ErrResidentDigest = errors.New("resident image digest mismatch")

// Bound memory independently of file length. At the usual 4 MiB block size
// this permits four TiB per image; larger manifests retain their private copy.
const maxResidentFlushBlocks = 1 << 20

type residentDiskIdentity struct {
	dev, inode   uint64
	mode         uint32
	size         int64
	mtime, ctime unix.Timespec
}

func residentDiskID(f *os.File, directory bool) (residentDiskIdentity, error) {
	var s unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &s); err != nil {
		return residentDiskIdentity{}, err
	}
	id := residentDiskIdentity{dev: uint64(s.Dev), inode: s.Ino, mode: uint32(s.Mode)}
	if directory {
		if s.Mode&unix.S_IFMT != unix.S_IFDIR {
			return id, ErrCacheMiss
		}
	} else {
		if s.Mode&unix.S_IFMT != unix.S_IFREG || s.Nlink != 1 {
			return id, ErrCacheMiss
		}
		id.size, id.mtime, id.ctime = s.Size, s.Mtim, s.Ctim
	}
	return id, nil
}

// walkResident opens only existing no-follow components below the retained
// volume-container FD. Intermediate descriptors close as the walk advances.
func walkResident(root *os.File, relative string, directory, remember bool, dirs map[string]residentDiskIdentity) (_ *os.File, retErr error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return nil, ErrCacheMiss
	}
	parts := strings.Split(relative, string(filepath.Separator))
	parent := root
	defer func() {
		if parent != root {
			_ = parent.Close()
		}
	}()
	prefix := ""
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, ErrCacheMiss
		}
		prefix = filepath.Join(prefix, part)
		isDir := i < len(parts)-1 || directory
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
		if isDir {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(int(parent.Fd()), part, flags, 0)
		if err != nil {
			return nil, err
		}
		child := os.NewFile(uintptr(fd), prefix)
		id, err := residentDiskID(child, isDir)
		if err == nil && isDir {
			old, known := dirs[prefix]
			if known && old != id || !known && !remember {
				err = ErrCacheMiss
			} else if !known {
				dirs[prefix] = id
			}
		}
		if err != nil {
			_ = child.Close()
			return nil, err
		}
		if i == len(parts)-1 {
			return child, nil
		}
		if parent != root {
			_ = parent.Close()
		}
		parent = child
	}
	return nil, ErrCacheMiss
}

func (r *Reader) syncResident(f *os.File, directory bool) error {
	if r.testSyncResident != nil {
		return r.testSyncResident(f, directory)
	}
	return f.Sync()
}

// FlushResidentImage verifies and syncs an already-resident complete image and
// its mappings without network access. It is an owned background handoff, never
// a foreground read. Cache eviction remains legal after this snapshot; callers
// still qualify namespace/binding and retain their private source on failure.
//
// The existing directory containing volume UUIDs is the durable root boundary.
// New shard/chunks/raw/volume entries are synced bottom-up. Every data CRC/hash
// and sync uses the same descriptor; a final no-follow walk rejects substituted
// names, parents or content metadata. Only a bounded number of FDs stay open.
func (r *Reader) FlushResidentImage(ctx context.Context, inode uint64, expected ResidentAttr, want [sha256.Size]byte) (_ ContentGraphExport, retErr error) {
	var empty ContentGraphExport
	if r == nil || r.stopped.Load() || !r.binding.valid() || r.blockSize != r.binding.BlockSize {
		return empty, ErrCacheMiss
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	r.mappingWriteMu.Lock()
	defer r.mappingWriteMu.Unlock()
	if r.mappingPath == "" || r.stopped.Load() {
		return empty, ErrCacheMiss
	}
	graph, err := r.ExportContentGraph(inode, expected)
	if err != nil {
		return empty, err
	}
	eof, err := r.ResidentEOF(inode, expected)
	if err != nil {
		return empty, err
	}
	current := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !graph.Token.Valid() || !eof.Valid() {
			return ErrCacheMiss
		}
		return nil
	}
	// Empty images need an actual EOF observation and durable mappings, but
	// no block or directory witness can be invented for their absent data.
	if expected.Size == 0 {
		if want != sha256.Sum256(nil) {
			return empty, ErrResidentDigest
		}
		if err := current(); err != nil {
			return empty, err
		}
		if err := r.writeMappingSnapshot(true); err != nil {
			return empty, err
		}
		if err := current(); err != nil {
			return empty, err
		}
		return graph, nil
	}
	chunks := filepath.Clean(r.cacheDir)
	raw := filepath.Dir(chunks)
	volume := filepath.Dir(raw)
	container := filepath.Dir(volume)
	if !filepath.IsAbs(chunks) || filepath.Base(chunks) != "chunks" || filepath.Base(raw) != "raw" || filepath.Base(volume) != r.binding.UUID {
		return empty, ErrCacheMiss
	}
	fd, err := unix.Open(container, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return empty, err
	}
	root := os.NewFile(uintptr(fd), container)
	defer root.Close()
	rootID, err := residentDiskID(root, true)
	if err != nil {
		return empty, err
	}
	dirs := map[string]residentDiskIdentity{"": rootID}
	base := filepath.Join(r.binding.UUID, "raw", "chunks")
	anchor, err := walkResident(root, base, true, true, dirs)
	if err != nil {
		return empty, err
	}
	if err = anchor.Close(); err != nil {
		return empty, err
	}
	files := make(map[string]residentDiskIdentity)
	digest := sha256.New()
	buffer := r.getBuffer(int(r.blockSize))
	defer r.putBuffer(buffer)
	for off := int64(0); off < expected.Size; {
		if err := current(); err != nil {
			return empty, err
		}
		offset := off % ChunkSize
		extent, ok := sliceAt(graph.graph.Chunks[off/ChunkSize].Extents, offset)
		if !ok || extent.SliceID == 0 {
			return empty, ErrCacheMiss
		}
		inSlice := int64(extent.Off) + offset - int64(extent.Pos)
		block := inSlice / r.blockSize
		inBlock := inSlice % r.blockSize
		length, ok := r.blockLength(extent, block)
		if !ok || inBlock >= length {
			return empty, ErrCacheMiss
		}
		amount := min(expected.Size-off, int64(extent.Pos)+int64(extent.Len)-offset, length-inBlock, ChunkSize-offset)
		if amount <= 0 {
			return empty, ErrCacheMiss
		}
		names := []string{r.blockPath(extent.SliceID, block, length), r.hashBlockPath(extent.SliceID, block, length)}
		var file *os.File
		var name string
		for _, candidate := range names {
			name, err = filepath.Rel(container, candidate)
			if err != nil {
				return empty, err
			}
			file, err = walkResident(root, name, false, true, dirs)
			if err == nil {
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				return empty, err
			}
		}
		if err != nil {
			return empty, err
		}
		before, verifyErr := residentDiskID(file, false)
		if prior, known := files[name]; verifyErr == nil && known && before != prior {
			verifyErr = ErrCacheMiss
		}
		if verifyErr == nil {
			verifyErr = r.readVerifiedBlockFD(file, length, 0, buffer[:length])
		}
		if verifyErr == nil {
			_, verifyErr = digest.Write(buffer[inBlock : inBlock+amount])
		}
		if verifyErr == nil {
			verifyErr = current()
		}
		if verifyErr == nil {
			if _, synced := files[name]; !synced {
				verifyErr = r.syncResident(file, false)
			}
		}
		after, statErr := residentDiskID(file, false)
		closeErr := file.Close()
		if verifyErr != nil || statErr != nil || closeErr != nil || before != after {
			return empty, errors.Join(ErrCacheMiss, verifyErr, statErr, closeErr)
		}
		files[name] = before
		if len(files) > maxResidentFlushBlocks || len(dirs) > 2*maxResidentFlushBlocks+4 {
			return empty, ErrCacheMiss
		}
		off += amount
	}
	var got [sha256.Size]byte
	copy(got[:], digest.Sum(nil))
	if got != want {
		return empty, ErrResidentDigest
	}
	paths := make([]string, 0, len(dirs))
	for name := range dirs {
		paths = append(paths, name)
	}
	sort.Slice(paths, func(i, j int) bool {
		a, b := strings.Count(paths[i], "/"), strings.Count(paths[j], "/")
		if a != b {
			return a > b
		}
		return paths[i] > paths[j]
	})
	for _, name := range paths {
		if err := current(); err != nil {
			return empty, err
		}
		directory := root
		if name != "" {
			directory, err = walkResident(root, name, true, false, dirs)
			if err != nil {
				return empty, err
			}
		}
		syncErr := r.syncResident(directory, true)
		after, statErr := residentDiskID(directory, true)
		var closeErr error
		if directory != root {
			closeErr = directory.Close()
		}
		if syncErr != nil || statErr != nil || closeErr != nil || after != dirs[name] {
			return empty, errors.Join(ErrCacheMiss, syncErr, statErr, closeErr)
		}
	}
	verifyNames := func() error {
		named, err := os.Lstat(container)
		if err != nil || !named.IsDir() {
			return ErrCacheMiss
		}
		info, err := root.Stat()
		if err != nil || !os.SameFile(named, info) {
			return ErrCacheMiss
		}
		for name, before := range files {
			if err := current(); err != nil {
				return err
			}
			f, err := walkResident(root, name, false, false, dirs)
			if err != nil {
				return err
			}
			after, statErr := residentDiskID(f, false)
			closeErr := f.Close()
			if statErr != nil || closeErr != nil || after != before {
				return errors.Join(ErrCacheMiss, statErr, closeErr)
			}
		}
		// Recheck the selected volume chain after all named blocks.
		f, err := walkResident(root, base, true, false, dirs)
		if err != nil {
			return err
		}
		return f.Close()
	}
	if err := verifyNames(); err != nil {
		return empty, err
	}
	if err := current(); err != nil {
		return empty, err
	}
	if err := r.writeMappingSnapshot(true); err != nil {
		return empty, fmt.Errorf("resident mapping flush: %w", err)
	}
	if err := verifyNames(); err != nil {
		return empty, err
	}
	if err := current(); err != nil {
		return empty, err
	}
	return graph, nil
}
