package nfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// privateBirthIntentDir is deliberately outside files/: a birth whose SQL
// transaction never began must still be discovered before boot's orphan scan.
const privateBirthIntentDir = "birth-intents"
const maxPrivateBirthIntent = 8192

var ErrPendingPrivateBirth = errors.New("private birth requires reconciliation")

// PrivateBirthCustody owns the seed and capacity reservation until a later,
// exact-identity reconciliation retires it. Close releases FDs only; it never
// removes the image/intent or releases capacity. No writer is published here.
type PrivateBirthCustody struct {
	mu                                            sync.Mutex
	store                                         *SpoolStore
	root, files, intents, image, record           *os.File
	intent                                        metadata.SpoolBirthIntent
	imageName, intentName                         string
	rootID, filesID, intentsID, imageID, recordID string
	seedSize                                      int64
	seedHash                                      [32]byte
	recordSize                                    int64
	recordHash                                    [32]byte
	prepared                                      bool
}

func birthFDIdentity(f *os.File) (string, int64, error) {
	if f == nil {
		return "", 0, ErrPendingPrivateBirth
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return "", 0, err
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), st.Size, nil
}

func birthEntryIdentity(dir *os.File, name string) (string, int64, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return "", 0, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return "", 0, ErrPendingPrivateBirth
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), st.Size, nil
}

func birthOpenDirectoryAt(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (s *SpoolStore) rememberBirth(c *PrivateBirthCustody) {
	s.birthMu.Lock()
	if s.pendingBirths == nil {
		s.pendingBirths = make(map[string]*PrivateBirthCustody)
	}
	s.pendingBirths[c.intent.BirthID] = c
	s.birthMu.Unlock()
}

func (s *SpoolStore) syncBirth(stage string, f *os.File) error {
	if s.birthSyncFailure != nil {
		if err := s.birthSyncFailure(stage); err != nil {
			return err
		}
	}
	return f.Sync()
}

// PreparePrivateBirth reserves seed bytes and writes the exact private image,
// then a bounded intent. File, intent, and their containing directories are
// synced before prepared becomes true. If a failure happens after image create,
// the returned custody and store's pending map retain ownership. The caller
// must keep the namespace unavailable and must not admit a writer on error.
func (s *SpoolStore) PreparePrivateBirth(path string, seed []byte) (c *PrivateBirthCustody, retErr error) {
	if path == "" || len(path) > 4096 || !utf8.ValidString(path) || !utf8.ValidString(s.root) || !filepath.IsAbs(s.root) || filepath.Clean(s.root) != s.root ||
		strings.HasPrefix(path, "/") || strings.HasPrefix(path, "../") || path == ".." ||
		strings.ContainsRune(path, 0) || pathpkg.Clean(path) != path || path == "." || s.closed.Load() {
		return nil, ErrPendingPrivateBirth
	}
	id, err := metadata.NewSpoolCustodyID()
	if err != nil {
		return nil, err
	}
	size := int64(len(seed))
	if size > 0 && !s.reserveCapacityOrWait(size, nil) {
		return nil, ErrSpoolFull
	}
	reserved := size > 0
	defer func() {
		if c == nil && reserved {
			s.releaseCapacity(size)
		}
	}()
	rootFD, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	root := os.NewFile(uintptr(rootFD), s.root)
	defer func() {
		if c == nil {
			root.Close()
		}
	}()
	files, err := birthOpenDirectoryAt(root, SpoolFilesSubdir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if c == nil {
			files.Close()
		}
	}()
	if err := unix.Mkdirat(int(root.Fd()), privateBirthIntentDir, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return nil, err
	}
	intents, err := birthOpenDirectoryAt(root, privateBirthIntentDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if c == nil {
			intents.Close()
		}
	}()
	if err := s.syncBirth("root", root); err != nil {
		return nil, err
	}
	imageName, intentName := "birth-"+id, id+".json"
	fd, err := unix.Openat(int(files.Fd()), imageName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	image := os.NewFile(uintptr(fd), imageName)
	imagePath := filepath.Join(s.root, SpoolFilesSubdir, imageName)
	// Ownership starts at the successful create, before any further fallible
	// identity lookup. Even a failed Fstat must leave a discoverable source.
	c = &PrivateBirthCustody{store: s, root: root, files: files, intents: intents, image: image,
		intent:    metadata.SpoolBirthIntent{BirthID: id, Path: path, SpoolFile: imagePath},
		imageName: imageName, intentName: intentName, seedSize: size, seedHash: sha256.Sum256(seed)}
	s.rememberBirth(c)
	rootID, _, err := birthFDIdentity(root)
	if err != nil {
		return c, err
	}
	filesID, _, err := birthFDIdentity(files)
	if err != nil {
		return c, err
	}
	intentsID, _, err := birthFDIdentity(intents)
	if err != nil {
		return c, err
	}
	imageID, _, err := birthFDIdentity(image)
	if err != nil {
		return c, err
	}
	c.intent.FileIdentity = imageID
	c.rootID, c.filesID, c.intentsID, c.imageID = rootID, filesID, intentsID, imageID
	if _, err := image.Write(seed); err != nil {
		return c, err
	}
	if err := s.syncBirth("image", image); err != nil {
		return c, err
	}
	recordBytes, err := json.Marshal(struct {
		Version         int                       `json:"version"`
		Intent          metadata.SpoolBirthIntent `json:"intent"`
		RootIdentity    string                    `json:"root_identity"`
		FilesIdentity   string                    `json:"files_identity"`
		IntentsIdentity string                    `json:"intents_identity"`
		Size            int64                     `json:"size"`
		SHA256          [32]byte                  `json:"sha256"`
	}{1, c.intent, rootID, filesID, intentsID, size, c.seedHash})
	if err != nil {
		return c, err
	}
	if len(recordBytes) > maxPrivateBirthIntent {
		return c, ErrPendingPrivateBirth
	}
	c.recordSize, c.recordHash = int64(len(recordBytes)), sha256.Sum256(recordBytes)
	recordFD, err := unix.Openat(int(intents.Fd()), intentName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return c, err
	}
	c.record = os.NewFile(uintptr(recordFD), intentName)
	c.recordID, _, err = birthFDIdentity(c.record)
	if err != nil {
		return c, err
	}
	if _, err := c.record.Write(recordBytes); err != nil {
		return c, err
	}
	if err := s.syncBirth("intent", c.record); err != nil {
		return c, err
	}
	if err := s.syncBirth("files-directory", files); err != nil {
		return c, err
	}
	if err := s.syncBirth("intent-directory", intents); err != nil {
		return c, err
	}
	if err := c.verifyLocked(); err != nil {
		return c, err
	}
	c.prepared = true
	return c, nil
}

// Intent is the immutable caller-known discriminator for an actual COMMIT/no
// return. An absent SQL binding never authorizes removal of this private source.
func (c *PrivateBirthCustody) Intent() metadata.SpoolBirthIntent { return c.intent }

func (c *PrivateBirthCustody) verifyLocked() error {
	if c.root == nil || c.files == nil || c.intents == nil || c.image == nil || c.record == nil {
		return ErrPendingPrivateBirth
	}
	for _, check := range []struct {
		f  *os.File
		id string
	}{{c.root, c.rootID}, {c.files, c.filesID}, {c.intents, c.intentsID}} {
		id, _, err := birthFDIdentity(check.f)
		if err != nil || id != check.id {
			return ErrPendingPrivateBirth
		}
	}
	rootFD, err := unix.Open(c.store.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrPendingPrivateBirth
	}
	currentRoot := os.NewFile(uintptr(rootFD), "root-check")
	defer currentRoot.Close()
	id, _, err := birthFDIdentity(currentRoot)
	if err != nil || id != c.rootID {
		return ErrPendingPrivateBirth
	}
	for _, item := range []struct {
		parent   *os.File
		name, id string
	}{{c.root, SpoolFilesSubdir, c.filesID}, {c.root, privateBirthIntentDir, c.intentsID}} {
		child, err := birthOpenDirectoryAt(item.parent, item.name)
		if err != nil {
			return ErrPendingPrivateBirth
		}
		id, _, err := birthFDIdentity(child)
		child.Close()
		if err != nil || id != item.id {
			return ErrPendingPrivateBirth
		}
	}
	for _, item := range []struct {
		parent, f *os.File
		name, id  string
		size      int64
	}{{c.files, c.image, c.imageName, c.imageID, c.seedSize}, {c.intents, c.record, c.intentName, c.recordID, c.recordSize}} {
		id, size, err := birthEntryIdentity(item.parent, item.name)
		if err != nil || id != item.id || (item.size >= 0 && size != item.size) {
			return ErrPendingPrivateBirth
		}
		fdID, fdSize, err := birthFDIdentity(item.f)
		if err != nil || fdID != item.id || (item.size >= 0 && fdSize != item.size) {
			return ErrPendingPrivateBirth
		}
	}
	if c.seedSize > 0 {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(c.image, 0, c.seedSize)); err != nil || !bytes.Equal(h.Sum(nil), c.seedHash[:]) {
			return ErrPendingPrivateBirth
		}
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(c.record, 0, c.recordSize)); err != nil || !bytes.Equal(h.Sum(nil), c.recordHash[:]) {
		return ErrPendingPrivateBirth
	}
	return nil
}

// CommitPrivateBirth calls the existing atomic metadata birth only after
// durable preparation and exact FD/directory checks. A failed or ambiguous
// result retains the custody and reservation for later reconciliation.
func (c *PrivateBirthCustody) CommitPrivateBirth(store *metadata.Store, expected metadata.MirrorToken, entry *metadata.Entry) (metadata.SpoolBirthReceipt, *metadata.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if store == nil || entry == nil || entry.Path != c.intent.Path || entry.Size != c.seedSize || !c.prepared {
		return metadata.SpoolBirthReceipt{}, nil, ErrPendingPrivateBirth
	}
	if err := c.verifyLocked(); err != nil {
		return metadata.SpoolBirthReceipt{}, nil, err
	}
	r, born, err := store.InsertBoundSpoolBirth(expected, entry, c.intent.SpoolFile, c.intent.BirthID)
	if verifyErr := c.verifyLocked(); verifyErr != nil {
		return r, nil, errors.Join(err, verifyErr, ErrPendingPrivateBirth)
	}
	if r.BirthID != c.intent.BirthID || r.Path != c.intent.Path || r.SpoolFile != c.intent.SpoolFile || r.FileIdentity != c.intent.FileIdentity {
		return r, nil, errors.Join(err, ErrPendingPrivateBirth)
	}
	if err == nil && (r.Outcome != metadata.SpoolBirthCommitted || born == nil || born.Path != c.intent.Path ||
		born.Size != c.seedSize || born.Version.Incarnation != r.Incarnation || born.Version.Revision != r.Revision) {
		return r, nil, ErrPendingPrivateBirth
	}
	return r, born, err
}

// Close only releases process-local descriptors. The durable image, intent,
// and reserved bytes remain owned by the store's pending cohort; boot refuses
// to scrub them until a future exact settlement protocol is implemented.
func (c *PrivateBirthCustody) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, f := range []*os.File{c.record, c.image, c.intents, c.files, c.root} {
		if f != nil {
			errs = append(errs, f.Close())
		}
	}
	c.record, c.image, c.intents, c.files, c.root = nil, nil, nil, nil, nil
	return errors.Join(errs...)
}

// checkPrivateBirthIntentRecovery runs before any recovery capacity reset or
// SQL-driven file cleanup. Every discoverable marker, including a partial or
// malformed one, is an unresolved birth; no negative SQL lookup settles it.
func (s *SpoolStore) checkPrivateBirthIntentRecovery() error {
	s.birthMu.Lock()
	pending := len(s.pendingBirths)
	s.birthMu.Unlock()
	if pending != 0 {
		return fmt.Errorf("%d process-owned private birth(s): %w", pending, ErrPendingPrivateBirth)
	}
	rootFD, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("birth root scan: %w", err)
	}
	root := os.NewFile(uintptr(rootFD), s.root)
	defer root.Close()
	intents, err := birthOpenDirectoryAt(root, privateBirthIntentDir)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("birth intent directory changed: %w: %w", err, ErrPendingPrivateBirth)
	}
	defer intents.Close()
	entries, err := intents.ReadDir(1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("birth intent scan: %w", err)
	}
	if len(entries) != 1 {
		return ErrPendingPrivateBirth
	}
	return fmt.Errorf("birth intent %q preserved: %w", entries[0].Name(), ErrPendingPrivateBirth)
}
