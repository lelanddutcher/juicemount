package health

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
	"golang.org/x/sys/unix"
)

// GenerationReservation is a durable, never-reused mount namespace and its
// process-local custody lock. Only an absent mount and a settled exact child
// allow retirement. This is not authority to detach a present mount.
type GenerationReservation struct {
	Path               string
	LogicalPath        string
	ID                 string
	bootID             string
	identities         []generationIdentity
	rootPath           string
	root, lock, ledger *os.File
	spent              bool
	retired            bool
	journalFailed      bool
}

type generationAnchor struct {
	LogicalPath string             `json:"logical_path"`
	Root        generationIdentity `json:"root"`
}

func checkGenerationAnchor(parent, root *os.File, name, logical, rootPath string, created bool) error {
	actual, err := generationFileIdentity(root, rootPath)
	if err != nil {
		return err
	}
	flags := os.O_RDONLY
	if created {
		flags = os.O_RDWR | os.O_CREATE | os.O_EXCL
	}
	f, err := privatefile.OpenAt(parent, name+".anchor", flags, 0600)
	if err != nil {
		return fmt.Errorf("generation anchor unavailable: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		return errors.New("generation anchor is not private")
	}
	if created {
		raw, err := json.Marshal(generationAnchor{LogicalPath: logical, Root: actual})
		if err != nil {
			return err
		}
		if n, err := f.Write(append(raw, '\n')); err != nil {
			return err
		} else if n != len(raw)+1 {
			return io.ErrShortWrite
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return parent.Sync()
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > 4096 || raw[len(raw)-1] != '\n' {
		return errors.New("generation anchor invalid")
	}
	var saved generationAnchor
	if err := json.Unmarshal(raw, &saved); err != nil {
		return err
	}
	if saved.LogicalPath != logical || saved.Root != actual {
		return errors.New("generation root moved or replaced")
	}
	return nil
}

type generationIdentity struct {
	Path   string `json:"path"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

type generationEvent struct {
	Version         int                  `json:"version"`
	State           string               `json:"state"`
	ID              string               `json:"id"`
	Nonce           string               `json:"nonce,omitempty"`
	LogicalPath     string               `json:"logical_path,omitempty"`
	Path            string               `json:"path,omitempty"`
	BootID          string               `json:"boot_id,omitempty"`
	ConfigDigest    string               `json:"config_digest,omitempty"`
	Parents         []generationIdentity `json:"parents,omitempty"`
	ChildPID        int                  `json:"child_pid,omitempty"`
	ParentPID       int                  `json:"parent_pid,omitempty"`
	VolumeUUID      string               `json:"volume_uuid,omitempty"`
	CustodyEndpoint string               `json:"custody_endpoint,omitempty"`
	AtUnixNano      int64                `json:"at_unix_nano"`
}

func generationConfigDigest(cfg FUSEConfig) string {
	// Hash only values that select the child/backend. Never journal credentials.
	raw, _ := json.Marshal(cfg)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func generationPathValid(path string) bool {
	return path != "" && path != "/" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

// Inventory rows may legitimately include the filesystem root. A generation
// mount argument may not be root; these are different validation domains.
func generationInventoryPathValid(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

// openGenerationAncestors walks from / with O_NOFOLLOW. Paths are checked
// again after allocation; moving an ancestor by an external actor remains a
// cooperative-namespace boundary, not a kernel guarantee.
func openGenerationAncestors(path string) (*os.File, []generationIdentity, error) {
	if !generationPathValid(path) {
		return nil, nil, errors.New("invalid generation parent path")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	var ids []generationIdentity
	var rootStat unix.Stat_t
	if e := unix.Fstat(fd, &rootStat); e != nil {
		unix.Close(fd)
		return nil, nil, e
	}
	ids = append(ids, generationIdentity{"/", uint64(rootStat.Dev), rootStat.Ino})
	current := "/"
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			unix.Close(fd)
			return nil, nil, errors.New("invalid generation ancestor")
		}
		next, e := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if e != nil {
			return nil, nil, fmt.Errorf("generation ancestor %q: %w", component, e)
		}
		fd = next
		current = filepath.Join(current, component)
		var st unix.Stat_t
		if e = unix.Fstat(fd, &st); e != nil {
			unix.Close(fd)
			return nil, nil, e
		}
		ids = append(ids, generationIdentity{current, uint64(st.Dev), st.Ino})
	}
	return os.NewFile(uintptr(fd), path), ids, nil
}

func generationFileIdentity(f *os.File, path string) (generationIdentity, error) {
	st, err := f.Stat()
	if err != nil {
		return generationIdentity{}, err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() {
		return generationIdentity{}, errors.New("generation directory identity unavailable")
	}
	return generationIdentity{Path: path, Device: uint64(sys.Dev), Inode: sys.Ino}, nil
}

func generationPrivateDir(f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.IsDir() || int(sys.Uid) != os.Geteuid() || st.Mode().Perm()&0077 != 0 {
		return errors.New("generation root must be a private directory owned by this user")
	}
	return nil
}

func completeGenerationSnapshot(s mounttable.Snapshot, logical, root string) error {
	if s.Version != 1 || s.BootID == "" || s.Entries == nil {
		return errors.New("generation mount observation incomplete")
	}
	for _, e := range s.Entries {
		if !generationInventoryPathValid(e.Path) || e.Source == "" || e.Type == "" {
			return errors.New("generation mount entry incomplete")
		}
		p := strings.ToLower(e.Path)
		for _, target := range []string{logical, root} {
			t := strings.ToLower(target)
			if p == t || strings.HasPrefix(p, t+string(os.PathSeparator)) {
				return fmt.Errorf("generation namespace occupied at %s", e.Path)
			}
		}
	}
	return nil
}

func generationEvents(f *os.File) ([]generationEvent, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > 4<<20 || raw[len(raw)-1] != '\n' {
		return nil, errors.New("generation ledger missing, oversized, or torn")
	}
	var events []generationEvent
	scan := bufio.NewScanner(strings.NewReader(string(raw)))
	for scan.Scan() {
		var e generationEvent
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil || e.Version != 1 {
			return nil, errors.New("generation ledger invalid")
		}
		events = append(events, e)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 || events[0].State != "install" || len(events[0].Nonce) != 32 {
		return nil, errors.New("generation installation identity missing")
	}
	return events, nil
}

func generationNames(root *os.File) ([]string, error) {
	fd, err := unix.Openat(int(root.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	copy := os.NewFile(uintptr(fd), "generation-root-scan")
	defer copy.Close()
	return copy.Readdirnames(-1)
}

func (r *GenerationReservation) append(e generationEvent) error {
	if r.journalFailed {
		return errors.New("generation ledger write previously failed")
	}
	e.Version = 1
	e.ID = r.ID
	e.AtUnixNano = time.Now().UnixNano()
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if n, err := r.ledger.Write(raw); err != nil {
		r.journalFailed = true
		return err
	} else if n != len(raw) {
		r.journalFailed = true
		return io.ErrShortWrite
	}
	if err := r.ledger.Sync(); err != nil {
		r.journalFailed = true
		return err
	}
	return nil
}

func (r *GenerationReservation) close() {
	if r.ledger != nil {
		_ = r.ledger.Close()
		r.ledger = nil
	}
	if r.lock != nil {
		_ = r.lock.Close()
		r.lock = nil
	}
	if r.root != nil {
		_ = r.root.Close()
		r.root = nil
	}
}

func ensureGenerationParent(path string) error {
	if _, err := os.Lstat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	grand, _, err := openGenerationAncestors(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer grand.Close()
	if err := unix.Mkdirat(int(grand.Fd()), filepath.Base(path), 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return grand.Sync()
}

// reserveFUSEGeneration holds the installation lock for this manager's entire
// uncertain/present lifetime. A prior unretired intent blocks a second manager.
func reserveFUSEGeneration(logical string, digest string, observe func(context.Context) (mounttable.Snapshot, error)) (_ *GenerationReservation, err error) {
	if !generationPathValid(logical) || filepath.Base(logical) == "." {
		return nil, errors.New("invalid requested FUSE path")
	}
	if observe == nil {
		return nil, errors.New("generation observer unavailable")
	}
	rootPath := logical + ".generations"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	snapshot, err := observe(ctx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("generation mount observation: %w", err)
	}
	if err := completeGenerationSnapshot(snapshot, logical, rootPath); err != nil {
		return nil, err
	}
	if err := ensureGenerationParent(filepath.Dir(logical)); err != nil {
		return nil, err
	}
	parent, parentIDs, err := openGenerationAncestors(filepath.Dir(logical))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	rootName := filepath.Base(rootPath)
	created := false
	if e := unix.Mkdirat(int(parent.Fd()), rootName, 0700); e != nil {
		if !errors.Is(e, unix.EEXIST) {
			return nil, e
		}
	} else {
		created = true
	}
	rootFD, err := unix.Openat(int(parent.Fd()), rootName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	r := &GenerationReservation{LogicalPath: logical, rootPath: rootPath, bootID: snapshot.BootID, root: os.NewFile(uintptr(rootFD), rootPath)}
	defer func() {
		if err != nil {
			r.close()
		}
	}()
	if err = generationPrivateDir(r.root); err != nil {
		return nil, err
	}
	if err = checkGenerationAnchor(parent, r.root, rootName, logical, rootPath, created); err != nil {
		return nil, err
	}
	if err = parent.Sync(); err != nil {
		return nil, err
	}
	r.lock, err = privatefile.OpenAt(r.root, "ledger.lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	lockInfo, lockErr := r.lock.Stat()
	if lockErr != nil {
		return nil, lockErr
	}
	lockStat, lockOK := lockInfo.Sys().(*syscall.Stat_t)
	if !lockOK || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0077 != 0 || lockStat.Nlink != 1 || int(lockStat.Uid) != os.Geteuid() {
		return nil, errors.New("generation lock is not a private regular file")
	}
	if err = unix.Flock(int(r.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("generation lifecycle already owned: %w", err)
	}
	r.ledger, err = privatefile.OpenAt(r.root, "ledger.jsonl", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	info, err := r.ledger.Stat()
	if err != nil {
		return nil, err
	}
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || int(sys.Uid) != os.Geteuid() || info.Mode().Perm()&0077 != 0 || sys.Nlink != 1 {
		return nil, errors.New("generation ledger is not a private regular file")
	}
	if info.Size() == 0 {
		entries, e := generationNames(r.root)
		if e != nil {
			return nil, e
		}
		for _, name := range entries {
			if name != "ledger.lock" && name != "ledger.jsonl" {
				return nil, errors.New("generation ledger missing while namespace exists")
			}
		}
		nonce := make([]byte, 16)
		if _, e := rand.Read(nonce); e != nil {
			return nil, e
		}
		if e := r.append(generationEvent{State: "install", Nonce: hex.EncodeToString(nonce), LogicalPath: logical}); e != nil {
			return nil, e
		}
		if e := r.root.Sync(); e != nil {
			return nil, e
		}
	}
	events, err := generationEvents(r.ledger)
	if err != nil {
		return nil, err
	}
	nonce := events[0].Nonce
	if decoded, decodeErr := hex.DecodeString(nonce); decodeErr != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != nonce {
		return nil, errors.New("invalid installation nonce")
	}
	if events[0].LogicalPath != logical {
		return nil, errors.New("generation ledger belongs to another logical path")
	}
	var highest uint64
	states := map[string]string{}
	for _, e := range events[1:] {
		if !strings.HasPrefix(e.ID, nonce+"-") || len(e.ID) != len(nonce)+17 {
			return nil, errors.New("generation ledger ID mismatch")
		}
		var n uint64
		if _, scanErr := fmt.Sscanf(strings.TrimPrefix(e.ID, nonce+"-"), "%016x", &n); scanErr != nil || n == 0 || fmt.Sprintf("%s-%016x", nonce, n) != e.ID {
			return nil, errors.New("generation ledger sequence invalid")
		}
		prior := states[e.ID]
		valid := (prior == "" && e.State == "intent") ||
			(prior == "intent" && e.State == "ready") ||
			(prior == "ready" && (e.State == "launching" || e.State == "retired")) ||
			(prior == "launching" && (e.State == "born" || e.State == "retired")) ||
			(prior == "born" && (e.State == "custody" || e.State == "retired")) ||
			(prior == "custody" && e.State == "retired")
		if !valid {
			return nil, errors.New("generation ledger state transition invalid")
		}
		if n > highest {
			highest = n
		}
		states[e.ID] = e.State
	}
	entries, err := generationNames(r.root)
	if err != nil {
		return nil, err
	}
	for _, name := range entries {
		if name != "ledger.lock" && name != "ledger.jsonl" && name != nonce {
			return nil, errors.New("untracked generation namespace entry")
		}
	}
	if e := unix.Mkdirat(int(r.root.Fd()), nonce, 0700); e != nil {
		if !errors.Is(e, unix.EEXIST) {
			return nil, e
		}
		if len(states) == 0 && len(events) == 1 {
			return nil, errors.New("pre-existing installation directory without intent")
		}
	} else if len(states) != 0 {
		return nil, errors.New("installation directory missing despite earlier generations")
	}
	nonceFD, err := unix.Openat(int(r.root.Fd()), nonce, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	install := os.NewFile(uintptr(nonceFD), nonce)
	defer install.Close()
	if err := generationPrivateDir(install); err != nil {
		return nil, err
	}
	entries, err = generationNames(install)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, name := range entries {
		id := nonce + "-" + name
		if states[id] == "" {
			return nil, errors.New("untracked generation directory")
		}
		seen[id] = true
	}
	for id, state := range states {
		if !seen[id] {
			return nil, errors.New("generation tombstone directory missing")
		}
		if state != "retired" {
			// H-3 L5: a prior owner died holding this generation. Its helper
			// receipt is the only identity-bound way to finish it; without a
			// RETIRED proof the new child stays refused, as before.
			og, ok := readObservedSidecar(install, nonce, id)
			if !ok || og.boot != r.bootID {
				return nil, errors.New("prior generation unresolved; new child refused")
			}
			// An explicit recovery's fsynced attempt can have an unknown
			// helper result. Ordinary reservation must not replay it.
			attempted, attemptErr := retainedRecoveryAttemptPresent(install, id)
			if attemptErr != nil || attempted {
				return nil, errors.New("prior generation explicit recovery unresolved; new child refused")
			}
			if err := r.resolvePriorWithHelper(id, og, nonce, observe); err != nil {
				return nil, fmt.Errorf("prior generation unresolved; new child refused: %w", err)
			}
			states[id] = "retired"
		}
	}
	for _, event := range events[1:] {
		if event.State != "ready" {
			continue
		}
		fd, actual, inspectErr := openGenerationAncestors(event.Path)
		if inspectErr != nil {
			return nil, fmt.Errorf("generation tombstone path unavailable: %w", inspectErr)
		}
		fd.Close()
		if len(actual) != len(event.Parents) {
			return nil, errors.New("generation tombstone ancestry changed")
		}
		for i := range actual {
			if actual[i] != event.Parents[i] {
				return nil, errors.New("generation tombstone ancestry changed")
			}
		}
	}
	if highest == ^uint64(0) {
		return nil, errors.New("generation sequence exhausted")
	}
	r.ID = fmt.Sprintf("%s-%016x", nonce, highest+1)
	dirName := fmt.Sprintf("%016x", highest+1)
	r.Path = filepath.Join(rootPath, nonce, dirName, "mount")
	// Intent precedes any namespace creation. A crash leaves this ID spent.
	if err = r.append(generationEvent{State: "intent", Path: r.Path, BootID: r.bootID, ConfigDigest: digest, Parents: parentIDs}); err != nil {
		return nil, err
	}
	if err = unix.Mkdirat(int(install.Fd()), dirName, 0700); err != nil {
		return nil, err
	}
	genFD, err := unix.Openat(int(install.Fd()), dirName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	gen := os.NewFile(uintptr(genFD), dirName)
	defer gen.Close()
	if err = unix.Mkdirat(int(gen.Fd()), "mount", 0700); err != nil {
		return nil, err
	}
	mountFD, err := unix.Openat(int(gen.Fd()), "mount", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	mount := os.NewFile(uintptr(mountFD), "mount")
	if err = generationPrivateDir(mount); err != nil {
		mount.Close()
		return nil, err
	}
	identities := append([]generationIdentity(nil), parentIDs...)
	for _, item := range []struct {
		file *os.File
		path string
	}{
		{r.root, rootPath}, {install, filepath.Join(rootPath, nonce)}, {gen, filepath.Join(rootPath, nonce, dirName)}, {mount, r.Path},
	} {
		identity, identityErr := generationFileIdentity(item.file, item.path)
		if identityErr != nil {
			mount.Close()
			return nil, identityErr
		}
		identities = append(identities, identity)
	}
	mount.Close()
	if err = gen.Sync(); err != nil {
		return nil, err
	}
	if err = install.Sync(); err != nil {
		return nil, err
	}
	if err = r.root.Sync(); err != nil {
		return nil, err
	}
	// An ancestry switch is an error after intent; the reservation stays spent.
	check, ids, e := openGenerationAncestors(filepath.Dir(logical))
	if e != nil {
		return nil, e
	}
	check.Close()
	if len(ids) != len(parentIDs) {
		return nil, errors.New("generation ancestry changed")
	}
	for i := range ids {
		if ids[i] != parentIDs[i] {
			return nil, errors.New("generation ancestry changed")
		}
	}
	// Reject a mount arriving while the directory was being created.
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	after, e := observe(ctx)
	cancel()
	if e != nil || after.BootID != r.bootID {
		return nil, errors.New("generation post-allocation mount observation unavailable or boot changed")
	}
	if e = completeGenerationSnapshot(after, logical, rootPath); e != nil {
		return nil, e
	}
	r.identities = identities
	if err = r.verifyAncestry(); err != nil {
		return nil, err
	}
	if err = r.append(generationEvent{State: "ready", Path: r.Path, BootID: r.bootID, Parents: identities}); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *GenerationReservation) verifyAncestry() error {
	if r == nil {
		return nil
	}
	fd, identities, err := openGenerationAncestors(r.Path)
	if err != nil {
		return err
	}
	fd.Close()
	if len(identities) != len(r.identities) {
		return errors.New("generation ancestry length changed")
	}
	for i := range identities {
		if identities[i] != r.identities[i] {
			return errors.New("generation ancestry identity changed")
		}
	}
	return nil
}

// verifyUnmountedAncestors excludes the leaf after spawn. A live macFUSE
// mount replaces its directory identity and opening it here can block on the
// data plane. The reserved leaf remains recorded for absent-mount retirement.
func (r *GenerationReservation) verifyUnmountedAncestors() error {
	if r == nil {
		return nil
	}
	fd, identities, err := openGenerationAncestors(filepath.Dir(r.Path))
	if err != nil {
		return err
	}
	fd.Close()
	if len(identities)+1 != len(r.identities) {
		return errors.New("generation ancestor length changed")
	}
	for i := range identities {
		if identities[i] != r.identities[i] {
			return errors.New("generation ancestor identity changed")
		}
	}
	return nil
}

func (r *GenerationReservation) markLaunching() error {
	if r == nil {
		return nil
	}
	if err := r.verifyAncestry(); err != nil {
		return err
	}
	r.spent = true
	return r.append(generationEvent{State: "launching", Path: r.Path, BootID: r.bootID})
}

func (r *GenerationReservation) markChild(pid int) error {
	if r == nil {
		return nil
	}
	if err := r.verifyUnmountedAncestors(); err != nil {
		return err
	}
	r.spent = true
	return r.append(generationEvent{State: "born", Path: r.Path, BootID: r.bootID, ChildPID: pid, ParentPID: os.Getpid()})
}

// observedSidecarName is written next to the generation's "mount" leaf.
// It is deliberately NOT a ledger event: the ledger grammar is unchanged,
// so an earlier 0.5 build reading this ledger after a rollback still
// accepts it (it never lists a generation directory's contents).
const observedSidecarName = "observed.json"

type observedSidecar struct {
	Version int      `json:"version"`
	ID      string   `json:"id"`
	Path    string   `json:"path"`
	BootID  string   `json:"boot_id"`
	FSID    [2]int32 `json:"fsid"`
}

// markObserved persists the kernel fsid of the generation's mount (H-3 L5).
// It runs before the custodian starts, so even a TCC-denied or pending root
// open leaves a reclaimable helper identity for a restarted owner.
func (r *GenerationReservation) markObserved(fsid [2]int32) error {
	if r == nil {
		return nil
	}
	if fsid == [2]int32{} || r.bootID == "" {
		return errors.New("incomplete FUSE observed generation identity")
	}
	raw, err := json.Marshal(observedSidecar{Version: 1, ID: r.ID, Path: r.Path, BootID: r.bootID, FSID: fsid})
	if err != nil {
		return err
	}
	dir, _, err := openGenerationAncestors(filepath.Dir(r.Path))
	if err != nil {
		return err
	}
	defer dir.Close()
	f, err := privatefile.OpenAt(dir, observedSidecarName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(raw, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return dir.Sync()
}

// readObservedSidecar reads one prior generation's observed identity from
// <install>/<gen>/observed.json. Missing, malformed or mismatched means
// "not observed": the generation then stays unresolved, as before.
func readObservedSidecar(install *os.File, nonce, id string) (observedGeneration, bool) {
	genHex := strings.TrimPrefix(id, nonce+"-")
	if genHex == id || len(genHex) != 16 {
		return observedGeneration{}, false
	}
	fd, err := unix.Openat(int(install.Fd()), genHex, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return observedGeneration{}, false
	}
	gen := os.NewFile(uintptr(fd), genHex)
	defer gen.Close()
	f, err := privatefile.OpenAt(gen, observedSidecarName, os.O_RDONLY, 0)
	if err != nil {
		return observedGeneration{}, false
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(raw) == 0 || len(raw) > 4096 {
		return observedGeneration{}, false
	}
	var sc observedSidecar
	if err := json.Unmarshal(raw, &sc); err != nil || sc.Version != 1 || sc.ID != id || sc.FSID == [2]int32{} ||
		FUSEHelperOpID(sc.Path) != "fuse-"+id {
		return observedGeneration{}, false
	}
	return observedGeneration{fsid: sc.FSID, boot: sc.BootID, path: sc.Path}, true
}

func (r *GenerationReservation) markCustody(uuid string, helperPID int) error {
	if r == nil {
		return nil
	}
	if len(uuid) != 36 || helperPID <= 1 || r.bootID == "" {
		return errors.New("incomplete FUSE custody generation identity")
	}
	return r.append(generationEvent{State: "custody", Path: r.Path, BootID: r.bootID,
		VolumeUUID: uuid, CustodyEndpoint: fmt.Sprintf("pid:%d", helperPID)})
}

func (r *GenerationReservation) retire() error {
	if r == nil || r.retired {
		return nil
	}
	if err := r.verifyAncestry(); err != nil {
		return err
	}
	if err := r.append(generationEvent{State: "retired", Path: r.Path, BootID: r.bootID}); err != nil {
		return err
	}
	r.retired = true
	r.close()
	return nil
}

type observedGeneration struct {
	fsid [2]int32
	boot string
	path string
}

// FUSEHelperOpID is the unmount helper op id of a generation mount path
// "<logical>.generations/<nonce>/<16-hex generation>/mount". It is derived,
// never stored: the path (in the ledger, and in the owner receipt a service
// reads) is the persistence. "" for any other path.
func FUSEHelperOpID(path string) string {
	nonce, gen, ok := parseGenerationMountPath(path)
	if !ok {
		return ""
	}
	return fmt.Sprintf("fuse-%s-%016x", nonce, gen)
}

func parseGenerationMountPath(path string) (string, uint64, bool) {
	if !generationPathValid(path) || filepath.Base(path) != "mount" {
		return "", 0, false
	}
	genDir := filepath.Dir(path)
	nonceDir := filepath.Dir(genDir)
	if !strings.HasSuffix(filepath.Dir(nonceDir), ".generations") {
		return "", 0, false
	}
	nonce, genHex := filepath.Base(nonceDir), filepath.Base(genDir)
	if b, err := hex.DecodeString(nonce); err != nil || len(b) != 16 || hex.EncodeToString(b) != nonce {
		return "", 0, false
	}
	var gen uint64
	if _, err := fmt.Sscanf(genHex, "%016x", &gen); err != nil || gen == 0 || fmt.Sprintf("%016x", gen) != genHex {
		return "", 0, false
	}
	return nonce, gen, true
}

// priorResolveWait bounds one prior-generation helper retirement during a
// reservation. The helper's own orphan window is 30 s.
var priorResolveWait = 45 * time.Second

// resolvePriorWithHelper finishes one same-boot generation whose owner died.
// It reclaims the dead owner's receipt (cancelling the helper's orphan timer)
// and asks for retirement. Juicefs of a dead owner is not our child, so its
// liveness is unknown, and with Unknown liveness the helper NEVER forces: it
// sends only ordinary (MNT_NOBLOCK) unmounts, retrying with backoff while the
// mount is busy. An idle orphaned mount therefore unmounts cleanly (with its
// flush); a wedged one keeps the new child refused until reboot. That costs
// availability, never data.
// Only a RETIRED outcome for exactly this op, followed by a fresh snapshot
// with the fsid and path gone, writes the tombstone. It never signals.
func (r *GenerationReservation) resolvePriorWithHelper(id string, og observedGeneration, nonce string, observe func(context.Context) (mounttable.Snapshot, error)) error {
	h := defaultUnmountHelper()
	if h == nil {
		return errors.New("unmount helper unavailable")
	}
	op := FUSEHelperOpID(og.path)
	if op == "" || op != "fuse-"+id || !strings.HasPrefix(id, nonce+"-") {
		return errors.New("prior generation path does not name its ledger ID")
	}
	ctx, cancel := context.WithTimeout(context.Background(), priorResolveWait+5*time.Second)
	defer cancel()
	lease, res, err := h.Reclaim(ctx, op)
	var out client.Outcome
	switch {
	case err == nil:
		defer lease.Close()
		if !res.Reclaimed {
			// No orphan timer was pending: another live connection still holds
			// this receipt. It is not ours to retire.
			return errors.New("prior generation receipt is still held by a live owner")
		}
		out, err = h.Retire(ctx, op, client.RetireRequest{Liveness: client.LivenessUnknown}, priorResolveWait)
		if err != nil {
			return fmt.Errorf("helper retirement of %s: %w (state %s)", op, err, out.State)
		}
	case errors.Is(err, client.ErrNoReceipt):
		out = res.Outcome // the helper's orphan sweep may already have finished it
	default:
		return fmt.Errorf("reclaim %s: %w", op, err)
	}
	if out.OpID != op || out.State != "RETIRED" {
		return fmt.Errorf("helper has no RETIRED proof for %s (state %q)", op, out.State)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	snap, err := observe(sctx)
	scancel()
	if err != nil || snap.Version != 1 || snap.BootID != r.bootID || snap.Entries == nil {
		return errors.New("post-retirement observation unavailable or boot changed")
	}
	for _, e := range snap.Entries {
		if e.FSID == og.fsid || strings.EqualFold(e.Path, og.path) || strings.HasPrefix(strings.ToLower(e.Path), strings.ToLower(og.path)+"/") {
			return errors.New("prior generation mount still present after helper retirement")
		}
	}
	saved := r.ID
	r.ID = id
	err = r.append(generationEvent{State: "retired", Path: og.path, BootID: og.boot})
	r.ID = saved
	if err != nil {
		return err
	}
	log.Printf("[fuse] prior generation %s retired by the unmount helper", id)
	return nil
}

// RetainedGenerationRecovery identifies one historical generation without
// authorizing a new owner, mount, namespace, or generation reservation.
type RetainedGenerationRecovery struct {
	Root, OwnerInstance, Build, ConfigDigest string
	LogicalPath, OpID                        string
}

// RetainedGenerationOutcome describes only this resource, not overall service
// quiescence or the original owner's Wait result.
type RetainedGenerationOutcome struct {
	State             string
	Retired           bool
	Forced            bool
	Syscalls          int
	ErrorCode         string
	HelperMayContinue bool
}

// RetainedGenerationHelper is deliberately narrower than a mount controller:
// it cannot register a new receipt, cancel a job, or authorize force.
type RetainedGenerationHelper interface {
	Status(context.Context, string) (client.Outcome, error)
	Reclaim(context.Context, string) (UnmountLease, client.ReclaimResult, error)
	Retire(context.Context, string, client.RetireRequest, time.Duration) (client.Outcome, error)
}

const recoveryAttemptName = "recovery-attempt.json"

type generationRecoveryAttempt struct {
	Version       int      `json:"version"`
	Root          string   `json:"root"`
	OwnerInstance string   `json:"owner_instance"`
	Build         string   `json:"build"`
	ConfigDigest  string   `json:"config_digest"`
	OpID          string   `json:"op_id"`
	Path          string   `json:"path"`
	BootID        string   `json:"boot_id"`
	FSID          [2]int32 `json:"fsid"`
}

func recoveryHold(code string) RetainedGenerationOutcome {
	return RetainedGenerationOutcome{State: "HOLD", ErrorCode: code, HelperMayContinue: true}
}

func recoveryAttemptHold(code string) RetainedGenerationOutcome {
	return RetainedGenerationOutcome{State: "HOLD", ErrorCode: code, HelperMayContinue: true}
}

// openRetainedGeneration opens only existing, private, anchored files. It does
// not call reserveFUSEGeneration, mkdir, create a ledger, or allocate a nonce.
func parseRecoveryGenerationID(id string) (string, uint64, bool) {
	nonce, sequence, ok := strings.Cut(id, "-")
	if !ok || len(nonce) != 32 || len(sequence) != 16 {
		return "", 0, false
	}
	if b, err := hex.DecodeString(nonce); err != nil || len(b) != 16 || hex.EncodeToString(b) != nonce {
		return "", 0, false
	}
	var n uint64
	if _, err := fmt.Sscanf(sequence, "%016x", &n); err != nil || n == 0 || fmt.Sprintf("%016x", n) != sequence {
		return "", 0, false
	}
	return nonce, n, true
}

func openRetainedGeneration(spec RetainedGenerationRecovery) (_ *GenerationReservation, _ observedGeneration, _ *os.File, err error) {
	if !generationPathValid(spec.LogicalPath) || !generationPathValid(spec.Root) ||
		spec.OwnerInstance == "" || spec.Build == "" || spec.ConfigDigest == "" ||
		!strings.HasPrefix(spec.OpID, "fuse-") {
		return nil, observedGeneration{}, nil, errors.New("invalid recovery identity")
	}
	rel, relErr := filepath.Rel(spec.Root, spec.LogicalPath)
	if relErr != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, observedGeneration{}, nil, errors.New("generation outside recovery root")
	}
	id := strings.TrimPrefix(spec.OpID, "fuse-")
	nonce, seq, ok := parseRecoveryGenerationID(id)
	if !ok {
		return nil, observedGeneration{}, nil, errors.New("invalid generation operation")
	}
	rootPath := spec.LogicalPath + ".generations"
	path := filepath.Join(rootPath, nonce, fmt.Sprintf("%016x", seq), "mount")
	if FUSEHelperOpID(path) != spec.OpID {
		return nil, observedGeneration{}, nil, errors.New("operation does not name generation")
	}
	parent, _, err := openGenerationAncestors(filepath.Dir(spec.LogicalPath))
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	defer parent.Close()
	rootName := filepath.Base(rootPath)
	fd, err := unix.Openat(int(parent.Fd()), rootName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	r := &GenerationReservation{LogicalPath: spec.LogicalPath, Path: path, ID: id, rootPath: rootPath, root: os.NewFile(uintptr(fd), rootPath)}
	defer func() {
		if err != nil {
			r.close()
		}
	}()
	if err = generationPrivateDir(r.root); err != nil {
		return nil, observedGeneration{}, nil, err
	}
	if err = checkGenerationAnchor(parent, r.root, rootName, spec.LogicalPath, rootPath, false); err != nil {
		return nil, observedGeneration{}, nil, err
	}
	r.lock, err = privatefile.OpenAt(r.root, "ledger.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	if err = privateGenerationRecoveryFile(r.lock, 0); err != nil {
		return nil, observedGeneration{}, nil, err
	}
	if err = unix.Flock(int(r.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, observedGeneration{}, nil, errors.New("generation lifetime held")
	}
	r.ledger, err = privatefile.OpenAt(r.root, "ledger.jsonl", os.O_RDWR, 0)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	if err = privateGenerationRecoveryFile(r.ledger, 4<<20); err != nil {
		return nil, observedGeneration{}, nil, err
	}
	events, err := generationEvents(r.ledger)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	if events[0].LogicalPath != spec.LogicalPath || events[0].Nonce != nonce {
		return nil, observedGeneration{}, nil, errors.New("generation installation mismatch")
	}
	states := make(map[string]string)
	var intent, ready generationEvent
	for _, e := range events[1:] {
		enonce, _, valid := parseRecoveryGenerationID(e.ID)
		if !valid || enonce != nonce {
			return nil, observedGeneration{}, nil, errors.New("generation ledger ID mismatch")
		}
		prior := states[e.ID]
		transition := (prior == "" && e.State == "intent") ||
			(prior == "intent" && e.State == "ready") ||
			(prior == "ready" && (e.State == "launching" || e.State == "retired")) ||
			(prior == "launching" && (e.State == "born" || e.State == "retired")) ||
			(prior == "born" && (e.State == "custody" || e.State == "retired")) ||
			(prior == "custody" && e.State == "retired")
		if !transition {
			return nil, observedGeneration{}, nil, errors.New("generation ledger transition invalid")
		}
		states[e.ID] = e.State
		if e.ID == id {
			if e.State == "intent" {
				intent = e
			}
			if e.State == "ready" {
				ready = e
			}
		}
	}
	if states[id] == "" || intent.State != "intent" || ready.State != "ready" ||
		intent.Path != path || ready.Path != path || intent.BootID == "" || ready.BootID != intent.BootID ||
		len(intent.ConfigDigest) != 64 {
		return nil, observedGeneration{}, nil, errors.New("exact generation intent unavailable")
	}
	// This digest is over the original FUSEConfig (including fields not
	// persisted in serving desired); it is distinct from the owner config
	// digest. Validate its encoding, while anchored path, ledger ID, boot,
	// and observed FSID provide the exact resource identity.
	if b, decodeErr := hex.DecodeString(intent.ConfigDigest); decodeErr != nil || len(b) != 32 || hex.EncodeToString(b) != intent.ConfigDigest {
		return nil, observedGeneration{}, nil, errors.New("generation config digest invalid")
	}
	if states[id] == "retired" {
		return nil, observedGeneration{}, nil, errors.New("generation already retired")
	}
	names, err := generationNames(r.root)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	for _, name := range names {
		if name != "ledger.lock" && name != "ledger.jsonl" && name != nonce {
			return nil, observedGeneration{}, nil, errors.New("untracked generation root entry")
		}
	}
	installFD, err := unix.Openat(int(r.root.Fd()), nonce, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, observedGeneration{}, nil, err
	}
	install := os.NewFile(uintptr(installFD), nonce)
	if err = generationPrivateDir(install); err != nil {
		install.Close()
		return nil, observedGeneration{}, nil, err
	}
	genNames, err := generationNames(install)
	if err != nil {
		install.Close()
		return nil, observedGeneration{}, nil, err
	}
	seen := make(map[string]bool)
	for _, name := range genNames {
		priorID := nonce + "-" + name
		if states[priorID] == "" {
			install.Close()
			return nil, observedGeneration{}, nil, errors.New("untracked generation directory")
		}
		seen[priorID] = true
	}
	for priorID, state := range states {
		if !seen[priorID] || (priorID != id && state != "retired") {
			install.Close()
			return nil, observedGeneration{}, nil, errors.New("other generation unresolved")
		}
	}
	if err := verifyPrivateObservedSidecar(install, id); err != nil {
		install.Close()
		return nil, observedGeneration{}, nil, err
	}
	og, ok := readObservedSidecar(install, nonce, id)
	if !ok || og.path != path || og.boot != intent.BootID || og.fsid == [2]int32{} {
		install.Close()
		return nil, observedGeneration{}, nil, errors.New("generation observed sidecar mismatch")
	}
	r.bootID = og.boot
	// Opening the mounted leaf could enter its data plane. Verify all saved
	// parents except the leaf now; the leaf is checked after retirement.
	fd2, ids, err := openGenerationAncestors(filepath.Dir(path))
	if err != nil {
		install.Close()
		return nil, observedGeneration{}, nil, err
	}
	fd2.Close()
	if len(ready.Parents) != len(ids)+1 {
		install.Close()
		return nil, observedGeneration{}, nil, errors.New("generation ancestry length changed")
	}
	for i := range ids {
		if ids[i] != ready.Parents[i] {
			install.Close()
			return nil, observedGeneration{}, nil, errors.New("generation ancestry changed")
		}
	}
	r.identities = ready.Parents
	return r, og, install, nil
}

func verifyPrivateObservedSidecar(install *os.File, id string) error {
	genName := id[len(id)-16:]
	fd, err := unix.Openat(int(install.Fd()), genName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	gen := os.NewFile(uintptr(fd), genName)
	defer gen.Close()
	if err := generationPrivateDir(gen); err != nil {
		return err
	}
	f, err := privatefile.OpenAt(gen, observedSidecarName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return privateGenerationRecoveryFile(f, 4096)
}

func privateGenerationRecoveryFile(f *os.File, maxSize int64) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || st.Nlink != 1 ||
		int(st.Uid) != os.Geteuid() || info.Size() > maxSize {
		return errors.New("generation recovery file is not private")
	}
	return nil
}

func recoverySnapshot(ctx context.Context, observe func(context.Context) (mounttable.Snapshot, error),
	logical, root string, og observedGeneration, requireAbsent bool) error {
	window, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s, err := observe(window)
	if err != nil || s.Version != 1 || s.BootID != og.boot || s.Entries == nil {
		return errors.New("generation inventory unavailable")
	}
	found := false
	for _, e := range s.Entries {
		if !generationInventoryPathValid(e.Path) || e.Source == "" || e.Type == "" {
			return errors.New("generation inventory incomplete")
		}
		under := func(p string) bool {
			return strings.EqualFold(e.Path, p) || strings.HasPrefix(strings.ToLower(e.Path), strings.ToLower(p)+"/")
		}
		if under(logical) || under(root) || e.FSID == og.fsid {
			if requireAbsent || found || e.Path != og.path || e.FSID != og.fsid || e.Type != "macfuse" {
				return errors.New("generation namespace occupied or identity changed")
			}
			found = true
		}
	}
	if !requireAbsent && !found {
		return errors.New("observed generation mount absent before retirement")
	}
	return nil
}

func retainedRecoveryAttemptPresent(install *os.File, id string) (bool, error) {
	genName := id[len(id)-16:]
	fd, err := unix.Openat(int(install.Fd()), genName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	gen := os.NewFile(uintptr(fd), genName)
	defer gen.Close()
	f, err := privatefile.OpenAt(gen, recoveryAttemptName, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := privateGenerationRecoveryFile(f, 4096); err != nil {
		return false, err
	}
	return true, nil
}

func recoveryAttempt(install *os.File, id string, expected generationRecoveryAttempt) (bool, error) {
	genName := id[len(id)-16:]
	fd, err := unix.Openat(int(install.Fd()), genName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return false, err
	}
	gen := os.NewFile(uintptr(fd), genName)
	defer gen.Close()
	if err := generationPrivateDir(gen); err != nil {
		return false, err
	}
	f, err := privatefile.OpenAt(gen, recoveryAttemptName, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := privateGenerationRecoveryFile(f, 4096); err != nil {
		return false, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(raw) == 0 || len(raw) > 4096 {
		return false, errors.New("recovery attempt unreadable")
	}
	var got generationRecoveryAttempt
	if err := json.Unmarshal(raw, &got); err != nil || got != expected {
		return false, errors.New("recovery attempt identity mismatch")
	}
	return true, nil
}

func publishRecoveryAttempt(install *os.File, id string, attempt generationRecoveryAttempt) error {
	genName := id[len(id)-16:]
	fd, err := unix.Openat(int(install.Fd()), genName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	gen := os.NewFile(uintptr(fd), genName)
	defer gen.Close()
	if err := generationPrivateDir(gen); err != nil {
		return err
	}
	raw, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	f, err := privatefile.OpenAt(gen, recoveryAttemptName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if n, e := f.Write(append(raw, '\n')); e != nil {
		f.Close()
		return e
	} else if n != len(raw)+1 {
		f.Close()
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return gen.Sync()
}

// RecoverRetainedGeneration acts on exactly one previously observed, same-boot
// generation. Its exclusive sidecar makes a lost Reclaim/Retire reply a HOLD on
// rerun; only an independently observed clean RETIRED helper state may settle
// the existing ledger. A pending helper job can outlive this call.
func RecoverRetainedGeneration(ctx context.Context, spec RetainedGenerationRecovery,
	h RetainedGenerationHelper, observe func(context.Context) (mounttable.Snapshot, error)) RetainedGenerationOutcome {
	if h == nil || observe == nil {
		return recoveryHold("helper_or_observer_unavailable")
	}
	r, og, install, err := openRetainedGeneration(spec)
	if err != nil {
		return recoveryHold("generation_identity_unavailable")
	}
	defer r.close()
	defer install.Close()
	attempt := generationRecoveryAttempt{Version: 1, Root: spec.Root, OwnerInstance: spec.OwnerInstance,
		Build: spec.Build, ConfigDigest: spec.ConfigDigest, OpID: spec.OpID, Path: og.path, BootID: og.boot, FSID: og.fsid}
	attempted, err := recoveryAttempt(install, r.ID, attempt)
	if err != nil {
		return recoveryAttemptHold("attempt_identity_unavailable")
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	status, err := h.Status(sctx, spec.OpID)
	cancel()
	if err != nil || status.OpID != spec.OpID || status.Forced || status.Err != "" || len(status.Anomaly) != 0 || len(status.Descendants) != 0 {
		if attempted {
			return recoveryAttemptHold("helper_status_unknown_or_unsafe")
		}
		return recoveryHold("helper_status_unknown_or_unsafe")
	}
	if status.State != "RETIRED" {
		if attempted {
			return recoveryAttemptHold("previous_helper_attempt_uncertain")
		}
		if status.State != "REGISTERED" {
			return recoveryHold("helper_not_registered")
		}
		if err := recoverySnapshot(ctx, observe, spec.LogicalPath, r.rootPath, og, false); err != nil {
			return recoveryHold("pre_retirement_inventory_unavailable")
		}
		if err := publishRecoveryAttempt(install, r.ID, attempt); err != nil {
			return recoveryHold("attempt_publication_uncertain")
		}
		// No helper request precedes the fsynced identity-bound attempt.
		rctx, rcancel := context.WithTimeout(ctx, priorResolveWait+5*time.Second)
		defer rcancel()
		lease, res, err := h.Reclaim(rctx, spec.OpID)
		if err != nil {
			return recoveryAttemptHold("reclaim_outcome_unknown")
		}
		if lease == nil {
			return recoveryAttemptHold("reclaim_without_lease")
		}
		defer lease.Close()
		if !res.Reclaimed || res.Outcome.OpID != spec.OpID ||
			res.Outcome.State != "REGISTERED" || res.Outcome.Forced || res.Outcome.Err != "" || len(res.Outcome.Anomaly) != 0 || len(res.Outcome.Descendants) != 0 {
			return recoveryAttemptHold("reclaim_not_ordinary_or_live_owner")
		}
		status, err = h.Retire(rctx, spec.OpID, client.RetireRequest{Liveness: client.LivenessUnknown}, priorResolveWait)
		if err != nil || status.OpID != spec.OpID {
			return recoveryAttemptHold("retire_outcome_unknown_helper_may_continue")
		}
	}
	result := RetainedGenerationOutcome{State: "HELPER_RETIRED_LOCAL_SETTLEMENT_PENDING",
		Forced: status.Forced, Syscalls: status.Syscalls}
	if status.State != "RETIRED" || status.Forced || status.Err != "" || len(status.Anomaly) != 0 || len(status.Descendants) != 0 {
		result.State, result.ErrorCode = "HOLD", "helper_not_cleanly_retired"
		result.HelperMayContinue = status.State != "RETIRED"
		return result
	}
	if err := recoverySnapshot(ctx, observe, spec.LogicalPath, r.rootPath, og, true); err != nil {
		result.ErrorCode = "post_retirement_inventory_unavailable"
		return result
	}
	if err := r.verifyAncestry(); err != nil {
		result.ErrorCode = "post_retirement_ancestry_changed"
		return result
	}
	if err := r.append(generationEvent{State: "retired", Path: og.path, BootID: og.boot}); err != nil {
		result.ErrorCode = "ledger_settlement_uncertain"
		return result
	}
	result.State, result.Retired = "RECOVERED_RESOURCE_RETIRED", true
	return result
}
