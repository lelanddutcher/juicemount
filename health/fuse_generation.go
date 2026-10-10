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
