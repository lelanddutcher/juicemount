// Package mountbroker records local mount creation transactions. These records
// are recovery evidence, not authority to adopt or detach a kernel mount. A
// surviving privileged service and instance-safe detach are separate concerns.
package mountbroker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"golang.org/x/sys/unix"
)

type State string

const (
	NotStarted State = "not_started"
	InFlight   State = "in_flight"
	Foreign    State = "foreign"
	Unknown    State = "unknown"
	Observed   State = "observed"
	Retired    State = "retired"
)

// Spec binds the intended export to the listener and local metadata identity.
// The generation is enforced by the NFS server; it is not a secret capability.
type Spec struct {
	MountPath    string `json:"mount_path"`
	Source       string `json:"source"`
	Endpoint     string `json:"endpoint"`
	ExportPath   string `json:"export_path"`
	MetadataPath string `json:"metadata_path"`
}

type Record struct {
	Version     int                    `json:"version"`
	OperationID string                 `json:"operation_id"`
	BootID      string                 `json:"boot_id"`
	Spec        Spec                   `json:"spec"`
	State       State                  `json:"state"`
	Mount       *mounttable.MountEntry `json:"mount,omitempty"`
}

type Result struct {
	State       State
	OperationID string
	// Attempted is true before entering the mount callback. An error after this
	// point must retain the serving dependencies, even when observation fails.
	Attempted bool
	Record    *Record
	Err       error
}

// CreationError carries the custody boundary to callers. In particular a CLI
// must not exit after Attempted becomes true merely because Create failed.
type CreationError struct{ Result }

func (e *CreationError) Error() string { return fmt.Sprintf("mount creation %s: %v", e.State, e.Err) }
func (e *CreationError) Unwrap() error { return e.Err }
func (r Result) Failure() error {
	if r.State == Observed && r.Err == nil {
		return nil
	}
	return &CreationError{Result: r}
}

// Creator serializes one mount path across processes. Observe must return a
// complete bounded snapshot; Mount must return only when its command waiter
// has finished. Neither callback may infer absence from an inspection error.
type Creator struct {
	Directory string
	Observe   func(context.Context) (mounttable.Snapshot, error)
	syncFile  func(*os.File) error
	syncDir   func(*os.File) error
}

func (s Spec) validate() error {
	if s.MountPath == "/" || !filepath.IsAbs(s.MountPath) || filepath.Clean(s.MountPath) != s.MountPath ||
		!filepath.IsAbs(s.MetadataPath) || filepath.Clean(s.MetadataPath) != s.MetadataPath {
		return errors.New("mount creation requires canonical absolute mount and metadata paths")
	}
	generation := strings.TrimPrefix(s.ExportPath, "/jm-")
	b, err := hex.DecodeString(generation)
	if err != nil || len(generation) != 32 || len(b) != 16 || strings.ToLower(generation) != generation || generation == strings.Repeat("0", 32) || s.ExportPath != "/jm-"+generation {
		return errors.New("mount creation requires an enforced export generation")
	}
	host, port, err := net.SplitHostPort(s.Endpoint)
	p, e := strconv.Atoi(port)
	if err != nil || e != nil || host == "" || p < 1 || p > 65535 {
		return errors.New("invalid NFS endpoint")
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if s.Source != host+":"+s.ExportPath {
		return errors.New("mount source does not match server identity")
	}
	return nil
}

// Create retains the original callback API for existing callers. A caller
// that needs to bind a live custodian to the durable operation uses CreateBound.
func (c Creator) Create(ctx context.Context, spec Spec, mount func() error) Result {
	if mount == nil {
		return Result{State: NotStarted, Err: errors.New("missing creation dependency")}
	}
	return c.CreateBound(ctx, spec, func(Record) error { return mount() })
}

// CreateBound publishes the exact operation before any effect. Only a prior
// durably retired operation may be superseded, after fresh absence checks.
func (c Creator) CreateBound(ctx context.Context, spec Spec, mount func(Record) error) Result {
	result := Result{State: NotStarted}
	if err := spec.validate(); err != nil {
		result.Err = err
		return result
	}
	if ctx == nil || c.Observe == nil || mount == nil {
		result.Err = errors.New("missing creation dependency")
		return result
	}
	journal, err := openJournal(c.Directory, spec.MountPath)
	if err != nil {
		result.State = Unknown
		result.Err = err
		return result
	}
	defer journal.close()
	if c.syncFile != nil {
		journal.syncFile = c.syncFile
	}
	if c.syncDir != nil {
		journal.syncDir = c.syncDir
	}
	old, err := journal.read()
	if err != nil {
		result.State = Unknown
		result.Err = fmt.Errorf("read mount intent: %w", err)
		return result
	}
	if old != nil && old.State != Retired {
		result.State = InFlight
		result.Record = old
		result.OperationID = old.OperationID
		result.Err = errors.New("prior mount transaction requires broker reconciliation")
		return result
	}
	before, err := c.Observe(ctx)
	if err != nil || !validSnapshot(before) {
		result.State = Unknown
		result.Err = fmt.Errorf("pre-mount observation unavailable: %w", observationError(err))
		return result
	}
	if entriesAt(before, spec.MountPath) != nil {
		result.State = Foreign
		result.Err = errors.New("mount path or a descendant is already occupied")
		return result
	}
	oldFSID := [2]int32{}
	if old != nil && old.BootID == before.BootID {
		// Kernel FSIDs can be reused after reboot; source/export identity cannot.
		oldFSID = recordFSID(*old)
	}
	if sourceOrFSIDPresent(before, spec.Source, [2]int32{}) || old != nil &&
		(old.Spec.MountPath != spec.MountPath || sourceOrFSIDPresent(before, old.Spec.Source, oldFSID)) {
		result.State = Foreign
		result.Err = errors.New("current or retired export identity remains mounted")
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Err = err
		return result
	}
	var op [16]byte
	if _, err := io.ReadFull(rand.Reader, op[:]); err != nil {
		result.Err = err
		return result
	}
	rec := Record{Version: 1, OperationID: hex.EncodeToString(op[:]), BootID: before.BootID, Spec: spec, State: InFlight}
	if err := journal.write(rec); err != nil {
		result.State = Unknown
		result.Err = fmt.Errorf("persist mount intent: %w", err)
		return result
	}
	result.State = InFlight
	result.OperationID = rec.OperationID
	result.Record = &rec
	if err := ctx.Err(); err != nil {
		result.Err = err
		return result
	}
	result.Attempted = true
	mountErr := mount(rec)
	after, observeErr := c.Observe(ctx)
	if observeErr != nil || !validSnapshot(after) || after.BootID != rec.BootID {
		result.State = Unknown
		result.Err = errors.Join(mountErr, fmt.Errorf("post-mount observation unavailable or boot changed: %w", observationError(observeErr)))
		return result
	}
	entries := entriesAt(after, spec.MountPath)
	if len(entries) != 1 || entries[0].Path != spec.MountPath || entries[0].Source != spec.Source || entries[0].Type != "nfs" || entries[0].FSID == [2]int32{} {
		result.State = Unknown
		result.Err = errors.Join(mountErr, errors.New("mount result is absent, ambiguous, or does not match the intent"))
		return result
	}
	observed := rec
	observed.State = Observed
	observed.Mount = &entries[0]
	if err := journal.write(observed); err != nil {
		result.State = Unknown
		result.Err = errors.Join(mountErr, fmt.Errorf("persist mount observation: %w", err))
		return result
	}
	result.State = Observed
	result.Record = &observed
	result.Err = mountErr
	return result
}

func observationError(err error) error {
	if err != nil {
		return err
	}
	return errors.New("invalid snapshot")
}
func validSnapshot(s mounttable.Snapshot) bool {
	if s.Version != 1 || strings.TrimSpace(s.BootID) == "" || s.Entries == nil {
		return false
	}
	for _, e := range s.Entries {
		if !filepath.IsAbs(e.Path) || filepath.Clean(e.Path) != e.Path || e.Source == "" || e.Type == "" {
			return false
		}
	}
	return true
}

func entriesAt(s mounttable.Snapshot, path string) []mounttable.MountEntry {
	var result []mounttable.MountEntry
	for _, e := range s.Entries {
		// Case variants are conservatively treated as occupied on macOS. This does
		// not grant adoption authority, including on case-sensitive filesystems.
		if strings.EqualFold(e.Path, path) || strings.HasPrefix(strings.ToLower(e.Path), strings.ToLower(path)+string(os.PathSeparator)) {
			result = append(result, e)
		}
	}
	return result
}

type journal struct {
	root              *os.Root
	dir, lock         *os.File
	name              string
	syncFile, syncDir func(*os.File) error
}

func privateFile(f *os.File, directory bool) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || st.Mode().Perm()&0077 != 0 || st.IsDir() != directory || (!directory && (!st.Mode().IsRegular() || stat.Nlink != 1)) {
		return errors.New("mount journal requires private, singly linked files owned by this user")
	}
	return nil
}

// The owner provisions this directory on durable local storage. The pinned
// directory descriptor and O_NOFOLLOW keep leaf replacement/symlink writes
// from escaping it. It is deliberately not a privileged ownership receipt.
func openJournal(directory, mountPath string) (*journal, error) {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return nil, errors.New("invalid journal directory")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	j := &journal{root: root, syncFile: func(f *os.File) error { return f.Sync() }, syncDir: func(f *os.File) error { return f.Sync() }}
	fail := func(err error) (*journal, error) { j.close(); return nil, err }
	j.dir, err = root.Open(".")
	if err != nil {
		return fail(err)
	}
	if err := privateFile(j.dir, true); err != nil {
		return fail(err)
	}
	sum := sha256.Sum256([]byte(strings.ToLower(mountPath)))
	j.name = hex.EncodeToString(sum[:]) + ".json"
	j.lock, err = privatefile.OpenAt(j.dir, j.name+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fail(err)
	}
	if err := privateFile(j.lock, false); err != nil {
		return fail(err)
	}
	if err := unix.Flock(int(j.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("mount transaction in flight: %w", err))
	}
	// The lock's name is persistent and is never removed on Close.
	return j, nil
}
func (j *journal) close() {
	if j.lock != nil {
		_ = j.lock.Close()
	}
	if j.dir != nil {
		_ = j.dir.Close()
	}
	if j.root != nil {
		_ = j.root.Close()
	}
}
func (j *journal) read() (*Record, error) {
	f, err := privatefile.OpenAt(j.dir, j.name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := privateFile(f, false); err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err != nil || st.Size() > 64<<10 {
		return nil, errors.New("mount journal exceeds size bound")
	}
	data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return nil, errors.New("cannot read bounded mount journal")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var rec Record
	if err := dec.Decode(&rec); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing mount journal data")
	}
	if rec.Version != 1 || !canonicalOperationID(rec.OperationID) || rec.BootID == "" || rec.Spec.validate() != nil ||
		(rec.State != InFlight && rec.State != Observed && rec.State != Retired) ||
		(rec.State == InFlight && rec.Mount != nil) ||
		(rec.State == Observed && rec.Mount == nil) ||
		(rec.Mount != nil && !validRecordMount(rec.Spec, rec.Mount)) {
		return nil, errors.New("invalid mount journal record")
	}
	// Only our canonical writer produces records. This also refuses duplicate
	// JSON keys whose last-value-wins decoding could disguise corruption.
	canonical, err := json.Marshal(rec)
	if err != nil || !bytes.Equal(data, canonical) {
		return nil, errors.New("noncanonical or ambiguous mount journal")
	}
	return &rec, nil
}
func (j *journal) write(rec Record) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return err
	}
	name := ".pending-" + hex.EncodeToString(nonce[:])
	f, err := j.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer j.root.Remove(name)
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := j.syncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := j.root.Rename(name, j.name); err != nil {
		return err
	}
	return j.syncDir(j.dir)
}

// ObservedAbsent is conservative and grants no detach/adoption authority.
func ObservedAbsent(snapshot mounttable.Snapshot, path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/" && validSnapshot(snapshot) && entriesAt(snapshot, path) == nil
}
