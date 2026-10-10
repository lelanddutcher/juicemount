package unmountd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	receiptVersion = 1
	// DefaultReceiptLimit bounds one uid's receipts. A healthy user has one
	// NFS and one FUSE mount, plus a few generations in flight during a
	// restart. The limit stops a same-uid caller from filling root's disk.
	DefaultReceiptLimit = 16
	maxKernelString     = 1024 // MAXPATHLEN: f_mntonname / f_mntfromname capacity
	maxReceiptBytes     = 16 << 10
	tempMarker          = ".tmp-"
)

var opIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// Receipt records one kernel mount that the JuiceMount service created and
// proved at registration time. It is the third leg of authority (with live
// owner and token); on its own it authorizes nothing.
type Receipt struct {
	Version     int       `json:"version"`
	UID         uint32    `json:"uid"`
	Boot        string    `json:"boot"`
	OpID        string    `json:"op_id"`
	Type        MountType `json:"type"`
	FSID        FSID      `json:"fsid"`
	Token       string    `json:"token"`
	MntOnName   string    `json:"mntonname"`
	MntFromName string    `json:"mntfromname"`
	Generation  uint64    `json:"generation"`
	CreatedAt   time.Time `json:"created_at"`
}

// validate checks a receipt's internal consistency, including that the
// kernel strings carry the token in the type-specific place:
//
//   - nfs:     mntfromname = "<host>:/jm-<token>" (mountbroker export generation);
//   - macfuse: mntonname   = ".../<token>/<generation %016x>/mount"
//     (health/fuse_generation.go installation nonce + generation directory).
func (r Receipt) validate() error {
	bad := func(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidReceipt, msg) }
	if r.Version != receiptVersion {
		return bad("version")
	}
	if !opIDPattern.MatchString(r.OpID) {
		return bad("op_id")
	}
	if r.Boot == "" || len(r.Boot) > 64 {
		return bad("boot")
	}
	if !r.Type.valid() {
		return bad("type")
	}
	if r.FSID.IsZero() {
		return bad("fsid")
	}
	if !validToken(r.Token) {
		return bad("token")
	}
	if r.MntOnName == "" || len(r.MntOnName) >= maxKernelString || !filepath.IsAbs(r.MntOnName) ||
		filepath.Clean(r.MntOnName) != r.MntOnName || r.MntOnName == "/" {
		return bad("mntonname")
	}
	if r.MntFromName == "" || len(r.MntFromName) >= maxKernelString {
		return bad("mntfromname")
	}
	if !tokenCarried(r.Type, r.Token, r.Generation, r.MntOnName, r.MntFromName) {
		return bad("kernel strings do not carry the token")
	}
	if r.CreatedAt.IsZero() {
		return bad("created_at")
	}
	return nil
}

func validToken(t string) bool {
	b, err := hex.DecodeString(t)
	return err == nil && len(b) == 16 && strings.ToLower(t) == t && t != strings.Repeat("0", 32)
}

// tokenCarried is the token leg of the predicate, applied to both the receipt
// and the live entry. The string positions are those written by the creators
// in internal/mountbroker and health/fuse_generation.go.
func tokenCarried(typ MountType, token string, generation uint64, onName, fromName string) bool {
	switch typ {
	case TypeNFS:
		return strings.HasSuffix(fromName, ":/jm-"+token)
	case TypeMacFUSE:
		if generation == 0 {
			return false
		}
		return strings.HasSuffix(onName, "/"+token+"/"+fmt.Sprintf("%016x", generation)+"/mount")
	}
	return false
}

// Store is the root-owned receipt directory:
//
//	<root>/<uid>/<op_id>.json   mode 0600, in a 0700 directory
//
// Writes are crash-safe: temp file in the same directory, fsync, rename,
// fsync the directory. A crash leaves either no receipt or a complete one;
// leftover temp files are removed on open and never read.
type Store struct {
	root  string
	limit int

	mu sync.Mutex
	// crash is a test seam. When it returns a non-nil error at a stage the
	// write stops right there with no cleanup, as a process crash would.
	crash func(stage string) error
}

// OpenStore opens (creating if needed) the receipt root and removes temp
// files left by a crash. limit <= 0 selects DefaultReceiptLimit.
func OpenStore(root string, limit int) (*Store, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errors.New("unmountd: receipt root must be a clean absolute path")
	}
	if limit <= 0 {
		limit = DefaultReceiptLimit
	}
	if err := ensureDir(root); err != nil {
		return nil, err
	}
	s := &Store{root: root, limit: limit}
	uids, err := s.UIDs()
	if err != nil {
		return nil, err
	}
	for _, uid := range uids {
		dir := s.uidDir(uid)
		names, err := readNames(dir)
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			if strings.Contains(n, tempMarker) {
				_ = os.Remove(filepath.Join(dir, n))
			}
		}
	}
	return s, nil
}

func ensureDir(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unmountd: %s is not a real directory", dir)
	}
	// The daemon runs as root, so this requires root ownership in production;
	// a directory another user owns could be swapped or filled by them.
	if st, ok := fi.Sys().(*syscall.Stat_t); !ok || st.Uid != uint32(os.Geteuid()) {
		owner := -1
		if ok {
			owner = int(st.Uid)
		}
		return fmt.Errorf("unmountd: %s is owned by uid %d, want %d", dir, owner, os.Geteuid())
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("unmountd: %s is group/other accessible", dir)
	}
	return nil
}

func readNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

func (s *Store) uidDir(uid uint32) string {
	return filepath.Join(s.root, strconv.FormatUint(uint64(uid), 10))
}

func (s *Store) path(uid uint32, op string) string {
	return filepath.Join(s.uidDir(uid), op+".json")
}

// UIDs lists the uids that have a receipt directory.
func (s *Store) UIDs() ([]uint32, error) {
	names, err := readNames(s.root)
	if err != nil {
		return nil, err
	}
	var out []uint32
	for _, n := range names {
		u, err := strconv.ParseUint(n, 10, 32)
		if err != nil || strconv.FormatUint(u, 10) != n {
			continue
		}
		out = append(out, uint32(u))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Put durably records a new receipt. It refuses to overwrite an existing op
// and refuses past the per-uid limit.
func (s *Store) Put(r Receipt) error {
	if err := r.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.uidDir(r.UID)
	if err := ensureDir(dir); err != nil {
		return err
	}
	existing, err := s.listLocked(r.UID)
	if err != nil {
		return err
	}
	for _, e := range existing {
		if e.OpID == r.OpID {
			return ErrReceiptExists
		}
	}
	if len(existing) >= s.limit {
		return ErrReceiptLimit
	}
	// A new receipt starts un-withdrawn.
	if err := os.Remove(s.withdrawnPath(r.UID, r.OpID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.writeAtomic(dir, r.OpID+".json", data)
}

func (s *Store) hook(stage string) error {
	if s.crash == nil {
		return nil
	}
	return s.crash(stage)
}

func (s *Store) writeAtomic(dir, name string, data []byte) (err error) {
	tmp, err := os.CreateTemp(dir, "."+name+tempMarker+"*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	crashed := false
	defer func() {
		if err != nil && !crashed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return err
	}
	half := len(data) / 2
	if _, err = tmp.Write(data[:half]); err != nil {
		return err
	}
	if err = s.hook("partial-write"); err != nil {
		crashed = true
		return err
	}
	if _, err = tmp.Write(data[half:]); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = s.hook("before-rename"); err != nil {
		crashed = true
		return err
	}
	if err = os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Get loads one receipt. A missing receipt is ErrNoReceipt; a corrupt or
// inconsistent one is ErrInvalidReceipt, never "absent".
func (s *Store) Get(uid uint32, op string) (Receipt, error) {
	if !opIDPattern.MatchString(op) {
		return Receipt{}, fmt.Errorf("%w: op_id", ErrInvalidReceipt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.load(uid, op+".json")
}

func (s *Store) load(uid uint32, name string) (Receipt, error) {
	f, err := os.OpenFile(filepath.Join(s.uidDir(uid), name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return Receipt{}, ErrNoReceipt
	}
	if err != nil {
		return Receipt{}, err
	}
	defer f.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(io.LimitReader(f, maxReceiptBytes+1)); err != nil {
		return Receipt{}, err
	}
	if buf.Len() > maxReceiptBytes {
		return Receipt{}, fmt.Errorf("%w: oversized", ErrInvalidReceipt)
	}
	dec := json.NewDecoder(&buf)
	dec.DisallowUnknownFields()
	var r Receipt
	if err := dec.Decode(&r); err != nil {
		return Receipt{}, fmt.Errorf("%w: %v", ErrInvalidReceipt, err)
	}
	if err := r.validate(); err != nil {
		return Receipt{}, err
	}
	if r.UID != uid || r.OpID+".json" != name {
		return Receipt{}, fmt.Errorf("%w: misfiled", ErrInvalidReceipt)
	}
	return r, nil
}

// List returns one uid's valid receipts. Any corrupt receipt is an error:
// a receipt that cannot be read must not silently vanish from the count.
func (s *Store) List(uid uint32) ([]Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked(uid)
}

func (s *Store) listLocked(uid uint32) ([]Receipt, error) {
	names, err := readNames(s.uidDir(uid))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var out []Receipt
	for _, n := range names {
		if strings.Contains(n, tempMarker) || !strings.HasSuffix(n, ".json") {
			continue
		}
		r, err := s.load(uid, n)
		if err != nil {
			return nil, fmt.Errorf("receipt %d/%s: %w", uid, n, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// Delete removes a receipt (and its withdrawn mark) durably. Deleting an
// absent receipt is not an error.
func (s *Store) Delete(uid uint32, op string) error {
	if !opIDPattern.MatchString(op) {
		return fmt.Errorf("%w: op_id", ErrInvalidReceipt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(uid, op))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(s.withdrawnPath(uid, op)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(s.uidDir(uid))
}

// The withdrawn mark records that an op's last owner request was a Cancel,
// so the orphan timer (also after a daemon restart, via the launch sweep)
// never restarts an ordinary retirement the owner withdrew:
//
//	<root>/<uid>/<op_id>.withdrawn   empty, mode 0600, beside the receipt
//
// It is not a receipt: List ignores it (only *.json is read), so an earlier
// helper build that does not know it is unaffected. A mark left behind by
// such a build only ever suppresses an orphan unmount, never causes one.
func (s *Store) withdrawnPath(uid uint32, op string) string {
	return filepath.Join(s.uidDir(uid), op+".withdrawn")
}

// MarkWithdrawn durably records the withdrawn mark for an existing receipt.
func (s *Store) MarkWithdrawn(uid uint32, op string) error {
	if !opIDPattern.MatchString(op) {
		return fmt.Errorf("%w: op_id", ErrInvalidReceipt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.load(uid, op+".json"); err != nil {
		return err
	}
	return s.writeAtomic(s.uidDir(uid), op+".withdrawn", nil)
}

// ClearWithdrawn removes the withdrawn mark. An absent mark is not an error.
func (s *Store) ClearWithdrawn(uid uint32, op string) error {
	if !opIDPattern.MatchString(op) {
		return fmt.Errorf("%w: op_id", ErrInvalidReceipt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.withdrawnPath(uid, op))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncDir(s.uidDir(uid))
}

// Withdrawn reports whether the op carries the withdrawn mark.
func (s *Store) Withdrawn(uid uint32, op string) (bool, error) {
	if !opIDPattern.MatchString(op) {
		return false, fmt.Errorf("%w: op_id", ErrInvalidReceipt)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := os.Lstat(s.withdrawnPath(uid, op))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}
