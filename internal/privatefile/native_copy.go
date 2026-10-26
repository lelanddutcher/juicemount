package privatefile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrNativeCopyBusy = errors.New("native copy owner busy")
var ErrNativeCopyChanged = errors.New("native copy identity changed")
var ErrNativeCopyUncertain = errors.New("native copy write outcome uncertain")

// NativeRoot is a separate local-copy namespace. It never opens server
// attempts or ready payloads. The caller supplies an existing owner-only root.
type NativeRoot struct {
	path                                                          string
	root, staging, copies, inventoryFile, markerFile              *os.File
	rootID, stagingID, copiesID, controlID, inventoryID, markerID Identity
	token, endpoint, library                                      string
	mu                                                            sync.Mutex
	active                                                        int
	closed                                                        bool
	closeErr                                                      error
}

func nativeCopyID(id string) bool {
	if len(id) != 37 || !strings.HasPrefix(id, "copy_") || id[5:] == strings.Repeat("0", 32) {
		return false
	}
	for i := 5; i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func nativeDirectory(parent *os.File, name string) (_ *os.File, err error) {
	if err := unix.Mkdirat(int(parent.Fd()), name, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return nil, err
	}
	if err := parent.Sync(); err != nil {
		return nil, err
	}
	return openDirectoryAt(parent, name)
}

func (r *NativeRoot) take() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrNativeCopyChanged
	}
	r.active++
	return nil
}
func (r *NativeRoot) leave() {
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
}
func (r *NativeRoot) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	if r.active != 0 {
		return ErrNativeCopyBusy
	}
	r.closed = true
	r.closeErr = errors.Join(r.markerFile.Close(), r.inventoryFile.Close(), r.copies.Close(), r.staging.Close(), r.root.Close())
	return r.closeErr
}

func (r *NativeRoot) Revalidate() (resultErr error) {
	if r == nil || r.root == nil || r.staging == nil || r.copies == nil {
		return ErrNativeCopyChanged
	}
	live, err := walkAbsoluteDirectory(r.path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, live.Close()) }()
	if id, err := privateDir(live); err != nil || id != r.rootID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	if id, err := privateDir(r.root); err != nil || id != r.rootID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	for _, child := range []struct {
		name string
		held *os.File
		want Identity
	}{{"staging", r.staging, r.stagingID}, {"copies", r.copies, r.copiesID}} {
		fresh, err := openDirectoryAt(r.root, child.name)
		if err != nil {
			return err
		}
		id, idErr := privateDir(fresh)
		closeErr := fresh.Close()
		if idErr != nil || closeErr != nil || id != child.want {
			return errors.Join(idErr, closeErr, ErrNativeCopyChanged)
		}
		if id, err := privateDir(child.held); err != nil || id != child.want {
			return errors.Join(err, ErrNativeCopyChanged)
		}
	}
	control, err := OpenAt(r.root, "control.lock", os.O_RDONLY, 0)
	if err != nil {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	controlID, idErr := privateRegular(control)
	closeErr := control.Close()
	if idErr != nil || closeErr != nil || controlID != r.controlID || controlID.Size != 0 {
		return errors.Join(idErr, closeErr, ErrNativeCopyChanged)
	}
	if err := r.rootEntries(); err != nil {
		return err
	}
	if err := r.copiesEntries(); err != nil {
		return err
	}
	return nil
}

type NativeControl struct {
	root        *NativeRoot
	lock        *os.File
	lockID      Identity
	inventory   *nativeInventory
	stagingSync func(*os.File) error
	closed      bool
	closeErr    error
}

func (r *NativeRoot) LockControl() (_ *NativeControl, resultErr error) {
	if err := r.take(); err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			r.leave()
		}
	}()
	lock, err := OpenAt(r.root, "control.lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, lock.Close())
		}
	}()
	id, err := privateRegular(lock)
	if err != nil || id.Size != 0 {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("native control lock: %w", ErrNativeCopyBusy)
	}
	check, err := OpenAt(r.root, "control.lock", os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	checkID, checkErr := privateRegular(check)
	closeErr := check.Close()
	if checkErr != nil || closeErr != nil || checkID != id {
		return nil, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	if err := r.revalidateInventory(); err != nil {
		return nil, err
	}
	db, err := nativeDB(r.path + "/accounting.sqlite")
	if err != nil {
		return nil, err
	}
	inventory := &nativeInventory{db: db, token: r.token, endpoint: r.endpoint, library: r.library}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, inventory.close())
		}
	}()
	if err := r.recognizeInventory(inventory); err != nil {
		return nil, err
	}
	if err := r.Revalidate(); err != nil {
		return nil, err
	}
	if err := r.revalidateInventory(); err != nil {
		return nil, err
	}
	control := &NativeControl{root: r, lock: lock, lockID: id, inventory: inventory}
	if _, err := control.inventoryRows(); err != nil {
		return nil, err
	}
	return control, nil
}
func (c *NativeControl) Close() error {
	if c == nil {
		return nil
	}
	if c.closed {
		return c.closeErr
	}
	c.closed = true
	c.closeErr = errors.Join(c.inventory.close(), c.lock.Close())
	c.root.leave()
	return c.closeErr
}
func (c *NativeControl) syncStaging() error {
	if c.stagingSync != nil {
		return c.stagingSync(c.root.staging)
	}
	return c.root.staging.Sync()
}
func (c *NativeControl) check() error {
	if c == nil || c.closed || c.lock == nil {
		return ErrNativeCopyChanged
	}
	if err := c.root.Revalidate(); err != nil {
		return err
	}
	if err := c.root.revalidateInventory(); err != nil {
		return err
	}
	if id, err := privateRegular(c.lock); err != nil || id != c.lockID || id != c.root.controlID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	return nil
}
func (c *NativeControl) CopyIDs() ([]string, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	fresh, err := openDirectoryAt(c.root.root, "staging")
	if err != nil {
		return nil, err
	}
	defer fresh.Close()
	if id, err := privateDir(fresh); err != nil || id != c.root.stagingID {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	names, err := fresh.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if !nativeCopyID(name) {
			return nil, ErrNativeCopyChanged
		}
	}
	sort.Strings(names)
	return names, nil
}
func (c *NativeControl) RootIdentity() Identity { return c.root.rootID }

type NativeCopy struct {
	root          *NativeRoot
	control       *NativeControl
	id            string
	dir, lock     *os.File
	dirID, lockID Identity
	closed        bool
	closeErr      error
	writeOps      *nativeWriteOps
}
type nativeWriteOps struct {
	syncFile  func(*os.File) error
	closeFile func(*os.File) error
	syncDir   func(*os.File) error
}

func (c *NativeControl) OpenCopy(id string) (_ *NativeCopy, resultErr error) {
	return c.openCopy(id, false)
}
func (c *NativeControl) CreateCopy(id string) (_ *NativeCopy, resultErr error) {
	return c.openCopy(id, true)
}

// ResumePendingCopy is only for an inventory intent_pending row. It completes
// missing not-yet-acknowledged namespace objects without replacing any leaf.
func (c *NativeControl) ResumePendingCopy(id string) (result *NativeCopy, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !nativeCopyID(id) {
		return nil, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if errors.Is(err, os.ErrNotExist) {
		return c.CreateCopy(id)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
			if result != nil {
				resultErr = errors.Join(resultErr, result.Close())
				result = nil
			}
		}
	}()
	if _, err := privateDir(dir); err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if name != "owner.lock" && name != "intent.json" {
			return nil, ErrNativeCopyChanged
		}
	}
	if slicesContain(names, "intent.json") && !slicesContain(names, "owner.lock") {
		return nil, ErrNativeCopyChanged
	}
	if !slicesContain(names, "owner.lock") {
		lock, err := OpenAt(dir, "owner.lock", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		id, checkErr := privateRegular(lock)
		if checkErr == nil {
			checkErr = lock.Sync()
		}
		closeErr := lock.Close()
		if checkErr != nil || closeErr != nil || id.Size != 0 {
			return nil, errors.Join(checkErr, closeErr, ErrNativeCopyUncertain)
		}
		if err := dir.Sync(); err != nil {
			return nil, errors.Join(err, ErrNativeCopyUncertain)
		}
	}
	if err := c.syncStaging(); err != nil {
		return nil, errors.Join(err, ErrNativeCopyUncertain)
	}
	return c.OpenCopy(id)
}
func (c *NativeControl) openCopy(id string, create bool) (_ *NativeCopy, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !nativeCopyID(id) {
		return nil, ErrNativeCopyChanged
	}
	if create {
		if err := unix.Mkdirat(int(c.root.staging.Fd()), id, 0700); err != nil {
			return nil, err
		}
		if err := c.syncStaging(); err != nil {
			return nil, errors.Join(err, ErrNativeCopyUncertain)
		}
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return nil, err
	}
	var lock *os.File
	defer func() {
		if resultErr != nil {
			if lock != nil {
				resultErr = errors.Join(resultErr, lock.Close())
			}
			resultErr = errors.Join(resultErr, dir.Close())
		}
	}()
	dirID, err := privateDir(dir)
	if err != nil {
		return nil, err
	}
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	lock, err = OpenAt(dir, "owner.lock", flags, 0600)
	if err != nil {
		return nil, err
	}
	lockID, err := privateRegular(lock)
	if err != nil || lockID.Size != 0 {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, ErrNativeCopyBusy
	}
	if create {
		if err := dir.Sync(); err != nil {
			return nil, errors.Join(err, ErrNativeCopyUncertain)
		}
	}
	copy := &NativeCopy{root: c.root, control: c, id: id, dir: dir, lock: lock, dirID: dirID, lockID: lockID}
	if err := c.root.take(); err != nil {
		return nil, err
	}
	if err := copy.Revalidate(); err != nil {
		c.root.leave()
		return nil, err
	}
	return copy, nil
}
func (n *NativeCopy) Close() error {
	if n == nil {
		return nil
	}
	if n.closed {
		return n.closeErr
	}
	n.closed = true
	n.closeErr = errors.Join(n.dir.Close(), n.lock.Close())
	n.root.leave()
	return n.closeErr
}
func (n *NativeCopy) RootIdentity() Identity      { return n.root.rootID }
func (n *NativeCopy) DirectoryIdentity() Identity { return n.dirID }
func (n *NativeCopy) LockIdentity() Identity      { return n.lockID }
func (n *NativeCopy) Revalidate() error {
	if n == nil || n.closed || n.dir == nil || n.lock == nil {
		return ErrNativeCopyChanged
	}
	if err := n.root.Revalidate(); err != nil {
		return err
	}
	fresh, err := openDirectoryAt(n.root.staging, n.id)
	if err != nil {
		return err
	}
	id, idErr := privateDir(fresh)
	closeErr := fresh.Close()
	if idErr != nil || closeErr != nil || id != n.dirID {
		return errors.Join(idErr, closeErr, ErrNativeCopyChanged)
	}
	if id, err := privateDir(n.dir); err != nil || id != n.dirID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	check, err := OpenAt(n.dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	checkID, checkErr := privateRegular(check)
	closeErr = check.Close()
	if checkErr != nil || closeErr != nil || checkID != n.lockID {
		return errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
	}
	if id, err := privateRegular(n.lock); err != nil || id != n.lockID {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	return nil
}

func nativeRecordLeaf(kind string) string {
	switch kind {
	case "intent":
		return "intent.json"
	case "selection":
		return "selection.json"
	case "checkpoint":
		return "checkpoint.json"
	case "prepared":
		return "prepared.json"
	case "verified":
		return "verified.json"
	case "released":
		return "released.json"
	}
	return ""
}
func readNativeRecord(dir *os.File, kind string) (_ []byte, resultErr error) {
	leaf := nativeRecordLeaf(kind)
	if leaf == "" {
		return nil, ErrNativeCopyChanged
	}
	f, err := OpenAt(dir, leaf, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	id, err := privateRegular(f)
	if err != nil || id.Size < 2 || id.Size > 4096 {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || int64(len(b)) != id.Size {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	if after, err := privateRegular(f); err != nil || after != id {
		return nil, errors.Join(err, ErrNativeCopyChanged)
	}
	return b, nil
}
func (c *NativeControl) readRecord(id, kind string) (_ []byte, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !nativeCopyID(id) {
		return nil, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if _, err := privateDir(dir); err != nil {
		return nil, err
	}
	return readNativeRecord(dir, kind)
}
func (c *NativeControl) ReadIntent(id string) ([]byte, error) { return c.readRecord(id, "intent") }
func (c *NativeControl) ReadSelection(id string) ([]byte, error) {
	return c.readRecord(id, "selection")
}
func (c *NativeControl) ReadCheckpoint(id string) ([]byte, error) {
	return c.readRecord(id, "checkpoint")
}
func (c *NativeControl) ReadPrepared(id string) ([]byte, error) { return c.readRecord(id, "prepared") }
func (c *NativeControl) ReadVerified(id string) ([]byte, error) { return c.readRecord(id, "verified") }
func (c *NativeControl) ReadReleased(id string) ([]byte, error) { return c.readRecord(id, "released") }
func (c *NativeControl) CopyEntries(id string) (_ []string, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !nativeCopyID(id) {
		return nil, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if _, err := privateDir(dir); err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{"owner.lock": true, "payload.partial": true, "intent.json": true, "selection.json": true, "checkpoint.json": true, "prepared.json": true, "verified.json": true, "released.json": true}
	for _, name := range names {
		if !allowed[name] {
			return nil, ErrNativeCopyChanged
		}
	}
	if !slicesContain(names, "owner.lock") {
		return nil, ErrNativeCopyChanged
	}
	for _, name := range names {
		if name != "owner.lock" && name != "payload.partial" {
			continue
		}
		f, err := OpenAt(dir, name, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		id, idErr := privateRegular(f)
		closeErr := f.Close()
		if idErr != nil || closeErr != nil || name == "owner.lock" && id.Size != 0 {
			return nil, errors.Join(idErr, closeErr, ErrNativeCopyChanged)
		}
	}
	sort.Strings(names)
	return names, nil
}
func (c *NativeControl) PendingCopyEntries(id string) (_ []string, resultErr error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if !nativeCopyID(id) {
		return nil, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if _, err := privateDir(dir); err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if name != "owner.lock" && name != "intent.json" {
			return nil, ErrNativeCopyChanged
		}
	}
	if slicesContain(names, "owner.lock") {
		lock, err := OpenAt(dir, "owner.lock", os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		id, checkErr := privateRegular(lock)
		closeErr := lock.Close()
		if checkErr != nil || closeErr != nil || id.Size != 0 {
			return nil, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
		}
	}
	if slicesContain(names, "intent.json") {
		if _, err := readNativeRecord(dir, "intent"); err != nil {
			return nil, err
		}
	}
	return names, nil
}
func (c *NativeControl) CopyPinnedIdentity(id string) (_ Identity, _ Identity, resultErr error) {
	if err := c.check(); err != nil {
		return Identity{}, Identity{}, err
	}
	if !nativeCopyID(id) {
		return Identity{}, Identity{}, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return Identity{}, Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	dirID, err := privateDir(dir)
	if err != nil {
		return Identity{}, Identity{}, err
	}
	lock, err := OpenAt(dir, "owner.lock", os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, Identity{}, err
	}
	lockID, checkErr := privateRegular(lock)
	closeErr := lock.Close()
	if checkErr != nil || closeErr != nil || lockID.Size != 0 {
		return Identity{}, Identity{}, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
	}
	return dirID, lockID, nil
}
func (c *NativeControl) CopyPartialIdentity(id string) (_ Identity, resultErr error) {
	if err := c.check(); err != nil {
		return Identity{}, err
	}
	if !nativeCopyID(id) {
		return Identity{}, ErrNativeCopyChanged
	}
	dir, err := openDirectoryAt(c.root.staging, id)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	if _, err := privateDir(dir); err != nil {
		return Identity{}, err
	}
	f, err := OpenAt(dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	return privateRegular(f)
}
func slicesContain(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}
func (n *NativeCopy) writeRecord(kind string, b []byte) (resultErr error) {
	if n.control == nil || len(b) < 2 || len(b) > 4096 || b[len(b)-1] != '\n' {
		return ErrNativeCopyChanged
	}
	if err := n.control.check(); err != nil {
		return err
	}
	if err := n.Revalidate(); err != nil {
		return err
	}
	leaf := nativeRecordLeaf(kind)
	if leaf == "" {
		return ErrNativeCopyChanged
	}
	f, err := OpenAt(n.dir, leaf, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		var closeErr error
		if n.writeOps != nil && n.writeOps.closeFile != nil {
			closeErr = n.writeOps.closeFile(f)
		} else {
			closeErr = f.Close()
		}
		if closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr, ErrNativeCopyUncertain)
		}
	}()
	id, err := privateRegular(f)
	if err != nil || id.Size != 0 {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if written, err := f.Write(b); err != nil || written != len(b) {
		return errors.Join(err, io.ErrShortWrite, ErrNativeCopyUncertain)
	}
	if n.writeOps != nil && n.writeOps.syncFile != nil {
		err = n.writeOps.syncFile(f)
	} else {
		err = f.Sync()
	}
	if err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if n.writeOps != nil && n.writeOps.syncDir != nil {
		err = n.writeOps.syncDir(n.dir)
	} else {
		err = n.dir.Sync()
	}
	if err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	return nil
}
func (n *NativeCopy) WriteIntent(b []byte) error            { return n.writeRecord("intent", b) }
func (n *NativeCopy) WriteSelection(b []byte) error         { return n.writeRecord("selection", b) }
func (n *NativeCopy) WriteInitialCheckpoint(b []byte) error { return n.writeRecord("checkpoint", b) }

// CreateZeroPartial is the only payload mutation in this first slice. A failed
// write/Sync/close leaves the entry intact for exact inspection on restart.
func (n *NativeCopy) CreateZeroPartial() (result Identity, resultErr error) {
	if n.control == nil {
		return Identity{}, ErrNativeCopyChanged
	}
	if err := n.control.check(); err != nil {
		return Identity{}, err
	}
	if err := n.Revalidate(); err != nil {
		return Identity{}, err
	}
	f, err := OpenAt(n.dir, "payload.partial", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Identity{}, err
	}
	defer func() {
		if e := f.Close(); e != nil {
			resultErr = errors.Join(resultErr, e, ErrNativeCopyUncertain)
		}
	}()
	id, err := privateRegular(f)
	if err != nil || id.Size != 0 {
		return Identity{}, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := f.Sync(); err != nil {
		return Identity{}, errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := n.dir.Sync(); err != nil {
		return Identity{}, errors.Join(err, ErrNativeCopyUncertain)
	}
	return id, nil
}
func (n *NativeCopy) PartialIdentity() (_ Identity, resultErr error) {
	if err := n.Revalidate(); err != nil {
		return Identity{}, err
	}
	f, err := OpenAt(n.dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	id, err := privateRegular(f)
	if err != nil {
		return Identity{}, err
	}
	check, err := OpenAt(n.dir, "payload.partial", os.O_RDONLY, 0)
	if err != nil {
		return Identity{}, err
	}
	checkID, checkErr := privateRegular(check)
	closeErr := check.Close()
	if checkErr != nil || closeErr != nil || checkID != id {
		return Identity{}, errors.Join(checkErr, closeErr, ErrNativeCopyChanged)
	}
	return id, nil
}
func (n *NativeCopy) ReadCheckpoint() ([]byte, error) {
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	return readNativeRecord(n.dir, "checkpoint")
}
func (n *NativeCopy) ReadIntent() ([]byte, error) {
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	return readNativeRecord(n.dir, "intent")
}
func (n *NativeCopy) ReadSelection() ([]byte, error) {
	if err := n.Revalidate(); err != nil {
		return nil, err
	}
	return readNativeRecord(n.dir, "selection")
}

func (n *NativeCopy) syncLeaf(leaf string) (resultErr error) {
	if n.control == nil {
		return ErrNativeCopyChanged
	}
	if err := n.control.check(); err != nil {
		return err
	}
	if err := n.Revalidate(); err != nil {
		return err
	}
	f, err := OpenAt(n.dir, leaf, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	id, err := privateRegular(f)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if err := n.dir.Sync(); err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	if after, err := privateRegular(f); err != nil || after != id {
		return errors.Join(err, ErrNativeCopyChanged)
	}
	return nil
}
func (n *NativeCopy) SyncIntent() error { return n.syncLeaf("intent.json") }

// SyncNamespace repeats the staging-parent barrier before intent acknowledgment.
// A prior successful Mkdirat with an uncertain parent Sync is never assumed durable.
func (n *NativeCopy) SyncNamespace() error {
	if n.control == nil {
		return ErrNativeCopyChanged
	}
	if err := n.control.check(); err != nil {
		return err
	}
	if err := n.Revalidate(); err != nil {
		return err
	}
	err := n.control.syncStaging()
	if err != nil {
		return errors.Join(err, ErrNativeCopyUncertain)
	}
	return nil
}
func (n *NativeCopy) SyncSelection() error         { return n.syncLeaf("selection.json") }
func (n *NativeCopy) SyncInitialCheckpoint() error { return n.syncLeaf("checkpoint.json") }
func (n *NativeCopy) SyncPartial() error           { return n.syncLeaf("payload.partial") }
