package nfs

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// HOW THE DRAINER PUBLISHES WITHOUT DESTROYING ANYTHING IT DID NOT CREATE (H-8).
//
// The drainer used to open the final pathname with os.Create and remove that
// pathname on every failure or cancel. A removal could delete a replacement
// written by another Mac, the farm or a later save.
//
// The drain now splits on whether the destination exists:
//
//   - EXISTS (a save of an existing file): open it in place with O_TRUNC, exactly
//     as before. The backend inode is kept, so NFS handles, pooled descriptors,
//     the SSD cache and ClipLogger's inode-primary identity stay valid.
//   - ABSENT (a new name): copy into a hidden stage created with O_EXCL, then
//     install it with one no-replace rename. If the name appears first, the
//     bytes become a conflict copy beside it; nothing is overwritten.
//   - REPLACE (F1: an atomic save renamed a spool-only temp over an existing
//     file, base "replace"): copy into a stage, then install it with one
//     REPLACING rename, but only while the destination is still the exact
//     file the save replaced (inode, mtime ns, length). Other Macs see the old
//     file until the new one takes the name; never a missing name. If the
//     destination changed, the bytes become a conflict copy. The inode
//     changes, as it already did on every atomic save (the temp's), so the
//     c28f1f0 objection to replace-by-rename (in-place edits) does not apply.
//
// In both cases the claim recheck and the step that touches the public name
// (the in-place open or the rename) run inside the namespace lease, so a REMOVE
// cannot slip between "row still ours" and "name written". Failure and cancel
// cleanup never remove the destination; they remove only a stage, and only if
// an Lstat proves it is still the inode this drain created.

// drainStage is a file this drain created on the backend.
type drainStage struct {
	path string
	dev  uint64
	ino  uint64
	f    *os.File
	// replace is set when the stage must take the name from an existing
	// file (base "replace"): the identity that file must still have.
	replace *replaceExpect
}

// replaceExpect is the destination identity a replacing install requires.
type replaceExpect struct {
	ino     uint64
	mtimeNs int64
	size    int64
}

func (r *replaceExpect) matches(st *unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && uint64(st.Ino) == r.ino &&
		st.Mtim.Nano() == r.mtimeNs && st.Size == r.size
}

var drainStageSeq struct {
	sync.Mutex
	n uint64
}

// drainStagePrefix is the drainer's stage name prefix. It extends the stream
// partial prefix so the mirror, farm and readdir filters already hide it.
const drainStagePrefix = streamTempPrefix + "drain-"

// drainStageName matches exactly the names drainStagePath produces:
// prefix + spool instance ID (16 hex) + "-" + rowID + "-" + creation UnixNano
// (hex) + "-" + sequence + "-" + base.
var drainStageName = regexp.MustCompile(`^` + regexp.QuoteMeta(drainStagePrefix) + `([0-9a-f]{16})-([0-9]+)-([0-9a-f]+)-([0-9]+)-`)

// The spool instance ID makes stage names unique across Macs that share a
// volume: row IDs are per-spool AUTOINCREMENT and collide between machines.
// It is persisted in the spool root so the boot sweep recognises the stages
// of THIS spool across restarts and nothing else.
//
// The spool root is copied verbatim by Migration Assistant, a Time Machine
// restore onto another Mac, or a cloned disk. Two Macs would then share one
// ID, and either Mac's boot sweep could delete the other's in-flight stages
// on the shared volume. So the ID is stored with the internal/deviceid digest
// of the Mac that created it (drainInstanceDeviceFile):
//   - digest matches: keep the ID;
//   - no digest stored (upgrade from a build without the binding): bind this
//     Mac's digest once and keep the ID;
//   - digest differs (clone): first hold every row already in the spool
//     (metadata.SpoolCloneHoldReason): they are the other Mac's pending
//     writes, and draining the copy would upload stale bytes over that Mac's
//     later edits. Only then mint a NEW ID, so this Mac's sweep never matches
//     a stage the other Mac created. Held rows wait for an explicit user
//     decision (ResolveCopiedSpoolRows); rows written here afterwards drain
//     normally;
//   - no fingerprint available (non-darwin, or IOKit refused): unbound and
//     unverified, as before. Nothing is held or rotated.
const (
	drainInstanceFile       = ".drain-instance-id"
	drainInstanceDeviceFile = ".drain-instance-device"
)

var drainInstanceRe = regexp.MustCompile(`^[0-9a-f]{16}$`)

// loadDrainInstanceID is loadDrainInstance with no clone hook.
func loadDrainInstanceID(spoolRoot string) (id string, persisted bool) {
	id, persisted, _ = loadDrainInstance(spoolRoot, nil)
	return id, persisted
}

// loadDrainInstance returns the persisted spool instance ID, creating it on
// first use or after a device mismatch. persisted=false means the ID (or its
// device binding) could not be stored: stages still get a unique random ID,
// but the sweep must not delete anything.
//
// On a mismatch onClone runs BEFORE the new ID is persisted. If it fails, the
// copied ID is left on disk (so the next start retries the hold) and the
// error is returned: the caller must not drain.
func loadDrainInstance(spoolRoot string, onClone func() error) (id string, persisted bool, err error) {
	if spoolRoot == "" {
		return newDrainInstanceID(), false, nil
	}
	p := filepath.Join(spoolRoot, drainInstanceFile)
	devPath := filepath.Join(spoolRoot, drainInstanceDeviceFile)
	device, devErr := deviceid.Fingerprint()
	if b, err := os.ReadFile(p); err == nil {
		if v := strings.TrimSpace(string(b)); drainInstanceRe.MatchString(v) {
			if devErr != nil {
				return v, true, nil // cannot verify: keep, as before the binding
			}
			bound := ""
			if db, err := os.ReadFile(devPath); err == nil {
				bound = strings.TrimSpace(string(db))
			}
			switch bound {
			case device:
				return v, true, nil
			case "":
				return v, writeDrainInstanceFile(devPath, device) == nil, nil
			}
			jmlog.Warn("drain: spool was created on another Mac (migrated or cloned); holding its rows and minting a new instance ID",
				"previous", v)
			if onClone != nil {
				if err := onClone(); err != nil {
					return newDrainInstanceID(), false, fmt.Errorf("spool copied from another Mac: hold its rows: %w", err)
				}
			}
		}
	}
	id = newDrainInstanceID()
	// The ID first, then its binding: a crash in between leaves a new ID
	// with a stale binding, which rotates again (the hold is idempotent),
	// never the copied ID with this Mac's binding.
	if writeDrainInstanceFile(p, id) != nil {
		return id, false, nil
	}
	if devErr == nil && writeDrainInstanceFile(devPath, device) != nil {
		return id, false, nil
	}
	return id, true, nil
}

// bindSpoolInstance loads (and if needed rotates) the spool instance ID at
// construction, before boot recovery can reset copied rows to ready. A clone
// whose rows cannot be held fails construction: no drainer, nothing drains.
func (d *Drainer) bindSpoolInstance() error {
	var err error
	d.instanceOnce.Do(func() {
		root := ""
		if d.spool != nil {
			root = d.spool.root
		}
		hold := func() error {
			if d.spool == nil || d.spool.Meta() == nil {
				return nil
			}
			n, err := d.spool.Meta().HoldRowsCopiedFromOtherMac()
			if err == nil && n > 0 {
				jmlog.Warn("drain: holding spool rows copied from another Mac until the user uploads or discards them", "rows", n)
			}
			return err
		}
		d.instance, d.instancePersisted, err = loadDrainInstance(root, hold)
	})
	return err
}

// CopiedSpoolRows returns the rows held because this spool was copied from
// another Mac. They still count as pending (PendingStats, /spool).
func (d *Drainer) CopiedSpoolRows() ([]*metadata.SpoolRow, error) {
	if d == nil || d.spool == nil {
		return nil, nil
	}
	return d.spool.Meta().ListCloneHeld()
}

// ResolveCopiedSpoolRows applies the user's decision for rows copied from
// another Mac. upload=true releases them to drain under this Mac's identity
// (each still meets the normal no-clobber checks). upload=false deletes the
// rows and their local spool files; rows under a completion hold are kept.
func (d *Drainer) ResolveCopiedSpoolRows(upload bool) (int, error) {
	if d == nil || d.spool == nil {
		return 0, nil
	}
	if upload {
		n, err := d.spool.Meta().ReleaseCloneHeld()
		if err != nil {
			return 0, err
		}
		if n > 0 {
			d.wakeNonBlocking()
		}
		return int(n), nil
	}
	rows, err := d.spool.Meta().DeleteCloneHeld()
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		if r.SpoolFile != "" {
			if err := os.Remove(r.SpoolFile); err != nil && !os.IsNotExist(err) {
				jmlog.Warn("drain: discard copied spool file", "path", r.NFSPath, "error", err.Error())
			}
		}
		switch r.DrainState {
		case metadata.DrainWriting, metadata.DrainReady, metadata.DrainDraining:
			d.spool.releaseCapacity(r.Size) // failed rows were released already
		}
	}
	jmlog.Warn("drain: discarded spool rows copied from another Mac", "rows", len(rows))
	return len(rows), nil
}

// writeDrainInstanceFile replaces path with value through a synced temp file.
func writeDrainInstanceFile(path, value string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(value+"\n"), 0o600); err != nil {
		return err
	}
	if f, err := os.Open(tmp); err == nil {
		_ = f.Sync()
		f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func newDrainInstanceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		binaryNow := uint64(time.Now().UnixNano())
		for i := range b {
			b[i] = byte(binaryNow >> (8 * i))
		}
	}
	return hex.EncodeToString(b[:])
}

// instanceID returns this drainer's spool instance ID, loading it once.
func (d *Drainer) instanceID() (string, bool) {
	_ = d.bindSpoolInstance() // normally already done by NewDrainer
	return d.instance, d.instancePersisted
}

// drainStagePath returns a unique hidden sibling for dest. The user name is
// truncated so the stage never exceeds the 255-byte component limit.
func drainStagePath(dest, instance string, rowID int64) (string, error) {
	base := filepath.Base(dest)
	if dest == "" || base == "." || base == string(filepath.Separator) {
		return "", fmt.Errorf("drain stage path: %q has no filename component", dest)
	}
	drainStageSeq.Lock()
	drainStageSeq.n++
	seq := drainStageSeq.n
	drainStageSeq.Unlock()
	if !drainInstanceRe.MatchString(instance) {
		return "", fmt.Errorf("drain stage path: invalid instance ID %q", instance)
	}
	prefix := fmt.Sprintf("%s%s-%d-%x-%d-", drainStagePrefix, instance, rowID, time.Now().UnixNano(), seq)
	room := 255 - len(prefix)
	if room < 0 {
		room = 0
	}
	if len(base) > room {
		base = base[:room]
		for len(base) > 0 && !utf8.ValidString(base) {
			base = base[:len(base)-1]
		}
	}
	return filepath.Join(filepath.Dir(dest), prefix+base), nil
}

// createDrainStage creates a new stage file. O_EXCL guarantees the drain never
// opens a file that already existed.
func createDrainStage(dest, instance string, rowID int64) (*drainStage, error) {
	path, err := drainStagePath(dest, instance, rowID)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return nil, err
	}
	st := &drainStage{path: path, f: f}
	if fi, err := f.Stat(); err == nil {
		if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
			st.dev, st.ino = uint64(sys.Dev), uint64(sys.Ino)
		}
	}
	return st, nil
}

// discard removes the stage only if the pathname still names the inode this
// drain created. It never touches the destination pathname.
func (s *drainStage) discard() {
	if s == nil || s.path == "" {
		return
	}
	fi, err := os.Lstat(s.path)
	if err != nil {
		return
	}
	sys, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || s.ino == 0 || uint64(sys.Dev) != s.dev || uint64(sys.Ino) != s.ino {
		return
	}
	_ = os.Remove(s.path)
}

// drainTarget is where a drain writes: the destination in place, or a stage.
type drainTarget struct {
	f     *os.File
	path  string      // the pathname the bytes are written through
	stage *drainStage // nil when writing the existing destination in place
	// conflict: the destination holds a file this row must not replace
	// (H-8B base mismatch). The stage is published beside it as a conflict
	// copy instead of at the destination.
	conflict string
}

// abandon releases a failed or canceled drain. It removes only a stage. A
// partially rewritten in-place destination is left for the retry to rewrite:
// removing it could delete a replacement that took the name meanwhile.
func (t *drainTarget) abandon() {
	if t != nil && t.stage != nil {
		t.stage.discard()
	}
}

// Test seams. Production uses the platform no-replace rename.
var (
	drainRenameNoReplace = renameStreamNoReplace
	// drainRenameReplace is the replacing install of a "replace" row.
	drainRenameReplace = os.Rename
	// drainPreReplaceHook runs inside the install lease, after the claim
	// recheck and before the destination identity check. Test-only.
	drainPreReplaceHook func(dest string)
	drainConflictHost   = defaultConflictHost
	drainConflictNow    = time.Now
	drainGeteuid        = os.Geteuid
	// drainOwnReadOnlyHook runs between the chmod and the reopen. Test-only.
	drainOwnReadOnlyHook func(dest string)
	// drainPreInstallHook runs inside the install lease, before the claim
	// recheck. Test-only; nil in production.
	drainPreInstallHook func(*metadata.SpoolRow)
	// drainPreOpenHook runs inside the open lease, before the claim recheck.
	// Test-only; nil in production.
	drainPreOpenHook func(*metadata.SpoolRow)
	// drainCheckedHook runs after the base decision allowed an in-place
	// rewrite and before the destination is opened. Test-only.
	drainCheckedHook func(dest string)
)

func defaultConflictHost() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown-host"
	}
	h = strings.TrimSuffix(h, ".local")
	return strings.NewReplacer("/", "-", ":", "-").Replace(h)
}

// conflictCopyPath returns "<name> (conflict <host> <date>)<ext>" beside dest.
// n > 1 adds a counter for the rare same-second collision.
func conflictCopyPath(dest, host string, now time.Time, n int) string {
	dir, base := filepath.Split(dest)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	if strings.HasPrefix(base, ".") && stem == "" {
		stem, ext = base, ""
	}
	stamp := now.Format("2006-01-02 150405")
	tag := fmt.Sprintf(" (conflict %s %s)", host, stamp)
	if n > 1 {
		tag = fmt.Sprintf(" (conflict %s %s %d)", host, stamp, n)
	}
	return filepath.Join(dir, stem+tag+ext)
}

// conflictCopyMarker is in the last_error of every row whose bytes were
// published as a conflict copy. Such a row is finished: RetryFailed skips it,
// or a retry would publish a duplicate copy.
const conflictCopyMarker = "published as conflict copy"

func isConflictCopyFailure(lastError string) bool {
	return strings.Contains(lastError, conflictCopyMarker)
}

// installConflictCopy publishes the stage beside dest with a no-replace rename.
func installConflictCopy(stage *drainStage, dest string) (string, error) {
	host, now := drainConflictHost(), drainConflictNow()
	var last error
	for n := 1; n <= 9; n++ {
		target := conflictCopyPath(dest, host, now, n)
		err := drainRenameNoReplace(stage.path, target)
		if err == nil {
			return target, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		last = err
	}
	return "", fmt.Errorf("drain install: no free conflict name for %q: %w", dest, last)
}

// beginInstallLease takes the same namespace lease the completion path takes.
// The coordinator never blocks: an overlapping foreground operation makes it
// return nil, and the row goes back to ready for a later attempt. No mutex is
// held while the lease is owned, so holding it across a syscall cannot
// deadlock; overlapping RPCs see JUKEBOX and retry, as they do during
// completion.
func (d *Drainer) beginInstallLease(row *metadata.SpoolRow) (*namespaceOperation, bool) {
	if d.onDrainAdmission == nil {
		return nil, true
	}
	op := d.onDrainAdmission(row.NFSPath)
	if op == nil {
		if err := d.spool.Meta().ResetToReady(row.ID); err != nil {
			log.Printf("drainer: install lease refused row %d; requeue failed: %v", row.ID, err)
		}
		d.scheduleDelayedWake(d.backoffDuration(1))
		return nil, false
	}
	return op, true
}

// drainStillClaimed reports whether row is still this worker's claimed drain.
// A REMOVE or replacement cancels the row (DeleteActiveByPath deletes draining
// rows), and a canceled drain must not write the name.
func (d *Drainer) drainStillClaimed(row *metadata.SpoolRow) (bool, error) {
	cur, err := d.spool.Meta().Get(row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return cur.RetiredBy == 0 && cur.DrainState == metadata.DrainDraining && cur.NFSPath == row.NFSPath &&
		cur.SpoolFile == row.SpoolFile, nil
}

// openDrainTarget decides in-place, new name or conflict copy under the
// install lease. It returns ok=false when it has already disposed of the row
// (requeue, cancel, hold or failure).
//
// H-8B: the decision compares the destination with the base identity the row
// recorded when the write opened (see drainBaseAllowsInPlace). A file the
// row did not expect is never truncated: the bytes go to a stage and publish
// beside it as a conflict copy.
func (d *Drainer) openDrainTarget(row *metadata.SpoolRow, dest string, lease leaseDecision) (*drainTarget, bool) {
	base, accepted := d.rowDrainBase(row)
	tainted := lease.taints(accepted)
	op, ok := d.beginInstallLease(row)
	if !ok {
		return nil, false
	}
	if drainPreOpenHook != nil {
		drainPreOpenHook(row)
	}
	claimed, err := d.drainStillClaimed(row)
	if err != nil || !claimed {
		if op != nil {
			op.finish(nil)
		}
		if err != nil {
			d.failTransient(row, fmt.Errorf("recheck drain claim: %w", err))
		} else {
			log.Printf("drainer: row %d (%s) canceled before open — destination untouched", row.ID, row.NFSPath)
		}
		return nil, false
	}
	var st unix.Stat_t
	lerr := unix.Lstat(dest, &st)
	conflict := ""
	exactBaseRefusal := false
	var replace *replaceExpect
	var f *os.File
	switch {
	case lerr == nil && st.Mode&unix.S_IFMT == unix.S_IFREG && base.Kind == metadata.SpoolBaseReplace:
		// F1: never truncate the file an atomic save replaces. Stage, and
		// take the name with a replacing rename at install.
		cur := &replaceExpect{ino: uint64(st.Ino), mtimeNs: st.Mtim.Nano(), size: st.Size}
		switch {
		case tainted:
			// A peer can rewrite the same inode/length and restore mtime.
			// This tuple cannot override known loss of writer continuity.
			conflict = "another Mac held the project after this atomic save was accepted"
		case cur.ino == base.Ino && cur.mtimeNs == base.MtimeNs && cur.size == base.Len:
			replace = cur
		case d.olderRowInstalledDest(row, dest):
			replace = cur // an unfinished older row of ours put it there
		default:
			conflict = "the file this save replaces changed since the save"
		}
	case lerr == nil && st.Mode&unix.S_IFMT == unix.S_IFREG:
		inPlace, why := drainBaseAllowsInPlace(base, &st, tainted)
		if !inPlace && base.Kind == metadata.SpoolBaseAbsent && d.olderRowInstalledDest(row, dest) {
			// An unfinished older row of ours created the name: rewrite it in
			// place to keep save order (same rule as installNewName).
			inPlace, why = true, ""
		}
		if !inPlace {
			exactBaseRefusal = base.Kind == metadata.SpoolBasePresent
			conflict = why
			break
		}
		// Claim the inode after the open proved it and BEFORE the first byte
		// changes, so a retry after a partial rewrite recognises its own
		// file (a partial write changes mtime, ctime and length).
		claim := func() error { return d.spool.Meta().ClaimDrainBase(row.ID, uint64(st.Ino)) }
		if drainCheckedHook != nil {
			drainCheckedHook(dest)
		}
		f, err = openExistingInPlace(dest, uint64(st.Ino), claim)
		if errors.Is(err, os.ErrPermission) {
			// Our own published file may carry its final read-only mode (a
			// Finder sidecar chmod'd 0444 by the drain effects). Rewrite it in
			// place; never remove it. Another owner's read-only file still fails.
			if cerr := claim(); cerr != nil {
				err = cerr
			} else {
				f, err = openOwnReadOnlyInPlace(dest, err)
			}
		}
	case lerr == nil:
		// A directory or symlink at the name: refuse exactly as the old
		// O_NOFOLLOW in-place open did.
		f, err = os.OpenFile(dest, os.O_RDWR|syscall.O_NOFOLLOW, 0)
		if err == nil {
			f.Close()
			err = fmt.Errorf("destination %q is not a regular file", row.NFSPath)
		}
	default:
		err = lerr
	}
	if err == nil && f != nil && op != nil {
		op.mutated = true
	}
	if op != nil {
		op.finish(nil)
	}
	if exactBaseRefusal {
		logExactBaseRefusal(row, base, &st, accepted, lease, conflict)
	}
	if conflict == "" && err == nil && f != nil {
		d.recordInstalled(row, f)
		return &drainTarget{f: f, path: dest}, true
	}
	if conflict == "" && replace == nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, unix.ENOENT) {
		d.failTransient(row, fmt.Errorf("open existing dest: %w", err))
		return nil, false
	}
	instance, _ := d.instanceID()
	stage, err := createDrainStage(dest, instance, row.ID)
	if err != nil {
		d.failTransient(row, fmt.Errorf("create drain stage: %w", err))
		return nil, false
	}
	if conflict != "" {
		log.Printf("drainer: row %d (%s) will publish a conflict copy: %s", row.ID, row.NFSPath, conflict)
	}
	stage.replace = replace
	return &drainTarget{f: stage.f, path: stage.path, stage: stage, conflict: conflict}, true
}

// openExistingInPlace opens the destination for rewrite WITHOUT O_TRUNC,
// proves it is still the inode the decision examined, runs beforeTruncate,
// then truncates through the descriptor. A file swapped in after the Lstat
// is never truncated.
func openExistingInPlace(dest string, ino uint64, beforeTruncate func() error) (*os.File, error) {
	f, err := os.OpenFile(dest, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		f.Close()
		return nil, err
	}
	if uint64(st.Ino) != ino {
		f.Close()
		return nil, fmt.Errorf("destination replaced between check and open (inode %d -> %d)", ino, st.Ino)
	}
	if beforeTruncate != nil {
		if err := beforeTruncate(); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// drainBaseAllowsInPlace decides whether a row may rewrite the regular file
// st found at its destination. why explains a refusal.
//
//	unknown  legacy row: in place, as before H-8B, unless a foreign holder
//	         had the project since the row was accepted
//	absent   the row expected no file here: never in place
//	present  exact identity (inode, mtime ns, ctime ns, length) only
//	claimed  same inode (this row already wrote it), and no foreign holder
//	moved    same inode, mtime ns and length, and no foreign holder
//	         (a rename resets ctime)
//	weak     same length and whole-second mtime, and no foreign holder
//
// A foreign holder ("tainted") cannot make an exact present match unsafe:
// identical ctime proves nobody wrote the inode since this Mac saw it.
func drainBaseAllowsInPlace(base metadata.SpoolBase, st *unix.Stat_t, tainted bool) (bool, string) {
	switch base.Kind {
	case metadata.SpoolBaseUnknown:
		if tainted {
			return false, "row predates base tracking and another Mac held the project since"
		}
		return true, ""
	case metadata.SpoolBaseAbsent:
		return false, "a file appeared at a name that was new when the write opened"
	case metadata.SpoolBaseAtomicConflict:
		return false, "atomic save could not verify the previous destination"
	case metadata.SpoolBasePresent:
		if uint64(st.Ino) == base.Ino && st.Mtim.Nano() == base.MtimeNs &&
			st.Ctim.Nano() == base.CtimeNs && st.Size == base.Len {
			return true, ""
		}
		return false, "the file changed since the write opened"
	case metadata.SpoolBaseClaimed:
		if uint64(st.Ino) != base.Ino {
			return false, "the file this drain started writing was replaced"
		}
		if tainted {
			return false, "another Mac held the project after this drain started"
		}
		return true, ""
	case metadata.SpoolBaseMoved:
		if tainted {
			return false, "another Mac held the project after this renamed save was accepted"
		}
		if uint64(st.Ino) == base.Ino && st.Mtim.Nano() == base.MtimeNs && st.Size == base.Len {
			return true, ""
		}
		return false, "the renamed file changed since the write opened"
	case metadata.SpoolBaseWeak:
		if st.Size == base.Len && st.Mtim.Sec == base.MtimeNs/1e9 && !tainted {
			return true, ""
		}
		return false, "the file changed since the write opened (offline base)"
	}
	return false, "unrecognised base identity"
}

// probeBase is the spool's base-identity probe (installed by NewDrainer). It
// runs once per NEW spool row, outside the OpenWrite shard lock.
//
//   - Derivative paths (.juicemount/...): UNKNOWN. They are governed by the
//     derivative clobber guard (deriv_clobber_guard.go), not by base identity.
//   - No mirror entry: ABSENT, with NO backend call. This is every new-name
//     CREATE (a card copy creates thousands), so it must stay at local-disk
//     speed and must not queue on nfsLstatGate behind Finder's navigation
//     Lstats. The price of a mirror miss over an existing backend file
//     (keyspace-push lag after another Mac wrote, a RAM eviction) is a
//     conflict copy beside that file, never an overwrite or a loss.
//   - A mirror entry, online: one bounded backend Lstat gives the exact base
//     (present), or absent if the file is gone. A timeout or error falls
//     back to the mirror's length and whole-second mtime (weak).
//   - A mirror entry, offline: weak.
func (d *Drainer) probeBase(nfsPath string) (metadata.SpoolBase, uint64) {
	leases := d.ProjectLeases()
	epoch := leases.EpochFor(nfsPath)
	leases.Touch(nfsPath)
	if isDerivativePath(nfsPath) {
		return metadata.SpoolBase{}, epoch
	}
	var e *metadata.Entry
	if store := d.spool.Meta().Owner(); store != nil {
		e = store.LookupByPath(nfsPath)
		if e == nil && strings.HasPrefix(nfsPath, "/") {
			e = store.LookupByPath(strings.TrimPrefix(nfsPath, "/"))
		}
	}
	if e == nil {
		return metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, epoch
	}
	if e.IsDir {
		return metadata.SpoolBase{}, epoch
	}
	weak := metadata.SpoolBase{Kind: metadata.SpoolBaseWeak, MtimeNs: e.Mtime.Unix() * 1e9, Len: e.Size}
	if pin.IsOffline() {
		return weak, epoch
	}
	st, err, ok := drainBaseLstat(filepath.Join(d.fuseRoot, nfsPath), drainBaseProbeTimeout)
	switch {
	case !ok:
		return weak, epoch
	case errors.Is(err, unix.ENOENT):
		return metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, epoch
	case err != nil:
		return weak, epoch
	case st.Mode&unix.S_IFMT != unix.S_IFREG:
		return metadata.SpoolBase{}, epoch
	}
	return metadata.SpoolBase{Kind: metadata.SpoolBasePresent, Ino: uint64(st.Ino),
		MtimeNs: st.Mtim.Nano(), CtimeNs: st.Ctim.Nano(), Len: st.Size}, epoch
}

// drainBaseLstat is the probe's backend Lstat. A test seam (call counting);
// production uses lstatUnixWithTimeout.
var drainBaseLstat = lstatUnixWithTimeout

// isDerivativePath reports whether p is under the derivative tree.
func isDerivativePath(p string) bool {
	p = strings.TrimPrefix(p, "/")
	return p == ".juicemount" || strings.HasPrefix(p, ".juicemount/")
}

// drainBaseProbeTimeout bounds the one backend Lstat a save over a file the
// mirror lists costs. On timeout the row records the mirror's weak base.
const drainBaseProbeTimeout = 2 * time.Second

// lstatUnixWithTimeout is lstatWithTimeout (handler.go) that keeps the errno
// and the raw stat (ctime, nanosecond times). ok=false
// means it did not return in time (the worker keeps its gate slot until the
// syscall returns, as lstatNotExistWithTimeout does).
func lstatUnixWithTimeout(p string, timeout time.Duration) (unix.Stat_t, error, bool) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	gateWait, depth, acquired := acquireFUSEGate(nfsLstatGate, timer)
	if !acquired {
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			0, gateWait, time.Since(start), metrics.FUSEOutcomeGateTimeout)
		return unix.Stat_t{}, nil, false
	}
	type result struct {
		st  unix.Stat_t
		err error
	}
	ch := make(chan result, 1)
	go func() {
		var st unix.Stat_t
		err := unix.Lstat(p, &st)
		ch <- result{st: st, err: err}
		<-nfsLstatGate
	}()
	select {
	case r := <-ch:
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeOK)
		return r.st, r.err, true
	case <-timer.C:
		metrics.Default().ObserveFUSECall(metrics.FUSESrcForeground, metrics.FUSEOpLstat, metrics.FUSEGateNFSLstat,
			depth, gateWait, time.Since(start), metrics.FUSEOutcomeTimeout)
		return unix.Stat_t{}, nil, false
	}
}

// publishConflictCopy installs a verified stage beside dest under the
// install lease. The row is then retired exactly like installNewName's
// conflict branch: failed with the published name, spool source retained.
func (d *Drainer) publishConflictCopy(row *metadata.SpoolRow, stage *drainStage, dest, why string) {
	op, ok := d.beginInstallLease(row)
	if !ok {
		stage.discard()
		return
	}
	finish := func() {
		if op != nil {
			op.finish(nil)
		}
	}
	claimed, err := d.drainStillClaimed(row)
	if err != nil || !claimed {
		finish()
		stage.discard()
		if err != nil {
			d.failTransient(row, fmt.Errorf("recheck drain claim: %w", err))
		} else {
			log.Printf("drainer: row %d (%s) canceled before conflict install — stage discarded", row.ID, row.NFSPath)
		}
		return
	}
	published, cerr := installConflictCopy(stage, dest)
	if cerr == nil && op != nil {
		op.mutated = true
	}
	finish()
	if cerr != nil {
		stage.discard()
		d.failTransient(row, fmt.Errorf("install conflict copy: %w", cerr))
		return
	}
	jmlog.Warn("drain: published a conflict copy instead of replacing a file this save did not expect",
		"path", row.NFSPath, "conflict_copy", filepath.Base(published), "reason", why)
	d.failPermanent(row, fmt.Sprintf("%s; %s %q", why, conflictCopyMarker, filepath.Base(published)))
}

// installNewName publishes a staged new file under the install lease. It
// returns ok=false when it has already disposed of the row.
func (d *Drainer) installNewName(row *metadata.SpoolRow, stage *drainStage, dest string) bool {
	op, ok := d.beginInstallLease(row)
	if !ok {
		stage.discard()
		return false
	}
	finish := func() {
		if op != nil {
			op.finish(nil)
		}
	}
	if drainPreInstallHook != nil {
		drainPreInstallHook(row)
	}
	claimed, err := d.drainStillClaimed(row)
	if err != nil || !claimed {
		finish()
		stage.discard()
		if err != nil {
			d.failTransient(row, fmt.Errorf("recheck drain claim: %w", err))
		} else {
			log.Printf("drainer: row %d (%s) canceled before install — stage discarded, destination untouched", row.ID, row.NFSPath)
		}
		return false
	}
	if stage.replace != nil {
		// F1: take the name from the exact file the save replaces, in one
		// replacing rename, under the same lease as the claim recheck. A
		// destination that is gone takes the no-replace path below (a REMOVE
		// meanwhile is the user's; the save then publishes as a new name).
		if drainPreReplaceHook != nil {
			drainPreReplaceHook(dest)
		}
		var st unix.Stat_t
		lerr := unix.Lstat(dest, &st)
		switch {
		case lerr == nil && stage.replace.matches(&st):
			err = drainRenameReplace(stage.path, dest)
		case lerr == nil:
			published, cerr := installConflictCopy(stage, dest)
			if cerr == nil && op != nil {
				op.mutated = true
			}
			finish()
			if cerr != nil {
				stage.discard()
				d.failTransient(row, fmt.Errorf("install conflict copy: %w", cerr))
				return false
			}
			why := "the file this save replaces changed while it uploaded"
			jmlog.Warn("drain: published a conflict copy instead of replacing a file this save did not expect",
				"path", row.NFSPath, "conflict_copy", filepath.Base(published), "reason", why)
			d.failPermanent(row, fmt.Sprintf("%s; %s %q", why, conflictCopyMarker, filepath.Base(published)))
			return false
		case errors.Is(lerr, unix.ENOENT):
			err = drainRenameNoReplace(stage.path, dest)
		default:
			finish()
			stage.discard()
			d.failTransient(row, fmt.Errorf("check replaced destination: %w", lerr))
			return false
		}
	} else {
		err = drainRenameNoReplace(stage.path, dest)
	}
	if err == nil {
		if op != nil {
			op.mutated = true
		}
		d.recordInstalledIdentity(row.ID, stage.dev, stage.ino)
		// H-8B: the name now holds THIS row's inode. A retry (a completion
		// that failed after the install) must rewrite it, not conflict-copy
		// it. Best-effort: on error the in-memory identity still covers this
		// process, and a restart errs toward a conflict copy.
		if cerr := d.spool.Meta().ClaimDrainBase(row.ID, stage.ino); cerr != nil {
			log.Printf("drainer: row %d claim installed inode: %v", row.ID, cerr)
		}
		finish()
		return true
	}
	if !errors.Is(err, os.ErrExist) {
		finish()
		stage.discard()
		d.failTransient(row, fmt.Errorf("install drain stage: %w", err))
		return false
	}
	// The name appeared after this drain opened its stage. If an UNFINISHED
	// older row of ours put exactly this inode there, retry: the retry finds
	// the name and rewrites it in place, preserving save order. Anything else
	// (another Mac, the farm, a finished row's leftovers) gets a conflict copy.
	//
	// Scope: this protects only the window between this drain's open and its
	// rename. A file another writer created BEFORE the drain opened is
	// rewritten in place exactly as on base (pre-existing H-8 gap; closing it
	// needs cross-writer leases or a recorded base identity per spool row).
	if d.olderRowInstalledDest(row, dest) {
		finish()
		stage.discard()
		d.failTransient(row, fmt.Errorf("install drain stage: %q appeared from an older local row; retrying in place", row.NFSPath))
		return false
	}
	published, cerr := installConflictCopy(stage, dest)
	finish()
	if cerr != nil {
		stage.discard()
		d.failTransient(row, fmt.Errorf("install conflict copy: %w", cerr))
		return false
	}
	// The bytes are safe beside dest. Completing the row would publish them at
	// row.NFSPath, so retain the spool source and stop.
	d.failPermanent(row, fmt.Sprintf("name created elsewhere while this new file drained; %s %q", conflictCopyMarker, filepath.Base(published)))
	return false
}

// recordInstalled remembers the backend identity a row wrote, so a later row
// can tell its own predecessor's file from another writer's.
func (d *Drainer) recordInstalled(row *metadata.SpoolRow, f *os.File) {
	if fi, err := f.Stat(); err == nil {
		if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
			d.recordInstalledIdentity(row.ID, uint64(sys.Dev), uint64(sys.Ino))
		}
	}
}

type installedIdentity struct {
	dev, ino uint64
	at       time.Time
}

func (d *Drainer) recordInstalledIdentity(rowID int64, dev, ino uint64) {
	if ino == 0 {
		return
	}
	d.installedMu.Lock()
	defer d.installedMu.Unlock()
	if d.installed == nil {
		d.installed = make(map[int64]installedIdentity)
	}
	if len(d.installed) >= 4096 {
		// Bounded: only rows still unfinished matter, and they are recent.
		cutoff := time.Now().Add(-time.Hour)
		for id, v := range d.installed {
			if v.at.Before(cutoff) {
				delete(d.installed, id)
			}
		}
	}
	d.installed[rowID] = installedIdentity{dev: dev, ino: ino, at: time.Now()}
}

// olderRowInstalledDest reports whether dest is the inode an UNFINISHED older
// row of this spool wrote in this process. Done and failed rows never count:
// a finished row's file may since have been replaced by another writer. The
// identity is in memory only, so after a restart the answer is false and the
// drain publishes a conflict copy (the safe direction).
func (d *Drainer) olderRowInstalledDest(row *metadata.SpoolRow, dest string) bool {
	fi, err := os.Lstat(dest)
	if err != nil {
		return false
	}
	sys, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.Mode().IsRegular() {
		return false
	}
	rows, err := d.spool.Meta().ListAll()
	if err != nil {
		return false
	}
	d.installedMu.Lock()
	defer d.installedMu.Unlock()
	for _, r := range rows {
		if r.ID >= row.ID || r.NFSPath != row.NFSPath {
			continue
		}
		switch r.DrainState {
		case metadata.DrainWriting, metadata.DrainReady, metadata.DrainDraining:
		default:
			continue
		}
		if v, ok := d.installed[r.ID]; ok && v.dev == uint64(sys.Dev) && v.ino == uint64(sys.Ino) {
			return true
		}
	}
	return false
}

// openOwnReadOnlyInPlace rewrites a read-only file that this process's user
// owns, keeping its inode. Sequence: Lstat (regular, our uid) -> add owner
// write without following a symlink -> reopen O_RDWR|O_NOFOLLOW -> fstat must
// equal the Lstat inode -> ftruncate -> restore the original mode on the
// descriptor.
// Any failure restores the mode by path, but only while the path still names
// the inode we changed. Nothing is removed.
func openOwnReadOnlyInPlace(dest string, permErr error) (*os.File, error) {
	fi, err := os.Lstat(dest)
	if err != nil || !fi.Mode().IsRegular() {
		return nil, permErr
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || int(st.Uid) != drainGeteuid() {
		return nil, permErr
	}
	orig := uint32(fi.Mode().Perm())
	if orig&0o200 != 0 {
		return nil, permErr // already owner-writable: the denial is not the mode
	}
	if err := chmodNoFollow(dest, orig|0o200); err != nil {
		return nil, permErr
	}
	restore := func() {
		if cur, err := os.Lstat(dest); err == nil {
			if cs, ok := cur.Sys().(*syscall.Stat_t); ok && cs.Dev == st.Dev && cs.Ino == st.Ino {
				_ = chmodNoFollow(dest, orig)
			}
		}
	}
	if drainOwnReadOnlyHook != nil {
		drainOwnReadOnlyHook(dest)
	}
	// Open WITHOUT O_TRUNC, prove it is the inode we changed, then truncate
	// through the descriptor: a file swapped in meanwhile is never truncated.
	f, err := os.OpenFile(dest, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		restore()
		return nil, err
	}
	got, err := f.Stat()
	gs, ok := statT(got, err)
	if !ok || gs.Dev != st.Dev || gs.Ino != st.Ino {
		f.Close()
		restore()
		return nil, permErr
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		restore()
		return nil, err
	}
	if err := f.Chmod(os.FileMode(orig)); err != nil {
		f.Close()
		restore()
		return nil, err
	}
	return f, nil
}

func statT(fi os.FileInfo, err error) (*syscall.Stat_t, bool) {
	if err != nil || fi == nil {
		return nil, false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return st, ok
}

// chmodNoFollow changes a mode without following a final symlink. Linux
// rejects AT_SYMLINK_NOFOLLOW for chmod; the caller has just Lstat'd a
// regular file there, so fall back to a plain chmod on that platform only.
func chmodNoFollow(path string, mode uint32) error {
	err := unix.Fchmodat(unix.AT_FDCWD, path, mode, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		return os.Chmod(path, os.FileMode(mode))
	}
	return err
}

// SweepDrainStages removes stages left by a crash. It deletes a file only when
// all of these hold:
//   - its name matches drainStageName exactly;
//   - its instance ID is THIS spool's persisted ID;
//   - its row ID is a row in THIS spool database whose path puts the stage in
//     that directory (IDs are AUTOINCREMENT, so they are never reused);
//   - the creation time in its name is before cutoff, so no drain started by
//     this process can own it;
//   - it is a regular file.
//
// Only parent directories of rows in this spool are listed. A stage for a row
// that was deleted before the crash is not found; it is hidden and inert.
func (d *Drainer) SweepDrainStages(cutoff time.Time) (removed int, err error) {
	if d == nil || d.spool == nil || d.fuseRoot == "" {
		return 0, nil
	}
	instance, persisted := d.instanceID()
	if !persisted {
		return 0, fmt.Errorf("drain stage sweep: spool instance ID not persisted; sweep skipped")
	}
	rows, err := d.spool.Meta().ListAll()
	if err != nil {
		return 0, fmt.Errorf("drain stage sweep: list rows: %w", err)
	}
	byDir := make(map[string]map[int64]bool)
	for _, r := range rows {
		if r == nil || r.NFSPath == "" {
			continue
		}
		dir := filepath.Dir(filepath.Join(d.fuseRoot, r.NFSPath))
		if byDir[dir] == nil {
			byDir[dir] = make(map[int64]bool)
		}
		byDir[dir][r.ID] = true
	}
	for dir, ids := range byDir {
		select {
		case <-d.stop:
			return removed, nil
		default:
		}
		ents, rerr := os.ReadDir(dir)
		if rerr != nil {
			continue
		}
		for _, e := range ents {
			m := drainStageName.FindStringSubmatch(e.Name())
			if m == nil {
				continue
			}
			if m[1] != instance {
				continue // another spool's stage (another Mac sharing the volume)
			}
			id, perr := strconv.ParseInt(m[2], 10, 64)
			if perr != nil || !ids[id] {
				continue
			}
			nanos, perr := strconv.ParseInt(m[3], 16, 64)
			if perr != nil || !time.Unix(0, nanos).Before(cutoff) {
				continue
			}
			p := filepath.Join(dir, e.Name())
			fi, lerr := os.Lstat(p)
			if lerr != nil || !fi.Mode().IsRegular() {
				continue
			}
			if os.Remove(p) == nil {
				removed++
			}
		}
	}
	return removed, nil
}

// A DRAIN THAT ALREADY CHANGED THE SERVER COPY IS PAUSED, NOT FAILED (F2).
//
// The in-place branch truncates the server file before it copies the new
// bytes (c28f1f0 keeps the inode, so there is no stage to fall back on). If a
// later step fails (copy, fsync, at-rest re-read: a refused storage key gives
// EIO on every attempt), the server copy is empty or partial. Spending the
// five-attempt budget then marked the row failed within about two minutes,
// evicted the spool shadow and left every Mac reading the damaged server
// copy, with nothing retrying it.
//
// A row whose recorded base is "claimed" has started writing the server copy
// (ClaimDrainBase runs before the truncate). A row whose base is "replace"
// (F1) is an atomic save whose old server copy the rename kept; if it failed
// on budget, the spool shadow would be evicted and this Mac would serve the
// previous version as current. For either row a retryable
// failure is a pause: the row goes back to ready with the reason below, the
// attempt counter still counts, but the attempt budget no longer ends it.
// Only time does: after drainPauseCap of failing retries the row is marked
// failed with a loud log, and the spool file is still kept for RetryFailed.
//
// The pause start is persisted inside last_error so it survives restarts.

// drainPausedSince is the machine-readable part of a paused row's last_error.
const drainPausedSince = "paused since "

var drainPausedRe = regexp.MustCompile(`\(` + drainPausedSince + `([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)\)`)

// serverIncompleteMarker is the status text for a paused in-place drain.
const serverIncompleteMarker = "server copy incomplete — will retry"

// serverPreviousMarker is the status text for a paused atomic-save replace
// (F1). The server still holds the version this save replaces; this Mac keeps
// serving the saved bytes from the spool while the row is paused, so a failed
// upload never makes this Mac read the previous version as if it were current
// (the 4b25448 concern). Only after drainPauseCap does the row fail, and its
// last_error then says the server has the previous version.
const serverPreviousMarker = "server still has the previous version — will retry"

var (
	// drainPauseCap bounds how long a paused row keeps retrying.
	drainPauseCap = 24 * time.Hour
	// drainPauseBackoff is the retry delay once a paused row has spent the
	// normal attempt budget. Each attempt rewrites the whole file, so a long
	// outage must not upload it every 30 s.
	drainPauseBackoff = 2 * time.Minute
	// drainPauseNow is the clock for the pause cap. Test seam.
	drainPauseNow = time.Now
)

// isDrainPaused reports whether lastError marks a paused row.
func isDrainPaused(lastError string) bool { return drainPausedRe.MatchString(lastError) }

// drainPausedAt returns the pause start recorded in lastError.
func drainPausedAt(lastError string) (time.Time, bool) {
	m := drainPausedRe.FindStringSubmatch(lastError)
	if m == nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, m[1])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// drainPauseMarker returns the status text for a row whose failure must not
// spend the attempt budget, or "" for an ordinary row.
func drainPauseMarker(base metadata.SpoolBase) string {
	switch base.Kind {
	case metadata.SpoolBaseClaimed:
		return serverIncompleteMarker
	case metadata.SpoolBaseReplace, metadata.SpoolBaseAtomicConflict:
		return serverPreviousMarker
	}
	return ""
}

// pauseIfServerCopyAtRisk handles a retryable failure of a row that has
// already changed the server copy. It returns true when it disposed of the
// row (paused it, or failed it at the time cap).
func (d *Drainer) pauseIfServerCopyAtRisk(row *metadata.SpoolRow, err error) bool {
	base, _ := d.rowDrainBase(row)
	marker := drainPauseMarker(base)
	if marker == "" {
		return false
	}
	now := drainPauseNow().UTC()
	since, paused := row.PauseSince, !row.PauseSince.IsZero()
	if !paused {
		since = now
	}
	if now.Sub(since) >= drainPauseCap {
		jmlog.Error("drain: GIVING UP on a file whose server copy this Mac already changed; the server copy is not the saved version",
			"path", row.NFSPath, "paused_since", since.Format(time.RFC3339), "cap", drainPauseCap.String(),
			"attempts", row.DrainAttempts+1, "error", err.Error())
		d.failPermanent(row, fmt.Sprintf("%s for %s, giving up (the full file is kept on this Mac; use Retry): %v",
			strings.TrimSuffix(marker, " — will retry"), drainPauseCap, err))
		return true
	}
	if !paused {
		jmlog.Warn("drain: server copy changed and the upload failed; pausing the file instead of failing it",
			"path", row.NFSPath, "status", marker, "error", err.Error())
	}
	reason := fmt.Sprintf("%s (%s%s): %v", marker, drainPausedSince, since.UTC().Format(time.RFC3339), err)
	next := row.DrainAttempts + 1
	delay := d.backoffDuration(next)
	if next >= d.maxAttempts {
		delay = drainPauseBackoff
	}
	if rErr := d.spool.MarkDrainRetry(row.ID, reason, since, now.Add(delay)); rErr != nil {
		log.Printf("drainer: pause row %d: %v", row.ID, rErr)
	}
	d.metrics.DrainsRetried.Add(1)
	d.scheduleDelayedWake(delay)
	return true
}
