package nfs

// Direct publish of small new files (JuiceFS patch 0025, JMCreateFiles).
//
// The stage path publishes a new file with create, write, fsync, utimes and a
// no-replace rename of a private stage in the destination folder. On a 200 ms
// link that is about 16 metadata round trips per file, and 7 of them run under
// the kernel's lock on the folder (the forced lookup and the create of the
// stage, the forced lookup of the target and the rename), so saves into one
// folder reach the server one every 1.6-1.9 s however many workers run.
//
// A small file (at most 1 MiB) whose name is free is instead sent whole to
// juicefs in a JMCreateFiles control message. juicefs uploads the bytes, then
// adds the name with its data, length, mode and mtime in one metadata
// transaction (2 round trips, no kernel folder lock). The name never appears
// empty or partial, as with the stage: a crash before the transaction leaves
// only an unreferenced object, and the spool row is still ready.
//
// Kept from the stage path: the lease gate and the install lease with the
// claim recheck, the base decision (only an absent name takes this path), the
// spool SHA check, a no-replace publish (EEXIST: retry in place for our own
// older row, otherwise a conflict copy beside it), the installed-identity
// record and ClaimDrainBase, the FUSE identity of the published file, the
// optional at-rest re-read, the mount-device check, and the same completion.
//
// JM_DRAIN_DIRECT_CREATE=0 turns it off (the stage path for every file). A
// juicefs without the message answers ENOTSUP once, and the path stays off.

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
)

func drainDirectEnabled() bool { return os.Getenv("JM_DRAIN_DIRECT_CREATE") != "0" }

const (
	// directMaxBytes: a small save, as the small-upload lane (patch 0024).
	directMaxBytes = jfsCreateFileMaxBytes
	// One message carries at most this many files and bytes. On macOS juicefs
	// keeps one control handle per process, so messages run one at a time.
	directBatchFiles = 32
	// 2 MiB: the control lock is held for the whole exchange, and folder
	// reservations wait behind it on a thin link.
	directBatchBytes = 2 << 20
)

type directResult struct {
	ino uint64
	err error
	// refused: juicefs wrote nothing for this file (a refusal of the whole
	// message, or of this file before its transaction).
	refused bool
	// whole: the message never reached juicefs or was refused whole; the
	// stage path handles the file.
	whole bool
	// lead: not a result; this request now sends the next batch.
	lead bool
}

type directReq struct {
	file jfsNewFile
	done chan directResult // buffered 1
}

// directPublisher batches the workers' files into JMCreateFiles messages.
// The first worker to arrive sends; files queued while a message is in
// flight go in the next one, which the oldest of them sends.
type directPublisher struct {
	ctl         *jfsControl
	mu          sync.Mutex
	queue       []*directReq
	sending     bool
	unsupported atomic.Bool
	// send replaces the exchange in tests.
	send func([]jfsNewFile) ([]jfsCreated, error)
}

func newDirectPublisher(ctl *jfsControl) *directPublisher {
	p := &directPublisher{ctl: ctl}
	p.send = ctl.createFiles
	return p
}

func (p *directPublisher) available() bool { return p != nil && !p.unsupported.Load() }

// takeLocked removes the next batch from the head of the queue.
func (p *directPublisher) takeLocked() []*directReq {
	n, size := 0, 0
	for n < len(p.queue) && n < directBatchFiles {
		if n > 0 && size+len(p.queue[n].file.data) > directBatchBytes {
			break
		}
		size += len(p.queue[n].file.data)
		n++
	}
	batch := append([]*directReq(nil), p.queue[:n]...)
	p.queue = append(p.queue[:0], p.queue[n:]...)
	return batch
}

// publish creates one file and returns juicefs's answer for it.
func (p *directPublisher) publish(f jfsNewFile) directResult {
	r := &directReq{file: f, done: make(chan directResult, 1)}
	p.mu.Lock()
	p.queue = append(p.queue, r)
	lead := !p.sending
	p.sending = true
	p.mu.Unlock()
	if !lead {
		res := <-r.done
		if !res.lead {
			return res
		}
	}
	// r is at the head of the queue: the first arrival, or the oldest waiter
	// the previous sender handed over to.
	p.mu.Lock()
	batch := p.takeLocked()
	p.mu.Unlock()
	p.sendBatch(batch)
	p.mu.Lock()
	if len(p.queue) > 0 {
		p.queue[0].done <- directResult{lead: true}
	} else {
		p.sending = false
	}
	p.mu.Unlock()
	return <-r.done
}

func (p *directPublisher) sendBatch(batch []*directReq) {
	files := make([]jfsNewFile, len(batch))
	for i, r := range batch {
		files[i] = r.file
	}
	res, err := p.send(files)
	var refused *errCreateFilesRefused
	whole := errors.As(err, &refused)
	// No .control (not a juicefs mount) counts as no support.
	if whole && (errors.Is(err, errControlUnsupported) || errors.Is(err, os.ErrNotExist)) {
		if !p.unsupported.Swap(true) {
			log.Printf("drainer: juicefs has no JMCreateFiles (patch 0025); small saves use the stage path")
		}
	}
	for i, r := range batch {
		switch {
		case err != nil:
			r.done <- directResult{err: err, refused: whole, whole: whole}
		case res[i].err == nil:
			r.done <- directResult{ino: res[i].ino}
		default:
			// Every per-file errno is a refusal before or by the transaction
			// (EEXIST, EACCES, EROFS, ...) or an ambiguous EXEC (EIO).
			e := res[i].err
			r.done <- directResult{err: e, refused: !errors.Is(e, syscall.EIO)}
		}
	}
}

// directEligible: a new name for a small regular file this path can open
// back as its owner.
func (d *Drainer) directEligible(row *metadata.SpoolRow) bool {
	if !d.direct.available() || row.Size < 0 || row.Size > directMaxBytes {
		return false
	}
	// A name juicefs would refuse goes the stage way, alone, instead of
	// refusing the whole message it would share (L-4).
	if n := len(filepath.Base(row.NFSPath)); n == 0 || n > 255 {
		return false
	}
	return d.directMode(row)&0o400 != 0
}

// directMode is the mode the file is created with: the one the mirror holds
// for the row (what the completion's chmod would set), else 0644.
func (d *Drainer) directMode(row *metadata.SpoolRow) os.FileMode {
	if d.spool.Meta() != nil {
		if store := d.spool.Meta().Owner(); store != nil {
			if e := store.LookupByPath(row.NFSPath); e != nil && !e.IsDir && e.Mode.Perm() != 0 {
				return e.Mode.Perm()
			}
		}
	}
	return 0o644
}

// directInode returns the inode number the file is sent with, recorded as
// the row's claimed base before the message leaves (ClaimDrainBase), so every
// retry and every restart recognises the file as its own whatever happened to
// the reply. A row that already claimed a number keeps it: an earlier send may
// have committed, or may still be running in juicefs. 0: no number (no
// reservation pool, or the record failed); the identical-bytes rule then
// guards the retry.
func (d *Drainer) directInode(row *metadata.SpoolRow, fresh bool) uint64 {
	if !fresh {
		if base, _ := d.rowDrainBase(row); base.Kind == metadata.SpoolBaseClaimed && base.Ino != 0 {
			return base.Ino
		}
	}
	if d.dirInodes == nil {
		return 0
	}
	ino, ok := d.dirInodes.take(directInodeWait)
	if !ok {
		return 0
	}
	if err := d.spool.Meta().ClaimDrainBase(row.ID, ino); err != nil {
		log.Printf("drainer: row %d record reserved inode: %v", row.ID, err)
		return 0
	}
	return ino
}

// directInodeWait bounds the wait for a reserved number (the pool refills in
// the background).
const directInodeWait = 2 * time.Second

// drainDirect publishes row through JMCreateFiles. It returns false, having
// changed nothing, when juicefs refused the file for a reason the stage path
// handles (no support, an ACL parent, a reserved name): the caller then drains
// it through a stage. Otherwise it disposed of the row.
func (d *Drainer) drainDirect(row *metadata.SpoolRow, src *os.File, dest string, spoolMtime time.Time, tainted bool, ph *drainPhases) bool {
	data, sum, ok := d.directReadSpool(row, src)
	if !ok {
		return true
	}
	var pst unix.Stat_t
	if err := unix.Stat(filepath.Dir(dest), &pst); err != nil {
		d.failTransient(row, fmt.Errorf("stat parent: %w", err))
		return true
	}
	if pst.Mode&unix.S_IFMT != unix.S_IFDIR {
		d.failTransient(row, fmt.Errorf("parent of %q is not a folder", row.NFSPath))
		return true
	}
	dev := uint64(pst.Dev) //nolint:unconvert // int32 on Darwin
	if err := pin.FUSEIdentityCheckDev(dev); err != nil {
		d.failTransient(row, err)
		return true
	}
	mode := d.directMode(row)
	mtime := spoolMtime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	ph.mark_(&ph.identity)

	op, ok := d.beginInstallLease(row)
	if !ok {
		return true
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
		if err != nil {
			d.failTransient(row, fmt.Errorf("recheck drain claim: %w", err))
		} else {
			log.Printf("drainer: row %d (%s) canceled before install — destination untouched", row.ID, row.NFSPath)
		}
		return true
	}
	file := jfsNewFile{parent: pst.Ino, preIno: d.directInode(row, false), name: filepath.Base(dest), mode: mode, mtime: mtime, data: data}
	res := d.direct.publish(file)
	if file.preIno != 0 && res.refused && errors.Is(res.err, syscall.ESRCH) {
		// juicefs does not hold the number (used up by a send that failed,
		// or reserved before juicefs restarted) and the name does not hold
		// it: no send of it can still commit. Take a new one, after dropping
		// a pool whose numbers a restarted juicefs voided.
		if !d.dirInodes.stillHeld() {
			d.dirInodes.invalidate()
		}
		file.preIno = d.directInode(row, true)
		res = d.direct.publish(file)
	}
	ph.mark_(&ph.sync_)
	switch {
	case res.err == nil:
	case res.whole || (res.refused && (errors.Is(res.err, errControlUnsupported) || errors.Is(res.err, syscall.EINVAL) ||
		errors.Is(res.err, syscall.ENAMETOOLONG) || errors.Is(res.err, syscall.EFBIG) || errors.Is(res.err, syscall.ESRCH))):
		finish()
		return false
	case res.refused && errors.Is(res.err, syscall.EAGAIN):
		// An earlier send of this file is still running in juicefs: its
		// outcome decides. Try again later.
		finish()
		d.failTransient(row, fmt.Errorf("direct publish: an earlier send of %q is still running: %w", row.NFSPath, res.err))
		return true
	case res.refused && errors.Is(res.err, syscall.EEXIST):
		if ino, own := d.directOwnAtName(dest, file.preIno); own {
			res.ino = ino // our own earlier send committed
			break
		}
		// The name appeared after the base decision (same rule as
		// installNewName): our own unfinished older row retries in place;
		// anything else gets a conflict copy beside it.
		if d.olderRowInstalledDest(row, dest, tainted) {
			finish()
			d.failTransient(row, fmt.Errorf("install drain stage: %q appeared from an older local row; retrying in place", row.NFSPath))
			return true
		}
		cfile := file
		cfile.preIno = 0
		published, cerr := d.directConflictCopy(cfile, dest)
		if cerr == nil && op != nil {
			op.mutated = true
		}
		finish()
		if cerr != nil {
			d.failTransient(row, fmt.Errorf("install conflict copy: %w", cerr))
			return true
		}
		d.failPermanent(row, fmt.Sprintf("name created elsewhere while this new file drained; %s %q", conflictCopyMarker, filepath.Base(published)))
		return true
	case !res.refused:
		// The transaction may have committed, or may still. With a recorded
		// number, the next attempt recognises the file (or juicefs answers
		// EAGAIN while the send runs); without one, adopt the name only if
		// it holds exactly these bytes.
		if op != nil {
			op.mutated = true
		}
		if ino, own := d.directOwnAtName(dest, file.preIno); own {
			res.ino = ino
			break
		}
		if file.preIno == 0 {
			if ino, same := directAdoptIdentical(dest, row.Size, sum[:]); same {
				jmlog.Info("drain: direct publish had no clear answer; the name holds exactly these bytes, adopting it",
					"path", row.NFSPath, "error", res.err.Error())
				res.ino = ino
				break
			}
		}
		finish()
		d.failTransient(row, fmt.Errorf("direct publish (outcome unknown): %w", res.err))
		return true
	default:
		finish()
		d.failTransient(row, fmt.Errorf("direct publish: %w", res.err))
		return true
	}
	if op != nil {
		op.mutated = true
	}
	d.recordInstalledIdentity(row.ID, dev, res.ino)
	// H-8B: the name now holds THIS row's inode; a retry recognises it.
	if res.ino != file.preIno {
		if cerr := d.spool.Meta().ClaimDrainBase(row.ID, res.ino); cerr != nil {
			log.Printf("drainer: row %d claim installed inode: %v", row.ID, cerr)
		}
	}
	finish()
	d.finishPublished(row, dest, res.ino, dev, mtime, data, ph)
	return true
}

// directReadSpool reads the whole spool file and checks it against the row.
func (d *Drainer) directReadSpool(row *metadata.SpoolRow, src *os.File) ([]byte, [32]byte, bool) {
	var sum [32]byte
	fi, err := src.Stat()
	if err != nil {
		d.failTransient(row, fmt.Errorf("stat spool: %w", err))
		return nil, sum, false
	}
	if fi.Size() != row.Size {
		d.failTransient(row, fmt.Errorf("size mismatch: expected %d, got %d", row.Size, fi.Size()))
		return nil, sum, false
	}
	data := make([]byte, row.Size)
	if _, err := io.ReadFull(io.NewSectionReader(src, 0, row.Size), data); err != nil {
		d.failTransient(row, fmt.Errorf("read spool: %w", err))
		return nil, sum, false
	}
	sum = sha256.Sum256(data)
	if len(row.SHA256) > 0 && !bytes.Equal(sum[:], row.SHA256) {
		d.quarantine(row, fmt.Sprintf("sha mismatch (spool): streamed=%x diskread=%x", row.SHA256, sum))
		return nil, sum, false
	}
	return data, sum, true
}

// directOwnAtName reports whether dest is a regular file with inode ino (the
// number this row recorded before sending): our own file.
func (d *Drainer) directOwnAtName(dest string, ino uint64) (uint64, bool) {
	if ino == 0 {
		return 0, false
	}
	var st unix.Stat_t
	if err := unix.Lstat(dest, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || uint64(st.Ino) != ino {
		return 0, false
	}
	return ino, true
}

// directNegativeCache is how long the kernel may answer ENOENT for a name the
// drainer (or the NFS layer) looked up before juicefs created it. Linux juicefs
// invalidates the entry; macOS juicefs cannot, and the app mounts with a
// negative-entry cache (JM_META_NEGATIVE_CACHE, default 5 s).
func directNegativeCache() time.Duration {
	v := os.Getenv("JM_META_NEGATIVE_CACHE")
	if c := os.Getenv("JM_META_CACHE_SECS"); c != "" {
		v = c
	}
	ttl := 5 * time.Second
	if v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			ttl = d
		} else if n, err := strconv.Atoi(v); err == nil {
			ttl = time.Duration(n) * time.Second
		}
	}
	if ttl < 0 || ttl > 30*time.Second {
		ttl = 30 * time.Second
	}
	return ttl + time.Second
}

// openPublished opens the file juicefs created at dest. A stale negative
// entry answers ENOENT until it expires; juicefs said the file exists, so
// wait it out instead of failing the drain (which would resend and, before the
// reserved number, make a conflict copy of our own file).
func (d *Drainer) openPublished(dest string) (*os.File, error) {
	deadline := time.Now().Add(directNegativeCache())
	wait := 100 * time.Millisecond
	waited := false
	for {
		f, err := os.OpenFile(dest, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err == nil || !errors.Is(err, os.ErrNotExist) || time.Now().After(deadline) {
			if waited && err == nil {
				d.metrics.DirectOpenWaits.Add(1)
			}
			return f, err
		}
		waited = true
		time.Sleep(wait)
		if wait < time.Second {
			wait *= 2
		}
	}
}

// finishPublished completes a row whose bytes the name dest holds as inode
// ino (published by this drain, or recognised as its own): the same checks
// and completion as the stage path after its rename.
func (d *Drainer) finishPublished(row *metadata.SpoolRow, dest string, ino, dev uint64, mtime time.Time, data []byte, ph *drainPhases) {
	// The completion keeps a descriptor of the published file (its identity,
	// the effects' chmod).
	f, err := d.openPublished(dest)
	if err != nil {
		d.failTransient(row, fmt.Errorf("open published file: %w", err))
		return
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
		}
	}()
	ofi, err := f.Stat()
	if err != nil {
		d.failTransient(row, fmt.Errorf("stat published file: %w", err))
		return
	}
	st, ok := ofi.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Ino) != ino || uint64(st.Dev) != dev || !ofi.Mode().IsRegular() || ofi.Size() != row.Size { //nolint:unconvert // int32 on Darwin
		// The identities, so a field run tells a stale kernel entry, a
		// replaced name and a wrong reply apart (R2).
		var seenIno, seenDev uint64
		if ok {
			seenIno, seenDev = uint64(st.Ino), uint64(st.Dev) //nolint:unconvert // int32 on Darwin
		}
		jmlog.Warn("drain: the published name does not show the file this drain published",
			"path", row.NFSPath, "published_ino", ino, "name_ino", seenIno, "published_dev", dev, "name_dev", seenDev,
			"size", ofi.Size(), "want_size", row.Size, "regular", ofi.Mode().IsRegular())
		d.failTransient(row, fmt.Errorf("published file %q changed before it was opened: %w", row.NFSPath, metadata.ErrMirrorConflict))
		return
	}
	if err := pin.FUSEIdentityCheckDev(dev); err != nil {
		d.failTransient(row, err)
		return
	}
	destIdentity := fmt.Sprintf("%d:%d", dev, st.Ino)
	if d.atRestVerify && len(row.SHA256) > 0 {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, row.Size)); err != nil {
			d.failTransient(row, fmt.Errorf("post-copy reread for SHA verify: %w", err))
			return
		}
		if got := h.Sum(nil); !bytes.Equal(got, row.SHA256) {
			d.failTransient(row, fmt.Errorf("sha mismatch (fuse at-rest): fuse=%x want=%x — spool good, re-draining", got, row.SHA256))
			return
		}
	}
	if fi, statErr := os.Stat(d.fuseRoot); statErr == nil {
		if sys, ok := fi.Sys().(*syscall.Stat_t); ok && uint64(sys.Dev) != dev { //nolint:unconvert // int32 on Darwin
			d.failTransient(row, fmt.Errorf("dest device %d != current mount device %d (remount raced the publish): %w",
				dev, sys.Dev, pin.ErrFUSEIdentityGate))
			return
		}
	}
	ph.mark_(&ph.chtimes)
	// Metadata and Redis events store whole Unix seconds (as the stage path).
	drainedMtime := time.Unix(mtime.Unix(), 0)
	var sidecarBody []byte
	if row.Size > 0 && row.Size <= sidecarMaxFile && cacheableMetaName(filepath.Base(row.NFSPath)) {
		if len(row.SHA256) == 0 {
			d.holdCompletion(row, metadata.ErrMirrorConflict)
			return
		}
		sidecarBody = data
	}
	keep = true
	pd := pendingDrain{row: row, dest: dest, n: row.Size, mtime: drainedMtime, fd: f, sidecarBody: sidecarBody, backingIdentity: destIdentity}
	if d.batch != nil {
		d.batch.enqueue(pd)
	} else {
		d.completeInline(pd)
	}
	d.metrics.DrainsDirect.Add(1)
	ph.mark_(&ph.mark)
	ph.report(row.NFSPath, row.Size)
}

// directAdoptTarget: the name already holds this row's own file, complete
// (the inode this row recorded, or for a new name exactly these bytes). The
// base decision then completes the row without writing.
func (d *Drainer) directAdoptTarget(row *metadata.SpoolRow, dest string, base metadata.SpoolBase, st *unix.Stat_t, tainted bool) bool {
	if d.direct == nil || row.Size < 0 || row.Size > directMaxBytes || len(row.SHA256) == 0 || st.Size != row.Size {
		return false
	}
	switch {
	case base.Kind == metadata.SpoolBaseClaimed && uint64(st.Ino) == base.Ino:
	case base.Kind == metadata.SpoolBaseAbsent && !tainted && row.Size > 0:
		// Exactly these bytes at a name that was free. Not for an empty file:
		// every empty file matches, so another writer's would be taken as ours.
	case base.Kind == metadata.SpoolBaseClaimed && !tainted && row.Size > 0:
		// R2 (laptop run 2026-11-14): after a link drop the name showed
		// another inode than the one this drain published and claimed, with
		// exactly these bytes. Only this Mac held the project, so those bytes
		// are this save's: adopt them instead of a conflict copy of its own
		// file. Different bytes, or another Mac in between, still conflict.
		jmlog.Warn("drain: the name holds this save's bytes under another inode than the drain claimed; adopting it if the bytes match",
			"path", row.NFSPath, "claimed_ino", base.Ino, "name_ino", uint64(st.Ino), "size", st.Size) //nolint:unconvert // uint64 on Darwin
	default:
		return false
	}
	ino, same := directAdoptIdentical(dest, row.Size, row.SHA256)
	return same && ino == uint64(st.Ino)
}

// directConflictCopy publishes the bytes beside dest under a conflict name.
func (d *Drainer) directConflictCopy(file jfsNewFile, dest string) (string, error) {
	host, now := drainConflictHost(), drainConflictNow()
	var last error
	for n := 1; n <= 9; n++ {
		target := conflictCopyPath(dest, host, now, n)
		file.name = filepath.Base(target)
		res := d.direct.publish(file)
		if res.err == nil {
			return target, nil
		}
		if !res.refused || !errors.Is(res.err, syscall.EEXIST) {
			return "", res.err
		}
		last = res.err
	}
	return "", fmt.Errorf("drain install: no free conflict name for %q: %w", dest, last)
}

// directAdoptIdentical reports whether dest is a regular file holding exactly
// size bytes with SHA-256 sum, and its inode.
func directAdoptIdentical(dest string, size int64, sum []byte) (uint64, bool) {
	f, err := os.OpenFile(dest, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != size {
		return 0, false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.LimitReader(f, size+1)); err != nil || n != size {
		return 0, false
	}
	if !bytes.Equal(h.Sum(nil), sum) {
		return 0, false
	}
	return uint64(st.Ino), true
}
