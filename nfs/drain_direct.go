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
	directBatchBytes = 8 << 20
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

// drainDirect publishes row through JMCreateFiles. It returns false, having
// changed nothing, when juicefs refused the file for a reason the stage path
// handles (no support, an ACL parent, a reserved name): the caller then drains
// it through a stage. Otherwise it disposed of the row.
func (d *Drainer) drainDirect(row *metadata.SpoolRow, src *os.File, dest string, spoolMtime time.Time, ph *drainPhases) bool {
	fi, err := src.Stat()
	if err != nil {
		d.failTransient(row, fmt.Errorf("stat spool: %w", err))
		return true
	}
	if fi.Size() != row.Size {
		d.failTransient(row, fmt.Errorf("size mismatch: expected %d, got %d", row.Size, fi.Size()))
		return true
	}
	data := make([]byte, row.Size)
	if _, err := io.ReadFull(io.NewSectionReader(src, 0, row.Size), data); err != nil {
		d.failTransient(row, fmt.Errorf("read spool: %w", err))
		return true
	}
	sum := sha256.Sum256(data)
	if len(row.SHA256) > 0 && !bytes.Equal(sum[:], row.SHA256) {
		d.quarantine(row, fmt.Sprintf("sha mismatch (spool): streamed=%x diskread=%x", row.SHA256, sum))
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
	if err := pin.FUSEIdentityCheckDev(uint64(pst.Dev)); err != nil { //nolint:unconvert // int32 on Darwin
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
	file := jfsNewFile{parent: pst.Ino, name: filepath.Base(dest), mode: mode, mtime: mtime, data: data}
	res := d.direct.publish(file)
	ph.mark_(&ph.sync_)
	switch {
	case res.err == nil:
	case res.whole || (res.refused && (errors.Is(res.err, errControlUnsupported) || errors.Is(res.err, syscall.EINVAL) ||
		errors.Is(res.err, syscall.ENAMETOOLONG) || errors.Is(res.err, syscall.EFBIG))):
		finish()
		return false
	case res.refused && errors.Is(res.err, syscall.EEXIST):
		// The name appeared after the base decision (same rule as
		// installNewName): our own unfinished older row retries in place;
		// anything else gets a conflict copy beside it.
		if d.olderRowInstalledDest(row, dest) {
			finish()
			d.failTransient(row, fmt.Errorf("install drain stage: %q appeared from an older local row; retrying in place", row.NFSPath))
			return true
		}
		published, cerr := d.directConflictCopy(file, dest)
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
		// The transaction may have committed. Adopt the name only if it holds
		// exactly these bytes; otherwise retry (the retry's base decision
		// sees whatever is there).
		if op != nil {
			op.mutated = true
		}
		ino, same := directAdoptIdentical(dest, row.Size, sum[:])
		if !same {
			finish()
			d.failTransient(row, fmt.Errorf("direct publish (outcome unknown): %w", res.err))
			return true
		}
		jmlog.Info("drain: direct publish had no clear answer; the name holds exactly these bytes, adopting it",
			"path", row.NFSPath, "error", res.err.Error())
		res.ino = ino
	default:
		finish()
		d.failTransient(row, fmt.Errorf("direct publish: %w", res.err))
		return true
	}
	if op != nil {
		op.mutated = true
	}
	d.recordInstalledIdentity(row.ID, uint64(pst.Dev), res.ino) //nolint:unconvert // int32 on Darwin
	// H-8B: the name now holds THIS row's inode; a retry rewrites it in place.
	if cerr := d.spool.Meta().ClaimDrainBase(row.ID, res.ino); cerr != nil {
		log.Printf("drainer: row %d claim installed inode: %v", row.ID, cerr)
	}
	finish()

	// The completion keeps a descriptor of the published file (its identity,
	// the effects' chmod).
	f, err := os.OpenFile(dest, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		d.failTransient(row, fmt.Errorf("open published file: %w", err))
		return true
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
		return true
	}
	st, ok := ofi.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Ino) != res.ino || uint64(st.Dev) != uint64(pst.Dev) || !ofi.Mode().IsRegular() || ofi.Size() != row.Size { //nolint:unconvert // int32 on Darwin
		d.failTransient(row, fmt.Errorf("published file %q changed before it was opened: %w", row.NFSPath, metadata.ErrMirrorConflict))
		return true
	}
	if err := pin.FUSEIdentityCheckDev(uint64(st.Dev)); err != nil { //nolint:unconvert // int32 on Darwin
		d.failTransient(row, err)
		return true
	}
	destIdentity := fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino) //nolint:unconvert // int32 on Darwin
	if d.atRestVerify && len(row.SHA256) > 0 {
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, 0, row.Size)); err != nil {
			d.failTransient(row, fmt.Errorf("post-copy reread for SHA verify: %w", err))
			return true
		}
		if got := h.Sum(nil); !bytes.Equal(got, row.SHA256) {
			d.failTransient(row, fmt.Errorf("sha mismatch (fuse at-rest): fuse=%x want=%x — spool good, re-draining", got, row.SHA256))
			return true
		}
	}
	if fi, statErr := os.Stat(d.fuseRoot); statErr == nil {
		if sys, ok := fi.Sys().(*syscall.Stat_t); ok && uint64(sys.Dev) != uint64(st.Dev) { //nolint:unconvert // int32 on Darwin
			d.failTransient(row, fmt.Errorf("dest device %d != current mount device %d (remount raced the publish): %w",
				st.Dev, sys.Dev, pin.ErrFUSEIdentityGate))
			return true
		}
	}
	ph.mark_(&ph.chtimes)
	// Metadata and Redis events store whole Unix seconds (as the stage path).
	drainedMtime := time.Unix(mtime.Unix(), 0)
	var sidecarBody []byte
	if row.Size > 0 && row.Size <= sidecarMaxFile && cacheableMetaName(filepath.Base(row.NFSPath)) {
		if len(row.SHA256) == 0 {
			d.holdCompletion(row, metadata.ErrMirrorConflict)
			return true
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
	return true
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
