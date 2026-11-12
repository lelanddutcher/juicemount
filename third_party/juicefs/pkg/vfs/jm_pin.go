/*
 * JuiceMount: the JMPinCache control message (patch 0026).
 *
 * Body: one op byte (meta.JMPinOp*), then the op's arguments. Every reply
 * starts with an errno byte; a nonzero errno ends the reply. A body shorter
 * than its op needs is EINVAL.
 *
 *	1 START  ino(8) [workers(1)]
 *	                 fill and pin the file's blocks in the background, with at
 *	                 most workers parallel downloads (1..4, default 4). A fill
 *	                 already running for ino is kept when the file's length
 *	                 and mtime still match; otherwise it is canceled and
 *	                 started again. Reply: errno.
 *	2 STATUS ino(8)  Reply: errno (ENOENT: no fill known), state(1: 1 running,
 *	                 2 done, 3 failed), fill errno(1), total(8), done(8),
 *	                 fetched(8) bytes.
 *	3 CHECK  ino(8)  local only. Reply: errno, known(1), total(8), missing(8),
 *	                 length(8), mtime_ns(8) as the fill saw them.
 *	4 SWEEP  phase(1) n(4) ino(8)*n
 *	                 keep the listed inodes. Phase bit 1 starts a sweep, bit 2
 *	                 ends it: at the end every owner not kept and not filling
 *	                 is unpinned. Reply: errno, dropped(4), pinned(8), cap(8).
 *	5 CANCEL ino(8)  stop a running fill (its blocks so far stay pinned).
 *	                 Reply: errno.
 *	6 VERIFY n(4) (ino(8) length(8) mtime_ns(8))*n
 *	                 local only. Reply: errno, count(4), then per item that is
 *	                 not pinned (1), pinned from another length or mtime (2) or
 *	                 missing blocks (3): ino(8) reason(1) missing(8).
 *	7 SETCAP bytes(8) grow the cache capacity to at least bytes (never below
 *	                 --cache-size). Reply: errno, capacity(8), pinned(8), cap(8).
 *
 * Errnos: ENOTSUP (no disk cache), ENOSPC (pinned bytes would pass the cap, or
 * the disk is at its free-space floor), EIO (a block could not be
 * downloaded), ECANCELED, ENOENT, EISDIR, EINVAL.
 */

package vfs

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	jmPinRunning = 1
	jmPinDone    = 2
	jmPinFailed  = 3
)

type jmPinStore interface {
	JMPinFill(ctx context.Context, owner uint64, slices []chunk.JMPinSlice, attr chunk.JMPinAttr, workers int, prog *chunk.JMPinProgress) error
	JMPinCheck(owner uint64) (known bool, total, missing int64, attr chunk.JMPinAttr, err error)
	JMPinVerify(items []chunk.JMPinVerifyItem) ([]chunk.JMPinVerifyResult, error)
	JMPinDrop(owner uint64) bool
	JMPinOwners() []uint64
	JMPinUsage() (pinned, limit int64)
	JMPinSetCapacity(bytes int64) int64
}

type jmPinJob struct {
	prog   chunk.JMPinProgress
	state  atomic.Int32
	errno  atomic.Int32
	cancel context.CancelFunc
	mu     sync.Mutex
	attr   *chunk.JMPinAttr // the file's attributes, once read
}

func (j *jmPinJob) seenAttr() *chunk.JMPinAttr {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.attr
}

type jmPinState struct {
	mu       sync.Mutex
	jobs     map[Ino]*jmPinJob
	sweeping bool
	keep     map[Ino]struct{}
}

var jmPinStates sync.Map // *VFS -> *jmPinState

func (v *VFS) jmPin() *jmPinState {
	if s, ok := jmPinStates.Load(v); ok {
		return s.(*jmPinState)
	}
	s, _ := jmPinStates.LoadOrStore(v, &jmPinState{jobs: make(map[Ino]*jmPinJob)})
	return s.(*jmPinState)
}

func jmAttrOf(a *Attr) chunk.JMPinAttr {
	return chunk.JMPinAttr{Length: a.Length, MtimeNS: a.Mtime*1e9 + int64(a.Mtimensec)}
}

// jmPinSlices lists the visible slices of ino's chunks, as warmup reads them.
func jmPinSlices(ctx meta.Context, m meta.Meta, ino Ino, length uint64) ([]chunk.JMPinSlice, syscall.Errno) {
	var out []chunk.JMPinSlice
	chunks := (length + meta.ChunkSize - 1) / meta.ChunkSize
	for indx := uint64(0); indx < chunks; indx++ {
		if ctx.Canceled() {
			return nil, syscall.ECANCELED
		}
		var slices []meta.Slice
		if st := m.Read(ctx, ino, uint32(indx), &slices); st != 0 {
			return nil, st
		}
		for _, s := range slices {
			if s.Id != 0 && s.Len > 0 {
				out = append(out, chunk.JMPinSlice{ID: s.Id, Size: s.Size, Off: s.Off, Len: s.Len})
			}
		}
	}
	return out, 0
}

func jmPinErrno(err error) syscall.Errno {
	var eno syscall.Errno
	switch {
	case err == nil:
		return 0
	case errors.Is(err, chunk.ErrJMPinFull), errors.Is(err, chunk.ErrJMPinNoSpace):
		return syscall.ENOSPC
	case errors.Is(err, chunk.ErrJMPinUnsupported):
		return syscall.ENOTSUP
	case errors.Is(err, context.Canceled):
		return syscall.ECANCELED
	case errors.As(err, &eno):
		return eno
	default:
		return syscall.EIO
	}
}

func (v *VFS) jmPinStart(store jmPinStore, ino Ino, workers int) syscall.Errno {
	st := v.jmPin()
	st.mu.Lock()
	running := st.jobs[ino]
	st.mu.Unlock()
	if running != nil && running.state.Load() == jmPinRunning {
		seen := running.seenAttr()
		if seen == nil {
			return 0
		}
		var attr Attr
		if eno := v.Meta.GetAttr(meta.Background(), ino, &attr); eno == 0 && jmAttrOf(&attr) == *seen {
			return 0
		}
		running.cancel() // the file changed since this fill read its slices
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &jmPinJob{cancel: cancel}
	job.state.Store(jmPinRunning)
	st.mu.Lock()
	st.jobs[ino] = job
	if st.sweeping {
		st.keep[ino] = struct{}{}
	}
	st.mu.Unlock()
	go func() {
		defer cancel()
		mctx := meta.WrapContext(ctx)
		err := func() error {
			var attr Attr
			if eno := v.Meta.GetAttr(mctx, ino, &attr); eno != 0 {
				return eno
			}
			if attr.Typ == meta.TypeDirectory {
				return syscall.EISDIR
			}
			if attr.Typ != meta.TypeFile {
				return syscall.EINVAL
			}
			a := jmAttrOf(&attr)
			job.mu.Lock()
			job.attr = &a
			job.mu.Unlock()
			slices, eno := jmPinSlices(mctx, v.Meta, ino, attr.Length)
			if eno != 0 {
				return eno
			}
			return store.JMPinFill(ctx, uint64(ino), slices, a, workers, &job.prog)
		}()
		if err != nil {
			logger.Warnf("pin inode %d: %s", ino, err)
			job.errno.Store(int32(jmPinErrno(err)))
			job.state.Store(jmPinFailed)
			return
		}
		logger.Infof("pin inode %d: %d bytes pinned, %d downloaded", ino, job.prog.Done.Load(), job.prog.Fetched.Load())
		job.state.Store(jmPinDone)
	}()
	return 0
}

func (v *VFS) jmPinMsg(r *utils.Buffer, out io.Writer) {
	fail := func(eno syscall.Errno) { _, _ = out.Write([]byte{uint8(eno)}) }
	need := func(n int) bool {
		if r.Left() < n {
			fail(syscall.EINVAL)
			return false
		}
		return true
	}
	if !need(1) {
		return
	}
	op := r.Get8()
	store, ok := v.Store.(jmPinStore)
	if !ok {
		fail(syscall.ENOTSUP)
		return
	}
	st := v.jmPin()
	switch op {
	case meta.JMPinOpStart:
		if !need(8) {
			return
		}
		ino := Ino(r.Get64())
		workers := 0
		if r.Left() >= 1 {
			workers = int(r.Get8())
		}
		fail(v.jmPinStart(store, ino, workers)) // errno 0 is the whole reply
	case meta.JMPinOpStatus:
		if !need(8) {
			return
		}
		ino := Ino(r.Get64())
		st.mu.Lock()
		job := st.jobs[ino]
		st.mu.Unlock()
		if job == nil {
			fail(syscall.ENOENT)
			return
		}
		w := utils.NewBuffer(1 + 1 + 1 + 8 + 8 + 8)
		w.Put8(0)
		w.Put8(uint8(job.state.Load()))
		w.Put8(uint8(job.errno.Load()))
		w.Put64(uint64(job.prog.Total.Load()))
		w.Put64(uint64(job.prog.Done.Load()))
		w.Put64(uint64(job.prog.Fetched.Load()))
		_, _ = out.Write(w.Bytes())
	case meta.JMPinOpCheck:
		if !need(8) {
			return
		}
		known, total, missing, attr, err := store.JMPinCheck(r.Get64())
		if err != nil {
			fail(jmPinErrno(err))
			return
		}
		w := utils.NewBuffer(1 + 1 + 8*4)
		w.Put8(0)
		if known {
			w.Put8(1)
		} else {
			w.Put8(0)
		}
		w.Put64(uint64(total))
		w.Put64(uint64(missing))
		w.Put64(attr.Length)
		w.Put64(uint64(attr.MtimeNS))
		_, _ = out.Write(w.Bytes())
	case meta.JMPinOpCancel:
		if !need(8) {
			return
		}
		ino := Ino(r.Get64())
		st.mu.Lock()
		job := st.jobs[ino]
		st.mu.Unlock()
		if job == nil {
			fail(syscall.ENOENT)
			return
		}
		job.cancel()
		fail(0)
	case meta.JMPinOpVerify:
		if !need(4) {
			return
		}
		n := int(r.Get32())
		if n > meta.JMPinVerifyMax || !need(24*n) {
			if n > meta.JMPinVerifyMax {
				fail(syscall.EINVAL)
			}
			return
		}
		items := make([]chunk.JMPinVerifyItem, n)
		for i := range items {
			items[i].Owner = r.Get64()
			items[i].Attr.Length = r.Get64()
			items[i].Attr.MtimeNS = int64(r.Get64())
		}
		bad, err := store.JMPinVerify(items)
		if err != nil {
			fail(jmPinErrno(err))
			return
		}
		w := utils.NewBuffer(uint32(1 + 4 + 17*len(bad)))
		w.Put8(0)
		w.Put32(uint32(len(bad)))
		for _, b := range bad {
			w.Put64(b.Owner)
			w.Put8(b.Reason)
			w.Put64(uint64(b.Missing))
		}
		_, _ = out.Write(w.Bytes())
	case meta.JMPinOpSetCap:
		if !need(8) {
			return
		}
		capacity := store.JMPinSetCapacity(int64(r.Get64()))
		pinned, limit := store.JMPinUsage()
		w := utils.NewBuffer(1 + 8*3)
		w.Put8(0)
		w.Put64(uint64(capacity))
		w.Put64(uint64(pinned))
		w.Put64(uint64(limit))
		_, _ = out.Write(w.Bytes())
	case meta.JMPinOpSweep:
		if !need(5) {
			return
		}
		phase := r.Get8()
		n := int(r.Get32())
		if n > meta.JMPinVerifyMax {
			fail(syscall.EINVAL)
			return
		}
		if !need(8 * n) {
			return
		}
		st.mu.Lock()
		if phase&1 != 0 {
			st.sweeping = true
			st.keep = make(map[Ino]struct{})
		}
		if !st.sweeping {
			st.mu.Unlock()
			fail(syscall.EINVAL)
			return
		}
		for i := 0; i < n; i++ {
			st.keep[Ino(r.Get64())] = struct{}{}
		}
		var dropped uint32
		if phase&2 != 0 {
			for _, o := range store.JMPinOwners() {
				ino := Ino(o)
				if _, kept := st.keep[ino]; kept {
					continue
				}
				if j := st.jobs[ino]; j != nil && j.state.Load() == jmPinRunning {
					continue
				}
				if store.JMPinDrop(o) {
					dropped++
				}
			}
			for ino, j := range st.jobs {
				if _, kept := st.keep[ino]; !kept && j.state.Load() != jmPinRunning {
					delete(st.jobs, ino)
				}
			}
			st.sweeping, st.keep = false, nil
		}
		st.mu.Unlock()
		pinned, limit := store.JMPinUsage()
		w := utils.NewBuffer(1 + 4 + 8 + 8)
		w.Put8(0)
		w.Put32(dropped)
		w.Put64(uint64(pinned))
		w.Put64(uint64(limit))
		_, _ = out.Write(w.Bytes())
	default:
		fail(syscall.EINVAL)
	}
}
