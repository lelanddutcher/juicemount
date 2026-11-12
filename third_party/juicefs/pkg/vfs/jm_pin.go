/*
 * JuiceMount: the JMPinCache control message (patch 0025).
 *
 * Body: one op byte, then the op's arguments. Every reply starts with an
 * errno byte; a nonzero errno ends the reply.
 *
 *	1 START  ino(8)            fill and pin the file's blocks in the background.
 *	                           Reply: errno. A fill already running for ino
 *	                           is kept. The file's slices are read when the
 *	                           fill starts.
 *	2 STATUS ino(8)            Reply: errno (ENOENT: no fill known), state(1:
 *	                           1 running, 2 done, 3 failed), fill errno(1),
 *	                           total(8), done(8), fetched(8) bytes.
 *	3 CHECK  ino(8)            local only. Reply: errno, known(1), total(8),
 *	                           missing(8) bytes of the pinned blocks.
 *	4 SWEEP  phase(1) n(4) ino(8)*n
 *	                           keep the listed inodes. Phase bit 1 starts a
 *	                           sweep, bit 2 ends it: at the end every owner
 *	                           not kept and not filling is unpinned. Reply:
 *	                           errno, dropped(4), pinned(8), cap(8) bytes.
 *
 * Errnos: ENOTSUP (no disk cache), ENOSPC (pinned bytes would pass the cap),
 * EIO (a block could not be downloaded), ENOENT, EINVAL.
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
	jmPinOpStart  = 1
	jmPinOpStatus = 2
	jmPinOpCheck  = 3
	jmPinOpSweep  = 4

	jmPinRunning = 1
	jmPinDone    = 2
	jmPinFailed  = 3
)

type jmPinStore interface {
	JMPinFill(ctx context.Context, owner uint64, slices []chunk.JMPinSlice, prog *chunk.JMPinProgress) error
	JMPinCheck(owner uint64) (known bool, total, missing int64, err error)
	JMPinDrop(owner uint64)
	JMPinOwners() []uint64
	JMPinUsage() (pinned, limit int64)
}

type jmPinJob struct {
	prog  chunk.JMPinProgress
	state atomic.Int32
	errno atomic.Int32
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

// jmPinSlices lists the slices of ino's chunks, as warmup does.
func jmPinSlices(ctx meta.Context, m meta.Meta, ino Ino) ([]chunk.JMPinSlice, syscall.Errno) {
	var attr Attr
	if st := m.GetAttr(ctx, ino, &attr); st != 0 {
		return nil, st
	}
	if attr.Typ != meta.TypeFile {
		return nil, syscall.EINVAL
	}
	var out []chunk.JMPinSlice
	chunks := (attr.Length + meta.ChunkSize - 1) / meta.ChunkSize
	for indx := uint64(0); indx < chunks; indx++ {
		var slices []meta.Slice
		if st := m.Read(ctx, ino, uint32(indx), &slices); st != 0 {
			return nil, st
		}
		for _, s := range slices {
			if s.Id != 0 {
				out = append(out, chunk.JMPinSlice{ID: s.Id, Size: s.Size})
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
	case errors.Is(err, chunk.ErrJMPinFull):
		return syscall.ENOSPC
	case errors.Is(err, chunk.ErrJMPinUnsupported):
		return syscall.ENOTSUP
	case errors.As(err, &eno):
		return eno
	default:
		return syscall.EIO
	}
}

func (v *VFS) jmPinStart(ino Ino) syscall.Errno {
	store, ok := v.Store.(jmPinStore)
	if !ok {
		return syscall.ENOTSUP
	}
	st := v.jmPin()
	st.mu.Lock()
	if j := st.jobs[ino]; j != nil && j.state.Load() == jmPinRunning {
		st.mu.Unlock()
		return 0
	}
	job := &jmPinJob{}
	job.state.Store(jmPinRunning)
	st.jobs[ino] = job
	if st.sweeping {
		st.keep[ino] = struct{}{}
	}
	st.mu.Unlock()
	go func() {
		ctx := meta.Background()
		slices, eno := jmPinSlices(ctx, v.Meta, ino)
		var err error
		if eno != 0 {
			err = eno
		} else {
			err = store.JMPinFill(context.Background(), uint64(ino), slices, &job.prog)
		}
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
	if !r.HasMore() {
		fail(syscall.EINVAL)
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
	case jmPinOpStart:
		fail(v.jmPinStart(Ino(r.Get64()))) // errno 0 is the whole reply
	case jmPinOpStatus:
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
	case jmPinOpCheck:
		known, total, missing, err := store.JMPinCheck(r.Get64())
		if err != nil {
			fail(jmPinErrno(err))
			return
		}
		w := utils.NewBuffer(1 + 1 + 8 + 8)
		w.Put8(0)
		if known {
			w.Put8(1)
		} else {
			w.Put8(0)
		}
		w.Put64(uint64(total))
		w.Put64(uint64(missing))
		_, _ = out.Write(w.Bytes())
	case jmPinOpSweep:
		phase := r.Get8()
		n := r.Get32()
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
		for i := uint32(0); i < n && r.HasMore(); i++ {
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
				store.JMPinDrop(o)
				dropped++
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
