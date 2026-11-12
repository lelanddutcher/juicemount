/*
 * JuiceMount-owned (patch 0025). Not part of upstream JuiceFS.
 *
 * JMCreateFiles (control message 1104) creates a batch of small files. Each
 * file's bytes are uploaded first (one object, through the small-upload lane
 * of patch 0024), then one metadata transaction per folder adds the names
 * with their data, length, mode and mtime (meta.JMCreateFilesIn). A name therefore never appears
 * empty or partial, and a crash before the transaction leaves only an
 * unreferenced object. The uploads of one message run concurrently, and so
 * do the transactions of different folders;
 * on macOS juicefs keeps one control handle per process, so the JuiceMount
 * drainer batches its saves into one message instead of sending many.
 *
 * Message body: count (u16), then per file: parent (u64), reserved inode
 * (u64, 0 for none; see meta.JMNewFile.PreIno), mode (u16), cumask (u16),
 * mtime seconds (u64), mtime nanoseconds (u32), name length (u8), name, data
 * length (u32), data. An empty name fails that file alone (EINVAL).
 * Reply: marker (u8, 0xA5), status (u8: 0, or the errno of a malformed or
 * refused message), count (u16), then per file: errno (u8) and inode (u64, 0
 * on error). A juicefs without this message answers one byte, EINVAL; the
 * marker tells the two apart.
 */

package vfs

import (
	"io"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	jmCreateFilesReplyMarker = 0xA5
	jmCreateFilesMaxCount    = 64
	jmCreateFilesMaxBytes    = chunk.SmallUploadBytes // per file: a small save
	jmCreateFilesWorkers     = 16
)

// jmFileCreator is implemented by the metadata engines (pkg/meta/jm_create_file.go).
type jmFileCreator interface {
	JMCreateFileSupported() bool
	JMCreateFilesIn(ctx meta.Context, parent meta.Ino, files []*meta.JMNewFile) syscall.Errno
}

type jmNewFile struct {
	parent       meta.Ino
	preIno       meta.Ino
	mode, cumask uint16
	mtime        time.Time
	name         string
	data         []byte
	slice        meta.Slice
	eno          syscall.Errno
	ino          meta.Ino
	// recognized: an earlier send created the file; this send's object is
	// unreferenced.
	recognized bool
}

// jmParseCreateFiles reads the message body. Any malformed field fails the
// whole message with EINVAL.
func jmParseCreateFiles(r *utils.Buffer) (int, []jmNewFile, syscall.Errno) {
	if r.Left() < 2 {
		return 0, nil, syscall.EINVAL
	}
	n := int(r.Get16())
	if n == 0 || n > jmCreateFilesMaxCount {
		return n, nil, syscall.EINVAL
	}
	files := make([]jmNewFile, n)
	for i := range files {
		if r.Left() < 8+8+2+2+8+4+1 {
			return n, nil, syscall.EINVAL
		}
		f := &files[i]
		f.parent = meta.Ino(r.Get64())
		f.preIno = meta.Ino(r.Get64())
		f.mode = r.Get16()
		f.cumask = r.Get16()
		sec := r.Get64()
		nsec := r.Get32()
		if nsec >= 1e9 || sec > 1<<62 {
			return n, nil, syscall.EINVAL
		}
		f.mtime = time.Unix(int64(sec), int64(nsec))
		nl := int(r.Get8())
		if r.Left() < nl+4 {
			return n, nil, syscall.EINVAL
		}
		f.name = string(r.Get(nl))
		if nl == 0 {
			f.eno = syscall.EINVAL // this file only
		}
		dl := int(r.Get32())
		if dl > jmCreateFilesMaxBytes || r.Left() < dl {
			return n, nil, syscall.EINVAL
		}
		// Copy: the buffer is the handle's pending bytes, which the next
		// write on the handle reuses.
		f.data = append([]byte(nil), r.Get(dl)...)
	}
	if r.HasMore() {
		return n, nil, syscall.EINVAL
	}
	return n, files, 0
}

// jmDefiniteRefusal: the metadata transaction wrote nothing, so the object
// uploaded for it is unreferenced and may be removed at once. After any other
// error (an ambiguous EXEC reply) the object is kept for the garbage
// collector: the name may hold it.
func jmDefiniteRefusal(eno syscall.Errno) bool {
	switch eno {
	case syscall.EEXIST, syscall.ENOENT, syscall.ENOTDIR, syscall.EACCES, syscall.EPERM, syscall.EROFS,
		syscall.EDQUOT, syscall.ENOSPC, syscall.ENOTSUP, syscall.EINVAL, syscall.ENAMETOOLONG,
		// EAGAIN: another create holds the number; ESRCH: the number is not
		// held and the name does not hold it. Neither writes anything.
		syscall.EAGAIN, syscall.ESRCH:
		return true
	}
	return false
}

// A folder this message cannot create in (a default ACL; ENOTSUP) is
// remembered for a minute, so the next batches for it do not upload bytes
// that would only be removed: the caller uses the ordinary create for them.
const jmParentRefusedFor = time.Minute

var (
	jmRefusedMu sync.Mutex
	jmRefused   = map[meta.Ino]time.Time{}
)

func jmNoteParentRefused(parent meta.Ino) {
	jmRefusedMu.Lock()
	defer jmRefusedMu.Unlock()
	if len(jmRefused) > 4096 {
		jmRefused = map[meta.Ino]time.Time{}
	}
	jmRefused[parent] = time.Now()
}

func jmParentRefused(parent meta.Ino) bool {
	jmRefusedMu.Lock()
	defer jmRefusedMu.Unlock()
	at, ok := jmRefused[parent]
	if ok && time.Since(at) > jmParentRefusedFor {
		delete(jmRefused, parent)
		return false
	}
	return ok
}

// jmUpload stores f's bytes as one object; f.slice names it.
func (v *VFS) jmUpload(ctx meta.Context, f *jmNewFile) {
	if f.eno != 0 || len(f.data) == 0 {
		return
	}
	if jmParentRefused(f.parent) {
		f.eno = syscall.ENOTSUP // a folder that refused lately: no upload
		return
	}
	var id uint64
	if st := v.Meta.NewSlice(ctx, &id); st != 0 {
		f.eno = st
		return
	}
	w := v.Store.NewWriter(id)
	// The reply promises the bytes are in the object store.
	w.SetWriteback(false)
	if m, ok := w.(smallSaveMarker); ok {
		m.SetSmallSave()
	}
	if _, err := w.WriteAt(f.data, 0); err != nil {
		w.Abort()
		f.eno = syscall.EIO
		return
	}
	if err := w.Finish(len(f.data)); err != nil {
		logger.Warnf("JMCreateFiles: upload %d/%s: %s", f.parent, f.name, err)
		w.Abort()
		f.eno = syscall.EIO
		return
	}
	f.slice = meta.Slice{Id: id, Size: uint32(len(f.data)), Len: uint32(len(f.data))}
}

// jmCreateIn creates the uploaded files of one folder in one transaction.
func (v *VFS) jmCreateIn(ctx meta.Context, jm jmFileCreator, parent meta.Ino, group []*jmNewFile) {
	var reqs []*meta.JMNewFile
	var mine []*jmNewFile
	for _, f := range group {
		if f.eno != 0 {
			continue
		}
		reqs = append(reqs, &meta.JMNewFile{Name: f.name, Mode: f.mode, Cumask: f.cumask, Mtime: f.mtime, Slice: f.slice, PreIno: f.preIno})
		mine = append(mine, f)
	}
	if len(reqs) > 0 {
		st := jm.JMCreateFilesIn(ctx, parent, reqs)
		if st == syscall.ENOTSUP {
			jmNoteParentRefused(parent)
		}
		for i, f := range mine {
			if st != 0 {
				f.eno = st
			} else {
				f.eno, f.ino = reqs[i].Errno, reqs[i].Inode
				f.recognized = reqs[i].Errno == 0 && reqs[i].RecognizedEarlierSend()
			}
		}
	}
	for _, f := range group {
		if f.eno != 0 {
			f.ino = 0
			if f.slice.Id != 0 && jmDefiniteRefusal(f.eno) {
				if err := v.Store.Remove(f.slice.Id, int(f.slice.Size)); err != nil {
					logger.Debugf("JMCreateFiles: remove unreferenced slice %d: %s", f.slice.Id, err)
				}
			}
			continue
		}
		if f.recognized && f.slice.Id != 0 {
			if err := v.Store.Remove(f.slice.Id, int(f.slice.Size)); err != nil {
				logger.Debugf("JMCreateFiles: remove unreferenced slice %d of a recognised resend: %s", f.slice.Id, err)
			}
		}
		if v.InvalidateEntry != nil {
			// The kernel may hold a negative entry for the name; the file was
			// not created through it.
			if st := v.InvalidateEntry(f.parent, f.name); st != 0 {
				logger.Debugf("Invalidate entry %d/%s: %s", f.parent, f.name, st)
			}
		}
	}
}

func (v *VFS) jmHandleCreateFiles(hctx meta.Context, r *utils.Buffer, out io.Writer) {
	// Not the handle's context: a FLUSH of the control file cancels that one,
	// and any process the caller forks sends a FLUSH when its exec closes the
	// inherited descriptor. A canceled batch would stop after its uploads.
	// The work is bounded (at most 64 small files), so it always runs to its
	// end and answers every file.
	ctx := meta.NewContext(hctx.Pid(), hctx.Uid(), hctx.Gids())
	n, files, eno := jmParseCreateFiles(r)
	jm, ok := v.Meta.(jmFileCreator)
	if eno == 0 && (!ok || !jm.JMCreateFileSupported()) {
		eno = syscall.ENOTSUP
	}
	if eno == 0 && v.Conf != nil && v.Conf.Meta != nil && v.Conf.Meta.ReadOnly {
		eno = syscall.EROFS
	}
	if eno == 0 {
		// Uploads run in parallel; then each folder's files are created in
		// one transaction (JuiceFS runs one per folder at a time, so one
		// transaction per file would queue).
		sem := make(chan struct{}, jmCreateFilesWorkers)
		var wg sync.WaitGroup
		for i := range files {
			wg.Add(1)
			sem <- struct{}{}
			go func(f *jmNewFile) {
				defer func() { <-sem; wg.Done() }()
				v.jmUpload(ctx, f)
			}(&files[i])
		}
		wg.Wait()
		groups := map[meta.Ino][]*jmNewFile{}
		var order []meta.Ino
		for i := range files {
			p := files[i].parent
			if _, ok := groups[p]; !ok {
				order = append(order, p)
			}
			groups[p] = append(groups[p], &files[i])
		}
		for _, p := range order {
			wg.Add(1)
			sem <- struct{}{}
			go func(p meta.Ino, g []*jmNewFile) {
				defer func() { <-sem; wg.Done() }()
				v.jmCreateIn(ctx, jm, p, g)
			}(p, groups[p])
		}
		wg.Wait()
	}
	if n < 0 || n > jmCreateFilesMaxCount {
		n = 0
	}
	w := utils.NewBuffer(uint32(4 + 9*n))
	w.Put8(jmCreateFilesReplyMarker)
	w.Put8(uint8(eno))
	w.Put16(uint16(n))
	for i := 0; i < n; i++ {
		if eno != 0 || files == nil {
			w.Put8(uint8(eno))
			w.Put64(0)
			continue
		}
		w.Put8(uint8(files[i].eno))
		w.Put64(uint64(files[i].ino))
	}
	_, _ = out.Write(w.Bytes())
}
