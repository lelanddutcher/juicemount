package meta

import (
	"sync"
	"time"
)

const (
	invalidateAllChunks = 0xFFFFFFFF
	invalidateAttrOnly  = 0xFFFFFFFE
)

var ofPool = sync.Pool{
	New: func() interface{} {
		return &openFile{}
	},
}

type openFile struct {
	sync.RWMutex
	attr      Attr
	refs      int
	lastCheck int64
	first     []Slice
	chunks    map[uint32][]Slice
	// attrAt is when attr was last read from the metadata engine with no
	// invalidation on this client while the read was in flight; zero once
	// invalidated (patch 0028).
	attrAt time.Time
	// firstAt is when Open seeded first from its own round trip; zero once
	// a Read used it or anything replaced it (patch 0028).
	firstAt time.Time
	// firstRaw is the length of the raw list behind a seeded first, for the
	// compaction decision Read makes when it uses it.
	firstRaw int
	// jmGen counts the slice-list invalidations of this file (patch 0032).
	jmGen uint64
}

func (o *openFile) invalidateChunk() {
	o.jmGen++ // JuiceMount patch 0032
	o.first = nil
	o.firstAt = time.Time{}
	for c := range o.chunks {
		delete(o.chunks, c)
	}
}

func (o *openFile) release() {
	o.attr = Attr{}
	o.refs = 0
	o.lastCheck = 0
	o.first = nil
	o.chunks = nil
	o.attrAt = time.Time{}
	o.firstAt = time.Time{}
	o.firstRaw = 0
	ofPool.Put(o)
}

type openfiles struct {
	sync.Mutex
	expire time.Duration
	limit  uint64
	files  map[Ino]*openFile
	// gen counts every chunk or attribute invalidation of any open file. A
	// caller that read slices before taking the lock compares it to know that
	// nothing changed in between (patch 0028).
	gen uint64
}

func newOpenFiles(expire time.Duration, limit uint64) *openfiles {
	of := &openfiles{
		expire: expire,
		limit:  limit,
		files:  make(map[Ino]*openFile),
	}
	go of.cleanup()
	return of
}

func (o *openfiles) cleanup() {
	for {
		var (
			cnt, deleted, todel int
			candidateIno        Ino
			candidateOf         *openFile
		)
		o.Lock()
		if o.limit > 0 && len(o.files) > int(o.limit) {
			todel = len(o.files) - int(o.limit)
		}
		now := time.Now().Unix()
		for ino, of := range o.files {
			cnt++
			if cnt > 1e3 || todel > 0 && deleted >= todel {
				break
			}
			if of.refs <= 0 {
				if now-of.lastCheck > 3600*12 {
					of.release()
					delete(o.files, ino)
					deleted++
					continue
				}
				if todel == 0 {
					continue
				}
				if candidateIno == 0 {
					candidateIno = ino
					candidateOf = of
					continue
				}
				if of.lastCheck < candidateOf.lastCheck {
					candidateIno = ino
					candidateOf = of
				}
				candidateOf.release()
				delete(o.files, candidateIno)
				deleted++
				candidateIno = 0
			}
		}
		o.Unlock()
		time.Sleep(time.Millisecond * time.Duration(1000*(cnt+1-deleted*2)/(cnt+1)))
	}
}

func (o *openfiles) OpenCheck(ino Ino, attr *Attr) bool {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if ok && time.Second*time.Duration(time.Now().Unix()-of.lastCheck) < o.expire {
		if attr != nil {
			*attr = of.attr
		}
		of.refs++
		return true
	}
	return false
}

func (o *openfiles) Open(ino Ino, attr *Attr) {
	o.Lock()
	defer o.Unlock()
	o.openLocked(ino, attr)
}

func (o *openfiles) openLocked(ino Ino, attr *Attr) {
	of, ok := o.files[ino]
	if !ok {
		of = ofPool.Get().(*openFile)
		of.jmGen++ // JuiceMount patch 0032: a pooled entry is a new file (A-B-A)
		o.files[ino] = of
	} else if attr != nil && attr.Mtime == of.attr.Mtime && attr.Mtimensec == of.attr.Mtimensec {
		attr.KeepCache = of.attr.KeepCache
	} else {
		of.invalidateChunk()
		o.gen++
	}
	if attr != nil {
		of.attr = *attr
	}
	// next open can keep cache if not modified
	of.attr.KeepCache = true
	of.refs++
	of.lastCheck = time.Now().Unix()
}

func (o *openfiles) Close(ino Ino) bool {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if ok {
		of.refs--
		return of.refs <= 0
	}
	return true
}

func (o *openfiles) Check(ino Ino, attr *Attr) bool {
	if attr == nil {
		panic("attr is nil")
	}
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if ok && time.Second*time.Duration(time.Now().Unix()-of.lastCheck) < o.expire {
		*attr = of.attr
		return true
	}
	return false
}

func (o *openfiles) Update(ino Ino, attr *Attr) bool {
	if attr == nil {
		return false
	}
	o.Lock()
	defer o.Unlock()
	return o.updateLocked(ino, attr)
}

// UpdateFresh is Update for an attr read from the engine after gen was taken
// from chunkGen. The attr may be served by RecentOpenAttr only when no
// invalidation happened while the read was in flight: a reply sent before a
// local write can land after it (patch 0028).
func (o *openfiles) UpdateFresh(ino Ino, attr *Attr, gen uint64) bool {
	if attr == nil {
		return false
	}
	o.Lock()
	defer o.Unlock()
	fresh := o.gen == gen
	if !o.updateLocked(ino, attr) {
		return false
	}
	if fresh {
		o.files[ino].attrAt = time.Now()
	}
	return true
}

func (o *openfiles) updateLocked(ino Ino, attr *Attr) bool {
	of, ok := o.files[ino]
	if ok {
		if attr.Mtime != of.attr.Mtime || attr.Mtimensec != of.attr.Mtimensec {
			of.invalidateChunk()
			o.gen++
		} else {
			attr.KeepCache = of.attr.KeepCache
		}
		of.attr = *attr
		of.lastCheck = time.Now().Unix()
		return true
	}
	return false
}

func (o *openfiles) IsOpen(ino Ino) bool {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	return ok && of.refs > 0
}

func (o *openfiles) ReadChunk(ino Ino, indx uint32) ([]Slice, bool) {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if !ok {
		return nil, false
	}
	if indx == 0 {
		return of.first, of.first != nil
	} else {
		cs, ok := of.chunks[indx]
		return cs, ok
	}
}

func (o *openfiles) CacheChunk(ino Ino, indx uint32, cs []Slice) {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if !ok {
		return
	}
	if indx == 0 {
		of.first = cs
		of.firstAt = time.Time{}
	} else {
		if of.chunks == nil {
			of.chunks = make(map[uint32][]Slice)
		}
		of.chunks[indx] = cs
	}
}

func (o *openfiles) InvalidateChunk(ino Ino, indx uint32) {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if ok {
		of.jmGen++ // JuiceMount patch 0032
		if indx == invalidateAllChunks {
			of.invalidateChunk()
		} else if indx == 0 {
			of.first = nil
			of.firstAt = time.Time{}
		} else {
			delete(of.chunks, indx)
		}
		of.lastCheck = 0
		of.attrAt = time.Time{}
	}
	o.gen++
}

// chunkGen returns the invalidation counter (patch 0028).
func (o *openfiles) chunkGen() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.gen
}

// OpenPrefetched is Open for a caller that read attr and the first chunk's
// slices (raw: the length of the raw list) in one metadata round trip, after
// taking gen from chunkGen. Only when no invalidation happened since gen was
// taken and the file was not open with another mtime are the attr marked
// fresh and the slices cached; a Read uses them within ReadPrefetched's
// window only (patch 0028). It reports whether the slices were cached.
func (o *openfiles) OpenPrefetched(ino Ino, attr *Attr, first []Slice, raw int, gen uint64) bool {
	o.Lock()
	defer o.Unlock()
	unchanged := o.gen == gen
	of, ok := o.files[ino]
	if ok && (attr.Mtime != of.attr.Mtime || attr.Mtimensec != of.attr.Mtimensec) {
		unchanged = false
	}
	o.openLocked(ino, attr)
	if !unchanged {
		return false
	}
	of = o.files[ino]
	of.attrAt = time.Now()
	if len(first) == 0 || of.first != nil {
		return false
	}
	of.first = first
	of.firstAt = of.attrAt
	of.firstRaw = raw
	return true
}

// ReadPrefetched is ReadChunk for Read. A list that Open seeded is used once,
// within window of the open; later it is dropped, so a first read long after
// the open fetches the list as upstream does. raw > 0 tells the caller it used
// a seeded list of that raw length (patch 0028).
func (o *openfiles) ReadPrefetched(ino Ino, indx uint32, window time.Duration) (cs []Slice, ok bool, raw int) {
	o.Lock()
	defer o.Unlock()
	of, found := o.files[ino]
	if !found {
		return nil, false, 0
	}
	if indx != 0 {
		cs, ok = of.chunks[indx]
		return cs, ok, 0
	}
	if !of.firstAt.IsZero() {
		seeded := time.Since(of.firstAt) < window
		raw = of.firstRaw
		of.firstAt, of.firstRaw = time.Time{}, 0
		if !seeded {
			of.first = nil
			return nil, false, 0
		}
		return of.first, of.first != nil, raw
	}
	return of.first, of.first != nil, 0
}

// RecentOpenAttr serves a GetAttr of a file this client holds open from the
// attr read fresh within window, the read that the open itself made. The
// kernel asks again right after an open; this answers without another
// round trip (patch 0028).
func (o *openfiles) RecentOpenAttr(ino Ino, attr *Attr, window time.Duration) bool {
	o.Lock()
	defer o.Unlock()
	of, ok := o.files[ino]
	if !ok || of.refs <= 0 || of.attrAt.IsZero() || time.Since(of.attrAt) >= window {
		return false
	}
	*attr = of.attr
	return true
}

func (o *openfiles) find(ino Ino) *openFile {
	o.Lock()
	defer o.Unlock()
	return o.files[ino]
}
