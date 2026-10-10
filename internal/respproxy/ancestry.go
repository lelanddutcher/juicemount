package respproxy

// ancestry.go resolves inodes to directories and directories to namespaces
// (H8 plan §1.4 steps 3-4).
//
//   - dirParent: every directory's parent, learned from the bootstrap scan
//     (bootstrap.go), from committed units, and from side reads.
//   - files: a bounded file -> parent cache learned from the wire.
//   - memo: directory -> location, generation-counted (projcache.go).
//
// A miss costs O(depth) side GETs on the proxy's own upstream pool, never on
// a client connection.

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
)

// sideDoer is the proxy's side pool (Proxy.Side()).
type sideDoer interface {
	Do(db int, args ...[]byte) (Value, error)
}

// fileLRU is an approximate LRU of file -> parent for files with exactly
// one link (Attr.Parent != 0): two generations of maps. Lookups promote
// from the old generation; when the current one reaches half the capacity
// it becomes the old one. Memory stays bounded at about cap entries.
// Hard-linked files are never cached: their parents are always read.
type fileLRU struct {
	mu       sync.Mutex
	cap      int
	cur, old map[uint64]uint64
}

func newFileLRU(capacity int) *fileLRU {
	if capacity < 2 {
		capacity = 2
	}
	return &fileLRU{cap: capacity, cur: map[uint64]uint64{}, old: map[uint64]uint64{}}
}

func (l *fileLRU) get(n uint64) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p, ok := l.cur[n]; ok {
		return p, true
	}
	if p, ok := l.old[n]; ok {
		l.putLocked(n, p)
		return p, true
	}
	return 0, false
}

func (l *fileLRU) putLocked(n, p uint64) {
	if len(l.cur) >= l.cap/2 {
		l.old, l.cur = l.cur, make(map[uint64]uint64, l.cap/2)
	}
	l.cur[n] = p
}

func (l *fileLRU) del(n uint64) {
	l.mu.Lock()
	delete(l.cur, n)
	delete(l.old, n)
	l.mu.Unlock()
}

func (l *fileLRU) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.cur) + len(l.old)
}

// tombstones order cache fills that come from side reads against
// invalidations from committed (or possibly committed) units. A side read
// records the sequence before it reads; its result is cached only if the
// inode was not invalidated after that point. Without this a read that
// raced a committed rename could re-insert the old parent after the
// rename's invalidation (review H8B-L2 #4).
type tombstones struct {
	mu    sync.Mutex
	seq   uint64
	floor uint64 // reads that started before floor are always rejected
	m     map[uint64]uint64
}

const tombMax = 1 << 18

func (t *tombstones) now() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seq
}

func (t *tombstones) kill(ns []uint64) {
	if len(ns) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq++
	if len(t.m)+len(ns) > tombMax {
		t.m = map[uint64]uint64{}
		t.floor = t.seq
	}
	for _, n := range ns {
		t.m[n] = t.seq
	}
}

// fresh: a side read of n that started at sequence start may be cached.
func (t *tombstones) fresh(n, start uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if start < t.floor {
		return false
	}
	return t.m[n] <= start
}

const rootIno = 1

var errUnresolved = errors.New("respproxy: inode not resolvable")

type ancestry struct {
	side  sideDoer
	db    int
	depth int // project depth (jmlease:v1:cfg depth, default 2)

	mu        sync.RWMutex
	dirParent map[uint64]uint64
	names     map[uint64]string // display path of directories at depth <= project depth
	jmDir     uint64            // /.juicemount
	derivRoot uint64            // /.juicemount/derivatives

	files *fileLRU
	memo  *projCache
	tomb  tombstones

	sideReads atomic.Int64
}

func newAncestry(side sideDoer, db, depth, fileCap int) *ancestry {
	if depth < 1 {
		depth = 2
	}
	return &ancestry{
		side: side, db: db, depth: depth,
		dirParent: map[uint64]uint64{rootIno: rootIno},
		names:     map[uint64]string{rootIno: "/"},
		files:     newFileLRU(fileCap),
		memo:      newProjCache(),
		tomb:      tombstones{m: map[uint64]uint64{}},
	}
}

// getAttr side-reads i<n>. found=false: the key does not exist.
func (a *ancestry) getAttr(n uint64) (attrInfo, bool, error) {
	a.sideReads.Add(1)
	v, err := a.side.Do(a.db, []byte("GET"), []byte("i"+strconv.FormatUint(n, 10)))
	if err != nil {
		return attrInfo{}, false, err
	}
	if v.Err() != nil {
		return attrInfo{}, false, v.Err()
	}
	if v.Null {
		return attrInfo{}, false, nil
	}
	ai, ok := parseAttr(v.Str)
	if !ok {
		return attrInfo{}, false, errUnresolved
	}
	return ai, true, nil
}

// cacheDir records a directory edge read from Redis, unless the directory
// was invalidated after the read began.
func (a *ancestry) cacheDir(d, parent, start uint64) {
	if !a.tomb.fresh(d, start) {
		return
	}
	a.mu.Lock()
	if _, ok := a.dirParent[d]; !ok {
		a.dirParent[d] = parent
	}
	a.mu.Unlock()
}

func (a *ancestry) cacheFile(n, parent, start uint64) {
	if !a.tomb.fresh(n, start) {
		return
	}
	a.files.mu.Lock()
	a.files.putLocked(n, parent)
	a.files.mu.Unlock()
}

// parentOfDir returns the parent of directory d.
func (a *ancestry) parentOfDir(d uint64) (uint64, error) {
	a.mu.RLock()
	p, ok := a.dirParent[d]
	a.mu.RUnlock()
	if ok {
		return p, nil
	}
	start := a.tomb.now()
	ai, found, err := a.getAttr(d)
	if err != nil {
		return 0, err
	}
	if !found || ai.typ != typeDir || ai.parent == 0 {
		return 0, errUnresolved
	}
	a.cacheDir(d, ai.parent, start)
	return ai.parent, nil
}

func (a *ancestry) isDerivRoot(d uint64) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.derivRoot != 0 && d == a.derivRoot
}

func (a *ancestry) projectLoc(root uint64, depth int) location {
	if a.isDerivRoot(root) {
		return location{kind: nsDerivatives, proj: root, depth: depth}
	}
	return location{kind: nsProject, proj: root, depth: depth}
}

// resolveDir returns the location of directory d.
func (a *ancestry) resolveDir(d uint64) (location, error) {
	if isTrashIno(d) {
		return location{kind: nsTrash, depth: -1}, nil
	}
	if d == rootIno {
		return location{kind: nsRoot, depth: 0}, nil
	}
	if loc, ok := a.memo.get(d); ok {
		return loc, nil
	}
	gen := a.memo.generation()
	chain := []uint64{d}
	cur := d
	for cur != rootIno {
		if len(chain) > 4096 {
			return location{kind: nsUnknown, depth: -1}, errUnresolved // a cycle
		}
		p, err := a.parentOfDir(cur)
		if err != nil {
			return location{kind: nsUnknown, depth: -1}, err
		}
		if isTrashIno(p) {
			loc := location{kind: nsTrash, depth: -1}
			a.memo.put(d, loc, gen)
			return loc, nil
		}
		chain = append(chain, p)
		cur = p
	}
	depth := len(chain) - 1
	if depth < a.depth {
		loc := location{kind: nsRoot, depth: depth}
		a.memo.put(d, loc, gen)
		return loc, nil
	}
	root := chain[len(chain)-1-a.depth]
	for i := 0; i <= depth-a.depth; i++ {
		loc := a.projectLoc(root, depth-i)
		a.memo.put(chain[i], loc, gen)
	}
	return a.projectLoc(root, depth), nil
}

// childLoc is the location of inode child (a directory when isDir) whose
// parent directory is dir.
func (a *ancestry) childLoc(dir, child uint64, isDir bool) (location, error) {
	return a.childLocP(dir, child, isDir, false)
}

// childLocP: placed means the child is being created or moved into dir by
// the unit itself. A directory placed directly under a directory one above
// the project depth is a project root being created there: an ns:root
// operation that does not need the new project's lease.
func (a *ancestry) childLocP(dir, child uint64, isDir, placed bool) (location, error) {
	lp, err := a.resolveDir(dir)
	if err != nil {
		return lp, err
	}
	switch lp.kind {
	case nsRoot:
		if isDir && lp.depth == a.depth-1 && !placed {
			return a.projectLoc(child, lp.depth+1), nil
		}
		return location{kind: nsRoot, depth: lp.depth + 1}, nil
	case nsProject, nsDerivatives:
		lp.depth++
		return lp, nil
	}
	return lp, nil
}

// placement is where an inode is stored right now (Redis truth, or a cache
// of it that every committed or possibly-committed unit invalidates).
type placement struct {
	exists  bool
	isDir   bool
	parent  uint64   // Attr.Parent (0 = more than one link)
	parents []uint64 // every parent: [parent], or the p<n> fields
}

// placementOf returns the stored placement of inode n. A file cache hit is
// only ever a single-link file; hard links are always read (review
// H8B-L2 #1: a cache hit must never skip the hard-link check).
func (a *ancestry) placementOf(n uint64) (placement, error) {
	if n == rootIno {
		return placement{exists: true, isDir: true, parent: rootIno}, nil
	}
	a.mu.RLock()
	dp, isDir := a.dirParent[n]
	a.mu.RUnlock()
	if isDir {
		return placement{exists: true, isDir: true, parent: dp, parents: []uint64{dp}}, nil
	}
	if p, ok := a.files.get(n); ok {
		return placement{exists: true, parent: p, parents: []uint64{p}}, nil
	}
	start := a.tomb.now()
	ai, found, err := a.getAttr(n)
	if err != nil {
		return placement{}, err
	}
	if !found {
		return placement{}, nil
	}
	pl := placement{exists: true, isDir: ai.typ == typeDir, parent: ai.parent}
	switch {
	case ai.parent == 0:
		ps, err := a.linkParents(n)
		if err != nil {
			return placement{}, err
		}
		pl.parents = ps
	case pl.isDir:
		pl.parents = []uint64{ai.parent}
		a.cacheDir(n, ai.parent, start)
	default:
		pl.parents = []uint64{ai.parent}
		if !isTrashIno(ai.parent) {
			a.cacheFile(n, ai.parent, start)
		}
	}
	return pl, nil
}

// linkParents reads every parent recorded in p<n>.
func (a *ancestry) linkParents(n uint64) ([]uint64, error) {
	a.sideReads.Add(1)
	v, err := a.side.Do(a.db, []byte("HGETALL"), []byte("p"+strconv.FormatUint(n, 10)))
	if err != nil {
		return nil, err
	}
	if v.Err() != nil {
		return nil, v.Err()
	}
	var out []uint64
	for i := 0; i+1 < len(v.Elems); i += 2 {
		p, ok := digits(string(v.Elems[i].Str))
		cnt, _ := strconv.ParseInt(string(v.Elems[i+1].Str), 10, 64)
		if ok && cnt > 0 {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, errUnresolved
	}
	return out, nil
}

// locsOf is the location of n under every stored parent.
func (a *ancestry) locsOf(n uint64, pl placement) ([]location, error) {
	out := make([]location, 0, len(pl.parents))
	for _, p := range pl.parents {
		if isTrashIno(p) {
			out = append(out, location{kind: nsTrash, depth: -1})
			continue
		}
		var l location
		var err error
		if pl.isDir {
			l, err = a.resolveDir(n)
		} else {
			l, err = a.childLoc(p, n, false)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// resolveInode: the locations of an existing inode (JM.LEASE ino:<n>).
func (a *ancestry) resolveInode(n uint64) ([]location, error) {
	pl, err := a.placementOf(n)
	if err != nil {
		return nil, err
	}
	if !pl.exists {
		return nil, errUnresolved
	}
	return a.locsOf(n, pl)
}

// entryAt side-reads the entry name in directory dir (ok=false: none).
func (a *ancestry) entryAt(dir uint64, name string) (ino uint64, typ byte, ok bool, err error) {
	a.sideReads.Add(1)
	v, err := a.side.Do(a.db, []byte("HGET"), []byte("d"+strconv.FormatUint(dir, 10)), []byte(name))
	if err != nil {
		return 0, 0, false, err
	}
	if v.Err() != nil {
		return 0, 0, false, v.Err()
	}
	if v.Null {
		return 0, 0, false, nil
	}
	ino, ok1 := entryInode(v.Str)
	typ, ok2 := entryType(v.Str)
	if !ok1 || !ok2 {
		return 0, 0, false, errUnresolved
	}
	return ino, typ, true, nil
}

// learnCommitted applies a committed unit. Directory edges come only from
// the unit's dentry writes (never from an Attr.Parent value: review
// H8B-L2 #2). Every inode the unit touched is invalidated in the file
// cache, so its next use re-reads Redis.
func (a *ancestry) learnCommitted(f *unitFacts) {
	a.tomb.kill(f.touched)
	for _, n := range f.touched {
		a.files.del(n)
	}
	derivChanged := false
	a.mu.Lock()
	for _, e := range f.edges {
		if !e.isDir {
			continue
		}
		a.dirParent[e.child] = e.parent
		if e.name == "" || isTrashIno(e.parent) {
			continue
		}
		switch {
		case e.parent == rootIno:
			a.names[e.child] = "/" + e.name
			if e.name == ".juicemount" {
				a.jmDir = e.child
			}
		case a.names[e.parent] != "" && a.dirParent[e.parent] == rootIno && a.depth >= 2:
			a.names[e.child] = a.names[e.parent] + "/" + e.name
		}
		if a.jmDir != 0 && e.parent == a.jmDir && e.name == "derivatives" && a.derivRoot != e.child {
			a.derivRoot = e.child
			derivChanged = true
		}
	}
	a.mu.Unlock()
	if f.topology || derivChanged {
		a.memo.bump()
	}
}

// forget handles a unit whose outcome is unknown (Lost, fence timeout):
// drop everything it would have taught, invalidate every inode and
// directory it touched, and bump the generation, so the next use
// re-resolves from Redis (review H8B-L2 #3).
func (a *ancestry) forget(f *unitFacts) {
	a.tomb.kill(f.touched)
	for _, n := range f.touched {
		a.files.del(n)
	}
	a.mu.Lock()
	for _, n := range f.touched {
		if n != rootIno {
			delete(a.dirParent, n)
		}
	}
	a.mu.Unlock()
	a.memo.bump()
}

// learnBootstrap records directory edges read at startup.
func (a *ancestry) learnBootstrap(edges []unitEdge) {
	a.learnCommitted(&unitFacts{edges: edges})
}

// label is the display path of a project root ("" when unknown).
func (a *ancestry) label(root uint64) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.names[root]
}

func (a *ancestry) dirCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.dirParent)
}
