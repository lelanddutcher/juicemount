package respproxy

// projcache.go is the directory -> project memo (H8 plan §1.4 step 4). A
// walk up the directory tree is memoised per directory; every memo entry
// carries the generation it was computed under. A unit that changes the
// shape of the tree above a project boundary (a directory moved between
// parents, a project root created by a move, a restore from trash) bumps
// the generation when it commits, so no decision after that commit can use
// a project computed from the old shape.

import "sync"

// nsKind is the namespace a directory or inode belongs to (plan §1.1).
type nsKind uint8

const (
	nsUnknown     nsKind = iota // could not be resolved: fail closed
	nsRoot                      // depth < project depth: the shared top of the tree
	nsProject                   // inside a project (keyed by the project root inode)
	nsTrash                     // inode >= TrashInode, or below a trash directory
	nsDerivatives               // /.juicemount/derivatives/**
)

func (k nsKind) String() string {
	switch k {
	case nsRoot:
		return "ns:root"
	case nsProject:
		return "project"
	case nsTrash:
		return "ns:trash"
	case nsDerivatives:
		return "ns:derivatives"
	}
	return "unknown"
}

// location is where a directory sits.
type location struct {
	kind  nsKind
	proj  uint64 // project root inode (nsProject, nsDerivatives)
	depth int    // the directory's depth (root = 0); -1 when unknown
}

type memoEnt struct {
	loc location
	gen uint64
}

// memoMax bounds the memo. On overflow it is dropped and rebuilt from the
// in-memory parent map, which costs map lookups, not side reads.
const memoMax = 2 << 20

type projCache struct {
	mu  sync.RWMutex
	gen uint64
	m   map[uint64]memoEnt
}

func newProjCache() *projCache { return &projCache{m: map[uint64]memoEnt{}} }

// generation is the current memo generation.
func (c *projCache) generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gen
}

func (c *projCache) get(dir uint64) (location, bool) {
	c.mu.RLock()
	e, ok := c.m[dir]
	gen := c.gen
	c.mu.RUnlock()
	if !ok || e.gen != gen {
		return location{}, false
	}
	return e.loc, true
}

// put stores loc for dir if it was computed under generation gen. A walk
// that raced with a bump is not memoised.
func (c *projCache) put(dir uint64, loc location, gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return
	}
	if len(c.m) >= memoMax {
		c.m = map[uint64]memoEnt{}
	}
	c.m[dir] = memoEnt{loc: loc, gen: gen}
}

// bump invalidates every memo entry.
func (c *projCache) bump() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	c.m = map[uint64]memoEnt{}
}

func (c *projCache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}
