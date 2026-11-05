package binder

import (
	"container/list"
	"sync/atomic"
)

// pathCache is a bounded least-recently-used map of path -> inode, used only
// by the worker. Evictions are counted: a replaced file whose path was
// evicted is not noticed (its location stays active until a rescan).
type pathCache struct {
	max       int
	order     *list.List // front = most recent; values are *pathEntry
	entries   map[string]*list.Element
	evictions atomic.Uint64 // read by Stats
}

type pathEntry struct {
	path string
	ino  uint64
}

func newPathCache(max int) *pathCache {
	return &pathCache{max: max, order: list.New(), entries: map[string]*list.Element{}}
}

func (c *pathCache) get(p string) (uint64, bool) {
	e, ok := c.entries[p]
	if !ok {
		return 0, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*pathEntry).ino, true
}

func (c *pathCache) put(p string, ino uint64) {
	if e, ok := c.entries[p]; ok {
		e.Value.(*pathEntry).ino = ino
		c.order.MoveToFront(e)
		return
	}
	c.entries[p] = c.order.PushFront(&pathEntry{p, ino})
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*pathEntry).path)
		c.evictions.Add(1)
	}
}

func (c *pathCache) remove(p string) {
	if e, ok := c.entries[p]; ok {
		c.order.Remove(e)
		delete(c.entries, p)
	}
}
