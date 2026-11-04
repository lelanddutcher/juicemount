package nfs

import (
	"os"
	"sync"
	"time"
)

// A namespace mutation that outlives the RPC mutation bound is answered
// NFS3ERR_JUKEBOX while its worker keeps the names and finishes the change
// (namespace_operations.go). The client then resends the same request, and by
// then the change it asked for has already happened. For RENAME that resend is
// not harmless: the source is gone, and the ordinary path cancels the
// destination's spool entry before discovering that, discarding the bytes of
// the file that was just renamed there and poisoning both names.
//
// completedMutations remembers each such late success briefly, so the resend
// of exactly that request is answered success without running it again. There
// is no XID duplicate-request cache to lean on: the macOS client gives a
// JUKEBOX retry a new XID (NFS kext nfs_request_finish: "req->r_xid = 0; //
// get a new XID"). The key is therefore the request itself: operation, both
// names and the RPC credential. An entry is replayed at most once, and only
// while the namespace still shows the outcome that worker produced.
//
// The TTL follows the same client code. It waits NFS_TRYLATERDEL (4 s) before
// the first resend and doubles the wait up to a 30 s cap. A resend that lands
// while the worker still owns the names is answered JUKEBOX again, so the
// resend that finds the finished state arrives within one cap interval of the
// worker finishing. 60 s covers that interval twice over; anything later is
// not this client's retry of this request.
const (
	completedMutationTTL = 60 * time.Second
	completedMutationCap = 256
)

type completedMutationKey struct {
	op        string
	principal string
	from, to  string
}

// mutationIdentity is the object a completed mutation left at its target name.
type mutationIdentity struct {
	inode       uint64
	isDir       bool
	incarnation string
}

type completedMutation struct {
	identity mutationIdentity
	recorded time.Time
}

type completedMutations struct {
	mu      sync.Mutex
	entries map[completedMutationKey]completedMutation
	now     func() time.Time // tests only
}

func (c *completedMutations) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *completedMutations) record(key completedMutationKey, id mutationIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if c.entries == nil {
		c.entries = make(map[completedMutationKey]completedMutation)
	}
	if _, ok := c.entries[key]; !ok && len(c.entries) >= completedMutationCap {
		var oldest completedMutationKey
		var oldestAt time.Time
		for k, e := range c.entries {
			if now.Sub(e.recorded) >= completedMutationTTL {
				delete(c.entries, k)
				continue
			}
			if oldestAt.IsZero() || e.recorded.Before(oldestAt) {
				oldest, oldestAt = k, e.recorded
			}
		}
		if len(c.entries) >= completedMutationCap {
			delete(c.entries, oldest)
		}
	}
	c.entries[key] = completedMutation{identity: id, recorded: now}
}

// take removes and returns the live entry for key. Removal is unconditional:
// a caller that finds the namespace changed must not be offered it again.
func (c *completedMutations) take(key completedMutationKey) (mutationIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return mutationIdentity{}, false
	}
	delete(c.entries, key)
	if c.clock().Sub(e.recorded) >= completedMutationTTL {
		return mutationIdentity{}, false
	}
	return e.identity, true
}

// SetMutationPrincipal scopes remembered late mutations to the RPC
// credential that sent them (see completedMutations). FromHandle returns a
// fresh filesystem value per RPC, so this never leaks across requests.
func (jfs *juiceFS) SetMutationPrincipal(principal string) {
	jfs.retryPrincipal = principal
}

// renameOnce is rename with replay of a late success: the resend of a rename
// whose worker already finished is answered from completedMutations instead
// of being run a second time. Runs inside the namespace worker, owning both
// names.
func (jfs *juiceFS) renameOnce(oldpath, newpath string, op *namespaceOperation) error {
	key := completedMutationKey{op: "rename", principal: jfs.retryPrincipal, from: oldpath, to: newpath}
	remember := func(id mutationIdentity) func() {
		return func() { jfs.handler.completedMutations.record(key, id) }
	}
	if id, ok := jfs.handler.completedMutations.take(key); ok && jfs.renameApplied(oldpath, newpath, id) {
		// If this reply is lost to the bound as well, the next resend
		// still needs the answer.
		op.completed = remember(id)
		op.checkpoint("replay", oldpath)
		return nil
	}
	if err := jfs.rename(oldpath, newpath, op); err != nil {
		return err
	}
	op.completed = func() {
		if id, ok := jfs.renameIdentity(newpath); ok {
			remember(id)()
		}
	}
	return nil
}

func (jfs *juiceFS) renameIdentity(newpath string) (mutationIdentity, bool) {
	e := jfs.handler.store.LookupByPath(newpath)
	if e == nil {
		return mutationIdentity{}, false
	}
	return mutationIdentity{inode: e.Inode, isDir: e.IsDir, incarnation: e.Version.Incarnation}, true
}

// renameApplied reports whether the namespace still shows exactly what the
// remembered rename produced: nothing at the source (mirror, spool and
// backend all agree) and the same object at the destination. A recreated
// source makes this a new rename; a replaced destination means something
// else ran in between. Either way the request is not answered from memory.
func (jfs *juiceFS) renameApplied(oldpath, newpath string, id mutationIdentity) bool {
	if jfs.handler.store.LookupByPath(oldpath) != nil {
		return false
	}
	if spool := jfs.handler.spool.Load(); spool != nil {
		if _, ok := spool.LookupActive(oldpath); ok {
			return false
		}
	}
	if _, err := os.Lstat(jfs.fullPath(oldpath)); !os.IsNotExist(err) {
		return false
	}
	got, ok := jfs.renameIdentity(newpath)
	return ok && got == id
}
