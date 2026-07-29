package nfs

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	fdIdleTimeout = 2 * time.Minute
	fdEvictTick   = 30 * time.Second
	// fdOrphanGrace bounds how long a displaced stale fd (see FlushStale /
	// displaceStaleLocked) stays open for its unreachable holders before the
	// evict loop closes it. The fd references a DEAD mount either way; the
	// grace only spares holders an ErrClosed during their (already-failing)
	// final reads.
	fdOrphanGrace = 2 * time.Minute
)

// FDPool manages a pool of reusable file descriptors for JuiceFS FUSE reads.
// Opening files on JuiceFS FUSE is expensive (~60ms); this pool amortizes
// that cost across many pread() calls on the same file.
//
// QA-37 fix: keyed by {path, write} so a previously-opened read fd does
// NOT get reused for a write call. Pre-fix, GetWrite would silently
// return a cached O_RDONLY fd if Get had opened one earlier (e.g. via
// Stat), and the next WriteAt would EBADF — surfacing to Finder as -36.
type fdKey struct {
	path  string
	write bool
}

type FDPool struct {
	mu      sync.Mutex
	entries map[fdKey]*poolEntry
	// orphans holds stale-but-held entries displaced by a fresh open — after
	// a remount (#12) or a rename/delete invalidation. Their holders' Releases
	// land on the NEW map entry (Release is key-based), so the orphan itself
	// can't be ref-tracked; it is closed by the evict loop after
	// fdOrphanGrace, which bounds how long a long-held handle keeps the fd
	// alive while its holder finishes. The refs those holders still owe are
	// tracked separately, on the key, as pendingRefs.
	orphans []orphanEntry
	// pendingRefs is refcount DEBT owed to a key by the holders of an entry
	// that was displaced out from under them (displaceStaleLocked). Release is
	// key-based, so those holders' Releases will land on whatever entry occupies
	// the key next; the debt is absorbed into that entry's initial refCount so a
	// mis-landed Release can never drop a LIVE holder's ref to 0 (which would
	// let Invalidate/evict close an fd mid-ReadAt).
	//
	// It lives on the POOL, keyed, rather than in a local variable inside
	// Get/GetWrite, because Get releases p.mu across the os.Open syscall: a
	// racing Get can win the insert in that window, and a local carry would be
	// applied to the new entry too late (or not at all) — proven by
	// TestFDPoolInvalidateRacesInFlightRead.
	//
	// Bounded: unclaimed debt is dropped by the evict loop after fdOrphanGrace,
	// the same window after which the corresponding orphan fd is closed.
	pendingRefs map[fdKey]pendingRef
	// openInFlight is the set of os.Open/os.OpenFile calls currently running
	// with p.mu RELEASED (Get/GetWrite drop the lock across the syscall because
	// a JuiceFS FUSE open costs ~60 ms and holding the pool mutex across it
	// convoys every other caller — the 2026-06-14 "error 100060" class).
	//
	// It exists to close the INVALIDATION-DURING-OPEN hole: an Invalidate that
	// lands in that window finds NO ENTRY at the key and no-ops, and the open
	// then pools an fd for the PRE-rename/PRE-delete inode — C1/C2/C3 again,
	// just narrowed from deterministic to racy. Invalidate/InvalidateTree/
	// FlushStale POISON the in-flight records they cover; a poisoned open is
	// still handed to its caller (the fd was valid when it was opened, and the
	// RPC that asked for it raced the rename) but is pooled as STALE, so no
	// LATER caller can ever be served that identity.
	//
	// Keyed by record pointer for O(1) retire. Bounded by the number of
	// CONCURRENT opens (dozens), which is also what makes poisoning cheap:
	// scanning it is orders of magnitude smaller than scanning p.entries.
	openInFlight map[*openRec]struct{}
	stopCh       chan struct{}
}

// openRec is one in-flight open, registered under p.mu before the lock is
// dropped for the syscall and retired under p.mu after it returns.
type openRec struct {
	key      fdKey
	poisoned bool
}

type pendingRef struct {
	refs  int
	since time.Time
}

type orphanEntry struct {
	fd       *os.File
	orphaned time.Time
}

type poolEntry struct {
	fd       *os.File
	lastUsed time.Time
	refCount int
	// stale marks an fd that must never be re-served because the identity
	// behind its key changed underneath it. Three producers:
	//
	//   - FlushStale (#12): the fd predates a FUSE remount and references
	//     the DEAD mount.
	//   - Invalidate/InvalidateTree (serving-path integrity, 2026-07-28):
	//     the PATH was renamed or removed, so the fd now references the
	//     PREVIOUS inode at that name — serving it returns another file's
	//     bytes (read slot) or writes into another file (write slot).
	//   - A POISONED in-flight open (openInFlight): one of the above landed
	//     while Get/GetWrite was inside the open syscall with p.mu released,
	//     so the fd it is about to pool may already be the previous inode.
	//
	// A stale entry is displaced by a fresh open on the next Get/GetWrite
	// and closed by the evict loop the moment its refs drain.
	stale bool
}

func NewFDPool() *FDPool {
	p := &FDPool{
		entries:     make(map[fdKey]*poolEntry),
		pendingRefs: make(map[fdKey]pendingRef),
		stopCh:      make(chan struct{}),
	}
	go p.evictLoop()
	return p
}

// Get returns a pooled read-only fd for the given path, opening it if
// necessary. The returned fd MUST NOT be used for writes; use GetWrite
// for that and Release/ReleaseWrite accordingly so the read+write fds
// stay segregated.
func (p *FDPool) Get(path string) (*os.File, error) {
	k := fdKey{path: path, write: false}
	p.mu.Lock()
	if entry, ok := p.entries[k]; ok && !entry.stale {
		entry.lastUsed = time.Now()
		entry.refCount++
		fd := entry.fd
		p.mu.Unlock()
		return fd, nil
	} else if ok {
		p.displaceStaleLocked(k, entry)
	}
	rec := p.beginOpenLocked(k)
	p.mu.Unlock()

	fd, err := os.Open(path)
	if err != nil {
		// Any displaced holders' debt stays parked on the key (pendingRefs) and
		// is absorbed by whichever Get next succeeds in creating an entry.
		p.mu.Lock()
		p.endOpenLocked(rec)
		p.mu.Unlock()
		return nil, err
	}

	p.mu.Lock()
	poisoned := p.endOpenLocked(rec)
	// Double-check under lock — another goroutine may have inserted
	if entry, ok := p.entries[k]; ok && !entry.stale {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close() // close the one we just opened
		return existingFD, nil
	} else if ok {
		p.displaceStaleLocked(k, entry)
	}
	p.entries[k] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1 + p.takePendingLocked(k),
		// Invalidated while this open was in flight: the fd may reference the
		// PRE-invalidation inode, so it is pooled DEAD-ON-ARRIVAL — never
		// re-served, displaced by the next Get, and closed by the evict loop
		// the moment this caller's ref drains. See openInFlight.
		stale: poisoned,
	}
	p.mu.Unlock()
	return fd, nil
}

// GetWrite returns a pooled fd for writing, opening with the given flags
// if necessary. Lives in its own keyspace slot (write=true) so a
// previously-cached read fd never satisfies a write call.
func (p *FDPool) GetWrite(path string, flag int, perm os.FileMode) (*os.File, error) {
	k := fdKey{path: path, write: true}
	p.mu.Lock()
	if entry, ok := p.entries[k]; ok && !entry.stale {
		entry.lastUsed = time.Now()
		entry.refCount++
		fd := entry.fd
		p.mu.Unlock()
		return fd, nil
	} else if ok {
		p.displaceStaleLocked(k, entry)
	}
	rec := p.beginOpenLocked(k)
	p.mu.Unlock()

	fd, err := os.OpenFile(path, flag, perm)
	if err != nil {
		p.mu.Lock()
		p.endOpenLocked(rec)
		p.mu.Unlock()
		return nil, err
	}

	p.mu.Lock()
	poisoned := p.endOpenLocked(rec)
	if entry, ok := p.entries[k]; ok && !entry.stale {
		entry.lastUsed = time.Now()
		entry.refCount++
		existingFD := entry.fd
		p.mu.Unlock()
		fd.Close()
		return existingFD, nil
	} else if ok {
		p.displaceStaleLocked(k, entry)
	}
	p.entries[k] = &poolEntry{
		fd:       fd,
		lastUsed: time.Now(),
		refCount: 1 + p.takePendingLocked(k),
		stale:    poisoned, // invalidated mid-open — see Get
	}
	p.mu.Unlock()
	return fd, nil
}

// beginOpenLocked registers an open that is about to run with p.mu RELEASED, so
// a concurrent Invalidate can mark it. Caller holds p.mu and MUST retire the
// record with endOpenLocked on every return path, error included.
func (p *FDPool) beginOpenLocked(k fdKey) *openRec {
	rec := &openRec{key: k}
	if p.openInFlight == nil {
		p.openInFlight = make(map[*openRec]struct{})
	}
	p.openInFlight[rec] = struct{}{}
	return rec
}

// endOpenLocked retires an in-flight open and reports whether its key was
// invalidated while the open ran. Caller holds p.mu.
func (p *FDPool) endOpenLocked(rec *openRec) bool {
	delete(p.openInFlight, rec)
	return rec.poisoned
}

// poisonOpensLocked marks every in-flight open whose key `match` selects, so the
// fd it is about to return is pooled stale instead of being served to later
// callers. Caller holds p.mu.
func (p *FDPool) poisonOpensLocked(match func(fdKey) bool) {
	for rec := range p.openInFlight {
		if !rec.poisoned && match(rec.key) {
			rec.poisoned = true
		}
	}
}

// displaceStaleLocked removes a stale entry from the map so a fresh open can
// take its slot. A 0-ref stale fd closes immediately; a HELD one is moved to
// the orphan list, whose fds are grace-closed by the evict loop (their
// holders' Releases are key-based and can no longer reach them).
//
// It also parks the displaced entry's outstanding refs as DEBT on the key
// (pendingRefs), to be absorbed by the next entry created there. Those holders'
// Releases are key-based and will land on that new entry; without the debt each
// mis-landed Release silently drops a LIVE holder's ref, refCount reaches 0
// while a reader is mid-ReadAt, and anything that closes at refCount<=0 yanks
// the fd out from under it ("file already closed" mid-playback).
//
// This was previously "tolerated" (Release clamps at 0, and only the evict
// loop's 2-minute idle path could act on the bogus 0). Invalidate makes it
// acute — it closes an unheld fd IMMEDIATELY — so the accounting is now made
// correct instead of merely survivable. Over-counting is the safe direction:
// an unclaimed debt only delays eviction of one fd, and is dropped after
// fdOrphanGrace anyway.
//
// Caller holds p.mu.
func (p *FDPool) displaceStaleLocked(k fdKey, entry *poolEntry) {
	delete(p.entries, k)
	if entry.refCount <= 0 {
		entry.fd.Close()
		return
	}
	p.orphans = append(p.orphans, orphanEntry{fd: entry.fd, orphaned: time.Now()})
	if p.pendingRefs == nil {
		p.pendingRefs = make(map[fdKey]pendingRef)
	}
	cur := p.pendingRefs[k]
	cur.refs += entry.refCount
	cur.since = time.Now()
	p.pendingRefs[k] = cur
}

// takePendingLocked consumes and clears the refcount debt parked on a key, for
// folding into a freshly-created entry's initial refCount. Caller holds p.mu.
func (p *FDPool) takePendingLocked(k fdKey) int {
	pr, ok := p.pendingRefs[k]
	if !ok {
		return 0
	}
	delete(p.pendingRefs, k)
	return pr.refs
}

// FlushStale invalidates every pooled fd (#12): after a FUSE remount the
// pooled fds reference the DEAD mount — reads through them fail forever,
// and Get kept RE-SERVING them (the "stale fd → 0-byte reads" class).
// Idle entries close immediately; held entries are marked stale so the
// next Get/GetWrite displaces them with a fresh open through the new
// mount. Returns (closed, marked) for the caller's log line.
func (p *FDPool) FlushStale() (closed, marked int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// An open that started BEFORE the remount may have resolved through the DEAD
	// mount; pooling it would re-create the #12 "stale fd → 0-byte reads" state
	// this call exists to clear. Same in-flight hole Invalidate has, same fix.
	p.poisonOpensLocked(func(fdKey) bool { return true })
	for k, e := range p.entries {
		if e.refCount <= 0 {
			e.fd.Close()
			delete(p.entries, k)
			closed++
		} else if !e.stale {
			e.stale = true
			marked++
		}
	}
	return closed, marked
}

// Invalidate drops BOTH pooled slots (read and write) for exactly `path`.
//
// WHY THIS EXISTS (serving-path data integrity, 2026-07-28). The pool is keyed
// by {path, write} — no inode, no generation — and until this existed NOTHING
// invalidated it on rename or delete. FlushStale (a watchdog FUSE remount) was
// the pool's ONLY invalidator anywhere in the tree. Three silent-corruption
// bugs followed directly:
//
//	C1 read-after-rename:  read A.mov (fd→inode A pooled) → mv A.mov A_OLD.mov
//	                       → create a NEW A.mov → read A.mov serves inode A's
//	                       bytes. If new <= old size: wrong content, RIGHT
//	                       LENGTH, no error anywhere.
//	C2 write-after-rename: mv p.prproj p_v1.prproj → an app rewrites p.prproj
//	                       in place (legacy non-spool write path) → GetWrite
//	                       returns the PRE-rename fd and every WriteAt lands
//	                       inside p_v1.prproj, destroying the archived version
//	                       while the new file is never written.
//	C3 write-after-delete: rm t.mov → recreate → GetWrite returns the fd to the
//	                       UNLINKED inode; the writes go to a ghost that the
//	                       kernel reclaims on close. No error at any layer.
//
// The 2-minute idle bound does NOT contain any of them: Get/GetWrite bump
// lastUsed on every hit and onRead re-opens per READ RPC, so a file under
// sustained playback pins its stale fd indefinitely.
//
// SAFETY (must not close an fd another goroutine is mid-ReadAt on). This is
// deliberately the SAME discipline FlushStale uses, scoped to one path:
//
//   - refCount == 0  → nobody holds it: delete the entry and close the fd.
//   - refCount  > 0  → a reader/writer is mid-I/O: mark it stale and leave the
//     fd open under its holder. The next Get/GetWrite displaces it (fresh
//     open; the held fd moves to the orphan list) and the evict loop closes it
//     once its refs drain. The in-flight holder finishes with the bytes it
//     already had — unavoidable, it holds the fd — but no LATER caller can ever
//     be served the stale identity.
//
// Closes happen OUTSIDE p.mu: a JuiceFS FUSE Close flushes pending data and can
// block for seconds under write load, and holding p.mu across it convoys every
// concurrent Get/GetWrite/Release (the 2026-06-14 "error 100060" class — see
// evictLoop). Invalidate runs on the RENAME/REMOVE RPC path, which is exactly
// when a Finder copy has dozens of parallel WRITE RPCs in GetWrite.
//
// Returns (closed, marked) for the caller's log line. Nil-safe.
func (p *FDPool) Invalidate(path string) (closed, marked int) {
	return p.invalidatePath(path, true)
}

// InvalidateReads is Invalidate restricted to the READ slot, leaving any pooled
// WRITE fd for the path untouched.
//
// WHY THIS EXISTS (xattr-loss regression, 2026-07-28). R1 wired the metadata
// pub/sub into Invalidate on the belief that those events describe REMOTE
// mutations. They do not: `juicemount:metadata` replays THIS process's own
// writes too — MetadataEvent (metadata/redis.go) carries no origin/writer field
// at all, so applyEvent structurally cannot tell our write from a peer's. The
// result was that our own write invalidated its own in-flight pooled write fd:
//
//	macOS sets xattrs → ._name AppleDouble sidecar rewritten ~6x in ~70ms →
//	each drain publishes an event → our own consumer applies it → Invalidate
//	drops the sidecar's WRITE slot mid-sequence → xattrs (quarantine,
//	FinderTags, whereFrom, FinderInfo) silently lost on copy.
//
// Caught by qa-battery 01-file-types: 2/2 LOST with R1, 2/2 PASS without it,
// on both 10GbE and WiFi, against a byte-identical battery.
//
// The boundary this encodes: a mutation we learn about SECOND-HAND is a reason
// to distrust a cached READ fd — the path may now be a different inode. It is
// never a reason to disturb a WRITE fd, because for a path this process is
// actively writing, this process is the authority; no event can tell us
// something about our own in-flight write that we do not already know. The
// LOCAL Rename/Remove RPC path deliberately still calls Invalidate (both
// slots): there we performed the mutation ourselves, and C2/C3 require the
// write slot to be dropped.
func (p *FDPool) InvalidateReads(path string) (closed, marked int) {
	return p.invalidatePath(path, false)
}

func (p *FDPool) invalidatePath(path string, includeWrite bool) (closed, marked int) {
	if p == nil || path == "" {
		return 0, 0
	}
	slots := [2]bool{false, true}
	n := 2
	if !includeWrite {
		n = 1 // read slot only
	}
	var toClose []*os.File
	p.mu.Lock()
	for _, write := range slots[:n] {
		k := fdKey{path: path, write: write}
		e, ok := p.entries[k]
		if !ok {
			continue
		}
		c, m := p.invalidateEntryLocked(k, e, &toClose)
		closed += c
		marked += m
	}
	// An entry that does not exist YET is the dangerous case, not a no-op: a
	// Get/GetWrite that missed and is currently inside os.Open (~60 ms on FUSE)
	// will insert an fd for the PRE-rename inode the moment it returns, and the
	// `!ok → continue` above cannot see it. Poison it. See openInFlight.
	p.poisonOpensLocked(func(k fdKey) bool {
		return k.path == path && (includeWrite || !k.write)
	})
	p.mu.Unlock()
	for _, fd := range toClose {
		fd.Close()
	}
	return closed, marked
}

// InvalidateTree drops the pooled slots for `root` AND for every pooled path
// beneath it (root + "/..."), read and write side alike.
//
// A DIRECTORY rename re-parents every descendant in one syscall, so every
// descendant's pooled fd becomes a C1/C2 stale-identity fd simultaneously —
// invalidating only the directory's own key would leave the whole subtree
// serving pre-rename inodes. It is a linear scan of p.entries (bounded: idle
// fds evict after fdIdleTimeout, so this is tens-to-low-hundreds of keys, a
// sub-microsecond in-memory scan) and is used in preference to a mirror-driven
// descendant list ON PURPOSE: it invalidates what the POOL actually holds,
// which stays correct even when the metadata mirror has evicted (or never
// knew) a descendant.
//
// Same close-outside-the-lock and refCount discipline as Invalidate.
func (p *FDPool) InvalidateTree(root string) (closed, marked int) {
	return p.invalidateTree(root, true)
}

// InvalidateReadsTree is InvalidateTree restricted to READ slots. Used for
// second-hand (pub/sub) directory mutations; see InvalidateReads for why a
// write slot must never be dropped on an event this process may itself have
// generated.
func (p *FDPool) InvalidateReadsTree(root string) (closed, marked int) {
	return p.invalidateTree(root, false)
}

func (p *FDPool) invalidateTree(root string, includeWrite bool) (closed, marked int) {
	if p == nil || root == "" || root == "/" {
		return 0, 0
	}
	prefix := strings.TrimSuffix(root, "/") + "/"
	inScope := func(k fdKey) bool {
		if !includeWrite && k.write {
			return false
		}
		return k.path == root || strings.HasPrefix(k.path, prefix)
	}
	var toClose []*os.File
	p.mu.Lock()
	for k, e := range p.entries {
		if !inScope(k) {
			continue
		}
		c, m := p.invalidateEntryLocked(k, e, &toClose)
		closed += c
		marked += m
	}
	// Descendant opens already in flight — same hole as Invalidate's.
	p.poisonOpensLocked(inScope)
	p.mu.Unlock()
	for _, fd := range toClose {
		fd.Close()
	}
	return closed, marked
}

// invalidateEntryLocked applies the Invalidate disposition to one entry:
// unheld entries are unmapped and queued for a close OUTSIDE the lock; held
// entries are marked stale so they can never be re-served (see Invalidate's
// safety note). Caller holds p.mu. Deleting during a range over p.entries is
// safe in Go.
func (p *FDPool) invalidateEntryLocked(k fdKey, e *poolEntry, toClose *[]*os.File) (closed, marked int) {
	if e.refCount <= 0 {
		delete(p.entries, k)
		*toClose = append(*toClose, e.fd)
		return 1, 0
	}
	if !e.stale {
		e.stale = true
		return 0, 1
	}
	return 0, 0
}

// Release decrements the refcount for a path on the READ-side slot.
// Use ReleaseWrite for fds obtained via GetWrite.
func (p *FDPool) Release(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(fdKey{path: path, write: false})
}

// ReleaseWrite decrements the refcount for a path on the WRITE-side slot.
func (p *FDPool) ReleaseWrite(path string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(fdKey{path: path, write: true})
}

// releaseLocked pays down exactly ONE outstanding ref on a key. Caller holds p.mu.
//
// Two places the ref can live, and BOTH must be able to absorb it:
//
//   - The entry currently at the key. Clamped at 0 as a backstop. After a
//     displacement (FlushStale or Invalidate) an OLD holder's Release lands here,
//     key-based — that ref is accounted for by displaceStaleLocked's CARRY, so
//     the decrement is correct rather than mis-landed. The clamp remains for a
//     genuinely double-released or unpaired Release.
//
//   - The DEBT parked on the key (pendingRefs) when there is no entry. Dropping
//     the decrement here (the pre-2026-07-28 behavior) double-counted the ref and
//     permanently inflated the pool: holder A holds the slot; Invalidate marks it
//     stale; B's Get displaces it, parking A's ref as debt, and is then inside
//     os.Open with p.mu released; A's Release arrives in that window, finds NO
//     entry, and was silently discarded; B's open then folded the still-parked
//     debt into the new entry, so refCount settled at 1 with ZERO real holders.
//     From there the entry was un-evictable forever (evictLoop needs refCount<=0),
//     its fd pinned open for the process lifetime, and HasOpenRefs stayed true —
//     which permanently disabled the phantom-purge Lstat gate for that path.
func (p *FDPool) releaseLocked(k fdKey) {
	if entry, ok := p.entries[k]; ok {
		if entry.refCount > 0 {
			entry.refCount--
		}
		return
	}
	pr, ok := p.pendingRefs[k]
	if !ok {
		return
	}
	pr.refs--
	if pr.refs <= 0 {
		delete(p.pendingRefs, k)
		return
	}
	p.pendingRefs[k] = pr
}

// HasOpenRefs returns true if there is at least one outstanding Get
// or GetWrite without a matching Release for `path` — i.e. somebody
// currently holds a FD (read OR write) on this file.
//
// QA-35 (2026-05-26): used by juiceFS.Stat to skip the phantom-purge
// FUSE Lstat gate when an active reader holds the file open. If a FD
// is open, the file is not a phantom — the reader proves it exists.
// Eliminates a per-metadata-RPC FUSE round-trip during sustained
// reads of a held file (Resolve playback, Finder Quick Look, etc.).
//
// QA-37: after the read/write keyspace split, check BOTH slots — an
// active writer is just as good as an active reader for proving the
// file isn't a phantom.
func (p *FDPool) HasOpenRefs(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.entries[fdKey{path: path, write: false}]; ok && entry.refCount > 0 {
		return true
	}
	if entry, ok := p.entries[fdKey{path: path, write: true}]; ok && entry.refCount > 0 {
		return true
	}
	return false
}

func (p *FDPool) evictLoop() {
	ticker := time.NewTicker(fdEvictTick)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			// Collect the idle fds under the lock, but CLOSE them outside it.
			// Closing a JuiceFS FUSE fd flushes pending data and can BLOCK for
			// seconds under write load; holding p.mu across that Close convoys
			// every concurrent GetWrite/Get/Release on the single pool mutex.
			// A deep-tree Finder copy generates dozens of parallel WRITE RPCs,
			// all calling GetWrite — 93 were observed wedged on this lock while
			// evictLoop held it inside a stuck Close, saturating the NFS server's
			// in-flight budget and timing out the copy with "error 100060"
			// (2026-06-14, reproduced via ditto of a 5598-file subtree).
			var toClose []*os.File
			p.mu.Lock()
			for path, entry := range p.entries {
				// Stale entries (post-remount, #12) are dead-mount fds:
				// close them the moment their refs drain, no idle wait.
				if entry.refCount <= 0 && (entry.stale || now.Sub(entry.lastUsed) > fdIdleTimeout) {
					toClose = append(toClose, entry.fd)
					delete(p.entries, path)
				}
			}
			// Orphaned stale fds close after the grace window (their
			// refcounts are untrackable — see displaceStaleLocked).
			keep := p.orphans[:0]
			for _, o := range p.orphans {
				if now.Sub(o.orphaned) > fdOrphanGrace {
					toClose = append(toClose, o.fd)
				} else {
					keep = append(keep, o)
				}
			}
			p.orphans = keep
			// Unclaimed refcount debt (a displaced holder that never
			// Released) is dropped on the same grace as the orphan fd it
			// belongs to — past that window the fd is closed anyway, so
			// keeping the debt would only inflate a future entry forever.
			for k, pr := range p.pendingRefs {
				if now.Sub(pr.since) > fdOrphanGrace {
					delete(p.pendingRefs, k)
				}
			}
			p.mu.Unlock()
			for _, fd := range toClose {
				fd.Close()
			}
		}
	}
}

// Stop closes all pooled fds.
func (p *FDPool) Stop() {
	close(p.stopCh)
	// Detach the fds under the lock, close them outside it (a FUSE fd Close can
	// block; don't hold p.mu across it — see evictLoop).
	p.mu.Lock()
	entries := p.entries
	p.entries = nil
	// Orphans (displaced-but-held fds) were previously left open at Stop —
	// harmless when only FlushStale produced them (once per remount), but
	// Invalidate produces them on the ordinary rename/delete path, so close
	// them here too rather than leaking an fd per displacement.
	orphans := p.orphans
	p.orphans = nil
	p.pendingRefs = nil
	p.mu.Unlock()
	for _, entry := range entries {
		entry.fd.Close()
	}
	for _, o := range orphans {
		o.fd.Close()
	}
	log.Printf("fdpool: stopped")
}

// Stats returns pool statistics.
func (p *FDPool) Stats() (open, active int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.entries {
		open++
		if entry.refCount > 0 {
			active++
		}
	}
	return
}
