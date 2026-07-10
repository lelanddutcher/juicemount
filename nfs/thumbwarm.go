package nfs

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// ThumbWarmer (#1, INSTANT-NAV — the P2 "hydration pack" substrate consumer).
// When a folder is LISTED (warm mirror readdir), hydrate its children's tiny
// farm-generated derivative blobs (kind "thumbnail" — poster.jpg, ~10-50KB)
// into the bounded local thumb cache in the background, so preview bytes are
// local before anything asks for source bytes. On a 300ms cellular link one
// poster is 1-2 RTTs (~50KB) vs the 14-53MB source pull Finder's preview
// probe otherwise triggers — a 300-1000× byte cut per file, and after the
// first visit the folder's previews are 0-RTT.
//
// Decoupled by construction: the cache is an interface and the manifest
// resolution + directory listing are injected closures (bridge wires the
// real thumbcache.Cache, derivatives.Store + farm.ReconcileOneSidecar, and
// metadata.Store.ListChildren). This file has zero new imports beyond what
// nfs already uses, and the warmer is unit-testable with fakes.
//
// Discipline (the crash-2026-06-14 / QA-35 lessons):
//   - The readdir-side enqueue (WarmDirAsync) is non-blocking: one mutex'd
//     TTL-map check + a select/default send. Never blocks, never spawns.
//   - Fixed worker pool (thumbWarmWorkers), never a goroutine per dir/file.
//   - Each blob fetch passes the read-QoS bulk lane via tryAcquireBulk —
//     prefetch class, sheds under contention (the interactive lane always
//     wins). A shed abandons the REST of the dir too; the next readdir
//     re-triggers it.
//   - Resolution (which may cost FUSE round-trips for a not-yet-ingested
//     sidecar) is budgeted per visit; unknown inodes enter a TTL'd negative
//     cache so a browse can't re-burn RTTs on underived folders.
//   - Offline: dirs are dropped, not queued (the next online readdir
//     re-triggers). No FUSE touch happens offline.
//
// Kill switch: JM_THUMB_WARM=0 (bridge simply doesn't construct the warmer;
// the handler hook is nil-guarded).
const (
	thumbWarmWorkers   = 2
	thumbWarmQueueCap  = 64
	thumbWarmKind      = "thumbnail"
	thumbWarmDedupeTTL = 10 * time.Minute
	thumbWarmNegTTL    = 30 * time.Minute
	// thumbWarmResolveBudget caps per-visit manifest RESOLUTIONS (each may
	// cost FUSE RTTs for the JM-15 one-sidecar reconcile of a never-seen
	// inode). Hydrations of already-resolved manifests are cheaper and get
	// the larger budget.
	thumbWarmResolveBudget = 64
	thumbWarmHydrateBudget = 128
	// thumbWarmBlobCap rejects oversized "thumbnails" — a poster is tens of
	// KB; anything past this is a mis-tagged blob we refuse to pull onto a
	// metered link or hold in the cache.
	thumbWarmBlobCap = 2 << 20
	// thumbWarmNegCap bounds the negative map; on overflow it is cleared
	// (re-probing once is cheaper than tracking precise LRU here).
	thumbWarmNegCap = 100_000
)

// ThumbBlobCache is the slice of thumbcache.Cache the warmer needs.
type ThumbBlobCache interface {
	Has(inode uint64, kind string) bool
	Put(inode uint64, kind string, r io.Reader) (int64, error)
}

// ThumbChildRef is one directory child from the metadata mirror.
type ThumbChildRef struct {
	Inode uint64
	Name  string
	IsDir bool
}

type ThumbWarmer struct {
	cache ThumbBlobCache
	// resolve returns the ABSOLUTE on-FUSE path of the ready thumbnail blob
	// for an inode (bridge composes Known → ReconcileOneSidecar → Manifest →
	// DerivBlobDir). ok=false means no ready thumbnail exists (yet).
	resolve func(inode uint64) (blobPath string, ok bool)
	// children lists a dir from the RAM mirror (bridge wires
	// store.ListChildren; µs, never a backend call).
	children func(dir string) []ThumbChildRef

	queue  chan string
	stopCh chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex
	recent   map[string]time.Time // dir → last visit (dedupe)
	negative map[uint64]time.Time // inode → no-thumbnail verdict (TTL)
}

func NewThumbWarmer(cache ThumbBlobCache, resolve func(uint64) (string, bool), children func(string) []ThumbChildRef) *ThumbWarmer {
	w := &ThumbWarmer{
		cache:    cache,
		resolve:  resolve,
		children: children,
		queue:    make(chan string, thumbWarmQueueCap),
		stopCh:   make(chan struct{}),
		recent:   make(map[string]time.Time),
		negative: make(map[uint64]time.Time),
	}
	for i := 0; i < thumbWarmWorkers; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	return w
}

// WarmDirAsync enqueues a directory for background thumb hydration.
// Non-blocking (readdir hot-path caller): TTL dedupe + select/default —
// a full queue or a recently-visited dir is silently dropped (the next
// readdir past the TTL re-triggers).
func (w *ThumbWarmer) WarmDirAsync(dir string) {
	if w == nil {
		return
	}
	now := time.Now()
	w.mu.Lock()
	if t, ok := w.recent[dir]; ok && now.Sub(t) < thumbWarmDedupeTTL {
		w.mu.Unlock()
		return
	}
	w.recent[dir] = now
	// Opportunistic sweep so the dedupe map can't grow unbounded across a
	// deep walk (cheap: only when large).
	if len(w.recent) > 4096 {
		for d, t := range w.recent {
			if now.Sub(t) >= thumbWarmDedupeTTL {
				delete(w.recent, d)
			}
		}
	}
	w.mu.Unlock()
	select {
	case w.queue <- dir:
	default:
		// queue full — drop; re-triggered by a later readdir
	}
}

func (w *ThumbWarmer) Stop() {
	if w == nil {
		return
	}
	close(w.stopCh)
	w.wg.Wait()
}

func (w *ThumbWarmer) worker() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stopCh:
			return
		case dir := <-w.queue:
			w.warmDir(dir)
		}
	}
}

func (w *ThumbWarmer) warmDir(dir string) {
	if pin.IsOffline() {
		return // dropped; the next online readdir re-triggers
	}
	kids := w.children(dir)
	if len(kids) == 0 {
		return
	}
	now := time.Now()
	resolves, hydrated := 0, 0
	for _, k := range kids {
		select {
		case <-w.stopCh:
			return
		default:
		}
		if k.IsDir || k.Inode == 0 || hydrated >= thumbWarmHydrateBudget {
			continue
		}
		if len(k.Name) >= 2 && k.Name[0] == '.' && k.Name[1] == '_' {
			continue // AppleDouble sidecars never have derivatives
		}
		if w.cache.Has(k.Inode, thumbWarmKind) {
			continue
		}
		w.mu.Lock()
		if t, neg := w.negative[k.Inode]; neg && now.Sub(t) < thumbWarmNegTTL {
			w.mu.Unlock()
			continue
		}
		w.mu.Unlock()

		if resolves >= thumbWarmResolveBudget {
			continue // budget spent; the next visit resumes
		}
		resolves++
		blobPath, ok := w.resolve(k.Inode)
		if !ok {
			w.noteNegative(k.Inode, now)
			metrics.Default().IncThumbWarmNegative()
			continue
		}

		// Prefetch-class link admission: shed → abandon the whole dir.
		release, admitted := defaultReadQoS.tryAcquireBulk()
		if !admitted {
			metrics.Default().IncThumbWarmShed()
			return
		}
		n, err := w.hydrateOne(k.Inode, blobPath)
		release()
		if err != nil {
			// Treat as negative (missing/oversized/unreadable blob) so a
			// broken derivative can't be re-probed every browse.
			w.noteNegative(k.Inode, now)
			metrics.Default().IncThumbWarmNegative()
			continue
		}
		hydrated++
		metrics.Default().IncThumbWarmHydrated()
		_ = n
	}
	if hydrated > 0 {
		jmlog.Debug("thumb warm: dir hydrated", "dir", dir, "thumbs", hydrated)
	}
}

// hydrateOne pulls one thumbnail blob from FUSE into the local cache.
// Bounded open (a wedged FUSE can't pin a warm worker) + size cap.
func (w *ThumbWarmer) hydrateOne(inode uint64, blobPath string) (int64, error) {
	f, err, ok := openFileWithTimeout(blobPath, os.O_RDONLY, 0, fuseStatTimeout)
	if !ok {
		return 0, errFUSETimeout
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if fi.Size() > thumbWarmBlobCap {
		return 0, os.ErrInvalid
	}
	return w.cache.Put(inode, thumbWarmKind, io.LimitReader(f, thumbWarmBlobCap))
}

func (w *ThumbWarmer) noteNegative(inode uint64, now time.Time) {
	w.mu.Lock()
	if len(w.negative) >= thumbWarmNegCap {
		w.negative = make(map[uint64]time.Time)
	}
	w.negative[inode] = now
	w.mu.Unlock()
}

// TryAcquireBulkRead exposes the read-QoS bulk lane's prefetch-class
// admission to other packages (bridge-side hydration consumers). Same
// semantics as the internal tryAcquireBulk: shed on contention, inert on
// medium/fast links.
func TryAcquireBulkRead() (release func(), ok bool) {
	return defaultReadQoS.tryAcquireBulk()
}
