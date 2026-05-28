package nfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// SpoolFilesSubdir is the sub-directory under the spool root that holds
// the on-disk write spool files. The root may also hold sibling dirs
// (`quarantine/`, `manifest.log`) added by later slices.
const SpoolFilesSubdir = "files"

// ErrSpoolFull is returned by OpenWrite/WriteAt when the spool's capacity
// cap would be exceeded. The handler translates this to NFS3ERR_NOSPC at
// the RPC boundary so the client sees a clean ENOSPC.
var ErrSpoolFull = errors.New("spool: capacity full")

// SpoolStore is the high-level write-spool API.
//
// Owns:
//   - the spool root directory on local SSD
//   - the in-memory index (path → entry)
//   - the SQLite-backed durable index (via metadata.SpoolStore)
//   - the capacity budget (used bytes vs cap bytes)
//
// Threadsafe — OpenWrite and lookups can race freely. Per-entry state is
// guarded by the entry's own mutex.
type SpoolStore struct {
	root     string
	capacity int64 // 0 means unlimited
	used     atomic.Int64
	meta     *metadata.SpoolStore
	index    *SpoolIndex
	closed   atomic.Bool
	openMu   sync.Mutex // serializes OpenWrite for the same path; tiny contention area

	// wakeDrainer is guarded by wakeMu so concurrent SetDrainerWake and
	// signalReady can't race on the func pointer.
	wakeMu      sync.RWMutex
	wakeDrainer func()

	// manifest is the append-only JSONL audit log under <root>/manifest.log.
	// May be nil if open failed at construction; that case becomes a
	// no-op for the audit path (drain still proceeds normally).
	manifest *manifestWriter
}

// NewSpoolStore creates the spool root if it doesn't exist and returns an
// empty store. It does NOT recover prior on-disk state — call Recover for
// that (Slice F adds the recovery scrubber; for Slice A this is a no-op).
//
// capacity is in bytes; 0 means unlimited. meta is the SQLite-backed index
// (callers should have called metadata.InitSpoolSchema on the underlying
// db before this).
func NewSpoolStore(root string, capacity int64, meta *metadata.SpoolStore) (*SpoolStore, error) {
	if root == "" {
		return nil, fmt.Errorf("spool: root path is required")
	}
	if meta == nil {
		return nil, fmt.Errorf("spool: metadata.SpoolStore is required")
	}
	filesDir := filepath.Join(root, SpoolFilesSubdir)
	if err := os.MkdirAll(filesDir, 0o755); err != nil {
		return nil, fmt.Errorf("spool: mkdir %s: %w", filesDir, err)
	}
	s := &SpoolStore{
		root:     root,
		capacity: capacity,
		meta:     meta,
		index:    NewSpoolIndex(),
	}
	if mw, err := newManifestWriter(root); err != nil {
		// Non-fatal — manifest is audit-only. Log and proceed.
		log.Printf("spool: manifest writer disabled: %v", err)
	} else {
		s.manifest = mw
	}
	return s, nil
}

// SetDrainerWake registers a callback invoked when an entry transitions
// to ready (i.e. on Close). The drainer (slice B) uses this to avoid
// polling — it sleeps on a signal channel and wakes when there's work.
// Calling with nil clears the callback. Safe to call concurrently with
// in-flight signalReady invocations.
func (s *SpoolStore) SetDrainerWake(fn func()) {
	s.wakeMu.Lock()
	s.wakeDrainer = fn
	s.wakeMu.Unlock()
}

// Root returns the spool root directory.
func (s *SpoolStore) Root() string { return s.root }

// Capacity returns (used, total) bytes. total=0 means unlimited.
func (s *SpoolStore) Capacity() (used, total int64) {
	return s.used.Load(), s.capacity
}

// Index returns the in-memory lookup table. Exposed so the NFS handler
// (slice D) can take O(1) lookups directly without going through the
// SpoolStore method surface on the hot path.
func (s *SpoolStore) Index() *SpoolIndex { return s.index }

// OpenWrite creates a new spool entry for nfsPath in `writing` state.
// Returns ErrSpoolFull if the capacity budget is already exhausted.
//
// Concurrent OpenWrite for the same nfsPath is serialized by openMu; if
// the index already has an entry for this path, that entry is returned
// directly (same-path-reopen). This matches the FDPool same-path-dedupe
// semantics so a single Finder copy's multi-RPC write lifecycle reuses
// one spool file.
func (s *SpoolStore) OpenWrite(nfsPath string) (*SpoolEntry, error) {
	if s.closed.Load() {
		return nil, fmt.Errorf("spool: store is closed")
	}
	s.openMu.Lock()
	defer s.openMu.Unlock()

	if existing, ok := s.index.Lookup(nfsPath); ok {
		existing.refcount.Add(1)
		return existing, nil
	}

	if s.capacity > 0 && s.used.Load() >= s.capacity {
		return nil, ErrSpoolFull
	}

	// Spool file basename: SHA-256(nfs_path) hex prefix + a microsecond
	// timestamp. SHA prefix avoids filesystem-path-character issues
	// (slashes, spaces, unicode). Timestamp suffix avoids collisions on
	// rapid re-opens after a Delete.
	h := sha256.Sum256([]byte(nfsPath))
	basename := hex.EncodeToString(h[:8]) + fmt.Sprintf("-%d", time.Now().UnixMicro())
	spoolFile := filepath.Join(s.root, SpoolFilesSubdir, basename)

	id, err := s.meta.Insert(nfsPath, spoolFile)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(spoolFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		// Roll back the SQL insert. We DELETE rather than MarkFailed:
		// nothing happened on disk to preserve, and leaving a `failed`
		// row would let a persistent disk failure (full / permissions)
		// grow spool_entries unboundedly because LookupByPath ignores
		// failed rows and every retry would Insert a new one.
		_ = s.meta.Delete(id)
		return nil, fmt.Errorf("spool: open file: %w", err)
	}

	entry := &SpoolEntry{
		id:        id,
		nfsPath:   nfsPath,
		spoolFile: spoolFile,
		file:      f,
		hasher:    sha256.New(),
		hashValid: true,
		store:     s,
	}
	entry.lastWrite.Store(time.Now().UnixNano())
	entry.refcount.Store(1)
	s.index.Insert(nfsPath, entry)
	return entry, nil
}

// LookupActive returns the in-memory index entry for nfsPath if one
// exists. O(1). Used by the read path in slice D.
func (s *SpoolStore) LookupActive(nfsPath string) (*SpoolEntry, bool) {
	return s.index.Lookup(nfsPath)
}

// Meta returns the underlying SQLite-backed index. Used by the drainer
// (slice B) which lives below the in-memory index abstraction.
func (s *SpoolStore) Meta() *metadata.SpoolStore { return s.meta }

// Stop closes the store. In-flight entries are NOT auto-closed — callers
// must close them first. Idempotent.
func (s *SpoolStore) Stop() {
	s.closed.Store(true)
	if s.manifest != nil {
		_ = s.manifest.Close()
	}
}

// Manifest returns the audit log writer, or nil if it failed to open.
// Exposed for tests and for the drainer (which appends per disposition).
func (s *SpoolStore) Manifest() *manifestWriter { return s.manifest }

// RecoveryReport summarizes what the boot scrubber did. Returned by
// RecoverOnBoot for log aggregation + admin UX.
type RecoveryReport struct {
	// OrphanFilesDeleted: files in the spool dir with no SQL row →
	// removed. Cleans up half-created files from a crash between
	// metadata.Insert and the first WriteAt.
	OrphanFilesDeleted int

	// OrphanRowsFailed: SQL rows in `ready` or `draining` state whose
	// spool file is no longer on disk → MarkFailed (no data to retry).
	OrphanRowsFailed int

	// WritingFailedRows: rows in `writing` state at boot time. The
	// partial spool file is unsafe to resume (NFS client doesn't know
	// the in-flight WRITEs aren't durable) → MarkFailed + delete the
	// partial file.
	WritingFailedRows int

	// DrainingReset: rows in `draining` state with spool file present
	// → ResetToReady so the drainer re-attempts the copy.
	DrainingReset int

	// ReadyResumed: rows in `ready` state with spool file present →
	// accounted against the capacity counter; drainer will pick them
	// up on next Start. No SQL state change.
	ReadyResumed int
}

// RecoverOnBoot reconciles on-disk spool files against the SQL index.
// Call once after NewSpoolStore + InitSpoolSchema and BEFORE
// drainer.Start, ideally as part of the same critical section as
// SetSpool — the method is NOT safe to run concurrently with active
// writers or the drainer.
//
// State transition rules:
//
//	file orphan (no SQL row)        → delete file
//	row.writing  → mark failed + delete (best-effort) the partial file
//	row.ready    → file present  : add bytes to capacity counter, leave state
//	             → file missing  : mark failed
//	row.draining → file present  : reset to ready (drainer retries)
//	             → file missing  : mark failed
//	row.done, row.failed         : no action (terminal states; done rows are
//	                               audit, failed rows are operator-actionable)
//
// Files inside spool_root/quarantine/ are forensic state preserved by
// the drainer's SHA-mismatch path; they are NOT touched here.
//
// The in-memory index (s.index) is NOT repopulated. `writing`-state
// entries are gone (we marked them failed); reads of those paths will
// fall through to FUSE. `ready`/`draining` paths haven't reached FUSE
// yet either, so reads briefly return ENOENT until the drainer copies
// them in. Same window as slice C's known limitation.
//
// Errors from ListAll or ReadDir are returned; per-row state-transition
// errors are logged but do not abort the reconciliation.
func (s *SpoolStore) RecoverOnBoot(ctx context.Context) (RecoveryReport, error) {
	var report RecoveryReport

	// Slice-F reviewer HIGH fix: enforce the no-concurrent-drainer
	// invariant at runtime. wakeDrainer is set exactly once by
	// drainer.Start via SetDrainerWake; if it's already non-nil here,
	// the drainer is alive and a concurrent releaseCapacity /
	// tryReserveCapacity will race with our Store(0) below. Panic
	// rather than silently corrupt the counter.
	s.wakeMu.RLock()
	wakeSet := s.wakeDrainer != nil
	s.wakeMu.RUnlock()
	if wakeSet {
		panic("nfs/spool: RecoverOnBoot called after drainer.Start — must run before SetSpool/Start")
	}

	// Reset the capacity counter before re-accounting. Idempotent
	// recovery: a second call must produce the same `used` value as
	// the first, not a doubled one. Safe because the guard above
	// proves no drainer / writer is mutating s.used concurrently.
	s.used.Store(0)

	rows, err := s.meta.ListAll()
	if err != nil {
		return report, fmt.Errorf("recover: list rows: %w", err)
	}

	// Build the set of file paths SQL knows about so we can detect
	// orphan files in one pass.
	expectedFiles := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		expectedFiles[r.SpoolFile] = struct{}{}
	}

	// Scan the files dir. We deliberately do NOT recurse — the
	// quarantine subdir (sibling under spool_root) is left alone, and
	// the files dir is flat.
	filesDir := filepath.Join(s.root, SpoolFilesSubdir)
	actualFiles := make(map[string]bool)
	if dirents, err := os.ReadDir(filesDir); err == nil {
		for _, d := range dirents {
			if d.IsDir() {
				continue
			}
			full := filepath.Join(filesDir, d.Name())
			actualFiles[full] = true
			if _, ok := expectedFiles[full]; !ok {
				if rmErr := os.Remove(full); rmErr == nil {
					report.OrphanFilesDeleted++
					log.Printf("spool recover: orphan file deleted: %s", full)
				} else {
					log.Printf("spool recover: orphan file remove failed: %s: %v", full, rmErr)
				}
			}
		}
	} else if !os.IsNotExist(err) {
		// Note: missing files dir is fine on fresh install. Other
		// errors (permission etc) are unusual; log + continue with
		// SQL reconciliation only.
		log.Printf("spool recover: readdir %s: %v", filesDir, err)
	}

	// Reconcile per-row.
	for _, r := range rows {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		fileExists := actualFiles[r.SpoolFile]
		switch r.DrainState {
		case metadata.DrainWriting:
			if mErr := s.meta.MarkFailed(r.ID, "writing-state row recovered after crash (partial spool file unsafe to resume)"); mErr != nil {
				log.Printf("spool recover: mark writing→failed %d: %v", r.ID, mErr)
				continue
			}
			if fileExists {
				if rmErr := os.Remove(r.SpoolFile); rmErr != nil {
					log.Printf("spool recover: writing partial file remove %s: %v", r.SpoolFile, rmErr)
				}
			}
			report.WritingFailedRows++

		case metadata.DrainReady:
			if !fileExists {
				if mErr := s.meta.MarkFailed(r.ID, "ready-state row missing spool file after crash"); mErr != nil {
					log.Printf("spool recover: mark ready→failed %d: %v", r.ID, mErr)
					continue
				}
				report.OrphanRowsFailed++
			} else {
				// File present; ready state preserved. Account bytes.
				s.used.Add(r.Size)
				report.ReadyResumed++
			}

		case metadata.DrainDraining:
			if !fileExists {
				if mErr := s.meta.MarkFailed(r.ID, "draining-state row missing spool file after crash"); mErr != nil {
					log.Printf("spool recover: mark draining→failed %d: %v", r.ID, mErr)
					continue
				}
				report.OrphanRowsFailed++
			} else {
				if rErr := s.meta.ResetToReady(r.ID); rErr != nil {
					log.Printf("spool recover: reset draining→ready %d: %v", r.ID, rErr)
					continue
				}
				s.used.Add(r.Size)
				report.DrainingReset++
			}

		case metadata.DrainDone, metadata.DrainFailed:
			// Terminal — no state transition. But the spool file
			// must not survive: `done` rows had their file removed by
			// MarkDrainComplete; `failed` rows from a writing-state
			// crash above MAY have left a partial file when our
			// best-effort os.Remove failed. Without this cleanup, a
			// stale file paired to a terminal SQL row stays in
			// expectedFiles indefinitely and the orphan-scan above
			// never reclaims it. Code-reviewer slice-F HIGH fix.
			if fileExists {
				if rmErr := os.Remove(r.SpoolFile); rmErr != nil {
					log.Printf("spool recover: terminal-row stale file remove %s: %v", r.SpoolFile, rmErr)
				}
			}
		}
	}

	log.Printf("spool recover: %+v", report)
	return report, nil
}

// MarkDrainComplete is the success path for the drainer (slice B). It
// marks the SQL row done; ONLY on SQL success does it remove the spool
// file from disk, release the capacity reservation, and evict the
// in-memory index entry.
//
// nfsPath/spoolFile/size come from the metadata.SpoolRow the drainer
// claimed; the drainer never touches the *SpoolEntry directly (it may
// no longer exist if the writer process restarted between Close and
// drain).
//
// HIGH-2 fix (slice B reviewer): on SQL failure we MUST NOT delete the
// spool file or release capacity. Doing so converts a retryable
// transient SQLite error (busy, WAL checkpoint, disk full on meta
// partition) into irrecoverable data loss for a successfully-drained
// file. The caller (drainOne) sees the SQL error and calls
// failTransient, which retries. Slice F's boot scrubber covers crashes
// between SQL success and the post-SQL cleanup (in that narrow window
// the file exists on disk + capacity stays counted, harmless).
func (s *SpoolStore) MarkDrainComplete(id int64, nfsPath, spoolFile string, size int64) error {
	if err := s.meta.MarkDone(id); err != nil {
		return err
	}
	if rmErr := os.Remove(spoolFile); rmErr != nil && !os.IsNotExist(rmErr) {
		log.Printf("spool: drain complete %d: remove %s: %v", id, spoolFile, rmErr)
	}
	s.releaseCapacity(size)
	if e, ok := s.index.Lookup(nfsPath); ok && e.id == id {
		s.index.DeleteIfMatches(nfsPath, e)
	}
	if s.manifest != nil {
		var (
			shaHex      string
			shaUnknown  bool
		)
		if row, gerr := s.meta.Get(id); gerr == nil && row != nil && len(row.SHA256) > 0 {
			shaHex = fmt.Sprintf("%x", row.SHA256)
		} else {
			shaUnknown = true
		}
		if appendErr := s.manifest.Append(ManifestRecord{
			Event:             ManifestEventDrainDone,
			Path:              nfsPath,
			SpoolFile:         spoolFile,
			Size:              size,
			SHA256Hex:         shaHex,
			SHA256Unavailable: shaUnknown,
		}); appendErr != nil {
			// AUDIT LOSS — the drain succeeded but the audit trail
			// no longer reflects it. Operator should investigate
			// (manifest disk full, permission change, etc).
			log.Printf("spool: AUDIT LOSS manifest append done id=%d path=%s: %v",
				id, nfsPath, appendErr)
		}
	}
	return nil
}

// MarkDrainRetry is the transient-failure path: bumps drain_attempts +
// last_error and resets the row to ready so the dispatcher picks it up
// again after backoff. Caller is responsible for the backoff delay.
func (s *SpoolStore) MarkDrainRetry(id int64, reason string) error {
	if err := s.meta.IncrementAttempts(id, reason); err != nil {
		return err
	}
	return s.meta.ResetToReady(id)
}

// QuarantineDrain is the SHA-mismatch path: marks the SQL row failed
// THEN moves the spool file to a quarantine subdir (preserving forensic
// state), releases the capacity reservation, and evicts the index entry.
// The manifest log (slice G) will record the quarantine event with
// timestamps + the mismatched SHAs.
//
// Ordering (HIGH-2 follow-on fix): MarkFailed runs BEFORE the rename.
// If SQL fails, the spool file stays in its original location and the
// row remains in `draining` state — the drainer's next attempt will
// re-detect the SHA mismatch and call QuarantineDrain again. If we
// renamed first then SQL-failed, the file would be in quarantine/ but
// the row's spool_file path would point to the original (now missing)
// location; slice F's scrubber would treat it as an orphan and
// failPermanent, losing the forensic copy.
//
// The quarantined file is left on disk. Operators can inspect/recover/
// delete it manually.
func (s *SpoolStore) QuarantineDrain(id int64, nfsPath, spoolFile string, size int64, reason string) error {
	if err := s.meta.MarkFailed(id, reason); err != nil {
		return err
	}
	quarantineDir := filepath.Join(s.root, "quarantine")
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		log.Printf("spool: quarantine mkdir: %v", err)
		// Continue — the SQL row is already failed; best-effort the
		// file move and the cleanup below.
	}
	dest := filepath.Join(quarantineDir, filepath.Base(spoolFile))
	if err := os.Rename(spoolFile, dest); err != nil && !os.IsNotExist(err) {
		log.Printf("spool: quarantine move %s→%s: %v", spoolFile, dest, err)
	}
	s.releaseCapacity(size)
	if e, ok := s.index.Lookup(nfsPath); ok && e.id == id {
		s.index.DeleteIfMatches(nfsPath, e)
	}
	if s.manifest != nil {
		var (
			shaHex     string
			shaUnknown bool
		)
		if row, gerr := s.meta.Get(id); gerr == nil && row != nil && len(row.SHA256) > 0 {
			shaHex = fmt.Sprintf("%x", row.SHA256)
		} else {
			shaUnknown = true
		}
		if appendErr := s.manifest.Append(ManifestRecord{
			Event:             ManifestEventQuarantine,
			Path:              nfsPath,
			SpoolFile:         dest,
			Size:              size,
			SHA256Hex:         shaHex,
			SHA256Unavailable: shaUnknown,
			Reason:            reason,
		}); appendErr != nil {
			// AUDIT LOSS — the quarantine event happened but the
			// audit trail lost it. This is the most serious manifest
			// failure mode because quarantine is the integrity
			// failure case the log exists to document.
			log.Printf("spool: AUDIT LOSS manifest append quarantine id=%d path=%s reason=%q: %v",
				id, nfsPath, reason, appendErr)
		}
	}
	return nil
}

// signalReady notifies the drainer that a new ready entry exists.
// No-op when no drainer callback is registered.
func (s *SpoolStore) signalReady() {
	s.wakeMu.RLock()
	fn := s.wakeDrainer
	s.wakeMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// tryReserveCapacity atomically reserves `delta` bytes against the cap
// using a CAS loop. Returns true if the reservation succeeded (used was
// bumped), false if the reservation would exceed capacity. Cap of 0
// means unlimited and always succeeds (with the add still happening).
//
// Why CAS instead of a check-then-add under per-entry mu: two concurrent
// WriteAt calls on DIFFERENT entries hold different e.mu locks but share
// the same store.used. Without CAS, both could read the same `used`,
// both pass the cap check, both commit — over-filling the cap by up to
// one write-payload-per-concurrent-writer. CAS bounds the over-fill to
// zero.
func (s *SpoolStore) tryReserveCapacity(delta int64) bool {
	if delta <= 0 {
		return true
	}
	for {
		cur := s.used.Load()
		if s.capacity > 0 && cur+delta > s.capacity {
			return false
		}
		if s.used.CompareAndSwap(cur, cur+delta) {
			return true
		}
	}
}

// releaseCapacity returns reserved bytes to the budget. Used when a
// reserved write partially failed and the actual delta committed to
// disk was less than reserved.
func (s *SpoolStore) releaseCapacity(delta int64) {
	if delta <= 0 {
		return
	}
	s.used.Add(-delta)
}

// SpoolEntry is one in-flight or pending-upload file. Holds a write fd to
// the on-disk spool file plus the streaming SHA-256 hasher.
//
// Lifecycle:
//
//	NewSpoolStore.OpenWrite → entry.WriteAt → ... → entry.Close
//	                                                    ↓
//	                                            drain_state=ready
//	                                                    ↓
//	                                        drainer claims via Meta.MarkDraining
//	                                                    ↓
//	                                        drainer reads via OpenForRead
//	                                                    ↓
//	                                        SpoolStore.MarkDone (slice B) deletes file
type SpoolEntry struct {
	id        int64
	nfsPath   string
	spoolFile string

	// Inode assigned at OpenFile time by the handler so Stat/Lstat
	// during the writing/ready/draining lifetime return a stable value.
	// Set via SetInode (once) after OpenWrite returns. Zero until set.
	//
	// atomic.Uint64 (slice D reviewer CRITICAL fix): SetInode and Inode
	// can race across goroutines (OpenFile-write sets; Stat/Lstat read
	// concurrently). Plain field had a data race that -race would catch
	// in a concurrent Stat + OpenWrite interleaving. CompareAndSwap
	// preserves the once-set semantics atomically.
	inode atomic.Uint64

	// lastWrite is atomic Unix-nanoseconds of the most recent WriteAt
	// that extended writtenEnd. Used by the slice D Stat/Lstat shadow
	// so an in-flight file's mtime reflects writer activity instead of
	// returning 1970-epoch. Initialized to OpenWrite time; updated on
	// each writing-extending WriteAt.
	lastWrite atomic.Int64

	mu         sync.RWMutex
	file       *os.File // nil after Close
	writtenEnd int64
	hasher     hash.Hash
	sha256     []byte // populated on Close iff streaming hash is trustworthy
	// hashValid tracks whether the streaming hasher reflects the on-disk
	// contents. False once we observe any out-of-order WriteAt (off <
	// current writtenEnd) — sparse / out-of-order writes make the streaming
	// hash diverge from the file's at-rest hash. The drainer (slice B)
	// re-hashes from disk regardless; this flag tells it whether the
	// streaming SHA is a usable optimization or just noise.
	hashValid bool
	closed    bool

	refcount atomic.Int32 // OpenWrite returns same entry for concurrent opens; closed when this hits 0
	store    *SpoolStore
}

// ID returns the SQLite spool_entries.id this entry is bound to.
func (e *SpoolEntry) ID() int64 { return e.id }

// NFSPath returns the in-mount path this entry shadows.
func (e *SpoolEntry) NFSPath() string { return e.nfsPath }

// SpoolFilePath returns the on-disk path of the spool file.
func (e *SpoolEntry) SpoolFilePath() string { return e.spoolFile }

// WrittenEnd returns the current high-water byte count. Safe to call
// concurrently with WriteAt; the value is monotonic.
func (e *SpoolEntry) WrittenEnd() int64 {
	e.mu.RLock()
	n := e.writtenEnd
	e.mu.RUnlock()
	return n
}

// SetInode sets the synthetic inode for this entry. Called once by the
// NFS handler immediately after OpenWrite so Stat/Lstat returning a
// FileInfo for this entry reports a stable inode for the lifetime of
// the entry. CAS preserves once-set semantics: only the first non-zero
// SetInode wins; subsequent calls are no-ops.
func (e *SpoolEntry) SetInode(inode uint64) {
	e.inode.CompareAndSwap(0, inode)
}

// Inode returns the synthetic inode set via SetInode. Zero if SetInode
// was never called (drainer rows recovered via boot scrubber will have
// 0 here; callers must guard).
func (e *SpoolEntry) Inode() uint64 { return e.inode.Load() }

// LastWrite returns the time of the most recent extending WriteAt as a
// time.Time. Used by the slice D Stat/Lstat shadow path for mtime.
func (e *SpoolEntry) LastWrite() time.Time {
	return time.Unix(0, e.lastWrite.Load())
}

// SHA256 returns the final streaming SHA-256 hash. Returns nil if Close
// has not completed OR if the entry observed any out-of-order WriteAt
// during its lifetime (in which case the streaming hash is unreliable
// and the drainer must re-hash from disk).
func (e *SpoolEntry) SHA256() []byte {
	e.mu.RLock()
	sha := e.sha256
	e.mu.RUnlock()
	return sha
}

// StreamingHashValid reports whether the streaming SHA-256 reflects the
// on-disk contents. False if any WriteAt arrived out of offset order.
// The drainer uses this to decide whether to trust SHA() or re-hash.
func (e *SpoolEntry) StreamingHashValid() bool {
	e.mu.RLock()
	v := e.hashValid
	e.mu.RUnlock()
	return v
}

// WriteAt appends bytes to the spool file at offset off and folds them
// into the streaming SHA-256 hasher.
//
// Honors the SpoolStore capacity cap: if adding n bytes would push used
// past capacity, returns ErrSpoolFull and writes nothing.
//
// Capacity reservation uses the store's CAS reservation so two concurrent
// writers on DIFFERENT entries can never both pass the cap check and
// over-fill. Any reserved-but-unused bytes (short write) are released
// after the underlying WriteAt returns.
//
// Out-of-order detection: if off < the current writtenEnd, the streaming
// hash diverges from the file's at-rest hash. We mark hashValid=false so
// the drainer knows to re-hash from disk rather than trust SHA256().
func (e *SpoolEntry) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	e.mu.Lock()
	if e.closed || e.file == nil {
		e.mu.Unlock()
		return 0, fmt.Errorf("spool: write to closed entry")
	}
	newEnd := off + int64(len(p))
	var reserved int64
	if newEnd > e.writtenEnd {
		reserved = newEnd - e.writtenEnd
		if !e.store.tryReserveCapacity(reserved) {
			e.mu.Unlock()
			return 0, ErrSpoolFull
		}
	}
	outOfOrder := off < e.writtenEnd

	n, err := e.file.WriteAt(p, off)
	if n > 0 {
		end := off + int64(n)
		if end > e.writtenEnd {
			actualDelta := end - e.writtenEnd
			if actualDelta < reserved {
				e.store.releaseCapacity(reserved - actualDelta)
			}
			e.writtenEnd = end
			e.lastWrite.Store(time.Now().UnixNano())
		} else if reserved > 0 {
			// Wrote bytes but did not extend writtenEnd — release the
			// whole reservation since we never actually grew the file.
			e.store.releaseCapacity(reserved)
		}
		if outOfOrder {
			e.hashValid = false
		}
		if e.hashValid {
			_, _ = e.hasher.Write(p[:n])
		}
	} else if reserved > 0 {
		// Zero-byte write — refund the reservation entirely.
		e.store.releaseCapacity(reserved)
	}
	e.mu.Unlock()
	return n, err
}

// OpenForRead returns a fresh read-only fd on the spool file. Caller
// is responsible for closing it. Used by:
//   - the drainer (slice B) to stream bytes into the FUSE mount
//   - the read path (slice D) to serve in-flight reads
//
// Returns an error if the entry has been fully drained and the file is
// no longer on disk.
func (e *SpoolEntry) OpenForRead() (*os.File, error) {
	e.mu.RLock()
	path := e.spoolFile
	e.mu.RUnlock()
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("spool: open read: %w", err)
	}
	return f, nil
}

// Close flushes + fsyncs the spool file, finalizes the SHA-256, and
// transitions the SQL row to ready. The drainer is signaled.
//
// Refcount-aware: if other OpenWrite callers hold this entry, Close
// decrements the refcount and returns immediately; the last caller does
// the actual flush + mark-ready.
func (e *SpoolEntry) Close() error {
	if n := e.refcount.Add(-1); n > 0 {
		return nil
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true

	var firstErr error
	if e.file != nil {
		if err := e.file.Sync(); err != nil {
			firstErr = fmt.Errorf("spool: fsync: %w", err)
		}
		if err := e.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("spool: close fd: %w", err)
		}
		e.file = nil
	}
	if e.hashValid && e.hasher != nil {
		e.sha256 = e.hasher.Sum(nil)
	}
	finalSize := e.writtenEnd
	finalSha := e.sha256 // nil if streaming hash was invalidated; drainer re-hashes
	e.mu.Unlock()

	if err := e.store.meta.MarkReady(e.id, finalSize, finalSha); err != nil && firstErr == nil {
		firstErr = err
	}
	e.store.signalReady()
	return firstErr
}

// CloseAndDelete is used by tests + the boot-scrubber failure path:
// abandons the entry, removes the spool file, marks the SQL row failed.
// Returns the first error encountered but always attempts every step.
//
// Idempotent — second invocation is a no-op. Crucially, if a regular
// Close() has already finalized the entry (transitioning the SQL row to
// `ready`), CloseAndDelete must NOT clobber that state — we early-out
// when closed and rely on the drainer to handle the ready row normally.
//
// Index removal is identity-checked (DeleteIfMatches) so a scrubber
// cleaning up entry A never accidentally evicts entry B that was
// re-inserted at the same path after A was closed.
func (e *SpoolEntry) CloseAndDelete(reason string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		// Even if already closed via Close(), make sure we don't leave
		// a stale index entry. DeleteIfMatches is safe: it only removes
		// the entry if WE are the current holder of the path slot.
		e.store.index.DeleteIfMatches(e.nfsPath, e)
		return nil
	}
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	path := e.spoolFile
	written := e.writtenEnd
	e.closed = true
	e.mu.Unlock()

	var firstErr error
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		firstErr = fmt.Errorf("spool: remove file: %w", err)
	} else if err == nil {
		e.store.releaseCapacity(written)
	}
	if err := e.store.meta.MarkFailed(e.id, reason); err != nil && firstErr == nil {
		firstErr = err
	}
	e.store.index.DeleteIfMatches(e.nfsPath, e)
	return firstErr
}
