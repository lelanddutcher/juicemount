// Package mutationauthority records whole-volume ownership and mutation
// outcomes for the future single NAS gateway. It is not a network gateway or
// a backend write fence; only a trusted gateway may open this journal.
package mutationauthority

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

var (
	ErrLocked          = errors.New("mutation journal is already open")
	ErrInvalid         = errors.New("invalid mutation authority argument")
	ErrOwnerActive     = errors.New("volume already has a live owner")
	ErrUnsettled       = errors.New("prior epoch has unsettled operations")
	ErrStaleEpoch      = errors.New("owner session or epoch is stale")
	ErrLeaseExpired    = errors.New("owner lease expired; new admission closed")
	ErrChangedRequest  = errors.New("operation ID reused with changed request")
	ErrUncertain       = errors.New("operation effect is uncertain; reconcile before retry or succession")
	ErrNoEffect        = errors.New("operation was confirmed not applied")
	ErrInFlight        = errors.New("backend callback is still in flight")
	ErrClosed          = errors.New("mutation journal closed")
	ErrNoSuchOperation = errors.New("operation does not exist")
)

// Lease is a monotonic whole-volume ownership epoch. The wall-clock deadline
// closes *new* admission; it does not settle an already prepared effect.
type Lease struct {
	Volume      string
	Session     string
	Epoch       int64
	ExpiresAtNS int64
}

// Request is immutable for an operation ID. Digest must be the lowercase hex
// SHA-256 of the gateway's canonical mutation request, including expected
// object/parent revisions and payload identity; this package cannot inspect
// the request body or invent those preconditions.
type Request struct {
	Lease
	ID               string
	Digest           string
	ExpectedRevision string
	Kind             OperationKind
	FileID           string
}

// DurabilityProof is supplied by the trusted gateway backend only after it
// verifies the precise operation's durable backend outcome. Evidence must be
// an opaque, stable identifier useful for later independent reconciliation.
// A successful syscall, FUSE close, or path/size lookup alone is not proof.
type DurabilityProof struct {
	Revision string
	Evidence string
}

// Receipt is stable across identical retries after journal commit.
type Receipt struct {
	Volume            string
	ID                string
	Session           string
	Epoch             int64
	Digest            string
	ExpectedRevision  string
	Revision          string
	Evidence          string
	CommittedAtUnixNS int64
	Kind              OperationKind `json:",omitempty"`
	FileID            string        `json:",omitempty"`
}

type State string

const (
	Prepared  State = "PREPARED"
	Committed State = "COMMITTED"
	NoEffect  State = "NO_EFFECT"
)

// Operation is a read-only journal view. A PREPARED row after an error or
// restart is uncertain, never eligible for blind replay.
type Operation struct {
	Request
	State              State
	Receipt            *Receipt
	ResolutionEvidence string
}

// Backend is the trusted gateway capability. Apply must not return a proof
// until the exact effect and its durability are confirmed. Any error or empty
// proof leaves PREPARED and requires explicit reconciliation.
type Backend interface {
	Apply(context.Context, Request) (DurabilityProof, error)
}

// Journal holds an exclusive process lock for the lifetime of its SQLite DB.
// The parent directory must be private and on durable local storage; neither
// SQLite nor flock can fence a separate gateway with retained backend creds.
type Journal struct {
	mu                    sync.Mutex
	db                    *sql.DB
	lock                  *os.File
	active                map[operationKey]struct{}
	deadlines             map[string]time.Time // process-monotonic; never restored from wall time
	closed                bool
	processPinned         bool
	backendStarted        bool
	backendRecorded       bool
	backendProcess        BackendProcess
	initialOperationRowID int64
	retainedGeneration    int64            // populated only by a trusted completed fence in this process
	now                   func() time.Time // must be monotonic; injectable for tests
	wallNow               func() time.Time // informational timestamps only
}

// Open refuses another process (or another Journal in this process) for the
// same path. The private parent must already exist and have durable directory
// entries; the gateway provisions and syncs it and its ancestors. The lock
// file must not be removed or replaced while in use.
func Open(path string) (*Journal, error) {
	if path == "" {
		return nil, ErrInvalid
	}
	full, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(full))
	if err != nil {
		return nil, err
	}
	if err := requirePrivateDirectory(parent); err != nil {
		return nil, err
	}
	full = filepath.Join(parent, filepath.Base(full))
	// One journal per private directory. A fixed lock name makes case-folded
	// path aliases converge on the same lock on macOS. Hardlinks/symlinks to
	// the DB or lock are refused, so a second directory cannot alias the DB.
	if err := requireSingleRegularFile(full); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(parent, ".mutationauthority.lock")
	if err := requireSingleRegularFile(lockPath); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := requireSingleRegularFile(lockPath); err != nil {
		_ = lock.Close()
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, err
	}
	cleanup := func() { _ = unix.Flock(int(lock.Fd()), unix.LOCK_UN); _ = lock.Close() }
	if same, err := sameOpenFile(lock, lockPath); err != nil || !same {
		cleanup()
		return nil, ErrInvalid
	}
	// Explicitly create a private DB inode before SQLite creates its WAL.
	file, err := os.OpenFile(full, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := file.Close(); err != nil {
		cleanup()
		return nil, err
	}
	if err := requireSingleRegularFile(full); err != nil {
		cleanup()
		return nil, err
	}
	db, err := sql.Open("sqlite", full)
	if err != nil {
		cleanup()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// Recognize an existing database before changing its journal mode or schema.
	if _, _, err := recognizeSchema(db); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	// FULL is required here: a committed receipt must survive a journal crash.
	for _, pragma := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000"} {
		if _, err = db.Exec(pragma); err != nil {
			_ = db.Close()
			cleanup()
			return nil, err
		}
	}
	var mode string
	var syncLevel int
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	if err := db.QueryRow(`PRAGMA synchronous`).Scan(&syncLevel); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	if !strings.EqualFold(mode, "wal") || syncLevel != 2 {
		_ = db.Close()
		cleanup()
		return nil, fmt.Errorf("%w: journal requires WAL and synchronous FULL", ErrInvalid)
	}
	if err = migrateSchema(db); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}

	// A process restart cannot reconstruct the old monotonic deadline from a
	// wall timestamp. Conservatively freeze every persisted owner, even when
	// expires_ns appears to be in the future. Exact receipt reads and trusted
	// reconciliation remain available; new effects require a settled next epoch.
	if _, err = db.Exec(`UPDATE owners SET admission_closed=1 WHERE admission_closed=0`); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	// Persist directory entries for the initial DB/lock/WAL creation before
	// returning a journal that can issue receipts.
	if err := syncDirectory(parent); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	var initialRowID int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(rowid),0) FROM operations`).Scan(&initialRowID); err != nil {
		_ = db.Close()
		cleanup()
		return nil, err
	}
	return &Journal{db: db, lock: lock, active: make(map[operationKey]struct{}), initialOperationRowID: initialRowID,
		deadlines: make(map[string]time.Time), now: time.Now, wallNow: time.Now}, nil
}

func requirePrivateDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%w: journal directory must be owned by gateway and mode 0700: %s", ErrInvalid, path)
	}
	return nil
}

func requireSingleRegularFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%w: journal file must be private, regular, and unlinked to other names: %s", ErrInvalid, path)
	}
	return nil
}

func sameOpenFile(file *os.File, path string) (bool, error) {
	openInfo, err := file.Stat()
	if err != nil {
		return false, err
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	return os.SameFile(openInfo, pathInfo), nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Close refuses to release the process lock while a local backend callback
// is running. Crash-surviving children/handles remain the gateway's fence.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	if j.processPinned {
		return ErrProcessLifetime
	}
	if len(j.active) != 0 {
		return ErrInFlight
	}
	if err := j.db.Close(); err != nil {
		return err
	}
	j.closed = true
	unlockErr := unix.Flock(int(j.lock.Fd()), unix.LOCK_UN)
	return errors.Join(unlockErr, j.lock.Close())
}

// Claim grants epoch 1 or a successor only after expiry and all prior effects
// have settled. A same-session renewal retains its epoch only before expiry;
// after expiry even that session needs a settled, incremented epoch.
func (j *Journal) Claim(ctx context.Context, volume, session string, ttl time.Duration) (Lease, error) {
	if volume == "" || session == "" || ttl <= 0 {
		return Lease{}, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Lease{}, ErrClosed
	}
	return j.claimLocked(ctx, volume, session, ttl)
}

// Renew extends this exact live epoch. Unlike Claim, it never creates an owner
// or advances an expired epoch. Network gateways must authenticate the session.
func (j *Journal) Renew(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Lease{}, ErrClosed
	}
	if err := j.checkLease(ctx, lease, true); err != nil {
		return Lease{}, err
	}
	// Renewal linearizes at the successful live-lease check above. Do not
	// enter Claim's successor branch if time advances while persisting it.
	now, wall := j.now(), j.wallNow()
	deadline, expires := now.Add(ttl), wall.Add(ttl).UnixNano()
	if !deadline.After(now) || expires <= wall.UnixNano() {
		return Lease{}, ErrInvalid
	}
	if _, err := j.db.ExecContext(ctx, `UPDATE owners SET expires_ns=? WHERE volume=? AND session=? AND epoch=?`, expires, lease.Volume, lease.Session, lease.Epoch); err != nil {
		return Lease{}, err
	}
	j.deadlines[lease.Volume] = deadline
	lease.ExpiresAtNS = expires
	return lease, nil
}

func (j *Journal) claimLocked(ctx context.Context, volume, session string, ttl time.Duration) (Lease, error) {
	now := j.now()
	deadline := now.Add(ttl)
	wallNow := j.wallNow()
	expires := wallNow.Add(ttl).UnixNano()
	if !deadline.After(now) || expires <= wallNow.UnixNano() {
		return Lease{}, ErrInvalid
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback()
	var prior Lease
	var closed int
	err = tx.QueryRowContext(ctx, `SELECT session,epoch,expires_ns,admission_closed FROM owners WHERE volume=?`, volume).
		Scan(&prior.Session, &prior.Epoch, &prior.ExpiresAtNS, &closed)
	currentDeadline, hasDeadline := j.deadlines[volume]
	live := closed == 0 && hasDeadline && now.Before(currentDeadline)
	if err == nil && !live {
		// Fail closed in memory even if the SQL latch below cannot commit.
		// Open will freeze the persisted owner after a process crash.
		delete(j.deadlines, volume)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		prior = Lease{Volume: volume, Session: session, Epoch: 1, ExpiresAtNS: expires}
		_, err = tx.ExecContext(ctx, `INSERT INTO owners(volume,session,epoch,expires_ns,admission_closed) VALUES(?,?,?,?,0)`, volume, session, prior.Epoch, expires)
	case err != nil:
		return Lease{}, err
	case live && prior.Session == session:
		prior.Volume, prior.ExpiresAtNS = volume, expires
		_, err = tx.ExecContext(ctx, `UPDATE owners SET expires_ns=? WHERE volume=?`, expires, volume)
	case live:
		return Lease{}, ErrOwnerActive
	default:
		if closed == 0 {
			if _, err = tx.ExecContext(ctx, `UPDATE owners SET admission_closed=1 WHERE volume=? AND epoch=?`, volume, prior.Epoch); err != nil {
				return Lease{}, err
			}
		}
		var unsettled int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations o WHERE volume=? AND state='PREPARED'
		 AND NOT (kind=? AND (EXISTS(SELECT 1 FROM media_retention r WHERE r.volume=o.volume AND r.id=o.id AND r.generation=?)
		 OR EXISTS(SELECT 1 FROM media_publication_retention r WHERE r.volume=o.volume AND r.id=o.id AND r.generation=?)))`, volume, MediaPublishOperation, j.retainedGeneration, j.retainedGeneration).Scan(&unsettled); err != nil {
			return Lease{}, err
		}
		if unsettled != 0 {
			if err = tx.Commit(); err != nil {
				return Lease{}, err
			}
			delete(j.deadlines, volume)
			return Lease{}, ErrUnsettled
		}
		if prior.Epoch == int64(^uint64(0)>>1) {
			return Lease{}, ErrInvalid
		}
		prior = Lease{Volume: volume, Session: session, Epoch: prior.Epoch + 1, ExpiresAtNS: expires}
		_, err = tx.ExecContext(ctx, `UPDATE owners SET session=?,epoch=?,expires_ns=?,admission_closed=0 WHERE volume=?`, session, prior.Epoch, expires, volume)
	}
	if err != nil {
		return Lease{}, err
	}
	if err = tx.Commit(); err != nil {
		return Lease{}, err
	}
	j.deadlines[volume] = deadline
	return prior, nil
}

func validRequest(r Request) bool {
	if r.Volume == "" || r.Session == "" || r.Epoch <= 0 || r.ID == "" || len(r.Digest) != 64 || strings.ToLower(r.Digest) != r.Digest {
		return false
	}
	_, err := hex.DecodeString(r.Digest)
	return err == nil && validOperationIdentity(r)
}

// A typed tuple cannot alias when a volume or operation ID contains NUL.
type operationKey struct{ volume, id string }

func keyForOperation(volume, id string) operationKey { return operationKey{volume: volume, id: id} }

// Execute commits PREPARED before invoking the backend outside the journal
// mutex/transaction. An identical committed retry returns the same receipt;
// PREPARED never invokes Apply twice. Callback errors leave uncertain custody.
func (j *Journal) Execute(ctx context.Context, r Request, backend Backend) (Receipt, error) {
	return j.execute(ctx, r, backend, nil)
}

func (j *Journal) execute(ctx context.Context, r Request, backend Backend, stage *StageIntent) (Receipt, error) {
	if !validRequest(r) || backend == nil {
		return Receipt{}, ErrInvalid
	}
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return Receipt{}, ErrClosed
	}
	if err := j.checkLease(ctx, r.Lease, false); err != nil {
		j.mu.Unlock()
		return Receipt{}, err
	}
	prior, err := j.readOperation(ctx, r.Volume, r.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		j.mu.Unlock()
		return Receipt{}, err
	}
	if err == nil {
		if stage != nil {
			priorStage, stageErr := j.readStage(ctx, r.Volume, r.ID)
			if stageErr != nil || !sameStageIntent(priorStage.Intent, *stage) {
				j.mu.Unlock()
				return Receipt{}, ErrChangedRequest
			}
		}
		j.mu.Unlock()
		if !sameRequest(prior.Request, r) {
			return Receipt{}, ErrChangedRequest
		}
		if stage != nil {
			return Receipt{}, ErrUncertain
		}
		if r.Kind == MediaPublishOperation {
			return Receipt{}, ErrInvalid
		}
		switch prior.State {
		case Committed:
			return *prior.Receipt, nil
		case NoEffect:
			return Receipt{}, ErrNoEffect
		default:
			return Receipt{}, ErrUncertain
		}
	}
	if stage == nil && r.Kind == MediaPublishOperation {
		j.mu.Unlock()
		return Receipt{}, ErrInvalid
	}
	// Expiry bars a new effect, but it must not hide an exact durable
	// receipt or an unresolved effect from the owning epoch.
	if err := j.checkLease(ctx, r.Lease, true); err != nil {
		j.mu.Unlock()
		return Receipt{}, err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		j.mu.Unlock()
		return Receipt{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO operations(volume,id,session,epoch,digest,expected_revision,kind,file_id,state)
		VALUES(?,?,?,?,?,?,?,?,'PREPARED')`, r.Volume, r.ID, r.Session, r.Epoch, r.Digest, r.ExpectedRevision, r.Kind, r.FileID); err != nil {
		j.mu.Unlock()
		return Receipt{}, err
	}
	if stage != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO media_stages(volume,id,body,path,backend,size,sha256) VALUES(?,?,?,?,?,?,?)`,
			r.Volume, r.ID, stage.Body, stage.Path, stage.Backend, stage.Size, stage.SHA256); err != nil {
			j.mu.Unlock()
			return Receipt{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		j.mu.Unlock()
		return Receipt{}, err
	}
	key := keyForOperation(r.Volume, r.ID)
	j.active[key] = struct{}{}
	j.mu.Unlock()

	// The callback is a trusted capability. A panic still releases the local
	// active marker; the durable PREPARED row remains for reconciliation.
	defer func() {
		j.mu.Lock()
		delete(j.active, key)
		j.mu.Unlock()
	}()
	proof, applyErr := backend.Apply(ctx, r)
	if stage != nil {
		// A stage checkpoint never becomes a publication receipt, regardless
		// of what a callback returns. It stays PREPARED for exact reconciliation.
		return Receipt{}, errors.Join(ErrUncertain, applyErr)
	}
	if applyErr != nil || proof.Revision == "" || proof.Evidence == "" {
		return Receipt{}, errors.Join(ErrUncertain, applyErr)
	}
	// Use a fresh context: cancellation of a client request must not prevent
	// recording an outcome that the backend has already durably confirmed.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receipt, err := j.resolveCommitted(commitCtx, r, proof)
	if err != nil {
		return Receipt{}, errors.Join(ErrUncertain, err)
	}
	return receipt, nil
}

func (j *Journal) checkLease(ctx context.Context, lease Lease, requireLive bool) error {
	var session string
	var epoch int64
	var closed int
	if err := j.db.QueryRowContext(ctx, `SELECT session,epoch,admission_closed FROM owners WHERE volume=?`, lease.Volume).Scan(&session, &epoch, &closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleEpoch
		}
		return err
	}
	if session != lease.Session || epoch != lease.Epoch {
		return ErrStaleEpoch
	}
	if requireLive {
		deadline, present := j.deadlines[lease.Volume]
		if closed != 0 || !present || !j.now().Before(deadline) {
			// Drop the live capability before attempting a durable update.
			// A SQL error must never let an in-process clock rollback revive it.
			delete(j.deadlines, lease.Volume)
			if closed == 0 {
				// The observed expiry is a durable one-way latch. A later wall
				// rollback cannot resurrect this epoch's admission.
				if _, err := j.db.ExecContext(ctx, `UPDATE owners SET admission_closed=1 WHERE volume=? AND epoch=?`, lease.Volume, epoch); err != nil {
					return err
				}
			}
			return ErrLeaseExpired
		}
	}
	return nil
}

func (j *Journal) readOperation(ctx context.Context, volume, id string) (Operation, error) {
	var o Operation
	var revision, evidence string
	var committedAt int64
	err := j.db.QueryRowContext(ctx, `SELECT session,epoch,digest,expected_revision,kind,file_id,state,revision,evidence,committed_ns
		FROM operations WHERE volume=? AND id=?`, volume, id).Scan(&o.Session, &o.Epoch, &o.Digest,
		&o.ExpectedRevision, &o.Kind, &o.FileID, &o.State, &revision, &evidence, &committedAt)
	if err != nil {
		return Operation{}, err
	}
	o.Volume, o.ID = volume, id
	if o.State == Committed {
		o.Receipt = &Receipt{Volume: volume, ID: id, Session: o.Session, Epoch: o.Epoch,
			Digest: o.Digest, ExpectedRevision: o.ExpectedRevision, Kind: o.Kind, FileID: o.FileID, Revision: revision,
			Evidence: evidence, CommittedAtUnixNS: committedAt}
	} else if o.State == NoEffect {
		o.ResolutionEvidence = evidence
	}
	return o, nil
}

// Inspect returns durable state for a specific operation. It never infers a
// backend effect from a pathname or replays one.
func (j *Journal) Inspect(ctx context.Context, volume, id string) (Operation, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Operation{}, ErrClosed
	}
	o, err := j.readOperation(ctx, volume, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNoSuchOperation
	}
	return o, err
}

// ListPrepared enumerates every unfinished operation in deterministic ID order.
// Pass the last returned ID as after; an empty page ends enumeration. This is a
// trusted reconciliation API, not an authenticated client endpoint. Resolving
// earlier rows while paging is safe; callers must close admission during a
// startup scan so newly inserted IDs cannot be missed.
func (j *Journal) ListPrepared(ctx context.Context, volume, after string, limit int) ([]Operation, error) {
	if volume == "" || limit < 1 || limit > 256 {
		return nil, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrClosed
	}
	rows, err := j.db.QueryContext(ctx, `SELECT id,session,epoch,digest,expected_revision,kind,file_id
		FROM operations WHERE volume=? AND state='PREPARED' AND id>? ORDER BY id LIMIT ?`, volume, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Operation
	for rows.Next() {
		o := Operation{State: Prepared}
		o.Volume = volume
		if err := rows.Scan(&o.ID, &o.Session, &o.Epoch, &o.Digest, &o.ExpectedRevision, &o.Kind, &o.FileID); err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	return result, rows.Err()
}

// RequireOnlyVolume validates a fixed-volume gateway's entire journal before
// startup reconciliation. Operations reference owners, so a foreign owner is
// refused even when it has no unfinished operation. Call before admission.
func (j *Journal) RequireOnlyVolume(ctx context.Context, volume string) error {
	if volume == "" {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	var foreign bool
	if err := j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM owners WHERE volume<>?
		UNION ALL SELECT 1 FROM backend_processes WHERE volume<>?
		UNION ALL SELECT 1 FROM backend_configuration WHERE volume<>?)`, volume, volume, volume).Scan(&foreign); err != nil {
		return err
	}
	if foreign {
		return fmt.Errorf("%w: foreign volume in gateway journal", ErrInvalid)
	}
	return nil
}

// ValidateRequests streams every persisted request across all volumes and
// states through a trusted, pure format validator before gateway admission.
// The validator must not call the journal or perform external effects.
func (j *Journal) ValidateRequests(ctx context.Context, accept func(Request) bool) error {
	if accept == nil {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	rows, err := j.db.QueryContext(ctx, `SELECT volume,id,session,epoch,digest,expected_revision,kind,file_id FROM operations`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r Request
		if err := rows.Scan(&r.Volume, &r.ID, &r.Session, &r.Epoch, &r.Digest, &r.ExpectedRevision, &r.Kind, &r.FileID); err != nil {
			return err
		}
		if !accept(r) {
			return fmt.Errorf("%w: unsupported journal request", ErrInvalid)
		}
	}
	return rows.Err()
}

// ResolveCommitted records a separately reconciled, durably confirmed effect.
// It rejects settlement while this process still has Apply running.
func (j *Journal) ResolveCommitted(ctx context.Context, r Request, proof DurabilityProof) (Receipt, error) {
	if !validRequest(r) || proof.Revision == "" || proof.Evidence == "" {
		return Receipt{}, ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Receipt{}, ErrClosed
	}
	if _, active := j.active[keyForOperation(r.Volume, r.ID)]; active {
		return Receipt{}, ErrInFlight
	}
	return j.resolveCommitted(ctx, r, proof)
}

func (j *Journal) resolveCommitted(ctx context.Context, r Request, proof DurabilityProof) (Receipt, error) {
	// Called by Execute while its active marker is held, or by the public
	// resolver under j.mu. No global mutex spans Apply.
	if err := j.checkLease(ctx, r.Lease, false); err != nil {
		return Receipt{}, err
	}
	o, err := j.readOperation(ctx, r.Volume, r.ID)
	if err != nil {
		return Receipt{}, err
	}
	if !sameRequest(o.Request, r) {
		return Receipt{}, ErrChangedRequest
	}
	// No qualified media publication/recovery adapter exists yet. Generic
	// evidence must not turn a stage (or historical media row) into a receipt.
	if r.Kind == MediaPublishOperation {
		return Receipt{}, ErrInvalid
	}
	if o.State == Committed {
		if o.Receipt.Revision != proof.Revision || o.Receipt.Evidence != proof.Evidence {
			return Receipt{}, ErrChangedRequest
		}
		return *o.Receipt, nil
	}
	if o.State != Prepared {
		return Receipt{}, ErrNoEffect
	}
	committedAt := j.wallNow().UnixNano()
	result, err := j.db.ExecContext(ctx, `UPDATE operations SET state='COMMITTED',revision=?,evidence=?,committed_ns=?
		WHERE volume=? AND id=? AND state='PREPARED'`, proof.Revision, proof.Evidence, committedAt, r.Volume, r.ID)
	if err != nil {
		return Receipt{}, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return Receipt{}, errors.Join(ErrUncertain, err)
	}
	return Receipt{Volume: r.Volume, ID: r.ID, Session: r.Session, Epoch: r.Epoch,
		Digest: r.Digest, ExpectedRevision: r.ExpectedRevision, Kind: r.Kind, FileID: r.FileID, Revision: proof.Revision,
		Evidence: proof.Evidence, CommittedAtUnixNS: committedAt}, nil
}

// ResolveNoEffect requires a trusted backend reconciliation that proves the
// exact PREPARED effect did not happen and cannot still happen. Cancellation,
// lease expiry, or a callback error is insufficient evidence.
func (j *Journal) ResolveNoEffect(ctx context.Context, r Request, evidence string) error {
	if !validRequest(r) || evidence == "" {
		return ErrInvalid
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrClosed
	}
	if _, active := j.active[keyForOperation(r.Volume, r.ID)]; active {
		return ErrInFlight
	}
	if err := j.checkLease(ctx, r.Lease, false); err != nil {
		return err
	}
	o, err := j.readOperation(ctx, r.Volume, r.ID)
	if err != nil {
		return err
	}
	if !sameRequest(o.Request, r) {
		return ErrChangedRequest
	}
	if r.Kind == MediaPublishOperation {
		return ErrInvalid
	}
	if o.State == Committed {
		return ErrChangedRequest
	}
	if o.State == NoEffect {
		if o.ResolutionEvidence != evidence {
			return ErrChangedRequest
		}
		return nil
	}
	_, err = j.db.ExecContext(ctx, `UPDATE operations SET state='NO_EFFECT',evidence=? WHERE volume=? AND id=? AND state='PREPARED'`, evidence, r.Volume, r.ID)
	return err
}

// DigestBytes is a convenience for callers constructing a request digest
// from already-canonical bytes. Canonicalization itself belongs to gateway.
func DigestBytes(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func (r Receipt) String() string {
	return fmt.Sprintf("%s/%s epoch=%d revision=%s", r.Volume, r.ID, r.Epoch, r.Revision)
}
