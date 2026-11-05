// Package auditlog is the RESP proxy's per-person mutation log: a durable,
// append-only, hash-chained JSON-lines file under the proxy's state
// directory, read by the Manager.
//
// Every line is one Record. Its "prev" field is the lower-case hex SHA-256 of
// the previous line's bytes (without the newline); the first line ever has
// 64 zeros. Changing, inserting or removing a line therefore breaks the
// chain at the next line (Verify). The chain is not keyed: whoever can write
// the directory can rewrite it and recompute every hash, and cutting lines
// off the END is not detectable from the file alone (the newest records can
// be lost to a crash as well as removed). The Manager narrows that gap with
// anchors (Anchor, Follow, VerifyAnchors): it records the head of the log
// every minute in its own state, outside this directory, and a rewrite of
// anything before the last anchor, or a truncation to before it, no longer
// verifies. Someone who can write both places can still rewrite both.
//
// The log is split into segments named audit-<first seq>.jsonl; the chain
// continues across them. A segment is closed at Options.SegmentBytes.
// Retention (Options.RetainBytes, Options.RetainAge) removes the oldest
// closed segments; before it does, it appends a "retention" record that
// names the first record kept and that record's "prev", so the chain still
// verifies from the retained start, and a log whose start was cut without
// such a record does not. That record is not authenticated either: whoever
// can write the directory can remove the oldest segments and append one,
// chained to the head. The chain alone cannot tell; the Manager judges each
// retention record it reads against the policy and its anchors
// (CheckRetention, Uncovered), which catches a cut the policy would not
// have made, but not one it could have.
//
// Append never blocks the caller on disk: it assigns the sequence number and
// queues the record; one writer goroutine encodes, writes and fsyncs in
// batches (group commit: everything queued while the previous batch was
// being written goes out with one write and one fsync). AppendDurable
// queues a record and waits until that batch is fsynced, for a caller that
// must not act before its record is on disk. A failed write or fsync, or a
// segment that is no longer at its path (the directory or file was removed
// or replaced; checked before every write), puts the writer in the failed
// state: Append then refuses (ErrUnavailable) until a retry reopens the log
// (running retention first, so a log that filled its disk can recover) and
// writes a "gap" record that counts the records lost meanwhile.
//
// The Manager keeps a second log in this format, in its own state
// directory, for agent tokens and the catalog edits agents make
// (manager/agenttokens.go): intent and outcome records with Action naming
// the change and Detail what it changed. Each log has its single writer.
//
// A Lazy writer touches nothing on disk until Activate: the proxy keeps the
// log off until the first person exists. With Options.HealthPath the writer
// keeps a small JSON health file (Health) there for the Manager, from the
// moment it is active.
package auditlog

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Record types.
const (
	TypeIntent   = "intent"   // a mutation unit about to be forwarded to Redis
	TypeOutcome  = "outcome"  // what happened to an intent (Unit = its seq)
	TypeRefused  = "refused"  // a unit the proxy refused; nothing reached Redis
	TypeModified = "modified" // file contents written by one principal in one window
	TypeGap      = "gap"      // records lost while the sink failed (Dropped)
	TypeOpen     = "open"     // the writer (re)opened the log
	// TypeRetention: older segments are about to be removed; the chain
	// continues from StartSeq, whose "prev" is StartPrev.
	TypeRetention = "retention"
)

// Outcomes of an intent (TypeOutcome) and of a refusal.
const (
	OutcomeCommitted = "committed" // Redis applied the unit
	OutcomeRefused   = "refused"   // nothing was applied (proxy refusal, Redis error, WATCH abort)
	OutcomeUnknown   = "unknown"   // the reply never arrived; Redis may have applied it
)

// Record is one line of the log.
type Record struct {
	Seq  uint64 `json:"seq"`
	TS   int64  `json:"ts_ms"`
	Type string `json:"type"`
	// Unit: for an outcome, the seq of its intent.
	Unit    uint64 `json:"unit,omitempty"`
	Outcome string `json:"outcome,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Detail  string `json:"detail,omitempty"`

	Principal  string `json:"principal,omitempty"` // the device (proxy principal)
	Kind       string `json:"kind,omitempty"`      // principal kind
	Host       string `json:"host,omitempty"`
	Person     string `json:"person,omitempty"` // stable person ID, "" = no person
	PersonName string `json:"person_name,omitempty"`
	// Agent, AgentName: the agent token (its ID and name) a record of the
	// Manager's catalog log was made with; Principal is then the Mac that
	// minted it.
	Agent     string `json:"agent,omitempty"`
	AgentName string `json:"agent_name,omitempty"`

	// Action summarizes a namespace unit: create, remove, rename, trash,
	// restore, link, setattr, quota (a quota or its usage, nothing else),
	// accounting (dir-stat or deferred-delete rows only) or other; and
	// "collapsed" for a refused record that counts refusals (Count, First,
	// Last) not logged one by one.
	Action   string    `json:"action,omitempty"`
	Projects []Project `json:"projects,omitempty"`
	Ops      []Op      `json:"ops,omitempty"`
	Inodes   []uint64  `json:"inodes,omitempty"` // inodes whose own keys a setattr unit writes
	// Rows: the dir-stat, quota and deferred-delete fields the unit
	// writes (also those that ride along a namespace change).
	Rows []Row `json:"rows,omitempty"`

	// TypeModified: Inode's contents were written Count times between
	// First and Last (unix ms). A collapsed TypeRefused: Count refusals
	// between First and Last.
	Inode        uint64 `json:"inode,omitempty"`
	First        int64  `json:"first_ms,omitempty"`
	Last         int64  `json:"last_ms,omitempty"`
	Count        int    `json:"count,omitempty"`
	Committed    int    `json:"committed,omitempty"`
	Unknown      int    `json:"unknown,omitempty"`
	NotCommitted int    `json:"not_committed,omitempty"`

	// TypeGap: records lost while the sink failed.
	Dropped uint64 `json:"dropped,omitempty"`

	// Attrs: attribute changes of existing inodes the unit makes (mode,
	// owner, group, flags), compared with the stored value.
	Attrs []AttrChange `json:"attrs,omitempty"`

	// TypeRetention: the first record kept and its "prev".
	StartSeq  uint64 `json:"start_seq,omitempty"`
	StartPrev string `json:"start_prev,omitempty"`

	// Omitted: entries (ops, rows, inodes, attribute changes, projects)
	// and values the record leaves out or shortens because the unit was
	// too large to log in full. Digest then identifies the complete unit:
	// "sha256:" and the hex SHA-256 of the unit's commands as the proxy
	// received them, each encoded as a RESP array of bulk strings,
	// concatenated in order, without MULTI and EXEC. It matches a capture
	// of the client's traffic; it matches Redis's AOF only when every
	// command of the unit wrote something (the AOF leaves out reads and
	// writes that changed nothing, and rewrites EXPIRE as PEXPIREAT and the
	// like).
	Omitted int    `json:"omitted,omitempty"`
	Digest  string `json:"digest,omitempty"`

	Prev string `json:"prev"`

	st uint8 // writer state: 0 queued, stDurable, stLost (under Writer.mu)
	// tooLarge: the writer wrote a stub in place of the record (it encoded
	// to more than MaxRecordBytes). Set before st becomes stDurable.
	tooLarge bool
}

// MaxRecordBytes bounds one line of the log. The writer never writes a
// longer one: it writes a stub in its place (the record's seq, time, type,
// who, action, unit and digest, and the size of the line it did not
// write), and AppendDurable reports ErrTooLarge for it. It is well below
// what the readers accept (maxLine), so a log stays readable.
const MaxRecordBytes = 4 << 20

const (
	stDurable = 1
	stLost    = 2
)

// AttrChange is one attribute of an existing inode a unit changes.
type AttrChange struct {
	Ino   uint64 `json:"ino"`
	Field string `json:"field"` // mode, uid, gid, flags, acl, or "attr" (unreadable or new)
	Old   string `json:"old,omitempty"`
	New   string `json:"new,omitempty"`
}

// Row is one field of a volume-wide accounting hash a unit writes: a
// dir-stat or quota-usage counter of directory Ino (dirUsedSpace,
// dirQuotaUsedInodes, ...), its quota definition (dirQuota), or a
// deferred-delete record naming inode Ino (delfiles, detachedNodes).
type Row struct {
	Hash  string `json:"hash"`
	Ino   uint64 `json:"ino"`
	Op    string `json:"op"`              // set, incr or del; add or remove (delfiles)
	Value string `json:"value,omitempty"` // set: the value; incr: the delta
	Path  string `json:"path,omitempty"`  // the directory's path, when known
}

// Project is a project root a unit touches, with its display path when the
// proxy's ancestry cache knows it.
type Project struct {
	Ino  uint64 `json:"ino"`
	Path string `json:"path,omitempty"`
}

// Op is one directory-entry change: "add" (an entry set) or "remove" (an
// entry deleted) of Name in directory Parent. Names that are not valid UTF-8
// are shown with U+FFFD.
type Op struct {
	Op     string `json:"op"`
	Parent uint64 `json:"parent"`
	Name   string `json:"name"`
	Ino    uint64 `json:"ino,omitempty"` // add: the inode the entry names
	Dir    bool   `json:"dir,omitempty"`
	Trash  bool   `json:"trash,omitempty"`
	Path   string `json:"path,omitempty"` // when the parent's path is known
}

var (
	// ErrUnavailable: the sink failed and has not recovered.
	ErrUnavailable = errors.New("auditlog: sink unavailable")
	// ErrQueueFull: more records are waiting than Options.QueueLen.
	ErrQueueFull = errors.New("auditlog: queue full")
	// ErrClosed: the writer was closed.
	ErrClosed = errors.New("auditlog: closed")
	// ErrTimeout: AppendDurable's record was not on disk in time (it may
	// still be written).
	ErrTimeout = errors.New("auditlog: record not durable in time")
	// ErrInactive: a Lazy writer was not activated.
	ErrInactive = errors.New("auditlog: not active")
	// ErrTooLarge: the record encoded to more than MaxRecordBytes; a stub
	// was written in its place.
	ErrTooLarge = errors.New("auditlog: record too large; a stub was written in its place")
)

// Options configures a Writer.
type Options struct {
	SegmentBytes int64         // close a segment past this size; default 64 MiB
	QueueLen     int           // records waiting for the writer; default 65536
	RetryEvery   time.Duration // reopen attempts while failed; default 1 s
	Now          func() time.Time
	Logf         func(format string, args ...any)
	// BeforeWrite, if set, runs before every write to the file; an error
	// is handled exactly like a failed write (tests inject sink failures).
	BeforeWrite func() error

	// Lazy: create nothing on disk until Activate.
	Lazy bool
	// RetainBytes: remove the oldest closed segments while all segments
	// together are larger; default DefaultRetainBytes, < 0 = no limit.
	RetainBytes int64
	// RetainAge: remove closed segments last written longer ago; default
	// DefaultRetainAge, < 0 = no limit.
	RetainAge time.Duration
	// HealthPath, if set, is where the writer keeps its Health (JSON),
	// refreshed every MaintainEvery and on every state change.
	HealthPath string
	// MaintainEvery: how often retention and the health file run; default
	// 10 s.
	MaintainEvery time.Duration
}

// Retention defaults.
const (
	DefaultRetainBytes = 2 << 30
	DefaultRetainAge   = 400 * 24 * time.Hour
)

const (
	segPrefix = "audit-"
	segSuffix = ".jsonl"
)

var zeroHash = strings.Repeat("0", 64)

// Writer appends records to the log in dir.
type Writer struct {
	dir  string
	opts Options

	actMu   sync.Mutex // serializes Activate and Close
	mu      sync.Mutex
	cond    *sync.Cond // durable / failed / closed changes
	nextSeq uint64
	queue   []*Record
	failed  error
	dropped uint64 // records lost since the last gap record
	durable uint64 // highest seq fsynced
	closed  bool
	exited  bool
	active  bool // started (always, unless Lazy and not yet activated)

	// health (under mu)
	lastFsync int64 // unix ms
	lost      uint64
	removed   int
	lastErr   string

	wake chan struct{}
	done chan struct{}

	// note (under mu): why a failed writer stays failed (Health.Note).
	note string

	// writer goroutine (and Open / Activate) only
	f          *os.File
	path       string      // f's segment
	fi         os.FileInfo // f when opened: a removed or replaced segment is noticed
	unnamed    *Record     // a retention that removed segments before its record was written
	segSize    int64
	lastHash   string
	lastHealth Health
	segCount   int
	segBytes   int64
}

// Open opens (or creates) the log in dir and starts its writer. A torn
// last line (a crash in the middle of a write) is cut off; an "open" record
// says so.
//
// If the log cannot be opened, Open returns the error AND a usable Writer
// that starts in the failed state (Append refuses) and retries every
// RetryEvery, so a proxy can keep serving and let its fail-closed rule
// decide. Close it like any other.
//
// With Options.Lazy, Open touches nothing and returns a writer that refuses
// records (ErrInactive) until Activate.
func Open(dir string, opts Options) (*Writer, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if opts.QueueLen <= 0 {
		opts.QueueLen = 1 << 16
	}
	if opts.RetryEvery <= 0 {
		opts.RetryEvery = time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.RetainBytes == 0 {
		opts.RetainBytes = DefaultRetainBytes
	}
	if opts.RetainAge == 0 {
		opts.RetainAge = DefaultRetainAge
	}
	if opts.MaintainEvery <= 0 {
		opts.MaintainEvery = 10 * time.Second
	}
	w := &Writer{dir: dir, opts: opts, wake: make(chan struct{}, 1), done: make(chan struct{})}
	w.cond = sync.NewCond(&w.mu)
	if opts.Lazy {
		// No health file for a log that is off (a single-user volume
		// keeps its state directory as before); one left by an earlier
		// run that had the log on is rewritten, so it does not go stale.
		if _, err := os.Stat(opts.HealthPath); opts.HealthPath != "" && err == nil {
			w.writeHealth(true)
		}
		return w, nil
	}
	return w, w.activate()
}

// Activate starts a Lazy writer: it opens the log (or starts failed and
// retries, like Open) and accepts records from then on. It is a no-op on a
// writer that is already active or closed.
func (w *Writer) Activate() error {
	w.actMu.Lock()
	defer w.actMu.Unlock()
	w.mu.Lock()
	skip := w.active || w.closed
	w.mu.Unlock()
	if skip {
		return nil
	}
	return w.activate()
}

// activate opens the log and starts the writer goroutine. Callers hold
// actMu (or own w exclusively, in Open).
func (w *Writer) activate() error {
	err := w.start()
	w.mu.Lock()
	if err != nil {
		w.closeFile()
		w.failed = err
		w.lastErr = err.Error()
	}
	w.active = true
	w.mu.Unlock()
	w.writeHealth(true) // before the loop owns it
	go w.loop()
	return err
}

// Active reports whether the writer was started (always true unless Lazy).
func (w *Writer) Active() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active
}

func (w *Writer) start() error {
	if err := os.MkdirAll(w.dir, 0o700); err != nil {
		return err
	}
	torn, err := w.reopen()
	if err != nil {
		return err
	}
	detail := "opened"
	if torn > 0 {
		detail = fmt.Sprintf("opened; cut %d bytes of a torn last line", torn)
	}
	open := &Record{Seq: w.nextSeq, TS: w.opts.Now().UnixMilli(), Type: TypeOpen, Detail: detail}
	w.nextSeq++
	if err := w.writeBatch([]*Record{open}); err != nil {
		return err
	}
	w.mu.Lock()
	w.durable = open.Seq
	w.lastFsync = w.opts.Now().UnixMilli()
	w.mu.Unlock()
	return nil
}

// segments lists the segment files of dir, oldest first.
func segments(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type seg struct {
		name  string
		first uint64
	}
	var out []seg
	for _, e := range ents {
		n := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(n, segPrefix) || !strings.HasSuffix(n, segSuffix) {
			continue
		}
		first, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, segPrefix), segSuffix), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, seg{n, first})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].first < out[j].first })
	names := make([]string, len(out))
	for i, s := range out {
		names[i] = filepath.Join(dir, s.name)
	}
	return names, nil
}

func segName(dir string, first uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%020d%s", segPrefix, first, segSuffix))
}

// tail scans one segment: the hash and seq of its last complete line, and
// where that line ends. Bytes after the last newline are a torn write.
func tail(path string) (lastHash string, lastSeq uint64, lines int, end, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, 0, 0, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	for {
		line, rerr := r.ReadBytes('\n')
		off += int64(len(line))
		if rerr == io.EOF {
			return lastHash, lastSeq, lines, end, off, nil
		}
		if rerr != nil {
			return "", 0, 0, 0, 0, rerr
		}
		body := line[:len(line)-1]
		sum := sha256.Sum256(body)
		lastHash = hex.EncodeToString(sum[:])
		var hdr struct {
			Seq uint64 `json:"seq"`
		}
		if json.Unmarshal(body, &hdr) == nil && hdr.Seq > lastSeq {
			lastSeq = hdr.Seq
		}
		lines++
		end = off
	}
}

// reopen finds the chain head on disk and opens the last segment for
// appending (creating the first one). It returns how many torn bytes it
// cut. Called by Open and by the writer goroutine when recovering.
func (w *Writer) reopen() (torn int64, err error) {
	segs, err := segments(w.dir)
	if err != nil {
		return 0, err
	}
	w.lastHash = zeroHash
	var lastSeq uint64
	path := ""
	if len(segs) > 0 {
		path = segs[len(segs)-1]
	}
	for i := len(segs) - 1; i >= 0; i-- {
		h, seq, lines, end, size, err := tail(segs[i])
		if err != nil {
			return 0, err
		}
		if i == len(segs)-1 && size > end {
			if err := os.Truncate(segs[i], end); err != nil {
				return 0, err
			}
			torn = size - end
		}
		if lines > 0 {
			w.lastHash, lastSeq = h, seq
			break
		}
	}
	if path == "" {
		// A new log; after a removed directory its records continue the
		// sequence (the Manager then reports the removal).
		w.mu.Lock()
		first := w.nextSeq
		w.mu.Unlock()
		if lastSeq >= first {
			first = lastSeq + 1
		}
		path = segName(w.dir, max(first, 1))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, err
	}
	if len(segs) == 0 {
		if err := syncDir(w.dir); err != nil {
			f.Close()
			return 0, err
		}
	}
	w.f, w.path, w.fi, w.segSize = f, path, st, st.Size()
	w.mu.Lock()
	if lastSeq >= w.nextSeq {
		w.nextSeq = lastSeq + 1
	}
	w.mu.Unlock()
	return torn, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (w *Writer) closeFile() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
}

// Append queues r and returns its sequence number. It never waits for the
// disk. r must not be used by the caller afterwards.
func (w *Writer) Append(r *Record) (uint64, error) {
	w.mu.Lock()
	switch {
	case w.closed:
		w.mu.Unlock()
		return 0, ErrClosed
	case !w.active:
		w.mu.Unlock()
		return 0, ErrInactive
	case w.failed != nil:
		w.dropped++
		w.mu.Unlock()
		return 0, ErrUnavailable
	case len(w.queue) >= w.opts.QueueLen:
		w.dropped++
		w.mu.Unlock()
		return 0, ErrQueueFull
	}
	r.Seq = w.nextSeq
	w.nextSeq++
	if r.TS == 0 {
		r.TS = w.opts.Now().UnixMilli()
	}
	w.queue = append(w.queue, r)
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return r.Seq, nil
}

// AppendDurable queues r like Append and waits until the batch that
// carries it is fsynced (group commit: records of every caller queued
// meanwhile share the write and the fsync). It returns the sequence number
// whenever r was queued; the error is ErrUnavailable if r's batch failed,
// ErrTimeout if it was not on disk within timeout (it may still be
// written), or Append's error.
func (w *Writer) AppendDurable(r *Record, timeout time.Duration) (uint64, error) {
	seq, err := w.Append(r)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.st == 0 {
		// The timer's own flag ends the wait: a deadline compared with the
		// clock after the wake-up could still lie ahead (it was taken after
		// the timer started), and nothing would wake the wait again.
		expired := false
		t := time.AfterFunc(timeout, func() {
			w.mu.Lock()
			expired = true
			w.cond.Broadcast()
			w.mu.Unlock()
		})
		defer t.Stop()
		for r.st == 0 && !w.exited && !expired {
			w.cond.Wait()
		}
	}
	switch {
	case r.st == stDurable && r.tooLarge:
		return seq, ErrTooLarge
	case r.st == stDurable:
		return seq, nil
	case r.st == stLost:
		return seq, ErrUnavailable
	case w.exited:
		return seq, ErrClosed
	}
	return seq, ErrTimeout
}

// Healthy reports whether Append currently accepts records.
func (w *Writer) Healthy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed == nil && !w.closed && w.active
}

// Sync waits until every record appended before the call is on disk. It
// returns ErrUnavailable if the sink failed first.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	target := w.nextSeq - 1
	for w.durable < target {
		if w.failed != nil {
			return ErrUnavailable
		}
		if w.exited {
			return ErrClosed
		}
		w.cond.Wait()
	}
	return nil
}

// Close writes what is queued, fsyncs and stops the writer.
func (w *Writer) Close() error {
	w.actMu.Lock()
	defer w.actMu.Unlock()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		<-w.done
		return nil
	}
	w.closed = true
	if !w.active {
		w.exited = true
		close(w.done)
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	<-w.done
	return nil
}

func (w *Writer) loop() {
	defer close(w.done)
	defer func() {
		w.closeFile()
		w.mu.Lock()
		w.exited = true
		w.cond.Broadcast()
		w.mu.Unlock()
		w.writeHealth(true)
	}()
	retry := time.NewTicker(w.opts.RetryEvery)
	defer retry.Stop()
	maintain := time.NewTicker(w.opts.MaintainEvery)
	defer maintain.Stop()
	w.retain()
	for {
		select {
		case <-w.wake:
		case <-retry.C:
		case <-maintain.C:
			if !w.failedNow() {
				w.retain()
			}
			w.writeHealth(true)
		}
		for {
			w.mu.Lock()
			batch := w.queue
			w.queue = nil
			closing, failed := w.closed, w.failed != nil
			w.mu.Unlock()
			if failed {
				ok := w.recover(batch)
				if !ok {
					w.lose(batch)
				}
				w.writeHealth(false)
				if ok {
					continue
				}
				if closing {
					return
				}
				break
			}
			if len(batch) == 0 {
				if closing {
					return
				}
				break
			}
			rotated, err := w.writeBatchRotating(batch)
			if err != nil {
				w.fail(err, batch)
				w.writeHealth(false)
				continue
			}
			w.mu.Lock()
			w.durable = batch[len(batch)-1].Seq
			w.lastFsync = w.opts.Now().UnixMilli()
			for _, r := range batch {
				r.st = stDurable
			}
			w.cond.Broadcast()
			w.mu.Unlock()
			if rotated {
				w.retain()
			}
			w.writeHealth(false)
		}
	}
}

func (w *Writer) failedNow() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed != nil
}

// lose counts records that will never be written.
func (w *Writer) lose(batch []*Record) {
	if len(batch) == 0 {
		return
	}
	w.mu.Lock()
	w.dropped += uint64(len(batch))
	w.lost += uint64(len(batch))
	for _, r := range batch {
		r.st = stLost
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *Writer) fail(err error, batch []*Record) {
	w.opts.Logf("auditlog: write failed, refusing records until it recovers: %v", err)
	w.closeFile()
	w.mu.Lock()
	w.failed = err
	w.lastErr = err.Error()
	w.dropped += uint64(len(batch))
	w.lost += uint64(len(batch))
	for _, r := range batch {
		r.st = stLost
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

// recover reopens the log after a failure, writes the records queued
// before the failure was seen (batch, all numbered below the gap) and the
// gap record. False: still failing (batch is not written). True: batch is
// written; the writer is healthy again unless the gap record for Appends
// refused while it was being written could not be written (failed stays
// set and the next attempt's gap counts them).
//
// Retention runs here too: a log that filled its disk past its size or age
// limit would otherwise stay failed (and, while grants are in force, keep
// every Mac write refused). The retention record goes out with the gap
// record; if even that cannot be written (the disk is full), the oldest
// segments the policy removes are removed first and the record, written
// right after, says so. Health.Note says why a writer stays failed.
func (w *Writer) recover(batch []*Record) bool {
	if err := os.MkdirAll(w.dir, 0o700); err != nil {
		w.setNote("the audit directory cannot be created: " + err.Error())
		return false
	}
	if _, err := w.reopen(); err != nil {
		w.closeFile()
		w.setNote("the log cannot be reopened: " + err.Error())
		return false
	}
	plan := w.planRetention()
	w.mu.Lock()
	var ret *Record
	switch {
	case plan != nil:
		ret = plan.record(w.nextSeq, w.opts.Now())
		w.nextSeq++
	case w.unnamed != nil:
		r := *w.unnamed
		ret = &r
		ret.Seq, ret.TS = w.nextSeq, w.opts.Now().UnixMilli()
		w.nextSeq++
	}
	gap := &Record{Seq: w.nextSeq, TS: w.opts.Now().UnixMilli(), Type: TypeGap, Dropped: w.dropped,
		Detail: "sink failed: " + w.failed.Error()}
	w.nextSeq++
	w.mu.Unlock()
	all := append([]*Record(nil), batch...)
	if ret != nil {
		all = append(all, ret)
	}
	all = append(all, gap)
	err := w.writeBatch(all)
	removedFirst := false
	if err != nil && plan != nil {
		// Nothing can be written: free the space the policy frees, then
		// name the new start.
		if start, ok := w.removeSegments(plan); ok {
			removedFirst = true
			w.unnamed = &Record{Type: TypeRetention, StartSeq: start.seq, StartPrev: start.prev,
				Detail: plan.detail() + "; removed before this record was written, while the log could not write: " + err.Error()}
			ret.StartSeq, ret.StartPrev, ret.Detail = start.seq, start.prev, w.unnamed.Detail
			err = w.writeBatch(all)
		}
	}
	if err != nil {
		w.closeFile()
		w.setNote(w.stuckNote(err))
		return false
	}
	if plan != nil && !removedFirst {
		w.removeSegments(plan)
	}
	w.unnamed = nil
	w.mu.Lock()
	w.note = ""
	w.dropped -= gap.Dropped
	w.durable = gap.Seq
	w.lastFsync = w.opts.Now().UnixMilli()
	for _, r := range all {
		r.st = stDurable
	}
	lost := gap.Dropped
	// The writer stays failed until the gap record is on disk, so Appends
	// refused meanwhile are not in it. Another gap record counts them
	// before the writer takes records again; the one that finds none left
	// clears the failure in the same critical section, so no refusal goes
	// uncounted (or into the gap of a later failure).
	for w.dropped > 0 {
		more := &Record{Seq: w.nextSeq, TS: w.opts.Now().UnixMilli(), Type: TypeGap, Dropped: w.dropped,
			Detail: gap.Detail + "; refused while the log was recovering"}
		w.nextSeq++
		w.mu.Unlock()
		err := w.writeBatch([]*Record{more})
		w.mu.Lock()
		if err != nil {
			// Still failed; the next attempt's gap record counts these.
			w.cond.Broadcast()
			w.mu.Unlock()
			w.closeFile()
			w.setNote(w.stuckNote(err))
			return true
		}
		w.dropped -= more.Dropped
		w.durable = more.Seq
		w.lastFsync = w.opts.Now().UnixMilli()
		more.st = stDurable
		lost += more.Dropped
	}
	w.failed = nil
	w.cond.Broadcast()
	w.mu.Unlock()
	w.opts.Logf("auditlog: recovered; %d records were lost", lost)
	return true
}

// writeBatchRotating is writeBatch; rotated reports a segment change.
func (w *Writer) writeBatchRotating(batch []*Record) (rotated bool, err error) {
	before := w.f
	err = w.writeBatch(batch)
	return err == nil && w.f != before, err
}

// writeBatch chains, writes and fsyncs records. On error nothing of the
// batch counts as written: the file is cut back to where it was.
func (w *Writer) writeBatch(batch []*Record) error {
	if w.f == nil {
		return ErrUnavailable
	}
	prevHash := w.lastHash
	var buf bytes.Buffer
	for _, r := range batch {
		r.Prev = w.lastHash
		line, err := json.Marshal(r)
		if err == nil && len(line) > MaxRecordBytes {
			line, err = json.Marshal(stubOf(r, line))
			r.tooLarge = true
		}
		if err != nil {
			w.lastHash = prevHash
			return err
		}
		sum := sha256.Sum256(line)
		w.lastHash = hex.EncodeToString(sum[:])
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if w.segSize > 0 && w.segSize+int64(buf.Len()) > w.opts.SegmentBytes {
		if err := w.rotate(batch[0].Seq); err != nil {
			w.lastHash = prevHash
			return err
		}
	}
	// The file must still be the segment it was opened as: a removed
	// directory or segment would otherwise take fsynced records into an
	// unlinked file and the writer would report itself healthy. The check
	// and the write are not atomic: a segment removed or replaced between
	// the two still takes this batch, which is then reported durable
	// although it is in no file at the segment's path; the next batch's
	// check fails the writer, and the log reopens with a gap record that
	// does not count those records (the Manager's anchors show a missing
	// tail only if one of them was anchored). Whoever can remove the
	// segment can also edit it, so this is not a new way in.
	err := w.sameSegment()
	if err == nil && w.opts.BeforeWrite != nil {
		err = w.opts.BeforeWrite()
	}
	if err == nil {
		_, err = w.f.Write(buf.Bytes())
	}
	if err == nil {
		err = w.f.Sync()
	}
	if err != nil {
		_ = w.f.Truncate(w.segSize)
		w.lastHash = prevHash
		return err
	}
	w.segSize += int64(buf.Len())
	return nil
}

// stubOf is what the writer writes in place of r, whose line was longer
// than MaxRecordBytes.
func stubOf(r *Record, line []byte) *Record {
	clip := func(v string) string {
		if len(v) > 256 {
			return v[:256]
		}
		return v
	}
	return &Record{Seq: r.Seq, TS: r.TS, Type: r.Type, Unit: r.Unit, Outcome: r.Outcome, Reason: clip(r.Reason),
		Principal: clip(r.Principal), Kind: clip(r.Kind), Host: clip(r.Host), Person: clip(r.Person),
		PersonName: clip(r.PersonName), Agent: clip(r.Agent), AgentName: clip(r.AgentName), Action: clip(r.Action), Prev: r.Prev,
		Omitted: max(1, len(r.Ops)+len(r.Rows)+len(r.Inodes)+len(r.Attrs)+len(r.Projects)), Digest: clip(r.Digest),
		Detail: fmt.Sprintf("record not written: it encoded to %d bytes, more than the limit of %d", len(line), MaxRecordBytes)}
}

func (w *Writer) rotate(first uint64) error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	path := segName(w.dir, first)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err == nil {
		err = syncDir(w.dir)
	}
	if err != nil {
		f.Close()
		return err
	}
	w.f.Close()
	w.f, w.path, w.fi, w.segSize = f, path, st, 0
	return nil
}

// sameSegment checks that the open file is still the segment at its path.
func (w *Writer) sameSegment() error {
	st, err := os.Stat(w.path)
	switch {
	case err != nil:
		return fmt.Errorf("audit segment %s is gone (the log was removed?): %w", filepath.Base(w.path), err)
	case w.fi != nil && !os.SameFile(st, w.fi):
		return fmt.Errorf("audit segment %s was replaced", filepath.Base(w.path))
	}
	return nil
}

func (w *Writer) setNote(n string) {
	w.mu.Lock()
	w.note = n
	w.mu.Unlock()
}
