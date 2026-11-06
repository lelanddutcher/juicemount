package manager

// auditstatus.go: the Manager's view of the proxy's audit log (Teams
// slice 1): its health, and anchors that make a rewrite or truncation of
// the log detectable.
//
// Health. jm-respproxy keeps <audit dir>.health (auditlog.Health): whether
// the log is on, accepts records, when it last fsynced, its size. A health
// file older than auditHealthMaxAge means the proxy stopped refreshing it
// (stopped, or a full disk fails that write too). While grants are in force
// a Mac write is refused whenever the log cannot take its intent, so the
// status says so plainly.
//
// Anchors. Every auditAnchorEvery the Manager follows the log from its last
// anchor to the head (auditlog.Follow: it reads only what is new, checks
// the anchored line is unchanged and every new line chains on) and records
// the new head (segment, offset, seq, hash) in audit-anchors.json beside
// its own state, outside the audit directory. A rewrite of the log, or a
// truncation to before the last anchor, then no longer verifies; the
// problem is kept until a full check with every anchor (GET
// /api/audit/verify?full=1) passes. Once anchored, a missing audit
// directory is a removed log.
//
// Retention. A retention record is not authenticated (auditlog), so the
// Manager judges every one it reads against the policy in the health file
// (auditlog.CheckRetention) and keeps the verdicts with its anchors; the
// log's start must be named by a retention record it read, and an anchor
// older than the start must have been removed by one that fits the
// policy. A retention that leaves less than auditMinHistory of history is
// a warning when the size limit made it (a flood of records, or too small
// a size limit).
//
// The limits: records written after the last anchor (up to a minute) can
// be cut unnoticed; a cut of the oldest records that the retention policy
// could have made itself (past the age limit, or with the log near its
// size limit) passes, as does any cut by someone who can also rewrite the
// health file's policy; and whoever can write both the audit directory and
// the Manager's state directory can rewrite both. If the log is deleted on
// purpose, delete audit-anchors.json too.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

var (
	// auditAnchorEvery: how often the Manager anchors the log head.
	auditAnchorEvery = time.Minute
	// auditHealthMaxAge: an older health file is stale (the proxy
	// refreshes it every 10 s).
	auditHealthMaxAge = 45 * time.Second
	// auditMinHistory: a retention that leaves less history than this
	// (the first record kept is younger) raises a warning: a flood of
	// records, or a size limit too small for the volume's activity.
	auditMinHistory = 7 * 24 * time.Hour
)

// auditFullRead is the whole-log read of checkStart (a test hook).
var auditFullRead = auditlog.VerifyAnchors

// maxAnchors bounds audit-anchors.json.
const maxAnchors = 64

// AuditStatus is the admin view of the audit log.
type AuditStatus struct {
	Configured bool `json:"configured"`
	// Enabled: the proxy's writer is on (it starts with the first person).
	Enabled bool `json:"enabled"`
	// Healthy: the log accepts records (and the health file is fresh).
	Healthy     bool   `json:"healthy"`
	Stale       bool   `json:"stale,omitempty"`
	HealthAgeMs int64  `json:"health_age_ms,omitempty"`
	Error       string `json:"error,omitempty"`
	LastError   string `json:"last_error,omitempty"`
	LastFsyncMs int64  `json:"last_fsync_ms,omitempty"`
	Bytes       int64  `json:"bytes"`
	Segments    int    `json:"segments"`
	Removed     int    `json:"removed_segments,omitempty"`
	Lost        uint64 `json:"lost,omitempty"`
	Queue       int    `json:"queue,omitempty"`
	QueueLen    int    `json:"queue_len,omitempty"`
	RetainBytes int64  `json:"retain_bytes,omitempty"`
	RetainAgeS  int64  `json:"retain_age_s,omitempty"`
	FreeBytes   uint64 `json:"free_bytes,omitempty"`
	// Note: why a failed log stays failed (what retention could do).
	Note string `json:"note,omitempty"`
	// RetentionWarning: retention removed recent history.
	RetentionWarning string `json:"retention_warning,omitempty"`
	// RetentionForecast: at the rate the log has grown, the size limit
	// will keep less than auditMinHistory (before it removes anything).
	RetentionForecast string `json:"retention_forecast,omitempty"`
	// Chain: the anchor checks.
	AnchorSeq   uint64 `json:"anchor_seq,omitempty"`
	AnchorAtMs  int64  `json:"anchor_at_ms,omitempty"`
	CheckedAtMs int64  `json:"checked_at_ms,omitempty"`
	ChainOK     bool   `json:"chain_ok"`
	ChainIssue  string `json:"chain_problem,omitempty"`
	// WritesRefused: while grants are in force the log cannot take
	// records, so Mac writes are refused (AUDIT_UNAVAILABLE); Problem says
	// why.
	WritesRefused bool   `json:"writes_refused,omitempty"`
	Problem       string `json:"problem,omitempty"`
}

// anchorState is audit-anchors.json.
type anchorState struct {
	Anchors   []auditlog.Anchor `json:"anchors,omitempty"`
	Problem   string            `json:"problem,omitempty"`
	ProblemAt int64             `json:"problem_at_ms,omitempty"`
	CheckedAt int64             `json:"checked_at_ms,omitempty"`
	// Retentions: the retention records the Manager read, each judged
	// against the retention policy when it was first read.
	Retentions []auditlog.Retention `json:"retentions,omitempty"`
	// Warning: retention removed recent history (see auditMinHistory);
	// kept, like Problem, until a full check.
	Warning   string `json:"warning,omitempty"`
	WarningAt int64  `json:"warning_at_ms,omitempty"`
	// ReadProblem: the log could not be read auditReadFailures times in a
	// row; cleared by the next successful read.
	ReadProblem string `json:"read_problem,omitempty"`
}

// auditWatch anchors the log and reports its state.
type auditWatch struct {
	dir, statePath string

	mu        sync.Mutex
	st        anchorState
	loadErr   error
	readFails int        // consecutive failed minute reads (in memory)
	fullFails int        // consecutive failed whole-log reads of checkStart
	fullMu    sync.Mutex // one full verify at a time

	stop, exited chan struct{}
	once         sync.Once
}

func newAuditWatch(dir, statePath string) *auditWatch {
	w := &auditWatch{dir: dir, statePath: statePath}
	if statePath != "" {
		b, err := os.ReadFile(statePath)
		switch {
		case err == nil:
			if err := json.Unmarshal(b, &w.st); err != nil {
				// Never start over silently: that would accept a rewritten log.
				w.loadErr = fmt.Errorf("%s is not valid: %w", statePath, err)
				w.st.Problem = "the Manager's audit anchors cannot be read: " + err.Error()
			}
		case !errors.Is(err, os.ErrNotExist):
			w.loadErr = err
			w.st.Problem = "the Manager's audit anchors cannot be read: " + err.Error()
		}
	}
	return w
}

// start runs the anchor loop until the returned stop.
func (w *auditWatch) start() (stop func()) {
	w.stop, w.exited = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(w.exited)
		w.tick()
		t := time.NewTicker(auditAnchorEvery)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				w.tick()
			}
		}
	}()
	return func() {
		w.once.Do(func() { close(w.stop) })
		<-w.exited
	}
}

func (w *auditWatch) healthPath() string { return w.dir + ".health" }

// tick follows the log from the last anchor and records the new head. It
// also judges every retention record it reads (judgeLocked) and checks the
// log's start (checkStart).
func (w *auditWatch) tick() {
	if w.statePath == "" || w.loadErr != nil {
		return
	}
	w.mu.Lock()
	var last *auditlog.Anchor
	if n := len(w.st.Anchors); n > 0 {
		a := w.st.Anchors[n-1]
		last = &a
	}
	w.mu.Unlock()
	if _, err := os.Stat(w.dir); err != nil {
		if last == nil {
			return // logging is off (no person yet) or not configured
		}
		// Once the Manager has anchored the log, a missing directory is
		// a removed log, not "logging off".
		now := time.Now().UnixMilli()
		w.mu.Lock()
		w.st.CheckedAt = now
		w.problemLocked(now, fmt.Sprintf("the audit directory is gone (%v), but the Manager recorded the log's head at seq %d on %s: the log was removed",
			err, last.Seq, time.UnixMilli(last.TS).UTC().Format(time.RFC3339)))
		w.saveLocked()
		w.mu.Unlock()
		return
	}
	h, herr := auditlog.ReadHealth(w.healthPath())
	var head auditlog.Anchor
	var rep auditlog.Report
	var err error
	for try := 0; try < 2; try++ {
		if last == nil {
			// The first anchor: one full check.
			rep, head, err = auditlog.VerifyAnchors(w.dir, nil)
			if h.Durable != 0 && head.Seq > h.Durable {
				head = auditlog.Anchor{} // not provably on disk yet: next time
			}
		} else {
			head, rep, err = auditlog.Follow(w.dir, *last, h.Durable)
		}
		if !errors.Is(err, os.ErrNotExist) {
			break
		}
		// Retention removed a segment while it was read: read again.
	}
	now := time.Now().UnixMilli()
	w.mu.Lock()
	w.st.CheckedAt = now
	switch {
	case err != nil:
		// A log the Manager cannot read cannot be anchored: the anchors
		// would freeze while the page says the chain is fine.
		w.readFailedLocked(err, false)
		w.saveLocked()
		w.mu.Unlock()
		return
	case !rep.OK:
		w.problemLocked(now, rep.Problem)
	case head.Seq != 0 && (last == nil || head.Seq > last.Seq):
		head.TS = now
		w.st.Anchors = append(w.st.Anchors, head)
		if len(w.st.Anchors) > maxAnchors {
			w.st.Anchors = append([]auditlog.Anchor(nil), w.st.Anchors[len(w.st.Anchors)-maxAnchors:]...)
		}
	}
	w.readOKLocked(false)
	// The retention policy is in the health file: without it the
	// retention checks wait for the next tick (a missed record is read
	// again by checkStart's full pass).
	if herr == nil {
		w.judgeLocked(rep.Retentions, h.Policy(), now)
	}
	w.saveLocked()
	w.mu.Unlock()
	if herr == nil {
		w.checkStart(h.Policy(), now)
	}
}

// auditReadFailures: consecutive failed reads of the log before the status
// reports it (one can be a race with the writer's retention); a successful
// read clears it.
const auditReadFailures = 2

// readFailedLocked counts a failed read of the log: the minute check's
// (full false) or the start check's whole-log read (full true).
func (w *auditWatch) readFailedLocked(err error, full bool) {
	n := &w.readFails
	if full {
		n = &w.fullFails
	}
	*n++
	log.Printf("manager: audit anchors: %v", err)
	if *n >= auditReadFailures {
		w.st.ReadProblem = fmt.Sprintf("the Manager cannot read the audit log (%d checks in a row): %v", *n, err)
	}
}

// readOKLocked records a successful read of either kind; the read problem
// goes once neither kind is failing.
func (w *auditWatch) readOKLocked(full bool) {
	if full {
		w.fullFails = 0
	} else {
		w.readFails = 0
	}
	if w.readFails < auditReadFailures && w.fullFails < auditReadFailures {
		w.st.ReadProblem = ""
	}
}

// problemLocked records the first problem (kept until a full check).
func (w *auditWatch) problemLocked(now int64, p string) {
	if w.st.Problem == "" {
		w.st.Problem, w.st.ProblemAt = p, now
		log.Printf("manager: WARNING: the audit log does not match the Manager's anchors: %s", p)
	}
}

// judgeLocked records the retention records not seen before, each judged
// against the policy (auditlog.CheckRetention): one that does not fit is a
// problem, and one that leaves less than auditMinHistory of history a
// warning.
func (w *auditWatch) judgeLocked(rs []auditlog.Retention, p auditlog.Policy, now int64) {
	seen := map[uint64]bool{}
	for _, r := range w.st.Retentions {
		seen[r.Seq] = true
	}
	for _, r := range rs {
		if seen[r.Seq] {
			continue
		}
		seen[r.Seq] = true
		r.SeenAt = now
		r.Problem = auditlog.CheckRetention(w.dir, r, w.st.Anchors, p, time.UnixMilli(now))
		w.st.Retentions = append(w.st.Retentions, r)
		if len(w.st.Retentions) > maxAnchors {
			w.st.Retentions = append([]auditlog.Retention(nil), w.st.Retentions[len(w.st.Retentions)-maxAnchors:]...)
		}
		if r.Problem != "" {
			w.problemLocked(now, r.Problem)
			continue
		}
		min := auditMinHistory
		if p.RetainAge > 0 && p.RetainAge < min {
			min = p.RetainAge
		}
		if !strings.Contains(r.Detail, "size limit") {
			continue // the age limit removes only what is older than itself
		}
		if st, ok, _ := auditlog.FirstRecord(w.dir); ok && st.Seq == r.StartSeq && st.TS > 0 {
			if kept := time.UnixMilli(now).Sub(time.UnixMilli(st.TS)); kept < min && w.st.Warning == "" {
				w.st.Warning, w.st.WarningAt = fmt.Sprintf("retention by the size limit left only %s of history (%s; the log now starts at %s). "+
					"A Mac sending refused writes in a loop, or a size limit too small for this volume (JM_AUDIT_RETAIN_MB), does this",
					max(kept.Round(time.Second), time.Second), r.Detail, time.UnixMilli(st.TS).UTC().Format(time.RFC3339)), now
				log.Printf("manager: WARNING: audit log: %s", w.st.Warning)
			}
		}
	}
}

// checkStart checks the log's first record: a removed start must be
// named by a retention record the Manager read, and every anchor older
// than the start must have been removed by one that fits the policy. When
// that fails it reads the whole log once for retention records it missed
// (it started after a retention, or a health file was unreadable). That
// read can take a minute on a large log, so it runs without w.mu (status
// and the People page do not wait for it) and merges what it found.
func (w *auditWatch) checkStart(p auditlog.Policy, now int64) {
	st, ok, err := auditlog.FirstRecord(w.dir)
	if err != nil || !ok {
		return
	}
	w.mu.Lock()
	issue := w.startIssueLocked(st)
	anchors := append([]auditlog.Anchor(nil), w.st.Anchors...)
	w.mu.Unlock()
	if issue == "" {
		return
	}
	rep, _, err := auditFullRead(w.dir, anchors)
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.saveLocked()
	if err != nil {
		// Unread is not removed: report the read, not a cut.
		w.readFailedLocked(err, true)
		return
	}
	w.readOKLocked(true)
	w.judgeLocked(rep.Retentions, p, now)
	if msg := w.startIssueLocked(st); msg != "" {
		w.problemLocked(now, msg)
	}
}

// startIssueLocked: why the log's start st is not accounted for ("" = it
// is, or a problem is already recorded).
func (w *auditWatch) startIssueLocked(st auditlog.Start) string {
	if w.st.Problem != "" {
		return ""
	}
	named := auditlog.ZeroPrev(st.Prev)
	for _, r := range w.st.Retentions {
		named = named || (r.StartSeq == st.Seq && r.StartPrev == st.Prev)
	}
	if !named {
		return fmt.Sprintf("the log starts at seq %d and no retention record the Manager read names that start: older records were removed", st.Seq)
	}
	if unc := auditlog.Uncovered(w.st.Anchors, st.Seq, w.st.Retentions); len(unc) > 0 {
		a := unc[len(unc)-1]
		return fmt.Sprintf("the log starts at seq %d, but the Manager recorded seq %d on %s and no retention record that fits the retention policy removed it: the log was cut",
			st.Seq, a.Seq, time.UnixMilli(a.TS).UTC().Format(time.RFC3339))
	}
	return ""
}

func (w *auditWatch) saveLocked() {
	b, err := json.MarshalIndent(w.st, "", "  ")
	if err == nil {
		err = writeFileAtomic0600(w.statePath, append(b, '\n'))
	}
	if err != nil {
		log.Printf("manager: audit anchors not saved: %v", err)
	}
}

// fullVerify checks the whole chain against every anchor; a pass clears a
// recorded problem.
func (w *auditWatch) fullVerify() (auditlog.Report, error) {
	w.fullMu.Lock()
	defer w.fullMu.Unlock()
	w.mu.Lock()
	anchors := append([]auditlog.Anchor(nil), w.st.Anchors...)
	w.mu.Unlock()
	rep, _, err := auditlog.VerifyAnchors(w.dir, anchors)
	if err != nil {
		return rep, err
	}
	h, herr := auditlog.ReadHealth(w.healthPath())
	w.mu.Lock()
	defer w.mu.Unlock()
	w.readOKLocked(false)
	w.readOKLocked(true)
	now := time.Now().UnixMilli()
	w.st.CheckedAt = now
	if herr == nil && w.loadErr == nil {
		w.judgeLocked(rep.Retentions, h.Policy(), now)
	}
	if rep.OK {
		// Anchors older than the start: removed by a retention that fits
		// the policy, or the log was cut.
		for _, r := range w.st.Retentions {
			if r.StartSeq == rep.StartSeq && r.Problem != "" {
				rep.OK, rep.Problem = false, r.Problem
			}
		}
		if unc := auditlog.Uncovered(anchors, rep.StartSeq, w.st.Retentions); rep.OK && len(unc) > 0 {
			a := unc[len(unc)-1]
			rep.OK, rep.BadSeq = false, a.Seq
			rep.Problem = fmt.Sprintf("the log starts at seq %d, but the Manager recorded seq %d on %s and no retention record that fits the retention policy removed it: the log was cut",
				rep.StartSeq, a.Seq, time.UnixMilli(a.TS).UTC().Format(time.RFC3339))
			if herr != nil {
				rep.Problem += " (the health file, which states the policy, cannot be read: " + herr.Error() + ")"
			}
		}
	}
	if rep.OK && w.loadErr == nil {
		w.st.Problem, w.st.ProblemAt = "", 0
		w.st.Warning, w.st.WarningAt = "", 0
	} else if !rep.OK && w.st.Problem == "" {
		w.st.Problem, w.st.ProblemAt = rep.Problem, w.st.CheckedAt
	}
	if w.statePath != "" && w.loadErr == nil {
		w.saveLocked()
	}
	return rep, nil
}

// status reads the health file and the anchor state. grantsInForce: Mac
// writes depend on the log.
func (w *auditWatch) status(grantsInForce bool) *AuditStatus {
	out := &AuditStatus{Configured: true}
	h, herr := auditlog.ReadHealth(w.healthPath())
	switch {
	case herr == nil:
		age := time.Since(time.UnixMilli(h.TS))
		out.Enabled, out.Healthy = h.Active, h.Healthy
		out.HealthAgeMs = age.Milliseconds()
		out.Error, out.LastError, out.LastFsyncMs = h.Error, h.LastError, h.LastFsync
		out.Bytes, out.Segments, out.Removed, out.Lost = h.Bytes, h.Segments, h.Removed, h.Lost
		out.Queue, out.QueueLen, out.RetainBytes, out.RetainAgeS = h.Queue, h.QueueLen, h.RetainBytes, h.RetainAgeSec
		if age > auditHealthMaxAge {
			out.Stale, out.Healthy = true, false
		}
	case errors.Is(herr, os.ErrNotExist):
		out.Error = "no health file: the proxy has not started the audit log (no person yet, an older proxy image, or JM_AUDIT=off)"
	default:
		out.Error = "health file unreadable: " + herr.Error()
	}
	probe := w.dir
	if _, err := os.Stat(probe); err != nil {
		probe = filepath.Dir(w.dir)
	}
	if fs, err := readFarmStorageSnapshot(probe); err == nil {
		out.FreeBytes = fs.AvailableBytes
	}
	w.mu.Lock()
	if n := len(w.st.Anchors); n > 0 {
		out.AnchorSeq, out.AnchorAtMs = w.st.Anchors[n-1].Seq, w.st.Anchors[n-1].TS
	}
	out.CheckedAtMs, out.ChainIssue = w.st.CheckedAt, w.st.Problem
	out.ChainOK = w.st.Problem == "" && w.st.ReadProblem == ""
	readProblem := w.st.ReadProblem
	out.RetentionWarning = w.st.Warning
	w.mu.Unlock()
	if herr == nil {
		out.Note = h.Note
	}

	why := ""
	switch {
	case !out.Enabled && herr == nil:
		// Off until the first person exists; the proxy starts it then.
	case out.Stale:
		why = fmt.Sprintf("the proxy has not refreshed the audit log's health for %s (stopped, or the disk is full)",
			(time.Duration(out.HealthAgeMs) * time.Millisecond).Round(time.Second))
	case herr != nil:
		why = out.Error
	case !out.Healthy:
		why = "the audit log cannot write: " + out.Error
		if out.Note != "" {
			why += " (" + out.Note + ")"
		}
	case out.QueueLen > 0 && out.Queue >= out.QueueLen:
		why = "the audit log's queue is full"
	}
	switch {
	case why != "" && grantsInForce:
		out.WritesRefused = out.Enabled || out.Stale
		out.Problem = "Mac writes may be refused while grants are in force: " + why
		if out.WritesRefused {
			out.Problem = "Mac writes are refused while grants are in force: " + why
		}
	case why != "" && out.Enabled:
		out.Problem = why + "; changes are not recorded"
	}
	if out.ChainIssue != "" {
		if out.Problem != "" {
			out.Problem += "; "
		}
		out.Problem += "the audit log does not match the Manager's anchors: " + out.ChainIssue
	}
	if readProblem != "" {
		if out.ChainIssue == "" {
			out.ChainIssue = readProblem
		}
		if out.Problem != "" {
			out.Problem += "; "
		}
		out.Problem += readProblem
	}
	if out.RetentionWarning != "" {
		if out.Problem != "" {
			out.Problem += "; "
		}
		out.Problem += "the audit log lost recent history: " + out.RetentionWarning
	} else if herr == nil {
		out.RetentionForecast = retentionForecast(w.dir, h, time.Now())
		if out.RetentionForecast != "" {
			if out.Problem != "" {
				out.Problem += "; "
			}
			out.Problem += out.RetentionForecast
		}
	}
	return out
}

// retentionForecast warns before the size limit cuts history short: at
// the rate the log grew since its first record, how much history the limit
// will keep ("" = at least auditMinHistory, or too little data to say).
func retentionForecast(dir string, h auditlog.Health, now time.Time) string {
	if h.RetainBytes <= 0 || h.Bytes < h.RetainBytes/4 {
		return ""
	}
	st, ok, err := auditlog.FirstRecord(dir)
	if err != nil || !ok || st.TS <= 0 {
		return ""
	}
	age := now.Sub(time.UnixMilli(st.TS))
	if age < time.Hour {
		return ""
	}
	keep := time.Duration(float64(age) * float64(h.RetainBytes) / float64(h.Bytes))
	if keep >= auditMinHistory {
		return ""
	}
	return fmt.Sprintf("at the rate the audit log grows (%d MB in %s), its size limit of %d MB keeps only about %s of history; "+
		"raise JM_AUDIT_RETAIN_MB, or look for a Mac writing in a loop", h.Bytes>>20, age.Round(time.Minute), h.RetainBytes>>20,
		keep.Round(time.Hour))
}
