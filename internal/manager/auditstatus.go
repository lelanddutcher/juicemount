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
// /api/audit/verify?full=1) passes. The limits: records written after the
// last anchor (up to a minute) can be cut unnoticed, and whoever can write
// both the audit directory and the Manager's state directory can rewrite
// both. If the log is deleted on purpose, delete audit-anchors.json too.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
)

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
}

// auditWatch anchors the log and reports its state.
type auditWatch struct {
	dir, statePath string

	mu      sync.Mutex
	st      anchorState
	loadErr error
	fullMu  sync.Mutex // one full verify at a time

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

// tick follows the log from the last anchor and records the new head.
func (w *auditWatch) tick() {
	if w.statePath == "" || w.loadErr != nil {
		return
	}
	if _, err := os.Stat(w.dir); err != nil {
		return // logging is off (no person yet) or not configured
	}
	h, _ := auditlog.ReadHealth(w.healthPath())
	w.mu.Lock()
	var last *auditlog.Anchor
	if n := len(w.st.Anchors); n > 0 {
		a := w.st.Anchors[n-1]
		last = &a
	}
	w.mu.Unlock()
	var head auditlog.Anchor
	var rep auditlog.Report
	var err error
	if last == nil {
		// The first anchor: one full check.
		rep, head, err = auditlog.VerifyAnchors(w.dir, nil)
		if h.Durable != 0 && head.Seq > h.Durable {
			head = auditlog.Anchor{} // not provably on disk yet: next time
		}
	} else {
		head, rep, err = auditlog.Follow(w.dir, *last, h.Durable)
	}
	now := time.Now().UnixMilli()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.st.CheckedAt = now
	switch {
	case err != nil:
		log.Printf("manager: audit anchors: %v", err)
		return
	case !rep.OK:
		if w.st.Problem == "" {
			w.st.Problem, w.st.ProblemAt = rep.Problem, now
			log.Printf("manager: WARNING: the audit log does not match the Manager's anchors: %s", rep.Problem)
		}
	case head.Seq != 0 && (last == nil || head.Seq > last.Seq):
		head.TS = now
		w.st.Anchors = append(w.st.Anchors, head)
		if len(w.st.Anchors) > maxAnchors {
			w.st.Anchors = append([]auditlog.Anchor(nil), w.st.Anchors[len(w.st.Anchors)-maxAnchors:]...)
		}
	}
	w.saveLocked()
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
	w.mu.Lock()
	defer w.mu.Unlock()
	w.st.CheckedAt = time.Now().UnixMilli()
	if rep.OK && w.loadErr == nil {
		w.st.Problem, w.st.ProblemAt = "", 0
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
		out.Error = "no health file: the proxy has not started the audit log (an older proxy image, or JM_AUDIT=off)"
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
	out.ChainOK = w.st.Problem == ""
	w.mu.Unlock()

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
	if !out.ChainOK {
		if out.Problem != "" {
			out.Problem += "; "
		}
		out.Problem += "the audit log does not match the Manager's anchors: " + out.ChainIssue
	}
	return out
}
