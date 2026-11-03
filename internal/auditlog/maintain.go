package auditlog

// maintain.go: retention and the health file, both run by the writer
// goroutine.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Health is the writer's state as the Manager sees it (Options.HealthPath).
type Health struct {
	TS        int64  `json:"ts_ms"`
	Active    bool   `json:"active"`  // false: Lazy and not activated (logging off)
	Healthy   bool   `json:"healthy"` // records are accepted
	Error     string `json:"error,omitempty"`
	LastError string `json:"last_error,omitempty"` // the last write failure, even if recovered
	LastFsync int64  `json:"last_fsync_ms,omitempty"`
	Durable   uint64 `json:"durable_seq,omitempty"` // highest seq on disk
	NextSeq   uint64 `json:"next_seq,omitempty"`
	Queue     int    `json:"queue"`
	QueueLen  int    `json:"queue_len"`
	Dropped   uint64 `json:"dropped,omitempty"` // lost since the last gap record
	Lost      uint64 `json:"lost,omitempty"`    // lost since the writer started
	Bytes     int64  `json:"bytes"`             // all segments
	Segments  int    `json:"segments"`
	Removed   int    `json:"removed_segments,omitempty"` // by retention, since the writer started
	// Note: why a failed writer stays failed (what retention could do).
	Note string `json:"note,omitempty"`
	// Retention limits (0 = none) and the segment size.
	RetainBytes  int64 `json:"retain_bytes,omitempty"`
	RetainAgeSec int64 `json:"retain_age_s,omitempty"`
	SegmentBytes int64 `json:"segment_bytes,omitempty"`
}

// Policy is the retention policy a health file states.
func (h Health) Policy() Policy {
	return Policy{RetainBytes: h.RetainBytes, RetainAge: time.Duration(h.RetainAgeSec) * time.Second, SegmentBytes: h.SegmentBytes}
}

// ReadHealth reads a health file.
func ReadHealth(path string) (Health, error) {
	var h Health
	b, err := os.ReadFile(path)
	if err != nil {
		return h, err
	}
	return h, json.Unmarshal(b, &h)
}

// writeHealth refreshes the health file; unless force, only when the state
// changed. A failed write is retried at the next call (a full disk usually
// fails this write too; the Manager then sees a stale file and says so).
func (w *Writer) writeHealth(force bool) {
	if w.opts.HealthPath == "" {
		return
	}
	w.mu.Lock()
	h := Health{Active: w.active, Healthy: w.failed == nil && !w.closed && w.active, LastError: w.lastErr,
		LastFsync: w.lastFsync, Durable: w.durable, NextSeq: w.nextSeq, Queue: len(w.queue), QueueLen: w.opts.QueueLen,
		Dropped: w.dropped, Lost: w.lost, Removed: w.removed, Note: w.note, SegmentBytes: w.opts.SegmentBytes}
	if w.failed != nil {
		h.Error = w.failed.Error()
	}
	if w.closed {
		h.Error = "closed"
	}
	w.mu.Unlock()
	if w.opts.RetainBytes > 0 {
		h.RetainBytes = w.opts.RetainBytes
	}
	if w.opts.RetainAge > 0 {
		h.RetainAgeSec = int64(w.opts.RetainAge / time.Second)
	}
	cmp := h
	cmp.LastFsync, cmp.Durable, cmp.NextSeq, cmp.Queue = 0, 0, 0, 0
	if !force && cmp == w.lastHealth {
		return
	}
	w.lastHealth = cmp
	if force && h.Active {
		// Sizes only on the periodic refresh: a directory listing per
		// batch would cost more than the batch.
		w.segCount, w.segBytes = 0, 0
		if segs, err := segments(w.dir); err == nil {
			w.segCount = len(segs)
			for _, sg := range segs {
				if st, err := os.Stat(sg); err == nil {
					w.segBytes += st.Size()
				}
			}
		}
	}
	h.Segments, h.Bytes = w.segCount, w.segBytes
	h.TS = w.opts.Now().UnixMilli()
	b, _ := json.Marshal(h)
	tmp := w.opts.HealthPath + ".tmp"
	err := os.WriteFile(tmp, append(b, '\n'), 0o600)
	if err == nil {
		err = os.Rename(tmp, w.opts.HealthPath)
	}
	if err != nil {
		w.lastHealth = Health{} // try again next time
	}
}

// retentionPlan is what the policy removes: the oldest k segments.
type retentionPlan struct {
	segs     []string
	k        int
	freed    int64
	start    segStart // the first record kept
	why      string
	total    int64
	segments int
}

type segStart struct {
	seq  uint64
	prev string
}

func (p *retentionPlan) detail() string {
	return fmt.Sprintf("removing %d segment(s), %d bytes, records before seq %d (%s)", p.k, p.freed, p.start.seq, p.why)
}

func (p *retentionPlan) record(seq uint64, now time.Time) *Record {
	return &Record{Seq: seq, TS: now.UnixMilli(), Type: TypeRetention, StartSeq: p.start.seq, StartPrev: p.start.prev,
		Detail: p.detail()}
}

// planRetention returns what the size and age limits remove now (nil:
// nothing). The segment being written is never removed.
func (w *Writer) planRetention() *retentionPlan {
	if w.f == nil {
		return nil
	}
	segs, err := segments(w.dir)
	if err != nil || len(segs) < 2 {
		return nil
	}
	sizes := make([]int64, len(segs))
	mods := make([]time.Time, len(segs))
	var total int64
	for i, sg := range segs {
		st, err := os.Stat(sg)
		if err != nil {
			return nil
		}
		sizes[i], mods[i] = st.Size(), st.ModTime()
		total += st.Size()
	}
	now := w.opts.Now()
	p := &retentionPlan{segs: segs, total: total, segments: len(segs)}
	for p.k < len(segs)-1 { // never the segment being written
		over := w.opts.RetainBytes > 0 && total-p.freed > w.opts.RetainBytes
		old := w.opts.RetainAge > 0 && now.Sub(mods[p.k]) > w.opts.RetainAge
		if !over && !old {
			break
		}
		if over {
			p.why = fmt.Sprintf("size limit %d bytes", w.opts.RetainBytes)
		} else if p.why == "" {
			p.why = "age limit " + w.opts.RetainAge.String()
		}
		p.freed += sizes[p.k]
		p.k++
	}
	if p.k == 0 {
		return nil
	}
	var ok bool
	if p.start.seq, p.start.prev, ok = firstRecord(segs[p.k]); !ok {
		w.opts.Logf("auditlog: retention: cannot read the first record of %s; keeping every segment", filepath.Base(segs[p.k]))
		return nil
	}
	return p
}

// removeSegments removes the plan's segments, oldest first, and returns
// the first record of the log afterwards (false: nothing was removed).
func (w *Writer) removeSegments(p *retentionPlan) (segStart, bool) {
	removed := 0
	for _, sg := range p.segs[:p.k] {
		if err := os.Remove(sg); err != nil {
			w.opts.Logf("auditlog: retention: %v", err)
			break
		}
		removed++
	}
	_ = syncDir(w.dir)
	w.mu.Lock()
	w.removed += removed
	w.mu.Unlock()
	start := p.start
	if removed < p.k {
		start.seq, start.prev, _ = firstRecord(p.segs[removed])
	}
	if removed > 0 {
		w.opts.Logf("auditlog: retention removed %d segment(s) (%s); the log now starts at seq %d", removed, p.why, start.seq)
	}
	return start, removed > 0
}

// stuckNote says why a failed writer could not recover.
func (w *Writer) stuckNote(err error) string {
	segs, _ := segments(w.dir)
	var total int64
	for _, sg := range segs {
		if st, err := os.Stat(sg); err == nil {
			total += st.Size()
		}
	}
	limit := "no size limit"
	if w.opts.RetainBytes > 0 {
		limit = fmt.Sprintf("its size limit is %d bytes", w.opts.RetainBytes)
	}
	return fmt.Sprintf("still cannot write (%v); the log is %d bytes in %d segment(s) and %s, so retention has nothing more to remove: free disk space or lower the limit",
		err, total, len(segs), limit)
}

// retain removes the oldest closed segments past the size or age limit.
// Before removing anything it appends (with the records still queued) a
// retention record naming the first record kept and that record's prev, so
// the retained log verifies from its new start.
func (w *Writer) retain() {
	p := w.planRetention()
	if p == nil {
		return
	}
	w.mu.Lock()
	batch := w.queue
	w.queue = nil
	rec := p.record(w.nextSeq, w.opts.Now())
	w.nextSeq++
	w.mu.Unlock()
	all := append(batch, rec)
	if err := w.writeBatch(all); err != nil {
		w.fail(err, all)
		return
	}
	w.mu.Lock()
	w.durable = rec.Seq
	w.lastFsync = w.opts.Now().UnixMilli()
	for _, r := range all {
		r.st = stDurable
	}
	w.cond.Broadcast()
	w.mu.Unlock()
	w.removeSegments(p)
}

// firstRecord returns the seq and prev of the first line of a segment.
func firstRecord(path string) (seq uint64, prev string, ok bool) {
	err := eachLine(path, func(_ int, line []byte) error {
		var r Record
		if json.Unmarshal(line, &r) == nil {
			seq, prev, ok = r.Seq, r.Prev, true
		}
		return errStop
	})
	if err != nil && err != errStop {
		return 0, "", false
	}
	return seq, prev, ok
}
