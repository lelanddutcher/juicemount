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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxLine bounds one record when reading (a unit with very many entries).
const maxLine = 16 << 20

// eachLine calls fn with every complete line of path (without the newline)
// and its 1-based line number. A torn last line is ignored.
func eachLine(path string, fn func(n int, line []byte) error) error {
	n := 0
	return scanLines(path, 0, func(_ int64, line []byte) error {
		n++
		return fn(n, line)
	})
}

// scanLines calls fn with every complete line of path that starts at or
// after offset start (which must be a line start), with its offset. A torn
// last line is ignored.
func scanLines(path string, start int64, fn func(off int64, line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if start > 0 {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return err
		}
	}
	r := bufio.NewReaderSize(f, 1<<20)
	off := start
	for {
		line, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			// A long line: collect it.
			buf := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) && len(buf) <= maxLine {
				line, err = r.ReadSlice('\n')
				buf = append(buf, line...)
			}
			line = buf
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				return fmt.Errorf("%s offset %d: a line longer than %d bytes", filepath.Base(path), off, maxLine)
			}
			return err
		}
		if err := fn(off, line[:len(line)-1]); err != nil {
			return err
		}
		off += int64(len(line))
	}
}

// Filter selects records. Empty fields match everything.
type Filter struct {
	Person    string // person ID
	Principal string // device / principal name
	// Project matches a record whose project paths, or entry paths, are
	// this path or below it ("/Clients/A" matches "/Clients/A/x").
	Project string
	Since   int64 // unix ms, inclusive
	Until   int64 // unix ms, exclusive
	Types   []string
}

func underPath(p, root string) bool {
	root = strings.TrimSuffix(root, "/")
	return p == root || (root == "" && strings.HasPrefix(p, "/")) || strings.HasPrefix(p, root+"/")
}

// Match reports whether r passes the filter.
func (f Filter) Match(r *Record) bool {
	if f.Person != "" && r.Person != f.Person {
		return false
	}
	if f.Principal != "" && r.Principal != f.Principal {
		return false
	}
	if f.Since != 0 && r.TS < f.Since {
		return false
	}
	if f.Until != 0 && r.TS >= f.Until {
		return false
	}
	if len(f.Types) > 0 {
		ok := false
		for _, t := range f.Types {
			ok = ok || t == r.Type
		}
		if !ok {
			return false
		}
	}
	if f.Project != "" {
		ok := false
		for _, p := range r.Projects {
			ok = ok || underPath(p.Path, f.Project)
		}
		for _, o := range r.Ops {
			ok = ok || (o.Path != "" && underPath(o.Path, f.Project))
		}
		if !ok {
			return false
		}
	}
	return true
}

// Page is one page of Query, newest record first.
type Page struct {
	Records []Record `json:"records"`
	// Next is the "before" cursor of the next (older) page; 0 = no more.
	Next uint64 `json:"next,omitempty"`
}

// QueryScanBytes bounds how much of the log one Query reads (whole
// segments, newest first; at least one).
var QueryScanBytes int64 = 64 << 20

// Query returns up to limit records matching f with seq < before (0 = from
// the newest), newest first. It reads segments newest first and stops once
// the page is full or it has read QueryScanBytes; then Next is the cursor
// to continue from, and a page can hold fewer than limit records (even
// none) with Next set. Next is 0 once the oldest segment was read.
func Query(dir string, f Filter, before uint64, limit int) (Page, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	segs, err := segments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Page{}, nil
		}
		return Page{}, err
	}
	var out []Record // newest first
	more := false
	var scanned int64
	var cursor uint64 // everything >= cursor was read
	i := len(segs) - 1
	for ; i >= 0 && len(out) <= limit; i-- {
		first, firstOK := segFirst(segs[i])
		if before != 0 && firstOK && first >= before {
			continue
		}
		if scanned > 0 && scanned >= QueryScanBytes {
			break
		}
		if st, err := os.Stat(segs[i]); err == nil {
			scanned += st.Size()
		}
		// The newest matches of this segment, at most what the page
		// still needs plus one (to learn there is more).
		need := limit + 1 - len(out)
		ring := make([]Record, 0, need)
		start := 0
		err := eachLine(segs[i], func(_ int, line []byte) error {
			var r Record
			if json.Unmarshal(line, &r) != nil {
				return nil
			}
			if (before != 0 && r.Seq >= before) || !f.Match(&r) {
				return nil
			}
			if len(ring) < need {
				ring = append(ring, r)
			} else {
				ring[start] = r
				start = (start + 1) % need
			}
			return nil
		})
		if errors.Is(err, os.ErrNotExist) {
			continue // removed by retention meanwhile
		}
		if err != nil {
			return Page{}, err
		}
		for j := len(ring) - 1; j >= 0; j-- {
			out = append(out, ring[(start+j)%len(ring)])
		}
		if firstOK {
			cursor = first
		}
	}
	if len(out) > limit {
		out, more = out[:limit], true
	}
	p := Page{Records: out}
	switch {
	case more && len(out) > 0:
		p.Next = out[len(out)-1].Seq
	case i >= 0 && cursor > 1:
		// Stopped at the scan budget: older segments are left.
		p.Next = cursor
	}
	return p, nil
}

func segFirst(path string) (uint64, bool) {
	n := filepath.Base(path)
	v, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, segPrefix), segSuffix), 10, 64)
	return v, err == nil
}

// Report is the result of Verify.
type Report struct {
	OK       bool   `json:"ok"`
	Records  int    `json:"records"`
	Segments int    `json:"segments"`
	LastSeq  uint64 `json:"last_seq,omitempty"`
	Head     string `json:"head,omitempty"` // hash of the last line
	// StartSeq: the first record on disk; above 1 when retention removed
	// older segments (a retention record names this start).
	StartSeq uint64 `json:"start_seq,omitempty"`
	// Anchors: how many of the given anchors were checked against the log.
	// Anchors older than the retained start cannot be: whether a retention
	// that fits the policy removed them is the caller's check (Uncovered).
	Anchors int `json:"anchors_checked,omitempty"`
	// Retentions: the retention records the check read (the newest
	// maxRetentions).
	Retentions []Retention `json:"retentions,omitempty"`
	// Problem names the first break: file, line and what is wrong.
	Problem string `json:"problem,omitempty"`
	BadSeq  uint64 `json:"bad_seq,omitempty"`
}

// Anchor is a point of the chain the Manager recorded outside the log
// directory: the line with sequence Seq hashes to Hash and occupies
// [Offset, End) of Segment.
type Anchor struct {
	Seq     uint64 `json:"seq"`
	Hash    string `json:"hash"`
	Segment string `json:"segment"` // base name
	Offset  int64  `json:"offset"`
	End     int64  `json:"end"`
	TS      int64  `json:"ts_ms,omitempty"` // when it was recorded
}

// Verify checks the whole chain: every line parses, names the previous
// line's hash, and has a larger seq than the line before; a log that does
// not start at the very first record starts where a retention record says.
func Verify(dir string) (Report, error) {
	rep, _, err := VerifyAnchors(dir, nil)
	return rep, err
}

// chainCheck verifies lines one after the other.
type chainCheck struct {
	rep        *Report
	prev       string
	lastSeq    uint64
	started    bool
	startPrev  string // prev of the first line (zero hash: the very start)
	retainedOK bool   // a retention record names the start
	head       Anchor
	bySeq      map[uint64]string // anchor seq -> hash wanted
	anchorsHit int
}

func (c *chainCheck) line(seg string, n int, off int64, line []byte) error {
	var r Record
	if err := json.Unmarshal(line, &r); err != nil {
		c.rep.Problem = fmt.Sprintf("%s line %d: not a record: %v", seg, n, err)
		return errStop
	}
	if !c.started {
		c.started, c.prev, c.startPrev = true, r.Prev, r.Prev
		c.rep.StartSeq = r.Seq
	}
	if r.Prev != c.prev {
		c.rep.Problem = fmt.Sprintf("%s line %d (seq %d): chain broken: prev %.12s, previous line hashes to %.12s",
			seg, n, r.Seq, r.Prev, c.prev)
		c.rep.BadSeq = r.Seq
		return errStop
	}
	if c.lastSeq != 0 && r.Seq <= c.lastSeq {
		c.rep.Problem = fmt.Sprintf("%s line %d: seq %d after %d", seg, n, r.Seq, c.lastSeq)
		c.rep.BadSeq = r.Seq
		return errStop
	}
	sum := sha256.Sum256(line)
	h := hex.EncodeToString(sum[:])
	if want, ok := c.bySeq[r.Seq]; ok {
		if want != h {
			c.rep.Problem = fmt.Sprintf("%s line %d (seq %d): does not match the anchor the Manager recorded (hash %.12s, anchor %.12s): the log was rewritten",
				seg, n, r.Seq, h, want)
			c.rep.BadSeq = r.Seq
			return errStop
		}
		c.anchorsHit++
	}
	if r.Type == TypeRetention {
		if r.StartSeq == c.rep.StartSeq && r.StartPrev == c.startPrev {
			c.retainedOK = true
		}
		if len(c.rep.Retentions) == maxRetentions {
			c.rep.Retentions = c.rep.Retentions[1:]
		}
		c.rep.Retentions = append(c.rep.Retentions, Retention{Seq: r.Seq, TS: r.TS, StartSeq: r.StartSeq, StartPrev: r.StartPrev,
			Detail: r.Detail})
	}
	c.prev, c.lastSeq = h, r.Seq
	c.head = Anchor{Seq: r.Seq, Hash: h, Segment: seg, Offset: off, End: off + int64(len(line)) + 1}
	c.rep.Records++
	return nil
}

// VerifyAnchors is Verify, and also checks every anchor at or after the
// retained start: its line must still be there with the same hash (a
// rewrite before an anchor, or a truncation of the newest records to before
// it, is reported). It returns the head of the log (zero when empty).
//
// A retention record is not authenticated: whoever can write the directory
// can remove the oldest segments and append one that names the new start,
// and the chain verifies. Anchors older than the start are therefore not
// accepted here or rejected: the caller checks them with Uncovered against
// the retention records it read and judged (CheckRetention).
func VerifyAnchors(dir string, anchors []Anchor) (Report, Anchor, error) {
	var rep Report
	segs, err := segments(dir)
	if err != nil {
		return rep, Anchor{}, err
	}
	rep.Segments = len(segs)
	c := &chainCheck{rep: &rep, prev: zeroHash, bySeq: map[uint64]string{}}
	for _, a := range anchors {
		c.bySeq[a.Seq] = a.Hash
	}
	for _, s := range segs {
		base := filepath.Base(s)
		n := 0
		err := scanLines(s, 0, func(off int64, line []byte) error {
			n++
			return c.line(base, n, off, line)
		})
		if errors.Is(err, errStop) {
			return rep, c.head, nil
		}
		if err != nil {
			return rep, c.head, err
		}
	}
	if c.started && c.startPrev != zeroHash && !c.retainedOK {
		rep.Problem = fmt.Sprintf("the log starts at seq %d and no retention record names that start: older records were removed", rep.StartSeq)
		rep.BadSeq = rep.StartSeq
		return rep, c.head, nil
	}
	for _, a := range anchors {
		if a.Seq > c.lastSeq {
			rep.Problem = fmt.Sprintf("the log ends at seq %d, but the Manager recorded seq %d at %s: newer records were removed",
				c.lastSeq, a.Seq, time.UnixMilli(a.TS).UTC().Format(time.RFC3339))
			rep.BadSeq = a.Seq
			return rep, c.head, nil
		}
		if a.Seq >= rep.StartSeq {
			rep.Anchors++
		}
	}
	if rep.Anchors != c.anchorsHit {
		rep.Problem = fmt.Sprintf("%d anchor(s) the Manager recorded name records that are not in the log", rep.Anchors-c.anchorsHit)
		return rep, c.head, nil
	}
	rep.OK, rep.LastSeq = true, c.lastSeq
	if rep.Records > 0 {
		rep.Head = c.prev
	}
	return rep, c.head, nil
}

// Follow checks the log from anchor a to its head: a's line must still be
// in place with its hash, and every line after it must chain on. It reads
// only what was written after a (the Manager calls it every minute). It
// returns the new head, never past seq maxSeq (0 = no limit; the Manager
// passes the writer's durable seq so it never anchors a line a power loss
// could still take back). If a's segment was removed by retention, Follow
// falls back to VerifyAnchors.
func Follow(dir string, a Anchor, maxSeq uint64) (Anchor, Report, error) {
	var rep Report
	segs, err := segments(dir)
	if err != nil {
		return a, rep, err
	}
	rep.Segments = len(segs)
	at := -1
	for i, s := range segs {
		if filepath.Base(s) == a.Segment {
			at = i
		}
	}
	if at < 0 {
		r, head, err := VerifyAnchors(dir, []Anchor{a})
		if maxSeq != 0 && head.Seq > maxSeq {
			head = a
		}
		return head, r, err
	}
	if a.End <= a.Offset || a.End-a.Offset > maxLine+1 {
		return a, rep, fmt.Errorf("auditlog: anchor with offsets %d-%d", a.Offset, a.End)
	}
	// a's own line.
	f, err := os.Open(segs[at])
	if err != nil {
		return a, rep, err
	}
	buf := make([]byte, a.End-a.Offset)
	_, err = f.ReadAt(buf, a.Offset)
	f.Close()
	sum := sha256.Sum256(bytes.TrimSuffix(buf, []byte("\n")))
	if err != nil || len(buf) == 0 || buf[len(buf)-1] != '\n' || hex.EncodeToString(sum[:]) != a.Hash {
		rep.Problem = fmt.Sprintf("%s offset %d: the record the Manager anchored (seq %d, recorded %s) is gone or changed: the log was rewritten or cut",
			a.Segment, a.Offset, a.Seq, time.UnixMilli(a.TS).UTC().Format(time.RFC3339))
		rep.BadSeq = a.Seq
		return a, rep, nil
	}
	c := &chainCheck{rep: &rep, prev: a.Hash, lastSeq: a.Seq, started: true, head: a}
	head := a
	for i := at; i < len(segs); i++ {
		start := int64(0)
		if i == at {
			start = a.End
		}
		base := filepath.Base(segs[i])
		n := 0
		err := scanLines(segs[i], start, func(off int64, line []byte) error {
			n++
			if err := c.line(base, n, off, line); err != nil {
				return err
			}
			if maxSeq == 0 || c.head.Seq <= maxSeq {
				head = c.head
			}
			return nil
		})
		if errors.Is(err, errStop) {
			return a, rep, nil
		}
		if err != nil {
			return a, rep, err
		}
	}
	rep.OK, rep.LastSeq, rep.Head = true, c.lastSeq, c.prev
	return head, rep, nil
}

var errStop = errors.New("stop")

// maxRetentions bounds Report.Retentions.
const maxRetentions = 64

// Retention is a retention record as a check read it. SeenAt and Problem
// are the reader's (the Manager's): when it first read the record, and why
// the removal it announces does not fit the retention policy ("" = it fits;
// CheckRetention).
type Retention struct {
	Seq       uint64 `json:"seq"`
	TS        int64  `json:"ts_ms"`
	StartSeq  uint64 `json:"start_seq"`
	StartPrev string `json:"start_prev"`
	Detail    string `json:"detail,omitempty"`
	SeenAt    int64  `json:"seen_at_ms,omitempty"`
	Problem   string `json:"problem,omitempty"`
}

// Policy is a writer's retention policy (Health.Policy); 0 = no limit.
type Policy struct {
	RetainBytes  int64
	RetainAge    time.Duration
	SegmentBytes int64
}

// retentionSlack: how far the times CheckRetention relies on may be off
// (the health file's durable seq, which caps anchors, is up to a refresh
// interval old; clocks).
const retentionSlack = 2 * time.Minute

// CheckRetention says whether removing every record before r.StartSeq fits
// policy p, given the log in dir as it is now and anchors (when the
// Manager saw which record): "" = it fits. A removal fits the size limit if
// the segments kept are at least the limit less two segments (retention
// stops once the rest fits; a segment is the larger of the segment size and
// the largest segment kept); it fits the age limit unless an anchor shows
// that a removed record was written within the age limit (a removed anchor
// that has an older anchor before it was written after that one was
// recorded). It cannot tell the writer's removal from a cut someone made
// that the policy could also have made.
func CheckRetention(dir string, r Retention, anchors []Anchor, p Policy, now time.Time) string {
	why := []string{}
	if p.RetainBytes <= 0 && p.RetainAge <= 0 {
		why = append(why, "the log has no retention limit")
	}
	if p.RetainBytes > 0 {
		seg := p.SegmentBytes
		if seg <= 0 {
			seg = 64 << 20
		}
		var kept int64
		segs, err := segments(dir)
		if err != nil {
			return "the log cannot be listed: " + err.Error()
		}
		for _, sg := range segs {
			if first, ok := segFirst(sg); ok && first >= r.StartSeq {
				if st, err := os.Stat(sg); err == nil {
					kept += st.Size()
					seg = max(seg, st.Size()) // one batch can outgrow a segment
				}
			}
		}
		if kept+2*seg > p.RetainBytes {
			return ""
		}
		why = append(why, fmt.Sprintf("the %d bytes kept are far below the size limit of %d bytes", kept, p.RetainBytes))
	}
	if p.RetainAge > 0 {
		sorted := append([]Anchor(nil), anchors...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
		j := -1
		for i, a := range sorted {
			if a.Seq < r.StartSeq {
				j = i
			}
		}
		if j < 1 {
			return "" // no removed record with a known lower bound on its time
		}
		after := time.UnixMilli(sorted[j-1].TS)
		if now.Sub(after) > p.RetainAge-retentionSlack {
			return ""
		}
		why = append(why, fmt.Sprintf("the removed record seq %d was written after %s, within the age limit of %s",
			sorted[j].Seq, after.UTC().Format(time.RFC3339), p.RetainAge))
	}
	return fmt.Sprintf("retention record seq %d removes the records before seq %d, which the retention policy does not explain: %s",
		r.Seq, r.StartSeq, strings.Join(why, "; "))
}

// Uncovered returns the anchors older than start (the log's first record)
// that no retention record in seen removed with a removal that fits the
// policy (a seen record with StartSeq above the anchor and no Problem).
func Uncovered(anchors []Anchor, start uint64, seen []Retention) []Anchor {
	var out []Anchor
	for _, a := range anchors {
		if a.Seq >= start {
			continue
		}
		ok := false
		for _, r := range seen {
			ok = ok || (r.Problem == "" && r.StartSeq > a.Seq && r.StartSeq <= start)
		}
		if !ok {
			out = append(out, a)
		}
	}
	return out
}

// Start is the first record of the log on disk.
type Start struct {
	Seq  uint64
	Prev string
	TS   int64
}

// FirstRecord returns the first record of the log in dir (false: the log
// is empty).
func FirstRecord(dir string) (Start, bool, error) {
	segs, err := segments(dir)
	if err != nil {
		return Start{}, false, err
	}
	for _, sg := range segs {
		var st Start
		found := false
		err := eachLine(sg, func(_ int, line []byte) error {
			var r Record
			if json.Unmarshal(line, &r) == nil {
				st, found = Start{Seq: r.Seq, Prev: r.Prev, TS: r.TS}, true
			}
			return errStop
		})
		if err != nil && err != errStop && !errors.Is(err, os.ErrNotExist) {
			return Start{}, false, err
		}
		if found {
			return st, true, nil
		}
	}
	return Start{}, false, nil
}

// ZeroPrev reports whether prev is the "prev" of the very first record of
// a log.
func ZeroPrev(prev string) bool { return prev == zeroHash }
