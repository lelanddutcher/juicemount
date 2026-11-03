package auditlog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// maxLine bounds one record when reading (a unit with very many entries).
const maxLine = 16 << 20

// eachLine calls fn with every complete line of path (without the newline)
// and its 1-based line number. A torn last line is ignored.
func eachLine(path string, fn func(n int, line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for n := 1; ; n++ {
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
				return fmt.Errorf("%s line %d: longer than %d bytes", filepath.Base(path), n, maxLine)
			}
			return err
		}
		if err := fn(n, line[:len(line)-1]); err != nil {
			return err
		}
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

// Query returns up to limit records matching f with seq < before (0 = from
// the newest), newest first. Segments are read newest first and the scan
// stops once the page is full; a filter that matches little reads the
// whole log.
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
	for i := len(segs) - 1; i >= 0 && len(out) <= limit; i-- {
		if before != 0 {
			if first, ok := segFirst(segs[i]); ok && first >= before {
				continue
			}
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
		if err != nil {
			return Page{}, err
		}
		for j := len(ring) - 1; j >= 0; j-- {
			out = append(out, ring[(start+j)%len(ring)])
		}
	}
	if len(out) > limit {
		out, more = out[:limit], true
	}
	p := Page{Records: out}
	if more && len(out) > 0 {
		p.Next = out[len(out)-1].Seq
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
	// Problem names the first break: file, line and what is wrong.
	Problem string `json:"problem,omitempty"`
	BadSeq  uint64 `json:"bad_seq,omitempty"`
}

// Verify checks the whole chain: every line parses, names the previous
// line's hash, and has a larger seq than the line before.
func Verify(dir string) (Report, error) {
	var rep Report
	segs, err := segments(dir)
	if err != nil {
		return rep, err
	}
	rep.Segments = len(segs)
	prev := zeroHash
	var lastSeq uint64
	for _, s := range segs {
		err := eachLine(s, func(n int, line []byte) error {
			var r Record
			if err := json.Unmarshal(line, &r); err != nil {
				rep.Problem = fmt.Sprintf("%s line %d: not a record: %v", filepath.Base(s), n, err)
				return errStop
			}
			if r.Prev != prev {
				rep.Problem = fmt.Sprintf("%s line %d (seq %d): chain broken: prev %.12s, previous line hashes to %.12s",
					filepath.Base(s), n, r.Seq, r.Prev, prev)
				rep.BadSeq = r.Seq
				return errStop
			}
			if r.Seq <= lastSeq {
				rep.Problem = fmt.Sprintf("%s line %d: seq %d after %d", filepath.Base(s), n, r.Seq, lastSeq)
				rep.BadSeq = r.Seq
				return errStop
			}
			sum := sha256.Sum256(line)
			prev = hex.EncodeToString(sum[:])
			lastSeq = r.Seq
			rep.Records++
			return nil
		})
		if errors.Is(err, errStop) {
			return rep, nil
		}
		if err != nil {
			return rep, err
		}
	}
	rep.OK, rep.LastSeq = true, lastSeq
	if rep.Records > 0 {
		rep.Head = prev
	}
	return rep, nil
}

var errStop = errors.New("stop")
