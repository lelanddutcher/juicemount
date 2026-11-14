package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// The playedit case (bug F1, laptop run 2026-11-14): the reader plays a big
// clip while the peer saves the same clip in place again and again.
//
// The player sends 1 MiB READs from offset 0 at -rate MiB/s, as a video
// player does (no catch-up after a stall). A JUKEBOX reply is retried after
// the waits the macOS client uses (4, 8, 16, 30, 30… s), so the stalls are
// the ones the Mac would show. After -lead the peer starts saving: each save
// is one open, a 4 KiB block (a 16-byte tag + random) every 8 MiB of the
// file, one close; saves follow each other until -save has passed.
//
// P2 check: after each close the peer stats the file. A READ reply whose
// mtime is at or past the mtime after close i carries bytes at least as new
// as save i. So a reply at a tagged offset (a multiple of 8 MiB) with such an
// mtime and the tag of an earlier save (or the fixture's bytes) is a
// violation: new attributes over old bytes.
//
// The P2 poller adds a 16-byte READ of the tagged offset behind the play
// head every 0.5 s, so the check sees parts the caches may hold.
//
// Final: after the last close and -tail of more playback, the reader polls
// 16 B at 8 tagged offsets around the play head until all carry the last
// tag (final_visible_s, from the last close).

const (
	videoTagStride = 8 * mib
	playReadLen    = mib
)

var macJukeboxWaits = []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}

type playRead struct {
	At      time.Time
	Off     uint64
	Took    time.Duration
	Jukebox int
	Mtime   nfsc.NFS3Time
	Tag     string // the 16 bytes at a tagged offset, else ""
	Err     string
}

// readPairMac is readPair with the Mac's JUKEBOX waits instead of 1 s.
func (p *probe) readPairMac(off uint64, n uint32) (nfsc.NFS3Time, []byte, int, error) {
	type readArgs struct {
		rpc.Header
		FH     []byte
		Offset uint64
		Count  uint32
	}
	type readRes struct {
		Attr  nfsc.PostOpAttr
		Count uint32
		EOF   uint32
		Data  struct{ Length uint32 }
	}
	jb := 0
	for {
		r, err := p.v.Call(&readArgs{
			Header: rpc.Header{Rpcvers: 2, Prog: nfsc.Nfs3Prog, Vers: nfsc.Nfs3Vers, Proc: nfsc.NFSProc3Read,
				Cred: p.auth, Verf: rpc.AuthNull},
			FH: p.fh, Offset: off, Count: n,
		})
		if err != nil {
			return nfsc.NFS3Time{}, nil, jb, err
		}
		status, err := xdr.ReadUint32(r)
		if err != nil {
			return nfsc.NFS3Time{}, nil, jb, err
		}
		if status == 10008 && jb < 12 {
			time.Sleep(macJukeboxWaits[min(jb, len(macJukeboxWaits)-1)])
			jb++
			continue
		}
		if status != 0 {
			return nfsc.NFS3Time{}, nil, jb, fmt.Errorf("READ status %d", status)
		}
		var res readRes
		if err := xdr.Read(r, &res); err != nil {
			return nfsc.NFS3Time{}, nil, jb, err
		}
		buf := make([]byte, res.Data.Length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nfsc.NFS3Time{}, nil, jb, err
		}
		if !res.Attr.IsSet {
			return nfsc.NFS3Time{}, buf, jb, fmt.Errorf("READ reply without attributes")
		}
		return res.Attr.Attr.Mtime, buf, jb, nil
	}
}

type peerSave struct {
	Tag     string        `json:"tag"`
	Start   time.Time     `json:"-"`
	Close   time.Time     `json:"-"`
	WriteS  float64       `json:"write_s"`
	CloseS  float64       `json:"close_s"`
	Mtime   nfsc.NFS3Time `json:"-"`
	StartAt float64       `json:"start_at_s"`
	CloseAt float64       `json:"close_at_s"`
}

func mtimeNS(t nfsc.NFS3Time) int64 { return int64(t.Seconds)*1e9 + int64(t.Nseconds) }

func newTag() string {
	b := make([]byte, tagLen/2)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

// saveOnce is one peer save: a 4 KiB block (tag + random) every 8 MiB.
func (p *probe) saveOnce(size int64, tag string) (peerSave, error) {
	s := peerSave{Tag: tag, Start: time.Now()}
	f, err := os.OpenFile(filepath.Join(p.peer, p.name), os.O_WRONLY, 0)
	if err != nil {
		return s, err
	}
	blk := make([]byte, 4096)
	for off := int64(0); off < size; off += videoTagStride {
		_, _ = rand.Read(blk[tagLen:])
		copy(blk, tag)
		if _, err := f.WriteAt(blk, off); err != nil {
			f.Close()
			return s, err
		}
	}
	w := time.Now()
	if err := f.Close(); err != nil {
		return s, err
	}
	s.Close = time.Now()
	s.WriteS, s.CloseS = secs(w.Sub(s.Start)), secs(s.Close.Sub(w))
	st, err := os.Stat(filepath.Join(p.peer, p.name))
	if err != nil {
		return s, err
	}
	mt := st.ModTime()
	s.Mtime = nfsc.NFS3Time{Seconds: uint32(mt.Unix()), Nseconds: uint32(mt.Nanosecond())}
	return s, nil
}

func (p *probe) playEdit(row map[string]any, size int64, rate float64, lead, save, tail time.Duration) error {
	// The client library sends a READ again after 5 s without a reply; the
	// Mac's TCP mount waits far longer. A re-sent READ would hide the real
	// stall and load jm5 with duplicates.
	rpc.DefaultReadTimeout = 2 * time.Minute
	var (
		mu    sync.Mutex
		reads []playRead
		stop  = make(chan struct{})
		done  = make(chan struct{})
		head  uint64
	)
	t0 := time.Now()
	go func() {
		defer close(done)
		off := uint64(0)
		every := time.Duration(float64(time.Second) * float64(playReadLen) / (rate * mib))
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := time.Now()
			m, b, jb, err := p.readPairMac(off, playReadLen)
			r := playRead{At: s, Off: off, Took: time.Since(s), Jukebox: jb, Mtime: m}
			if err != nil {
				r.Err = err.Error()
			} else if off%videoTagStride == 0 && len(b) >= tagLen {
				r.Tag = string(b[:tagLen])
			}
			mu.Lock()
			reads = append(reads, r)
			mu.Unlock()
			if err == nil {
				off += uint64(len(b))
				if len(b) == 0 || off >= uint64(size) {
					off = 0
				}
				mu.Lock()
				head = off
				mu.Unlock()
			} else {
				time.Sleep(500 * time.Millisecond)
			}
			if d := every - time.Since(s); d > 0 {
				time.Sleep(d)
			}
		}
	}()
	// The P2 poller: every 0.5 s a 16-byte READ of the tagged offset just
	// behind the play head, a part the reader has read before and the
	// caches may still hold. Its replies count in the P2 check only.
	var polls []playRead
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			mu.Lock()
			h := head
			mu.Unlock()
			off := h / videoTagStride * videoTagStride
			s := time.Now()
			m, b, jb, err := p.readPairMac(off, tagLen)
			r := playRead{At: s, Off: off, Took: time.Since(s), Jukebox: jb, Mtime: m}
			if err != nil {
				r.Err = err.Error()
			} else if len(b) >= tagLen {
				r.Tag = string(b[:tagLen])
			}
			mu.Lock()
			polls = append(polls, r)
			mu.Unlock()
		}
	}()
	// The saver (-writes): a 256 KiB file through NFS every second, as the
	// Mac's own saves; jm5 drains them through juicefs's control channel,
	// which the content refresh shares (review M1 of #97).
	var writes []time.Duration
	writeErrs := 0
	writeErr := ""
	// Drain time: when each saved file shows its full size through the
	// peer's mount (the NAS), from the NFS close (F2: the drain shares
	// juicefs's control channel with the content refresh).
	closedAt := map[string]time.Time{}
	drained := map[string]time.Duration{}
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		if !p.writes {
			return
		}
		dir := path.Dir(p.name)
		stamp := time.Now().UnixNano()
		blob := make([]byte, 256<<10)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
			_, _ = rand.Read(blob)
			s := time.Now()
			var f *nfsc.File
			var err error
			for try := 0; try < 30; try++ { // JUKEBOX: wait and send again, as the Mac does
				if f, err = p.v.OpenFile(fmt.Sprintf("%s/w-%d-%04d.bin", dir, stamp, i), 0o644); err == nil ||
					!(errors.Is(err, os.ErrInvalid) || strings.Contains(err.Error(), "invalid argument")) {
					break
				}
				time.Sleep(time.Second)
			}
			if err == nil {
				_, err = f.Write(blob)
				if cerr := f.Close(); err == nil {
					err = cerr
				}
			}
			if err != nil {
				mu.Lock()
				if writeErr == "" {
					writeErr = err.Error()
				}
				mu.Unlock()
			}
			mu.Lock()
			if err != nil {
				writeErrs++
			} else {
				writes = append(writes, time.Since(s))
				closedAt[fmt.Sprintf("w-%d-%04d.bin", stamp, i)] = time.Now()
			}
			mu.Unlock()
		}
	}()
	drainDone := make(chan struct{})
	drainStop := make(chan struct{})
	go func() {
		defer close(drainDone)
		if !p.writes {
			return
		}
		dir := filepath.Join(p.peer, path.Dir(p.name))
		for {
			select {
			case <-drainStop:
				return
			case <-time.After(500 * time.Millisecond):
			}
			mu.Lock()
			var todo []string
			for n := range closedAt {
				if _, ok := drained[n]; !ok {
					todo = append(todo, n)
				}
			}
			mu.Unlock()
			for _, n := range todo {
				if st, err := os.Stat(filepath.Join(dir, n)); err == nil && st.Size() == 256<<10 {
					mu.Lock()
					drained[n] = time.Since(closedAt[n])
					mu.Unlock()
				}
			}
		}
	}()
	time.Sleep(lead)
	var saves []peerSave
	saveStart := time.Now()
	var saveErr error
	for len(saves) == 0 || time.Since(saveStart) < save {
		s, err := p.saveOnce(size, newTag())
		s.StartAt, s.CloseAt = secs(s.Start.Sub(t0)), secs(s.Close.Sub(t0))
		if err != nil {
			saveErr = err
			break
		}
		saves = append(saves, s)
	}
	time.Sleep(tail)
	close(stop)
	<-done
	<-pollDone
	<-writeDone
	if p.writes {
		// Give the drain a bounded tail, then report what reached the NAS.
		for wait := time.Now(); time.Since(wait) < 60*time.Second; time.Sleep(time.Second) {
			mu.Lock()
			all := len(drained) == len(closedAt)
			mu.Unlock()
			if all {
				break
			}
		}
		close(drainStop)
		<-drainDone
		var ds []time.Duration
		for _, d := range drained {
			ds = append(ds, d)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		row["drain_saved"], row["drain_on_nas"] = len(closedAt), len(ds)
		if n := len(ds); n > 0 {
			row["drain_p50_s"], row["drain_p90_s"], row["drain_max_s"] = secs(ds[n/2]), secs(ds[n*9/10]), secs(ds[n-1])
		}
	} else {
		close(drainStop)
		<-drainDone
	}
	if p.writes {
		sort.Slice(writes, func(i, j int) bool { return writes[i] < writes[j] })
		row["writes"], row["write_errors"], row["write_first_error"] = len(writes), writeErrs, writeErr
		if n := len(writes); n > 0 {
			row["write_p50_s"], row["write_p95_s"], row["write_max_s"] = secs(writes[n/2]), secs(writes[n*95/100]), secs(writes[n-1])
		}
	}
	if saveErr != nil {
		row["peer_error"] = saveErr.Error()
	}
	if len(saves) == 0 {
		return fmt.Errorf("no peer save: %v", saveErr)
	}
	last := saves[len(saves)-1]

	// Final bytes: 8 tagged offsets around the play head.
	mu.Lock()
	h := head
	mu.Unlock()
	base := int64(h) / videoTagStride
	var offs []uint64
	for k := int64(-4); k <= 4; k++ {
		if k == 0 {
			continue
		}
		o := (base + k) * videoTagStride
		if o < 0 || o >= size {
			continue
		}
		offs = append(offs, uint64(o))
	}
	finalAt := -1.0
	finalJukebox := 0
	var finals []playRead // every final-poll reply, for the same-mtime check
	finalRead := func(o uint64) (string, error) {
		s := time.Now()
		m, b, jb, err := p.readPairMac(o, tagLen)
		finalJukebox += jb
		r := playRead{At: s, Off: o, Took: time.Since(s), Jukebox: jb, Mtime: m}
		if err != nil {
			r.Err = err.Error()
		} else if len(b) >= tagLen {
			r.Tag = string(b[:tagLen])
		}
		finals = append(finals, r)
		return r.Tag, err
	}
	for time.Since(last.Close) < p.timeout {
		all := true
		for _, o := range offs {
			tag, err := finalRead(o)
			if err != nil || tag != last.Tag {
				all = false
				break
			}
		}
		if all {
			finalAt = secs(time.Since(last.Close))
			break
		}
		time.Sleep(p.poll)
	}

	// Settled: the attributes and bytes once every commit has long landed
	// (a design that reports another mtime while commits arrive switches
	// back by then).
	time.Sleep(p.settle)
	for _, o := range offs {
		_, _ = finalRead(o)
	}

	// The play summary.
	tagIndex := map[string]int{}
	for i, s := range saves {
		tagIndex[s.Tag] = i
	}
	var stalls []map[string]any
	var longest, total time.Duration
	jukebox, tagged, p2, errs := 0, 0, 0, 0
	var p2Examples []map[string]any
	pollJukebox := 0
	for _, r := range polls {
		pollJukebox += r.Jukebox
	}
	for i, r := range append(append([]playRead(nil), reads...), polls...) {
		poll := i >= len(reads)
		if !poll {
			jukebox += r.Jukebox
		}
		if r.Err != "" {
			errs++
		}
		if !poll && r.Took > 2*time.Second {
			stalls = append(stalls, map[string]any{"at_s": secs(r.At.Sub(t0)), "off_mib": r.Off / mib, "took_s": secs(r.Took), "jukebox": r.Jukebox})
			total += r.Took
		}
		if !poll {
			longest = max(longest, r.Took)
		}
		if r.Tag == "" || r.Err != "" {
			continue
		}
		tagged++
		// The newest save whose post-close mtime the reply's mtime reaches.
		k := -1
		for i, s := range saves {
			if mtimeNS(r.Mtime) >= mtimeNS(s.Mtime) {
				k = i
			}
		}
		got, ok := tagIndex[r.Tag]
		if !ok {
			got = -1
		}
		if got < k {
			p2++
			if len(p2Examples) < 5 {
				p2Examples = append(p2Examples, map[string]any{"at_s": secs(r.At.Sub(t0)), "off_mib": r.Off / mib, "reply_save": k, "bytes_save": got, "poll": poll})
			}
		}
	}
	// P2 as the Mac's NFS client sees it (the mtime-span finding): it keeps
	// the bytes of a reply until it sees another mtime. So two replies at one
	// offset with the same mtime and different bytes mean the Mac may keep
	// the earlier bytes for good. This counts late commits too: a reply under
	// a save's final mtime before its last part committed. Each reply whose
	// bytes differ from a later reply's at the same offset and mtime counts.
	all := append(append(append([]playRead(nil), reads...), polls...), finals...)
	sort.SliceStable(all, func(i, j int) bool { return all[i].At.Before(all[j].At) })
	type offMtime struct {
		off uint64
		mt  int64
	}
	lastTag := map[offMtime]string{}
	sameMtime := 0
	var sameExamples []map[string]any
	for i := len(all) - 1; i >= 0; i-- {
		r := all[i]
		if r.Tag == "" || r.Err != "" {
			continue
		}
		k := offMtime{r.Off, mtimeNS(r.Mtime)}
		if later, ok := lastTag[k]; ok && later != r.Tag {
			sameMtime++
			if len(sameExamples) < 5 {
				got, ok := tagIndex[r.Tag]
				if !ok {
					got = -1
				}
				sameExamples = append(sameExamples, map[string]any{"at_s": secs(r.At.Sub(t0)), "off_mib": r.Off / mib, "bytes_save": got, "later_save": tagIndex[later]})
			}
			continue
		}
		lastTag[k] = r.Tag
	}
	row["p2_same_mtime"], row["p2_same_mtime_examples"] = sameMtime, sameExamples
	// p2_kept: the bytes the Mac would keep. It keeps a reply's bytes while
	// the server keeps reporting that mtime. Every block of save k has
	// committed once save k's close returned, so a reply whose mtime the
	// server still reported after that close must carry save k's bytes or
	// newer. This counts a save's early parts read under its final mtime
	// (the mtime-span finding) without counting replies whose mtime moved on.
	lastSeen := map[int64]time.Time{}
	for _, r := range all {
		if r.Err == "" {
			if m := mtimeNS(r.Mtime); r.At.After(lastSeen[m]) {
				lastSeen[m] = r.At
			}
		}
	}
	kept := 0
	var keptExamples []map[string]any
	for _, r := range all {
		if r.Tag == "" || r.Err != "" {
			continue
		}
		seen := lastSeen[mtimeNS(r.Mtime)]
		k := -1
		for i, s := range saves {
			if s.Close.Before(seen) {
				k = i
			}
		}
		got, ok := tagIndex[r.Tag]
		if !ok {
			got = -1
		}
		if got < k {
			kept++
			if len(keptExamples) < 5 {
				keptExamples = append(keptExamples, map[string]any{"at_s": secs(r.At.Sub(t0)), "off_mib": r.Off / mib,
					"bytes_save": got, "mtime_seen_until_s": secs(seen.Sub(t0)), "newer_save": k})
			}
		}
	}
	row["p2_kept"], row["p2_kept_examples"] = kept, keptExamples
	// Every reply, for offline checks: [at_s, offset MiB, mtime ns, save
	// index of the bytes (-1 the fixture, -2 untagged)].
	var replies [][4]any
	for _, r := range all {
		if r.Err != "" {
			continue
		}
		got := -2
		if r.Tag != "" {
			if g, ok := tagIndex[r.Tag]; ok {
				got = g
			} else {
				got = -1
			}
		}
		replies = append(replies, [4]any{secs(r.At.Sub(t0)), r.Off / mib, mtimeNS(r.Mtime), got})
	}
	row["replies"] = replies
	var closes []float64
	for _, s := range saves {
		closes = append(closes, secs(s.Close.Sub(t0)))
	}
	row["save_close_s"] = closes
	if n := len(finals); n > 0 {
		row["settled_mtime_ns"] = mtimeNS(finals[n-1].Mtime)
		row["final_mtime_ns"] = mtimeNS(last.Mtime)
	}
	sort.Slice(stalls, func(i, j int) bool { return stalls[i]["took_s"].(float64) > stalls[j]["took_s"].(float64) })
	if len(stalls) > 10 {
		stalls = stalls[:10]
	}
	row["saves"] = saves
	row["reads"], row["read_errors"], row["tagged_reads"] = len(reads), errs, tagged
	row["play_jukebox"], row["final_jukebox"], row["poll_jukebox"], row["polls"] = jukebox, finalJukebox, pollJukebox, len(polls)
	row["stalls_over_2s"], row["stall_count"] = stalls, len(stallsCount(reads))
	row["longest_read_s"], row["total_stall_s"] = secs(longest), secs(total)
	row["p2_violations"], row["p2_examples"] = p2, p2Examples
	row["final_visible_s"], row["final_offsets"] = finalAt, len(offs)
	row["head_mib"] = h / mib
	return nil
}

func stallsCount(reads []playRead) []playRead {
	var out []playRead
	for _, r := range reads {
		if r.Took > 2*time.Second {
			out = append(out, r)
		}
	}
	return out
}
