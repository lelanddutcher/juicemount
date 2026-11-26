package main

import (
	"sort"
	"time"
)

// The Mac's page cache, modelled for playedit (finding R1, laptop runs
// 2026-11-14 and 2026-11-16).
//
// The macOS NFS client keeps a file's pages under the attributes they came
// with. A READ reply (or GETATTR) that carries another mtime makes it drop
// the file's pages, including pages it fetched a moment earlier for the same
// player read, and send those READs again. While a teammate's save moves the
// mtime every 2-4 s and one READ round abroad takes 2-6 s, one player read
// can need 3-4 rounds (laptop: 12-17 s player reads, ~20 % repeat READs).
// The lab's NFS client has no page cache, so without this model the lab sees
// only the wire READ times.
//
// The model: one player read needs macPages consecutive 1 MiB pages that all
// carry one mtime. The pages are read in order. A reply with an mtime other
// than the one the client knows makes that the known mtime, and every page
// of the player read held under another mtime must be read again (a repeat).
// The player read ends when all its pages hold the known mtime (at most
// macRoundsMax rounds). This is the mechanism only: the Mac's real readahead
// and its GETATTR revalidation differ, so the laptop keeps the final word.

const macRoundsMax = 8

type macPlay struct {
	At      time.Time
	Off     uint64
	Took    time.Duration
	Reads   int // wire READs, repeats included
	Repeats int // READs of a page this player read had already fetched
	Rounds  int
	Err     string
}

// macPlayRead runs one modelled player read at off. known is the mtime the
// client knows (0: none yet); record receives every wire READ.
func (p *probe) macPlayRead(off uint64, size uint64, known *int64, record func(playRead)) (macPlay, uint64) {
	n := p.macPages
	if rest := (size - off + playReadLen - 1) / playReadLen; uint64(n) > rest {
		n = int(rest)
	}
	mp := macPlay{At: time.Now(), Off: off}
	held := make([]int64, n)   // the mtime each page is held under
	have := make([]bool, n)    // the page is held
	fetched := make([]bool, n) // the page was read before in this player read
	var advanced uint64
	for mp.Rounds = 1; ; mp.Rounds++ {
		for k := 0; k < n; k++ {
			if have[k] && held[k] == *known {
				continue
			}
			o := off + uint64(k)*playReadLen
			s := time.Now()
			m, b, jb, err := p.readPairMac(o, playReadLen)
			r := playRead{At: s, Off: o, Took: time.Since(s), Jukebox: jb, Mtime: m}
			if err != nil {
				r.Err = err.Error()
			} else if o%videoTagStride == 0 && len(b) >= tagLen {
				r.Tag = string(b[:tagLen])
			}
			record(r)
			mp.Reads++
			if fetched[k] {
				mp.Repeats++
			}
			fetched[k] = true
			if err != nil {
				mp.Err = err.Error()
				mp.Took = time.Since(mp.At)
				return mp, advanced
			}
			if mt := mtimeNS(m); mt != *known {
				*known = mt // the other pages under the old mtime are dropped
			}
			held[k], have[k] = *known, true
			if end := uint64(k)*playReadLen + uint64(len(b)); end > advanced {
				advanced = end
			}
		}
		all := true
		for k := 0; k < n; k++ {
			if !have[k] || held[k] != *known {
				all = false
			}
		}
		if all || mp.Rounds >= macRoundsMax {
			break
		}
	}
	mp.Took = time.Since(mp.At)
	return mp, advanced
}

// macSummary adds the modelled player's numbers to row.
func macSummary(row map[string]any, plays []macPlay, t0 time.Time) {
	if len(plays) == 0 {
		return
	}
	var took []time.Duration
	reads, repeats, over10, capped := 0, 0, 0, 0
	for _, m := range plays {
		took = append(took, m.Took)
		reads += m.Reads
		repeats += m.Repeats
		if m.Took > 10*time.Second {
			over10++
		}
		if m.Rounds >= macRoundsMax {
			capped++
		}
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	worst := append([]macPlay(nil), plays...)
	sort.Slice(worst, func(i, j int) bool { return worst[i].Took > worst[j].Took })
	var ex []map[string]any
	for _, m := range worst[:min(5, len(worst))] {
		ex = append(ex, map[string]any{"at_s": secs(m.At.Sub(t0)), "off_mib": m.Off / mib, "took_s": secs(m.Took),
			"reads": m.Reads, "repeats": m.Repeats, "rounds": m.Rounds})
	}
	row["mac_player_reads"] = len(plays)
	row["mac_player_max_s"] = secs(took[len(took)-1])
	row["mac_player_p95_s"] = secs(took[len(took)*95/100])
	row["mac_player_over10"] = over10
	row["mac_wire_reads"], row["mac_repeat_reads"] = reads, repeats
	row["mac_repeat_share"] = float64(repeats) / float64(max(reads, 1))
	row["mac_rounds_capped"] = capped
	row["mac_worst"] = ex
}
