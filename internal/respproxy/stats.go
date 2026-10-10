package respproxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type writeRec struct {
	cmd  Command
	args [][]byte
}

type cmdKey struct {
	Name, Shape, Ctx string
	Class            Class
	Trash            bool
}

// Anchor says how an inode-keyed write can be tied to a parent directory
// using only what the proxy sees on the wire (coverage measurement; L2's
// ancestry engine is the product version of this).
const (
	AnchorDentryInTxn   = "dentry write in same txn names this inode"
	AnchorDirOfDentry   = "inode is the dir whose d<ino> the txn writes"
	AnchorAttrValue     = "parent decoded from SET i<ino> value in same txn"
	AnchorLearned       = "parent learned from earlier traffic"
	AnchorNeedsAncestry = "NONE on the wire: needs ancestry lookup"
)

// Decision outcomes recorded per unit.
const (
	DecAllow     = "allow"
	DecDeny      = "deny"
	DecWouldDeny = "would_deny"
)

type decKey struct {
	Outcome   string
	Reason    Reason
	Principal string
	Kind      Kind
}

// DecisionLog is one refused (or would-be refused) unit.
type DecisionLog struct {
	Time      time.Time `json:"time"`
	Outcome   string    `json:"outcome"`
	Reason    Reason    `json:"reason"`
	Principal string    `json:"principal"`
	Kind      Kind      `json:"kind"`
	Remote    string    `json:"remote"`
	Conn      uint64    `json:"conn"`
	Unit      string    `json:"unit"`
	Detail    string    `json:"detail"`
}

const recentDecisions = 256

// Stats is the proxy's counters. All fields are safe for concurrent use.
type Stats struct {
	commands, replies, flushes, conns atomic.Int64
	authOK, authBad, authThrottled    atomic.Int64
	unauthRefused, discards           atomic.Int64
	synthetic, protoErrors            atomic.Int64
	connRefused, overflowClosed       atomic.Int64

	mu         sync.Mutex
	byCmd      map[cmdKey]int64
	unknownW   map[string]int64 // raw unknown write key -> count
	unknownCmd map[string]int64
	anchors    map[string]map[string]int64 // shape -> anchor -> count
	units      map[string]int64            // "txn"/"bare" write units
	newNoDent  int64
	newNoDentX []string
	seenIno    map[uint64]struct{}
	parent     map[uint64]uint64
	decisions  map[decKey]int64
	recent     []DecisionLog
	lastLog    map[string]time.Time
	live       map[*connStats]struct{}
	mode       Mode
	Logf       func(string, ...any)
	// TrackAnchors enables the per-write anchor analysis (coverage runs).
	// It keeps an inode->parent map, so it is off by default in product.
	TrackAnchors bool
}

func NewStats() *Stats {
	return &Stats{
		byCmd:      map[cmdKey]int64{},
		unknownW:   map[string]int64{},
		unknownCmd: map[string]int64{},
		anchors:    map[string]map[string]int64{},
		units:      map[string]int64{},
		seenIno:    map[uint64]struct{}{},
		parent:     map[uint64]uint64{},
		decisions:  map[decKey]int64{},
		lastLog:    map[string]time.Time{},
		live:       map[*connStats]struct{}{},
	}
}

// Commands is the op counter used by validity gates.
func (s *Stats) Commands() int64 {
	n := s.commands.Load()
	s.mu.Lock()
	for cs := range s.live {
		cs.mu.Lock()
		n += cs.n
		cs.mu.Unlock()
	}
	s.mu.Unlock()
	return n
}

// connStats holds one connection's counters. Only its own goroutines (and a
// rare Snapshot) touch its mutex, so the per-command path never contends
// on the global Stats lock (review H8B-L1 #9). Counts merge into Stats when
// the connection closes; Snapshot adds live connections in.
type connStats struct {
	mu   sync.Mutex
	n    int64
	cmds map[cmdKey]int64
	dec  map[decKey]int64
}

func (s *Stats) newConnStats() *connStats {
	cs := &connStats{cmds: map[cmdKey]int64{}, dec: map[decKey]int64{}}
	s.mu.Lock()
	s.live[cs] = struct{}{}
	s.mu.Unlock()
	return cs
}

// closeConnStats folds a closed connection's counters into the totals.
func (s *Stats) closeConnStats(cs *connStats) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, cs)
	s.commands.Add(cs.n)
	for k, v := range cs.cmds {
		s.byCmd[k] += v
	}
	for k, v := range cs.dec {
		s.decisions[k] += v
	}
	cs.n, cs.cmds, cs.dec = 0, map[cmdKey]int64{}, map[decKey]int64{}
}

// statName is the bounded name a command is counted under: a name the
// proxy knows, "JM.*", or "UNKNOWN". A client-chosen name never becomes a
// stats key (review H8B-L1b #3: 8,000 unique 15 KB names grew the heap by
// 126 MB, unauthenticated).
func statName(c Command) string {
	switch {
	case c.Class == ClassLocal:
		return "JM.*"
	case c.Class == ClassUnknown:
		return "UNKNOWN"
	}
	if _, ok := commands[c.Name]; ok {
		return c.Name
	}
	switch c.Name {
	case "CONFIG", "SCRIPT", "CLIENT":
		return c.Name
	}
	return "UNKNOWN"
}

// clip bounds client-chosen text kept in stats and logs.
func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func (cs *connStats) command(c Command, ctx string) {
	name := statName(c)
	cs.mu.Lock()
	cs.n++
	if len(c.Keys) == 0 {
		cs.cmds[cmdKey{Name: name, Shape: "-", Ctx: ctx, Class: c.Class}]++
	} else {
		for _, k := range c.Keys {
			cs.cmds[cmdKey{Name: name, Shape: k.Shape, Ctx: ctx, Class: c.Class, Trash: k.Trash}]++
		}
	}
	cs.mu.Unlock()
}

func (cs *connStats) allow(p Principal) {
	cs.mu.Lock()
	cs.dec[decKey{Outcome: DecAllow, Principal: p.Name, Kind: p.Kind}]++
	cs.mu.Unlock()
}

// recordUnknown notes unknown commands and unknown write keys (rare; global
// lock only then).
func (s *Stats) recordUnknown(c Command) {
	unknown := c.Class == ClassUnknown
	if !unknown && c.Class == ClassWrite {
		for _, k := range c.Keys {
			if k.Shape == ShapeUnknown {
				unknown = true
				break
			}
		}
	}
	if !unknown {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.Class == ClassUnknown {
		name := sanitizeToken(clip(c.Name, 64))
		if len(s.unknownCmd) < 1000 || s.unknownCmd[name] > 0 {
			s.unknownCmd[name]++
		}
		return
	}
	for i, k := range c.Keys {
		if k.Shape != ShapeUnknown || i >= len(c.Raw) {
			continue
		}
		raw := sanitizeToken(clip(c.Raw[i], 128))
		if s.unknownW[raw] == 0 && len(s.unknownW) < 1000 && s.Logf != nil {
			s.Logf("respproxy: UNKNOWN write key %q (cmd %s)", sanitizeToken(raw), c.Name)
		}
		if s.unknownW[raw] > 0 || len(s.unknownW) < 1000 {
			s.unknownW[raw]++
		}
	}
}

// recordCommand counts one command directly in the totals (tests and
// callers without a connection).
func (s *Stats) recordCommand(c Command, ctx string) {
	cs := s.newConnStats()
	cs.command(c, ctx)
	s.closeConnStats(cs)
	s.recordUnknown(c)
}

func (s *Stats) setMode(m Mode) {
	s.mu.Lock()
	s.mode = m
	s.mu.Unlock()
}

// recordDecision counts one gated unit and logs refusals, rate-limited to
// one line per second per principal and reason.
func (s *Stats) recordDecision(outcome string, d *Denial, p Principal, u *Unit) {
	k := decKey{Outcome: outcome, Principal: p.Name, Kind: p.Kind}
	if d != nil {
		k.Reason = d.Reason
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decisions[k]++
	if d == nil {
		return
	}
	entry := DecisionLog{
		Time: time.Now(), Outcome: outcome, Reason: d.Reason, Principal: p.Name, Kind: p.Kind,
		Unit: summarizeUnit(u), Detail: clip(d.Detail, 256),
	}
	if u != nil {
		entry.Remote, entry.Conn = u.Remote, u.ConnID
	}
	if len(s.recent) >= recentDecisions {
		copy(s.recent, s.recent[1:])
		s.recent = s.recent[:recentDecisions-1]
	}
	s.recent = append(s.recent, entry)
	lk := outcome + "|" + p.Name + "|" + string(d.Reason)
	if time.Since(s.lastLog[lk]) >= time.Second && s.Logf != nil {
		s.lastLog[lk] = time.Now()
		tag := "DENY"
		if outcome == DecWouldDeny {
			tag = "WOULD_DENY"
		}
		s.Logf("respproxy: %s %s principal=%s kind=%s conn=%d remote=%s unit=[%s] detail=%q",
			tag, d.Reason, p.Name, p.Kind, entry.Conn, entry.Remote, entry.Unit, entry.Detail)
	}
}

func summarizeUnit(u *Unit) string {
	if u == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(string(u.Kind))
	b.WriteString(":")
	for i, c := range u.Cmds {
		if i >= 8 {
			b.WriteString(" ...")
			break
		}
		b.WriteString(" ")
		b.WriteString(statName(c.Info))
		for j, r := range c.Info.Raw {
			if j >= 2 {
				break
			}
			b.WriteString(" ")
			if len(r) > 64 {
				r = r[:64]
			}
			b.WriteString(sanitizeToken(r))
		}
		if i < len(u.Cmds)-1 {
			b.WriteString(";")
		}
	}
	return b.String()
}

func (s *Stats) seen(keys []KeyRef) {
	if !s.TrackAnchors || len(keys) == 0 {
		return
	}
	s.mu.Lock()
	for _, k := range keys {
		if k.HasIno {
			s.seenIno[k.Ino] = struct{}{}
		}
	}
	s.mu.Unlock()
}

// analyse classifies one write unit for the anchor question (coverage).
func (s *Stats) analyse(ctx string, writes []writeRec) {
	if !s.TrackAnchors || len(writes) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.units[ctx]++

	childOf := map[uint64]uint64{} // ino -> parent, from d<P> HSET values
	dentryDirs := map[uint64]bool{}
	attrPar := map[uint64]uint64{}
	for _, w := range writes {
		for _, k := range w.cmd.Keys {
			if k.Shape == ShapeDentry {
				dentryDirs[k.Ino] = true
				if isHSet(w.cmd.Name) {
					for i := 3; i < len(w.args); i += 2 {
						if c, ok := entryInode(w.args[i]); ok {
							childOf[c] = k.Ino
						}
					}
				}
			}
			if k.Shape == ShapeInode && w.cmd.Name == "SET" && len(w.args) >= 3 {
				if p, ok := attrParent(w.args[2]); ok && p != 0 {
					attrPar[k.Ino] = p
				}
			}
		}
	}
	for _, w := range writes {
		for _, k := range w.cmd.Keys {
			if !k.HasIno || k.Trash || k.Group != "inode" {
				continue
			}
			anchor := AnchorNeedsAncestry
			if _, ok := childOf[k.Ino]; ok {
				anchor = AnchorDentryInTxn
			} else if dentryDirs[k.Ino] {
				anchor = AnchorDirOfDentry
			} else if _, ok := attrPar[k.Ino]; ok {
				anchor = AnchorAttrValue
			} else if _, ok := s.parent[k.Ino]; ok {
				anchor = AnchorLearned
			}
			m := s.anchors[k.Shape]
			if m == nil {
				m = map[string]int64{}
				s.anchors[k.Shape] = m
			}
			m[anchor]++
			if k.Shape == ShapeInode && w.cmd.Name == "SET" {
				if _, seen := s.seenIno[k.Ino]; !seen && len(dentryDirs) == 0 {
					s.newNoDent++
					if len(s.newNoDentX) < 20 {
						s.newNoDentX = append(s.newNoDentX, ctx+": "+w.cmd.Name+" "+k.Shape)
					}
				}
			}
		}
		for _, k := range w.cmd.Keys {
			if k.HasIno {
				s.seenIno[k.Ino] = struct{}{}
			}
		}
	}
	for c, p := range childOf {
		s.parent[c] = p
	}
	for c, p := range attrPar {
		s.parent[c] = p
	}
}

// Row is one line of the coverage report.
type Row struct {
	Cmd   string `json:"cmd"`
	Shape string `json:"shape"`
	Ctx   string `json:"ctx"`
	Class Class  `json:"class"`
	Trash bool   `json:"trash,omitempty"`
	Lease string `json:"lease,omitempty"`
	Count int64  `json:"count"`
}

// DecisionRow is one decision counter.
type DecisionRow struct {
	Outcome   string `json:"outcome"`
	Reason    Reason `json:"reason,omitempty"`
	Principal string `json:"principal"`
	Kind      Kind   `json:"kind"`
	Count     int64  `json:"count"`
}

// Snapshot is the JSON body of /stats.
type Snapshot struct {
	Mode              Mode                        `json:"mode"`
	Commands          int64                       `json:"commands"`
	Replies           int64                       `json:"replies"`
	Flushes           int64                       `json:"flushes"`
	Conns             int64                       `json:"conns"`
	AuthOK            int64                       `json:"auth_ok"`
	AuthBad           int64                       `json:"auth_bad"`
	AuthThrottled     int64                       `json:"auth_throttled"`
	UnauthRefused     int64                       `json:"unauthenticated_refused"`
	DiscardsIssued    int64                       `json:"discards_issued"`
	SyntheticReplies  int64                       `json:"synthetic_replies"`
	ProtocolErrors    int64                       `json:"protocol_errors"`
	ConnRefused       int64                       `json:"connections_refused"`
	OverflowClosed    int64                       `json:"overflow_closed"`
	Decisions         []DecisionRow               `json:"decisions"`
	Recent            []DecisionLog               `json:"recent_refusals"`
	Rows              []Row                       `json:"rows"`
	UnknownWriteKeys  map[string]int64            `json:"unknown_write_keys"`
	UnknownCommands   map[string]int64            `json:"unknown_commands"`
	Anchors           map[string]map[string]int64 `json:"anchors"`
	WriteUnits        map[string]int64            `json:"write_units"`
	NewInodeNoDentry  int64                       `json:"new_inode_without_dentry"`
	NewInodeExamples  []string                    `json:"new_inode_examples"`
	LearnedParentEdge int                         `json:"learned_parent_edges"`
}

func (s *Stats) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := Snapshot{
		Mode:     s.mode,
		Commands: s.commands.Load(), Replies: s.replies.Load(),
		Flushes: s.flushes.Load(), Conns: s.conns.Load(),
		AuthOK: s.authOK.Load(), AuthBad: s.authBad.Load(), AuthThrottled: s.authThrottled.Load(),
		UnauthRefused: s.unauthRefused.Load(), DiscardsIssued: s.discards.Load(),
		SyntheticReplies: s.synthetic.Load(), ProtocolErrors: s.protoErrors.Load(),
		ConnRefused: s.connRefused.Load(), OverflowClosed: s.overflowClosed.Load(),
		Recent:           append([]DecisionLog(nil), s.recent...),
		UnknownWriteKeys: copyMap(s.unknownW), UnknownCommands: copyMap(s.unknownCmd),
		Anchors: map[string]map[string]int64{}, WriteUnits: copyMap(s.units),
		NewInodeNoDentry: s.newNoDent, NewInodeExamples: append([]string(nil), s.newNoDentX...),
		LearnedParentEdge: len(s.parent),
	}
	for k, v := range s.anchors {
		snap.Anchors[k] = copyMap(v)
	}
	decisions := map[decKey]int64{}
	byCmd := map[cmdKey]int64{}
	for k, n := range s.decisions {
		decisions[k] += n
	}
	for k, n := range s.byCmd {
		byCmd[k] += n
	}
	for cs := range s.live {
		cs.mu.Lock()
		snap.Commands += cs.n
		for k, n := range cs.dec {
			decisions[k] += n
		}
		for k, n := range cs.cmds {
			byCmd[k] += n
		}
		cs.mu.Unlock()
	}
	for k, n := range decisions {
		snap.Decisions = append(snap.Decisions, DecisionRow{Outcome: k.Outcome, Reason: k.Reason, Principal: k.Principal, Kind: k.Kind, Count: n})
	}
	sort.Slice(snap.Decisions, func(i, j int) bool {
		a, b := snap.Decisions[i], snap.Decisions[j]
		if a.Outcome != b.Outcome {
			return a.Outcome < b.Outcome
		}
		if a.Reason != b.Reason {
			return a.Reason < b.Reason
		}
		return a.Principal < b.Principal
	})
	for k, n := range byCmd {
		r := Row{Cmd: k.Name, Shape: k.Shape, Ctx: k.Ctx, Class: k.Class, Trash: k.Trash, Count: n}
		if k.Class == ClassWrite {
			ref := ParseKey(placeholder(k.Shape))
			ref.Trash = k.Trash
			r.Lease = LeaseNeed(ref)
		}
		snap.Rows = append(snap.Rows, r)
	}
	sort.Slice(snap.Rows, func(i, j int) bool {
		a, b := snap.Rows[i], snap.Rows[j]
		if a.Class != b.Class {
			return a.Class < b.Class
		}
		if a.Shape != b.Shape {
			return a.Shape < b.Shape
		}
		if a.Cmd != b.Cmd {
			return a.Cmd < b.Cmd
		}
		return a.Ctx < b.Ctx
	})
	return snap
}

// placeholder turns a shape back into a parseable key for LeaseNeed.
func placeholder(shape string) string {
	switch shape {
	case ShapeInode:
		return "i1"
	case ShapeDentry:
		return "d1"
	case ShapeParent:
		return "p1"
	case ShapeXattr:
		return "x1"
	case ShapeSymlink:
		return "s1"
	case ShapeChunk:
		return "c1_0"
	case ShapeFlock:
		return "lockf1"
	case ShapePlock:
		return "lockp1"
	case ShapeLocked:
		return "locked1"
	case ShapeSession:
		return "session1"
	case ShapePresence:
		return "jum:presence:h"
	case ShapeLinkBench:
		return "jm:link-bench:0"
	case ShapeFarm:
		return "juicefarm:x"
	case ShapeJMFarm:
		return "jmfarm:x"
	case ShapeJMCtl:
		return "jmctl:x"
	case ShapeLease:
		return "jmlease:x"
	}
	return shape
}

func copyMap(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Handler serves /stats (full JSON), /counter (op counter only) and
// /metrics (Prometheus text: decisions, auth, connections).
func (s *Stats) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(s.Snapshot())
	})
	mux.HandleFunc("/counter", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"commands": s.Commands(), "flushes": s.flushes.Load()})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := s.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "jm_respproxy_commands_total %d\n", snap.Commands)
		fmt.Fprintf(w, "jm_respproxy_connections %d\n", snap.Conns)
		fmt.Fprintf(w, "jm_respproxy_auth_total{result=\"ok\"} %d\n", snap.AuthOK)
		fmt.Fprintf(w, "jm_respproxy_auth_total{result=\"bad\"} %d\n", snap.AuthBad)
		fmt.Fprintf(w, "jm_respproxy_auth_total{result=\"throttled\"} %d\n", snap.AuthThrottled)
		fmt.Fprintf(w, "jm_respproxy_unauthenticated_refused_total %d\n", snap.UnauthRefused)
		fmt.Fprintf(w, "jm_respproxy_discards_issued_total %d\n", snap.DiscardsIssued)
		fmt.Fprintf(w, "jm_respproxy_unknown_write_keys %d\n", len(snap.UnknownWriteKeys))
		for _, m := range []Mode{ModePassthrough, ModeObserve, ModeEnforce} {
			v := 0
			if snap.Mode == m {
				v = 1
			}
			fmt.Fprintf(w, "jm_respproxy_mode{mode=%q} %d\n", m, v)
		}
		for _, d := range snap.Decisions {
			fmt.Fprintf(w, "jm_respproxy_decisions_total{outcome=%q,reason=%q,principal=%q,kind=%q} %d\n",
				d.Outcome, d.Reason, d.Principal, d.Kind, d.Count)
		}
	})
	return mux
}
