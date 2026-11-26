package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// trip is one round trip seen by the relay: the first client byte after the
// connection was idle, and the first server byte that answered it.
type trip struct {
	Start int64 `json:"s"` // unix ns
	End   int64 `json:"e"`
	Conn  int   `json:"c"`
}

type relay struct {
	mu    sync.Mutex
	trips []trip // ring of the last maxTrips
	next  int
	total int64
	conns int
}

const maxTrips = 1 << 16

func (r *relay) add(t trip) {
	r.mu.Lock()
	if len(r.trips) < maxTrips {
		r.trips = append(r.trips, t)
	} else {
		r.trips[r.next] = t
		r.next = (r.next + 1) % maxTrips
	}
	r.total++
	r.mu.Unlock()
}

// connState tracks one connection: a request is pending from the first
// client byte until the first server byte. Server bytes while nothing is
// pending (pub/sub pushes) are not round trips.
type connState struct {
	mu      sync.Mutex
	pending bool
	start   int64
}

func (r *relay) serve(c net.Conn, to string) {
	defer c.Close()
	s, err := net.DialTimeout("tcp", to, 10*time.Second)
	if err != nil {
		return
	}
	defer s.Close()
	r.mu.Lock()
	r.conns++
	id := r.conns
	r.mu.Unlock()
	st := &connState{}
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn, up bool) {
		buf := make([]byte, 64<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				now := time.Now().UnixNano()
				st.mu.Lock()
				if up && !st.pending {
					st.pending, st.start = true, now
				} else if !up && st.pending {
					st.pending = false
					r.add(trip{Start: st.start, End: now, Conn: id})
				}
				st.mu.Unlock()
				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		done <- struct{}{}
	}
	go pipe(s, c, true)
	go pipe(c, s, false)
	<-done
	<-done
}

func (r *relay) window(from, to int64) []trip {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []trip
	for _, t := range r.trips {
		if t.Start >= from && t.End <= to {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

func cmdRelay(args []string) error {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:16379", "address the engine connects to")
	to := fs.String("to", "", "Redis or RESP proxy address (host:port)")
	httpAddr := fs.String("http", "127.0.0.1:16380", "counter endpoint: GET /trips?from=NS&to=NS")
	_ = fs.Parse(args)
	if *to == "" {
		return errors.New("relay: -to is required")
	}
	r := &relay{}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/trips", func(w http.ResponseWriter, q *http.Request) {
		from, _ := strconv.ParseInt(q.URL.Query().Get("from"), 10, 64)
		until, _ := strconv.ParseInt(q.URL.Query().Get("to"), 10, 64)
		if until == 0 {
			until = time.Now().UnixNano()
		}
		_ = json.NewEncoder(w).Encode(r.window(from, until))
	})
	mux.HandleFunc("/total", func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		fmt.Fprintf(w, "{\"round_trips\":%d,\"conns\":%d}\n", r.total, r.conns)
	})
	hl, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		return err
	}
	go func() { _ = http.Serve(hl, mux) }()
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go r.serve(c, *to)
	}
}

// tripCounter asks a relay for the round trips inside a step.
type tripCounter struct{ url string }

type tripStats struct {
	Trips  int     `json:"rt"`
	Serial int     `json:"rt_serial"` // round trips that happened one after another
	RTTms  float64 `json:"rt_median_ms"`
}

func (c *tripCounter) between(from, to time.Time) *tripStats {
	if c == nil || c.url == "" {
		return nil
	}
	// The relay sees a reply a moment after the engine's read returns; wait
	// a little so a step's last trip is in the window.
	time.Sleep(5 * time.Millisecond)
	resp, err := http.Get(fmt.Sprintf("%s/trips?from=%d&to=%d", c.url, from.UnixNano(), to.Add(2*time.Millisecond).UnixNano()))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var ts []trip
	if json.NewDecoder(resp.Body).Decode(&ts) != nil {
		return nil
	}
	return summarize(ts)
}

// summarize counts the serial chain: the largest set of round trips with no
// two overlapping in time (greedy by end time). Trips that overlap ran in
// parallel and cost one round trip of waiting together.
func summarize(ts []trip) *tripStats {
	st := &tripStats{Trips: len(ts)}
	if len(ts) == 0 {
		return st
	}
	byEnd := append([]trip(nil), ts...)
	sort.Slice(byEnd, func(i, j int) bool { return byEnd[i].End < byEnd[j].End })
	var last int64
	for _, t := range byEnd {
		if t.Start >= last {
			st.Serial++
			last = t.End
		}
	}
	ds := make([]float64, len(ts))
	for i, t := range ts {
		ds[i] = float64(t.End-t.Start) / 1e6
	}
	sort.Float64s(ds)
	st.RTTms = ds[len(ds)/2]
	return st
}
