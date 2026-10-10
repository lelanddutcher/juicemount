package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
)

// Read-side counters for the derivative plane.
//
// WHY THIS EXISTS. The whole point of contributing derivatives back is that the
// NEXT client reads them instead of regenerating. We had no way to tell whether
// that was happening: /derivatives and /blob had zero instrumentation — no log
// line, no counter — so "the consumer pulled the poster" and "the consumer
// regenerated the poster locally" were indistinguishable from this side.
//
// That gap is not academic. It is exactly the question broader multi-machine
// sync turns on: when a second Mac opens a folder the first Mac already
// processed, does it PULL or does it re-derive? Without a counter, the only
// available answer is "nobody knows", and a regression to re-deriving would be
// completely silent — it costs CPU and time, never correctness, so nothing
// would ever fail to reveal it.
//
// Counters only: no branching reads them, so they cannot change behaviour. They
// are atomics on a request path that already does file I/O, so the cost is
// nil relative to what the handler is about to do anyway.
var derivReads struct {
	manifestQueries atomic.Int64 // GET /derivatives?inode=
	manifestWithRow atomic.Int64 // ...that returned at least one derivative
	manifestEmpty   atomic.Int64 // ...that returned none (caller will regenerate)
	blobServed      atomic.Int64 // GET /blob that returned bytes
	blobBytes       atomic.Int64 // total bytes served
	blobMissing     atomic.Int64 // GET /blob that found nothing

	mu      sync.Mutex
	byKind  map[string]int64 // blob serves per kind
	byBytes map[string]int64
}

func noteManifestRead(rows int) {
	derivReads.manifestQueries.Add(1)
	if rows > 0 {
		derivReads.manifestWithRow.Add(1)
	} else {
		derivReads.manifestEmpty.Add(1)
	}
}

func noteBlobServed(kind string, n int64) {
	derivReads.blobServed.Add(1)
	derivReads.blobBytes.Add(n)
	derivReads.mu.Lock()
	if derivReads.byKind == nil {
		derivReads.byKind = map[string]int64{}
		derivReads.byBytes = map[string]int64{}
	}
	derivReads.byKind[kind]++
	derivReads.byBytes[kind] += n
	derivReads.mu.Unlock()
}

func noteBlobMissing() { derivReads.blobMissing.Add(1) }

// handleDerivReadsHTTP serves GET /deriv-reads — "is anyone actually consuming
// the derivative plane, and for which kinds?"
//
// Read it as a ratio, not a total. manifest_empty climbing while blob_served
// stays flat means callers are asking and finding nothing (so they regenerate);
// blob_served climbing means the contribution is doing its job and a second
// machine is reusing work the first one paid for.
func handleDerivReadsHTTP(w http.ResponseWriter, r *http.Request) {
	derivReads.mu.Lock()
	kinds := make(map[string]int64, len(derivReads.byKind))
	bytes := make(map[string]int64, len(derivReads.byBytes))
	for k, v := range derivReads.byKind {
		kinds[k] = v
	}
	for k, v := range derivReads.byBytes {
		bytes[k] = v
	}
	derivReads.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"manifest_queries":  derivReads.manifestQueries.Load(),
		"manifest_with_row": derivReads.manifestWithRow.Load(),
		"manifest_empty":    derivReads.manifestEmpty.Load(),
		"blob_served":       derivReads.blobServed.Load(),
		"blob_bytes":        derivReads.blobBytes.Load(),
		"blob_missing":      derivReads.blobMissing.Load(),
		"blob_by_kind":      kinds,
		"blob_bytes_byKind": bytes,
		"note": "Counters since process start; they reset on restart. blob_served > 0 means a " +
			"client is REUSING contributed derivatives rather than regenerating them.",
	})
}

// countingManifestHandler / countingBlobHandler WRAP the handlers instead of
// instrumenting inside them.
//
// The first version of this counted at the call sites, and the test caught it
// immediately: both handlers have several early returns, and the sites I picked
// were not the ones the requests actually took. Instrumenting N return paths is
// the same trap as guarding N call sites — the next branch someone adds is
// silently uncounted, and a counter that is quietly wrong is worse than no
// counter, because it gets believed.
//
// Wrapping is one place, and it cannot be bypassed by a new branch.
func countingManifestHandler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &bodySniffer{ResponseWriter: w, status: 200}
		h(rec, r)
		// "Did this caller find a derivative, or will it regenerate?" is the
		// whole question, and it is answerable only from the body: an absent
		// asset and an asset with no derivatives are both 200 with an empty
		// list. The payload is a small JSON manifest, so buffering it is cheap.
		rows := 0
		if rec.status == 200 {
			var m struct {
				Derivatives []json.RawMessage `json:"derivatives"`
			}
			if json.Unmarshal(rec.buf.Bytes(), &m) == nil {
				rows = len(m.Derivatives)
			}
		}
		noteManifestRead(rows)
	}
}

func countingBlobHandler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &countingWriter{ResponseWriter: w, status: 200}
		h(rec, r)
		switch {
		case rec.status == 200 || rec.status == 206:
			noteBlobServed(r.URL.Query().Get("kind"), rec.n)
		case rec.status == 404:
			noteBlobMissing()
		}
	}
}

// bodySniffer buffers the response so the manifest's row count can be read
// after the handler returns, while still writing through to the client.
type bodySniffer struct {
	http.ResponseWriter
	buf    bytes.Buffer
	status int
}

func (b *bodySniffer) WriteHeader(code int) {
	b.status = code
	b.ResponseWriter.WriteHeader(code)
}

func (b *bodySniffer) Write(p []byte) (int, error) {
	b.buf.Write(p)
	return b.ResponseWriter.Write(p)
}

// countingWriter records status and bytes without buffering — a blob can be a
// multi-megabyte proxy and must stream straight through.
type countingWriter struct {
	http.ResponseWriter
	n      int64
	status int
}

func (c *countingWriter) WriteHeader(code int) {
	c.status = code
	c.ResponseWriter.WriteHeader(code)
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.n += int64(n)
	return n, err
}
