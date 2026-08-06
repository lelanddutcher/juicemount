package nfs

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// THE STREAMER — the background loop that actually moves bytes.
//
// Lives on the Drainer rather than the SpoolStore for two reasons: the Drainer
// owns fuseRoot (the SpoolStore has no idea where the backend is mounted), and
// it already models "background work against spool entries", which is exactly
// what this is.
//
// NOT SPAWNED FROM OpenWrite. That was the obvious-looking home and it is wrong:
// the go-nfs fork calls OpenFile→WriteAt→Close on EVERY WRITE RPC, so spawning
// there would start a copier per RPC rather than per file. The same misreading
// is why finalize is driven by the idle sweeper instead of by refcount hitting
// zero — see the refcount comment on SpoolEntry.
//
// One ticker, one session per streamed entry, and the whole thing does not even
// start when JM_SPOOL_STREAM_DRAIN is unset.

// streamSession is the per-entry state a streamed copy needs across ticks:
// handles that must not be reopened per chunk, and the paths to publish to.
type streamSession struct {
	entry    *SpoolEntry
	src      *os.File // O_RDWR on the spool file — punching needs write access
	dest     *os.File // the hidden sibling being written
	verifyFD *os.File // separate read handle for the at-rest readback
	tempPath string
	realPath string
}

func (ss *streamSession) close() {
	for _, f := range []*os.File{ss.src, ss.dest, ss.verifyFD} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// streamer holds the live sessions.
type streamer struct {
	mu       sync.Mutex
	sessions map[int64]*streamSession
}

// StartStreamer runs the streaming copier until the returned stop is called.
//
// Returns a no-op stop when streaming is disabled, and does not start a
// goroutine at all — a disabled feature should cost nothing, and a ticker that
// wakes only to decide it has no work is exactly the kind of idle churn this
// codebase has had to hunt down before.
func (d *Drainer) StartStreamer(tick time.Duration) (stop func()) {
	if !streamDrainEnabled() {
		return func() {}
	}
	if tick <= 0 {
		tick = time.Second
	}
	if d.streamer == nil {
		d.streamer = &streamer{sessions: map[int64]*streamSession{}}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		for {
			select {
			case <-done:
				d.closeAllStreamSessions()
				return
			case <-t.C:
				d.streamOnce()
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// streamOnce advances every eligible in-flight entry by one chunk.
func (d *Drainer) streamOnce() {
	if d == nil || d.spool == nil || d.streamer == nil {
		return
	}
	for _, e := range d.spool.index.Snapshot() {
		d.streamEntryOnce(e)
	}
}

func (d *Drainer) streamEntryOnce(e *SpoolEntry) {
	if e == nil {
		return
	}
	// FINISHED vs MERELY PAUSED. NFS does OpenFile->WriteAt->Close on every WRITE
	// RPC, so refcount hitting zero says nothing about completion — that trap is
	// documented on SpoolEntry.refcount and is why finalize runs off the idle
	// sweeper. `closed` is the real signal: the sweeper has finalized the entry
	// and no further writes will arrive.
	if ss := d.existingSession(e.ID()); ss != nil && e.IsClosed() {
		d.finishStreamSession(e, ss)
		return
	}

	if reason := streamIneligible(e); reason != "" {
		// A session that exists but has become ineligible has LOST eligibility
		// mid-stream — in practice a late out-of-order write flipping hashValid,
		// which is exactly what a below-punch rewrite now does deliberately.
		//
		// The stream cannot simply stop: a prefix is already punched, so the spool
		// file can no longer reconstruct the file on its own and the whole-file
		// drain is not a fallback. But it also must not keep streaming against a
		// hash it can no longer trust. Freeze it: close the handles, leave the
		// partial and punchedEnd alone, and let finalize resolve it. punchedEnd
		// stays set so boot recovery still refuses to re-drain the holey spool
		// file — clearing it here would re-arm the zeros-over-good-data hazard.
		if ss := d.existingSession(e.ID()); ss != nil {
			jmlog.Warn("stream: entry lost eligibility mid-stream, freezing the session",
				"path", e.NFSPath(), "reason", reason, "punched_end", e.PunchedEnd())
			d.dropSession(e.ID())
		}
		return
	}

	ss := d.sessionFor(e)
	if ss == nil {
		return
	}
	moved, err := streamStep(e, ss.src, ss.dest, d.spool, spoolMetaDurability{meta: d.spool.Meta()},
		destReadbackVerifier{f: ss.verifyFD})
	if err != nil {
		// The advance failed with the spool prefix intact — that is the whole
		// point of verifying before punching. Log and retry on the next tick
		// rather than tearing the session down, since most failures here
		// (transient FUSE, a blip) clear on their own.
		jmlog.Warn("stream: advance failed, prefix intact — will retry",
			"path", e.NFSPath(), "error", err.Error())
		return
	}
	if moved > 0 {
		jmlog.Debug("stream: reclaimed spool bytes",
			"path", e.NFSPath(), "bytes", moved, "punched_end", e.PunchedEnd())
	}
}

// existingSession returns the live session for an entry, or nil.
func (d *Drainer) existingSession(id int64) *streamSession {
	d.streamer.mu.Lock()
	defer d.streamer.mu.Unlock()
	return d.streamer.sessions[id]
}

// dropSession closes and forgets a session WITHOUT touching the partial or
// punchedEnd. Used when a stream is frozen rather than completed.
func (d *Drainer) dropSession(id int64) {
	d.streamer.mu.Lock()
	ss := d.streamer.sessions[id]
	delete(d.streamer.sessions, id)
	d.streamer.mu.Unlock()
	if ss != nil {
		ss.close()
	}
}

// finishStreamSession copies whatever the streamer never sealed and publishes
// the file at its real path.
//
// The unsealed tail is the deliberate cost of the seal margins: they hold back
// the last chunk and the head reserve precisely so a late rewrite stays on the
// fast spool path. At finalize there is no more rewriting to fear, so the
// remainder is copied straight across.
func (d *Drainer) finishStreamSession(e *SpoolEntry, ss *streamSession) {
	defer d.dropSession(e.ID())

	start := e.PunchedEnd()
	end := e.WrittenEnd()
	if end > start {
		buf := make([]byte, end-start)
		if _, err := ss.src.ReadAt(buf, start); err != nil {
			jmlog.Warn("stream: cannot read the unsealed tail; leaving the partial for "+
				"the boot sweep", "path", e.NFSPath(), "error", err.Error())
			return
		}
		if _, err := ss.dest.WriteAt(buf, start); err != nil {
			jmlog.Warn("stream: cannot write the unsealed tail", "path", e.NFSPath(),
				"error", err.Error())
			return
		}
		if err := ss.dest.Sync(); err != nil {
			jmlog.Warn("stream: cannot sync before publish", "path", e.NFSPath(),
				"error", err.Error())
			return
		}
	}
	if err := finishStream(e, ss.tempPath, ss.realPath); err != nil {
		jmlog.Warn("stream: publish failed; the partial remains for the boot sweep",
			"path", e.NFSPath(), "error", err.Error())
		return
	}
	jmlog.Info("stream: published a streamed file", "path", e.NFSPath(), "bytes", end)
}

// sessionFor returns the live session for an entry, creating it on first use.
// A creation failure is logged once and retried next tick; it must not be fatal,
// because the whole-file drain remains a correct fallback.
func (d *Drainer) sessionFor(e *SpoolEntry) *streamSession {
	id := e.ID()
	d.streamer.mu.Lock()
	defer d.streamer.mu.Unlock()
	if ss, ok := d.streamer.sessions[id]; ok {
		return ss
	}

	realPath := filepath.Join(d.fuseRoot, e.NFSPath())
	tempPath, err := streamTempPath(realPath, id)
	if err != nil {
		jmlog.Warn("stream: cannot derive temp path", "path", e.NFSPath(), "error", err.Error())
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(realPath), 0o755); err != nil {
		jmlog.Warn("stream: mkdir dest parent", "path", e.NFSPath(), "error", err.Error())
		return nil
	}
	// O_RDWR on the spool file: punchRange needs write access (a read-only fd
	// fails with EPERM — that mistake cost a debugging round earlier).
	src, err := os.OpenFile(e.SpoolFilePath(), os.O_RDWR, 0o644)
	if err != nil {
		jmlog.Warn("stream: open spool rw", "path", e.NFSPath(), "error", err.Error())
		return nil
	}
	dest, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		src.Close()
		jmlog.Warn("stream: create partial", "path", tempPath, "error", err.Error())
		return nil
	}
	verifyFD, err := os.Open(tempPath)
	if err != nil {
		src.Close()
		dest.Close()
		jmlog.Warn("stream: open partial for verify", "path", tempPath, "error", err.Error())
		return nil
	}

	ss := &streamSession{
		entry: e, src: src, dest: dest, verifyFD: verifyFD,
		tempPath: tempPath, realPath: realPath,
	}
	d.streamer.sessions[id] = ss
	// Publish the destination so the read shadow can serve punched ranges. MUST
	// happen before the first punch, or a punched read has nowhere to go.
	e.SetStreamDest(tempPath)
	jmlog.Info("stream: began streaming a large file",
		"path", e.NFSPath(), "partial", filepath.Base(tempPath))
	return ss
}

// closeAllStreamSessions releases handles on shutdown. Partials are deliberately
// LEFT on disk: the boot sweep reclaims them, and deleting them here would throw
// away work that a restart could otherwise have continued from.
func (d *Drainer) closeAllStreamSessions() {
	if d.streamer == nil {
		return
	}
	d.streamer.mu.Lock()
	defer d.streamer.mu.Unlock()
	for id, ss := range d.streamer.sessions {
		ss.close()
		delete(d.streamer.sessions, id)
	}
}
