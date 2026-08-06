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
	if reason := streamIneligible(e); reason != "" {
		// An entry that never became eligible has no session to tear down. One
		// that HAD a session and lost eligibility is a different case and is not
		// handled here — losing eligibility mid-stream (hashValid flipping on a
		// late out-of-order write) needs the whole stream abandoned via
		// rollbackStream, and doing that correctly requires knowing the entry is
		// finished rather than merely paused. Left explicit rather than guessed.
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
