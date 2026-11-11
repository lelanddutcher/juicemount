package nfs

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// Per-read timeline (2026-11-11 laptop lab, slow-link diagnosis).
//
// One line per step of a read: the NFS READ and each of its subreads
// (internal/nfs), the FUSE data gate admission, the FUSE pread start and end
// (also the late end of a pread that timed out), a JUKEBOX retry that joins a
// running pread, and each read-ahead block. The bundled JuiceFS writes the
// matching object GET lines ("rt jfs ...") to its own log.
//
// JM_READ_TIMELINE=1 writes the lines at Info (a lab switch; it is also
// passed to the JuiceFS child). Otherwise they are written at Debug, and
// only when the log level is Debug.
var readTimelineInfo = os.Getenv("JM_READ_TIMELINE") == "1"

// readTimelineOn reports whether timeline lines are written now.
func readTimelineOn() bool {
	if readTimelineInfo {
		return true
	}
	l := jmlog.Logger()
	return l != nil && l.Enabled(context.Background(), slog.LevelDebug)
}

// readTimeline writes one timeline line. Callers check readTimelineOn
// first, so an untraced read builds no arguments.
func readTimeline(msg string, args ...any) {
	if readTimelineInfo {
		jmlog.Info(msg, args...)
		return
	}
	jmlog.Debug(msg, args...)
}

// readTimelineSeq numbers FUSE preads, so the late end of a pread that
// timed out can be matched to its start.
var readTimelineSeq atomic.Uint64

func init() {
	nfslib.ReadTimelineOn = readTimelineOn
	nfslib.ReadTimeline = readTimeline
}

// fuseGateCounts is the data gate occupancy for a timeline line.
func fuseGateCounts() (total, background int64) {
	return fuseDataInFlight.Load(), fuseBackgroundInFlight.Load()
}

// errString is "" for nil, so a timeline field keeps one type.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
