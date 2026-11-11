package nfs

import (
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// offlineRefusalLogEvery bounds how often an offline-gate refusal is logged at
// Info. A cold folder can refuse hundreds of requests a second; one line per
// window, with a count of the rest, keeps the log readable.
const offlineRefusalLogEvery = 10 * time.Second

// offlineRefusalLog rate-limits logOfflineRefusal. Process-wide, like the
// offline flags it reports.
var offlineRefusalLog struct {
	mu         sync.Mutex
	last       time.Time
	suppressed int64
	now        func() time.Time // test hook; nil means time.Now
}

// logOfflineRefusal records that an offline gate refused a request with
// pin.ErrOfflineNotAvailable (ENXIO to the application, "Device not
// configured"). It logs at Info at most once per offlineRefusalLogEvery, with
// the gate (op, why), the path, and why the volume is offline (user toggle or
// auto-offline with its reason). Refusals inside the window log at Debug and
// are counted in the next Info line's suppressed field.
//
// These refusals used to log only at Debug, so a lossy-link flap that made
// Premiere or Resolve mark media offline left no trace in a normal log.
func logOfflineRefusal(op, why, path string) {
	now := time.Now
	offlineRefusalLog.mu.Lock()
	if offlineRefusalLog.now != nil {
		now = offlineRefusalLog.now
	}
	t := now()
	if !offlineRefusalLog.last.IsZero() && t.Sub(offlineRefusalLog.last) < offlineRefusalLogEvery {
		offlineRefusalLog.suppressed++
		offlineRefusalLog.mu.Unlock()
		jmlog.Debug("offline gate refused request", "op", op, "why", why, "path", path)
		return
	}
	suppressed := offlineRefusalLog.suppressed
	windowStart := offlineRefusalLog.last
	offlineRefusalLog.suppressed = 0
	offlineRefusalLog.last = t
	offlineRefusalLog.mu.Unlock()

	st := pin.State()
	jmlog.Info("offline gate refused request (ENXIO to the app)",
		"op", op,
		"why", why,
		"path", path,
		"user_offline", st.UserOffline,
		"auto_offline", st.AutoOffline,
		"auto_reason", st.Reason,
		"offline_for_sec", st.SinceSec,
		"suppressed_since_last", suppressed,
		// The suppressed count covers refusals after this time. It may span an
		// earlier offline period if no summary was written in between.
		"suppressed_window_start", formatWindowStart(windowStart))
}

// FlushOfflineRefusalLog writes the count of refusals not yet reported, then
// starts a fresh window, so the next refusal logs at once. Call it when
// offline mode lifts: without it the last outage's count would be printed
// only at the next refusal, possibly in a later outage, or never.
func FlushOfflineRefusalLog(event string) {
	offlineRefusalLog.mu.Lock()
	suppressed := offlineRefusalLog.suppressed
	windowStart := offlineRefusalLog.last
	offlineRefusalLog.suppressed = 0
	offlineRefusalLog.last = time.Time{}
	offlineRefusalLog.mu.Unlock()
	if suppressed == 0 {
		return
	}
	jmlog.Info("offline gate refusals summary",
		"event", event,
		"suppressed_since_last", suppressed,
		"suppressed_window_start", formatWindowStart(windowStart))
}

func formatWindowStart(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
