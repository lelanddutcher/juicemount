package enginehost

import (
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// LogEvents writes each event to the JuiceMount log. The lab and the laptop
// run read these lines ("engine helper ready" carries start_ms).
func LogEvents(e Event) {
	kv := []any{"event", e.Kind}
	if e.PID != 0 {
		kv = append(kv, "pid", e.PID)
	}
	if e.Epoch != 0 {
		kv = append(kv, "epoch", e.Epoch)
	}
	if e.Elapsed != 0 {
		kv = append(kv, "ms", e.Elapsed.Milliseconds())
	}
	if e.Attempt != 0 {
		kv = append(kv, "attempt", e.Attempt)
	}
	if e.Err != nil {
		kv = append(kv, "error", e.Err.Error())
	}
	switch e.Kind {
	case EventReady:
		jmlog.Info("engine helper ready", append(kv, "start_ms", e.Elapsed.Milliseconds())...)
	case EventLaunch, EventBackoff:
		jmlog.Debug("engine helper "+e.Kind, kv...)
	case EventStopped:
		jmlog.Info("engine helper stopped", kv...)
	case EventPersistent, EventRefused:
		jmlog.Error("engine helper "+e.Kind, kv...)
	default:
		jmlog.Warn("engine helper "+e.Kind, kv...)
	}
}

// StartShadow starts a supervisor for level shadow and returns it at once.
// The helper reports in the log; nothing waits for it.
func StartShadow(cfg Config) *Supervisor {
	if cfg.OnEvent == nil {
		cfg.OnEvent = LogEvents
	}
	if cfg.ExpectVersion == "" {
		cfg.ExpectVersion = ExpectedJuiceFSVersion
	}
	s := New(cfg)
	s.Start()
	jmlog.Info("engine helper starting", "level", LevelShadow.String(), "bin", cfg.Bin)
	return s
}

// StopShadow stops a supervisor (nil is fine) and logs how.
func StopShadow(s *Supervisor) {
	if s == nil {
		return
	}
	how, took := s.Stop()
	jmlog.Info("engine helper stop", "how", how, "ms", took.Milliseconds())
}
