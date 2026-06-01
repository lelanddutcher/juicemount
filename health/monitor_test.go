package health

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMonitorStartStop(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)
	m.Start()
	// Give it a moment so the initial check completes.
	time.Sleep(200 * time.Millisecond)
	m.Stop()
	// Reaching here without hanging means start/stop works.
}

func TestRedisHealthCheck(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)
	m.Start()
	time.Sleep(200 * time.Millisecond)
	defer m.Stop()

	s := m.Status()
	if !s.Redis.Healthy {
		t.Fatalf("expected Redis healthy, got unhealthy: %s", s.Redis.Message)
	}
	if s.Redis.LastCheck.IsZero() {
		t.Fatal("Redis LastCheck should not be zero")
	}
}

func TestMinIOHealthCheck(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)
	m.Start()
	time.Sleep(200 * time.Millisecond)
	defer m.Stop()

	s := m.Status()
	if !s.MinIO.Healthy {
		t.Fatalf("expected MinIO healthy, got unhealthy: %s", s.MinIO.Message)
	}
}

func TestFUSEMountCheck(t *testing.T) {
	fp := fusePath(t)

	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fp,
	}
	m := New(cfg)
	m.Start()
	time.Sleep(200 * time.Millisecond)
	defer m.Stop()

	s := m.Status()
	if !s.FUSE.Healthy {
		t.Fatalf("expected FUSE healthy (path %s exists), got unhealthy: %s", fp, s.FUSE.Message)
	}
}

// TestFUSEDebounceSuppressesFlap certifies the anti-flap hysteresis: a single
// unhealthy blip must not flip the reported FUSE status (that strobing was the
// user-visible bug), while a sustained outage does, and recovery is immediate.
func TestFUSEDebounceSuppressesFlap(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)

	healthy := func() ComponentStatus {
		return ComponentStatus{Healthy: true, LastCheck: time.Now(), Message: "ok"}
	}
	unhealthy := func() ComponentStatus {
		return ComponentStatus{Healthy: false, LastCheck: time.Now(), Message: "stat timed out (wedged FUSE mount)"}
	}

	// Initialize debounced state as healthy.
	if got := m.debounceFUSE(healthy()); !got.Healthy {
		t.Fatalf("initial healthy probe should report healthy, got %+v", got)
	}

	// A single unhealthy blip must NOT flip the reported status — anti-flap.
	if got := m.debounceFUSE(unhealthy()); !got.Healthy {
		t.Fatalf("single unhealthy blip should stay reported-healthy, got degraded: %+v", got)
	}

	// One healthy probe resets the streak; the link is flapping, not down.
	if got := m.debounceFUSE(healthy()); !got.Healthy {
		t.Fatalf("recovery probe should report healthy, got %+v", got)
	}

	// A SUSTAINED outage — FUSEFlapToDegraded consecutive unhealthy probes —
	// must flip the reported status to degraded and surface the real reason.
	var last ComponentStatus
	for i := 0; i < FUSEFlapToDegraded; i++ {
		last = m.debounceFUSE(unhealthy())
	}
	if last.Healthy {
		t.Fatalf("after %d consecutive unhealthy probes, status should be degraded, got healthy: %+v",
			FUSEFlapToDegraded, last)
	}
	if last.Message == "" {
		t.Fatal("degraded status should surface the raw reason message, got empty")
	}

	// Recovery is immediate on the first healthy probe.
	if got := m.debounceFUSE(healthy()); !got.Healthy {
		t.Fatalf("first healthy probe after outage should recover immediately, got %+v", got)
	}
}

// TestFUSEDebounceAlternatingNeverDegrades drives the classic strobe pattern
// (bad, good, bad, good…). A link that passes every other probe is working
// enough — it must never be reported degraded, since that up/down/up/down
// flicker is exactly what we're killing.
func TestFUSEDebounceAlternatingNeverDegrades(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)
	m.debounceFUSE(ComponentStatus{Healthy: true, LastCheck: time.Now(), Message: "ok"})
	for i := 0; i < 10; i++ {
		bad := m.debounceFUSE(ComponentStatus{Healthy: false, LastCheck: time.Now(), Message: "blip"})
		if !bad.Healthy {
			t.Fatalf("iteration %d: alternating pattern flipped to degraded — strobe not suppressed", i)
		}
		good := m.debounceFUSE(ComponentStatus{Healthy: true, LastCheck: time.Now(), Message: "ok"})
		if !good.Healthy {
			t.Fatalf("iteration %d: healthy probe reported degraded", i)
		}
	}
}

func TestStatusReturnsCorrectState(t *testing.T) {
	cfg := Config{
		RedisURL: "127.0.0.1:6379",
		MinIOURL: "http://127.0.0.1:9000",
		FUSEPath: fusePath(t),
	}
	m := New(cfg)
	m.Start()
	time.Sleep(200 * time.Millisecond)
	defer m.Stop()

	s := m.Status()

	// All individual checks should be healthy against live services.
	if !s.Redis.Healthy {
		t.Errorf("Redis unhealthy: %s", s.Redis.Message)
	}
	if !s.MinIO.Healthy {
		t.Errorf("MinIO unhealthy: %s", s.MinIO.Message)
	}
	if !s.FUSE.Healthy {
		t.Errorf("FUSE unhealthy: %s", s.FUSE.Message)
	}
	// NFS mount point not configured, should default healthy.
	if !s.NFS.Healthy {
		t.Errorf("NFS unhealthy: %s", s.NFS.Message)
	}
	if !s.Overall {
		t.Error("expected overall healthy")
	}
}

// fusePath returns the expanded ~/.juicemount/fuse-internal path,
// creating the directory if it doesn't already exist so the FUSE
// check can succeed.
func fusePath(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("cannot determine home dir: %v", err)
	}
	fp := filepath.Join(home, ".juicemount", "fuse-internal")
	if err := os.MkdirAll(fp, 0o755); err != nil {
		t.Fatalf("cannot create fuse path %s: %v", fp, err)
	}
	return fp
}
