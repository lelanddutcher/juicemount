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
