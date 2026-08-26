package main

import (
	"testing"

	"github.com/lelanddutcher/juicemount/health"
)

func TestNewHealthConfigUsesResolvedDependencies(t *testing.T) {
	cfg := newHealthConfig(
		"redis://redis:6379/1",
		"http://minio:9000",
		"/mnt/juicefs",
		"/mnt/nfs-export",
		true,
	)
	if cfg.RedisURL != "redis:6379" {
		t.Fatalf("RedisURL = %q, want redis:6379", cfg.RedisURL)
	}
	if cfg.MinIOURL != "http://minio:9000" {
		t.Fatalf("MinIOURL = %q, want configured container endpoint", cfg.MinIOURL)
	}
	if cfg.NFSMountPoint != "" {
		t.Fatalf("server-only NFSMountPoint = %q, want empty", cfg.NFSMountPoint)
	}

	appCfg := newHealthConfig("redis://redis:6379/1", "http://minio:9000", "/fuse", "/Volumes/zpool", false)
	if appCfg.NFSMountPoint != "/Volumes/zpool" {
		t.Fatalf("app NFSMountPoint = %q, want client mount", appCfg.NFSMountPoint)
	}
}

func TestJM5HealthSnapshotServerModeUsesListener(t *testing.T) {
	status := health.HealthStatus{
		Redis: health.ComponentStatus{Healthy: true},
		MinIO: health.ComponentStatus{Healthy: true},
		FUSE:  health.ComponentStatus{Healthy: true},
		NFS:   health.ComponentStatus{Healthy: false, Message: "not mounted"},
	}

	healthy := jm5HealthSnapshot(status, true, true)
	if !healthy.Healthy || healthy.Components["nfs"] != "ok" {
		t.Fatalf("running server snapshot = %+v, want healthy NFS listener", healthy)
	}

	stopped := jm5HealthSnapshot(status, true, false)
	if stopped.Healthy || stopped.Components["nfs"] != "listener stopped" {
		t.Fatalf("stopped server snapshot = %+v, want degraded listener", stopped)
	}

	client := jm5HealthSnapshot(status, false, true)
	if client.Healthy || client.Components["nfs"] != "not mounted" {
		t.Fatalf("client-mode snapshot = %+v, want mount health preserved", client)
	}
}
