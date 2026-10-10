package health

import (
	"path/filepath"
	"testing"
)

func TestRefreshNFSUpdatesCachedComponentAndOverallFromProbe(t *testing.T) {
	m := New(Config{}) // empty mount point is the checkNFS "not configured" healthy case
	t.Cleanup(func() { _ = m.rdb.Close() })

	m.status = HealthStatus{
		Redis:   ComponentStatus{Healthy: true, Message: "ok"},
		MinIO:   ComponentStatus{Healthy: true, Message: "ok"},
		FUSE:    ComponentStatus{Healthy: true, Message: "ok"},
		NFS:     ComponentStatus{Healthy: false, Message: nfsMsgNotMounted},
		Overall: false,
	}

	got := m.RefreshNFS()
	if !got.Healthy || got.Message != "not configured" {
		t.Fatalf("RefreshNFS probe = %+v, want healthy not-configured", got)
	}
	status := m.Status()
	if !status.NFS.Healthy || !status.Overall {
		t.Fatalf("cached status after RefreshNFS = %+v, want NFS+overall healthy", status)
	}
}

func TestRefreshNFSDoesNotPublishOptimisticMountedState(t *testing.T) {
	// An absent leaf proves ENOENT without requiring a packaged mount helper.
	m := New(Config{NFSMountPoint: filepath.Join(t.TempDir(), "absent-mount")})
	t.Cleanup(func() { _ = m.rdb.Close() })
	m.status = HealthStatus{
		Redis:   ComponentStatus{Healthy: true, Message: "ok"},
		MinIO:   ComponentStatus{Healthy: true, Message: "ok"},
		FUSE:    ComponentStatus{Healthy: true, Message: "ok"},
		NFS:     ComponentStatus{Healthy: true, Message: "ok"},
		Overall: true,
	}

	got := m.RefreshNFS()
	if got.Healthy || got.Message != nfsMsgNotMounted {
		t.Fatalf("RefreshNFS on absent path = %+v, want explicit not-mounted", got)
	}
	status := m.Status()
	if status.NFS.Healthy || status.Overall {
		t.Fatalf("cached status after absent RefreshNFS = %+v, want unhealthy", status)
	}
}
