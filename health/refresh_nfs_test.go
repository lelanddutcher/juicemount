package health

import "testing"

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
	m := New(Config{NFSMountPoint: t.TempDir()})
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
		t.Fatalf("RefreshNFS on plain directory = %+v, want explicit not-mounted", got)
	}
	status := m.Status()
	if status.NFS.Healthy || status.Overall {
		t.Fatalf("cached status after absent RefreshNFS = %+v, want unhealthy", status)
	}
}
