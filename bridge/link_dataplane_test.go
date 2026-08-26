package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestLinkRedisDataPlaneBenchmarkRoundTrip(t *testing.T) {
	redisURL := os.Getenv("JM_TEST_REDIS")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:6379/15"
	}
	dialer := (&net.Dialer{Timeout: time.Second}).DialContext
	probeCtx, probeCancel := context.WithTimeout(context.Background(), time.Second)
	conn, err := dialer(probeCtx, "tcp", "127.0.0.1:6379")
	probeCancel()
	if err != nil {
		t.Skipf("local Redis integration fixture unavailable: %v", err)
	}
	_ = conn.Close()

	upload, download, err := linkRedisDataPlaneBenchmarkWithDial(redisURL, 256<<10, 5*time.Second, dialer)
	if err != nil {
		t.Fatal(err)
	}
	if upload <= 0 || download <= 0 {
		t.Fatalf("invalid throughput upload=%f download=%f", upload, download)
	}
}

func TestLinkRedisDataPlaneBenchmarkRejectsInvalidInput(t *testing.T) {
	dialer := (&net.Dialer{}).DialContext
	if _, _, err := linkRedisDataPlaneBenchmarkWithDial("redis://127.0.0.1:6379/15", 0, time.Second, dialer); err == nil {
		t.Fatal("zero-sized benchmark was accepted")
	}
	if _, _, err := linkRedisDataPlaneBenchmarkWithDial("redis://127.0.0.1:6379/15", 1, time.Second, nil); err == nil {
		t.Fatal("nil benchmark dialer was accepted")
	}
}

func TestLinkDataPlaneSampleSelectsCellularPolicyBeforeMount(t *testing.T) {
	if linkDataPlaneBenchmarkBytes < 256<<10 {
		t.Fatalf("startup benchmark %d bytes is below the wire-sample floor", linkDataPlaneBenchmarkBytes)
	}
	p := netprofile.New()
	p.ObserveRTT(300 * time.Millisecond)
	observeLinkDataPlaneDownload(p, linkDataPlaneBenchmarkBytes, 2.0)

	snap := p.Snapshot()
	if !snap.HaveRTT || !snap.HaveBW {
		t.Fatalf("startup Link sample did not fully measure profile: %+v", snap)
	}
	if snap.Class != netprofile.ClassMetered {
		t.Fatalf("2 Mbps / 300 ms Link class = %s, want metered", snap.Class)
	}
	if got := p.Readahead(); got.Enabled || got.Blocks != 1 || got.Workers != 1 {
		t.Fatalf("metered read-ahead = %+v, want disabled 1-block/1-worker", got)
	}
	if got := p.JuiceFS(); got.BufferSizeMB != 256 || got.Prefetch != 0 {
		t.Fatalf("metered JuiceFS policy = %+v, want 256 MiB/prefetch 0", got)
	}
}
