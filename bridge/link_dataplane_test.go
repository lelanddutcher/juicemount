package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
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
