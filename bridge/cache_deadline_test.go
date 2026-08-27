package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/redis/go-redis/v9"
)

// TestCacheProbeHonorsContextBehindSilentLinkProxy reproduces the live route-
// loss failure: the loopback Link proxy accepts Redis TCP immediately, but its
// far-side route never returns a RESP reply. The cache probe's 200 ms context
// must win over the client's much longer ReadTimeout or an NFS READ blocks.
func TestCacheProbeHonorsContextBehindSilentLinkProxy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	opts := cacheRedisOptions(ln.Addr().String(), 0)
	if !opts.ContextTimeoutEnabled {
		t.Fatal("cache Redis client must honor caller context deadlines")
	}
	client := redis.NewClient(opts)
	defer client.Close()
	reader := cache.NewReader(t.TempDir(), cache.DefaultBlockSize, client)
	defer reader.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	var block [4096]byte
	_, _ = reader.ReadBlock(ctx, 123, 0, block[:])
	elapsed := time.Since(started)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("silent Link proxy held cache probe for %s; want caller deadline", elapsed)
	}

	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		t.Fatal("cache probe never reached the silent proxy")
	}
}
