package nfs_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/nfs/helpers"
	"github.com/lelanddutcher/juicemount/internal/nfs/helpers/memfs"
)

func TestConnTracking(t *testing.T) {
	fs := memfs.New()
	handler := helpers.NewNullAuthHandler(fs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &nfs.Server{
		Handler: handler,
		Context: ctx,
	}

	// Listen on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := ln.Addr().String()

	// Start serving in background.
	go func() {
		_ = srv.Serve(ln)
	}()

	// Initially no active connections.
	if got := srv.ActiveConnections(); got != 0 {
		t.Fatalf("expected 0 active connections before connect, got %d", got)
	}

	// Connect a TCP client.
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	// Give the server goroutine a moment to accept and start serving.
	time.Sleep(100 * time.Millisecond)

	if got := srv.ActiveConnections(); got != 1 {
		t.Fatalf("expected 1 active connection after connect, got %d", got)
	}

	// Connect a second client.
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial second client: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if got := srv.ActiveConnections(); got != 2 {
		t.Fatalf("expected 2 active connections, got %d", got)
	}

	// Disconnect the first client.
	conn1.Close()
	time.Sleep(200 * time.Millisecond)

	if got := srv.ActiveConnections(); got != 1 {
		t.Fatalf("expected 1 active connection after first disconnect, got %d", got)
	}

	// Disconnect the second client.
	conn2.Close()
	time.Sleep(200 * time.Millisecond)

	if got := srv.ActiveConnections(); got != 0 {
		t.Fatalf("expected 0 active connections after all disconnects, got %d", got)
	}
}
