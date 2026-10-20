package metrics

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestContinuityMetricsStopAndWaitJoinsOriginalServe(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewRegistry())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, worker := range server.serveDone {
		select {
		case <-worker:
		default:
			t.Fatal("StopAndWait returned before original Serve joined")
		}
	}
	select {
	case err := <-server.ServeErrors():
		t.Fatalf("normal stop reported unexpected failure: %v", err)
	default:
	}
}

func TestContinuityMetricsReportsEachBoundListenerFailure(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewRegistry())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	listeners := []interface{ Close() error }{server.listener}
	if server.listener6 != nil {
		listeners = append(listeners, server.listener6)
	}
	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-server.ServeErrors():
			if err == nil {
				t.Fatal("nil Serve failure")
			}
		case <-time.After(time.Second):
			t.Fatal("bound listener failure was discarded")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.StopAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, worker := range server.serveDone {
		select {
		case <-worker:
		default:
			t.Fatal("failed Serve worker not joined")
		}
	}
}

func TestContinuityMetricsFailedStartRetiresWithoutServing(t *testing.T) {
	server := NewServer("0.0.0.0:0", NewRegistry())
	if err := server.Start(); err == nil {
		t.Fatal("widened existing loopback boundary")
	}
	if err := server.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(server.serveDone) != 0 {
		t.Fatal("invalid address launched Serve")
	}
	if err := server.Start(); err == nil {
		t.Fatal("retired server restarted")
	}
}
