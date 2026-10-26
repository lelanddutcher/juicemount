package metrics

import (
	"context"
	"errors"
	"io"
	"net"
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

func TestContinuityDeliveryMetricsBindsBeforeServeAndJoinsGracefully(t *testing.T) {
	registry := NewRegistry()
	registry.SetHealthProvider(func() HealthSnapshot {
		return HealthSnapshot{Healthy: true, Components: map[string]string{"fixture": "ready"}}
	})
	server := NewServer("127.0.0.1:0", registry)
	var bound []string
	server.BindListeners = func(listeners ...net.Listener) error {
		if len(server.serveDone) != 0 || len(listeners) == 0 {
			t.Fatal("Serve started before listener authority was frozen")
		}
		for _, listener := range listeners {
			bound = append(bound, listener.Addr().String())
		}
		return nil
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.StopAndWait(ctx); err != nil {
			t.Errorf("metrics HTTP owner did not stop and join: %v", err)
		}
		for _, worker := range server.serveDone {
			select {
			case <-worker:
			default:
				t.Error("metrics original Serve worker remained after cleanup")
			}
		}
	})
	if len(bound) == 0 || bound[0] != server.Addr() {
		t.Fatal("bound listener authority differed from active address")
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("actual HTTP completion: %v %v %d", readErr, closeErr, response.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.ShutdownAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, worker := range server.serveDone {
		select {
		case <-worker:
		default:
			t.Fatal("original Serve was not joined")
		}
	}
}

func TestContinuityDeliveryMetricsBindFailureClosesAllBeforeServe(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewRegistry())
	refusal := errors.New("delivery authority refused")
	var listeners []net.Listener
	server.BindListeners = func(bound ...net.Listener) error { listeners = append(listeners, bound...); return refusal }
	if err := server.Start(); !errors.Is(err, refusal) {
		t.Fatalf("missing bind refusal: %v", err)
	}
	if len(server.serveDone) != 0 || len(listeners) == 0 {
		t.Fatal("failed bind started Serve or owned no listener")
	}
	for _, listener := range listeners {
		if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("failed bind retained listener: %v", err)
		}
	}
	if err := server.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestContinuityDeliveryMetricsForceCloseCannotProveGraceful(t *testing.T) {
	server := NewServer("127.0.0.1:0", NewRegistry())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if err := server.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.ShutdownAndWait(context.Background()); !errors.Is(err, errHTTPGracefulShutdownUnavailable) {
		t.Fatalf("forced Stop claimed graceful drain: %v", err)
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
