package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func TestContinuityManagerCloseRetriesOnlyIncomplete(t *testing.T) {
	terminal := errors.New("terminal close failure")
	calls := 0
	err := closeManagerContinuity(func(context.Context) error {
		calls++
		if calls == 1 {
			return errors.Join(catalog.ErrDiscoveryDraining, context.DeadlineExceeded)
		}
		return terminal
	})
	if !errors.Is(err, terminal) || calls != 2 {
		t.Fatalf("close disposition: %v calls=%d", err, calls)
	}
}

func TestContinuityManagerListenerFailureClosesOwner(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	terminal := errors.New("close failure")
	calls := 0
	err = serveManagerHTTP(&http.Server{Addr: listener.Addr().String()}, func(context.Context) error { calls++; return terminal }, nil)
	if err == nil || !errors.Is(err, terminal) || calls != 1 {
		t.Fatalf("listener failure dropped owner/error: %v calls=%d", err, calls)
	}
}

func TestContinuityDeliveryManagerBindFailureClosesActualListener(t *testing.T) {
	server := &http.Server{Addr: "127.0.0.1:0"}
	refusal := errors.New("delivery authority refused")
	var bound string
	closed := 0
	err := serveManagerHTTP(server, func(context.Context) error { closed++; return nil }, nil, func(listener net.Listener) error {
		bound = listener.Addr().String()
		if _, port, err := net.SplitHostPort(bound); err != nil || port == "0" {
			t.Fatalf("actual listener not bound: %q %v", bound, err)
		}
		return refusal
	})
	if !errors.Is(err, refusal) || closed != 1 || bound == "" {
		t.Fatalf("bind failure lost owner: %v closes=%d bound=%q", err, closed, bound)
	}
	listener, err := net.Listen("tcp", bound)
	if err != nil {
		t.Fatalf("failed delivery bind retained listener: %v", err)
	}
	listener.Close()
}

func TestContinuityManagerServeSignalJoinsOriginalWorker(t *testing.T) {
	address := make(chan string, 1)
	signals := make(chan os.Signal, 1)
	done := make(chan error, 1)
	calls := 0
	server := &http.Server{
		Addr:        "127.0.0.1:0",
		Handler:     http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ready") }),
		BaseContext: func(l net.Listener) context.Context { address <- l.Addr().String(); return context.Background() },
	}
	go func() { done <- serveManagerHTTP(server, func(context.Context) error { calls++; return nil }, signals) }()
	joined := false
	t.Cleanup(func() {
		if joined {
			return
		}
		select {
		case signals <- syscall.SIGTERM:
		default:
		}
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("original manager Serve did not join")
		}
	})
	var target string
	select {
	case target = <-address:
	case <-time.After(time.Second):
		t.Fatal("listener not adopted")
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + target)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "ready" {
		t.Fatal("real production Serve path did not answer")
	}
	signals <- syscall.SIGTERM
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("manager shutdown did not join")
	}
	if calls != 1 {
		t.Fatalf("owner close calls %d", calls)
	}
}
