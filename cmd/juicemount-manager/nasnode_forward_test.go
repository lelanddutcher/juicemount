package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestNASRouteTCPHandlerScopesAndForwards(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	backendDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			backendDone <- acceptErr
			return
		}
		defer conn.Close()
		line, readErr := bufio.NewReader(conn).ReadString('\n')
		if readErr != nil {
			backendDone <- readErr
			return
		}
		_, writeErr := io.WriteString(conn, "echo:"+line)
		backendDone <- writeErr
	}()

	dialer := &net.Dialer{Timeout: time.Second}
	handlerFor := nasRouteTCPHandler(netip.MustParsePrefix("127.0.0.0/8"), dialer.DialContext)
	outside := netip.MustParseAddrPort("192.0.2.1:9000")
	if handler, intercept := handlerFor(netip.AddrPort{}, outside); intercept || handler != nil {
		t.Fatalf("outside route was intercepted: handler=%v intercept=%v", handler != nil, intercept)
	}

	backendAddr := listener.Addr().(*net.TCPAddr).AddrPort()
	handler, intercept := handlerFor(netip.AddrPort{}, backendAddr)
	if !intercept || handler == nil {
		t.Fatalf("inside route was not intercepted: handler=%v intercept=%v", handler != nil, intercept)
	}
	client, routed := net.Pipe()
	proxyDone := make(chan struct{})
	go func() {
		handler(routed)
		close(proxyDone)
	}()

	if _, err := io.WriteString(client, "juice\n"); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(client).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if want := "echo:juice\n"; got != want {
		t.Fatalf("forwarded response = %q, want %q", got, want)
	}
	_ = client.Close()

	select {
	case err := <-backendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend did not finish")
	}
	select {
	case <-proxyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("route proxy did not finish")
	}
}

func TestNASRouteTCPHandlerPropagatesDialFailure(t *testing.T) {
	wantErr := fmt.Errorf("backend down")
	dial := func(context.Context, string, string) (net.Conn, error) { return nil, wantErr }
	handlerFor := nasRouteTCPHandler(netip.MustParsePrefix("192.0.2.0/24"), dial)
	handler, intercept := handlerFor(netip.AddrPort{}, netip.MustParseAddrPort("192.0.2.5:6379"))
	if !intercept || handler == nil {
		t.Fatal("expected in-route handler")
	}
	client, routed := net.Pipe()
	done := make(chan struct{})
	go func() {
		handler(routed)
		close(done)
	}()
	defer client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dial failure did not terminate handler")
	}
}
