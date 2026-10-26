package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// Only an incomplete discovery drain permits a retry while retaining the
// catalog. Terminal close errors are returned once for host reporting.
func closeManagerContinuity(closeService func(context.Context) error) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := closeService(ctx)
		cancel()
		if !errors.Is(err, catalog.ErrDiscoveryDraining) {
			return err
		}
		log.Print("continuity shutdown incomplete; retaining catalog until requests drain")
	}
}

// serveManagerHTTP owns the listener and joins its original Serve worker.
// Binding here exposes the actual address for --listen=127.0.0.1:0 without a
// probe/release/rebind race.
func serveManagerHTTP(server *http.Server, closeService func(context.Context) error, signals <-chan os.Signal, bind ...func(net.Listener) error) error {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return errors.Join(err, closeManagerContinuity(closeService))
	}
	if len(bind) != 0 && bind[0] != nil {
		if err := bind[0](listener); err != nil {
			return errors.Join(err, listener.Close(), closeManagerContinuity(closeService))
		}
	}
	log.Printf("manager HTTP listening on %s", listener.Addr())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	var serveErr error
	joined := false
	select {
	case <-signals:
	case serveErr = <-done:
		joined = true
	}
	closeErr := closeManagerContinuity(closeService)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownErr := server.Shutdown(ctx)
	cancel()
	if shutdownErr != nil {
		shutdownErr = errors.Join(shutdownErr, server.Close())
	}
	// Shutdown may race the goroutine's initial adoption of this listener.
	listenerErr := listener.Close()
	if errors.Is(listenerErr, net.ErrClosed) {
		listenerErr = nil
	}
	if !joined {
		serveErr = <-done
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, closeErr, shutdownErr, listenerErr)
}
