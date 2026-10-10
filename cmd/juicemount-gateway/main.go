// juicemount-gateway serves immutable upload custody over mutually authenticated
// TLS. It does not yet publish into JuiceFS or replace the legacy write path.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationbackend"
	"github.com/lelanddutcher/juicemount/internal/mutationgateway"
)

func run(args []string, out io.Writer) error {
	f := flag.NewFlagSet("juicemount-gateway", flag.ContinueOnError)
	f.SetOutput(out)
	state := f.String("state", "", "existing private durable local state directory")
	volume := f.String("volume", "", "fixed volume identifier")
	listen := f.String("listen", "127.0.0.1:9443", "dedicated TLS listen address")
	certPath := f.String("tls-cert", "", "server certificate PEM path")
	keyPath := f.String("tls-key", "", "server private key PEM path")
	caPath := f.String("client-ca", "", "trusted client CA PEM path")
	ttl := f.Duration("lease-ttl", time.Minute, "new-operation lease duration (maximum 5m)")
	sdkPath := f.String("sdk-config", "", "optional private configuration for a preexisting JuiceFS backend")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *state == "" || *volume == "" || *certPath == "" || *keyPath == "" || *caPath == "" {
		return errors.New("state, volume, tls-cert, tls-key and client-ca are required; positional arguments are forbidden")
	}
	cert, err := tls.LoadX509KeyPair(*certPath, *keyPath)
	if err != nil {
		return err
	}
	ca, err := os.ReadFile(*caPath)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return errors.New("client CA contains no certificate")
	}
	cfg := mutationgateway.Config{Directory: *state, Volume: *volume, LeaseTTL: *ttl}
	if *sdkPath != "" {
		cfg.SDK, err = mutationbackend.ReadSDKConfig(*sdkPath)
		if err != nil {
			return err
		}
	}
	g, err := mutationgateway.Open(cfg)
	if err != nil {
		return err
	}
	defer g.Close()
	srv, err := g.Server(cert, pool)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(ln, "", "") }()
	fmt.Fprintf(out, "custody gateway listening %s\n", ln.Addr())
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return errors.New("gateway stopped without a completed request drain")
		}
		return err
	case <-ctx.Done():
		// Keep exclusive custody until every admitted request finishes. HTTP
		// read/write deadlines bound slow peers; cancellation does not delete
		// stages or unlock an in-flight backend callback.
		if err := srv.Shutdown(context.Background()); err != nil {
			return err
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		if err := g.Close(); err != mutationauthority.ErrProcessLifetime {
			return err
		}
		// Only this exact retained-pin outcome is orderly. run is called once
		// by main, which immediately exits; SDK Close is not a worker join.
		return nil
	}
}

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "gateway:", err)
		os.Exit(1)
	}
}
