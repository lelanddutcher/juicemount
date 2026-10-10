package main

import (
	"io"
	"testing"
)

func TestGatewayRequiresExplicitTLSConfiguration(t *testing.T) {
	for _, args := range [][]string{nil, {"--unknown"}, {"unexpected"}, {"--state", "missing", "--volume", "media"}} {
		if err := run(args, io.Discard); err == nil {
			t.Fatalf("incomplete configuration accepted: %v", args)
		}
	}
}
