package fusecustody

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCapabilityProbeRequiresSingleCompleteFrameAndSuccessfulJoin(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"available", "printf '%s\\n' '{\"version\":1,\"session\":\"\",\"id\":0,\"state\":\"available\"}'", true},
		{"extra", "printf '%s\\n' '{\"version\":1,\"session\":\"\",\"id\":0,\"state\":\"available\"}' 'extra'", false},
		{"malformed", "printf 'oops\\n'", false},
		{"nonzero", "printf '%s\\n' '{\"version\":1,\"session\":\"\",\"id\":0,\"state\":\"available\"}'; exit 7", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "capability")
			if err := os.WriteFile(path, []byte("#!/bin/sh\n"+tc.body+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			p, err := StartCapability(path)
			if err != nil {
				t.Fatal(err)
			}
			err = p.Wait(time.Second)
			if (err == nil) != tc.want || !p.Settled() {
				t.Fatalf("capability result=%v settled=%t", err, p.Settled())
			}
		})
	}
}
