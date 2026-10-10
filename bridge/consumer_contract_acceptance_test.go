//go:build consumer_acceptance

package main

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// This gate runs the real consumer transport/decoder against the real provider
// handlers. It needs an explicitly built consumer probe, never the user's app,
// mount, preferences, or live storage. Build with scripts/build-consumer-contract-probe.sh.
func TestConsumerContractProductionClient(t *testing.T) {
	probe := os.Getenv("JM_CONSUMER_PROBE")
	if !filepath.IsAbs(probe) {
		t.Fatal("consumer acceptance requires absolute JM_CONSUMER_PROBE from scripts/build-consumer-contract-probe.sh")
	}
	defer seedControlPlane(t)()
	globalMu.Lock()
	oldSpool, oldDrainer, oldRC, oldPrefetcher := globalSpool, globalDrainer, globalRC, globalPrefetcher
	globalSpool, globalDrainer, globalRC, globalPrefetcher = nil, nil, nil, nil
	fuseRoot := globalFUSEPath
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalSpool, globalDrainer, globalRC, globalPrefetcher = oldSpool, oldDrainer, oldRC, oldPrefetcher
		globalMu.Unlock()
	}()
	fixture := filepath.Join(fuseRoot, "consumer-fixture")
	if err := os.MkdirAll(fixture, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture, "clip.txt"), []byte("consumer pin fixture"), 0600); err != nil {
		t.Fatal(err)
	}

	s := metrics.NewServer("127.0.0.1:0", metrics.NewRegistry())
	s.ExtraRoutes = map[string]http.HandlerFunc{
		"/whoami": handleWhoamiHTTP, "/lookup": handleLookupHTTP,
		"/residency": handleResidencyHTTP, "/derivatives": handleDerivativesHTTP,
		"/metadata": handleMetadataHTTP, "/cache-status": handleCacheStatusHTTP,
		"/spool": handleSpoolHTTP, "/pin": handlePinHTTP, "/unpin": handleUnpinHTTP,
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	// The consumer fix must not pass because the provider stopped enforcing
	// native intent. A browser-style headerless POST remains denied.
	t.Run("headerless-denied", func(t *testing.T) {
		client := &http.Client{Timeout: 2 * time.Second}
		for _, route := range []string{"/pin", "/unpin"} {
			resp, err := client.Post("http://"+s.Addr()+route+"?path=/Volumes/zpool/consumer-fixture", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("headerless %s status=%d, want 403", route, resp.StatusCode)
			}
		}
	})
	// Any admitted pin walk must stop using the temporary stores before restore.
	defer func() {
		deadline := time.Now().Add(5 * time.Second)
		for len(pin.ScanningRoots()) != 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if len(pin.ScanningRoots()) != 0 {
			t.Error("consumer pin walk did not settle")
		}
	}()
	for _, mode := range []string{"reads", "pin-unpin"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, probe, "http://"+s.Addr(), mode).CombinedOutput()
			if err != nil {
				t.Fatalf("consumer %s: %v\n%s", mode, err, out)
			}
			if !strings.Contains(string(out), "PASS "+mode+":") {
				t.Fatalf("consumer returned without a verdict: %s", out)
			}
			t.Log(strings.TrimSpace(string(out)))
		})
	}
}
