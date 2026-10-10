//go:build h7_acceptance && !serviceclient

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/testguard"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Run with explicitly built jmctl, juicemount and the production Swift bridge
// probe. No service registration, mount command, Redis connection or user cache
// is used. This proves native HTTP compatibility, not mounted/app packaging.
func TestLocalControlProductionClients(t *testing.T) {
	binaries := map[string]string{}
	for _, key := range []string{"JM_H7_JMCTL", "JM_H7_CLI", "JM_H7_SWIFT_PROBE"} {
		path := os.Getenv(key)
		if !filepath.IsAbs(path) {
			t.Fatalf("%s requires an absolute, freshly built binary path", key)
		}
		binaries[key] = path
	}
	t.Setenv(testguard.RefuseFUSEContentReadsEnv, "1")
	t.Setenv("JM_PIN_WARM_METADATA", "0")
	defer seedControlPlane(t)()
	spool, _ := shutdownTestDrainer(t) // Empty private spool; drainer never starts.
	rc, err := metadata.NewRedisClientDeferred("redis://127.0.0.1:1/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()
	globalMu.Lock()
	oldSpool, oldRC := globalSpool, globalRC
	globalSpool, globalRC = spool, rc
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalSpool, globalRC = oldSpool, oldRC
		globalMu.Unlock()
	}()
	selfTestMu.Lock()
	oldSelfTest := selfTestLast
	selfTestLast = SelfTestResult{}
	selfTestMu.Unlock()
	defer func() {
		selfTestMu.Lock()
		selfTestLast = oldSelfTest
		selfTestMu.Unlock()
	}()
	var mountCalls atomic.Int32
	withMountNowEnv(t, filepath.Join(t.TempDir(), "volume"),
		func(string) bool { return false },
		func(string, string) error {
			mountCalls.Add(1)
			return errors.New("injected mount refusal")
		},
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			s := metrics.NewServer("127.0.0.1:0", metrics.NewRegistry())
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			defer s.Stop()
			// The exact map used by NFSServerStart, registered after Listen.
			s.RegisterRoutes(controlPlaneRoutes())
			run := func(t *testing.T, key string, args ...string) string {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binaries[key], args...)
				cmd.Env = append(os.Environ(), "JUICEMOUNT_ADDR="+s.Addr())
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%s: %v\n%s", key, err, out)
				}
				return string(out)
			}
			t.Run("jmctl-sync", func(t *testing.T) {
				out := run(t, "JM_H7_JMCTL", "-addr", s.Addr(), "sync")
				if !strings.Contains(out, `"ok": true`) {
					t.Fatalf("sync not accepted: %s", out)
				}
			})
			t.Run("jmctl-offline", func(t *testing.T) {
				run(t, "JM_H7_JMCTL", "-addr", s.Addr(), "offline-on")
				if !pin.IsUserOffline() {
					t.Fatal("production CLI did not set offline state")
				}
				run(t, "JM_H7_JMCTL", "-addr", s.Addr(), "offline-off")
				if pin.IsUserOffline() {
					t.Fatal("production CLI did not clear offline state")
				}
			})
			t.Run("juicemount-offline", func(t *testing.T) {
				run(t, "JM_H7_CLI", "offline", "on")
				if !pin.IsUserOffline() {
					t.Fatal("production CLI did not set offline state")
				}
				run(t, "JM_H7_CLI", "offline", "off")
				if pin.IsUserOffline() {
					t.Fatal("production CLI did not clear offline state")
				}
			})
			t.Run("juicemount-pin-unpin", func(t *testing.T) {
				root := t.TempDir()
				file := filepath.Join(root, "clip.txt")
				if err := os.WriteFile(file, []byte("private pin fixture"), 0600); err != nil {
					t.Fatal(err)
				}
				run(t, "JM_H7_CLI", "pin", root)
				// Admission is asynchronous: wait for the walk to release the store
				// and check the resulting row, rather than accepting only HTTP 200.
				deadline := time.Now().Add(5 * time.Second)
				for len(pin.ScanningRoots()) != 0 && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if len(pin.ScanningRoots()) != 0 {
					t.Fatal("private pin walk did not settle")
				}
				if _, ok := globalPinStore.Get(file); !ok {
					t.Fatal("pin request did not populate the private store")
				}
				run(t, "JM_H7_CLI", "unpin", root)
				if _, ok := globalPinStore.Get(file); ok {
					t.Fatal("unpin request retained the pin row")
				}
			})
			t.Run("swift-controls", func(t *testing.T) {
				out := run(t, "JM_H7_SWIFT_PROBE", s.Addr())
				if !strings.Contains(out, "PASS native Swift controls: 7") || mountCalls.Load() != 1 {
					t.Fatalf("Swift controls incomplete, mount calls=%d: %s", mountCalls.Load(), out)
				}
			})
		})
}
