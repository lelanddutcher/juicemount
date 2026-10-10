package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestH8BAppliedReceiptAuthority(t *testing.T) {
	for _, name := range []string{"legacy-signal", "unissued-applied", "current", "same-hash-new-nonce", "foreign-current", "stale-heartbeat", "future-heartbeat"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "state.json"), ProxyDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			write := func(n, text string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, n), []byte(text+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			old := requestLine(filepath.Join(dir, reloadRequestName))
			if len(strings.Fields(old)) != 3 {
				t.Fatal("fixture request lacks nonce")
			}
			write(reloadHeartbeatName, "1")
			switch name {
			case "legacy-signal":
				write(legacyReloadAckName, old)
			case "unissued-applied":
				write(reloadAckName, strings.Fields(old)[0]+" 1 0000000000000000")
			default:
				write(reloadAckName, old)
			}
			if name == "same-hash-new-nonce" {
				s.mu.Lock()
				err = s.writeProxyFilesLocked()
				s.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				latest := requestLine(filepath.Join(dir, reloadRequestName))
				if latest == old || strings.Fields(latest)[0] != strings.Fields(old)[0] {
					t.Fatal("fixture did not issue same hash with new nonce")
				}
			}
			if name == "foreign-current" {
				write(reloadRequestName, strings.Fields(old)[0]+" 2 1111111111111111")
			}
			if name == "stale-heartbeat" || name == "future-heartbeat" {
				offset := -time.Minute
				if name == "future-heartbeat" {
					offset = time.Minute
				}
				at := time.Now().Add(offset)
				if err := os.Chtimes(filepath.Join(dir, reloadHeartbeatName), at, at); err != nil {
					t.Fatal(err)
				}
			}
			snap := s.Snapshot()
			wantLive := name == "current" || name == "same-hash-new-nonce" || name == "foreign-current"
			if snap.ProxyLive != wantLive || snap.ReloadApplied != (name == "current") {
				t.Fatalf("live=%v applied=%v, want live=%v current=%v", snap.ProxyLive, snap.ReloadApplied, wantLive, name == "current")
			}
			if name == "legacy-signal" || name == "unissued-applied" {
				if snap.ReloadAcked != "" {
					t.Fatal("unvalidated receipt exposed as applied hash")
				}
			} else if snap.ReloadAcked != strings.Fields(old)[0] {
				t.Fatal("validated historical applied hash missing")
			}
			if name == "legacy-signal" && !strings.Contains(snap.ProxyProblem, "update the jm-respproxy image") {
				t.Fatalf("old signal-only image not diagnosed: %s", snap.ProxyProblem)
			}
		})
	}
}
