package client

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

// The GUI (Swift) speaks the status channel without this package. Its copies
// of the wire constants live in Core/UnmountHelperService.swift; pin them to
// the Go definitions so the two sides cannot drift.
func TestSwiftStatusClientMatchesWire(t *testing.T) {
	path := filepath.Join("..", "..", "..", "app", "JuiceMount", "Sources", "JuiceMount", "Core", "UnmountHelperService.swift")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Swift client: %v", err)
	}
	src := string(b)
	raw := func(name string) string {
		t.Helper()
		m := regexp.MustCompile(`static let ` + name + ` =\s*#"(.*)"#`).FindStringSubmatch(src)
		if m == nil {
			t.Fatalf("Swift constant %s (raw string) not found", name)
		}
		return m[1]
	}
	plain := func(name string) string {
		t.Helper()
		m := regexp.MustCompile(`static let ` + name + ` = "([^"]*)"`).FindStringSubmatch(src)
		if m == nil {
			t.Fatalf("Swift constant %s not found", name)
		}
		return m[1]
	}
	if got, want := raw("daemonRequirement"), DaemonRequirement(); got != want {
		t.Errorf("Swift daemonRequirement drifted from DaemonRequirement():\n swift: %s\n    go: %s", got, want)
	}
	if got, want := raw("appDeveloperIDRequirement"), developerIDClauses; got != want {
		t.Errorf("Swift appDeveloperIDRequirement drifted from developerIDClauses:\n swift: %s\n    go: %s", got, want)
	}
	if got := plain("statusMachService"); got != StatusMachService {
		t.Errorf("Swift statusMachService %q, want %q", got, StatusMachService)
	}
	if got := plain("keyVersion"); got != KeyVersion {
		t.Errorf("Swift keyVersion %q, want %q", got, KeyVersion)
	}
	if got := plain("keyBody"); got != KeyBody {
		t.Errorf("Swift keyBody %q, want %q", got, KeyBody)
	}
	m := regexp.MustCompile(`static let protocolVersion: Int64 = (\d+)`).FindStringSubmatch(src)
	if m == nil {
		t.Fatal("Swift protocolVersion not found")
	}
	if v, _ := strconv.Atoi(m[1]); v != ProtocolVersion {
		t.Errorf("Swift protocolVersion %d, want %d", v, ProtocolVersion)
	}
	// The Swift ping request must be a valid v1 ping for the daemon.
	pm := regexp.MustCompile(`static let pingBody = Data\(#"(.*)"#\.utf8\)`).FindStringSubmatch(src)
	if pm == nil {
		t.Fatal("Swift pingBody not found")
	}
	req, err := DecodeRequest(ProtocolVersion, []byte(pm[1]))
	if err != nil || req.Op != OpPing {
		t.Errorf("Swift ping body %q does not decode as a ping: %v", pm[1], err)
	}
	// And the role the GUI requires in the reply is the one the status
	// listener admits: the GUI must never accept a "service" admission.
	if !regexp.MustCompile(`guard p\.role == "gui" else`).MatchString(src) {
		t.Error(`Swift ping verdict no longer requires role "gui"`)
	}
}
