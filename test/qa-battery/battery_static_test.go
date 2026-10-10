package qa_battery

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A cumulative `.failed` metric is historical telemetry, not current broken
// work. Gating the release battery on it made every later run fail forever after
// one recovered or deliberately-declined row. Keep shell assertions on the
// actionable `failed_files` view exposed by qa_spool_actionable_failed.
func TestBatteryNeverGatesOnCumulativeFailedMetric(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	badCall := regexp.MustCompile(`qa_spool_field[[:space:]]+failed[)]`)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sh" {
			continue
		}
		body, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		if badCall.Match(body) {
			t.Errorf("%s gates on cumulative .failed; use qa_spool_actionable_failed", entry.Name())
		}
	}
}

// Drain responsiveness is a release gate, so the default orchestrator must run
// it rather than relying on a human to remember a separate script invocation.
func TestDefaultBatteryIncludesDrainLatencyGate(t *testing.T) {
	body, err := os.ReadFile("run-all.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "11-drain-latency") {
		t.Fatal("run-all.sh omits the required 11-drain-latency category")
	}
}
