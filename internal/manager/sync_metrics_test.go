package manager

import (
	"net"
	"strings"
	"testing"
)

// The live failure this reproduces (2026-08-13): a 210 GB migration ran at
// ~475 MB/s while the UI showed 0 files / 0 bytes / 0% for its entire
// duration. `juicefs sync` was told --metrics 127.0.0.1:9567, but the manager
// container's own long-lived `juicefs mount` had defaulted to that same port
// 27 days earlier and owned it. Scrapes SUCCEEDED and returned the mount's
// metrics, which contain no juicefs_sync_* series, so the parser produced a
// zero event that matched the previous zero event and nothing was ever
// emitted.

// mountMetricsBody is a verbatim excerpt of what 127.0.0.1:9567 actually
// served during the incident — the juicefs MOUNT's Prometheus output. Real
// text rather than a hand-written stub, because the whole failure is that this
// body looks perfectly healthy: HTTP 200, well-formed, full of juicefs_*
// series. It just belongs to the wrong process.
const mountMetricsBody = `# HELP juicefs_blockcache_blocks number of cached blocks
juicefs_blockcache_blocks{instance="Leland-Mac.localdomain",vol_name="zpool"} 28961
juicefs_blockcache_bytes{instance="Leland-Mac.localdomain",vol_name="zpool"} 1.04945802139e+11
juicefs_blockcache_hits{instance="Leland-Mac.localdomain",vol_name="zpool"} 1.678833e+06
juicefs_blockcache_miss{instance="Leland-Mac.localdomain",vol_name="zpool"} 249999
juicefs_meta_ops_durations_histogram_seconds_count{vol_name="zpool"} 2.554298e+06
juicefs_object_request_data_bytes{method="GET",vol_name="zpool"} 8.80099929963e+11
juicefs_uptime{vol_name="zpool"} 2332800
`

// syncMetricsBody is the VERBATIM format juicefs 1.3.1 emits, captured from a
// live `juicefs sync --metrics` run on the NAS. The label block and the
// scientific-notation float are both load-bearing: an earlier draft of this
// test invented bare unlabelled lines, the regex correctly refused them, and
// the test failed. The parser was right; the guess was wrong.
const syncMetricsBody = `# HELP juicefs_sync_copied copied objects
juicefs_sync_copied{cmd="sync",pid="152196"} 4213
juicefs_sync_copied_bytes{cmd="sync",pid="152196"} 1.69629002e+11
juicefs_sync_cpu_usage{cmd="sync",pid="152196"} 0.555503
juicefs_sync_excluded{cmd="sync",pid="152196"} 0
juicefs_sync_failed{cmd="sync",pid="152196"} 2
`

// THE BUG. Scraping the mount must be reported as "no sync series here", not
// as "the sync has copied nothing".
func TestMountMetricsAreNotMistakenForSyncProgress(t *testing.T) {
	ev, sawSync := parseJuicefsMetrics([]byte(mountMetricsBody))
	if sawSync {
		t.Fatal("the juicefs MOUNT's metrics were accepted as sync progress")
	}
	if ev.Files != 0 || ev.Bytes != 0 {
		t.Errorf("parsed %d files / %d bytes out of mount metrics", ev.Files, ev.Bytes)
	}
	// The point of the second return: the counters ALONE cannot tell these
	// apart, which is why the collision was invisible for so long.
	zeroEv, zeroSaw := parseJuicefsMetrics([]byte(syncMetricsBodyAtZero))
	if !zeroSaw {
		t.Fatal("a real sync that has copied nothing yet must still report sawSync=true")
	}
	if zeroEv.Files != ev.Files || zeroEv.Bytes != ev.Bytes {
		t.Fatal("premise broken: the two bodies were supposed to be counter-identical")
	}
}

// A sync that is genuinely at zero looks identical in the counters to a wrong
// endpoint. Only sawSync separates them.
const syncMetricsBodyAtZero = `juicefs_sync_copied{cmd="sync",pid="99"} 0
juicefs_sync_copied_bytes{cmd="sync",pid="99"} 0
juicefs_sync_failed{cmd="sync",pid="99"} 0
`

func TestRealSyncMetricsParse(t *testing.T) {
	ev, sawSync := parseJuicefsMetrics([]byte(syncMetricsBody))
	if !sawSync {
		t.Fatal("real sync metrics reported sawSync=false")
	}
	if ev.Files != 4213 {
		t.Errorf("Files = %d, want 4213", ev.Files)
	}
	if ev.Bytes != 169629002000 {
		t.Errorf("Bytes = %d, want 169629002000", ev.Bytes)
	}
	if ev.Errors != 2 {
		t.Errorf("Errors = %d, want 2", ev.Errors)
	}
}

func TestEmptyBodyIsNotSyncProgress(t *testing.T) {
	for _, body := range []string{"", "# nothing here\n", "<html>404</html>"} {
		if _, sawSync := parseJuicefsMetrics([]byte(body)); sawSync {
			t.Errorf("body %q reported sawSync=true", body)
		}
	}
}

// freeMetricsAddr must hand back a port that is actually free, and must not
// hand back the same one twice in a row — the fixed 9567 is precisely what
// collided with the container's mount.
func TestFreeMetricsAddrIsUsableAndNotFixed(t *testing.T) {
	a := freeMetricsAddr()
	host, port, err := net.SplitHostPort(a)
	if err != nil {
		t.Fatalf("freeMetricsAddr returned %q, not host:port: %v", a, err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host = %q, want loopback — the metrics endpoint must not be reachable off-box", host)
	}
	if port == "0" {
		t.Error("port 0 was returned; juicefs would bind a random port we cannot then scrape")
	}
	// It must be bindable, i.e. genuinely free right now.
	l, err := net.Listen("tcp", a)
	if err != nil {
		t.Fatalf("address %q is not bindable: %v", a, err)
	}
	_ = l.Close()

	// Two consecutive calls should not both return the old hardcoded port.
	b := freeMetricsAddr()
	if strings.HasSuffix(a, ":9567") && strings.HasSuffix(b, ":9567") {
		t.Error("freeMetricsAddr returned the hardcoded 9567 twice — the fallback is being " +
			"used, which reintroduces the collision with the container's juicefs mount")
	}
}

// A held port must never be handed out, which is the entire difference from
// the old constant.
func TestFreeMetricsAddrAvoidsAPortInUse(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("cannot bind loopback")
	}
	defer held.Close()
	taken := held.Addr().String()

	for i := 0; i < 25; i++ {
		if got := freeMetricsAddr(); got == taken {
			t.Fatalf("freeMetricsAddr handed out %s while it was held open", taken)
		}
	}
}
