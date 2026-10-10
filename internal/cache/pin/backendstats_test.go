package pin

import "testing"

// realDaemonBody is a verbatim excerpt of a LIVE juicefs 1.3.1 /metrics scrape
// taken from this machine on 2026-08-11 (mount /Users/…/.juicemount/
// fuse-internal, vol zpool). Using the real emitted format — labels, Prometheus
// float notation, the method="GET"/"PUT" split — rather than a hand-written
// ideal, because the label block and the exponent form are exactly what a
// naive parser gets wrong.
const realDaemonBody = `# HELP juicefs_blockcache_hits count of cached blocks
juicefs_blockcache_blocks{instance="Leland-Mac.localdomain",vol_name="zpool"} 28961
juicefs_blockcache_bytes{instance="Leland-Mac.localdomain",vol_name="zpool"} 1.04945802139e+11
juicefs_blockcache_hit_bytes{instance="Leland-Mac.localdomain",vol_name="zpool"} 1.408147061069e+12
juicefs_blockcache_hits{instance="Leland-Mac.localdomain",vol_name="zpool"} 1.678833e+06
juicefs_blockcache_miss{instance="Leland-Mac.localdomain",vol_name="zpool"} 249999
juicefs_blockcache_miss_bytes{instance="Leland-Mac.localdomain",vol_name="zpool"} 7.26141440176e+11
juicefs_meta_ops_durations_histogram_seconds_count{instance="Leland-Mac.localdomain",vol_name="zpool"} 2.554298e+06
juicefs_object_request_data_bytes{instance="Leland-Mac.localdomain",method="GET",storage_class="STANDARD",vol_name="zpool"} 8.80099929963e+11
juicefs_object_request_data_bytes{instance="Leland-Mac.localdomain",method="PUT",storage_class="STANDARD",vol_name="zpool"} 5.5826145909e+10
`

func TestParseBackendStatsAgainstRealDaemonOutput(t *testing.T) {
	got, ok := parseBackendStats([]byte(realDaemonBody))
	if !ok {
		t.Fatal("parseBackendStats returned ok=false on a real juicefs body")
	}
	for _, c := range []struct {
		name string
		got  int64
		want int64
	}{
		{"CacheHits", got.CacheHits, 1678833},
		{"CacheMiss", got.CacheMiss, 249999},
		{"CacheHitBytes", got.CacheHitBytes, 1408147061069},
		{"CacheMissBytes", got.CacheMissBytes, 726141440176},
		{"ObjectGetBytes", got.ObjectGetBytes, 880099929963},
		{"ObjectPutBytes", got.ObjectPutBytes, 55826145909},
		{"MetaOps", got.MetaOps, 2554298},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// The GET and PUT lines share one metric name and differ only by label. A
// parser that keyed on the name alone would silently report upload bytes as
// download bytes — on a cellular test that inverts the headline conclusion.
func TestGetAndPutAreNotConflated(t *testing.T) {
	got, _ := parseBackendStats([]byte(realDaemonBody))
	if got.ObjectGetBytes == got.ObjectPutBytes {
		t.Fatal("GET and PUT byte counts are identical — the method label was ignored")
	}
	if got.ObjectGetBytes < got.ObjectPutBytes {
		t.Errorf("GET (%d) < PUT (%d) on a read-heavy session — labels look swapped",
			got.ObjectGetBytes, got.ObjectPutBytes)
	}
}

// THE VALIDITY GATE. A body we cannot recognise must report unavailable, never
// all-zeros-with-ok. Zeros would read as "nothing crossed the link" — a perfect
// cache-hit session, the most flattering possible way to be wrong, and exactly
// the class of invalid measurement that produced five bogus numbers in one
// night. Silence beats a fabricated number.
func TestUnrecognisedBodyReportsUnavailableNotZeroes(t *testing.T) {
	for _, body := range []string{
		"",
		"# HELP something_else\nsomething_else 42\n",
		"<html>404 not found</html>",
		// A juicefs endpoint that exposes no cache counters at all (wrong
		// port: the sync-path daemon rather than the mount daemon).
		"juicefs_uptime{vol_name=\"zpool\"} 1234\n",
	} {
		got, ok := parseBackendStats([]byte(body))
		if ok {
			t.Errorf("body %q reported ok=true with %+v — a failed scrape must "+
				"be unavailable, not a zero-backend reading", body, got)
		}
	}
}

func TestPartialBodyStillReportsWhatItFound(t *testing.T) {
	// A juicefs version that renamed some counters should still yield the ones
	// it does emit, rather than discarding the whole scrape.
	body := `juicefs_blockcache_hits{vol_name="zpool"} 500
juicefs_blockcache_miss{vol_name="zpool"} 100
`
	got, ok := parseBackendStats([]byte(body))
	if !ok {
		t.Fatal("ok=false despite two recognised counters")
	}
	if got.CacheHits != 500 || got.CacheMiss != 100 {
		t.Errorf("got hits=%d miss=%d, want 500/100", got.CacheHits, got.CacheMiss)
	}
	if got.ObjectGetBytes != 0 {
		t.Errorf("ObjectGetBytes = %d from a body that had no such line", got.ObjectGetBytes)
	}
}

func TestNoAddrConfiguredReportsUnavailable(t *testing.T) {
	SetBlockCacheMetricsAddr("")
	if _, ok := BackendStatsSnapshot(); ok {
		t.Error("BackendStatsSnapshot reported available with no addr configured")
	}
}
