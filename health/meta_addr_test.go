package health

import "testing"

// metaURLForMount is a DEV override used to point juicefs at the latency proxy
// (test/latency-proxy.py) so a cellular-RTT link can be reproduced on a LAN.
// It sits directly on the mount path, so the unset case must be provably inert.
func TestMetaURLPassthroughWhenUnset(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "")
	const in = "redis://192.168.0.197:30179/1"
	if got := MetaURLForMount(in); got != in {
		t.Fatalf("override unset but URL changed: %q -> %q; a debug knob must never "+
			"alter the production mount", in, got)
	}
}

func TestMetaURLRewritesHostOnly(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "127.0.0.1:16379")
	got := MetaURLForMount("redis://192.168.0.197:30179/1")
	if got != "redis://127.0.0.1:16379/1" {
		t.Fatalf("got %q, want redis://127.0.0.1:16379/1 — scheme and DB index must survive", got)
	}
}

// Credentials and DB index must survive the rewrite, or the mount silently
// authenticates wrong / lands in the wrong keyspace.
func TestMetaURLPreservesCredentialsAndDB(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "127.0.0.1:16379")
	got := MetaURLForMount("redis://user:secret@192.168.0.197:30179/3")
	if got != "redis://user:secret@127.0.0.1:16379/3" {
		t.Fatalf("got %q — credentials or DB index lost in rewrite", got)
	}
}

// A URL we cannot parse must be passed through untouched rather than mounted
// against a half-built address.
func TestMetaURLLeavesUnparseableAlone(t *testing.T) {
	t.Setenv("JM_DEBUG_META_ADDR", "127.0.0.1:16379")
	const junk = "not-a-url"
	if got := MetaURLForMount(junk); got != junk {
		t.Fatalf("unparseable URL was rewritten to %q", got)
	}
}
