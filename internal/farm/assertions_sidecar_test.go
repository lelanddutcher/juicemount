package farm

import (
	"path/filepath"
	"testing"
)

// Encoder choice determines desired freshness codec, never the published token.
func TestProxyDesiredCodec(t *testing.T) {
	cases := []struct {
		vcodec    string
		wantCodec string
	}{
		{"libx264", "h264"},
		{"", "h264"},
		{"h264_nvenc", "h264"},
		{"hevc_nvenc", "hevc"},
		{"libx265", "hevc"},
		{"libaom-av1", "av1"},
	}
	for _, c := range cases {
		codec := proxyDesiredCodec(c.vcodec)
		if codec != c.wantCodec {
			t.Errorf("proxyDesiredCodec(%q) = %q, want %q", c.vcodec, codec, c.wantCodec)
		}
	}
}

// TestApplyAssertionLifecycle: the sidecar writer is LWW + merge-not-clobber, and
// a retract is recorded (value:null kept).
func TestApplyAssertionLifecycle(t *testing.T) {
	dir := t.TempDir()
	sc := filepath.Join(dir, "clip.mov.loupe.json")
	const ak, name = "xxh3:2e9cf3ae98300fda", "clip.mov"

	// 1) fresh accept → sidecar created.
	r, err := ApplyAssertion(sc, ak, name, SidecarAssertion{
		Namespace: "log_profile", Key: "value", Value: "clog3",
		AssertedBy: "ol:a", AssertedAt: "2026-06-27T18:40:00Z",
	})
	if err != nil || !r.Accepted {
		t.Fatalf("fresh: %+v err=%v", r, err)
	}
	got, ok, _ := ReadAssertionSidecar(sc)
	if !ok || got.AssetKey != ak || len(got.Assertions) != 1 {
		t.Fatalf("after fresh: %+v", got)
	}

	// 2) reject-stale → file unchanged, winner reported.
	r, _ = ApplyAssertion(sc, ak, name, SidecarAssertion{
		Namespace: "log_profile", Key: "value", Value: "slog3",
		AssertedBy: "ol:b", AssertedAt: "2026-06-27T10:00:00Z",
	})
	if r.Accepted || r.WinningAssertedAt != "2026-06-27T18:40:00Z" {
		t.Fatalf("stale: %+v, want rejected, winner 18:40", r)
	}
	got, _, _ = ReadAssertionSidecar(sc)
	if got.Assertions[0].Value != "clog3" {
		t.Fatalf("stale must not overwrite: %v", got.Assertions[0].Value)
	}

	// 3) merge a person namespace → both kept.
	_, _ = ApplyAssertion(sc, ak, name, SidecarAssertion{
		Namespace: "person", Key: "c_3f", Value: "Bob",
		AssertedBy: "ol:a", AssertedAt: "2026-06-27T18:41:12Z",
	})
	got, _, _ = ReadAssertionSidecar(sc)
	if len(got.Assertions) != 2 {
		t.Fatalf("after merge: %d triples, want 2", len(got.Assertions))
	}

	// 4) retract the log_profile (newer null) → kept as null, person intact.
	_, _ = ApplyAssertion(sc, ak, name, SidecarAssertion{
		Namespace: "log_profile", Key: "value", Value: nil,
		AssertedBy: "ol:a", AssertedAt: "2026-06-27T19:05:00Z",
	})
	got, _, _ = ReadAssertionSidecar(sc)
	if len(got.Assertions) != 2 {
		t.Fatalf("retract changed count: %d", len(got.Assertions))
	}
	for _, a := range got.Assertions {
		if a.Namespace == "log_profile" && a.Value != nil {
			t.Errorf("log_profile not retracted: %v", a.Value)
		}
	}
}

// TestReadAssertionSidecarAbsent: no file → (nil, false, nil).
func TestReadAssertionSidecarAbsent(t *testing.T) {
	_, ok, err := ReadAssertionSidecar(filepath.Join(t.TempDir(), "none.loupe.json"))
	if ok || err != nil {
		t.Fatalf("absent: ok=%v err=%v, want (false,nil)", ok, err)
	}
}
