package proxy

import "testing"

func TestDetectByExtension(t *testing.T) {
	cases := []struct {
		path string
		want Codec
	}{
		{"a.r3d", CodecR3D},
		{"FOO.R3D", CodecR3D},
		{"clip.ari", CodecARRI},
		{"clip.arx", CodecARRI},
		{"clip.arri", CodecARRI},
		{"clip.braw", CodecBRAW},
		{"clip.BRAW", CodecBRAW},
		{"footage.mxf", CodecXAVC},
		{"foo.mov", CodecUnknown},  // ambiguous; needs magic check
		{"foo.mp4", CodecUnknown},
		{"foo.png", CodecUnknown},
		{"foo", CodecUnknown},
		{"path/to/clip.r3d", CodecR3D},
		{"deep/nested/clip.BRAW", CodecBRAW},
		{"single.dng", CodecUnknown}, // single DNG handled by Quick Look
	}
	for _, c := range cases {
		got := DetectByExtension(c.path)
		if got != c.want {
			t.Errorf("DetectByExtension(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}

func TestCodecString(t *testing.T) {
	cases := map[Codec]string{
		CodecR3D:        "R3D",
		CodecARRI:       "ARRIRAW",
		CodecBRAW:       "BRAW",
		CodecProResRAW:  "ProResRAW",
		CodecXAVC:       "XAVC",
		CodecCinemaDNG:  "CinemaDNG",
		CodecUnknown:    "unknown",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Codec(%d).String() = %q, want %q", c, got, want)
		}
	}
}

func TestIsProxyable(t *testing.T) {
	if !CodecR3D.IsProxyable() {
		t.Error("R3D should be proxyable")
	}
	if CodecUnknown.IsProxyable() {
		t.Error("Unknown should NOT be proxyable")
	}
}

func TestContainsBytes(t *testing.T) {
	cases := []struct {
		hay, needle string
		want        bool
	}{
		{"hello world", "world", true},
		{"hello world", "xyz", false},
		{"", "x", false},
		{"x", "", false},
		{"abc", "abcd", false},
		{"abcd", "abcd", true},
		{"prefixaprn suffix", "aprn", true},
		{"prefixaprH suffix", "aprn", false},
	}
	for _, c := range cases {
		got := containsBytes([]byte(c.hay), []byte(c.needle))
		if got != c.want {
			t.Errorf("containsBytes(%q, %q) = %v, want %v", c.hay, c.needle, got, c.want)
		}
	}
}
