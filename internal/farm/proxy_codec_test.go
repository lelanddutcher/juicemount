package farm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func proxyConfigFixture(codec string) codecProbeStream {
	var b []byte
	s := codecProbeStream{Type: "video", Codec: codec}
	switch codec {
	case "h264":
		s.Tag = "avc1"
		b = []byte{1, 100, 0, 12, 255, 225, 0, 4, 103, 100, 0, 12, 1, 0, 1, 104}
	case "hevc":
		s.Tag = "hvc1"
		b = make([]byte, 23)
		b[0] = 1
		b[1] = 1
		b[2] = 0x60
		b[6] = 0x90
		b[12] = 60
		b[13] = 0xf0
		b[15] = 0xfc
		b[16] = 0xfd
		b[17] = 0xf8
		b[18] = 0xf8
		b[21] = 3
		b[22] = 3
		for _, kind := range []byte{32, 33, 34} {
			b = append(b, kind, 0, 1, 0, 2, kind<<1, 1)
		}
	case "av1":
		s.Tag = "av01"
		s.Range = "tv"
		b = []byte{0x81, 0, 0x0d, 0}
	default:
		panic(codec)
	}
	s.Extra = proxyConfigDump(b)
	return s
}
func proxyConfigDump(b []byte) string {
	var out strings.Builder
	for offset := 0; offset < len(b); offset += 16 {
		end := offset + 16
		if end > len(b) {
			end = len(b)
		}
		fmt.Fprintf(&out, "\n%08x: %s  configuration", offset, hex.EncodeToString(b[offset:end]))
	}
	return out.String() + "\n"
}
func proxyProbeJSON(streams ...codecProbeStream) string {
	b, err := json.Marshal(struct {
		Streams []codecProbeStream `json:"streams"`
	}{streams})
	if err != nil {
		panic(err)
	}
	return string(b)
}
func proxyOutputProbeFixture(codec string) string { return proxyProbeJSON(proxyConfigFixture(codec)) }

func TestProxyOutputMetadataCodecConfigurations(t *testing.T) {
	for _, tc := range []struct{ name, codec, want string }{
		{"AVC", "h264", "avc1.64000c"},
		{"HEVC", "hevc", "hvc1.1.6.L60.90"},
		{"AV1", "av1", "av01.0.00M.08.0.111.02.02.02.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := proxyConfigFixture(tc.codec)
			got, err := parseProxyOutputMetadata([]byte(proxyProbeJSON(s)))
			if err != nil || got.Codec != tc.codec || got.CodecString != tc.want {
				t.Fatalf("metadata=%+v err=%v want=%s", got, err, tc.want)
			}
			audio := codecProbeStream{Type: "audio", Codec: "aac", Tag: "mp4a", Profile: "LC", Extra: proxyConfigDump([]byte{0x11, 0x90, 0x56, 0xe5, 0})}
			got, err = parseProxyOutputMetadata([]byte(proxyProbeJSON(audio, s)))
			if err != nil || got.CodecString != tc.want+", mp4a.40.2" {
				t.Fatalf("audio metadata=%+v err=%v", got, err)
			}
		})
	}
	t.Run("AVCConstraintBits", func(t *testing.T) {
		for _, v := range []struct {
			p, c, l byte
			want    string
		}{{66, 0xe0, 30, "avc1.42e01e"}, {77, 0x40, 31, "avc1.4d401f"}} {
			s := proxyConfigFixture("h264")
			b, _ := decodeProbeHex(s.Extra)
			b[1], b[2], b[3] = v.p, v.c, v.l
			b[9], b[10], b[11] = v.p, v.c, v.l
			s.Extra = proxyConfigDump(b)
			got, err := parseProxyOutputMetadata([]byte(proxyProbeJSON(s)))
			if err != nil || got.CodecString != v.want {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		}
	})
	t.Run("HEVCSpaceTierConstraints", func(t *testing.T) {
		s := proxyConfigFixture("hevc")
		s.Tag = "hev1"
		b, _ := decodeProbeHex(s.Extra)
		b[1] = 0x62
		b[6] = 0xb0
		b[8] = 1
		b[12] = 120
		s.Extra = proxyConfigDump(b)
		got, err := parseProxyOutputMetadata([]byte(proxyProbeJSON(s)))
		if err != nil || got.CodecString != "hev1.A2.6.H120.B0.0.1" {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
	t.Run("AV1HDR10", func(t *testing.T) {
		s := proxyConfigFixture("av1")
		s.Extra = proxyConfigDump([]byte{0x81, 4, 0x4e, 0})
		s.Primaries = "bt2020"
		s.Transfer = "smpte2084"
		s.Matrix = "bt2020nc"
		s.Range = "tv"
		got, err := parseProxyOutputMetadata([]byte(proxyProbeJSON(s)))
		if err != nil || got.CodecString != "av01.0.04M.10.0.112.09.16.09.0" {
			t.Fatalf("got=%+v err=%v", got, err)
		}
	})
}

func TestProxyOutputMetadataRejectsAmbiguousOrMalformed(t *testing.T) {
	good := proxyConfigFixture("h264")
	cases := map[string]string{"invalidJSON": "{", "noStreams": "{}", "duplicateVideo": proxyProbeJSON(good, good), "oversizeJSON": strings.Repeat(" ", 256<<10+1)}
	for _, codec := range []string{"h264", "hevc", "av1"} {
		s := proxyConfigFixture(codec)
		b, _ := decodeProbeHex(s.Extra)
		s.Extra = proxyConfigDump(b[:len(b)-1])
		cases[codec+"Truncated"] = proxyProbeJSON(s)
		s = proxyConfigFixture(codec)
		s.Tag = "wrong"
		cases[codec+"Tag"] = proxyProbeJSON(s)
	}
	s := proxyConfigFixture("av1")
	s.Range = ""
	cases["AV1MissingRange"] = proxyProbeJSON(s)
	s.Range = "unknown"
	cases["AV1UnknownRange"] = proxyProbeJSON(s)
	s = good
	s.Extra = strings.Replace(s.Extra, "00000000", "00000001", 1)
	cases["offsetGap"] = proxyProbeJSON(s)
	s = good
	s.Extra = "\n00000000: zz  invalid\n"
	cases["invalidHex"] = proxyProbeJSON(s)
	s = good
	b, _ := decodeProbeHex(s.Extra)
	b[10] = 0x40
	s.Extra = proxyConfigDump(b)
	cases["SPSDisagrees"] = proxyProbeJSON(s)
	for _, audio := range []codecProbeStream{
		{Type: "audio", Codec: "aac", Tag: "mp4a", Extra: proxyConfigDump([]byte{0x11, 0x90})},
		{Type: "audio", Codec: "aac", Tag: "mp4a", Profile: "HE-AAC", Extra: proxyConfigDump([]byte{0x11, 0x90})},
		{Type: "audio", Codec: "aac", Tag: "mp4a", Profile: "LC", Extra: proxyConfigDump([]byte{0x2b, 0x92})},
		{Type: "audio", Codec: "opus", Tag: "Opus", Extra: proxyConfigDump([]byte{1, 2})},
	} {
		cases["audio"+audio.Codec+audio.Profile+audio.Extra] = proxyProbeJSON(good, audio)
	}
	audio := codecProbeStream{Type: "audio", Codec: "aac", Tag: "mp4a", Profile: "LC", Extra: proxyConfigDump([]byte{0x11, 0x90})}
	cases["duplicateAudio"] = proxyProbeJSON(good, audio, audio)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if got, err := parseProxyOutputMetadata([]byte(raw)); err == nil {
				t.Fatalf("accepted %+v", got)
			}
		})
	}
}

func TestProxyOutputMetadataBoundsToolOutput(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "blob")
	if err := os.WriteFile(path, []byte("held"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	probe := filepath.Join(root, "ffprobe")
	if err := os.WriteFile(probe, []byte("#!/bin/sh\nexec /bin/dd if=/dev/zero bs=1024 count=257 2>/dev/null\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := probeProxyOutputMetadata(context.Background(), probe, f); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversized output: %v", err)
	}
}

func TestProxyOutputMetadataRealMedia(t *testing.T) {
	if os.Getenv("JM_PROXY_CODEC_REAL_MEDIA") != "1" {
		t.Skip("requires owned retained media and pinned ffprobe")
	}
	probe := os.Getenv("JM_FARM_CPU_MEDIA_FFPROBE")
	wantHash := os.Getenv("JM_FARM_CPU_MEDIA_FFPROBE_SHA256")
	binary, err := os.ReadFile(probe)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	if wantHash == "" || fmt.Sprintf("%x", digest) != wantHash {
		t.Fatal("ffprobe identity mismatch")
	}
	for _, tc := range []struct{ codec, want string }{{"H264", "avc1.64000c, mp4a.40.2"}, {"HEVC", "hvc1.1.6.L60.90, mp4a.40.2"}, {"AV1", "av01.0.00M.08.0.111.02.02.02.0, mp4a.40.2"}} {
		t.Run(tc.codec, func(t *testing.T) {
			path := os.Getenv("JM_PROXY_CODEC_" + tc.codec)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			got, err := probeProxyOutputMetadata(context.Background(), probe, f)
			if err != nil || got.CodecString != tc.want {
				t.Fatalf("actual media=%+v err=%v want=%s", got, err, tc.want)
			}
			after, err := os.ReadFile(path)
			if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
				t.Fatal("media changed during metadata probe")
			}
			t.Logf("codec=%s token=%s bytes=%d sha256=%x", got.Codec, got.CodecString, len(before), sha256.Sum256(before))
		})
	}
}
