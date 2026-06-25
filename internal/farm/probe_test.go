package farm

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// decode helps build an ffProbe from inline JSON (the real ffprobe wire shape),
// so the mapping is tested against representative output, not Go structs.
func decode(t *testing.T, raw string) *ffProbe {
	t.Helper()
	var p ffProbe
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("decode ffprobe json: %v", err)
	}
	return &p
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// TestMapTechDiverse covers codec/color/audio variety the uniform BMD proxy set
// doesn't exercise: HDR10 (PQ), HLG, 8-bit H.264, ProRes w/ timecode, multichannel
// PCM, and missing color metadata. Each mapped tech is ALSO validated against the
// real contract schema (wrapped in the /metadata response shape).
func TestMapTechDiverse(t *testing.T) {
	sch := compileMetadataSchema(t)

	cases := []struct {
		name   string
		probe  string
		fbSize int64
		check  func(t *testing.T, tech *Tech)
	}{
		{
			name: "hevc_10bit_sdr",
			probe: `{"streams":[
				{"codec_type":"video","codec_name":"hevc","width":1080,"height":1920,"pix_fmt":"yuv420p10le","r_frame_rate":"60000/1001","color_primaries":"bt709","color_transfer":"bt709","color_space":"bt709","tags":{"timecode":"11:50:52;11"}},
				{"codec_type":"audio","codec_name":"aac","channels":1,"sample_rate":"48000","sample_fmt":"fltp"}
			],"format":{"format_name":"mov,mp4,m4a","duration":"5.781000","size":"4005679"}}`,
			check: func(t *testing.T, tech *Tech) {
				if tech.Container != "mov" || tech.DurationMS != 5781 || tech.SizeBytes != 4005679 {
					t.Errorf("container/dur/size = %q/%d/%d", tech.Container, tech.DurationMS, tech.SizeBytes)
				}
				v := tech.Video
				if v == nil || v.Codec != "hevc" || v.BitDepth != 10 || v.FPS != 59.94 {
					t.Errorf("video = %+v", v)
				}
				if v.HDR != nil {
					t.Errorf("bt709 must be SDR (hdr null), got %q", deref(v.HDR))
				}
				if deref(v.Timecode) != "11:50:52;11" {
					t.Errorf("timecode = %q", deref(v.Timecode))
				}
				if len(tech.Audio) != 1 || tech.Audio[0].BitDepth != nil {
					t.Errorf("audio = %+v (aac bit_depth must be null)", tech.Audio)
				}
			},
		},
		{
			name: "hdr10_pq_bt2020",
			probe: `{"streams":[
				{"codec_type":"video","codec_name":"hevc","width":3840,"height":2160,"pix_fmt":"yuv420p10le","r_frame_rate":"24000/1001","color_primaries":"bt2020","color_transfer":"smpte2084","color_space":"bt2020nc"}
			],"format":{"format_name":"mov","duration":"10.000000","size":"100"}}`,
			check: func(t *testing.T, tech *Tech) {
				if deref(tech.Video.HDR) != "hdr10" {
					t.Errorf("smpte2084 must be hdr10, got %q", deref(tech.Video.HDR))
				}
				if tech.Video.FPS != 23.976 {
					t.Errorf("fps = %v, want 23.976", tech.Video.FPS)
				}
			},
		},
		{
			name: "hlg",
			probe: `{"streams":[
				{"codec_type":"video","codec_name":"hevc","width":3840,"height":2160,"pix_fmt":"yuv420p10le","r_frame_rate":"50/1","color_primaries":"bt2020","color_transfer":"arib-std-b67","color_space":"bt2020nc"}
			],"format":{"format_name":"mov","duration":"4.0","size":"100"}}`,
			check: func(t *testing.T, tech *Tech) {
				if deref(tech.Video.HDR) != "hlg" {
					t.Errorf("arib-std-b67 must be hlg, got %q", deref(tech.Video.HDR))
				}
				if tech.Video.FPS != 50 {
					t.Errorf("fps = %v, want 50", tech.Video.FPS)
				}
			},
		},
		{
			name: "h264_8bit_no_color",
			probe: `{"streams":[
				{"codec_type":"video","codec_name":"h264","width":1920,"height":1080,"pix_fmt":"yuv420p","r_frame_rate":"30/1"},
				{"codec_type":"audio","codec_name":"aac","channels":2,"sample_rate":"44100","sample_fmt":"fltp"}
			],"format":{"format_name":"mp4","duration":"60.0","size":"5000000"}}`,
			check: func(t *testing.T, tech *Tech) {
				v := tech.Video
				if v.BitDepth != 8 {
					t.Errorf("yuv420p must be 8-bit, got %d", v.BitDepth)
				}
				if v.ColorPrimaries != nil || v.Transfer != nil || v.Matrix != nil || v.HDR != nil {
					t.Errorf("missing color must map to null, got %+v", v)
				}
				if tech.Audio[0].Channels != 2 {
					t.Errorf("audio channels = %d", tech.Audio[0].Channels)
				}
			},
		},
		{
			name: "prores_pcm24_multichannel",
			probe: `{"streams":[
				{"codec_type":"video","codec_name":"prores","width":4096,"height":2160,"pix_fmt":"yuv422p10le","r_frame_rate":"24/1","color_primaries":"bt709","color_transfer":"bt709","color_space":"bt709","tags":{"timecode":"01:00:00:00"}},
				{"codec_type":"audio","codec_name":"pcm_s24le","channels":8,"sample_rate":"48000","bits_per_raw_sample":"24","sample_fmt":"s32","tags":{"language":"eng"}}
			],"format":{"format_name":"mov","duration":"3.5","size":"900000000"}}`,
			check: func(t *testing.T, tech *Tech) {
				if tech.Video.BitDepth != 10 || tech.Video.Codec != "prores" {
					t.Errorf("video = %+v", tech.Video)
				}
				if deref(tech.Video.Timecode) != "01:00:00:00" {
					t.Errorf("timecode = %q", deref(tech.Video.Timecode))
				}
				a := tech.Audio[0]
				if a.Channels != 8 || a.BitDepth == nil || *a.BitDepth != 24 || deref(a.Language) != "eng" {
					t.Errorf("audio = %+v (want 8ch, 24-bit, eng)", a)
				}
			},
		},
		{
			name: "audio_only_wav",
			probe: `{"streams":[
				{"codec_type":"audio","codec_name":"pcm_s16le","channels":2,"sample_rate":"48000","bits_per_raw_sample":"16","sample_fmt":"s16","tags":{"language":"und"}}
			],"format":{"format_name":"wav","duration":"30.0","size":"5760000"}}`,
			check: func(t *testing.T, tech *Tech) {
				if tech.Video != nil {
					t.Errorf("audio-only must have null video, got %+v", tech.Video)
				}
				a := tech.Audio[0]
				if a.BitDepth == nil || *a.BitDepth != 16 {
					t.Errorf("pcm_s16 bit_depth = %v, want 16", a.BitDepth)
				}
				if a.Language != nil {
					t.Errorf("language 'und' must map to null, got %q", deref(a.Language))
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tech := mapTech(decode(t, tc.probe), tc.fbSize)
			tc.check(t, tech)

			// Validate against the real contract schema, wrapped in the
			// /metadata response shape the handler emits.
			payload, _ := json.Marshal(tech)
			hash := "deadbeefdeadbeef"
			resp := map[string]any{
				"inode": 1, "exists": true, "kind": "tech",
				"producer": "macos-node", "version": 1, "hash": hash,
				"tech": json.RawMessage(payload),
			}
			var doc any
			b, _ := json.Marshal(resp)
			_ = json.Unmarshal(b, &doc)
			if err := sch.Validate(doc); err != nil {
				t.Errorf("produced tech does not validate against metadata.schema.json:\n%v\ntech=%s", err, payload)
			}
		})
	}
}

func TestParseFPS(t *testing.T) {
	cases := map[string]float64{
		"60000/1001": 59.94, "24000/1001": 23.976, "30000/1001": 29.97,
		"24/1": 24, "25/1": 25, "50/1": 50, "0/0": 0, "": 0,
	}
	for in, want := range cases {
		if got := parseFPS(in); got != want {
			t.Errorf("parseFPS(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestBitDepthFromPixFmt(t *testing.T) {
	cases := map[string]int{
		"yuv420p10le": 10, "yuv422p10le": 10, "yuv444p12le": 12,
		"yuv420p": 8, "yuvj420p": 8, "rgb24": 8, "": 0,
		// semi-planar + packed high-bit-depth (review MEDIUM #2):
		"p010le": 10, "p016le": 16, "gray16le": 16, "gray12le": 12,
		"rgb48le": 16, "rgba64le": 16, "bgr48be": 16,
	}
	for in, want := range cases {
		if got := bitDepthFromPixFmt(in, ""); got != want {
			t.Errorf("bitDepthFromPixFmt(%q) = %d, want %d", in, got, want)
		}
	}
	// bits_per_raw_sample is authoritative when present (wins over pix_fmt).
	if got := bitDepthFromPixFmt("", "12"); got != 12 {
		t.Errorf("bits_per_raw_sample fallback = %d, want 12", got)
	}
	if got := bitDepthFromPixFmt("yuv420p", "10"); got != 10 {
		t.Errorf("bits_per_raw_sample should win over pix_fmt: got %d, want 10", got)
	}
}

func compileMetadataSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	// internal/farm → repo root → contract/spec/schema
	path := filepath.Join("..", "..", "contract", "spec", "schema", "metadata.schema.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var meta struct {
		ID string `json:"$id"`
	}
	_ = json.Unmarshal(b, &meta)
	id := meta.ID
	if id == "" {
		id = "metadata.schema.json"
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft2020
	if err := c.AddResource(id, bytes.NewReader(b)); err != nil {
		t.Fatalf("add schema: %v", err)
	}
	sch, err := c.Compile(id)
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	return sch
}
