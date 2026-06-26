// Package farm is the derivative PRODUCER: it turns a media file into the
// machine-derived artifacts the Tier-B index (contract JM-14) serves —
// structured ffprobe `tech` metadata today, plus poster/filmstrip blobs.
//
// It is deliberately transport-agnostic and reusable: the same Probe/SampleHash/
// Thumbnail primitives back the jmfarm CLI tonight, the in-process JM-15 sync
// feed later, the Linux fast-lane (JM-16), AND the future JuiceMount web UI.
// Nothing here knows about NFS, the control plane, or OpenLoupe — it produces
// derivatives + writes them through a derivatives.Store.
package farm

import (
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

// Tech is the structured tech/EXIF payload — the `tech` object of
// contract/spec/schema/metadata.schema.json. Marshaled verbatim into the
// Tier-B `metadata` table and returned by GET /metadata?kind=tech.
type Tech struct {
	Container  string       `json:"container"`
	DurationMS int64        `json:"duration_ms"`
	SizeBytes  int64        `json:"size_bytes"`
	BitRate    int64        `json:"bit_rate,omitempty"` // overall container bitrate, bits/sec (format.bit_rate); 0/omitted when unknown
	Video      *VideoTrack  `json:"video"`
	Audio      []AudioTrack `json:"audio"`
}

// VideoTrack mirrors metadata.schema.json tech.video. Nullable fields are
// pointers so they serialize as JSON null (not "") when ffprobe omits them.
type VideoTrack struct {
	Codec          string  `json:"codec"`
	Width          int     `json:"width"`
	Height         int     `json:"height"`
	FPS            float64 `json:"fps"`
	PixFmt         string  `json:"pix_fmt"`
	BitDepth       int     `json:"bit_depth"`
	ColorPrimaries *string `json:"color_primaries"`
	Transfer       *string `json:"transfer"`
	Matrix         *string `json:"matrix"`
	HDR            *string `json:"hdr"`
	LogFormat      *string `json:"log_format"`
	Timecode       *string `json:"timecode"`
	BitRate        int64   `json:"bit_rate,omitempty"` // video-stream bitrate, bits/sec (stream.bit_rate); 0/omitted when unknown
}

// AudioTrack mirrors metadata.schema.json tech.audio[].
type AudioTrack struct {
	Codec      string  `json:"codec"`
	Channels   int     `json:"channels"`
	SampleRate int     `json:"sample_rate"`
	BitDepth   *int    `json:"bit_depth"`
	Language   *string `json:"language"`
}

// --- ffprobe JSON (loose; only the fields we map) ---

type ffProbe struct {
	Streams []ffStream `json:"streams"`
	Format  ffFormat   `json:"format"`
}

type ffStream struct {
	CodecType        string            `json:"codec_type"`
	CodecName        string            `json:"codec_name"`
	Width            int               `json:"width"`
	Height           int               `json:"height"`
	PixFmt           string            `json:"pix_fmt"`
	RFrameRate       string            `json:"r_frame_rate"`
	AvgFrameRate     string            `json:"avg_frame_rate"`
	ColorPrimaries   string            `json:"color_primaries"`
	ColorTransfer    string            `json:"color_transfer"`
	ColorSpace       string            `json:"color_space"`
	BitsPerRawSample string            `json:"bits_per_raw_sample"`
	SampleFmt        string            `json:"sample_fmt"`
	Channels         int               `json:"channels"`
	SampleRate       string            `json:"sample_rate"`
	Duration         string            `json:"duration"`
	BitRate          string            `json:"bit_rate"`
	Tags             map[string]string `json:"tags"`
}

type ffFormat struct {
	FormatName string            `json:"format_name"`
	Duration   string            `json:"duration"`
	Size       string            `json:"size"`
	BitRate    string            `json:"bit_rate"`
	Tags       map[string]string `json:"tags"`
}

// Probe runs ffprobe and maps its output to the contract `tech` shape. fallback
// size (the stat size) is used when the container omits format.size. Returns the
// payload ready to marshal, plus the raw Tech for callers that want fields
// (e.g. blob generation gated on video presence).
func Probe(ffprobeBin, path string, fallbackSize int64) (*Tech, error) {
	if ffprobeBin == "" {
		ffprobeBin = "ffprobe"
	}
	out, err := exec.Command(ffprobeBin, "-v", "quiet", "-print_format", "json",
		"-show_format", "-show_streams", path).Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe %q: %w", path, err)
	}
	var p ffProbe
	if err := json.Unmarshal(out, &p); err != nil {
		return nil, fmt.Errorf("ffprobe parse %q: %w", path, err)
	}
	return mapTech(&p, fallbackSize), nil
}

func mapTech(p *ffProbe, fallbackSize int64) *Tech {
	t := &Tech{
		Container:  firstToken(p.Format.FormatName),
		DurationMS: durationMS(p),
		SizeBytes:  parseInt(p.Format.Size, fallbackSize),
		BitRate:    parseInt(p.Format.BitRate, 0), // overall container bitrate, bits/sec
		Audio:      []AudioTrack{},
	}
	for i := range p.Streams {
		s := &p.Streams[i]
		switch s.CodecType {
		case "video":
			if t.Video == nil { // first video stream wins
				t.Video = mapVideo(s)
			}
		case "audio":
			t.Audio = append(t.Audio, mapAudio(s))
		}
	}
	// Prefer format.bit_rate; fall back to the video stream's when the container omits it.
	if t.BitRate == 0 && t.Video != nil {
		t.BitRate = t.Video.BitRate
	}
	return t
}

func mapVideo(s *ffStream) *VideoTrack {
	v := &VideoTrack{
		Codec:          s.CodecName,
		Width:          s.Width,
		Height:         s.Height,
		FPS:            parseFPS(s.RFrameRate, s.AvgFrameRate),
		PixFmt:         s.PixFmt,
		BitDepth:       bitDepthFromPixFmt(s.PixFmt, s.BitsPerRawSample),
		ColorPrimaries: nilIfEmpty(s.ColorPrimaries),
		Transfer:       nilIfEmpty(s.ColorTransfer),
		Matrix:         nilIfEmpty(s.ColorSpace),
		HDR:            deriveHDR(s.ColorTransfer, s.ColorPrimaries),
		LogFormat:      deriveLog(s),
		Timecode:       tag(s.Tags, "timecode"),
		BitRate:        parseInt(s.BitRate, 0), // video-stream bitrate, bits/sec
	}
	return v
}

func mapAudio(s *ffStream) AudioTrack {
	return AudioTrack{
		Codec:      s.CodecName,
		Channels:   s.Channels,
		SampleRate: int(parseInt(s.SampleRate, 0)),
		BitDepth:   audioBitDepth(s),
		Language:   audioLang(s.Tags),
	}
}

// --- field helpers ---

func firstToken(s string) string {
	if i := strings.IndexByte(s, ','); i >= 0 {
		return s[:i]
	}
	return s
}

func durationMS(p *ffProbe) int64 {
	if d := parseFloat(p.Format.Duration); d > 0 {
		return int64(math.Round(d * 1000))
	}
	for i := range p.Streams { // fall back to the first stream that has one
		if d := parseFloat(p.Streams[i].Duration); d > 0 {
			return int64(math.Round(d * 1000))
		}
	}
	return 0
}

// parseFPS turns ffprobe's "num/den" rational into a rounded fps. Prefers
// r_frame_rate (the container's nominal rate); falls back to avg_frame_rate.
func parseFPS(rates ...string) float64 {
	for _, r := range rates {
		n, d, ok := strings.Cut(r, "/")
		if !ok {
			if f := parseFloat(r); f > 0 {
				return round3(f)
			}
			continue
		}
		num, den := parseFloat(n), parseFloat(d)
		if num > 0 && den > 0 {
			return round3(num / den)
		}
	}
	return 0
}

// bitDepthFromPixFmt reads the per-component depth from the pixel format.
// bits_per_raw_sample wins when ffprobe provides it (authoritative, and set for
// most high-bit formats). Otherwise: planar/semi-planar carry the depth as a
// suffix (yuv420p10le→10, p010le→10, gray16le→16); packed RGB names TOTAL bits
// across channels (rgb48le→16/ch, rgba64le→16/ch); plain yuv420p→8.
func bitDepthFromPixFmt(pixFmt, bitsPerRaw string) int {
	if b := int(parseInt(bitsPerRaw, 0)); b > 0 {
		return b
	}
	for _, d := range []int{16, 14, 12, 10} {
		ds := strconv.Itoa(d)
		// yuv420p10le / yuv422p12le (planar) | p010le / p016le (semi-planar) | gray16le
		if strings.Contains(pixFmt, "p"+ds) || strings.HasPrefix(pixFmt, "p0"+ds) || strings.HasPrefix(pixFmt, "gray"+ds) {
			return d
		}
	}
	switch {
	case strings.Contains(pixFmt, "64"): // rgba64/bgra64 = 16 bits/channel
		return 16
	case strings.Contains(pixFmt, "48"): // rgb48/bgr48 = 16 bits/channel
		return 16
	}
	if pixFmt != "" {
		return 8 // a known pixel format with no depth marker is 8-bit
	}
	return 0
}

// deriveHDR classifies the transfer/primaries into an HDR label or null (SDR).
func deriveHDR(transfer, primaries string) *string {
	t := strings.ToLower(transfer)
	switch {
	case strings.Contains(t, "2084"): // smpte2084 / smptest2084 = PQ
		return strptr("hdr10")
	case strings.Contains(t, "arib-std-b67") || strings.Contains(t, "hlg"):
		return strptr("hlg")
	}
	return nil
}

// deriveLog detects a camera log curve from encoder/handler tags. ffprobe rarely
// exposes this, so it's best-effort and usually null.
func deriveLog(s *ffStream) *string {
	hay := strings.ToLower(s.Tags["encoder"] + " " + s.Tags["handler_name"])
	for _, lg := range []string{"slog3", "slog2", "logc4", "logc3", "logc", "vlog", "flog", "redlog", "log3g10", "canonlog3", "canonlog"} {
		if strings.Contains(hay, lg) {
			return strptr(lg)
		}
	}
	return nil
}

func audioBitDepth(s *ffStream) *int {
	if b := int(parseInt(s.BitsPerRawSample, 0)); b > 0 {
		return &b
	}
	switch s.SampleFmt { // PCM sample formats carry depth; compressed (aac) don't
	case "s16", "s16p":
		return intptr(16)
	case "s32", "s32p":
		return intptr(32)
	}
	return nil
}

func audioLang(tags map[string]string) *string {
	if tags == nil {
		return nil
	}
	l := tags["language"]
	if l == "" || l == "und" {
		return nil
	}
	return &l
}

func tag(tags map[string]string, key string) *string {
	if tags == nil {
		return nil
	}
	if v := tags[key]; v != "" {
		return &v
	}
	return nil
}

func nilIfEmpty(s string) *string {
	if s == "" || s == "unknown" {
		return nil
	}
	return &s
}

func parseFloat(s string) float64 { f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64); return f }

func parseInt(s string, fallback int64) int64 {
	if v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
		return v
	}
	return fallback
}

func round3(f float64) float64 { return math.Round(f*1000) / 1000 }

func strptr(s string) *string { return &s }
func intptr(i int) *int       { return &i }
