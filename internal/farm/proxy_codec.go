package farm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"time"
)

type proxyOutputMetadata struct{ Codec, CodecString string }

// The configuration belongs to the held output, never the requested encoder or
// a consumer-writable manifest. A failed inspection is a metadata error, not
// evidence that a previously qualified proxy should be encoded again.
func probeProxyOutputMetadata(ctx context.Context, ffprobeBin string, file *os.File) (proxyOutputMetadata, error) {
	if file == nil {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: nil file")
	}
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: not a regular file: %v", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return proxyOutputMetadata{}, err
	}
	if ffprobeBin == "" {
		ffprobeBin = "ffprobe"
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := commandContext(ctx, ffprobeBin, "-v", "error", "-show_data", "-show_entries",
		"stream=codec_type,codec_name,codec_tag_string,profile,extradata,color_primaries,color_transfer,color_space,color_range", "-of", "json", "/dev/fd/3")
	cmd.ExtraFiles = []*os.File{file}
	out, diagnostic := codecProbeBuffer{limit: 256 << 10}, codecProbeBuffer{limit: 8 << 10}
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	runErr := cmd.Run()
	if out.overflow || diagnostic.overflow {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: ffprobe output exceeds limit")
	}
	if runErr != nil {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata probe: %w: %s", runErr, diagnostic.buf.String())
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: held file changed during probe")
	}
	return parseProxyOutputMetadata(out.buf.Bytes())
}

type codecProbeBuffer struct {
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func (b *codecProbeBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.limit - b.buf.Len()
	if n > remaining {
		b.overflow = true
		p = p[:remaining]
	}
	_, _ = b.buf.Write(p)
	return n, nil
}

type codecProbeStream struct {
	Type      string `json:"codec_type"`
	Codec     string `json:"codec_name"`
	Tag       string `json:"codec_tag_string"`
	Profile   string `json:"profile"`
	Extra     string `json:"extradata"`
	Primaries string `json:"color_primaries"`
	Transfer  string `json:"color_transfer"`
	Matrix    string `json:"color_space"`
	Range     string `json:"color_range"`
}

func parseProxyOutputMetadata(raw []byte) (proxyOutputMetadata, error) {
	var doc struct {
		Streams []codecProbeStream `json:"streams"`
	}
	if len(raw) > 256<<10 {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: JSON exceeds limit")
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata JSON: %w", err)
	}
	if len(doc.Streams) > 16 {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: too many streams")
	}
	var result proxyOutputMetadata
	videos, audios := 0, 0
	for _, s := range doc.Streams {
		if s.Type == "data" {
			continue
		}
		extra, err := decodeProbeHex(s.Extra)
		if err != nil {
			return result, err
		}
		switch s.Type {
		case "video":
			videos++
			result.Codec = s.Codec
			switch s.Codec {
			case "h264":
				result.CodecString, err = avcCodecToken(s.Tag, extra)
			case "hevc":
				result.CodecString, err = hevcCodecToken(s.Tag, extra)
			case "av1":
				result.CodecString, err = av1CodecToken(s, extra)
			default:
				err = fmt.Errorf("unsupported video codec %q", s.Codec)
			}
		case "audio":
			audios++
			// Our MP4 output supports AAC-LC. An extension profile must not be
			// advertised as LC merely because its base AudioSpecificConfig is LC.
			if s.Codec != "aac" || s.Tag != "mp4a" || len(extra) < 2 || extra[0]>>3 != 2 || s.Profile != "LC" {
				err = fmt.Errorf("unsupported proxy audio configuration")
			}
		default:
			err = fmt.Errorf("unsupported proxy stream type %q", s.Type)
		}
		if err != nil {
			return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: %w", err)
		}
	}
	if videos != 1 || audios > 1 {
		return proxyOutputMetadata{}, fmt.Errorf("proxy metadata: ambiguous streams video=%d audio=%d", videos, audios)
	}
	if audios == 1 {
		result.CodecString += ", mp4a.40.2"
	}
	return result, nil
}

// ffprobe's -show_data is an offset-prefixed hexadecimal dump with an ASCII
// column. Decode only the hex column and require contiguous, bounded offsets.
func decodeProbeHex(dump string) ([]byte, error) {
	if len(dump) > 192<<10 {
		return nil, fmt.Errorf("proxy metadata: oversized configuration")
	}
	var data []byte
	for _, line := range strings.Split(dump, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		left, right, ok := strings.Cut(line, ": ")
		offset, err := strconv.ParseUint(left, 16, 32)
		if !ok || err != nil || offset != uint64(len(data)) {
			return nil, fmt.Errorf("proxy metadata: invalid configuration offset")
		}
		column, _, ok := strings.Cut(right, "  ")
		if !ok {
			return nil, fmt.Errorf("proxy metadata: missing hex column boundary")
		}
		encoded := strings.Join(strings.Fields(column), "")
		b, err := hex.DecodeString(encoded)
		if err != nil || len(b) == 0 || len(b) > 16 || len(data)+len(b) > 64<<10 {
			return nil, fmt.Errorf("proxy metadata: invalid configuration hex")
		}
		data = append(data, b...)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("proxy metadata: missing configuration")
	}
	return data, nil
}

// RFC 6381 section 3.3: sample entry plus the three AVC SPS bytes. Validate
// that every declared SPS agrees with avcC; profile names omit constraint bits.
func avcCodecToken(tag string, b []byte) (string, error) {
	if (tag != "avc1" && tag != "avc3") || len(b) < 7 || b[0] != 1 || b[4]&0xfc != 0xfc || b[4]&3 == 2 || b[5]&0xe0 != 0xe0 {
		return "", fmt.Errorf("invalid AVC configuration")
	}
	pos := 6
	readNALs := func(count, kind int) error {
		if count == 0 {
			return fmt.Errorf("missing AVC parameter sets")
		}
		for i := 0; i < count; i++ {
			if pos+2 > len(b) {
				return io.ErrUnexpectedEOF
			}
			n := int(binary.BigEndian.Uint16(b[pos:]))
			pos += 2
			if n == 0 || n > len(b)-pos || b[pos]&31 != byte(kind) {
				return fmt.Errorf("invalid AVC parameter set")
			}
			if kind == 7 && (n < 4 || !bytes.Equal(b[pos+1:pos+4], b[1:4])) {
				return fmt.Errorf("ambiguous AVC profile or level")
			}
			pos += n
		}
		return nil
	}
	if err := readNALs(int(b[5]&31), 7); err != nil {
		return "", err
	}
	if pos >= len(b) {
		return "", io.ErrUnexpectedEOF
	}
	count := int(b[pos])
	pos++
	if err := readNALs(count, 8); err != nil {
		return "", err
	}
	if pos < len(b) {
		switch b[1] {
		case 100, 110, 122, 144:
		default:
			return "", fmt.Errorf("unexpected AVC extension")
		}
		if len(b)-pos < 4 || b[pos]&0xfc != 0xfc || b[pos+1]&0xf8 != 0xf8 || b[pos+2]&0xf8 != 0xf8 {
			return "", fmt.Errorf("invalid AVC extension")
		}
		count = int(b[pos+3])
		pos += 4
		if count > 0 {
			if err := readNALs(count, 13); err != nil {
				return "", err
			}
		}
	}
	if pos != len(b) {
		return "", fmt.Errorf("trailing AVC configuration bytes")
	}
	return fmt.Sprintf("%s.%02x%02x%02x", tag, b[1], b[2], b[3]), nil
}

// ISO/IEC 14496-15 Annex E: profile space, reversed compatibility flags,
// tier/level and nonzero trailing constraint bytes are taken from hvcC.
func hevcCodecToken(tag string, b []byte) (string, error) {
	if (tag != "hvc1" && tag != "hev1") || len(b) < 23 || b[0] != 1 || b[21]&3 == 2 {
		return "", fmt.Errorf("invalid HEVC configuration")
	}
	pos := 23
	seen := map[byte]bool{}
	for a := 0; a < int(b[22]); a++ {
		if pos+3 > len(b) {
			return "", io.ErrUnexpectedEOF
		}
		kind := b[pos] & 63
		n := int(binary.BigEndian.Uint16(b[pos+1:]))
		pos += 3
		for i := 0; i < n; i++ {
			if pos+2 > len(b) {
				return "", io.ErrUnexpectedEOF
			}
			size := int(binary.BigEndian.Uint16(b[pos:]))
			pos += 2
			if size < 2 || size > len(b)-pos || (b[pos]>>1)&63 != kind {
				return "", fmt.Errorf("invalid HEVC parameter set")
			}
			seen[kind] = true
			pos += size
		}
	}
	if pos != len(b) || !seen[32] || !seen[33] || !seen[34] {
		return "", fmt.Errorf("incomplete HEVC parameter sets")
	}
	space := []string{"", "A", "B", "C"}[b[1]>>6]
	tier := "L"
	if b[1]&32 != 0 {
		tier = "H"
	}
	token := fmt.Sprintf("%s.%s%d.%X.%s%d", tag, space, b[1]&31, bits.Reverse32(binary.BigEndian.Uint32(b[2:6])), tier, b[12])
	end := 12
	for end > 6 && b[end-1] == 0 {
		end--
	}
	for _, v := range b[6:end] {
		token += fmt.Sprintf(".%X", v)
	}
	return token, nil
}

// AV1-ISOBMFF section 5. Include the optional color/chroma group together,
// rather than silently assigning BT.709 defaults to unspecified or HDR media.
func av1CodecToken(s codecProbeStream, b []byte) (string, error) {
	if s.Tag != "av01" || len(b) < 4 || b[0] != 0x81 || b[1]>>5 > 2 || b[1]&31 > 23 || b[3]&0xe0 != 0 {
		return "", fmt.Errorf("invalid AV1 configuration")
	}
	profile, level := b[1]>>5, b[1]&31
	high, twelve := b[2]&64 != 0, b[2]&32 != 0
	if (twelve && (!high || profile != 2)) || (level < 8 && b[2]&128 != 0) {
		return "", fmt.Errorf("invalid AV1 depth or tier")
	}
	depth := 8
	if high {
		depth = 10
	}
	if twelve {
		depth = 12
	}
	tier := "M"
	if b[2]&128 != 0 {
		tier = "H"
	}
	mono, x, y, position := (b[2]>>4)&1, (b[2]>>3)&1, (b[2]>>2)&1, b[2]&3
	if (profile == 1 && mono != 0) || ((x == 0 || y == 0) && position != 0) {
		return "", fmt.Errorf("invalid AV1 chroma")
	}
	primaries, ok1 := codecColorCode(s.Primaries, "primaries")
	transfer, ok2 := codecColorCode(s.Transfer, "transfer")
	matrix, ok3 := codecColorCode(s.Matrix, "matrix")
	if !ok1 || !ok2 || !ok3 {
		return "", fmt.Errorf("unknown AV1 color metadata")
	}
	full := 0
	switch s.Range {
	case "pc":
		full = 1
	case "tv":
	default:
		return "", fmt.Errorf("unknown AV1 range")
	}
	return fmt.Sprintf("av01.%d.%02d%s.%02d.%d.%d%d%d.%02d.%02d.%02d.%d", profile, level, tier, depth, mono, x, y, position, primaries, transfer, matrix, full), nil
}

func codecColorCode(name, kind string) (int, bool) {
	if name == "" || name == "unknown" || name == "unspecified" {
		return 2, true
	}
	values := map[string]int{"bt709": 1, "bt470m": 4, "bt470bg": 5, "smpte170m": 6, "smpte240m": 7, "film": 8, "bt2020": 9, "smpte428": 10, "smpte431": 11, "smpte432": 12, "jedec-p22": 22}
	if kind == "transfer" {
		values = map[string]int{"bt709": 1, "gamma22": 4, "gamma28": 5, "smpte170m": 6, "smpte240m": 7, "linear": 8, "log100": 9, "log316": 10, "iec61966-2-4": 11, "bt1361e": 12, "iec61966-2-1": 13, "bt2020-10": 14, "bt2020-12": 15, "smpte2084": 16, "smpte428": 17, "arib-std-b67": 18}
	}
	if kind == "matrix" {
		values = map[string]int{"gbr": 0, "bt709": 1, "fcc": 4, "bt470bg": 5, "smpte170m": 6, "smpte240m": 7, "ycgco": 8, "bt2020nc": 9, "bt2020c": 10, "smpte2085": 11, "chroma-derived-nc": 12, "chroma-derived-c": 13, "ictcp": 14}
	}
	v, ok := values[name]
	return v, ok
}
