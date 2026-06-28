package farm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
)

// WaveformJSON is the de-facto-standard BBC audiowaveform / waveform-data.js
// shape (JM-18) — both OpenLoupe and an off-the-shelf web renderer parse it
// unchanged. `data` is flat interleaved min,max pairs per pixel (length*2*channels
// for channels=1 ⇒ length*2). bits=8 ⇒ values in [-128,127].
type WaveformJSON struct {
	Version         int   `json:"version"`
	Channels        int   `json:"channels"`
	SampleRate      int   `json:"sample_rate"`
	SamplesPerPixel int   `json:"samples_per_pixel"`
	Bits            int   `json:"bits"`
	Length          int   `json:"length"`
	Data            []int `json:"data"`
}

const waveformSampleRate = 48000

// Waveform decodes the first audio track to mono PCM and writes an 8-bit peak
// overview (min,max per pixel-bucket) as a BBC-format JSON blob. Streams the PCM
// so a feature-length file doesn't buffer in memory. Returns the pixel length.
func Waveform(ffmpegBin, srcPath, outPath string, samplesPerPixel int) (int, error) {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if samplesPerPixel <= 0 {
		samplesPerPixel = 1024
	}
	// Fold ALL audio streams + channels to mono (the silent-data-loss fix —
	// audioFoldArgs). -v error (not quiet) so a real decode failure surfaces in
	// stderr + trips the cmd.Wait() error path instead of a silent flatline.
	foldArgs, hasAudio, ferr := audioFoldArgs(ffmpegBin, srcPath, waveformSampleRate)
	if ferr != nil {
		return 0, fmt.Errorf("waveform: probe audio %q: %w", srcPath, ferr)
	}
	if !hasAudio {
		return 0, nil // no audio ⇒ no waveform
	}
	cmdArgs := append([]string{"-v", "error", "-i", srcPath}, foldArgs...)
	cmdArgs = append(cmdArgs, "-f", "s16le", "-")
	cmd := exec.Command(ffmpegBin, cmdArgs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}

	data := make([]int, 0, 4096)
	const i16Max, i16Min = int16(math.MaxInt16), int16(math.MinInt16)
	curMin, curMax := i16Max, i16Min
	inBucket := 0
	flush := func() {
		// scale int16 [-32768,32767] → 8-bit [-128,127]
		data = append(data, int(curMin)/256, int(curMax)/256)
		curMin, curMax = i16Max, i16Min
		inBucket = 0
	}

	buf := make([]byte, 64*1024)
	var carry []byte // a leftover odd byte between reads
	for {
		n, rerr := stdout.Read(buf)
		if n > 0 {
			b := buf[:n]
			if len(carry) > 0 {
				b = append(carry, b...)
				carry = nil
			}
			i := 0
			for ; i+1 < len(b); i += 2 {
				s := int16(binary.LittleEndian.Uint16(b[i : i+2]))
				if s < curMin {
					curMin = s
				}
				if s > curMax {
					curMax = s
				}
				inBucket++
				if inBucket >= samplesPerPixel {
					flush()
				}
			}
			if i < len(b) { // odd trailing byte → carry to next read
				carry = append(carry[:0], b[i:]...)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = cmd.Wait()
			return 0, fmt.Errorf("waveform read pcm %q: %w", srcPath, rerr)
		}
	}
	if inBucket > 0 { // partial final bucket
		flush()
	}
	if err := cmd.Wait(); err != nil {
		return 0, fmt.Errorf("ffmpeg waveform %q: %w", srcPath, err)
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("waveform %q: no audio samples", srcPath)
	}

	wf := WaveformJSON{
		Version: 2, Channels: 1, SampleRate: waveformSampleRate,
		SamplesPerPixel: samplesPerPixel, Bits: 8, Length: len(data) / 2, Data: data,
	}
	payload, err := json.Marshal(wf)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return 0, err
	}
	if err := atomicWriteFile(outPath, payload, 0o644); err != nil {
		return 0, err
	}
	return wf.Length, nil
}
