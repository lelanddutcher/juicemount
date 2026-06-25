package farm

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// Proxy transcodes the source to the contract-locked OL-3 proxy: a SINGLE
// progressive MP4 with the moov atom first (-movflags +faststart) so both
// AVFoundation (local file + HTTP byte-range) and a browser <video> seek it
// natively over HTTP Range. The interchange-locked fields below MUST NOT vary —
// a farm proxy and OpenLoupe's local transcode are byte-interchangeable only
// because every client plays exactly this codec/container:
//
//	Container : MP4, single file, +faststart (moov before mdat)
//	Video     : H.264 (libx264), -pix_fmt yuv420p (8-bit 4:2:0), progressive
//	Audio     : AAC, -b:a 128k, 48 kHz, stereo
//	media_type: video/mp4
//
// CRF + preset are the farm's QUALITY knob (size/quality only, not
// playability/interchange): the farm encodes OFFLINE so it picks quality-oriented
// -crf 21 -preset slow per OL-3, NOT OpenLoupe's realtime fallback values. vcodec
// defaults to libx264 (CPU); pass a hardware encoder (h264_nvenc/qsv/vaapi) on a
// GPU/APU NAS — the locked container/pix_fmt/audio stay identical so the blob is
// still interchangeable. The HTTP Range/206 serving is a SEPARATE lane.
func Proxy(ffmpegBin, vcodec, srcPath, outPath string) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if vcodec == "" {
		vcodec = "libx264"
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	// -pix_fmt yuv420p forces 8-bit 4:2:0 from any source (10-bit/HDR/422
	// originals included), the lowest-common-denominator both decoders accept.
	cmd := exec.Command(ffmpegBin, "-y", "-loglevel", "error",
		"-i", srcPath,
		"-c:v", vcodec, "-pix_fmt", "yuv420p", "-crf", "21", "-preset", "slow",
		"-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2",
		"-movflags", "+faststart",
		outPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg proxy %q: %w: %s", srcPath, err, out)
	}
	return nil
}

// ProxyResult is the per-file outcome of a proxy-generation pass.
type ProxyResult struct {
	Path  string
	Inode uint64
	Hash  string
	Wrote bool
	Err   error
}

// GenerateProxy renders the OL-3 proxy for one file and commits a single `proxy`
// manifest row. It is a SEPARATE pass from Process() (which commits the fast
// tech/poster/filmstrip/waveform derivatives atomically): a -preset slow whole-
// clip transcode can take minutes, so decoupling it keeps the cheap derivatives
// publishing immediately instead of withholding them behind the proxy encode.
func GenerateProxy(store *derivatives.Store, path string, opt Options) ProxyResult {
	res := ProxyResult{Path: path}
	fi, err := os.Stat(path)
	if err != nil {
		res.Err = err
		return res
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		res.Err = fmt.Errorf("stat: no syscall.Stat_t for %q", path)
		return res
	}
	inode := uint64(st.Ino)
	res.Inode = inode

	hash, err := SampleHash(path, fi.Size())
	if err != nil {
		res.Err = fmt.Errorf("hash: %w", err)
		return res
	}
	res.Hash = hash

	tech, err := Probe(opt.FFprobeBin, path, fi.Size())
	if err != nil {
		res.Err = err
		return res
	}
	if tech.Video == nil {
		return res // audio-only / no video stream → no proxy (Wrote stays false)
	}

	rel := "proxy.mp4"
	mt := "video/mp4"
	out := filepath.Join(DerivBlobDir(opt.Mount, inode), rel)
	row := derivatives.DerivRow{
		Kind: "proxy", Producer: opt.Producer, Version: opt.Version,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
	}
	stampSource(&row, fi)
	if err := Proxy(opt.FFmpegBin, opt.ProxyVCodec, path, out); err != nil {
		// Non-fatal: publish a failed row so the consumer regenerates locally.
		res.Err = err
		row.Status = "failed"
	} else {
		res.Wrote = true
		row.Status = "ready"
	}
	if err := store.PutSource(inode, &hash); err != nil {
		res.Err = fmt.Errorf("put source: %w", err)
		return res
	}
	if err := store.PutDeriv(inode, row); err != nil {
		res.Err = fmt.Errorf("put proxy deriv: %w", err)
		return res
	}
	if opt.Mount != "" {
		_ = WriteManifestSidecar(store, opt.Mount, inode) // JM-15: best-effort
	}
	return res
}
