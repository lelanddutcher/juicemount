package farm

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// QL preview proxies (T1.1 / F4 — "spacebar plays BRAW/REDCODE").
//
// A Quick Look preview needs PLAYABLE bytes: a small H.264 MP4 the appex can
// hand to AVPlayer without ever decoding the camera-native source. The OL-3
// proxy (proxy.go) already exists but is a -preset slow WHOLE-CLIP transcode —
// minutes per clip, so it cannot back a "browse and spacebar" workflow. This
// pass renders a bounded, fast-encoded stand-in per clip alongside the poster:
//
//	Container : MP4, single file, +faststart (moov before mdat)
//	Video     : H.264 (libx264), -pix_fmt yuv420p, scaled into a fit box
//	            (default 960px), -preset veryfast -crf 26
//	Duration  : capped (default first 20s) — a preview, not an edit proxy
//	Audio     : AAC 96k, 48 kHz stereo
//	Blob      : qlpreview.mp4 in the asset's derivative dir, kind "qlpreview",
//	            media_type video/mp4
//
// The row carries only fields every blob kind may carry (hash, paths, media
// type, source vouch): sanitizeSidecarRow strips codec/geometry extras off any
// kind that does not own them, so setting them here would make the stored row
// and its reconciled sidecar row permanently disagree and re-publish forever.
//
// KNOWN LIMIT (honest): ffmpeg has no BRAW decoder at all, so Blackmagic RAW
// sources fail Probe/decode and publish a status:"failed" row — same honest
// state as any other undecodable source. R3D decodes via ffmpeg's r3d demuxer
// where the build supports JPEG-2000; failures are recorded, never silent.
const (
	QLPreviewKind     = "qlpreview"
	qlPreviewBlobName = "qlpreview.mp4"
	qlPreviewMedia    = "video/mp4"

	// Defaults for the encode box; both overridable via Options.
	defaultQLMaxDim     = 960
	defaultQLSeconds    = 20
	qlPreviewCRF        = 26
	qlPreviewX264Preset = "veryfast"
)

// QLResult is the per-file outcome of a QL-preview generation pass.
type QLResult struct {
	Path  string
	Inode uint64
	Hash  string
	Wrote bool
	// SkippedFresh reports that a READY qlpreview row already vouches for this
	// exact source size — the pass is idempotent and sweeps re-run it freely.
	SkippedFresh bool
	Err          error
}

// QLBlobName exposes the reserved on-disk blob name for kind "qlpreview".
func QLBlobName() string { return qlPreviewBlobName }

// buildQLArgs assembles the ffmpeg argument vector for one QL preview encode.
// Split out from QLPreview so the arg contract is unit-testable without media.
func buildQLArgs(srcPath, outPath string, maxDim, maxSeconds int) []string {
	if maxDim <= 0 {
		maxDim = defaultQLMaxDim
	}
	if maxSeconds <= 0 {
		maxSeconds = defaultQLSeconds
	}
	scale := fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease", maxDim, maxDim)
	args := append([]string{"-y", "-loglevel", "error"}, proxyThreadArgs()...)
	// Duration cap before output; scale keeps aspect; yuv420p is the decode
	// floor (10-bit/HDR/422 originals included); +faststart so AVPlayer can
	// start playback from a byte-range fetch of the head of the file. Force MP4
	// so container selection never depends on the caller's scratch filename.
	return append(args,
		"-t", strconv.Itoa(maxSeconds),
		"-i", srcPath,
		"-vf", scale,
		"-c:v", "libx264", "-preset", qlPreviewX264Preset, "-crf", strconv.Itoa(qlPreviewCRF),
		// PROXY_CODEC_SPEC decode floor invariants (ratified 2026-06-27):
		// High@8-bit 4:2:0, BT.709 SDR tags, CFR, closed GOP — identical
		// discipline to kind:"proxy" so every consumer that plays a proxy
		// plays a preview.
		"-profile:v", "high",
		"-pix_fmt", "yuv420p",
		"-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
		"-fps_mode", "cfr",
		"-x264-params", "no-open-gop=1:scenecut=40",
		"-c:a", "aac", "-profile:a", "aac_low", "-b:a", "128k", "-ar", "48000", "-ac", "2",
		"-movflags", "+faststart",
		"-f", "mp4",
		outPath,
	)
}

// QLPreview encodes one clip's QL preview MP4 to outPath. The caller stages
// outPath inside the held derivative-directory descriptor (O_EXCL|O_NOFOLLOW),
// exactly like Thumbnail/Proxy — this function adds no atomicity of its own.
func QLPreview(ffmpegBin, srcPath, outPath string, maxDim, maxSeconds int) error {
	return QLPreviewContext(context.Background(), ffmpegBin, srcPath, outPath, maxDim, maxSeconds)
}

func QLPreviewContext(ctx context.Context, ffmpegBin, srcPath, outPath string, maxDim, maxSeconds int) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	cmd := commandContext(ctx, ffmpegBin, buildQLArgs(srcPath, outPath, maxDim, maxSeconds)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg qlpreview %q: %w: %s", srcPath, err, out)
	}
	return nil
}

// GenerateQLPreview renders the QL preview for one file and commits a single
// kind="qlpreview" manifest row. Its OWN entry point (like GenerateProxy), so
// it runs as a separate mutually-exclusive pass and never blocks the cheap
// tech/poster/filmstrip/waveform derivatives behind an encode.
func GenerateQLPreview(store *derivatives.Store, path string, opt Options) QLResult {
	res := QLResult{Path: path}
	ctx := optionContext(opt)
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}
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
	size := fi.Size()

	hash, err := SampleHash(path, size)
	if err != nil {
		res.Err = fmt.Errorf("hash: %w", err)
		return res
	}
	res.Hash = hash
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}

	if opt.Mount == "" {
		res.Err = fmt.Errorf("qlpreview requires Options.Mount")
		return res
	}

	// Idempotence gate: a READY row vouching for this exact source size means
	// the work is done. Size is checked against the row's stamped vouch, and
	// the blob itself must still be on the volume — the index outliving the
	// data is a known failure mode (see skipIfFresh), so verify the bytes too.
	rows, mErr := store.Manifest(inode)
	if mErr == nil {
		for _, d := range rows {
			if d.Kind == QLPreviewKind && d.Status == "ready" &&
				d.SourceSize != nil && *d.SourceSize == size &&
				opt.Mount != "" {
				if _, serr := derivatives.StatRegularUnder(opt.Mount,
					derivatives.DerivBlobRel(inode, qlPreviewBlobName)); serr == nil {
					res.SkippedFresh = true
					return res
				}
			}
		}
	}

	tech, err := ProbeContext(ctx, opt.FFprobeBin, path, size)
	if err != nil {
		res.Err = err
		return res
	}
	if tech.Video == nil {
		return res // audio-only → no video preview (Wrote stays false)
	}

	maxDim := opt.QLMaxDim
	if maxDim <= 0 {
		maxDim = defaultQLMaxDim
	}
	maxSec := opt.QLSeconds // <=0 → buildQLArgs applies the default

	derivDir, ddErr := derivDirFor(opt.Mount, inode)
	if ddErr != nil {
		res.Err = fmt.Errorf("derivative dir: %w", ddErr)
		return res
	}
	defer derivDir.Close()
	scratch, err := createEncodeScratch(opt, "qlpreview")
	if err != nil {
		res.Err = fmt.Errorf("create local qlpreview scratch: %w", err)
		return res
	}
	scratchPath := scratch.Name()
	if err := scratch.Close(); err != nil {
		_ = os.Remove(scratchPath)
		res.Err = fmt.Errorf("close local qlpreview scratch: %w", err)
		return res
	}
	defer os.Remove(scratchPath)

	rel := qlPreviewBlobName
	mt := qlPreviewMedia
	row := derivatives.DerivRow{
		Kind: QLPreviewKind, Producer: opt.Producer, Version: opt.Version,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
	}
	stampSource(&row, fi)

	err = QLPreviewContext(ctx, opt.FFmpegBin, path, scratchPath, maxDim, maxSec)
	if ctxErr := ctx.Err(); ctxErr != nil {
		res.Err = ctxErr
		return res
	}
	staged := ""
	if err == nil {
		local, openErr := os.Open(scratchPath)
		if openErr != nil {
			err = fmt.Errorf("open completed local qlpreview: %w", openErr)
		} else {
			staged, err = derivatives.StageReaderAt(derivDir, qlPreviewBlobName, local, 0o644)
			_ = local.Close()
			if err != nil {
				err = fmt.Errorf("copy completed qlpreview into shared storage: %w", err)
			}
		}
	}
	if err == nil {
		err = derivatives.CommitStagedAt(derivDir, staged, qlPreviewBlobName)
	}
	if err != nil {
		if staged != "" {
			derivatives.DiscardStagedAt(derivDir, staged)
		}
		// Non-fatal: publish a failed row so consumers know the artifact will
		// never appear (BRAW under stock ffmpeg lands here, deliberately).
		res.Err = err
		row.Status = "failed"
	} else {
		res.Wrote = true
		row.Status = "ready"
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		res.Err = ctxErr
		return res
	}

	if err := store.PutSource(inode, &hash); err != nil {
		res.Err = fmt.Errorf("put source: %w", err)
		return res
	}
	if err := store.PutDeriv(inode, row); err != nil {
		res.Err = fmt.Errorf("put qlpreview deriv: %w", err)
		return res
	}
	_ = WriteManifestSidecar(store, opt.Mount, inode) // JM-15: best-effort
	return res
}
