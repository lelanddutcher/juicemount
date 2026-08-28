package farm

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// proxyEncodeArgs returns the video-encoder portion of the ffmpeg command for
// a contract-locked proxy. Encoder families require materially different
// options: passing the software CRF/preset pair to VAAPI is a hard ffmpeg error
// and VAAPI needs frames uploaded to the DRM device before it can encode.
//
// H.264 remains the default floor. HEVC is an explicit per-worker/per-job
// choice; proxyCodecStrings records the resulting codec honestly in the
// derivative row.
func proxyEncodeArgs(vcodec string, crf int, preset string) []string {
	switch {
	case strings.HasSuffix(vcodec, "_vaapi"):
		if crf <= 0 {
			crf = 23
		}
		return []string{
			"-vf", "scale_vaapi=format=nv12",
			"-c:v", vcodec,
			"-qp", strconv.Itoa(crf),
			"-compression_level", "3",
			"-bf", "2",
		}
	case strings.HasSuffix(vcodec, "_qsv"):
		if crf <= 0 {
			crf = 23
		}
		if preset == "" {
			preset = "veryfast"
		}
		return []string{
			"-vf", "scale_qsv=format=nv12",
			"-c:v", vcodec,
			"-global_quality", strconv.Itoa(crf),
			"-preset", preset,
		}
	case strings.HasSuffix(vcodec, "_nvenc"):
		// NVENC's constant-quality switch is -cq, not the software
		// encoders' -crf. Keep the public Farm quality control as CRF-like
		// 1-51, but translate it at the encoder boundary.
		return []string{
			"-c:v", vcodec,
			"-cq", strconv.Itoa(crf),
			"-preset", preset,
		}
	default:
		return []string{
			"-c:v", vcodec,
			"-pix_fmt", "yuv420p",
			"-crf", strconv.Itoa(crf),
			"-preset", preset,
		}
	}
}

// proxyDecodeArgs forces accelerator-backed decode whenever an accelerator
// encoder was selected. There is deliberately no software-decode retry inside
// Proxy: a render node must either complete the whole video path on its GPU or
// fail the job so the queue can visibly re-route it to the server fallback.
func proxyDecodeArgs(vcodec string) []string {
	switch {
	case strings.HasSuffix(vcodec, "_vaapi"):
		return []string{"-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi"}
	case strings.HasSuffix(vcodec, "_qsv"):
		return []string{"-hwaccel", "qsv", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "qsv"}
	case strings.HasSuffix(vcodec, "_nvenc"):
		return []string{"-hwaccel", "cuda", "-hwaccel_output_format", "cuda"}
	default:
		return nil
	}
}

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
func Proxy(ffmpegBin, vcodec string, crf int, preset, srcPath, outPath string) error {
	return ProxyContext(context.Background(), ffmpegBin, vcodec, crf, preset, srcPath, outPath)
}

func ProxyContext(ctx context.Context, ffmpegBin, vcodec string, crf int, preset, srcPath, outPath string) error {
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	if vcodec == "" {
		vcodec = "libx264"
	}
	if crf <= 0 {
		crf = 21
	}
	if preset == "" {
		preset = "slow"
	}
	// WRITES DIRECTLY TO outPath, which the caller already created inside the
	// derivative directory with O_EXCL|O_NOFOLLOW and will commit onto the real
	// blob name with renameat through a held descriptor.
	//
	// This function used to add its OWN atomic layer — encode to a temp sibling,
	// then rename by path — which was redundant once the caller had one, and was
	// the last unanchored surface here: the sibling was NOT created by us, so it
	// could already BE a planted symlink, and os.Rename/os.Open/syncDir all
	// resolve by name. On the farm host that is a root-privileged write out of
	// the volume. Reader visibility is unaffected: the staged name is dot-
	// prefixed and is never the blob's real name, so a reader only ever sees the
	// final name appear atomically at the caller's renameat.
	// proxyEncodeArgs selects the correct quality and device wiring for the
	// configured encoder. The staged output path is intentionally still passed
	// directly: its descriptor-anchored creation and final rename are owned by
	// GenerateProxy, not this function.
	args := append([]string{"-y", "-loglevel", "error"}, proxyThreadArgs()...)
	args = append(args, proxyDecodeArgs(vcodec)...)
	args = append(args,
		"-i", srcPath,
		"-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2",
		"-movflags", "+faststart",
		// Force the MP4 muxer: the temp path lacks the .mp4 extension ffmpeg
		// would otherwise infer the container from.
		"-f", "mp4")
	args = append(args, proxyEncodeArgs(vcodec, crf, preset)...)
	if strings.Contains(vcodec, "hevc") || strings.Contains(vcodec, "h265") || strings.Contains(vcodec, "265") {
		args = append(args, "-tag:v", "hvc1")
	}
	args = append(args, outPath)
	cmd := commandContext(ctx, ffmpegBin, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg proxy %q: %w: %s", srcPath, err, out)
	}
	return nil
}

// ProxyResult is the per-file outcome of a proxy-generation pass.
type ProxyResult struct {
	Path         string
	Inode        uint64
	Hash         string
	Wrote        bool
	SkippedFresh bool
	Err          error
}

// proxyFresh reports whether the current proxy is already the exact class this
// pass would produce. A generic source-hash gate is not enough here: a CPU
// fallback may have left a perfectly current H.264 proxy that must be upgraded
// when an HEVC worker returns. Conversely, a repeated HEVC watcher job must not
// spend the accelerator re-encoding identical bytes.
//
// Fail closed. The source record, derivative vouch, codec label, and actual
// regular blob must all agree. Missing/legacy fields regenerate once and become
// eligible for future skips. RegenerateFresh is the explicit operator override.
func proxyFresh(store *derivatives.Store, inode uint64, hash string, size int64, opt Options) bool {
	if store == nil || opt.RegenerateFresh || opt.Mount == "" {
		return false
	}
	if proxyFreshIndexed(store, inode, hash, size, opt) {
		return true
	}
	// Farm workers keep private SQLite caches, while manifest.json is the
	// cross-worker index. A NAS fallback cannot decide freshness from its local
	// rows alone: the successful HEVC proxy may have been produced by a GPU whose
	// /state database is on another host. Reconcile on a local miss, then repeat
	// the exact same fail-closed checks. This read is avoided for the common hot
	// path where the current worker already owns a valid row.
	if _, err := ReconcileOneSidecar(store, opt.Mount, inode); err != nil {
		return false
	}
	return proxyFreshIndexed(store, inode, hash, size, opt)
}

func proxyFreshIndexed(store *derivatives.Store, inode uint64, hash string, size int64, opt Options) bool {
	known, sourceHash := store.Known(inode)
	if !known || sourceHash == nil || *sourceHash != hash {
		return false
	}
	rows, err := store.Manifest(inode)
	if err != nil {
		return false
	}
	desiredCodec, _ := proxyCodecStrings(opt.ProxyVCodec, nil)
	for _, row := range rows {
		codecSatisfied := row.Codec != nil && *row.Codec == desiredCodec
		if opt.PreserveHEVCOnFallback && desiredCodec == "h264" &&
			row.Codec != nil && *row.Codec == "hevc" {
			// HEVC is the preferred farm result. A CPU fallback exists to keep
			// an unserviceable source moving, not to downgrade successful files
			// from the same directory-shaped retry.
			codecSatisfied = true
		}
		if row.Kind != "proxy" || row.Status != "ready" ||
			row.Hash == nil || *row.Hash != hash ||
			row.SourceSize == nil || *row.SourceSize != size ||
			!codecSatisfied ||
			row.BlobRelPath == nil || *row.BlobRelPath != "proxy.mp4" {
			continue
		}
		blobRel := derivatives.DerivBlobRel(inode, "proxy.mp4")
		blobInfo, err := derivatives.StatRegularUnder(opt.Mount, blobRel)
		if err != nil {
			continue
		}
		// Cross-worker replacement is possible: a GPU cache can still say HEVC
		// after a CPU worker atomically replaced the shared bytes with H.264.
		// Generated rows carry the exact blob size, so reject that stale vouch.
		if row.BlobSize != nil && *row.BlobSize != blobInfo.Size() {
			continue
		}
		if opt.PreserveHEVCOnFallback && row.Codec != nil && *row.Codec == "hevc" {
			// The preservation exception must validate the bytes, not merely trust
			// the sidecar's label. manifest.json is consumer-writable; an ffprobe
			// of the held regular file proves the shared blob is actually HEVC.
			actual, err := probeProxyBlobCodecContext(optionContext(opt), opt.FFprobeBin, opt.Mount, blobRel)
			if err != nil || actual != "hevc" {
				continue
			}
		}
		return true
	}
	return false
}

func probeProxyBlobCodec(ffprobeBin, mount, rel string) (string, error) {
	return probeProxyBlobCodecContext(context.Background(), ffprobeBin, mount, rel)
}

func probeProxyBlobCodecContext(ctx context.Context, ffprobeBin, mount, rel string) (string, error) {
	if ffprobeBin == "" {
		ffprobeBin = "ffprobe"
	}
	f, err := derivatives.OpenRegularUnder(mount, rel)
	if err != nil {
		return "", err
	}
	defer f.Close()
	// Pass the already-open, O_NOFOLLOW-validated descriptor to ffprobe. Using
	// the joined path here would reintroduce a check/use race after the anchored
	// stat above. ExtraFiles exposes f as descriptor 3 in the child on Unix.
	cmd := commandContext(ctx, ffprobeBin,
		"-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_name",
		"-of", "default=nk=1:nw=1", "/dev/fd/3")
	cmd.ExtraFiles = []*os.File{f}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffprobe proxy codec: %w: %s", err, out)
	}
	codec := strings.ToLower(strings.TrimSpace(string(out)))
	if line, _, ok := strings.Cut(codec, "\n"); ok {
		codec = strings.TrimSpace(line)
	}
	if codec == "h265" {
		codec = "hevc"
	}
	return codec, nil
}

// GenerateProxy renders the OL-3 proxy for one file and commits a single `proxy`
// manifest row. It is a SEPARATE pass from Process() (which commits the fast
// tech/poster/filmstrip/waveform derivatives atomically): a -preset slow whole-
// clip transcode can take minutes, so decoupling it keeps the cheap derivatives
// publishing immediately instead of withholding them behind the proxy encode.
func GenerateProxy(store *derivatives.Store, path string, opt Options) ProxyResult {
	res := ProxyResult{Path: path}
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

	hash, err := SampleHash(path, fi.Size())
	if err != nil {
		res.Err = fmt.Errorf("hash: %w", err)
		return res
	}
	res.Hash = hash
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}

	if proxyFresh(store, inode, hash, fi.Size(), opt) {
		res.SkippedFresh = true
		// A valid row+blob can predate or outlive its transport sidecar. Keep the
		// skip cheap while repairing that index boundary best-effort.
		_ = WriteManifestSidecar(store, opt.Mount, inode)
		return res
	}
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}

	tech, err := ProbeContext(ctx, opt.FFprobeBin, path, fi.Size())
	if err != nil {
		res.Err = err
		return res
	}
	if tech.Video == nil {
		return res // audio-only / no video stream → no proxy (Wrote stays false)
	}

	rel := "proxy.mp4"
	mt := "video/mp4"
	// GenerateProxy is its OWN entry point — cmd/jmfarm runs -proxy as a separate
	// mutually-exclusive pass that never reaches farm.Process, so the guard added
	// there did not apply here at all. Hold the descriptor and stage under it.
	derivDir, ddErr := derivDirFor(opt.Mount, inode)
	if ddErr != nil {
		res.Err = fmt.Errorf("derivative dir: %w", ddErr)
		return res
	}
	defer derivDir.Close()
	staged, out, err := derivatives.StageNameAt(derivDir, rel)
	stErr := err
	if stErr != nil {
		res.Err = fmt.Errorf("stage proxy: %w", stErr)
		return res
	}
	// PROXY-CODEC (#50): stamp which codec THIS blob is + its exact
	// canPlayType/isTypeSupported token so OL/web can gate without re-probing.
	// The codec is derived from the configured encoder (libx264 ⇒ h264 floor;
	// the locked container/audio make the codec_string deterministic). audio
	// channels carry the AAC suffix.
	codec, codecString := proxyCodecStrings(opt.ProxyVCodec, tech)
	row := derivatives.DerivRow{
		Kind: "proxy", Producer: opt.Producer, Version: opt.Version,
		Hash: &hash, BlobRelPath: &rel, MediaType: &mt,
		Codec: &codec, CodecString: &codecString,
	}
	stampSource(&row, fi)
	err = ProxyContext(ctx, opt.FFmpegBin, opt.ProxyVCodec, opt.ProxyCRF, opt.ProxyPreset, path, out)
	if ctxErr := ctx.Err(); ctxErr != nil {
		derivatives.DiscardStagedAt(derivDir, staged)
		res.Err = ctxErr
		return res
	}
	if err == nil {
		err = derivatives.CommitStagedAt(derivDir, staged, rel)
	}
	if err != nil {
		derivatives.DiscardStagedAt(derivDir, staged)
		// Non-fatal: publish a failed row so the consumer regenerates locally.
		res.Err = err
		row.Status = "failed"
	} else {
		res.Wrote = true
		row.Status = "ready"
		// blob_size = actual produced bytes (required-intent on a ready proxy).
		// Best-effort stat; a stat failure leaves blob_size absent (honest — the
		// schema permits null), it does not fail the row.
		if bi, serr := derivatives.StatRegularUnder(opt.Mount, derivatives.DerivBlobRel(inode, rel)); serr == nil {
			sz := bi.Size()
			row.BlobSize = &sz
		}
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
		res.Err = fmt.Errorf("put proxy deriv: %w", err)
		return res
	}
	if opt.Mount != "" {
		_ = WriteManifestSidecar(store, opt.Mount, inode) // JM-15: best-effort
	}
	return res
}

// proxyCodecStrings derives the PROXY-CODEC `codec` label and the exact
// MediaSource.isTypeSupported()/canPlayType() `codec_string` token for the proxy
// THIS run produces (#50, derivatives.schema.json v2). The farm's interchange
// lock fixes the rest of the token: MP4 container, +faststart, yuv420p 8-bit, AAC
// 48 kHz stereo — so given the video codec the token is deterministic.
//
// libx264 (the floor + default) ⇒ "h264" / "avc1.640028, mp4a.40.2" (High@4.0).
// A hardware H.264 encoder (h264_nvenc/qsv/vaapi) is still the H.264 floor.
// An HEVC encoder ⇒ "hevc" / "hvc1.1.6.L120.90, …"; AV1 ⇒ "av1" / "av01.…".
// The audio half is always mp4a.40.2 (AAC-LC) because the Proxy() command hard-
// codes `-c:a aac` regardless of the source.
func proxyCodecStrings(vcodec string, tech *Tech) (codec, codecString string) {
	const aac = "mp4a.40.2" // AAC-LC, the locked proxy audio
	audioSuffix := ""
	if tech != nil && len(tech.Audio) > 0 {
		audioSuffix = ", " + aac
	}
	switch {
	case strings.Contains(vcodec, "hevc") || strings.Contains(vcodec, "h265") || strings.Contains(vcodec, "265"):
		return "hevc", "hvc1.1.6.L120.90" + audioSuffix
	case strings.Contains(vcodec, "av1"):
		return "av1", "av01.0.08M.08" + audioSuffix
	default:
		// libx264 / h264_nvenc / h264_qsv / h264_vaapi / "" — all the H.264 floor.
		return "h264", "avc1.640028" + audioSuffix
	}
}
