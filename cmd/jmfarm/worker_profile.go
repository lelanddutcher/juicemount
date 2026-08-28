package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
)

type workerProfile struct {
	Role               string
	Capabilities       []string
	Encoders           []string
	Decoders           []string
	TranscriptBackends []string
	Benchmarks         farmqueue.WorkerBenchmarks
}

type hardwareEncoderProbe struct {
	Name string
	FPS  float64
}

// probeWorkerProfile admits a node from work it actually completes, not device
// names or container flags. A render node with an exposed-but-broken GPU fails
// admission instead of silently becoming a CPU worker.
func probeWorkerProfile(ctx context.Context, cfg queueConfig) (workerProfile, error) {
	p := workerProfile{
		Capabilities:       []string{"cpu", "metadata"},
		TranscriptBackends: []string{"cpu"},
		Benchmarks: farmqueue.WorkerBenchmarks{
			ProbedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}
	var probeErrs []string
	encoderProbes, encoderErrs := probeHardwareEncoders(ctx)
	probeErrs = append(probeErrs, encoderErrs...)
	for _, encoder := range encoderProbes {
		decoder, fps, err := probeHardwareDecode(ctx, encoder.Name)
		if err != nil {
			probeErrs = append(probeErrs, err.Error())
			continue
		}
		// Video admission is per codec and device family. Keeping only
		// encoders whose matching decoder passed a live bitstream probe prevents
		// an HEVC-capable node from claiming H.264 input it cannot decode (or the
		// reverse) and then burning CPU without Manager knowing.
		p.Encoders = append(p.Encoders, encoder.Name)
		p.Decoders = appendUnique(p.Decoders, decoder)
		p.Capabilities = appendUnique(p.Capabilities, "decoder:"+decoder)
		if encoder.FPS > p.Benchmarks.EncodeFPS {
			p.Benchmarks.EncodeFPS = encoder.FPS
		}
		if fps > p.Benchmarks.DecodeFPS {
			p.Benchmarks.DecodeFPS = fps
		}
	}
	for _, enc := range p.Encoders {
		p.Capabilities = append(p.Capabilities, "encoder:"+enc)
		if family := hardwareFamily(enc); family != "" {
			p.Capabilities = appendUnique(p.Capabilities, family)
		}
	}

	device := strings.ToLower(strings.TrimSpace(cfg.tDevice))
	if device != "" && device != "cpu" {
		if ratio, err := probeTranscriptBackend(ctx, cfg.wBin, cfg.wModel, device); err == nil {
			p.TranscriptBackends = append(p.TranscriptBackends, device)
			p.Capabilities = append(p.Capabilities, "transcript:"+device)
			p.Benchmarks.TranscriptXReal = ratio
		} else {
			probeErrs = append(probeErrs, err.Error())
		}
	}
	p.Benchmarks.AccessMBps = benchmarkMountRead(ctx, cfg.mount)

	if err := ctx.Err(); err != nil {
		return p, err
	}
	requested := strings.ToLower(strings.TrimSpace(cfg.role))
	if requested == "" {
		requested = "auto"
	}
	accelerated := len(p.Encoders) > 0 || len(p.TranscriptBackends) > 1
	p.Benchmarks.ProbeError = workerProbeFailure(probeErrs, accelerated)
	switch requested {
	case "auto":
		p.Role = farmqueue.QueueClassServer
		if accelerated {
			p.Role = farmqueue.QueueClassRender
		}
	case farmqueue.QueueClassServer:
		p.Role = farmqueue.QueueClassServer
	case farmqueue.QueueClassRender:
		if !accelerated {
			return p, fmt.Errorf("role=render but no hardware encoder or non-CPU transcript backend passed a live probe (%s)", p.Benchmarks.ProbeError)
		}
		p.Role = farmqueue.QueueClassRender
	default:
		return p, fmt.Errorf("unknown worker role %q (want auto, server, or render)", cfg.role)
	}
	return p, nil
}

// workerProbeFailure distinguishes a failed worker admission from unavailable
// alternatives. Intel ffmpeg commonly lists both QSV and VAAPI even when only
// VAAPI is usable inside Docker. If verified VAAPI decode+encode succeeds, a
// failed QSV experiment is not a worker error and must not paint the healthy
// node red in Manager. The exact admitted Encoders/Decoders lists remain the
// truthful capability record used by scheduling.
func workerProbeFailure(probeErrs []string, accelerated bool) string {
	if accelerated || len(probeErrs) == 0 {
		return ""
	}
	return strings.Join(probeErrs, "; ")
}

func probeHardwareEncoders(parent context.Context) ([]hardwareEncoderProbe, []string) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-encoders").CombinedOutput()
	if err != nil {
		return nil, []string{"ffmpeg encoder inventory unavailable"}
	}
	inventory := string(out)
	candidates := []string{
		"hevc_vaapi", "hevc_qsv", "hevc_nvenc",
		"h264_vaapi", "h264_qsv", "h264_nvenc",
	}
	var verified []hardwareEncoderProbe
	var errs []string
	for _, enc := range candidates {
		if !strings.Contains(inventory, enc) || !hardwareDevicePresent(ctx, enc) {
			continue
		}
		fps, err := runEncoderProbe(parent, enc)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s probe failed", enc))
			continue
		}
		verified = append(verified, hardwareEncoderProbe{Name: enc, FPS: fps})
	}
	return verified, errs
}

func runEncoderProbe(parent context.Context, encoder string) (float64, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30", "-frames:v", "90"}
	switch hardwareFamily(encoder) {
	case "vaapi":
		args = append(args, "-vaapi_device", "/dev/dri/renderD128", "-vf", "format=nv12,hwupload", "-c:v", encoder, "-qp", "25")
	case "qsv":
		args = append(args, "-c:v", encoder, "-global_quality", "25")
	case "nvenc":
		args = append(args, "-c:v", encoder, "-cq", "25", "-preset", "p4")
	default:
		return 0, fmt.Errorf("not a hardware encoder: %s", encoder)
	}
	args = append(args, "-f", "null", "-")
	started := time.Now()
	if out, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err != nil {
		return 0, fmt.Errorf("%s: %w: %s", encoder, err, strings.TrimSpace(string(out)))
	}
	seconds := time.Since(started).Seconds()
	if seconds <= 0 {
		return 0, nil
	}
	return 90 / seconds, nil
}

// probeHardwareDecode makes a real bitstream with the verified encoder, then
// requires the same GPU family to decode it. This prevents a node from claiming
// "GPU proxy" while ffmpeg is quietly decoding on CPU.
func probeHardwareDecode(parent context.Context, encoder string) (string, float64, error) {
	tmp, err := os.MkdirTemp("", "jmfarm-decode-probe-")
	if err != nil {
		return "", 0, err
	}
	defer os.RemoveAll(tmp)
	sample := filepath.Join(tmp, "sample.mp4")
	makeCtx, makeCancel := context.WithTimeout(parent, 20*time.Second)
	defer makeCancel()
	family := hardwareFamily(encoder)
	makeArgs := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=30", "-frames:v", "90"}
	switch family {
	case "vaapi":
		makeArgs = append(makeArgs, "-vaapi_device", "/dev/dri/renderD128", "-vf", "format=nv12,hwupload", "-c:v", encoder, "-qp", "25")
	case "qsv":
		makeArgs = append(makeArgs, "-c:v", encoder, "-global_quality", "25")
	case "nvenc":
		makeArgs = append(makeArgs, "-c:v", encoder, "-cq", "25", "-preset", "p4")
	default:
		return "", 0, fmt.Errorf("decode probe has no hardware family for %s", encoder)
	}
	if strings.HasPrefix(encoder, "hevc_") {
		makeArgs = append(makeArgs, "-tag:v", "hvc1")
	}
	makeArgs = append(makeArgs, sample)
	if out, err := exec.CommandContext(makeCtx, "ffmpeg", makeArgs...).CombinedOutput(); err != nil {
		return "", 0, fmt.Errorf("decode sample generation failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	args := []string{"-hide_banner", "-loglevel", "error"}
	switch family {
	case "vaapi":
		args = append(args, "-hwaccel", "vaapi", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "vaapi")
	case "qsv":
		args = append(args, "-hwaccel", "qsv", "-hwaccel_device", "/dev/dri/renderD128", "-hwaccel_output_format", "qsv")
	case "nvenc":
		args = append(args, "-hwaccel", "cuda", "-hwaccel_output_format", "cuda")
	default:
		return "", 0, fmt.Errorf("decode probe has no hardware family for %s", encoder)
	}
	args = append(args, "-i", sample, "-frames:v", "90", "-f", "null", "-")
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	started := time.Now()
	if out, err := exec.CommandContext(ctx, "ffmpeg", args...).CombinedOutput(); err != nil {
		return "", 0, fmt.Errorf("%s hardware decode probe failed: %w: %s", family, err, strings.TrimSpace(string(out)))
	}
	codec := "h264"
	if strings.HasPrefix(encoder, "hevc_") {
		codec = "hevc"
	}
	return codec + "_" + family, 90 / time.Since(started).Seconds(), nil
}

func probeTranscriptBackend(parent context.Context, bin, model, device string) (float64, error) {
	if bin == "" {
		bin = "whisper-cli"
	}
	if model == "" {
		return 0, fmt.Errorf("%s transcript probe skipped: model path is empty", device)
	}
	model = resolveModelPath(model)
	if fi, err := os.Stat(model); err != nil || fi.IsDir() {
		return 0, fmt.Errorf("%s transcript probe skipped: model is unavailable", device)
	}
	if err := verifyTranscriptAccelerator(parent, device); err != nil {
		return 0, err
	}
	tmp, err := os.MkdirTemp("", "jmfarm-transcript-probe-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tmp)
	wav := filepath.Join(tmp, "probe.wav")
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1", "-ar", "16000", "-ac", "1", wav).CombinedOutput(); err != nil {
		return 0, fmt.Errorf("transcript audio probe: %w: %s", err, strings.TrimSpace(string(out)))
	}
	started := time.Now()
	outPrefix := filepath.Join(tmp, "out")
	args := []string{"-m", model, "-f", wav, "-oj", "-of", outPrefix, "-l", "auto", "-np"}
	args = append(args, farm.WhisperDeviceArgs(device)...)
	if out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput(); err != nil {
		return 0, fmt.Errorf("%s transcript probe failed: %w: %s", device, err, strings.TrimSpace(string(out)))
	}
	seconds := time.Since(started).Seconds()
	if seconds <= 0 {
		return 0, nil
	}
	return 1 / seconds, nil
}

// verifyTranscriptAccelerator prevents a compiled GPU backend from being
// admitted when the container can only see a software device. In particular,
// Mesa's llvmpipe exposes a valid Vulkan API but executes on CPU; advertising
// that as a render capability would defeat measured scheduling.
func verifyTranscriptAccelerator(parent context.Context, device string) error {
	device = strings.ToLower(strings.TrimSpace(device))
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	switch device {
	case "vulkan":
		out, err := exec.CommandContext(ctx, "vulkaninfo", "--summary").CombinedOutput()
		if err != nil {
			return fmt.Errorf("vulkan transcript probe skipped: hardware inventory unavailable")
		}
		s := string(out)
		for _, physical := range []string{
			"PHYSICAL_DEVICE_TYPE_DISCRETE_GPU",
			"PHYSICAL_DEVICE_TYPE_INTEGRATED_GPU",
			"PHYSICAL_DEVICE_TYPE_VIRTUAL_GPU",
		} {
			if strings.Contains(s, physical) {
				return nil
			}
		}
		return fmt.Errorf("vulkan transcript probe rejected: no physical GPU (software Vulkan is not a render node)")
	case "cuda":
		if out, err := exec.CommandContext(ctx, "nvidia-smi", "-L").CombinedOutput(); err == nil && strings.Contains(string(out), "GPU ") {
			return nil
		}
		return fmt.Errorf("cuda transcript probe rejected: no physical NVIDIA GPU")
	case "sycl":
		if out, err := exec.CommandContext(ctx, "sycl-ls").CombinedOutput(); err == nil && strings.Contains(strings.ToLower(string(out)), "gpu") {
			return nil
		}
		return fmt.Errorf("sycl transcript probe rejected: no physical GPU")
	default:
		return fmt.Errorf("unknown transcript accelerator %q", device)
	}
}

func benchmarkMountRead(parent context.Context, root string) float64 {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	result := make(chan float64, 1)
	go func() {
		result <- benchmarkMountReadBlocking(root)
	}()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		return 0
	}
}

func benchmarkMountReadBlocking(root string) float64 {
	var candidate string
	visited := 0
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || candidate != "" {
			return nil
		}
		visited++
		if visited > 2000 {
			return filepath.SkipAll
		}
		if d.IsDir() {
			if path != root && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !mediaExts[strings.ToLower(filepath.Ext(path))] {
			return nil
		}
		if fi, statErr := d.Info(); statErr == nil && fi.Size() >= 4<<20 {
			candidate = path
		}
		return nil
	})
	if candidate == "" {
		return 0
	}
	f, err := os.Open(candidate)
	if err != nil {
		return 0
	}
	defer f.Close()
	started := time.Now()
	n, err := io.CopyN(io.Discard, f, 16<<20)
	if err != nil && err != io.EOF {
		return 0
	}
	seconds := time.Since(started).Seconds()
	if seconds <= 0 {
		return 0
	}
	return float64(n) / (1024 * 1024) / seconds
}

func hardwareFamily(name string) string {
	for _, family := range []string{"vaapi", "qsv", "nvenc"} {
		if strings.HasSuffix(name, "_"+family) {
			return family
		}
	}
	return ""
}

func hardwareDevicePresent(ctx context.Context, encoder string) bool {
	switch hardwareFamily(encoder) {
	case "vaapi", "qsv":
		_, err := os.Stat("/dev/dri/renderD128")
		return err == nil
	case "nvenc":
		if _, err := os.Stat("/dev/nvidia0"); err == nil {
			return true
		}
		return exec.CommandContext(ctx, "nvidia-smi", "-L").Run() == nil
	}
	return false
}

func appendUnique(in []string, value string) []string {
	for _, existing := range in {
		if existing == value {
			return in
		}
	}
	return append(in, value)
}
