package farm

import "strconv"

// ffmpeg thread governor (2026-07-14, farm-throughput audit). Live evidence
// from the running farm: each ffmpeg auto-threaded to ~61 threads (the
// default `-threads 0` = one decode thread per logical core, PLUS filter
// threads), so N parallel workers spawned N×cores threads that thrashed a
// 22-core box (load average 33-45, CPU "pinned" but throughput ~7.6s/file).
// Derivative work is decode-bound and each generator is one mostly-serial
// demux→decode pipe: past a handful of threads ffmpeg gets sub-linear
// returns, so the right model is MANY single-/few-threaded jobs in
// parallel, not FEW jobs each grabbing the whole box. cmd/jmfarm sets the
// per-ffmpeg cap so that workers × threads ≈ physical cores.
//
// 0 (the zero value / test default) = don't emit -threads, i.e. ffmpeg's
// historical auto behavior — so existing generator tests are unaffected.
var ffmpegThreadCap int

// SetFFmpegThreads sets the per-invocation ffmpeg thread cap. Negative
// clamps to 0 (uncapped). Called once at farm startup.
func SetFFmpegThreads(n int) {
	if n < 0 {
		n = 0
	}
	ffmpegThreadCap = n
}

// FFmpegThreads returns the current cap (0 = uncapped).
func FFmpegThreads() int { return ffmpegThreadCap }

// ffmpegThreadArgs returns the global ffmpeg options that cap BOTH the
// codec (decoder) thread pool and the filtergraph thread pool. Placed
// before -i so they apply to the decode side (the dominant cost for the
// full-decode filmstrip/waveform generators). Empty when uncapped.
func ffmpegThreadArgs() []string {
	if ffmpegThreadCap <= 0 {
		return nil
	}
	n := strconv.Itoa(ffmpegThreadCap)
	return []string{"-threads", n, "-filter_threads", n, "-filter_complex_threads", n}
}

// proxyThreadCap is the SEPARATE ffmpeg thread cap for the proxy TRANSCODE.
// Proxy generation is ENCODE-bound (x264 -preset slow scales well to ~8-16
// threads), the inverse of the decode-bound derivative rule — capping it to
// the derivative thread count would throttle the exact workload that most
// wants threads. Proxy parallelism is already bounded separately by pConc
// (few workers), so 0 (uncapped, x264's own auto) is the right default.
var proxyThreadCap int

// SetProxyThreads sets the proxy-transcode thread cap (0 = uncapped).
func SetProxyThreads(n int) {
	if n < 0 {
		n = 0
	}
	proxyThreadCap = n
}

// proxyThreadArgs is the proxy encoder's thread-cap args (uncapped by default).
func proxyThreadArgs() []string {
	if proxyThreadCap <= 0 {
		return nil
	}
	n := strconv.Itoa(proxyThreadCap)
	return []string{"-threads", n}
}
