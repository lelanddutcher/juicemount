// jmfarm is the derivative-producer CLI (contract JM-14/JM-16 MVP). It walks a
// directory of media on a mounted JuiceMount volume, derives tech/EXIF metadata
// (and optionally poster thumbnails) for each file, and writes them into the
// Tier-B index (derivatives.db) the control plane serves at /metadata +
// /derivatives. Run it against a cached/pinned folder so ffprobe reads from SSD.
//
// It writes directly to the same derivatives.db the running app has open; SQLite
// WAL makes that safe (one writer here, the app only reads on the query path).
// The producer logic lives in internal/farm so JM-15 (in-process sync) and
// JM-16 (the Linux fast lane) reuse it unchanged.
//
//	jmfarm --root "/Volumes/zpool/Film Projects/.../REEL_0065/Proxy" --blobs
//
// Queue mode (JM-16, the standing farm worker): with -queue -meta <redis://…> it
// drains the shared juicefarm: queue instead of doing a single -root sweep — it
// dials the volume's JuiceFS Redis, then loops { heartbeat; BRPOP a job; run the
// passes the job asks for scoped to job.Path; mark done/failed } until SIGTERM.
// The same internal/farm generators + farm-status rollup the one-shot modes use
// are reused per job, so results flow back the proven JM-15 way (sidecars →
// reconcile → /derivatives). -mount stays the FUSE files path; -meta is the queue.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
	buildversion "github.com/lelanddutcher/juicemount/internal/version"
)

// mediaExts are the file types we probe. Extension gate is a cheap pre-filter;
// ffprobe is the real arbiter (a non-media file just errors and is skipped).
var mediaExts = map[string]bool{
	".mov": true, ".mp4": true, ".m4v": true, ".mxf": true, ".mkv": true,
	".avi": true, ".mts": true, ".m2ts": true, ".braw": true, ".r3d": true,
	".wav": true, ".aif": true, ".aiff": true, ".flac": true, ".mp3": true, ".m4a": true,
}

// passOpts bundles everything a single sweep needs so runPasses can be shared by
// the one-shot CLI modes and the queue loop. The booleans select the generator;
// at most one of Transcript/Proxy/QLPreview is set (else basic derivatives run).
type passOpts struct {
	ctx      context.Context // queue signal context; nil means a non-cancellable one-shot sweep
	opt      farm.Options    // resolved generator options (already merged with job overrides in queue mode)
	mode     string          // "derivatives" | "proxy" | "ql-preview" | "transcript(AI)" — display + status stamp
	transcr  bool
	proxyGen bool
	qlGen    bool
	dryRun   bool
	verbose  bool
	effConc  int    // resolved worker count for this sweep
	status   string // farm-status.json path ("" → don't write)
	mount    string // for the Tier-A blob dir + proxy_economics stat-walk
	producer string
	target   string        // human label for the status rollup (root or "files:…")
	gov      farm.Governor // resolved governor stamp for the status file
	store    *derivatives.Store
	// process is an injectable seam for accounting tests. Production callers
	// leave it nil and use farm.Process.
	process  func(*derivatives.Store, string, farm.Options) farm.Result
	progress func(stage string, pct int)
}

// runPasses dispatches one sweep over targets with the worker pool + live
// progress ticker + final farm-status rollup, exactly as the one-shot path does.
// It returns the counts plus the exact failed paths so a partially successful
// render batch can retry only its failures. The generator dispatch below is the
// single source of truth: the basic, proxy and transcript modes all funnel
// through the same internal/farm calls.
func runPasses(po passOpts, targets []string) (processed, failed int, failedTargets, failureDetails []string, safetyErr error) {
	start := time.Now()
	parentCtx := po.ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	// A backend storage fault is a farm-wide safety event, not an ordinary
	// per-media failure. Cancel this pass immediately so work that has not yet
	// started cannot consume more of the exhausted pool. Keep this child context
	// separate from the durable job context: the queue loop still needs to
	// distinguish an operator cancellation from an internally-triggered pause.
	ctx, cancelPass := context.WithCancel(parentCtx)
	defer cancelPass()
	po.opt.Context = ctx
	var ok, fail, thumbs, strips, waves, speech, transcriptSkipped, proxies, proxySkipped, qls int64
	// skippedFresh counts assets whose derivatives already matched byte-identical
	// source — a moved or re-enqueued file that cost no re-encode.
	var skippedFresh, sidecarsRepaired int64
	var mu sync.Mutex
	var firstErrs []string
	var failedPaths []string
	completedPaths := make(map[string]bool, len(targets))
	captureSafety := func(err error) {
		if isFarmStoragePressure(err) {
			firstStorageFault := false
			mu.Lock()
			if safetyErr == nil {
				safetyErr = err
				firstStorageFault = true
			}
			mu.Unlock()
			if firstStorageFault {
				cancelPass()
			}
		}
	}
	captureFailure := func(path string, err error) {
		recordFailure(&mu, &firstErrs, &failedPaths, path, err)
		captureSafety(err)
	}
	captureSuccess := func(path string) {
		mu.Lock()
		completedPaths[path] = true
		mu.Unlock()
	}
	process := po.process
	if process == nil {
		process = farm.Process
	}
	if po.progress != nil {
		po.progress(po.mode, 0)
	}
	stopHeartbeatProgress := make(chan struct{})
	var heartbeatProgressWG sync.WaitGroup
	if po.progress != nil {
		heartbeatProgressWG.Add(1)
		go func() {
			defer heartbeatProgressWG.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-stopHeartbeatProgress:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					done := atomic.LoadInt64(&ok) + atomic.LoadInt64(&fail)
					pct := 0
					if len(targets) > 0 {
						pct = int(done * 100 / int64(len(targets)))
					}
					po.progress(po.mode, pct)
				}
			}
		}()
	}

	// Live progress: while the sweep runs, write farm-status.json every ~3s with an
	// in_progress block {pass, total, done, failed, started_at} so the manager Farm
	// tab can show a progress bar + ETA. Cheap by design (Stats GROUP BY only; the
	// proxy_economics stat-walk is skipped until the final write). Only when a status
	// path is set and we're really writing (store!=nil, !dry-run). The ticker is
	// stopped before the final WriteFarmStatus so the last write (which clears
	// in_progress) always wins.
	stopProgress := make(chan struct{})
	var progressWG sync.WaitGroup
	if po.status != "" && !po.dryRun && po.store != nil {
		progressWG.Add(1)
		go func() {
			defer progressWG.Done()
			tick := time.NewTicker(3 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-stopProgress:
					return
				case <-ctx.Done():
					return
				case <-tick.C:
					done := atomic.LoadInt64(&ok) + atomic.LoadInt64(&fail)
					ip := farm.InProgress{
						Pass:      po.mode,
						Total:     len(targets),
						Done:      int(done),
						Failed:    int(atomic.LoadInt64(&fail)),
						StartedAt: start.Unix(),
					}
					sweep := farm.SweepInfo{
						Mode: po.mode, Producer: po.producer, Target: po.target,
						Processed: int(done), Failed: int(atomic.LoadInt64(&fail)),
						StartedAt: start.Unix(), DurationMS: time.Since(start).Milliseconds(),
					}
					if err := farm.WriteFarmProgress(po.store, po.status, po.mount, sweep, po.gov, ip); err != nil {
						fmt.Fprintf(os.Stderr, "jmfarm: progress write: %v\n", err)
						captureSafety(err)
					}
				}
			}
		}()
	}

	if po.effConc < 1 {
		po.effConc = 1
	}
	sem := make(chan struct{}, po.effConc)
	var wg sync.WaitGroup
targetLoop:
	for _, p := range targets {
		if ctx.Err() != nil {
			break
		}
		p := p
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break targetLoop
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if ctx.Err() != nil {
				return
			}

			if po.dryRun {
				tech, e := farm.ProbeContext(ctx, po.opt.FFprobeBin, p, 0)
				if ctx.Err() != nil {
					return
				}
				if e != nil {
					atomic.AddInt64(&fail, 1)
					captureFailure(p, e)
					return
				}
				atomic.AddInt64(&ok, 1)
				captureSuccess(p)
				if po.verbose {
					fmt.Printf("  [dry] %-50s %s %dms\n", filepath.Base(p), tech.Container, tech.DurationMS)
				}
				return
			}

			if po.qlGen {
				qr := farm.GenerateQLPreview(po.store, p, po.opt)
				if ctx.Err() != nil {
					return
				}
				if qr.Err != nil {
					atomic.AddInt64(&fail, 1)
					captureFailure(p, qr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), qr.Inode, qr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				captureSuccess(p)
				if qr.Wrote {
					atomic.AddInt64(&qls, 1)
				}
				if po.verbose {
					fmt.Printf("  [ok] %-50s inode=%d qlpreview=%v skipped=%v\n",
						filepath.Base(p), qr.Inode, qr.Wrote, qr.SkippedFresh)
				}
				return
			}

			if po.proxyGen {
				pr := farm.GenerateProxy(po.store, p, po.opt)
				if ctx.Err() != nil {
					return
				}
				if pr.Err != nil {
					atomic.AddInt64(&fail, 1)
					captureFailure(p, pr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), pr.Inode, pr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				captureSuccess(p)
				if pr.Wrote {
					atomic.AddInt64(&proxies, 1)
				}
				if pr.SkippedFresh {
					atomic.AddInt64(&proxySkipped, 1)
				}
				if po.verbose {
					fmt.Printf("  [ok] %-50s inode=%d proxy=%v skipped=%v\n",
						filepath.Base(p), pr.Inode, pr.Wrote, pr.SkippedFresh)
				}
				return
			}

			if po.transcr {
				tr := farm.GenerateTranscript(po.store, p, po.opt)
				if ctx.Err() != nil {
					return
				}
				if tr.Err != nil {
					atomic.AddInt64(&fail, 1)
					captureFailure(p, tr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), tr.Inode, tr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				captureSuccess(p)
				if tr.HasSpeech {
					atomic.AddInt64(&speech, 1)
				}
				if tr.SkippedFresh {
					atomic.AddInt64(&transcriptSkipped, 1)
				}
				if po.verbose {
					fmt.Printf("  [ok] %-50s inode=%d speech=%v segments=%d skipped=%v\n",
						filepath.Base(p), tr.Inode, tr.HasSpeech, tr.Segments, tr.SkippedFresh)
				}
				return
			}

			r := process(po.store, p, po.opt)
			if ctx.Err() != nil {
				return
			}
			if r.Err != nil {
				atomic.AddInt64(&fail, 1)
				captureFailure(p, r.Err)
				if po.verbose {
					fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), r.Inode, r.Err)
				}
				return
			}
			// A move must be VISIBLE as a move. Without its own counter a skipped
			// asset is indistinguishable from one that generated nothing, and the
			// whole point of the freshness gate is being able to tell that a
			// reorganised folder cost no compute.
			if r.SkippedFresh {
				atomic.AddInt64(&ok, 1)
				captureSuccess(p)
				atomic.AddInt64(&skippedFresh, 1)
				if r.SidecarRepaired {
					atomic.AddInt64(&sidecarsRepaired, 1)
				}
				if po.verbose {
					fmt.Printf("  [skip] %-50s inode=%d unchanged (moved or re-enqueued); no re-encode%s\n",
						filepath.Base(p), r.Inode,
						map[bool]string{true: ", manifest repaired"}[r.SidecarRepaired])
				}
				return
			}
			if r.ThumbWrote {
				atomic.AddInt64(&thumbs, 1)
			}
			if r.FilmWrote {
				atomic.AddInt64(&strips, 1)
			}
			if r.WaveWrote {
				atomic.AddInt64(&waves, 1)
			}
			if r.BlobErr != nil {
				// The metadata row may have published successfully, but this pass asks
				// for the blob set too. Counting a partial artifact failure as `ok`
				// made live progress and the terminal Redis job claim zero failures
				// while the derivative index accumulated status:"failed" rows.
				// Preserve the successful metadata, but make the file/job outcome
				// truthful and retryable.
				atomic.AddInt64(&fail, 1)
				captureFailure(p, r.BlobErr)
				if po.verbose {
					fmt.Printf("  [PARTIAL FAIL] %-42s inode=%d %v\n", filepath.Base(p), r.Inode, r.BlobErr)
				}
				return
			}
			atomic.AddInt64(&ok, 1)
			captureSuccess(p)
			if po.verbose {
				fmt.Printf("  [ok] %-50s inode=%d hash=%s %dms vid=%v thumb=%v strip=%v wave=%v\n",
					filepath.Base(p), r.Inode, r.Hash, r.DurationMS, r.HasVideo, r.ThumbWrote, r.FilmWrote, r.WaveWrote)
			}
		}()
	}
	wg.Wait()
	close(stopHeartbeatProgress)
	heartbeatProgressWG.Wait()
	if po.progress != nil {
		done := atomic.LoadInt64(&ok) + atomic.LoadInt64(&fail)
		pct := 0
		if len(targets) > 0 {
			pct = int(done * 100 / int64(len(targets)))
		}
		po.progress(po.mode, pct)
	}

	// Stop the live ticker before the final write so the final status (which clears
	// in_progress + measures proxy_economics) is the last thing on disk.
	close(stopProgress)
	progressWG.Wait()

	if ctx.Err() != nil {
		fmt.Printf("\njmfarm cancelled after %s: %d completed, %d failed — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, len(targets))
	} else if po.transcr {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d with-speech (transcribed), %d skipped-current — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, speech, transcriptSkipped, len(targets))
	} else if po.qlGen {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d ql-previews — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, qls, len(targets))
	} else if po.proxyGen {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d proxies, %d skipped-current — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, proxies, proxySkipped, len(targets))
	} else {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d thumbnails, %d filmstrips, %d waveforms, "+
			"%d skipped-unchanged (%d manifests repaired) — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, thumbs, strips, waves,
			skippedFresh, sidecarsRepaired, len(targets))
	}
	if len(firstErrs) > 0 {
		fmt.Printf("first errors (%d shown):\n", len(firstErrs))
		for _, e := range firstErrs {
			fmt.Printf("  - %s\n", e)
		}
	}

	if po.status != "" && !po.dryRun && po.store != nil {
		sweep := farm.SweepInfo{
			Mode: po.mode, Producer: po.producer, Target: po.target,
			Processed: int(ok), Failed: int(fail),
			StartedAt: start.Unix(), DurationMS: time.Since(start).Milliseconds(),
			Canceled: ctx.Err() != nil,
		}
		// Final (idle) write: clears in_progress + measures proxy_economics (stat-
		// walks the proxy blobs under mount). gov was stamped before the sweep with
		// the RESOLVED run settings so the manager's read-only knob inspector shows
		// real values (nice/ionice/interval are applied by the entrypoint).
		if err := farm.WriteFarmStatus(po.store, po.status, po.mount, sweep, po.gov); err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: status write: %v\n", err)
			captureSafety(err)
		}
	}
	// Once storage safety aborts a pass, retry every target that did not reach a
	// proven successful return. This includes the triggering failure, work that
	// was canceled in flight, and work that was never launched. Successfully
	// committed targets stay out of the retry list so ProcessedOffset remains
	// exact instead of counting a freshness skip twice on the resumed claim.
	if safetyErr != nil {
		mu.Lock()
		successful := make(map[string]bool, len(completedPaths))
		for path := range completedPaths {
			successful[path] = true
		}
		mu.Unlock()
		failedPaths = failedPaths[:0]
		for _, path := range targets {
			if !successful[path] {
				failedPaths = append(failedPaths, path)
			}
		}
	}
	sort.Strings(failedPaths)
	return int(ok), int(fail), failedPaths, append([]string(nil), firstErrs...), safetyErr
}

func main() {
	var (
		buildInfo  = flag.Bool("build-info", false, "print release version and source commit, then exit")
		dbPath     = flag.String("db", defaultDBPath(), "derivatives.db path (the one the app serves)")
		root       = flag.String("root", "", "directory to walk for media (required unless -files)")
		files      = flag.String("files", "", "comma-separated explicit file list (alternative to -root)")
		mount      = flag.String("mount", "/Volumes/zpool", "mount point (for Tier-A blob dir)")
		blobs      = flag.Bool("blobs", false, "also generate poster thumbnails into Tier-A")
		thumbDim   = flag.Int("thumb-dim", 720, "poster fit box in px")
		filmstr    = flag.Bool("filmstrip", false, "also generate filmstrip sprite-sheets into Tier-A (JM-16)")
		filmCell   = flag.Int("filmstrip-cell", 320, "filmstrip cell width in px")
		wave       = flag.Bool("waveform", false, "also generate audio waveform overviews into Tier-A (JM-18)")
		regen      = flag.Bool("regenerate", false, "re-derive assets the freshness gate would skip (the ONLY way to force work on an asset whose derivatives already exist — use after a generator or codec fix)")
		waveSPP    = flag.Int("waveform-spp", 1024, "waveform samples per pixel")
		transcr    = flag.Bool("transcript", false, "AI mode: generate whisper transcripts → ai.logger.json (instead of basic derivatives)")
		proxyGen   = flag.Bool("proxy", false, "proxy mode: generate faststart MP4 proxies (OL-3), separate from basic derivatives")
		qlGen      = flag.Bool("ql-preview", false, "QL-preview mode: generate short spacebar-playable H.264 previews (T1.1/F4), separate from basic derivatives")
		qlDim      = flag.Int("ql-dim", 960, "QL preview fit box in px")
		qlSecs     = flag.Int("ql-seconds", 20, "QL preview duration cap in seconds (<=0 → 20)")
		postAlways = flag.Bool("poster-always", false, "poster-always policy (T1.1): generate posters for every video clip even below -min-size-mb")
		vcodec     = flag.String("vcodec", "libx264", "proxy video encoder (GPU: h264_nvenc/h264_qsv/h264_vaapi)")
		pCRF       = flag.Int("crf", 21, "proxy CRF quality (lower = sharper/bigger)")
		pPreset    = flag.String("preset", "slow", "proxy x264 preset (faster preset = quicker, larger)")
		pConc      = flag.Int("proxy-concurrency", 0, "separate (lower) worker count for proxy mode; 0 = use -concurrency (proxy transcode is the CPU hog)")
		wModel     = flag.String("whisper-model", "", "path to a ggml whisper model (required with -transcript)")
		wBin       = flag.String("whisper-bin", "whisper-cli", "whisper.cpp CLI binary")
		limit      = flag.Int("limit", 0, "max files to process (0 = no limit)")
		// Throughput audit (2026-07-14): ffmpeg default -threads 0 spawns one
		// decode thread per core PLUS filter threads (~61 on a 22-core box), so
		// N parallel workers oversubscribed the box (load 33-45, CPU pinned but
		// ~7.6s/file). Cap per-ffmpeg threads and size the worker pool so
		// workers×threads ≈ cores. Env: JM_FARM_FFMPEG_THREADS, JM_FARM_CONCURRENCY.
		// threads=2: decode frame-threads sub-linearly (~1.8x at 2, flat after),
		// so spend parallelism on FILES not threads (90k independent files). The
		// keyframe-only filmstrip makes decode nearly free, so few threads / many
		// workers is ideal. CAUTION: runtime.NumCPU() returns the HOST logical
		// cores in a cgroup-limited container, not the quota — set
		// JM_FARM_CONCURRENCY explicitly on the deploy to the real core budget.
		ffThreads = flag.Int("ffmpeg-threads", farmEnvInt("JM_FARM_FFMPEG_THREADS", 2),
			"threads per derivative ffmpeg (0 = uncapped); keep workers*threads ≈ cores")
		proxyThreads = flag.Int("proxy-threads", farmEnvInt("JM_FARM_PROXY_THREADS", 0),
			"threads per proxy TRANSCODE (0 = x264 auto; encode-bound, wants threads — separate from derivative cap)")
		conc = flag.Int("concurrency", farmEnvInt("JM_FARM_CONCURRENCY", defaultConcurrency(farmEnvInt("JM_FARM_FFMPEG_THREADS", 2))),
			"parallel workers (default ≈ cores / ffmpeg-threads; SET EXPLICITLY in containers — NumCPU is the host count)")
		// Size floor (2026-07-14, user-directed): media below this is skipped
		// ENTIRELY at collection — no tech, no poster/filmstrip/waveform ("if
		// something is too small to be relevant, under 20 megs, we shouldn't [cache
		// it]"). Also passed to farm.Process as MinBlobSizeBytes so any file that
		// slips through (e.g. -files list) still gates its blobs. Env:
		// JM_FARM_MIN_SIZE_MB. 0 = no minimum (process everything).
		minSizeMB = flag.Int("min-size-mb", farmEnvInt("JM_FARM_MIN_SIZE_MB", 20),
			"skip media smaller than this many MB entirely — no derivatives at all (0 = no minimum)")
		producer  = flag.String("producer", "macos-node", "producer tag")
		version   = flag.Int("version", 1, "producer version")
		dryRun    = flag.Bool("dry-run", false, "probe + report, do not write")
		verbose   = flag.Bool("verbose", false, "per-file logging")
		status    = flag.String("status", "", "after the sweep, write a rollup status JSON here (manager Farm tab)")
		reconcile = flag.Bool("reconcile", false, "JM-15: ingest volume manifest.json sidecars into -db (no media processing); closes the farm→client discovery loop")
		emitSide  = flag.Bool("emit-sidecars", false, "JM-15 BACKFILL: re-emit manifest.json for every asset already in -db (no media processing). The inverse of -reconcile: store → volume.")
		queue     = flag.Bool("queue", false, "JM-16: standing worker — drain the shared juicefarm: queue (-meta) instead of a one-shot -root sweep")
		meta      = flag.String("meta", "", "redis:// URL for the queue (required with -queue; the same JM_META the volume uses)")
		// Informational governor knobs: the entrypoint actually APPLIES these via
		// nice/ionice + its sleep loop; jmfarm only RECORDS them in the status
		// stamp so the manager's read-only knob inspector shows real values.
		gNice     = flag.Int("nice", 0, "informational: CPU niceness the entrypoint applied (recorded in status, not applied here)")
		gIONice   = flag.Int("ionice", 0, "informational: best-effort IO class the entrypoint applied (recorded in status, not applied here)")
		gInterval = flag.Int("interval", 0, "informational: seconds between sweeps the entrypoint loops on (recorded in status, not applied here)")
		// Manager-config integration (FARM-NODE-CONFIG spec): -name gives the
		// worker a stable identity for per-node overrides; -kinds declares
		// which dedicated queue(s) this worker drains (empty = catch-all only);
		// -transcript-device selects whisper.cpp's backend (vulkan/cuda/sycl).
		wName   = flag.String("name", defaultStr(os.Getenv("JM_WORKER_NAME"), ""), "stable worker name (manager per-node overrides key)")
		wKinds  = flag.String("kinds", os.Getenv("JM_FARM_KINDS"), "comma-separated kinds this worker drains from dedicated queues (e.g. transcript,proxy); empty = catch-all only")
		wRole   = flag.String("role", defaultStr(os.Getenv("JM_WORKER_ROLE"), "auto"), "worker role: auto|server|render (render requires a verified accelerator)")
		wDevice = flag.String("transcript-device", defaultStr(os.Getenv("JM_FARM_TRANSCRIPT_DEVICE"), "cpu"), "whisper.cpp compute device: cpu|vulkan|cuda|sycl")
	)
	flag.Parse()
	if *buildInfo {
		fmt.Printf("jmfarm %s (%s)\n", buildversion.Version, buildversion.Commit)
		return
	}

	// JM-15 reconcile mode: walk the volume sidecars → local db, then exit. The
	// running app serves the same db (WAL), so reconciled rows appear live.
	// JM-15 BACKFILL. Assets generated before the sidecar emit worked have blobs
	// on the volume and rows in the store, but no manifest.json — so the
	// consumer's reconcile refuses them as "not indexed" and the ENTIRE existing
	// corpus is invisible to it. Measured 2026-08-17: 26,447 known assets,
	// 98,824 derivative rows, and ZERO manifests anywhere on the volume.
	//
	// This re-encodes NOTHING. WriteManifestSidecar reads the committed rows and
	// writes one small JSON per asset, which is why backfilling the whole volume
	// is cheap enough to simply run rather than schedule.
	if *emitSide {
		if *mount == "" {
			fmt.Fprintln(os.Stderr, "jmfarm: -emit-sidecars requires -mount <volume>")
			os.Exit(2)
		}
		store, err := derivatives.Open(*dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: open %s: %v\n", *dbPath, err)
			os.Exit(1)
		}
		defer store.Close()
		inodes, err := store.ListKnownInodes()
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: list inodes: %v\n", err)
			os.Exit(1)
		}
		var wrote, skipped, errs int
		for i, ino := range inodes {
			if *limit > 0 && wrote >= *limit {
				break
			}
			// Known()==false means no source row — WriteManifestSidecar returns
			// nil silently in that case, so count it rather than calling it and
			// reporting a write that never happened.
			if known, _ := store.Known(ino); !known {
				skipped++
				continue
			}
			if err := farm.WriteManifestSidecar(store, *mount, ino); err != nil {
				errs++
				if *verbose || errs <= 5 {
					fmt.Fprintf(os.Stderr, "  inode %d: %v\n", ino, err)
				}
				continue
			}
			wrote++
			if *verbose || (i%2000 == 0 && i > 0) {
				fmt.Printf("  ... %d/%d\n", i, len(inodes))
			}
		}
		fmt.Printf("jmfarm emit-sidecars: %d written, %d skipped (no source row), %d errors, of %d known assets (mount=%s db=%s)\n",
			wrote, skipped, errs, len(inodes), *mount, *dbPath)
		if errs > 0 {
			os.Exit(1)
		}
		return
	}

	if *reconcile {
		if *mount == "" {
			fmt.Fprintln(os.Stderr, "jmfarm: -reconcile requires -mount <volume>")
			os.Exit(2)
		}
		store, err := derivatives.Open(*dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: open %s: %v\n", *dbPath, err)
			os.Exit(1)
		}
		defer store.Close()
		res, err := farm.ReconcileSidecars(store, *mount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: reconcile: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("jmfarm reconcile: %d sidecars → %d assets, %d rows ingested, %d errors (db=%s)\n",
			res.Sidecars, res.Assets, res.Rows, res.Errs, *dbPath)
		return
	}

	// JM-16 queue mode: a standing worker draining the shared juicefarm: queue.
	// It needs BOTH -mount (the FUSE files path the generators read) and -meta
	// (the Redis the queue lives in). All the other flags supply the run defaults
	// a job's options override where non-zero.
	// Apply the ffmpeg thread cap globally before any generator runs (queue or
	// one-shot). Log the effective throughput config so the ratio is auditable.
	farm.SetFFmpegThreads(*ffThreads)
	farm.SetProxyThreads(*proxyThreads)
	fmt.Printf("jmfarm: throughput config — workers=%d ffmpeg-threads=%d proxy-threads=%d (NumCPU=%d) min-blob-size=%dMB\n",
		*conc, *ffThreads, *proxyThreads, runtime.NumCPU(), *minSizeMB)
	minSizeBytes := int64(*minSizeMB) << 20

	if *queue {
		runQueue(queueConfig{
			meta:     *meta,
			dbPath:   *dbPath,
			mount:    *mount,
			producer: *producer,
			version:  *version,
			status:   *status,
			storagePath: func() string {
				if explicit := strings.TrimSpace(os.Getenv("JM_FARM_STORAGE_PATH")); explicit != "" {
					return explicit
				}
				if *status != "" {
					return filepath.Dir(*status)
				}
				return ""
			}(),
			storageMinFree:  farmEnvUint64("JM_FARM_MIN_FREE_BYTES", 0),
			storageCapacity: farmEnvUint64("JM_FARM_STORAGE_CAPACITY_BYTES", 0),
			verbose:         *verbose,
			conc:            *conc,
			pConc:           *pConc,
			vcodec:          *vcodec,
			crf:             *pCRF,
			preset:          *pPreset,
			wModel:          *wModel,
			wBin:            *wBin,
			thumbDim:        *thumbDim,
			filmCell:        *filmCell,
			waveSPP:         *waveSPP,
			minSize:         minSizeBytes,
			gNice:           *gNice,
			gIONice:         *gIONice,
			qlMaxDim:        *qlDim,
			qlSecs:          *qlSecs,
			postAlways:      *postAlways,
			name:            *wName,
			kinds:           splitKinds(*wKinds),
			role:            strings.ToLower(strings.TrimSpace(*wRole)),
			tDevice:         *wDevice,
		})
		return
	}

	if *root == "" && *files == "" {
		fmt.Fprintln(os.Stderr, "jmfarm: need -root <dir> or -files <a,b,c>")
		flag.Usage()
		os.Exit(2)
	}

	targets, err := collectTargets(*root, *files, *limit, minSizeBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jmfarm: %v\n", err)
		os.Exit(1)
	}
	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "jmfarm: no media files found")
		os.Exit(1)
	}

	var store *derivatives.Store
	if !*dryRun {
		store, err = derivatives.Open(*dbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: open %s: %v\n", *dbPath, err)
			os.Exit(1)
		}
		defer store.Close()
	}

	if *transcr && *wModel == "" {
		fmt.Fprintln(os.Stderr, "jmfarm: -transcript requires -whisper-model <ggml model path>")
		os.Exit(2)
	}
	opt := farm.Options{
		Producer: *producer, Version: *version, Mount: *mount,
		Blobs: *blobs, ThumbMaxDim: *thumbDim, Filmstrip: *filmstr, FilmstripCell: *filmCell,
		Waveform: *wave, WaveformSPP: *waveSPP,
		WhisperBin: *wBin, WhisperModel: *wModel,
		ProxyVCodec: *vcodec, ProxyCRF: *pCRF, ProxyPreset: *pPreset,
		MinBlobSizeBytes: minSizeBytes,
		PosterAlways:     *postAlways,
		QLMaxDim:         *qlDim, QLSeconds: *qlSecs,
		// The freshness gate is right to skip by default, but a generator fix
		// leaves behind assets it can now handle and the gate is precisely what
		// stops them being revisited. Until this flag existed RegenerateFresh
		// was set by NOTHING — declared, read in one place, wired to no CLI flag
		// and no queue-job field — so there was no way at all to force a
		// re-derive after fixing a decoder.
		RegenerateFresh: *regen,
	}

	// Proxy transcode pins a core per clip, so it gets its own (lower)
	// concurrency when requested — the manager's CPU governor sets this.
	effConc := *conc
	if *proxyGen && *pConc > 0 {
		effConc = *pConc
	}

	mode := "derivatives"
	if *transcr {
		mode = "transcript(AI)"
	} else if *qlGen {
		mode = "ql-preview"
	} else if *proxyGen {
		mode = "proxy"
	}
	fmt.Printf("jmfarm: %d files → %s  (mode=%s, producer=%s v%d, dry-run=%v, workers=%d)\n",
		len(targets), *dbPath, mode, *producer, *version, *dryRun, effConc)

	// Resolve the target + governor stamp up front so the live progress ticker
	// (inside runPasses) writes the same sweep/governor shape the final write uses.
	target := *root
	if target == "" {
		target = "files:" + *files
	}
	gov := farm.NewGovernor(*wModel, *vcodec, *pPreset, mode,
		*pCRF, *conc, *pConc, *gNice, *gIONice, *gInterval)

	_, failed, _, _, _ := runPasses(passOpts{
		opt: opt, mode: mode, transcr: *transcr, proxyGen: *proxyGen, qlGen: *qlGen,
		dryRun: *dryRun, verbose: *verbose, effConc: effConc,
		status: *status, mount: *mount, producer: *producer,
		target: target, gov: gov, store: store,
	}, targets)

	processed := len(targets) - failed
	if failed > 0 && processed == 0 {
		os.Exit(1)
	}
}

// queueConfig carries the run-default flags into the standing worker loop. A
// drained job's options override the matching field where it is non-zero.
type queueConfig struct {
	meta            string
	dbPath          string
	mount           string
	producer        string
	version         int
	status          string
	storagePath     string
	storageMinFree  uint64
	storageCapacity uint64
	verbose         bool
	conc            int
	pConc           int
	vcodec          string
	crf             int
	preset          string
	wModel          string
	wBin            string
	thumbDim        int
	filmCell        int
	waveSPP         int
	minSize         int64 // skip media smaller than this (bytes); 0 = no minimum
	gNice           int
	gIONice         int
	qlMaxDim        int // QL preview fit box px (T1.1/F4)
	qlSecs          int // QL preview duration cap s (<=0 → default 20)
	postAlways      bool

	// Manager-config integration (FARM-NODE-CONFIG spec):
	name     string                      // stable worker identity (JM_WORKER_NAME / -name)
	kinds    []string                    // declared kinds → per-kind queue drain + watch filter
	role     string                      // auto|server|render; render is admitted only after live probes
	tDevice  string                      // whisper compute device (cpu|vulkan|cuda|sycl)
	progress func(stage string, pct int) // per-job heartbeat detail; nil outside queue execution
}

// restartConfigDrift keeps the Manager's restart badge honest. A config
// document may include restart-class values even when the entrypoint already
// launched this worker with those exact settings; only actual drift should be
// reported as requiring a restart.
func restartConfigDrift(effective map[string]any, requested []string, nice, ionice int) []string {
	current := map[string]int{"nice": nice, "ionice": ionice}
	var drift []string
	for _, key := range requested {
		want, ok := configInteger(effective[key])
		got, known := current[key]
		if !known || !ok || want != got {
			drift = append(drift, key)
		}
	}
	return drift
}

func configInteger(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), n == float64(int(n))
	case int:
		return n, true
	case int64:
		return int(n), int64(int(n)) == n
	default:
		return 0, false
	}
}

// runQueue is the JM-16 standing worker. It dials the queue Redis, opens the
// derivatives store once, then loops { heartbeat; BRPOP; run the job's passes;
// mark done/failed } until SIGTERM/SIGINT. Redis hiccups are logged and the loop
// continues — a transient meta outage must not kill a standing container.
func runQueue(cfg queueConfig) {
	if cfg.meta == "" {
		fmt.Fprintln(os.Stderr, "jmfarm: -queue requires -meta <redis:// URL>")
		os.Exit(2)
	}
	if cfg.mount == "" {
		fmt.Fprintln(os.Stderr, "jmfarm: -queue requires -mount <volume>")
		os.Exit(2)
	}

	q, err := farmqueue.Open(cfg.meta)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jmfarm: queue open %q: %v\n", cfg.meta, err)
		os.Exit(1)
	}
	defer q.Close()

	store, err := derivatives.Open(cfg.dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jmfarm: open %s: %v\n", cfg.dbPath, err)
		os.Exit(1)
	}
	defer store.Close()

	// Cancel the loop on SIGTERM/SIGINT (container stop) for a clean exit.
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ctx, cancelLifecycle := context.WithCancel(signalCtx)
	defer cancelLifecycle()

	profile, profileErr := probeWorkerProfile(ctx, cfg, store)
	if profileErr != nil {
		fmt.Fprintf(os.Stderr, "jmfarm: worker capability admission failed: %v\n", profileErr)
		os.Exit(1)
	}
	worker := farmqueue.Worker{
		ID:                   farmqueue.NewID(),
		StartedAt:            time.Now().UTC().Format(time.RFC3339),
		BuildVersion:         buildversion.Version,
		BuildCommit:          buildversion.Commit,
		Name:                 cfg.name,
		Kinds:                farmqueue.DrainKinds(cfg.kinds),
		Capabilities:         profile.Capabilities,
		Role:                 profile.Role,
		Encoders:             profile.Encoders,
		Decoders:             profile.Decoders,
		DecodeLimits:         profile.DecodeLimits,
		TranscriptBackends:   profile.TranscriptBackends,
		Benchmarks:           profile.Benchmarks,
		RequireStoragePermit: profile.Role == farmqueue.QueueClassRender,
		State:                "idle",
	}
	fmt.Printf("jmfarm queue: worker %s (name=%q role=%s encoders=%v transcript=%v caps=%v) draining %s (db=%s mount=%s producer=%s)\n",
		worker.ID, worker.Name, worker.Role, worker.Encoders, worker.TranscriptBackends,
		worker.Capabilities, cfg.meta, cfg.dbPath, cfg.mount, cfg.producer)
	_ = q.AppendWorkerLog(ctx, worker.ID, fmt.Sprintf("worker online name=%s role=%s build=%s@%s", worker.Name, worker.Role, worker.BuildVersion, worker.BuildCommit))

	// Stable-name lifecycle control is independent of quality/config updates.
	// Disable is a durable admission gate: the worker stays visible with a
	// disabled heartbeat but never claims work. Restart is a one-shot command:
	// acknowledge first, then cancel the process so an unless-stopped container
	// starts a fresh exact artifact. A job-specific cancel lets disable drain an
	// active claim back to its same lane without killing the whole process.
	var nodeDisabled atomic.Bool
	var nodePaused atomic.Bool
	var nodeDrain atomic.Bool
	var restartRequested atomic.Bool
	var currentJobMu sync.Mutex
	var cancelCurrentJob context.CancelFunc
	var lifecycleDone chan struct{}
	if farmqueue.ValidWorkerControlName(cfg.name) {
		if disabled, err := q.WorkerDisabled(ctx, cfg.name); err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: lifecycle admission check: %v\n", err)
		} else {
			nodeDisabled.Store(disabled)
		}
		lifecycleDone = make(chan struct{})
		go func() {
			defer close(lifecycleDone)
			for ctx.Err() == nil {
				// Per-node pause/drain is Manager-owned hot config. Poll it here as
				// well as in the main loop so a long transcode reports draining within
				// one control interval rather than only after it completes.
				if fc, configErr := q.GetConfig(ctx); configErr == nil && fc != nil {
					eff, _ := fc.ResolveFor(cfg.name)
					paused, _ := eff["paused"].(bool)
					drain, _ := eff["drain"].(bool)
					nodePaused.Store(paused)
					nodeDrain.Store(drain)
				}
				disabled, err := q.WorkerDisabled(ctx, cfg.name)
				if err != nil && ctx.Err() == nil {
					fmt.Fprintf(os.Stderr, "jmfarm queue: lifecycle admission poll: %v\n", err)
				} else if err == nil {
					wasDisabled := nodeDisabled.Swap(disabled)
					if disabled && !wasDisabled {
						fmt.Printf("jmfarm queue: node %q disabled; returning active work to its lane\n", cfg.name)
						currentJobMu.Lock()
						cancel := cancelCurrentJob
						currentJobMu.Unlock()
						if cancel != nil {
							cancel()
						}
					} else if !disabled && wasDisabled {
						fmt.Printf("jmfarm queue: node %q enabled; compatible claiming resumed\n", cfg.name)
					}
				}

				cmd, ok, err := q.WaitWorkerCommand(ctx, cfg.name, 2*time.Second)
				if err != nil {
					if ctx.Err() == nil {
						fmt.Fprintf(os.Stderr, "jmfarm queue: lifecycle command poll: %v\n", err)
					}
					continue
				}
				if !ok {
					continue
				}
				ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
				ackErr := q.AcknowledgeWorkerCommand(ackCtx, cmd, worker.ID)
				ackCancel()
				if ackErr != nil {
					fmt.Fprintf(os.Stderr, "jmfarm queue: lifecycle command ack %s: %v\n", cmd.ID, ackErr)
					continue
				}
				restartRequested.Store(true)
				fmt.Printf("jmfarm queue: acknowledged manager restart command %s\n", cmd.ID)
				cancelLifecycle()
				return
			}
		}()
		defer func() {
			cancelLifecycle()
			<-lifecycleDone
		}()
	} else {
		fmt.Fprintln(os.Stderr, "jmfarm queue: lifecycle controls unavailable; set a lowercase stable JM_WORKER_NAME")
	}

	// Manager-config state: last revision applied + restart-class drift. The
	// config doc is polled every loop iteration (idle or post-job); hot knobs
	// swap this worker's run-defaults immediately, restart-class keys land in
	// pendingRestart and surface on the heartbeat for the Farm tab badge.
	cfgMu := sync.Mutex{}
	appliedRev := int64(0)
	var pendingRestart []string

	applyConfig := func(fc *farmqueue.FarmConfig) {
		eff, restart := fc.ResolveFor(cfg.name)
		paused, _ := eff["paused"].(bool)
		drain, _ := eff["drain"].(bool)
		nodePaused.Store(paused)
		nodeDrain.Store(drain)
		cfgMu.Lock()
		defer cfgMu.Unlock()
		appliedRev = fc.Revision
		pendingRestart = restartConfigDrift(eff, restart, cfg.gNice, cfg.gIONice)
		// Hot knobs → mutate the live run-defaults (guarded by cfgMu; runJob
		// reads them through effectiveConfig so a job never sees a torn read).
		if v, ok := eff["crf"].(float64); ok && v >= 1 && v <= 51 {
			cfg.crf = int(v)
		}
		if v, ok := eff["preset"].(string); ok && v != "" {
			cfg.preset = v
		}
		if v, ok := eff["vcodec"].(string); ok && v != "" {
			cfg.vcodec = v
		}
		if v, ok := eff["model"].(string); ok && v != "" {
			cfg.wModel = resolveModelPath(v)
		}
		if v, ok := eff["workers"].(float64); ok && v >= 1 {
			cfg.conc = int(v)
		}
		if v, ok := eff["proxy_workers"].(float64); ok && v >= 0 {
			cfg.pConc = int(v)
		}
		if v, ok := eff["ffmpeg_threads"].(float64); ok && v >= 0 {
			farm.SetFFmpegThreads(int(v))
		}
		if v, ok := eff["transcript_device"].(string); ok && v != "" {
			cfg.tDevice = v
		}
		fmt.Printf("jmfarm queue: applied config rev %d (restart-class pending: %v)\n", fc.Revision, pendingRestart)
	}

	pollConfig := func() {
		fc, err := q.GetConfig(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: config poll: %v\n", err)
			return
		}
		if fc == nil {
			return // unmanaged (no key) — keep current defaults
		}
		cfgMu.Lock()
		cur := appliedRev
		cfgMu.Unlock()
		if fc.Revision != cur {
			// Revision went BACKWARD (key deleted + recreated): accept it too,
			// the manager is the source of truth either way.
			applyConfig(fc)
		}
	}
	pollConfig() // initial apply before first heartbeat

	setHeartbeatExtras := func(w *farmqueue.Worker) {
		cfgMu.Lock()
		defer cfgMu.Unlock()
		w.ConfigRevision = appliedRev
		w.PendingRestart = pendingRestart
		w.Effective = map[string]string{
			"crf":               fmt.Sprint(cfg.crf),
			"preset":            cfg.preset,
			"vcodec":            cfg.vcodec,
			"model":             filepath.Base(cfg.wModel),
			"workers":           fmt.Sprint(cfg.conc),
			"proxy_workers":     fmt.Sprint(cfg.pConc),
			"transcript_device": cfg.tDevice,
		}
	}

	// Discovery is a server/control-plane responsibility. Render workers only
	// execute accelerator jobs; subscribing every GPU node made any of them able
	// to become discovery leader and run recursive metadata walks remotely.
	if workerRunsDiscovery(worker) {
		kinds := farm.WatchKindsFromEnv()
		var watchLeader atomic.Bool
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				ctl, ctlErr := q.GetControl(ctx)
				if ctlErr == nil && !nodeDisabled.Load() && !nodePaused.Load() && !nodeDrain.Load() && !ctl.Paused && ctl.WatchEnabled {
					leader, leaderErr := q.AcquireWatchLeadership(ctx, worker.ID, 45*time.Second)
					watchLeader.Store(leaderErr == nil && leader)
				} else {
					watchLeader.Store(false)
					_ = q.ReleaseWatchLeadership(context.Background(), worker.ID)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		watcher := farm.NewWatcher(farm.WatchConfig{
			MetaURL: cfg.meta,
			Mount:   cfg.mount,
			Enabled: func(context.Context) bool { return watchLeader.Load() },
			Enqueue: func(ectx context.Context, relDir string) error {
				// Job.Path is worker-absolute by convention (runJob walks it
				// verbatim; the manager passes its callers' /jfs paths through
				// unmodified) — live-proven: a mount-relative path fails the
				// runner's lstat. The watcher core stays mount-relative for
				// filtering; join here at the seam.
				_, err := q.EnqueueDiscovered(ectx, filepath.Join(cfg.mount, relDir), kinds, "farm-watch")
				return err
			},
			Logf: func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) },
		})
		go func() {
			if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "farm watch: exited: %v (manager sweeps remain the discovery path)\n", err)
			}
		}()
		go runWatchBackstop(ctx, cfg, q, worker.ID, kinds)
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer releaseCancel()
			_ = q.ReleaseWatchLeadership(releaseCtx, worker.ID)
		}()
	} else if !farm.WatchEnabled() {
		fmt.Fprintln(os.Stderr, "farm watch: disabled (JM_FARM_WATCH=0)")
	} else {
		fmt.Fprintln(os.Stderr, "farm watch: render node does not run server-side discovery")
	}

	nextReap := time.Time{}
	storagePermitBlocked := false
	for {
		if ctx.Err() != nil {
			break
		}

		// Manager-config poll: cheap GET; applies only on revision change.
		pollConfig()

		ctl, ctlErr := q.GetControl(ctx)
		if ctlErr != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: control poll: %v\n", ctlErr)
			ctl = farmqueue.DefaultFarmControl()
		}
		// Only the server/control-plane worker probes physical backend
		// headroom. A render node's /state and cache are local disks and cannot
		// tell whether the NAS object/metadata pool is about to exhaust. Safe
		// probes refresh a short Redis permit; unsafe or missing probes revoke it.
		// Render claim admission checks that permit in the same Lua transaction
		// that moves a job into its durable processing list.
		if worker.Role == farmqueue.QueueClassServer && cfg.storagePath != "" {
			headroom, probeErr := checkWorkerStorageHeadroom(
				cfg.storagePath, cfg.storageMinFree, cfg.storageCapacity)
			unsafe := probeErr != nil || headroom.Available < headroom.Required
			if unsafe {
				if revokeErr := q.RevokeStoragePermit(ctx); revokeErr != nil {
					fmt.Fprintf(os.Stderr, "jmfarm queue: storage permit revoke: %v\n", revokeErr)
				}
				code := "storage-pressure"
				reason := "backend free space is below the farm safety reserve; reclaim storage before resuming"
				if probeErr != nil {
					code = "storage-probe-failed"
					reason = "backend storage headroom could not be verified"
				}
				if !ctl.Paused {
					stored, pauseErr := q.PauseForSafety(ctx, code, reason)
					if pauseErr != nil {
						fmt.Fprintf(os.Stderr, "jmfarm queue: storage safety check: %v (pause failed: %v)\n", probeErr, pauseErr)
					} else {
						ctl = stored
						fmt.Fprintf(os.Stderr, "jmfarm queue: safety pause: %s\n", reason)
					}
				}
			} else if permitErr := q.PublishStoragePermit(ctx, farmqueue.StoragePermit{
				Source: "server-worker", TotalBytes: headroom.Total,
				AvailableBytes: headroom.Available, RequiredBytes: headroom.Required,
			}); permitErr != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: storage permit refresh: %v\n", permitErr)
			}
		}

		storagePermitReady := true
		var storagePermitErr error
		if worker.RequireStoragePermit {
			_, storagePermitErr = q.GetStoragePermit(ctx)
			storagePermitReady = storagePermitErr == nil
			if !storagePermitReady && !storagePermitBlocked {
				fmt.Fprintf(os.Stderr, "jmfarm queue: render admission waiting for fresh backend storage permit: %v\n", storagePermitErr)
			} else if storagePermitReady && storagePermitBlocked {
				fmt.Fprintln(os.Stderr, "jmfarm queue: backend storage permit restored; render claiming eligible")
			}
			storagePermitBlocked = !storagePermitReady
		}

		// Heartbeat (idle/paused): publish presence so Manager can distinguish a
		// healthy paused node from an offline one.
		worker.CurrentJob = ""
		worker.CurrentActivity = nil
		worker.State = "idle"
		if nodeDisabled.Load() {
			worker.State = "disabled"
		} else if ctl.Paused {
			worker.State = "paused"
		} else if nodeDrain.Load() {
			worker.State = "draining"
		} else if nodePaused.Load() {
			worker.State = "paused"
		} else if !storagePermitReady {
			worker.State = "waiting-storage-permit"
		}
		setHeartbeatExtras(&worker)
		if err := q.Heartbeat(ctx, worker); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: heartbeat: %v\n", err)
		}

		// Queue maintenance is not work execution. It must continue while the farm
		// is paused so a worker that disappears during a pause cannot strand a
		// durable processing receipt forever. ClaimForWorker independently checks
		// the control document atomically, so recovered jobs remain unclaimable
		// until play/resume.
		if nextReap.IsZero() || time.Now().After(nextReap) {
			if recovered, err := q.RecoverAbandonedWorkers(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: recovery scan: %v\n", err)
			} else if recovered > 0 {
				fmt.Printf("jmfarm queue: recovered %d abandoned job(s)\n", recovered)
			}
			if recovered, err := q.RecoverUnserviceableReady(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: ready-lane recovery: %v\n", err)
			} else if recovered > 0 {
				fmt.Printf("jmfarm queue: re-routed %d unserviceable render job(s)\n", recovered)
			}
			if promoted, err := q.PromoteServiceableReady(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: ready-lane promotion: %v\n", err)
			} else if promoted > 0 {
				fmt.Printf("jmfarm queue: promoted %d temporary CPU fallback job(s) to verified hardware\n", promoted)
			}
			nextReap = time.Now().Add(15 * time.Second)
		}
		if nodeDisabled.Load() || nodePaused.Load() || nodeDrain.Load() || ctl.Paused || !storagePermitReady {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}

		claim, ok, err := q.ClaimForWorker(ctx, 5*time.Second, worker)
		if err != nil {
			if ctx.Err() != nil {
				break // cancelled mid-BRPOP → clean exit
			}
			// Redis disconnect / transient error: log + back off briefly, keep looping.
			fmt.Fprintf(os.Stderr, "jmfarm queue: claim: %v\n", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if !ok {
			continue // timeout, nothing waiting → loop re-heartbeats
		}
		job := claim.Job

		// Claim the job: stamp current_job on the heartbeat, flip it to running.
		worker.CurrentJob = job.ID
		worker.State = "working"
		activityMu := sync.RWMutex{}
		activity := farmqueue.WorkerActivity{
			ID: job.ID, Kind: strings.Join(job.Kinds, ","), Path: job.Path,
			StartedAt: time.Now().UTC().Format(time.RFC3339), Stage: "claimed",
		}
		setActivity := func(stage string, pct int) {
			if pct < 0 {
				pct = 0
			}
			if pct > 100 {
				pct = 100
			}
			activityMu.Lock()
			activity.Stage = stage
			activity.Pct = pct
			activityMu.Unlock()
		}
		snapshotActivity := func() *farmqueue.WorkerActivity {
			activityMu.RLock()
			defer activityMu.RUnlock()
			copy := activity
			return &copy
		}
		worker.CurrentActivity = snapshotActivity()
		setHeartbeatExtras(&worker)
		if err := q.Heartbeat(ctx, worker); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: heartbeat(claim): %v\n", err)
		}
		if err := q.MarkClaimRunning(ctx, claim, worker); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: mark-running %s: %v\n", job.ID, err)
		}
		_ = q.AppendWorkerLog(ctx, worker.ID, fmt.Sprintf("claimed job %s kind=%s backend=%s", job.ID, strings.Join(job.Kinds, ","), job.SelectedBackend))

		// Keep the heartbeat fresh DURING the job. A big sweep (1000s of files,
		// proxy/transcript) runs for hours inside runJob; without this the 30s
		// worker TTL expires mid-job and the worker falsely reads as "offline"
		// (ActiveWorkers empty) even though it's actively processing. A ~10s
		// ticker (well inside WorkerTTL) holds it alive until the job returns.
		hbCtx, hbStop := context.WithCancel(ctx)
		hbDone := make(chan struct{})
		hbWorker := worker
		go func() {
			defer close(hbDone)
			t := time.NewTicker(3 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-hbCtx.Done():
					return
				case <-t.C:
					hbWorker.CurrentActivity = snapshotActivity()
					if nodeDrain.Load() {
						hbWorker.State = "draining"
					} else if nodePaused.Load() {
						hbWorker.State = "paused"
					} else {
						hbWorker.State = "working"
					}
					_ = q.Heartbeat(hbCtx, hbWorker)
					_ = q.RenewClaim(hbCtx, claim)
				}
			}
		}()

		cfgMu.Lock()
		jobCfg := cfg
		cfgMu.Unlock()
		jobCfg.progress = setActivity
		jobCtx, jobCancel := context.WithCancel(ctx)
		currentJobMu.Lock()
		cancelCurrentJob = jobCancel
		currentJobMu.Unlock()
		jobStarted := time.Now()
		var operatorCancel atomic.Bool
		cancelPollCtx, cancelPollStop := context.WithCancel(ctx)
		cancelPollDone := make(chan struct{})
		if requested, cancelErr := q.JobCancellationRequested(cancelPollCtx, job.ID); cancelErr == nil && requested {
			operatorCancel.Store(true)
			jobCancel()
		}
		go func() {
			defer close(cancelPollDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				requested, err := q.JobCancellationRequested(cancelPollCtx, job.ID)
				if err == nil && requested {
					operatorCancel.Store(true)
					jobCancel()
					return
				}
				select {
				case <-cancelPollCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		processed, failed, failedTargets, runErr := runJob(jobCtx, q, store, jobCfg, worker, job)
		currentJobMu.Lock()
		cancelCurrentJob = nil
		currentJobMu.Unlock()
		jobWasCancelled := jobCtx.Err() != nil
		jobCancel()
		cancelPollStop()
		<-cancelPollDone
		hbStop()
		<-hbDone
		// Backend writeback failure says nothing about codec/device speed or
		// reliability. Do not poison measured scheduling history with a storage
		// outage that every worker would have hit.
		if !errors.Is(runErr, errFarmStoragePressure) {
			recordCompletedJobBenchmark(&worker, job, processed, failed, time.Since(jobStarted))
		}
		if ctx.Err() != nil {
			// Leave the durable processing receipt intact. A live worker will
			// recover it after this heartbeat expires.
			break
		}
		if operatorCancel.Load() {
			if err := q.MarkCanceled(context.Background(), job.ID, job.ProcessedOffset+processed, failed); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: mark-canceled %s: %v\n", job.ID, err)
				return
			}
			if err := q.AckClaim(context.Background(), claim); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: ack canceled %s: %v\n", job.ID, err)
				return
			}
			_ = q.ClearJobCancellation(context.Background(), job.ID)
			_ = q.AppendWorkerLog(context.Background(), worker.ID, fmt.Sprintf("job %s canceled by operator after %d completed", job.ID, job.ProcessedOffset+processed))
			continue
		}
		if nodeDisabled.Load() && jobWasCancelled {
			if len(failedTargets) > 0 {
				claim.Job.RetryTargets = failedTargets
			}
			claim.Job.ProcessedOffset += processed
			if err := q.RequeueClaimSameRoute(context.Background(), claim, "worker disabled by Manager; claim returned without changing backend admission"); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: disable requeue %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			continue
		}
		if errors.Is(runErr, errJobDispatched) {
			if err := q.MarkDispatched(context.Background(), job.ID); err != nil {
				// Children are idempotently durable, but the parent receipt is our
				// recovery proof until its truthful state is committed. Stop and let
				// another worker replay the claim instead of acknowledging ambiguity.
				fmt.Fprintf(os.Stderr, "jmfarm queue: mark-dispatched %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			if err := q.AckClaim(context.Background(), claim); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: ack dispatched parent %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			continue
		}
		if errors.Is(runErr, errJobDispatchRetry) {
			if err := q.RequeueClaimSameRoute(context.Background(), claim, runErr.Error()); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: dispatch retry %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			continue
		}
		if errors.Is(runErr, errFarmStoragePressure) {
			reason := "storage pressure interrupted durable output; farm auto-paused and job retained on its proven route: " + runErr.Error()
			if _, err := q.PauseForSafety(context.Background(), "storage-pressure", reason); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: safety pause %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			if len(failedTargets) == 0 {
				// Every requested media output completed; only the auxiliary status
				// rollup exposed the backend fault. Finish this claim exactly once
				// while leaving the global safety pause engaged. Requeueing an empty
				// retry set would re-walk the directory and double-count freshness
				// skips after the operator restores headroom.
				if err := q.MarkDone(context.Background(), job.ID, job.ProcessedOffset+processed, failed); err != nil {
					fmt.Fprintf(os.Stderr, "jmfarm queue: mark storage-complete %s: %v; stopping with durable claim intact\n", job.ID, err)
					return
				}
				if err := q.AckClaim(context.Background(), claim); err != nil {
					fmt.Fprintf(os.Stderr, "jmfarm queue: ack storage-complete %s: %v; stopping with durable claim intact\n", job.ID, err)
					return
				}
				fmt.Fprintf(os.Stderr, "jmfarm queue: %s (media complete; claim acknowledged)\n", reason)
				continue
			}
			if len(failedTargets) > 0 {
				claim.Job.RetryTargets = failedTargets
			}
			claim.Job.ProcessedOffset += processed
			if err := q.RequeueClaimSameRoute(context.Background(), claim, reason); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: storage requeue %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			fmt.Fprintf(os.Stderr, "jmfarm queue: %s\n", reason)
			continue
		}
		if requeue, fallbackCPU, reason := renderFailureDisposition(worker, job, runErr); requeue {
			if len(failedTargets) > 0 {
				claim.Job.RetryTargets = failedTargets
			}
			claim.Job.ProcessedOffset += processed
			if err := q.RequeueClaimAfterHardwareFailure(context.Background(), claim, fallbackCPU, reason); err != nil {
				// The durable receipt is still intact. Stop this worker so its
				// heartbeat expires and another worker's reaper can recover it;
				// continuing while we still own the claim would strand the job.
				fmt.Fprintf(os.Stderr, "jmfarm queue: failure requeue %s: %v; stopping with durable claim intact\n", job.ID, err)
				return
			}
			continue
		}
		terminalWritten := false
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: job %s failed: %v\n", job.ID, runErr)
			if err := q.MarkFailedWithCounts(context.Background(), job.ID, job.ProcessedOffset+processed, failed, runErr.Error()); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: mark-failed %s: %v\n", job.ID, err)
			} else {
				terminalWritten = true
			}
		} else {
			if err := q.MarkDone(context.Background(), job.ID, job.ProcessedOffset+processed, failed); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: mark-done %s: %v\n", job.ID, err)
			} else {
				terminalWritten = true
			}
		}
		if terminalWritten {
			if err := q.AckClaim(context.Background(), claim); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: ack %s: %v\n", job.ID, err)
			} else {
				state := "done"
				if runErr != nil {
					state = "failed"
				}
				_ = q.AppendWorkerLog(context.Background(), worker.ID, fmt.Sprintf("job %s %s processed=%d failed=%d", job.ID, state, job.ProcessedOffset+processed, failed))
			}
		}
	}

	if restartRequested.Load() {
		fmt.Printf("jmfarm queue: worker %s stopping (manager restart acknowledged)\n", worker.ID)
	} else {
		fmt.Printf("jmfarm queue: worker %s stopping (signal)\n", worker.ID)
	}
}

var (
	errJobDispatched      = errors.New("job dispatched into bounded target shards")
	errJobDispatchRetry   = errors.New("job target dispatch needs same-route retry")
	errRenderExecution    = errors.New("verified render execution failed")
	errRenderIncompatible = errors.New("source cannot stay on the verified hardware decoder")
)

func workerRunsDiscovery(worker farmqueue.Worker) bool {
	return farm.WatchEnabled() && worker.Role == farmqueue.QueueClassServer
}

func recordCompletedJobBenchmark(worker *farmqueue.Worker, job farmqueue.Job, processed, failed int, elapsed time.Duration) {
	if worker == nil || processed+failed == 0 {
		// Empty/no-media discovery jobs are skipped work, not performance
		// samples. Counting them drove LastJobSeconds toward zero and made the
		// scheduler prefer nodes that had only scanned benchmark directories.
		return
	}
	seconds := elapsed.Seconds()
	worker.Benchmarks.LastJobSeconds = seconds
	worker.Benchmarks.JobsCompleted++
	worker.Benchmarks.FilesProcessed += int64(processed)
	worker.Benchmarks.FilesFailed += int64(failed)
	if len(job.Kinds) != 1 {
		return
	}
	switch job.Kinds[0] {
	case farmqueue.KindProxy:
		worker.Benchmarks.LastProxySeconds = seconds
		worker.Benchmarks.ProxyJobsCompleted++
		worker.Benchmarks.ProxyFilesProcessed += int64(processed)
		worker.Benchmarks.ProxyFilesFailed += int64(failed)
	case farmqueue.KindTranscript:
		worker.Benchmarks.LastTranscriptSeconds = seconds
		worker.Benchmarks.TranscriptJobsCompleted++
		worker.Benchmarks.TranscriptFilesProcessed += int64(processed)
		worker.Benchmarks.TranscriptFilesFailed += int64(failed)
	}
}

// runJob runs the passes a single dequeued job asks for, scoped to job.Path, with
// the job's non-zero options overriding the container run-defaults. It returns
// the summed processed/failed across the selected passes; a non-nil error is a
// HARD failure (couldn't collect targets / nothing usable) → MarkFailed.
func runJob(ctx context.Context, q *farmqueue.Client, store *derivatives.Store, cfg queueConfig, worker farmqueue.Worker, job farmqueue.Job) (processed, failed int, failedTargets []string, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, nil, err
	}
	if cfg.progress != nil {
		cfg.progress("discovering", 0)
	}
	if !farmqueue.WorkerSupports(worker, job.RequiredCapabilities) {
		return 0, 0, nil, fmt.Errorf("worker %s does not satisfy job capabilities %v", worker.Name, job.RequiredCapabilities)
	}
	// Resolve per-job overrides over the container defaults (zero ⇒ keep default).
	crf := cfg.crf
	if job.CRF != 0 {
		crf = job.CRF
	}
	preset := cfg.preset
	if job.Preset != "" {
		preset = job.Preset
	}
	vcodec := cfg.vcodec
	if job.VCodec != "" {
		vcodec = job.VCodec
	}
	wModel := cfg.wModel
	if job.Model != "" {
		wModel = job.Model
	}
	conc := cfg.conc
	if job.Workers > 0 {
		conc = job.Workers
	}
	pConc := cfg.pConc
	if job.ProxyWorkers > 0 {
		pConc = job.ProxyWorkers
	}
	tDevice := cfg.tDevice
	if len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindTranscript && job.SelectedBackend != "" {
		tDevice = job.SelectedBackend
	}

	// Collect the media under the job's path once; every selected pass sweeps the
	// same target set. cfg.minSize applies the same size floor here (skip entirely)
	// that MinBlobSizeBytes applies in Process — single source, the -min-size-mb flag.
	var targets []string
	var cErr error
	if len(job.RetryTargets) > 0 {
		targets, cErr = collectRetryTargets(cfg.mount, job.Path, job.RetryTargets, cfg.minSize)
	} else {
		targets, cErr = collectTargets(job.Path, "", 0, cfg.minSize)
	}
	if cErr != nil {
		return 0, 0, nil, fmt.Errorf("collect %q: %w", job.Path, cErr)
	}
	if cfg.progress != nil {
		cfg.progress("planning", 0)
	}

	// FARM-3: run the software-only decode classes first (HEVC Rext 4:2:2/4:4:4,
	// XF-AVC 4K60) — the media where a farm proxy actually changes whether
	// playback is usable. Only reorders media we have ALREADY probed; the codec
	// is not knowable from a filesystem walk, and paying an ffprobe per file just
	// to decide the order would cost more than the ordering saves. See
	// farm.PrioritizeTargets.
	targets = farm.PrioritizeTargets(targets, func(p string) *farm.VideoTrack {
		return knownVideoTrack(store, p)
	})
	if len(targets) == 0 {
		// Not a failure: an empty path OR one whose media is all excluded (a
		// Proxy/ folder, sub-threshold clips) legitimately has nothing to
		// derive. Marking it done-with-zero keeps proxy dirs out of the failed
		// count — otherwise every excluded folder shows up as a false failure.
		fmt.Printf("jmfarm queue: job %s path=%q — no derivable media (empty or all-excluded); nothing to do\n",
			job.ID, job.Path)
		return 0, 0, nil, nil
	}
	if shouldDispatchDerivativePlan(job) {
		children, created, planErr := dispatchDerivativePlan(ctx, q, store, job, targets)
		if planErr != nil {
			return 0, 0, nil, fmt.Errorf("%w: publish derivative execution plan: %v", errJobDispatchRetry, planErr)
		}
		fmt.Printf("jmfarm queue: job %s released composite derivatives into %d bounded child job(s), %d newly queued\n",
			job.ID, children, created)
		return 0, 0, nil, errJobDispatched
	}
	if shouldDispatchProxyPlan(job) {
		children, created, planErr := dispatchProxyPlan(ctx, q, store, job, targets, targetShardSize(job))
		if planErr != nil {
			return 0, 0, nil, fmt.Errorf("%w: publish proxy execution plan: %v", errJobDispatchRetry, planErr)
		}
		if children == 0 {
			fmt.Printf("jmfarm queue: job %s found no video sources requiring proxies; nothing to dispatch\n", job.ID)
			return 0, 0, nil, nil
		}
		fmt.Printf("jmfarm queue: job %s released source-aware proxy plan into %d bounded child job(s), %d newly queued\n",
			job.ID, children, created)
		return 0, 0, nil, errJobDispatched
	}
	if shardSize := targetShardSize(job); q != nil && shardSize > 0 && (job.PlanOnly || len(targets) > shardSize) {
		children, created, splitErr := q.EnqueueTargetShards(ctx, job, targets, shardSize)
		if splitErr != nil {
			return 0, 0, nil, fmt.Errorf("%w: enqueue bounded target shards: %v", errJobDispatchRetry, splitErr)
		}
		fmt.Printf("jmfarm queue: job %s released directory claim into %d bounded shard(s), %d newly queued, max %d files each\n",
			job.ID, len(children), created, shardSize)
		// The durable children now own every target. A distinct non-failure result
		// lets the queue loop mark the parent as dispatched rather than falsely
		// claiming it processed zero files successfully.
		return 0, 0, nil, errJobDispatched
	}
	if len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindProxy && worker.Role == farmqueue.QueueClassRender {
		hardwareTargets, fallbackTargets := partitionRenderProxyTargets(worker, vcodec, targets, probeRenderVideoTrack)
		if len(fallbackTargets) > 0 {
			paths := make([]string, 0, len(fallbackTargets))
			for _, target := range fallbackTargets {
				paths = append(paths, target.Path)
			}
			reason := fmt.Sprintf("%d source(s) partitioned to explicit CPU/H.264 fallback; first reason: %s",
				len(fallbackTargets), fallbackTargets[0].Reason)
			fallbackJob, created, splitErr := q.EnqueueCPUFallbackSubset(ctx, job, paths, reason)
			if splitErr != nil {
				return 0, 0, nil, fmt.Errorf("%w: enqueue CPU fallback subset: %v", errJobDispatchRetry, splitErr)
			}
			state := "already present in"
			if created {
				state = "queued as"
			}
			fmt.Printf("jmfarm queue: job %s kept %d source(s) on %s; %d source(s) %s CPU child %s\n",
				job.ID, len(hardwareTargets), vcodec, len(fallbackTargets), state, fallbackJob.ID)
		}
		targets = hardwareTargets
		if len(targets) == 0 {
			// The CPU child owns every source. Completing this render parent frees
			// the accelerator immediately and leaves an inspectable split record.
			return 0, 0, nil, nil
		}
	}
	liveDecodeBitDepth := job.SourceBitDepth
	if len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindDerivatives &&
		job.DerivativePass == farmqueue.DerivativePassPreviews && worker.Role == farmqueue.QueueClassRender {
		for _, target := range targets {
			track, probeErr := probeRenderVideoTrack(target)
			if probeErr != nil {
				return 0, 1, []string{target}, fmt.Errorf("%w: live preview probe failed: %v", errRenderIncompatible, probeErr)
			}
			if reason := unsupportedHardwareDecodeReason(worker, job.SelectedBackend, track); reason != "" {
				return 0, 1, []string{target}, fmt.Errorf("%w: %s", errRenderIncompatible, reason)
			}
			if track != nil {
				liveDecodeBitDepth = track.BitDepth
			}
		}
	}

	// Expand the job's kinds into the concrete passes to run, in cheap→expensive
	// order so the fast derivatives publish before the slow proxy/transcript.
	kinds := normalizeKinds(job.Kinds)

	type pass struct {
		mode     string
		transcr  bool
		proxyGen bool
		qlGen    bool
		effConc  int
	}
	var passes []pass
	if kinds[KindDerivatives] {
		passes = append(passes, pass{mode: "derivatives", effConc: conc})
	}
	if kinds[KindQLPreview] {
		passes = append(passes, pass{mode: "ql-preview", qlGen: true, effConc: conc})
	}
	if kinds[KindProxy] {
		ec := conc
		if pConc > 0 {
			ec = pConc
		}
		passes = append(passes, pass{mode: "proxy", proxyGen: true, effConc: ec})
	}
	if kinds[KindTranscript] {
		if wModel == "" {
			return 0, 0, nil, fmt.Errorf("transcript kind needs a whisper model (job.model or -whisper-model)")
		}
		passes = append(passes, pass{mode: "transcript(AI)", transcr: true, effConc: conc})
	}
	if len(passes) == 0 {
		return 0, 0, nil, fmt.Errorf("job %s has no runnable kinds (%v)", job.ID, job.Kinds)
	}
	var failureDetails []string
	var storageFailure error

	base := farm.Options{
		Context:  ctx,
		Producer: cfg.producer, Version: cfg.version, Mount: cfg.mount,
		Blobs: true, ThumbMaxDim: cfg.thumbDim, Filmstrip: true, FilmstripCell: cfg.filmCell,
		Waveform: true, WaveformSPP: cfg.waveSPP,
		WhisperBin: cfg.wBin, WhisperModel: wModel,
		TranscriptDevice: tDevice,
		ProxyVCodec:      vcodec, ProxyCRF: crf, ProxyPreset: preset,
		// A CPU lane selected by the scheduler is an explicit fallback. Its
		// directory retry must fill only the files hardware could not publish;
		// existing HEVC results are already better and must not be downgraded.
		PreserveHEVCOnFallback: job.QueueClass == farmqueue.QueueClassCPU && vcodec == "libx264",
		// Automatic sweeps fill missing output; they do not silently migrate a
		// current proxy between codecs. On JuiceFS, every such replacement keeps
		// the old slices for the trash-retention window and can consume terabytes.
		PreserveExistingProxy: true,
		MinBlobSizeBytes:      cfg.minSize,
		PosterAlways:          cfg.postAlways,
		QLMaxDim:              cfg.qlMaxDim, QLSeconds: cfg.qlSecs,
	}
	if len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindDerivatives {
		switch job.DerivativePass {
		case farmqueue.DerivativePassMetadata:
			base.Blobs = false
			base.Filmstrip = false
			base.Waveform = true
		case farmqueue.DerivativePassPreviews:
			base.Blobs = true
			base.Filmstrip = true
			base.Waveform = false
			if worker.Role == farmqueue.QueueClassRender {
				base.VideoDecoder = job.SelectedBackend
				base.VideoDecodeBitDepth = liveDecodeBitDepth
			}
		}
	}

	fmt.Printf("jmfarm queue: job %s path=%q kinds=%v files=%d\n",
		job.ID, job.Path, job.Kinds, len(targets))

	for _, p := range passes {
		if ctx.Err() != nil {
			break // cancelled between passes → stop cleanly, report what we did
		}
		gov := farm.NewGovernor(wModel, vcodec, preset, p.mode, crf, conc, pConc, cfg.gNice, cfg.gIONice, 0)
		pr, pf, failedPaths, details, safetyErr := runPasses(passOpts{
			ctx: ctx,
			opt: base, mode: p.mode, transcr: p.transcr, proxyGen: p.proxyGen, qlGen: p.qlGen,
			dryRun: false, verbose: cfg.verbose, effConc: p.effConc,
			status: cfg.status, mount: cfg.mount, producer: cfg.producer,
			target: job.Path, gov: gov, store: store,
			progress: cfg.progress,
		}, targets)
		processed += pr
		failed += pf
		failedTargets = append(failedTargets, failedPaths...)
		failureDetails = append(failureDetails, details...)
		if storageFailure == nil && safetyErr != nil {
			storageFailure = safetyErr
		}
		if safetyErr != nil {
			break
		}
	}
	if storageFailure != nil {
		return processed, failed, uniqueSortedStrings(failedTargets),
			fmt.Errorf("%w: %v", errFarmStoragePressure, storageFailure)
	}
	if failed > 0 {
		sort.Strings(failedTargets)
		passErr := passFailure(processed, failed)
		if len(failureDetails) > 0 {
			passErr = fmt.Errorf("%w; first errors: %s", passErr, strings.Join(failureDetails, "; "))
		}
		if worker.Role == farmqueue.QueueClassRender {
			passErr = fmt.Errorf("%w: %v", errRenderExecution, passErr)
		}
		return processed, failed, uniqueSortedStrings(failedTargets), passErr
	}
	return processed, failed, nil, nil
}

func shouldDispatchDerivativePlan(job farmqueue.Job) bool {
	return len(job.Kinds) == 1 && job.Kinds[0] == farmqueue.KindDerivatives && job.DerivativePass == ""
}

// targetShardSize caps one durable lease while preserving each worker's useful
// in-process parallelism. Proxy/transcript work is latency-heavy and gets small
// shards. The derivative size remains only for migration of composite legacy
// jobs; current derivative planning owns metadata and previews separately.
// A child carries ShardCount and is never split recursively.
func targetShardSize(job farmqueue.Job) int {
	if job.ShardCount > 0 || len(job.Kinds) != 1 {
		return 0
	}
	clamp := func(value, low, high int) int {
		if value < low {
			return low
		}
		if value > high {
			return high
		}
		return value
	}
	switch job.Kinds[0] {
	case farmqueue.KindProxy:
		// Use only immutable job fields: a crash replay on a differently sized
		// worker must produce identical boundaries for the same child IDs.
		effective := job.ProxyWorkers
		if effective <= 0 {
			effective = 2
		}
		return clamp(effective*2, 2, 8)
	case farmqueue.KindTranscript:
		return 1
	case farmqueue.KindDerivatives:
		effective := job.Workers
		if effective <= 0 {
			effective = 8
		}
		return clamp(effective*4, 16, 64)
	default:
		return 0
	}
}

const renderHardwareRetries = 1

// renderFailureDisposition keeps CPU fallback visible and deliberate. A
// transient accelerator admission failure first gets one newly routed hardware
// retry (which may select another live GPU); only a repeated hardware failure is
// demoted to the server's CPU/H.264 lane. Successful files are protected by the
// freshness gate, so retrying a partially successful batch only recomputes the
// files that did not publish a valid proxy.
func renderFailureDisposition(worker farmqueue.Worker, job farmqueue.Job, runErr error) (requeue, fallbackCPU bool, reason string) {
	if worker.Role != farmqueue.QueueClassRender {
		return false, false, ""
	}
	if errors.Is(runErr, errRenderIncompatible) {
		return true, true, fmt.Sprintf("live source admission rejected the selected hardware decoder; queued explicit CPU decode fallback: %v", runErr)
	}
	if !errors.Is(runErr, errRenderExecution) {
		return false, false, ""
	}
	if job.HardwareFailures < renderHardwareRetries {
		return true, false, fmt.Sprintf("render backend failed; retrying on a verified hardware worker: %v", runErr)
	}
	return true, true, fmt.Sprintf("render backend failed after %d hardware retries; queued explicit CPU/H.264 fallback: %v",
		renderHardwareRetries, runErr)
}

func passFailure(processed, failed int) error {
	if failed <= 0 {
		return nil
	}
	return fmt.Errorf("%d of %d media operations failed; inspect worker logs for the per-file errors",
		failed, processed+failed)
}

// defaultStr returns s if non-empty, otherwise fallback (env-fallback flag
// defaults — same pattern as cmd/jm5).
func defaultStr(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}

// splitKinds parses a comma-separated kinds list, lowercases and validates it
// against the known kinds (unknown entries are dropped with a warning).
func splitKinds(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.ToLower(strings.TrimSpace(p))
		switch p {
		case farmqueue.KindDerivatives, farmqueue.KindProxy, farmqueue.KindTranscript, farmqueue.KindAll:
			out = append(out, p)
		case "":
		default:
			fmt.Fprintf(os.Stderr, "jmfarm: -kinds: dropping unknown kind %q\n", p)
		}
	}
	return out
}

// detectCapabilities self-reports what this worker can do, from the device
// setting plus cheap binary probes. Best-effort: capability detection failure
// never blocks startup — the manager UI just shows fewer badges.
func detectCapabilities(tDevice string) []string {
	caps := []string{"cpu"}
	if tDevice != "" && tDevice != "cpu" {
		caps = append(caps, tDevice)
	}
	// VAAPI presence: a working iHD driver on /dev/dri/renderD128.
	if _, err := os.Stat("/dev/dri/renderD128"); err == nil {
		if out, err := exec.Command("vainfo", "--display", "drm",
			"--device", "/dev/dri/renderD128").CombinedOutput(); err == nil &&
			strings.Contains(string(out), "EntrypointEncSlice") {
			caps = append(caps, "vaapi")
		}
	}
	return caps
}

// resolveModelPath turns a configured model reference into a path the whisper
// runner accepts: an absolute/relative existing PATH passes through; a bare
// NAME (e.g. "large-v3") maps to the baked default location or the on-demand
// download dir the entrypoint uses (/models/ggml-<name>.bin or /state/models/...).
func resolveModelPath(ref string) string {
	ref = normalizeModelReference(ref)
	if ref == "" {
		return ref
	}
	if strings.ContainsRune(ref, '/') || strings.HasSuffix(ref, ".bin") {
		return ref // a path already
	}
	for _, cand := range []string{
		"/models/ggml-" + ref + ".bin",
		"/state/models/ggml-" + ref + ".bin",
	} {
		if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
			return cand
		}
	}
	return ref // let whisper fail loudly with the name; entrypoint may fetch it
}

// normalizeModelReference accepts the portable contract identifier Manager
// stores as well as a raw model name. "whisper.cpp/medium.en" is an identity,
// not a filesystem path; treating its slash as a path prevents model loading.
func normalizeModelReference(ref string) string {
	ref = strings.TrimSpace(ref)
	return strings.TrimPrefix(ref, "whisper.cpp/")
}

// normalizeKinds expands a job's kinds list into the concrete passes to run.
// "all" (or an empty list) means the full pipeline; unknown kinds are ignored.
func normalizeKinds(kinds []string) map[string]bool {
	out := map[string]bool{}
	if len(kinds) == 0 {
		kinds = []string{KindAll}
	}
	for _, k := range kinds {
		switch strings.ToLower(strings.TrimSpace(k)) {
		case KindAll:
			out[KindDerivatives], out[KindQLPreview], out[KindProxy], out[KindTranscript] = true, true, true, true
		case KindDerivatives:
			out[KindDerivatives] = true
		case KindQLPreview:
			out[KindQLPreview] = true
		case KindProxy:
			out[KindProxy] = true
		case KindTranscript:
			out[KindTranscript] = true
		}
	}
	return out
}

// Kind constants mirror farmqueue's so the dispatch reads locally; they are the
// same wire strings. KindQLPreview is defined HERE rather than in farmqueue:
// the queue package's vocabulary is frozen at its last release and the QL pass
// is additive — a job carrying kinds:["qlpreview"] round-trips as an ordinary
// string and older workers ignore it.
const (
	KindDerivatives = farmqueue.KindDerivatives
	KindProxy       = farmqueue.KindProxy
	KindTranscript  = farmqueue.KindTranscript
	KindAll         = farmqueue.KindAll
	KindQLPreview   = "qlpreview"
)

func recordErr(mu *sync.Mutex, errs *[]string, path string, err error) {
	mu.Lock()
	defer mu.Unlock()
	if len(*errs) < 10 {
		*errs = append(*errs, fmt.Sprintf("%s: %v", filepath.Base(path), err))
	}
}

func recordFailure(mu *sync.Mutex, errs, failedTargets *[]string, path string, err error) {
	mu.Lock()
	defer mu.Unlock()
	if len(*errs) < 10 {
		*errs = append(*errs, fmt.Sprintf("%s: %v", filepath.Base(path), err))
	}
	*failedTargets = append(*failedTargets, path)
}

// collectRetryTargets validates worker-authored partial-batch paths before they
// are trusted from the Redis wire again. Every target must remain inside both
// the configured mount and the original job scope; then the normal collector
// re-applies media-extension, exclusion, and minimum-size policy.
func collectRetryTargets(mount, scope string, requested []string, minBytes int64) ([]string, error) {
	cleanMount := filepath.Clean(mount)
	cleanScope := filepath.Clean(scope)
	scopeInfo, err := os.Stat(cleanScope)
	if err != nil {
		return nil, err
	}
	if !pathWithin(cleanMount, cleanScope) {
		return nil, fmt.Errorf("job scope %q escapes mount %q", scope, mount)
	}
	seen := make(map[string]bool, len(requested))
	out := make([]string, 0, len(requested))
	for _, requestedPath := range requested {
		candidate := filepath.Clean(requestedPath)
		if !filepath.IsAbs(candidate) || !pathWithin(cleanMount, candidate) {
			return nil, fmt.Errorf("retry target %q escapes mount %q", requestedPath, mount)
		}
		if scopeInfo.IsDir() {
			if !pathWithin(cleanScope, candidate) {
				return nil, fmt.Errorf("retry target %q escapes job scope %q", requestedPath, scope)
			}
		} else if candidate != cleanScope {
			return nil, fmt.Errorf("retry target %q differs from file job scope %q", requestedPath, scope)
		}
		if seen[candidate] {
			continue
		}
		collected, err := collectTargets(candidate, "", 1, minBytes)
		if err != nil {
			return nil, fmt.Errorf("retry target %q: %w", requestedPath, err)
		}
		if len(collected) == 1 {
			seen[candidate] = true
			out = append(out, collected[0])
		}
	}
	return out, nil
}

func pathWithin(base, candidate string) bool {
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func uniqueSortedStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, s := range in[1:] {
		if s != out[len(out)-1] {
			out = append(out, s)
		}
	}
	return out
}

func collectTargets(root, files string, limit int, minBytes int64) ([]string, error) {
	var out []string
	add := func(p string) bool {
		if limit > 0 && len(out) >= limit {
			return false
		}
		out = append(out, p)
		return true
	}

	// Derivative-exclusion policy (user-directed): proxies (any spelling, folder
	// or filename), media under the size floor (minBytes, from -min-size-mb), and
	// NLE ephemeral dirs are skipped entirely — never queued, so no
	// tech/poster/filmstrip ("shouldn't cache it with any items"). See
	// internal/farm/exclude.go. Counts logged by reason at the end.
	skipSubs := farm.SkipDirSubstrings()
	var nProxy, nSmall, nDir int

	if files != "" {
		for _, f := range strings.Split(files, ",") {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			var sz int64 = -1
			if fi, serr := os.Stat(f); serr == nil {
				sz = fi.Size()
			}
			switch r := farm.ExcludeReason(f, sz, minBytes, skipSubs); {
			case r == "proxy":
				nProxy++
				continue
			case r == "too-small":
				nSmall++
				continue
			case strings.HasPrefix(r, "skip-dir"):
				nDir++
				continue
			}
			if !add(f) {
				break
			}
		}
		logExcludes(nProxy, nSmall, nDir, minBytes)
		return out, nil
	}

	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			// Skip unreadable entries but make it audible — a stale/partial
			// mount can silently drop a whole subtree otherwise.
			fmt.Fprintf(os.Stderr, "jmfarm: skip unreadable %q: %v\n", p, err)
			return nil
		}
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") && p != root {
				return filepath.SkipDir // skip dotdirs (incl. .juicemount)
			}
			// Prune whole excluded dirs at the dir level — don't descend into a
			// Proxies/ or Media Cache/ tree at all. Bucket by reason (size rule
			// disabled with -1/0; a dir has no size) so a pruned proxy tree counts
			// as proxy, not skip-dir.
			if p != root {
				switch r := farm.ExcludeReason(p, -1, 0, skipSubs); {
				case r == "proxy":
					nProxy++
					return filepath.SkipDir
				case strings.HasPrefix(r, "skip-dir"):
					nDir++
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.HasPrefix(info.Name(), "._") {
			return nil // AppleDouble sidecar
		}
		if !mediaExts[strings.ToLower(filepath.Ext(p))] {
			return nil
		}
		// Per-file exclusion (proxy in the filename, too-small, or a skip-dir
		// substring the walk-level prune above didn't catch).
		switch r := farm.ExcludeReason(p, info.Size(), minBytes, skipSubs); {
		case r == "proxy":
			nProxy++
			return nil
		case r == "too-small":
			nSmall++
			return nil
		case strings.HasPrefix(r, "skip-dir"):
			nDir++
			return nil
		}
		if !add(p) {
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	logExcludes(nProxy, nSmall, nDir, minBytes)
	return out, nil
}

// logExcludes prints a one-line rollup of what the exclusion policy skipped, so a
// sweep's coverage is auditable ("why is this not derived?"). Counts mix skipped
// files with pruned directory subtrees (a pruned dir counts once, not per file
// under it). Silent when nothing was excluded.
func logExcludes(nProxy, nSmall, nDir int, minBytes int64) {
	if nProxy == 0 && nSmall == 0 && nDir == 0 {
		return
	}
	fmt.Printf("jmfarm: excluded (files + pruned dirs) — proxy=%d too-small(<%dMB)=%d skip-dir=%d\n",
		nProxy, minBytes>>20, nSmall, nDir)
}

// defaultConcurrency sizes the worker pool so workers × ffmpeg-threads ≈
// physical cores (no oversubscription). Clamped to [2, 32].
func defaultConcurrency(ffThreads int) int {
	if ffThreads < 1 {
		ffThreads = 1
	}
	n := runtime.NumCPU() / ffThreads
	if n < 2 {
		n = 2
	}
	if n > 32 {
		n = 32
	}
	return n
}

// farmEnvInt reads a non-negative int from env, falling back to def.
func farmEnvInt(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func farmEnvUint64(name string, def uint64) uint64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "derivatives.db"
	}
	return filepath.Join(home, "Library", "Application Support", "JuiceMount", "derivatives.db")
}

// knownVideoTrack returns the video track for path from ALREADY-STORED tech, or
// nil when we have never probed it.
//
// Deliberately never probes: this feeds FARM-3's proxy ordering, and probing to
// decide what to probe would spend the budget the ordering is meant to protect.
// Unknown media keeps its walk position (PrioritizeTargets treats nil as
// "normal"), so a first sweep behaves exactly as before and later sweeps get the
// benefit for free.
func knownVideoTrack(store *derivatives.Store, path string) *farm.VideoTrack {
	if store == nil {
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	meta, err := store.Metadata(uint64(st.Ino), "tech")
	if err != nil || meta == nil || len(meta.Payload) == 0 {
		return nil
	}
	var tech farm.Tech
	if json.Unmarshal(meta.Payload, &tech) != nil {
		return nil
	}
	return tech.Video
}
