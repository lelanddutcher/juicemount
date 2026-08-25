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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
	"github.com/lelanddutcher/juicemount/internal/farmqueue"
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
	opt      farm.Options // resolved generator options (already merged with job overrides in queue mode)
	mode     string       // "derivatives" | "proxy" | "ql-preview" | "transcript(AI)" — display + status stamp
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
}

// runPasses dispatches one sweep over targets with the worker pool + live
// progress ticker + final farm-status rollup, exactly as the one-shot path does.
// It returns the (processed, failed) counts so the queue loop can MarkDone with
// them. The generator dispatch below is the single source of truth: the basic,
// proxy and transcript modes all funnel through the same internal/farm calls.
func runPasses(po passOpts, targets []string) (processed, failed int) {
	start := time.Now()
	var ok, fail, thumbs, strips, waves, speech, proxies, qls int64
	// skippedFresh counts assets whose derivatives already matched byte-identical
	// source — a moved or re-enqueued file that cost no re-encode.
	var skippedFresh, sidecarsRepaired int64
	var mu sync.Mutex
	var firstErrs []string

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
					}
				}
			}
		}()
	}

	sem := make(chan struct{}, po.effConc)
	var wg sync.WaitGroup
	for _, p := range targets {
		p := p
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if po.dryRun {
				tech, e := farm.Probe(po.opt.FFprobeBin, p, 0)
				if e != nil {
					atomic.AddInt64(&fail, 1)
					recordErr(&mu, &firstErrs, p, e)
					return
				}
				atomic.AddInt64(&ok, 1)
				if po.verbose {
					fmt.Printf("  [dry] %-50s %s %dms\n", filepath.Base(p), tech.Container, tech.DurationMS)
				}
				return
			}

			if po.qlGen {
				qr := farm.GenerateQLPreview(po.store, p, po.opt)
				if qr.Err != nil {
					atomic.AddInt64(&fail, 1)
					recordErr(&mu, &firstErrs, p, qr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), qr.Inode, qr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
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
				if pr.Err != nil {
					atomic.AddInt64(&fail, 1)
					recordErr(&mu, &firstErrs, p, pr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), pr.Inode, pr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				if pr.Wrote {
					atomic.AddInt64(&proxies, 1)
				}
				if po.verbose {
					fmt.Printf("  [ok] %-50s inode=%d proxy=%v\n", filepath.Base(p), pr.Inode, pr.Wrote)
				}
				return
			}

			if po.transcr {
				tr := farm.GenerateTranscript(po.store, p, po.opt)
				if tr.Err != nil {
					atomic.AddInt64(&fail, 1)
					recordErr(&mu, &firstErrs, p, tr.Err)
					if po.verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), tr.Inode, tr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				if tr.HasSpeech {
					atomic.AddInt64(&speech, 1)
				}
				if po.verbose {
					fmt.Printf("  [ok] %-50s inode=%d speech=%v segments=%d\n",
						filepath.Base(p), tr.Inode, tr.HasSpeech, tr.Segments)
				}
				return
			}

			r := farm.Process(po.store, p, po.opt)
			if r.Err != nil {
				atomic.AddInt64(&fail, 1)
				recordErr(&mu, &firstErrs, p, r.Err)
				if po.verbose {
					fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), r.Inode, r.Err)
				}
				return
			}
			atomic.AddInt64(&ok, 1)
			// A move must be VISIBLE as a move. Without its own counter a skipped
			// asset is indistinguishable from one that generated nothing, and the
			// whole point of the freshness gate is being able to tell that a
			// reorganised folder cost no compute.
			if r.SkippedFresh {
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
			if r.BlobErr != nil { // non-fatal: tech published, a blob couldn't render
				recordErr(&mu, &firstErrs, p, r.BlobErr)
			}
			if po.verbose {
				fmt.Printf("  [ok] %-50s inode=%d hash=%s %dms vid=%v thumb=%v strip=%v wave=%v\n",
					filepath.Base(p), r.Inode, r.Hash, r.DurationMS, r.HasVideo, r.ThumbWrote, r.FilmWrote, r.WaveWrote)
			}
		}()
	}
	wg.Wait()

	// Stop the live ticker before the final write so the final status (which clears
	// in_progress + measures proxy_economics) is the last thing on disk.
	close(stopProgress)
	progressWG.Wait()

	if po.transcr {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d with-speech (transcribed) — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, speech, len(targets))
	} else if po.qlGen {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d ql-previews — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, qls, len(targets))
	} else if po.proxyGen {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d proxies — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, fail, proxies, len(targets))
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
		}
		// Final (idle) write: clears in_progress + measures proxy_economics (stat-
		// walks the proxy blobs under mount). gov was stamped before the sweep with
		// the RESOLVED run settings so the manager's read-only knob inspector shows
		// real values (nice/ionice/interval are applied by the entrypoint).
		if err := farm.WriteFarmStatus(po.store, po.status, po.mount, sweep, po.gov); err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: status write: %v\n", err)
		}
	}
	return int(ok), int(fail)
}

func main() {
	var (
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
		wDevice = flag.String("transcript-device", defaultStr(os.Getenv("JM_FARM_TRANSCRIPT_DEVICE"), "cpu"), "whisper.cpp compute device: cpu|vulkan|cuda|sycl")
	)
	flag.Parse()

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
			meta:       *meta,
			dbPath:     *dbPath,
			mount:      *mount,
			producer:   *producer,
			version:    *version,
			status:     *status,
			verbose:    *verbose,
			conc:       *conc,
			pConc:      *pConc,
			vcodec:     *vcodec,
			crf:        *pCRF,
			preset:     *pPreset,
			wModel:     *wModel,
			wBin:       *wBin,
			thumbDim:   *thumbDim,
			filmCell:   *filmCell,
			waveSPP:    *waveSPP,
			minSize:    minSizeBytes,
			gNice:      *gNice,
			gIONice:    *gIONice,
			qlMaxDim:   *qlDim,
			qlSecs:     *qlSecs,
			postAlways: *postAlways,
			name:       *wName,
			kinds:      splitKinds(*wKinds),
			tDevice:    *wDevice,
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

	_, failed := runPasses(passOpts{
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
	meta       string
	dbPath     string
	mount      string
	producer   string
	version    int
	status     string
	verbose    bool
	conc       int
	pConc      int
	vcodec     string
	crf        int
	preset     string
	wModel     string
	wBin       string
	thumbDim   int
	filmCell   int
	waveSPP    int
	minSize    int64 // skip media smaller than this (bytes); 0 = no minimum
	gNice      int
	gIONice    int
	qlMaxDim   int // QL preview fit box px (T1.1/F4)
	qlSecs     int // QL preview duration cap s (<=0 → default 20)
	postAlways bool

	// Manager-config integration (FARM-NODE-CONFIG spec):
	name    string   // stable worker identity (JM_WORKER_NAME / -name)
	kinds   []string // declared kinds → per-kind queue drain + watch filter
	tDevice string   // whisper compute device (cpu|vulkan|cuda|sycl)
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	worker := farmqueue.Worker{
		ID:           farmqueue.NewID(),
		StartedAt:    time.Now().UTC().Format(time.RFC3339),
		Name:         cfg.name,
		Kinds:        farmqueue.DrainKinds(cfg.kinds),
		Capabilities: detectCapabilities(cfg.tDevice),
	}
	fmt.Printf("jmfarm queue: worker %s (name=%q kinds=%v caps=%v) draining %s (db=%s mount=%s producer=%s)\n",
		worker.ID, worker.Name, worker.Kinds, worker.Capabilities, cfg.meta, cfg.dbPath, cfg.mount, cfg.producer)

	// Manager-config state: last revision applied + restart-class drift. The
	// config doc is polled every loop iteration (idle or post-job); hot knobs
	// swap this worker's run-defaults immediately, restart-class keys land in
	// pendingRestart and surface on the heartbeat for the Farm tab badge.
	cfgMu := sync.Mutex{}
	appliedRev := int64(0)
	var pendingRestart []string

	applyConfig := func(fc *farmqueue.FarmConfig) {
		eff, restart := fc.ResolveFor(cfg.name)
		cfgMu.Lock()
		defer cfgMu.Unlock()
		appliedRev = fc.Revision
		pendingRestart = append([]string(nil), restart...)
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

	// Wave-3 auto-discovery: subscribe to the volume's keyspace events and
	// self-enqueue derivative jobs for directories where new media settles —
	// the farm no longer waits for a manager sweep to notice ingests. Runs in
	// this process beside the drain loop; jobs it enqueues flow through the
	// exact same BRPOP path below. JM_FARM_WATCH=0 disables.
	if farm.WatchEnabled() {
		kinds := farm.WatchKindsFromEnv()
		watcher := farm.NewWatcher(farm.WatchConfig{
			MetaURL: cfg.meta,
			Mount:   cfg.mount,
			Enqueue: func(ectx context.Context, relDir string) error {
				// Job.Path is worker-absolute by convention (runJob walks it
				// verbatim; the manager passes its callers' /jfs paths through
				// unmodified) — live-proven: a mount-relative path fails the
				// runner's lstat. The watcher core stays mount-relative for
				// filtering; join here at the seam.
				return q.Enqueue(ectx, farmqueue.NewJob(filepath.Join(cfg.mount, relDir), kinds, "farm-watch"))
			},
			Logf: func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) },
		})
		go func() {
			if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "farm watch: exited: %v (manager sweeps remain the discovery path)\n", err)
			}
		}()
	} else {
		fmt.Fprintln(os.Stderr, "farm watch: disabled (JM_FARM_WATCH=0)")
	}

	for {
		if ctx.Err() != nil {
			break
		}

		// Manager-config poll: cheap GET; applies only on revision change.
		pollConfig()

		// Heartbeat (idle): publish presence so a producer sees the farm is draining.
		worker.CurrentJob = ""
		setHeartbeatExtras(&worker)
		if err := q.Heartbeat(ctx, worker); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: heartbeat: %v\n", err)
		}

		// Block up to 5s for a job across the worker's declared kind queues,
		// falling back to the catch-all (DequeueKinds always appends it last).
		job, ok, err := q.DequeueKinds(ctx, 5*time.Second, cfg.kinds)
		if err != nil {
			if ctx.Err() != nil {
				break // cancelled mid-BRPOP → clean exit
			}
			// Redis disconnect / transient error: log + back off briefly, keep looping.
			fmt.Fprintf(os.Stderr, "jmfarm queue: dequeue: %v\n", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if !ok {
			continue // timeout, nothing waiting → loop re-heartbeats
		}

		// Claim the job: stamp current_job on the heartbeat, flip it to running.
		worker.CurrentJob = job.ID
		setHeartbeatExtras(&worker)
		if err := q.Heartbeat(ctx, worker); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: heartbeat(claim): %v\n", err)
		}
		if err := q.MarkRunning(ctx, job.ID); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: mark-running %s: %v\n", job.ID, err)
		}

		// Keep the heartbeat fresh DURING the job. A big sweep (1000s of files,
		// proxy/transcript) runs for hours inside runJob; without this the 30s
		// worker TTL expires mid-job and the worker falsely reads as "offline"
		// (ActiveWorkers empty) even though it's actively processing. A ~10s
		// ticker (well inside WorkerTTL) holds it alive until the job returns.
		hbCtx, hbStop := context.WithCancel(ctx)
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-hbCtx.Done():
					return
				case <-t.C:
					_ = q.Heartbeat(context.Background(), worker)
				}
			}
		}()

		processed, failed, runErr := runJob(ctx, store, cfg, worker, job)
		hbStop()
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: job %s failed: %v\n", job.ID, runErr)
			if err := q.MarkFailed(context.Background(), job.ID, runErr.Error()); err != nil {
				fmt.Fprintf(os.Stderr, "jmfarm queue: mark-failed %s: %v\n", job.ID, err)
			}
		} else if err := q.MarkDone(context.Background(), job.ID, processed, failed); err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm queue: mark-done %s: %v\n", job.ID, err)
		}
	}

	fmt.Printf("jmfarm queue: worker %s stopping (signal)\n", worker.ID)
}

// runJob runs the passes a single dequeued job asks for, scoped to job.Path, with
// the job's non-zero options overriding the container run-defaults. It returns
// the summed processed/failed across the selected passes; a non-nil error is a
// HARD failure (couldn't collect targets / nothing usable) → MarkFailed.
func runJob(ctx context.Context, store *derivatives.Store, cfg queueConfig, worker farmqueue.Worker, job farmqueue.Job) (processed, failed int, err error) {
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

	// Collect the media under the job's path once; every selected pass sweeps the
	// same target set. cfg.minSize applies the same size floor here (skip entirely)
	// that MinBlobSizeBytes applies in Process — single source, the -min-size-mb flag.
	targets, cErr := collectTargets(job.Path, "", 0, cfg.minSize)
	if cErr != nil {
		return 0, 0, fmt.Errorf("collect %q: %w", job.Path, cErr)
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
		return 0, 0, nil
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
			return 0, 0, fmt.Errorf("transcript kind needs a whisper model (job.model or -whisper-model)")
		}
		passes = append(passes, pass{mode: "transcript(AI)", transcr: true, effConc: conc})
	}
	if len(passes) == 0 {
		return 0, 0, fmt.Errorf("job %s has no runnable kinds (%v)", job.ID, job.Kinds)
	}

	base := farm.Options{
		Producer: cfg.producer, Version: cfg.version, Mount: cfg.mount,
		Blobs: true, ThumbMaxDim: cfg.thumbDim, Filmstrip: true, FilmstripCell: cfg.filmCell,
		Waveform: true, WaveformSPP: cfg.waveSPP,
		WhisperBin: cfg.wBin, WhisperModel: wModel,
		TranscriptDevice: cfg.tDevice,
		ProxyVCodec:      vcodec, ProxyCRF: crf, ProxyPreset: preset,
		MinBlobSizeBytes: cfg.minSize,
		PosterAlways:     cfg.postAlways,
		QLMaxDim:         cfg.qlMaxDim, QLSeconds: cfg.qlSecs,
	}

	fmt.Printf("jmfarm queue: job %s path=%q kinds=%v files=%d\n",
		job.ID, job.Path, job.Kinds, len(targets))

	for _, p := range passes {
		if ctx.Err() != nil {
			break // cancelled between passes → stop cleanly, report what we did
		}
		gov := farm.NewGovernor(wModel, vcodec, preset, p.mode, crf, conc, pConc, cfg.gNice, cfg.gIONice, 0)
		pr, pf := runPasses(passOpts{
			opt: base, mode: p.mode, transcr: p.transcr, proxyGen: p.proxyGen, qlGen: p.qlGen,
			dryRun: false, verbose: cfg.verbose, effConc: p.effConc,
			status: cfg.status, mount: cfg.mount, producer: cfg.producer,
			target: job.Path, gov: gov, store: store,
		}, targets)
		processed += pr
		failed += pf
	}
	return processed, failed, nil
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
