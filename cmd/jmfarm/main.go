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
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/farm"
)

// mediaExts are the file types we probe. Extension gate is a cheap pre-filter;
// ffprobe is the real arbiter (a non-media file just errors and is skipped).
var mediaExts = map[string]bool{
	".mov": true, ".mp4": true, ".m4v": true, ".mxf": true, ".mkv": true,
	".avi": true, ".mts": true, ".m2ts": true, ".braw": true, ".r3d": true,
	".wav": true, ".aif": true, ".aiff": true, ".flac": true, ".mp3": true, ".m4a": true,
}

func main() {
	var (
		dbPath    = flag.String("db", defaultDBPath(), "derivatives.db path (the one the app serves)")
		root      = flag.String("root", "", "directory to walk for media (required unless -files)")
		files     = flag.String("files", "", "comma-separated explicit file list (alternative to -root)")
		mount     = flag.String("mount", "/Volumes/zpool", "mount point (for Tier-A blob dir)")
		blobs     = flag.Bool("blobs", false, "also generate poster thumbnails into Tier-A")
		thumbDim  = flag.Int("thumb-dim", 640, "poster fit box in px")
		filmstr   = flag.Bool("filmstrip", false, "also generate filmstrip sprite-sheets into Tier-A (JM-16)")
		filmCell  = flag.Int("filmstrip-cell", 160, "filmstrip cell width in px")
		wave      = flag.Bool("waveform", false, "also generate audio waveform overviews into Tier-A (JM-18)")
		waveSPP   = flag.Int("waveform-spp", 1024, "waveform samples per pixel")
		transcr   = flag.Bool("transcript", false, "AI mode: generate whisper transcripts → ai.loupe.json (instead of basic derivatives)")
		proxyGen  = flag.Bool("proxy", false, "proxy mode: generate faststart MP4 proxies (OL-3), separate from basic derivatives")
		vcodec    = flag.String("vcodec", "libx264", "proxy video encoder (GPU: h264_nvenc/h264_qsv/h264_vaapi)")
		pCRF      = flag.Int("crf", 21, "proxy CRF quality (lower = sharper/bigger)")
		pPreset   = flag.String("preset", "slow", "proxy x264 preset (faster preset = quicker, larger)")
		pConc     = flag.Int("proxy-concurrency", 0, "separate (lower) worker count for proxy mode; 0 = use -concurrency (proxy transcode is the CPU hog)")
		wModel    = flag.String("whisper-model", "", "path to a ggml whisper model (required with -transcript)")
		wBin      = flag.String("whisper-bin", "whisper-cli", "whisper.cpp CLI binary")
		limit     = flag.Int("limit", 0, "max files to process (0 = no limit)")
		conc      = flag.Int("concurrency", 4, "parallel workers")
		producer  = flag.String("producer", "macos-node", "producer tag")
		version   = flag.Int("version", 1, "producer version")
		dryRun    = flag.Bool("dry-run", false, "probe + report, do not write")
		verbose   = flag.Bool("verbose", false, "per-file logging")
		status    = flag.String("status", "", "after the sweep, write a rollup status JSON here (manager Farm tab)")
		reconcile = flag.Bool("reconcile", false, "JM-15: ingest volume manifest.json sidecars into -db (no media processing); closes the farm→client discovery loop")
		// Informational governor knobs: the entrypoint actually APPLIES these via
		// nice/ionice + its sleep loop; jmfarm only RECORDS them in the status
		// stamp so the manager's read-only knob inspector shows real values.
		gNice     = flag.Int("nice", 0, "informational: CPU niceness the entrypoint applied (recorded in status, not applied here)")
		gIONice   = flag.Int("ionice", 0, "informational: best-effort IO class the entrypoint applied (recorded in status, not applied here)")
		gInterval = flag.Int("interval", 0, "informational: seconds between sweeps the entrypoint loops on (recorded in status, not applied here)")
	)
	flag.Parse()

	// JM-15 reconcile mode: walk the volume sidecars → local db, then exit. The
	// running app serves the same db (WAL), so reconciled rows appear live.
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

	if *root == "" && *files == "" {
		fmt.Fprintln(os.Stderr, "jmfarm: need -root <dir> or -files <a,b,c>")
		flag.Usage()
		os.Exit(2)
	}

	targets, err := collectTargets(*root, *files, *limit)
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
	} else if *proxyGen {
		mode = "proxy"
	}
	fmt.Printf("jmfarm: %d files → %s  (mode=%s, producer=%s v%d, dry-run=%v, workers=%d)\n",
		len(targets), *dbPath, mode, *producer, *version, *dryRun, effConc)

	start := time.Now()
	var ok, failed, thumbs, strips, waves, speech, proxies int64
	var mu sync.Mutex
	var firstErrs []string

	// Resolve the target + governor stamp up front so the live progress ticker
	// (below) can write the same sweep/governor shape the final write uses.
	target := *root
	if target == "" {
		target = "files:" + *files
	}
	gov := farm.NewGovernor(*wModel, *vcodec, *pPreset, mode,
		*pCRF, *conc, *pConc, *gNice, *gIONice, *gInterval)

	// Live progress: while the sweep runs, write farm-status.json every ~3s with an
	// in_progress block {pass, total, done, failed, started_at} so the manager Farm
	// tab can show a progress bar + ETA. Cheap by design (Stats GROUP BY only; the
	// proxy_economics stat-walk is skipped until the final write). Only when a status
	// path is set and we're really writing (store!=nil, !dry-run). The ticker is
	// stopped before the final WriteFarmStatus so the last write (which clears
	// in_progress) always wins.
	stopProgress := make(chan struct{})
	var progressWG sync.WaitGroup
	if *status != "" && !*dryRun && store != nil {
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
					done := atomic.LoadInt64(&ok) + atomic.LoadInt64(&failed)
					ip := farm.InProgress{
						Pass:      mode,
						Total:     len(targets),
						Done:      int(done),
						Failed:    int(atomic.LoadInt64(&failed)),
						StartedAt: start.Unix(),
					}
					sweep := farm.SweepInfo{
						Mode: mode, Producer: *producer, Target: target,
						Processed: int(done), Failed: int(atomic.LoadInt64(&failed)),
						StartedAt: start.Unix(), DurationMS: time.Since(start).Milliseconds(),
					}
					if err := farm.WriteFarmProgress(store, *status, *mount, sweep, gov, ip); err != nil {
						fmt.Fprintf(os.Stderr, "jmfarm: progress write: %v\n", err)
					}
				}
			}
		}()
	}

	sem := make(chan struct{}, effConc)
	var wg sync.WaitGroup
	for _, p := range targets {
		p := p
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			if *dryRun {
				tech, e := farm.Probe(opt.FFprobeBin, p, 0)
				if e != nil {
					atomic.AddInt64(&failed, 1)
					recordErr(&mu, &firstErrs, p, e)
					return
				}
				atomic.AddInt64(&ok, 1)
				if *verbose {
					fmt.Printf("  [dry] %-50s %s %dms\n", filepath.Base(p), tech.Container, tech.DurationMS)
				}
				return
			}

			if *proxyGen {
				pr := farm.GenerateProxy(store, p, opt)
				if pr.Err != nil {
					atomic.AddInt64(&failed, 1)
					recordErr(&mu, &firstErrs, p, pr.Err)
					if *verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), pr.Inode, pr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				if pr.Wrote {
					atomic.AddInt64(&proxies, 1)
				}
				if *verbose {
					fmt.Printf("  [ok] %-50s inode=%d proxy=%v\n", filepath.Base(p), pr.Inode, pr.Wrote)
				}
				return
			}

			if *transcr {
				tr := farm.GenerateTranscript(store, p, opt)
				if tr.Err != nil {
					atomic.AddInt64(&failed, 1)
					recordErr(&mu, &firstErrs, p, tr.Err)
					if *verbose {
						fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), tr.Inode, tr.Err)
					}
					return
				}
				atomic.AddInt64(&ok, 1)
				if tr.HasSpeech {
					atomic.AddInt64(&speech, 1)
				}
				if *verbose {
					fmt.Printf("  [ok] %-50s inode=%d speech=%v segments=%d\n",
						filepath.Base(p), tr.Inode, tr.HasSpeech, tr.Segments)
				}
				return
			}

			r := farm.Process(store, p, opt)
			if r.Err != nil {
				atomic.AddInt64(&failed, 1)
				recordErr(&mu, &firstErrs, p, r.Err)
				if *verbose {
					fmt.Printf("  [FAIL] %-50s inode=%d %v\n", filepath.Base(p), r.Inode, r.Err)
				}
				return
			}
			atomic.AddInt64(&ok, 1)
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
			if *verbose {
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

	if *transcr {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d with-speech (transcribed) — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, failed, speech, len(targets))
	} else if *proxyGen {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d proxies — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, failed, proxies, len(targets))
	} else {
		fmt.Printf("\njmfarm done in %s: %d ok, %d failed, %d thumbnails, %d filmstrips, %d waveforms — %d total\n",
			time.Since(start).Round(time.Millisecond), ok, failed, thumbs, strips, waves, len(targets))
	}
	if len(firstErrs) > 0 {
		fmt.Printf("first errors (%d shown):\n", len(firstErrs))
		for _, e := range firstErrs {
			fmt.Printf("  - %s\n", e)
		}
	}

	if *status != "" && !*dryRun && store != nil {
		sweep := farm.SweepInfo{
			Mode: mode, Producer: *producer, Target: target,
			Processed: int(ok), Failed: int(failed),
			StartedAt: start.Unix(), DurationMS: time.Since(start).Milliseconds(),
		}
		// Final (idle) write: clears in_progress + measures proxy_economics (stat-
		// walks the proxy blobs under *mount). gov was stamped before the sweep with
		// the RESOLVED run settings so the manager's read-only knob inspector shows
		// real values (nice/ionice/interval are applied by the entrypoint).
		if err := farm.WriteFarmStatus(store, *status, *mount, sweep, gov); err != nil {
			fmt.Fprintf(os.Stderr, "jmfarm: status write: %v\n", err)
		}
	}
	if failed > 0 && ok == 0 {
		os.Exit(1)
	}
}

func recordErr(mu *sync.Mutex, errs *[]string, path string, err error) {
	mu.Lock()
	defer mu.Unlock()
	if len(*errs) < 10 {
		*errs = append(*errs, fmt.Sprintf("%s: %v", filepath.Base(path), err))
	}
}

func collectTargets(root, files string, limit int) ([]string, error) {
	var out []string
	add := func(p string) bool {
		if limit > 0 && len(out) >= limit {
			return false
		}
		out = append(out, p)
		return true
	}

	if files != "" {
		for _, f := range strings.Split(files, ",") {
			f = strings.TrimSpace(f)
			if f != "" && !add(f) {
				break
			}
		}
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
			return nil
		}
		if strings.HasPrefix(info.Name(), "._") {
			return nil // AppleDouble sidecar
		}
		if !mediaExts[strings.ToLower(filepath.Ext(p))] {
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
	return out, nil
}

func defaultDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "derivatives.db"
	}
	return filepath.Join(home, "Library", "Application Support", "JuiceMount", "derivatives.db")
}
