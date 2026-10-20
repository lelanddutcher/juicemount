package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"strconv"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	jmlibnfs "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
	"github.com/lelanddutcher/juicemount/internal/version"
	"github.com/lelanddutcher/juicemount/metadata"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// defaultStr returns s if non-empty, otherwise fallback. Used for
// env-var-fallback flag defaults so JM_* env vars override the
// hardcoded defaults without breaking explicit flag values (Go's
// flag.String uses its default as the resolved value when the flag
// isn't passed).
func defaultStr(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}

// splitNonEmpty is a small helper for parsing comma-separated CLI args.
func splitNonEmpty(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func openCheckedStore(path string) (*metadata.Store, error) {
	store, err := metadata.Open(path)
	if err != nil {
		return nil, err
	}
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("initialize spool schema: %w", err)
	}
	if err := store.CheckSpoolRecovery(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("inspect spool publication before serving: %w", err)
	}
	return store, nil
}

type cliStartupCleanup struct{ steps []func() error }

func (c *cliStartupCleanup) add(step func()) {
	c.addError(func() error { step(); return nil })
}

func (c *cliStartupCleanup) addError(step func() error) { c.steps = append(c.steps, step) }

func (c *cliStartupCleanup) fail(err error) error {
	for i := len(c.steps) - 1; i >= 0; i-- {
		if closeErr := c.steps[i](); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	c.steps = nil
	return err
}

var errCLICleanupIncomplete = errors.New("CLI cleanup incomplete; serving dependencies retained")

func failCLIStartup(err error, retire func() error, quiesce, stopHandler func() bool,
	rollback func() error, cleanup *cliStartupCleanup) error {
	retireErr := retire()
	if retireErr != nil {
		return errors.Join(err, errCLICleanupIncomplete, fmt.Errorf("NFS listener retirement incomplete: %w", retireErr))
	}
	if !quiesce() {
		return errors.Join(err, errCLICleanupIncomplete, errors.New("NFS clients did not quiesce"))
	}
	if !stopHandler() {
		return errors.Join(err, errCLICleanupIncomplete, errors.New("NFS handler cleanup incomplete"))
	}
	if rollbackErr := rollback(); rollbackErr != nil {
		return errors.Join(err, errCLICleanupIncomplete, rollbackErr)
	}
	return cleanup.fail(err)
}

func runCLIStartup(prepareSpool, prepareFUSE func() error, syncMirror func(), startListener func() error,
	startWorkers func(), onFailure func(error) error) error {
	if err := prepareSpool(); err != nil {
		return onFailure(err)
	}
	if err := prepareFUSE(); err != nil {
		return onFailure(err)
	}
	syncMirror()
	if err := startListener(); err != nil {
		return onFailure(err)
	}
	startWorkers()
	return nil
}

// An explicit recovery request must not silently become an inactive queue.
func validateCLIReplacementRecovery(enabled, spoolConfigured bool) error {
	if enabled && !spoolConfigured {
		return errors.New("replacement recovery requires JM_SPOOL_ENABLE=1")
	}
	return nil
}

// Construct without dialing. Startup owns Start; cleanup owns Stop only after
// the handler has joined its drainer, including an unstarted failure path.
func newCLIReplacementLeases(enabled bool, redisURL string, spool *jmnfs.SpoolStore, drainer *jmnfs.Drainer) (*jmnfs.ProjectLeaseClient, error) {
	if !enabled {
		return nil, nil
	}
	if spool == nil || drainer == nil {
		return nil, errors.New("replacement recovery requires a spool and drainer")
	}
	leases, err := jmnfs.NewProjectLeaseClient(jmnfs.ProjectLeaseConfig{RedisURL: redisURL, Spool: spool})
	if err != nil {
		return nil, err
	}
	drainer.SetProjectLeases(leases)
	return leases, nil
}

func main() {
	if err := runJM5(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func runJM5() error {
	buildInfo := flag.Bool("build-info", false, "print release version and source commit, then exit")
	replacementRecovery := flag.Bool("replacement-recovery", false, "Recover accepted atomic saves (requires spool and enforcing lease proxy)")
	redisURL := flag.String("redis", "redis://127.0.0.1:6379/1", "Redis URL")
	fusePath := flag.String("fuse-path", "", "Path to JuiceFS FUSE mount (auto-detected if empty)")
	mountPoint := flag.String("mount", "/Volumes/zpool", "Mount point for NFS volume")
	listenAddr := flag.String("listen", "127.0.0.1:11049", "NFS server listen address")
	dbPath := flag.String("db", "", "SQLite database path (default: ~/.juicemount/metadata.db)")
	noMount := flag.Bool("no-mount", false, "Start NFS server without mounting (for testing)")
	noFuse := flag.Bool("no-fuse", false, "Skip JuiceFS FUSE mount (assume already mounted)")
	cacheSize := flag.String("cache-size", "100000", "JuiceFS SSD cache size in MB")
	bucketOverride := flag.String("bucket-override", "", "Override the S3 bucket URL stored in Redis (e.g. http://<server-ip>:30151/<bucket>). Empty = use the URL written by juicefs format.")
	logFile := flag.String("log-file", "", "Optional path to additionally write JSON log records")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	metricsAddr := flag.String("metrics-addr", "127.0.0.1:11050", "HTTP listen address for /metrics and /health")
	// SLICE 2: MinIO URL the Overview dashboard pings via
	// /minio/health/live. Defaults to localhost:9000 for backwards
	// compat with single-host deployments. JM_MINIO_URL env var
	// fallback so docker-compose deployments don't have to wire a flag.
	managerMinIOURL := flag.String("manager-minio-url", defaultStr(os.Getenv("JM_MINIO_URL"), "http://127.0.0.1:9000"), "Base URL the Overview dashboard probes for MinIO liveness. Use the same value Mac clients connect to so the dashboard reflects what they see.")
	// SLICE 0 rename: --migrator-* flags kept as backward-compat
	// aliases (one release). The canonical flags are --manager-*.
	// If both are set, --manager-* wins.
	managerSourceRoots := flag.String("manager-source-roots", "", "Comma-separated host paths the manager's web UI may browse from. Empty = manager disabled.")
	managerAdminKey := flag.String("manager-admin-key", "", "Admin key for the manager's X-JuiceMount-Admin-Key header. Empty disables auth. JM_ADMIN_KEY env var used as fallback if neither this nor --migrator-admin-key is set explicitly.")
	managerContinuityCatalog := flag.String("manager-continuity-catalog", "", "Existing private continuity catalog (requires enabled Manager and library)")
	managerContinuityLibrary := flag.String("manager-continuity-library", "", "Expected continuity library identity")
	migratorSourceRoots := flag.String("migrator-source-roots", "", "DEPRECATED: alias for --manager-source-roots, kept for one-release compat.")
	migratorAdminKey := flag.String("migrator-admin-key", "", "DEPRECATED: alias for --manager-admin-key, kept for one-release compat.")
	flag.Parse()
	if *buildInfo {
		fmt.Printf("jm5 %s (%s)\n", version.Version, version.Commit)
		return nil
	}

	if err := validateCLIReplacementRecovery(*replacementRecovery, os.Getenv("JM_SPOOL_ENABLE") == "1"); err != nil {
		return err
	}

	// Own stop signals before starting any child or admitting mount effects.
	// During incomplete cleanup a signal wakes the next observation; it never
	// authorizes abandoning a mount or pending work by exiting the process.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// Initialize structured logging before anything else logs.
	if err := jmlog.Init(jmlog.Config{
		LogFile: *logFile,
		Level:   jmlog.ParseLevel(*logLevel),
	}); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer jmlog.Close()

	effectiveSourceRoots, effectiveAdminKey := resolveContinuityManagerSettings(
		*managerSourceRoots, *migratorSourceRoots, *managerAdminKey, *migratorAdminKey, os.Getenv("JM_ADMIN_KEY"))
	discovery, err := prepareEmbeddedContinuity(context.Background(), effectiveSourceRoots, manager.ContinuityConfig{
		CatalogPath: *managerContinuityCatalog, LibraryID: *managerContinuityLibrary, ManagerAdminKey: effectiveAdminKey,
	})
	if err != nil {
		return err
	}
	var startupCleanup cliStartupCleanup
	startupCleanup.addError(func() error { return closeEmbeddedContinuity(discovery.Close) })
	// Register the scoped subtree on its future production mux before any
	// storage effect. Legacy Register attaches its routes only after startup.
	mgrMux := http.NewServeMux()
	if err := discovery.RegisterRoutes(mgrMux, "/manager"); err != nil {
		return startupCleanup.fail(err)
	}

	// Defaults
	if *fusePath == "" {
		*fusePath = filepath.Join(os.Getenv("HOME"), ".juicemount", "fuse-internal")
	}
	if *dbPath == "" {
		dir := filepath.Join(os.Getenv("HOME"), ".juicemount")
		os.MkdirAll(dir, 0755)
		*dbPath = filepath.Join(dir, "metadata.db")
	}
	// Refuse unresolved spool publication before stale-session cleanup, FUSE,
	// reconcile, recovery, or either listener can affect the existing volume.
	store, err := openCheckedStore(*dbPath)
	if err != nil {
		return startupCleanup.fail(fmt.Errorf("open metadata store: %w", err))
	}
	startupCleanup.add(func() { _ = store.Close() })

	// Previous processes and mounts require broker reconciliation. A name or
	// path is not authority to kill or force-unmount a stale session.

	jmlog.Info("starting JuiceMount5",
		"version", version.Version,
		"commit", version.Commit,
		"redis", "configured",
		"fuse", *fusePath,
		"mount", *mountPoint,
		"db", *dbPath,
		"metrics_addr", *metricsAddr,
	)

	// FUSE startup follows spool recovery below, so a refused local queue
	// cannot leave a new mount or remount monitor behind.
	var fuseMgr *health.FUSEManager
	var fuseReceipt *health.MountReceipt
	// 2. Connect to Redis. Sync follows spool recovery below.
	rc, err := metadata.NewRedisClient(*redisURL, store)
	if err != nil {
		return startupCleanup.fail(fmt.Errorf("Redis connect: %w", err))
	}
	startupCleanup.add(rc.Stop)

	// Bind the selected backend and load only its durable path eligibility
	// before FUSE or metadata publication. An offline saved Reader exists at
	// NewSession return and must be attached explicitly below.
	cachePaths, err := metadata.OpenCachePathEligibility(store)
	if err != nil {
		return startupCleanup.fail(fmt.Errorf("open cache path eligibility: %w", err))
	}
	// --no-fuse serves an externally owned generation. Its executable is
	// unknown; finding another installed binary cannot prove cache layout.
	juiceFSBin := ""
	if !*noFuse {
		juiceFSBin = health.ResolveJuiceFSBinary()
	}
	stateDir, err := filepath.Abs(filepath.Dir(*dbPath))
	if err != nil {
		return startupCleanup.fail(fmt.Errorf("cache state directory: %w", err))
	}
	var cacheHandler atomic.Pointer[jmnfs.JuiceMountHandler]
	cacheNetwork := newCLICacheNetworkOwner(nil, rc)
	cacheSession, err := cache.NewSession(cache.SessionConfig{
		Store: store, LogicalURL: *redisURL,
		Selection: []string{*bucketOverride, "", ""},
		Options:   rc.RawDB().Options(), StateDir: stateDir, JuiceFSBin: juiceFSBin,
		VerifyReader: func(r *cache.Reader) error {
			if juiceFSBin == "" {
				return cache.ErrReaderUnavailable
			}
			return r.VerifyWithBinary(context.Background(), juiceFSBin)
		},
		OnReader: func(r *cache.Reader) {
			if h := cacheHandler.Load(); h != nil {
				h.SetCacheReader(r)
			}
		},
		OnQualified: cacheNetwork.qualificationChanged,
		OnStatus: func(qualified bool, err error) {
			if h := cacheHandler.Load(); h != nil {
				h.SetCacheBackendStatus(qualified, err)
			}
		},
	})
	if err != nil {
		return startupCleanup.fail(fmt.Errorf("bind cache session: %w", err))
	}
	startupCleanup.add(cacheSession.Stop)
	cacheNetwork.session = cacheSession
	startupCleanup.add(cacheNetwork.stop)

	// Reserve the physical generation before constructing any FUSE consumer.
	// The logical --fuse-path is used only for allocation; all later paths
	// refer to this generation's immutable mount directory.
	if !*noFuse {
		fuseMgr, err = health.NewGenerationFUSEManager(health.FUSEConfig{
			RedisURL: *redisURL, MountPoint: *fusePath, CacheSize: *cacheSize, JuiceFSBin: juiceFSBin,
			BucketOverride: *bucketOverride,
		})
		if err != nil {
			return startupCleanup.fail(fmt.Errorf("reserve FUSE generation: %w", err))
		}
		*fusePath = fuseMgr.MountPoint()
	}

	// 4. Start NFS server
	// V2.3 G0: arm the FUSE identity gate before the server (and its
	// drainer) starts — drains/purges/prunes refuse to act while the
	// mountpoint has no real filesystem mounted on it.
	pin.SetFUSEIdentityPath(*fusePath)
	srv := jmnfs.NewServer(jmnfs.Config{
		ListenAddr: *listenAddr,
		FUSEPath:   *fusePath,
	}, store)

	// Wire dependencies before opening the listener; the first RPC must never
	// observe a partially initialized handler.
	srv.Handler().SetBoundCacheSession(cacheSession, rc, cachePaths)
	cacheHandler.Store(srv.Handler())
	if saved := cacheSession.Reader(); saved != nil {
		srv.Handler().SetCacheReader(saved)
	}
	srv.Handler().SetRedisClient(rc)
	// The selected saved binding is attached above. Restore Finder images
	// before the first listener request, including offline and no-Reader boot.
	if *dbPath != ":memory:" {
		srv.Handler().SidecarPersistEnable(filepath.Join(stateDir, "sidecars.gob"))
		startupCleanup.add(srv.Handler().SidecarPersistStop)
	}
	// Spool wiring (Option 2). Same env-gated path as bridge/cbridge.go
	// — JM_SPOOL_ENABLE=1 + optional JM_SPOOL_DIR / JM_SPOOL_SIZE_GB.
	// Defaults to ~/Library/Application Support/JuiceMount/spool/ on
	// Darwin or $TMPDIR/jm5-spool/ elsewhere.
	var (
		spoolStore    *jmnfs.SpoolStore
		drainer       *jmnfs.Drainer
		projectLeases *jmnfs.ProjectLeaseClient
	)
	prepareSpool := func() error {
		if os.Getenv("JM_SPOOL_ENABLE") == "1" {
			spoolDir := os.Getenv("JM_SPOOL_DIR")
			if spoolDir == "" {
				home, err := os.UserHomeDir()
				if err != nil || home == "" {
					spoolDir = filepath.Join(os.TempDir(), "jm5-spool")
					jmlog.Warn("UserHomeDir failed; spool dir fell back to tmp",
						"dir", spoolDir, "error", fmt.Sprintf("%v", err))
				} else {
					spoolDir = filepath.Join(home, "Library", "Application Support", "JuiceMount", "spool")
				}
			}
			// Auto-size to free disk by default so a large SD-card offload fits;
			// explicit JM_SPOOL_SIZE_GB still wins (clamped to disk in NewSpoolStore).
			spoolCapacity := jmnfs.AutoSpoolCapacity(spoolDir)
			if s := os.Getenv("JM_SPOOL_SIZE_GB"); s != "" {
				if v, err := strconv.ParseInt(s, 10, 64); err != nil || v < 1 {
					jmlog.Warn("JM_SPOOL_SIZE_GB ignored (must be >= 1), using auto-sized default",
						"raw", s)
				} else {
					spoolCapacity = v << 30
				}
			}
			meta := metadata.NewSpoolStore(store.DB())
			store.SetSpoolStore(meta) // recovery validates accepted local images against the mirror
			ss, err := jmnfs.NewSpoolStore(spoolDir, spoolCapacity, meta)
			if err != nil {
				return fmt.Errorf("open spool store before serving: %w", err)
			} else {
				startupCleanup.add(ss.Stop)
				dr, err := jmnfs.NewDrainer(ss, jmnfs.DrainerConfig{FuseRoot: *fusePath, ReplacementRecovery: *replacementRecovery})
				if err != nil {
					return fmt.Errorf("construct spool drainer before serving: %w", err)
				} else {
					// Slice F: boot-time scrubber. MUST run BEFORE
					// drainer.Start so it doesn't race with workers. Generous
					// deadline (5 min): recovery integrity outranks boot speed,
					// and 30s stranded `draining` rows at 50k+ (see cbridge.go).
					recCtx, recCancel := context.WithTimeout(context.Background(), 5*time.Minute)
					recReport, recErr := ss.RecoverOnBoot(recCtx)
					recCancel()
					if recErr != nil {
						return fmt.Errorf("recover local spool before serving: %w", recErr)
					} else if recReport.OrphanFilesDeleted > 0 || recReport.OrphanRowsFailed > 0 ||
						recReport.WritingFailedRows > 0 || recReport.DrainingReset > 0 ||
						recReport.ReadyResumed > 0 {
						jmlog.Info("spool boot recovery",
							"orphan_files_deleted", recReport.OrphanFilesDeleted,
							"orphan_rows_failed", recReport.OrphanRowsFailed,
							"writing_failed", recReport.WritingFailedRows,
							"draining_reset", recReport.DrainingReset,
							"ready_resumed", recReport.ReadyResumed)
					}

					projectLeases, err = newCLIReplacementLeases(*replacementRecovery, *redisURL, ss, dr)
					if err != nil {
						return fmt.Errorf("configure replacement recovery: %w", err)
					}
					if projectLeases != nil {
						startupCleanup.add(projectLeases.Stop)
					}
					srv.Handler().SetSpool(ss, dr)
					// QA-30 Layer D: scopedPrune spares paths with a live,
					// not-yet-drained spool entry (absent from Redis AND FUSE)
					// so an in-flight spool file is never pruned mid-copy and its
					// Track-B NFS handle never Forgotten (build-438 error 100070).
					rc.SetSpoolGuard(ss.HasPending)
					spoolStore = ss
					drainer = dr
					jmlog.Info("spool ready",
						"dir", spoolDir,
						"capacity_gb", float64(spoolCapacity)/(1<<30))
				}
			}
		}
		return nil
	}
	if err := runCLIStartup(prepareSpool, func() error {
		if *noFuse {
			if _, err := os.Stat(*fusePath); err != nil {
				return fmt.Errorf("FUSE mount not found at %s (use without --no-fuse to auto-mount): %w", *fusePath, err)
			}
			return nil
		}
		var err error
		fuseReceipt, err = fuseMgr.MountWithReceipt()
		if err != nil {
			return fmt.Errorf("JuiceFS FUSE mount: %w", err)
		}
		return nil
	}, func() {
		cacheSession.SetOnline(true)
		qualifyCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := cacheSession.Qualify(qualifyCtx); err != nil {
			jmlog.Warn("cache backend qualification unavailable; serving local mirror", "error", err)
		}
		cancel()
		jmlog.Info("initial metadata sync starting")
		start := time.Now()
		if err := rc.SyncOnce(); err != nil {
			// A transient sync failure leaves the existing mirror available;
			// the background reconcile loop retries after startup.
			jmlog.Warn("initial sync failed — serving existing mirror; reconcile loop retries",
				"error", err.Error())
		}
		count, _ := store.Count()
		jmlog.Info("initial metadata sync complete",
			"entries", count,
			"duration_ms", time.Since(start).Round(time.Millisecond).Milliseconds())
	}, func() error {
		if err := srv.Start(); err != nil {
			return fmt.Errorf("NFS server start: %w", err)
		}
		return nil
	}, func() {
		if fuseMgr != nil {
			fuseMgr.StartMonitor()
		}
		if projectLeases != nil {
			projectLeases.Start()
		}
		if drainer != nil {
			drainer.Start()
		}
		rc.Start()
		cacheNetwork.start(*redisURL, *fusePath)
	}, func(err error) error {
		return waitForCLICleanup(func() error {
			return failCLIStartup(err, srv.DisableStart, func() bool {
				return srv.QuiesceClients(7 * time.Second)
			}, srv.Handler().StopHandler, func() error {
				// Even a launch with no child receipt can have unknown path
				// occupancy. The manager owns that unresolved lifecycle.
				if fuseMgr != nil {
					result := fuseMgr.Stop()
					if !result.Complete() {
						return fmt.Errorf("FUSE: %s", result.String())
					}
					return nil
				}
				return fuseReceipt.Rollback()
			}, &startupCleanup)
		}, sigCh, 5*time.Second)
	}); err != nil {
		return err
	}
	jmlog.Info("nfs server listening", "addr", srv.Addr())

	// Wire per-RPC observation into the metrics package.
	jmlibnfs.SetObserver(metrics.ObserveRPC)

	// Start metrics HTTP server (exposes /metrics and /health, plus
	// the embedded manager UI at /manager/ when source roots are set).
	metricsSrv := metrics.NewServer(*metricsAddr, metrics.Default())

	// Wire the manager's routes onto the metrics listener — single
	// process, no separate container. Manager writes through the
	// in-process FUSE mount so chunks flow via the same juicefs
	// daemon as the NFS gateway.
	//
	// Pure root/key resolution and scoped preparation already completed before
	// storage startup. Preserve legacy alias and environment precedence.
	var mgrJobMgr *manager.JobManager
	if effectiveSourceRoots != "" {
		roots := splitNonEmpty(effectiveSourceRoots, ",")
		if metricsSrv.ExtraRoutes == nil {
			metricsSrv.ExtraRoutes = make(map[string]http.HandlerFunc)
		}
		// metrics.Server.ExtraRoutes maps a path → handler. The
		// manager's Register attaches its routes to a ServeMux,
		// not directly to ExtraRoutes — so we build a sub-mux and
		// register one catch-all that forwards everything under
		// /manager/ to it.
		mgrJobMgr = manager.Register(mgrMux, "/manager", manager.Config{
			JuiceFSBin:  "juicefs", // PATH lookup
			FUSEMount:   *fusePath, // embedded mode: write via in-process FUSE mount
			SourceRoots: roots,
			DestMount:   "/jfs",
			AdminKey:    effectiveAdminKey,
			// SLICE 2: Overview dashboard probes. Embedded mode doesn't
			// set MetaURL (FUSE handles writes), so we hand the Redis
			// URL in via OverviewMetaURL — same value, separate field
			// to keep the existing destination-resolution logic
			// untouched. MinIOURL matches the health-monitor's pin.
			MinIOURL:        *managerMinIOURL,
			OverviewMetaURL: *redisURL,
		})
		metricsSrv.ExtraRoutes["/manager/"] = mgrMux.ServeHTTP
		// Also handle the bare /manager (no trailing slash) so a
		// browser hitting that URL gets redirected to the UI.
		metricsSrv.ExtraRoutes["/manager"] = func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/manager/", http.StatusFound)
		}
		// SLICE 0 compat shim: 301 every legacy /migrator/* request
		// to the corresponding /manager/* path. The redirect is
		// permanent (browsers/proxies may cache) — the migrator
		// alias is dropped one release after slice-0 ships.
		redirector := manager.RedirectMigrator()
		metricsSrv.ExtraRoutes["/migrator/"] = redirector
		metricsSrv.ExtraRoutes["/migrator"] = redirector
		jmlog.Info("manager enabled",
			"prefix", "/manager",
			"source_roots", roots,
			"auth_enabled", effectiveAdminKey != "",
			"migrator_redirect", "/migrator/* → /manager/*")
	}

	// /spool — slice E. Always register the route; the handler 503s
	// when the spool isn't enabled so callers can detect the state
	// without conditional URLs.
	if metricsSrv.ExtraRoutes == nil {
		metricsSrv.ExtraRoutes = make(map[string]http.HandlerFunc)
	}
	metricsSrv.ExtraRoutes["/spool"] = func(w http.ResponseWriter, r *http.Request) {
		jmnfs.WriteSpoolStatusJSON(w, spoolStore, drainer)
	}

	// /whoami (contract JM-1). The jm5 CLI serves a SMALLER control plane than
	// the GUI core — only health/metrics/spool — so deployment="cli" and the
	// capability list derives to ["health","metrics","spool","whoami"].
	jm5InstanceID := cplane.LoadOrMintInstanceID(*dbPath)
	metricsSrv.ExtraRoutes["/whoami"] = func(w http.ResponseWriter, r *http.Request) {
		served := []string{"/health", "/metrics", "/whoami"}
		for route := range metricsSrv.ExtraRoutes {
			served = append(served, route)
		}
		addr := *metricsAddr
		if a := metricsSrv.Addr(); a != "" {
			addr = a
		}
		who := jm5WhoAmI(jm5InstanceID, *mountPoint, addr, *dbPath, served)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(who)
	}

	var continuityHostErr error
	if err := metricsSrv.Start(); err != nil {
		jmlog.Warn("metrics server failed to start", "error", err, "addr", *metricsAddr)
		if discovery != nil {
			continuityHostErr = err
		}
	} else {
		jmlog.Info("metrics server listening", "addr", metricsSrv.Addr())
	}
	monitorsStarted := continuityHostErr == nil
	nfsMountAttempted := false

	// 5. Start network watcher + health monitor
	netWatcher := health.NewNetWatcher(1 * time.Second)
	netWatcher.OnChange(func(oldIface, newIface string) {
		jmlog.Info("network interface changed",
			"old", oldIface, "new", newIface)
		cacheNetwork.interfaceChanged()
	})
	if monitorsStarted {
		netWatcher.Start()
	}

	healthCfg := newHealthConfig(*redisURL, *managerMinIOURL, *fusePath, *mountPoint, *noMount)
	healthCfg.RedisOptions = rc.RawDB().Options()
	healthMon := health.New(healthCfg)
	healthMon.SetNetWatcher(netWatcher)
	if monitorsStarted {
		healthMon.Start()
	}

	// Expose health status to /health endpoint.
	metrics.Default().SetHealthProvider(func() metrics.HealthSnapshot {
		return jm5HealthSnapshot(healthMon.Status(), *noMount, srv.IsRunning())
	})

	// 6. Mount NFS (unless --no-mount)
	if continuityHostErr == nil && !*noMount {
		nfsMountAttempted = true
		if err := mountNFS(srv.Addr(), srv.ExportPath(), *mountPoint, *dbPath); err != nil {
			jmlog.Error("NFS mount outcome incomplete; retaining serving dependencies", "error", err)
		} else {
			jmlog.Info("nfs mounted", "mount_point", *mountPoint)
		}
	}

	// Allow auto-remount only when we own the mount lifecycle.
	if continuityHostErr == nil && !*noMount {
		healthMon.EnableNFSRemount(func() error {
			return mountNFSNonInteractive(srv.Addr(), srv.ExportPath(), *mountPoint, *dbPath)
		})
	}
	// An opted-in listener failure shares the normal storage cleanup path.
	// A startup failure skips subsequent mount/admission instead of continuing
	// with a silently unavailable explicitly configured service.
	if continuityHostErr == nil {
		jmlog.Info("juicemount5 serving, awaiting signal")
		var failures <-chan error
		if discovery != nil {
			failures = metricsSrv.ServeErrors()
		}
		select {
		case <-sigCh:
		case continuityHostErr = <-failures:
		}
	}
	continuityCloseErr := closeEmbeddedContinuity(discovery.Close)
	if continuityCloseErr != nil {
		jmlog.Error("continuity closed with error", "error", continuityCloseErr)
	}

	// Graceful shutdown in dependency order:
	// 1. Unmount NFS (stop new RPCs from reaching the server)
	// 2. Stop NFS server (close listener, drain in-flight RPCs)
	// 3. Stop handler resources (fd pool, membuf, readahead)
	// 4. Stop network watcher and health monitor
	// 5. Stop Redis (close subscription and reconciliation)
	// 6. Close SQLite
	if nfsMountAttempted {
		cliNFSMu.Lock()
		cliNFSStopping = true
		cliNFSMu.Unlock()
	}
	jmlog.Info("shutting down")

	if nfsMountAttempted {
		jmlog.Info("unmounting", "mount_point", *mountPoint)
		healthMon.EnableNFSRemount(nil)
		healthMon.EnableNFSAbsentRemount(nil)
		waitForNFSMountAbsent(*mountPoint, sigCh, 5*time.Second, unmountNFS)
	}

	_ = waitForCLICleanup(func() error {
		if !prepareServerOnlyShutdown(srv, 7*time.Second) {
			return errors.Join(errCLICleanupIncomplete, errors.New("NFS clients still active"))
		}
		if !srv.Handler().StopHandler() {
			return errors.Join(errCLICleanupIncomplete, errors.New("NFS handler still active"))
		}
		return nil
	}, sigCh, 5*time.Second)

	if projectLeases != nil {
		projectLeases.Stop() // StopHandler proved the original drainer has joined.
	}

	if mgrJobMgr != nil {
		// Cancel in-flight migrations before tearing down the FUSE mount;
		// otherwise juicefs sync writes against a path that's
		// disappearing under it.
		mgrJobMgr.StopAll()
	}
	// FUSE still needs its backend while a child, helper or mount is live.
	// Retain SQLite/spool/Redis and status until the manager proves completion.
	if fuseMgr != nil {
		_ = waitForCLICleanup(func() error {
			result := fuseMgr.Stop()
			if !result.Complete() {
				return errors.Join(errCLICleanupIncomplete, fmt.Errorf("FUSE: %s", result.String()))
			}
			return nil
		}, sigCh, 5*time.Second)
	}
	var metricsCloseErr error
	if metricsSrv != nil {
		if discovery != nil {
			metricsCloseErr = stopEmbeddedMetrics(metricsSrv)
		} else {
			metricsSrv.Stop()
		}
	}
	if monitorsStarted {
		// The start-failure path above never started these monitors.
		netWatcher.Stop()
		healthMon.Stop()
	}
	cacheNetwork.stop()
	cacheHandler.Store(nil)
	srv.Handler().SidecarPersistStop()
	cacheSession.Stop()
	rc.Stop()
	store.Close()

	return errors.Join(continuityHostErr, continuityCloseErr, metricsCloseErr)
}

// componentLabel returns "ok" for healthy, or the underlying reason.
func componentLabel(healthy bool, msg string) string {
	if healthy {
		return "ok"
	}
	if msg == "" {
		return "unhealthy"
	}
	return msg
}

func newHealthConfig(redisURL, minioURL, fusePath, mountPoint string, serverOnly bool) health.Config {
	redisAddr, _, _ := metadata.ParseRedisURL(redisURL)
	if serverOnly {
		// A server container exports NFS to other machines; it intentionally
		// does not mount its own export as a client.
		mountPoint = ""
	}
	return health.Config{
		RedisURL:      redisAddr,
		MinIOURL:      minioURL,
		FUSEPath:      fusePath,
		NFSMountPoint: mountPoint,
	}
}

func jm5HealthSnapshot(st health.HealthStatus, serverOnly, serverRunning bool) metrics.HealthSnapshot {
	nfsHealthy := st.NFS.Healthy
	nfsMessage := st.NFS.Message
	overall := st.Overall
	if serverOnly {
		nfsHealthy = serverRunning
		nfsMessage = "listener stopped"
		overall = st.Redis.Healthy && st.MinIO.Healthy && st.FUSE.Healthy && serverRunning
	}
	components := map[string]string{
		"redis": componentLabel(st.Redis.Healthy, st.Redis.Message),
		"minio": componentLabel(st.MinIO.Healthy, st.MinIO.Message),
		"fuse":  componentLabel(st.FUSE.Healthy, st.FUSE.Message),
		"nfs":   componentLabel(nfsHealthy, nfsMessage),
	}
	reason := ""
	if !overall {
		reason = "degraded"
	}
	return metrics.HealthSnapshot{Healthy: overall, Components: components, Reason: reason}
}

func cliNFSMountSource(addr, exportPath string) (source, port string, err error) {
	if !validNFSExportPath(exportPath) {
		return "", "", fmt.Errorf("invalid NFS export path %q", exportPath)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "", "", fmt.Errorf("invalid addr: %s", addr)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", "", fmt.Errorf("invalid NFS listener port %q", port)
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return host + ":" + exportPath, port, nil
}

func validNFSExportPath(path string) bool {
	if len(path) != 36 || !strings.HasPrefix(path, "/jm-") {
		return false
	}
	nonzero := false
	for i := 4; i < len(path); i++ {
		b := path[i]
		if (b < '0' || b > '9') && (b < 'a' || b > 'f') {
			return false
		}
		nonzero = nonzero || b != '0'
	}
	return nonzero
}

func mountNFS(addr, exportPath, mountPoint, metadataPath string) error {
	return mountNFSWithProvisioning(addr, exportPath, mountPoint, metadataPath, true)
}
func mountNFSNonInteractive(addr, exportPath, mountPoint, metadataPath string) error {
	return mountNFSWithProvisioning(addr, exportPath, mountPoint, metadataPath, false)
}
func mountNFSWithProvisioning(addr, exportPath, mountPoint, metadataPath string, interactive bool) error {
	source, port, err := cliNFSMountSource(addr, exportPath)
	if err != nil {
		return err
	}
	creator, err := newCLINFSMountCreator()
	if err != nil {
		return err
	}
	spec := mountbroker.Spec{
		MountPath: mountPoint, Source: source, Endpoint: addr,
		ExportPath: exportPath, MetadataPath: metadataPath,
	}
	// Preserve the CLI's existing NFS options while moving birth and retirement
	// into the same exact-operation descriptor controller as the app service.
	opts := fmt.Sprintf("port=%s,mountport=%s,hard,intr,timeo=300,retrans=5,nonegnamecache,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=16,acregmin=3600,acregmax=3600,acdirmin=3,acdirmax=15,vers=3,tcp", port, port)
	cliNFSMu.Lock()
	if cliNFSStopping || cliNFSCustody != nil {
		cliNFSMu.Unlock()
		return errors.New("NFS generation already owned or stopping")
	}
	controller, err := newCLINFSCustodyController(creator, spec, opts, interactive)
	if err != nil {
		cliNFSMu.Unlock()
		return err
	}
	cliNFSCustody = controller
	cliNFSMu.Unlock()
	return controller.Start(context.Background())
}

func splitAddr(addr string) []string {
	// Split "host:port" — handles IPv4 only
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return []string{addr[:i], addr[i+1:]}
		}
	}
	return []string{addr}
}

func prepareServerOnlyShutdown(srv *jmnfs.Server, deadline time.Duration) bool {
	if err := srv.DisableStart(); err != nil {
		jmlog.Error("cannot retire NFS listener", "error", err)
		return false
	}
	return srv.QuiesceClients(deadline)
}

// Without an instance-safe broker, only observed absence completes shutdown.
// An error is unknown, never absence, and no pathname unmount is attempted.
func unmountNFS(mountPoint string) bool {
	cliNFSMu.Lock()
	controller := cliNFSCustody
	cliNFSMu.Unlock()
	if controller != nil {
		if err := controller.Stop(context.Background(), 20*time.Second); err != nil {
			jmlog.Error("NFS custody retirement incomplete", "error", err)
			return false
		}
	}
	creator, err := newCLINFSMountCreator()
	if err != nil {
		return false
	}
	snapshot, err := creator.Observe(context.Background())
	if err != nil {
		return false
	}
	if !mountbroker.ObservedAbsent(snapshot, mountPoint) {
		return false
	}
	if controller != nil {
		cliNFSMu.Lock()
		if cliNFSCustody == controller {
			cliNFSCustody = nil
		}
		cliNFSMu.Unlock()
	}
	return true
}

var newCLINFSMountCreator = mountbroker.LocalCreator

type cliNFSController interface {
	Start(context.Context) error
	Stop(context.Context, time.Duration) error
}

var cliNFSMu sync.Mutex
var cliNFSCustody cliNFSController
var cliNFSStopping bool
var newCLINFSCustodyController = func(creator mountbroker.Creator, spec mountbroker.Spec, opts string, interactive bool) (cliNFSController, error) {
	return nfscustody.NewWithProvisioning(creator, spec, opts, interactive)
}

func waitForNFSMountAbsent(mountPoint string, signals <-chan os.Signal, interval time.Duration, inspect func(string) bool) {
	for !inspect(mountPoint) {
		jmlog.Warn("NFS shutdown incomplete; retaining serving dependencies", "mount_point", mountPoint)
		timer := time.NewTimer(interval)
		select {
		case <-signals:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

// Each attempt is bounded by its owning component. An incomplete attempt
// retains dependencies and retries automatically; repeated signals only wake
// an observation and never turn uncertainty into successful process exit.
func waitForCLICleanup(attempt func() error, signals <-chan os.Signal, interval time.Duration) error {
	for {
		err := attempt()
		if !errors.Is(err, errCLICleanupIncomplete) {
			return err
		}
		jmlog.Warn("CLI cleanup incomplete; retaining serving dependencies", "error", err)
		timer := time.NewTimer(interval)
		select {
		case <-signals:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

// jm5WhoAmI builds the CLI's /whoami body. device_digest is filled the same
// way as the GUI core's (cplane.DeviceDigest) so both servers agree.
func jm5WhoAmI(instanceID, mountPoint, addr, dbPath string, served []string) cplane.WhoAmI {
	return cplane.WhoAmI{
		App:             "JuiceMount",
		Version:         version.Version,
		ContractVersion: cplane.ContractVersion,
		InstanceID:      instanceID,
		DeviceDigest:    cplane.DeviceDigest(),
		VolumeName:      filepath.Base(mountPoint),
		MountPoint:      mountPoint,
		NASRoot:         mountPoint,
		ControlPlane:    "http://" + addr,
		MetadataDBPath:  dbPath,
		Deployment:      "cli",
		WireTerms:       cplane.WireTerms,
		Capabilities:    cplane.DeriveCapabilities(served),
	}
}
