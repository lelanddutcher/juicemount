package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	jmlibnfs "github.com/lelanddutcher/juicemount/internal/nfs"
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

func main() {
	redisURL := flag.String("redis", "redis://127.0.0.1:6379/1", "Redis URL")
	fusePath := flag.String("fuse-path", "", "Path to JuiceFS FUSE mount (auto-detected if empty)")
	mountPoint := flag.String("mount", "/Volumes/zpool", "Mount point for NFS volume")
	listenAddr := flag.String("listen", "127.0.0.1:11049", "NFS server listen address")
	dbPath := flag.String("db", "", "SQLite database path (default: ~/.juicemount/metadata.db)")
	noMount := flag.Bool("no-mount", false, "Start NFS server without mounting (for testing)")
	noFuse := flag.Bool("no-fuse", false, "Skip JuiceFS FUSE mount (assume already mounted)")
	cacheSize := flag.String("cache-size", "100000", "JuiceFS SSD cache size in MB")
	bucketOverride := flag.String("bucket-override", "", "Override the S3 bucket URL stored in Redis (e.g. http://192.168.0.197:30151/zpool). Empty = use the URL written by juicefs format.")
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
	migratorSourceRoots := flag.String("migrator-source-roots", "", "DEPRECATED: alias for --manager-source-roots, kept for one-release compat.")
	migratorAdminKey := flag.String("migrator-admin-key", "", "DEPRECATED: alias for --manager-admin-key, kept for one-release compat.")
	flag.Parse()

	// Initialize structured logging before anything else logs.
	if err := jmlog.Init(jmlog.Config{
		LogFile: *logFile,
		Level:   jmlog.ParseLevel(*logLevel),
	}); err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer jmlog.Close()

	// Defaults
	if *fusePath == "" {
		*fusePath = filepath.Join(os.Getenv("HOME"), ".juicemount", "fuse-internal")
	}
	if *dbPath == "" {
		dir := filepath.Join(os.Getenv("HOME"), ".juicemount")
		os.MkdirAll(dir, 0755)
		*dbPath = filepath.Join(dir, "metadata.db")
	}

	// Clean up stale sessions from previous runs
	cleanupStaleSession(*mountPoint, *listenAddr)

	jmlog.Info("starting JuiceMount5",
		"redis", *redisURL,
		"fuse", *fusePath,
		"mount", *mountPoint,
		"db", *dbPath,
		"metrics_addr", *metricsAddr,
	)

	// 0. Mount JuiceFS FUSE (unless --no-fuse)
	var fuseMgr *health.FUSEManager
	if !*noFuse {
		fuseMgr = health.NewFUSEManager(health.FUSEConfig{
			RedisURL:       *redisURL,
			MountPoint:     *fusePath,
			CacheSize:      *cacheSize,
			BucketOverride: *bucketOverride,
		})
		if err := fuseMgr.Mount(); err != nil {
			jmlog.Error("juicefs FUSE mount failed", "error", err)
			log.Fatalf("JuiceFS FUSE mount: %v", err)
		}
		fuseMgr.StartMonitor()
	} else {
		// Verify FUSE mount exists when not managing it
		if _, err := os.Stat(*fusePath); err != nil {
			log.Fatalf("FUSE mount not found at %s (use without --no-fuse to auto-mount)", *fusePath)
		}
	}

	// 1. Open metadata store
	store, err := metadata.Open(*dbPath)
	if err != nil {
		log.Fatalf("Open metadata store: %v", err)
	}

	// 2. Connect to Redis and do initial sync
	rc, err := metadata.NewRedisClient(*redisURL, store)
	if err != nil {
		log.Fatalf("Redis connect: %v", err)
	}

	jmlog.Info("initial metadata sync starting")
	start := time.Now()
	if err := rc.SyncOnce(); err != nil {
		log.Fatalf("Initial sync: %v", err)
	}
	count, _ := store.Count()
	jmlog.Info("initial metadata sync complete",
		"entries", count,
		"duration_ms", time.Since(start).Round(time.Millisecond).Milliseconds(),
	)

	// Start background sync (SUBSCRIBE + reconciliation)
	rc.Start()

	// 3. Set up SSD cache reader
	cacheDir := cache.DetectCacheDir()
	var cr *cache.Reader
	if cacheDir != "" {
		addr, db, _ := metadata.ParseRedisURL(*redisURL)
		rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})
		cr = cache.NewReader(cacheDir, cache.DefaultBlockSize, rdb)
		if err := cr.Verify(); err != nil {
			jmlog.Warn("ssd cache reader disabled", "error", err)
			cr = nil
		} else {
			jmlog.Info("ssd cache reader enabled")
		}
	}

	// 4. Start NFS server
	srv := jmnfs.NewServer(jmnfs.Config{
		ListenAddr: *listenAddr,
		FUSEPath:   *fusePath,
	}, store)

	if err := srv.Start(); err != nil {
		log.Fatalf("NFS server start: %v", err)
	}

	// Attach cache reader and redis client to handler
	if cr != nil {
		srv.Handler().SetCacheReader(cr)
	}
	srv.Handler().SetRedisClient(rc)

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
	// SLICE 0 rename compat: --manager-* wins if set; otherwise fall
	// back to the legacy --migrator-* aliases.
	effectiveSourceRoots := *managerSourceRoots
	if effectiveSourceRoots == "" {
		effectiveSourceRoots = *migratorSourceRoots
	}
	// Admin-key precedence: explicit --manager-admin-key > explicit
	// --migrator-admin-key (legacy alias) > JM_ADMIN_KEY env var. The
	// env var is only a fallback so an operator who passes a flag
	// value explicitly always overrides whatever's in env (matches
	// pre-rename behaviour of --migrator-admin-key, which had env as
	// its flag default and thus was overridden by any explicit value).
	effectiveAdminKey := *managerAdminKey
	if effectiveAdminKey == "" {
		effectiveAdminKey = *migratorAdminKey
	}
	if effectiveAdminKey == "" {
		effectiveAdminKey = os.Getenv("JM_ADMIN_KEY")
	}
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
		mgrMux := http.NewServeMux()
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

	if err := metricsSrv.Start(); err != nil {
		jmlog.Warn("metrics server failed to start", "error", err, "addr", *metricsAddr)
	} else {
		jmlog.Info("metrics server listening", "addr", metricsSrv.Addr())
	}

	// 5. Start network watcher + health monitor
	netWatcher := health.NewNetWatcher(1 * time.Second)
	netWatcher.OnChange(func(oldIface, newIface string) {
		jmlog.Info("network interface changed",
			"old", oldIface, "new", newIface)
		if err := rc.Reconnect(); err != nil {
			jmlog.Warn("redis reconnect failed (will retry)", "error", err)
		} else {
			jmlog.Info("redis reconnected, triggering immediate sync")
			rc.TriggerSync()
		}
	})
	netWatcher.Start()

	healthMon := health.New(health.Config{
		RedisURL: func() string {
			addr, _, _ := metadata.ParseRedisURL(*redisURL)
			return addr
		}(),
		MinIOURL:      "http://127.0.0.1:9000",
		FUSEPath:      *fusePath,
		NFSMountPoint: *mountPoint,
	})
	healthMon.SetNetWatcher(netWatcher)
	// Allow auto-remount only when we own the mount lifecycle.
	if !*noMount {
		healthMon.EnableNFSRemount(func() error {
			return mountNFS(srv.Addr(), *mountPoint)
		})
	}
	healthMon.Start()

	// Expose health status to /health endpoint.
	metrics.Default().SetHealthProvider(func() metrics.HealthSnapshot {
		st := healthMon.Status()
		comps := map[string]string{
			"redis": componentLabel(st.Redis.Healthy, st.Redis.Message),
			"minio": componentLabel(st.MinIO.Healthy, st.MinIO.Message),
			"fuse":  componentLabel(st.FUSE.Healthy, st.FUSE.Message),
			"nfs":   componentLabel(st.NFS.Healthy, st.NFS.Message),
		}
		reason := ""
		if !st.Overall {
			reason = "degraded"
		}
		return metrics.HealthSnapshot{
			Healthy:    st.Overall,
			Components: comps,
			Reason:     reason,
		}
	})

	// 6. Mount NFS (unless --no-mount)
	if !*noMount {
		if err := mountNFS(srv.Addr(), *mountPoint); err != nil {
			log.Fatalf("NFS mount: %v", err)
		}
		jmlog.Info("nfs mounted", "mount_point", *mountPoint)
	}

	jmlog.Info("juicemount5 ready, awaiting signal")

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown in dependency order:
	// 1. Unmount NFS (stop new RPCs from reaching the server)
	// 2. Stop NFS server (close listener, drain in-flight RPCs)
	// 3. Stop handler resources (fd pool, membuf, readahead)
	// 4. Stop network watcher and health monitor
	// 5. Stop Redis (close subscription and reconciliation)
	// 6. Close SQLite
	jmlog.Info("shutting down")

	if !*noMount {
		jmlog.Info("unmounting", "mount_point", *mountPoint)
		unmountNFS(*mountPoint)
	}

	srv.Stop()
	srv.Handler().StopHandler()

	if mgrJobMgr != nil {
		// Cancel in-flight migrations before tearing down the FUSE mount;
		// otherwise juicefs sync writes against a path that's
		// disappearing under it.
		mgrJobMgr.StopAll()
	}
	if metricsSrv != nil {
		metricsSrv.Stop()
	}
	netWatcher.Stop()
	healthMon.Stop()

	if cr != nil {
		cr.Stop()
	}
	rc.Stop()
	store.Close()

	// 7. Unmount JuiceFS FUSE last (after everything else is stopped)
	if fuseMgr != nil {
		fuseMgr.Stop()
	}
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

func mountNFS(addr, mountPoint string) error {
	// /Volumes requires root to create dirs
	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		exec.Command("sudo", "mkdir", "-p", mountPoint).Run()
	}

	// Parse host:port
	host := "127.0.0.1"
	port := "11049"
	parts := splitAddr(addr)
	if len(parts) == 2 {
		host = parts[0]
		port = parts[1]
	} else {
		return fmt.Errorf("invalid addr: %s", addr)
	}

	opts := fmt.Sprintf("port=%s,mountport=%s,soft,intr,timeo=300,retrans=5,nolocks,locallocks,rsize=1048576,wsize=1048576,readahead=128,actimeo=3600,vers=3,tcp", port, port)
	cmd := exec.Command("sudo", "mount_nfs", "-o", opts,
		fmt.Sprintf("%s:/", host), mountPoint)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount_nfs: %v\n%s", err, out)
	}
	return nil
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

// cleanupStaleSession kills any leftover JM5 processes, frees the NFS port,
// and unmounts stale NFS mounts from a previous run that didn't shut down cleanly.
func cleanupStaleSession(mountPoint, listenAddr string) {
	// Kill any old jm5 processes (but not us or JuiceFS)
	myPID := os.Getpid()
	out, _ := exec.Command("pgrep", "-x", "jm5").Output()
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pid := strings.TrimSpace(line)
		if pid == "" {
			continue
		}
		pidInt := 0
		fmt.Sscanf(pid, "%d", &pidInt)
		if pidInt != 0 && pidInt != myPID {
			jmlog.Info("killing stale jm5 process", "pid", pid)
			exec.Command("kill", "-9", pid).Run()
		}
	}

	// Unmount stale NFS mount
	exec.Command("sudo", "umount", "-f", mountPoint).Run()

	// Wait for port to be free
	parts := splitAddr(listenAddr)
	if len(parts) == 2 {
		for i := 0; i < 10; i++ {
			out, _ := exec.Command("lsof", "-i", ":"+parts[1]).Output()
			if !strings.Contains(string(out), "LISTEN") {
				break
			}
			jmlog.Info("waiting for port to release", "port", parts[1])
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func unmountNFS(mountPoint string) {
	// Try graceful unmount first
	if err := exec.Command("sudo", "umount", mountPoint).Run(); err != nil {
		// Force unmount
		exec.Command("sudo", "umount", "-f", mountPoint).Run()
	}
	os.Remove(mountPoint)
}
