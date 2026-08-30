// juicemount-manager is a standalone wrapper around the
// internal/manager package. It exists for users who run the
// manager as its own container — typically because they're NOT
// running juicemount-server on the same host (e.g. TrueNAS users
// who only have MinIO + Redis + a vanilla juicefs container with
// the NFS gateway running on their Mac).
//
// Two deployment modes are supported (pick one):
//
//	Standalone, no FUSE (default for this binary):
//	  --meta redis://host:port/N --vol-name zpool
//	  Writes go via jfs://<vol-name>/<path>; juicefs sync talks to
//	  Redis + MinIO directly. Requires network reachability to both.
//	  The manager container has no FUSE mount of its own.
//
//	Embedded-style, FUSE-mounted:
//	  --fuse-mount /jfs
//	  Writes go via file:///<fuse-mount>/<path>; the container must
//	  have the JuiceFS volume FUSE-mounted at /jfs before launch.
//	  Use this when juicefs sync needs to inherit a pre-existing
//	  mount (e.g. shared with juicemount-server).
//
// For users running juicemount-server on the host, the manager is
// automatically embedded — set --manager-source-roots when launching
// jm5 and the UI mounts at /manager/ on the existing metrics port.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/internal/version"
)

func main() {
	buildInfo := flag.Bool("build-info", false, "print release version and source commit, then exit")
	addr := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	juicefsBin := flag.String("juicefs", "juicefs", "Path to juicefs binary (or just 'juicefs' for PATH lookup)")
	fuseMount := flag.String("fuse-mount", "", "If set: embedded-mode FUSE mount path (writes via file:///<fuse-mount>/<path>). Mutually exclusive with --meta.")
	metaURL := flag.String("meta", "", "Standalone-mode Redis URL (e.g. redis://<server-ip>:30179/1). Mutually exclusive with --fuse-mount.")
	volName := flag.String("vol-name", "zpool", "Standalone-mode JuiceFS volume name (used in jfs:// destination URIs)")
	destMount := flag.String("dest-mount", "/jfs", "User-facing destination prefix shown in the UI")
	sourceRoots := flag.String("source-roots", "/sources", "Comma-separated host paths the manager may browse from")
	adminKey := flag.String("admin-key", os.Getenv("JM_ADMIN_KEY"), "Admin key for X-JuiceMount-Admin-Key auth (empty = disabled)")
	stateFile := flag.String("state-file", os.Getenv("JM_STATE_FILE"), "Optional JSON path for job-history persistence (empty = jobs lost on restart). Bind-mount the dir to make history survive container churn.")
	minioURL := flag.String("minio-url", envOr("JM_MINIO_URL", ""), "SLICE 2: MinIO base URL the Overview dashboard pings via /minio/health/live. Empty disables the MinIO probe (Overview card shows an actionable hint). Use the same URL Mac clients connect to so the dashboard reflects what they see.")
	farmStatus := flag.String("farm-status", envOr("JM_FARM_STATUS", ""), "Path to the juicefarm rollup (farm-status.json) for the Farm tab. Empty = Farm tab shows an empty state. Mount the juicefarm-state volume read-only to enable.")
	farmStorage := flag.String("farm-storage-path", envOr("JM_FARM_STORAGE_PATH", ""), "Local path on the JuiceFS backend pool used for the farm headroom safety interlock. Empty derives the parent of --farm-status.")
	farmMinFree := flag.Uint64("farm-min-free-bytes", envUint64Or("JM_FARM_MIN_FREE_BYTES", 0), "Minimum backend bytes required before farm enqueue/resume. Zero uses max(64 GiB, 1% of the probed filesystem).")
	farmStorageCapacity := flag.Uint64("farm-storage-capacity-bytes", envUint64Or("JM_FARM_STORAGE_CAPACITY_BYTES", 0), "Physical backend-pool capacity for correct reporting and the default 1% reserve. Set this on ZFS because statfs is dataset-scoped; zero falls back to statfs.")
	mountOwner := flag.String("mount-owner", envOr("JM_MOUNT_OWNER", ""), "POSIX owner (uid[:gid], e.g. 501:20) that migrated data is chowned to after an embedded-mode sync, so the CLIENT mounting the volume can WRITE it — not just read it. The manager runs as root on the NAS, so without this, `juicefs sync` leaves migrated files root:wheel and a uid-501 Mac client can only read them. Empty = leave raw sync ownership. Set to the uid your Mac client mounts as (usually 501:20).")
	overviewMeta := flag.String("overview-meta", envOr("JM_OVERVIEW_META", ""), "Redis URL for the Overview tab's `juicefs status` + Redis INFO probes. Use this in EMBEDDED mode (--fuse-mount), where --meta is unavailable (mutually exclusive), so Overview still works. In standalone mode --meta already serves both and this can stay empty.")
	flag.Parse()
	if *buildInfo {
		fmt.Printf("juicemount-manager %s (%s)\n", version.Version, version.Commit)
		return
	}

	roots := splitNonEmpty(*sourceRoots, ",")
	if len(roots) == 0 {
		log.Fatal("--source-roots must contain at least one path")
	}
	if *fuseMount == "" && *metaURL == "" {
		log.Fatal("exactly one of --fuse-mount or --meta must be set")
	}
	if *fuseMount != "" && *metaURL != "" {
		log.Fatal("--fuse-mount and --meta are mutually exclusive (pick one mode)")
	}
	if *metaURL != "" && *volName == "" {
		log.Fatal("--vol-name is required with --meta (standalone mode)")
	}
	if err := validateAdminKey(headscaleEnabled(), *adminKey); err != nil {
		log.Fatal(err)
	}

	ownerUID, ownerGID := parseOwner(*mountOwner)

	mux := http.NewServeMux()
	cfg := manager.Config{
		JuiceFSBin:               *juicefsBin,
		FUSEMount:                *fuseMount, // embedded mode if non-empty
		MetaURL:                  *metaURL,   // standalone mode if non-empty
		VolName:                  *volName,
		SourceRoots:              roots,
		DestMount:                *destMount,
		AdminKey:                 *adminKey,
		StateFile:                *stateFile,
		MinIOURL:                 *minioURL,
		FarmStatusPath:           *farmStatus,
		FarmStoragePath:          *farmStorage,
		FarmMinFreeBytes:         *farmMinFree,
		FarmStorageCapacityBytes: *farmStorageCapacity,
		MountOwnerUID:            ownerUID,
		MountOwnerGID:            ownerGID,
		OverviewMetaURL:          *overviewMeta,
	}
	mgr := manager.Register(mux, "", cfg)

	// Teams/seats is intentionally withheld from the RC. Its old account
	// scaffold did not enforce permissions on Manager or JuiceFS operations,
	// so enabling it would create a false security boundary.

	// Tier-2 T2.1: embedded Headscale + pairing endpoint (JuiceMount Link).
	// Off by default; JM_NET_HEADSCALE=on opts the deployment in.
	if headscaleEnabled() {
		hs := &headscaleSupervisor{}
		if err := hs.Start(); err != nil {
			// Link must never take the manager down with it: a bad config or
			// binary mismatch degrades to "pairing unavailable", not a dead UI.
			log.Printf("ERROR: headscale supervisor failed — Link disabled: %v", err)
		} else {
			defer hs.Stop()
			log.Printf("JuiceMount Link: headscale supervising on %s (external %s)", hsListen, externalURL())
			// T2.2: join the tailnet as the NAS itself and advertise/approve
			// the LAN subnet so linked Macs reach Redis/MinIO/manager remotely.
			if nasNodeEnabled() {
				nas := &nasNodeSupervisor{}
				if err := nas.Start(hsDataDir + "/config.yaml"); err != nil {
					log.Printf("WARN: nas tailnet node failed — remote LAN routing disabled: %v", err)
				} else {
					defer nas.Stop()
				}
			}
		}
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mode := "standalone (jfs://)"
	if *fuseMount != "" {
		mode = "embedded (file://)"
	}
	log.Printf("juicemount-manager %s (%s) starting on %s [mode: %s]", version.Version, version.Commit, *addr, mode)
	log.Printf("  juicefs:      %s", *juicefsBin)
	if *fuseMount != "" {
		log.Printf("  fuse-mount:   %s", *fuseMount)
	} else {
		log.Printf("  meta:         %s", *metaURL)
		log.Printf("  vol-name:     %s", *volName)
	}
	log.Printf("  dest-mount:   %s", *destMount)
	log.Printf("  source-roots: %v", roots)
	log.Printf("  auth enabled: %v", *adminKey != "")
	log.Printf("  state-file:   %s", *stateFile)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down...")
	mgr.StopAll()
	_ = srv.Close()
}

// validateAdminKey enforces the Manager deployment authentication boundary.
// Link enrollment grants network reachability to Redis, object storage, and the
// Manager itself, so Link always requires authentication. Local development may
// deliberately run without auth while Link is disabled, but a supplied key must
// never be a short/placeholder value: accepting one would let an unedited
// TrueNAS YAML appear healthy with a publicly known credential.
func validateAdminKey(linkEnabled bool, adminKey string) error {
	key := strings.TrimSpace(adminKey)
	if key == "" {
		if !linkEnabled && adminKey == "" {
			return nil
		}
		if !linkEnabled {
			return fmt.Errorf("JM_ADMIN_KEY must not contain only whitespace")
		}
		return fmt.Errorf("JuiceMount Link requires JM_ADMIN_KEY; refusing to start an unauthenticated remote control plane")
	}
	upper := strings.ToUpper(key)
	for _, prefix := range []string{"CHANGEME", "CHANGE", "REPLACE", "REPLACEME"} {
		if strings.HasPrefix(upper, prefix) {
			return fmt.Errorf("JM_ADMIN_KEY is still a placeholder; edit the deployment configuration")
		}
	}
	if len(key) < 32 {
		return fmt.Errorf("JM_ADMIN_KEY must contain at least 32 characters")
	}
	return nil
}

// envOr returns the environment variable's value if set+non-empty,
// otherwise fallback. Used as a flag-default helper so JM_* env vars
// override the hardcoded defaults without breaking explicit flag values.
func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func envUint64Or(name string, fallback uint64) uint64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		log.Fatalf("%s must be an unsigned byte count: %v", name, err)
	}
	return n
}

// parseOwner parses a "uid[:gid]" mount-owner string into numeric ids.
// Empty / unparseable → (0, -1), which the migration treats as "unset"
// (uid <= 0 skips the post-sync chown, preserving raw ownership). A gid is
// optional; when absent it returns -1, leaving the group unchanged.
func parseOwner(s string) (uid, gid int) {
	uid, gid = 0, -1
	s = strings.TrimSpace(s)
	if s == "" {
		return uid, gid
	}
	parts := strings.SplitN(s, ":", 2)
	if u, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
		uid = u
	}
	if len(parts) == 2 {
		if g, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			gid = g
		}
	}
	return uid, gid
}

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
