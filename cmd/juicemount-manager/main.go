// juicemount-manager is a standalone wrapper around the
// internal/manager package. It exists for users who run the
// manager as its own container — typically because they're NOT
// running juicemount-server on the same host (e.g. TrueNAS users
// who only have MinIO + Redis + a vanilla juicefs container with
// the NFS gateway running on their Mac).
//
// Two deployment modes are supported (pick one):
//
//   Standalone, no FUSE (default for this binary):
//     --meta redis://host:port/N --vol-name zpool
//     Writes go via jfs://<vol-name>/<path>; juicefs sync talks to
//     Redis + MinIO directly. Requires network reachability to both.
//     The manager container has no FUSE mount of its own.
//
//   Embedded-style, FUSE-mounted:
//     --fuse-mount /jfs
//     Writes go via file:///<fuse-mount>/<path>; the container must
//     have the JuiceFS volume FUSE-mounted at /jfs before launch.
//     Use this when juicefs sync needs to inherit a pre-existing
//     mount (e.g. shared with juicemount-server).
//
// For users running juicemount-server on the host, the manager is
// automatically embedded — set --manager-source-roots when launching
// jm5 and the UI mounts at /manager/ on the existing metrics port.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
)

var version = "dev"

func main() {
	addr := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	juicefsBin := flag.String("juicefs", "juicefs", "Path to juicefs binary (or just 'juicefs' for PATH lookup)")
	fuseMount := flag.String("fuse-mount", "", "If set: embedded-mode FUSE mount path (writes via file:///<fuse-mount>/<path>). Mutually exclusive with --meta.")
	metaURL := flag.String("meta", "", "Standalone-mode Redis URL (e.g. redis://192.168.0.197:30179/1). Mutually exclusive with --fuse-mount.")
	volName := flag.String("vol-name", "zpool", "Standalone-mode JuiceFS volume name (used in jfs:// destination URIs)")
	destMount := flag.String("dest-mount", "/jfs", "User-facing destination prefix shown in the UI")
	sourceRoots := flag.String("source-roots", "/sources", "Comma-separated host paths the manager may browse from")
	adminKey := flag.String("admin-key", os.Getenv("JM_ADMIN_KEY"), "Admin key for X-JuiceMount-Admin-Key auth (empty = disabled)")
	stateFile := flag.String("state-file", os.Getenv("JM_STATE_FILE"), "Optional JSON path for job-history persistence (empty = jobs lost on restart). Bind-mount the dir to make history survive container churn.")
	minioURL := flag.String("minio-url", envOr("JM_MINIO_URL", ""), "SLICE 2: MinIO base URL the Overview dashboard pings via /minio/health/live. Empty disables the MinIO probe (Overview card shows an actionable hint). Use the same URL Mac clients connect to so the dashboard reflects what they see.")
	flag.Parse()

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

	mux := http.NewServeMux()
	cfg := manager.Config{
		JuiceFSBin:  *juicefsBin,
		FUSEMount:   *fuseMount, // embedded mode if non-empty
		MetaURL:     *metaURL,   // standalone mode if non-empty
		VolName:     *volName,
		SourceRoots: roots,
		DestMount:   *destMount,
		AdminKey:    *adminKey,
		StateFile:   *stateFile,
		MinIOURL:    *minioURL,
	}
	mgr := manager.Register(mux, "", cfg)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mode := "standalone (jfs://)"
	if *fuseMount != "" {
		mode = "embedded (file://)"
	}
	log.Printf("juicemount-manager %s starting on %s [mode: %s]", version, *addr, mode)
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

// envOr returns the environment variable's value if set+non-empty,
// otherwise fallback. Used as a flag-default helper so JM_* env vars
// override the hardcoded defaults without breaking explicit flag values.
func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
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
