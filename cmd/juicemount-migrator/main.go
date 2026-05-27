// juicemount-migrator is a standalone wrapper around the
// internal/migrator package. It exists for users who run the
// migrator as its own container — typically because they're NOT
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
//     The migrator container has no FUSE mount of its own.
//
//   Embedded-style, FUSE-mounted:
//     --fuse-mount /jfs
//     Writes go via file:///<fuse-mount>/<path>; the container must
//     have the JuiceFS volume FUSE-mounted at /jfs before launch.
//     Use this when juicefs sync needs to inherit a pre-existing
//     mount (e.g. shared with juicemount-server).
//
// For users running juicemount-server on the host, the migrator is
// automatically embedded — set --migrator-source-roots when launching
// jm5 and the UI mounts at /migrator/ on the existing metrics port.
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

	"github.com/lelanddutcher/juicemount/internal/migrator"
)

var version = "dev"

func main() {
	addr := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	juicefsBin := flag.String("juicefs", "juicefs", "Path to juicefs binary (or just 'juicefs' for PATH lookup)")
	fuseMount := flag.String("fuse-mount", "", "If set: embedded-mode FUSE mount path (writes via file:///<fuse-mount>/<path>). Mutually exclusive with --meta.")
	metaURL := flag.String("meta", "", "Standalone-mode Redis URL (e.g. redis://192.168.0.197:30179/1). Mutually exclusive with --fuse-mount.")
	volName := flag.String("vol-name", "zpool", "Standalone-mode JuiceFS volume name (used in jfs:// destination URIs)")
	destMount := flag.String("dest-mount", "/jfs", "User-facing destination prefix shown in the UI")
	sourceRoots := flag.String("source-roots", "/sources", "Comma-separated host paths the migrator may browse from")
	adminKey := flag.String("admin-key", os.Getenv("JM_ADMIN_KEY"), "Admin key for X-JuiceMount-Admin-Key auth (empty = disabled)")
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
	cfg := migrator.Config{
		JuiceFSBin:  *juicefsBin,
		FUSEMount:   *fuseMount, // embedded mode if non-empty
		MetaURL:     *metaURL,   // standalone mode if non-empty
		VolName:     *volName,
		SourceRoots: roots,
		DestMount:   *destMount,
		AdminKey:    *adminKey,
	}
	mgr := migrator.Register(mux, "", cfg)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mode := "standalone (jfs://)"
	if *fuseMount != "" {
		mode = "embedded (file://)"
	}
	log.Printf("juicemount-migrator %s starting on %s [mode: %s]", version, *addr, mode)
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
