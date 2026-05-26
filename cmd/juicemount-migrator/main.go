//go:build migrator_wip
// +build migrator_wip

// juicemount-migrator is a small HTTP server that drives `juicefs sync`
// for migrating media libraries from an existing dataset (or any source
// backend juicefs sync supports) into a freshly-installed JuiceFS
// volume.
//
// Designed to ship as a sidecar container alongside the JuiceMount
// TrueNAS app. The container mounts the user's existing source datasets
// read-only and the new JuiceFS volume read-write, and serves a small
// web UI on port :8080 that lets the user browse the source tree,
// select directories, and kick off background sync jobs with live
// progress streaming via SSE.
//
// Auth: optional admin-key gate via X-JuiceMount-Admin-Key header,
// matching the JuiceMount server convention. Set JM_ADMIN_KEY in env
// to enable; empty means no auth (LAN-only use).
//
// Endpoints:
//   GET    /                       — static index.html (the UI)
//   GET    /api/sources             — list browsable source roots
//   GET    /api/browse?path=...     — list directory entries under path
//   POST   /api/migrate             — start a sync job
//   GET    /api/jobs                — list all jobs
//   GET    /api/jobs/{id}           — get one job's current state
//   GET    /api/jobs/{id}/stream    — SSE stream of progress updates
//   DELETE /api/jobs/{id}           — cancel a running job

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// version is overwritten by ldflags at build time.
var version = "dev"

func main() {
	addr := flag.String("listen", "0.0.0.0:8080", "HTTP listen address")
	juicefsBin := flag.String("juicefs", "juicefs", "Path to juicefs binary")
	metaURL := flag.String("meta", "", "JuiceFS metadata URL (e.g. redis://redis:6379/1). REQUIRED.")
	destMount := flag.String("dest-mount", "/jfs", "Filesystem path of the JuiceFS mount (used for path-style destinations in the UI)")
	sourceRoots := flag.String("source-roots", "/sources", "Comma-separated list of host paths the migrator may read from (typically bind-mounted into the container)")
	adminKey := flag.String("admin-key", os.Getenv("JM_ADMIN_KEY"), "Admin key for the X-JuiceMount-Admin-Key header. Empty disables auth.")
	flag.Parse()

	if *metaURL == "" {
		log.Fatal("--meta is required (e.g. redis://redis:6379/1)")
	}

	roots := splitNonEmpty(*sourceRoots, ",")
	if len(roots) == 0 {
		log.Fatal("--source-roots must contain at least one path")
	}

	mgr := NewJobManager(*juicefsBin, *metaURL)
	api := &API{
		jobs:        mgr,
		sourceRoots: roots,
		destMount:   *destMount,
		adminKey:    *adminKey,
		juicefsBin:  *juicefsBin,
	}

	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("juicemount-migrator %s starting on %s", version, *addr)
	log.Printf("  meta:          %s", *metaURL)
	log.Printf("  dest-mount:    %s", *destMount)
	log.Printf("  source-roots:  %v", roots)
	log.Printf("  auth enabled:  %v", *adminKey != "")

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

func fmtBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
