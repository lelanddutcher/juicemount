// juicemount is a CLI for power-user operations against a running
// JuiceMount NFS server. It talks to the server via the metrics HTTP
// endpoint (no separate IPC channel needed).
//
// Usage:
//
//	juicemount pin <path>             — recursively pre-cache a directory
//	juicemount unpin <path>           — remove a pin root
//	juicemount status                 — print pin counts + offline mode + live progress
//	juicemount offline [on|off]       — toggle offline mode (no arg = print state)
//	juicemount prefetch-project <file> — pre-cache every media file referenced by an NLE project
//
// Designed for the Sovereign Video Engineer persona: ssh into your Mac
// from anywhere, pin a project for tonight's flight, log out.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/nle"
)

const (
	defaultMetricsAddr = "127.0.0.1:11050"
	defaultMountRoot   = "/Volumes/zpool"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "pin":
		cmdPin(args)
	case "unpin":
		cmdUnpin(args)
	case "status":
		cmdStatus(args)
	case "offline":
		cmdOffline(args)
	case "prefetch-project":
		cmdPrefetchProject(args)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `juicemount - power-user CLI for the JuiceMount NFS server

Commands:
  pin <path>                 Recursively pre-cache a directory for offline use
  unpin <path>               Remove a previously-pinned root
  status                     Show pin counts, offline mode, live prefetch progress
  offline [on|off]           Toggle offline mode (no arg = print current state)
  prefetch-project <file>    Pre-cache all media referenced by a Premiere/Resolve/FCPX project

Examples:
  juicemount pin "/Volumes/zpool/Film Projects/Scott Adams"
  juicemount status
  juicemount offline on
  juicemount prefetch-project "/Volumes/zpool/Film Projects/Scott Adams/proj.prproj"

The CLI talks to the running JuiceMount.app via http://127.0.0.1:11050/.
Override with --addr if you've configured a non-default metrics address.`)
}

// httpAddr returns the host:port to talk to. Honors --addr or env JUICEMOUNT_ADDR.
func httpAddr() string {
	if v := os.Getenv("JUICEMOUNT_ADDR"); v != "" {
		return v
	}
	return defaultMetricsAddr
}

// callAPI sends a request to the JuiceMount control HTTP endpoint and
// returns the decoded JSON.
func callAPI(method, path string, query url.Values) (map[string]any, error) {
	if query != nil {
		path = path + "?" + query.Encode()
	}
	req, err := http.NewRequest(method, "http://"+httpAddr()+path, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Minute} // pin can take a while on big trees
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w (is JuiceMount running?)", httpAddr(), err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w (body: %s)", err, body)
	}
	return out, nil
}

func cmdPin(args []string) {
	fs := flag.NewFlagSet("pin", flag.ExitOnError)
	addr := fs.String("addr", "", "override server address")
	fs.Parse(args)
	if *addr != "" {
		os.Setenv("JUICEMOUNT_ADDR", *addr)
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: juicemount pin <path>")
		os.Exit(2)
	}
	path := absPath(fs.Arg(0))

	q := url.Values{}
	q.Set("path", path)
	res, err := callAPI("POST", "/pin", q)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pin:", err)
		os.Exit(1)
	}
	if errStr, _ := res["error"].(string); errStr != "" {
		fmt.Fprintln(os.Stderr, "pin failed:", errStr)
		os.Exit(1)
	}
	fp, _ := res["files_pinned"].(float64)
	bt, _ := res["bytes_total"].(float64)
	fmt.Printf("✓ Pinned %d files (%s) under %s\n", int(fp), humanBytes(int64(bt)), path)
	fmt.Println("  Run 'juicemount status' to watch prefetch progress.")
}

func cmdUnpin(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: juicemount unpin <path>")
		os.Exit(2)
	}
	q := url.Values{}
	q.Set("path", absPath(args[0]))
	res, err := callAPI("POST", "/unpin", q)
	if err != nil {
		fmt.Fprintln(os.Stderr, "unpin:", err)
		os.Exit(1)
	}
	fp, _ := res["files_pinned"].(float64)
	fmt.Printf("✓ Unpinned %d files under %s\n", int(fp), absPath(args[0]))
}

func cmdStatus(args []string) {
	res, err := callAPI("GET", "/cache-status", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "status:", err)
		os.Exit(1)
	}

	// Aggregate
	if a, ok := res["aggregate"].(map[string]any); ok {
		fmt.Println("=== Aggregate ===")
		fmt.Printf("  %d total files (%s)\n",
			intOf(a["TotalFiles"]),
			humanBytes(int64Of(a["TotalBytes"])))
		fmt.Printf("  ready:    %d  (%s cached)\n",
			intOf(a["ReadyFiles"]),
			humanBytes(int64Of(a["CachedBytes"])))
		fmt.Printf("  pending:  %d\n", intOf(a["PendingFiles"]))
		fmt.Printf("  failed:   %d\n", intOf(a["FailedFiles"]))
	}

	// Roots
	if rs, ok := res["roots"].([]any); ok && len(rs) > 0 {
		fmt.Println("\n=== Pin roots ===")
		for _, r := range rs {
			rm, _ := r.(map[string]any)
			fmt.Printf("  %s\n", rm["Root"])
			fmt.Printf("    %d files | ready=%d pending=%d failed=%d | %s of %s cached\n",
				intOf(rm["TotalFiles"]),
				intOf(rm["ReadyFiles"]),
				intOf(rm["PendingFiles"]),
				intOf(rm["FailedFiles"]),
				humanBytes(int64Of(rm["CachedBytes"])),
				humanBytes(int64Of(rm["TotalBytes"])))
		}
	}

	// Live
	if lv, ok := res["live"].(map[string]any); ok {
		fmt.Println("\n=== Live ===")
		fmt.Printf("  prefetched this session: %d files (%s)\n",
			intOf(lv["FilesPrefetched"]),
			humanBytes(int64Of(lv["BytesPrefetched"])))
		if cur, _ := lv["CurrentFile"].(string); cur != "" {
			fmt.Printf("  currently working on: %s\n", cur)
		}
		fmt.Printf("  workers: %d\n", intOf(lv["Workers"]))
	}

	// Offline
	off, _ := res["offline_mode"].(bool)
	fmt.Printf("\n=== Mode ===\n  offline_mode: %v\n", off)
}

func cmdOffline(args []string) {
	if len(args) == 0 {
		// Print state
		res, err := callAPI("GET", "/cache-status", nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "status:", err)
			os.Exit(1)
		}
		off, _ := res["offline_mode"].(bool)
		fmt.Printf("offline_mode: %v\n", off)
		return
	}
	if len(args) != 1 || (args[0] != "on" && args[0] != "off") {
		fmt.Fprintln(os.Stderr, "usage: juicemount offline [on|off]")
		os.Exit(2)
	}
	q := url.Values{}
	q.Set("on", args[0])
	res, err := callAPI("POST", "/offline", q)
	if err != nil {
		fmt.Fprintln(os.Stderr, "offline:", err)
		os.Exit(1)
	}
	off, _ := res["offline_mode"].(bool)
	if off {
		fmt.Println("🔌 OFFLINE MODE ON — reads on un-cached files will fail fast.")
	} else {
		fmt.Println("🌐 OFFLINE MODE OFF — reads will fall through to backend on cache miss.")
	}
}

func cmdPrefetchProject(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: juicemount prefetch-project <project-file>")
		os.Exit(2)
	}
	projPath := absPath(args[0])
	kind := nle.DetectKind(projPath)
	if kind == nle.KindUnknown {
		fmt.Fprintf(os.Stderr, "unrecognized project format: %s\n", projPath)
		os.Exit(1)
	}
	fmt.Printf("Parsing %s project: %s\n", kindName(kind), projPath)

	refs, err := nle.Parse(projPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Found %d media references in project.\n", len(refs))

	// Filter to references that look like they're under the JuiceMount mount
	mountRoot := defaultMountRoot
	if v := os.Getenv("JUICEMOUNT_MOUNT"); v != "" {
		mountRoot = v
	}
	var pinnable []nle.MediaRef
	for _, r := range refs {
		if strings.HasPrefix(r.Path, mountRoot) {
			pinnable = append(pinnable, r)
		}
	}
	fmt.Printf("Of those, %d are under %s and will be pinned.\n", len(pinnable), mountRoot)

	if len(pinnable) == 0 {
		fmt.Println("Nothing to pin.")
		return
	}

	// Walk via the existing Pin handler — that path takes a directory root,
	// but we want individual files. The simplest mapping: pin each file's
	// PARENT directory. Or we could add a /pin-files endpoint that takes a
	// list. For prototype: pin the project file's parent so the related
	// media folder is captured. This matches the typical case where a
	// .prproj sits next to its media.
	//
	// Better long-term: pass the explicit list to a /pin-paths endpoint.
	// For now, walk parents and dedupe.
	parents := map[string]struct{}{}
	for _, r := range pinnable {
		dir := r.Path
		// strip filename
		if idx := strings.LastIndex(dir, "/"); idx > 0 {
			dir = dir[:idx]
		}
		parents[dir] = struct{}{}
	}

	for d := range parents {
		q := url.Values{}
		q.Set("path", d)
		res, err := callAPI("POST", "/pin", q)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pin %s: %v\n", d, err)
			continue
		}
		fp, _ := res["files_pinned"].(float64)
		fmt.Printf("  ✓ pinned %s (%d files)\n", d, int(fp))
	}
	fmt.Println("\nRun 'juicemount status' to watch prefetch progress.")
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

func absPath(p string) string {
	if strings.HasPrefix(p, "/") {
		return p
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd + "/" + p
	}
	return p
}

func intOf(v any) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func int64Of(v any) int64 {
	if f, ok := v.(float64); ok {
		return int64(f)
	}
	return 0
}

func humanBytes(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func kindName(k nle.ProjectKind) string {
	switch k {
	case nle.KindPremiere:
		return "Premiere"
	case nle.KindResolve:
		return "DaVinci Resolve"
	case nle.KindFCPX:
		return "Final Cut Pro X"
	default:
		return "unknown"
	}
}
