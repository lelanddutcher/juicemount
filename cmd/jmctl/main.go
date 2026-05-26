// jmctl is a thin CLI wrapper around the JuiceMount HTTP control plane
// at 127.0.0.1:11050. Built to be used from perf-test scripts that need
// to drive JM operations (sync, health, cache-clear, pin/unpin) and
// scrape metrics deterministically — without going through the Swift UI
// or hand-rolling curl invocations.
//
// Design notes:
//   - All commands hit endpoints that already exist on the bridge HTTP
//     server (see bridge/cbridge.go ExtraRoutes); jmctl just exposes
//     them with sane defaults, JSON pretty-printing, and exit codes
//     that scripts can rely on.
//   - Output goes to stdout in machine-readable form (raw JSON or
//     line-oriented for `get-metric`). Diagnostics on stderr.
//   - Exit codes: 0 = success; 1 = command-level failure (e.g.,
//     non-2xx HTTP); 2 = usage error.
//   - Intentionally NOT a daemon, not stateful. Single-shot per
//     invocation. Suitable for shell loops at any cadence.
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
)

const defaultAddr = "127.0.0.1:11050"

func main() {
	addr := flag.String("addr", defaultAddr, "JuiceMount metrics/control HTTP addr")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call HTTP timeout")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `jmctl — JuiceMount control-plane CLI

Usage: jmctl [-addr ADDR] [-timeout DUR] COMMAND [ARGS...]

Commands:
  health                       GET  /health             — component status JSON
  metrics                      GET  /metrics            — full RPC metrics JSON
  cache-status                 GET  /cache-status       — pin coverage + live JSON
  offline                      GET  /offline            — current offline state
  offline-on                   POST /offline?on=true    — engage user-offline
  offline-off                  POST /offline?on=false   — clear user-offline
  sync                         POST /sync               — trigger immediate metadata sync
  reclaim                      POST /reclaim            — thin Time Machine local snapshots
  cache-clear [-keep-pinned]   POST /cache-clear        — drop JuiceFS chunk cache
  self-test [-force]           GET/POST /self-test      — read-throughput probe
  verify-pins                  POST /verify-pins        — re-enqueue every pinned file
  force-eject                  POST /force-eject        — privileged umount (admin prompt)
  stop                         POST /stop               — soft-stop the NFS server

  get-metric RPC FIELD         scrape /metrics; print one number from rpcs.<RPC>.<FIELD>
                               e.g.  jmctl get-metric READ p95_us
                                     jmctl get-metric WRITE mean_us
                                     jmctl get-metric LOOKUP count

  metric-counters              one-line summary: uptime_sec rpc_total rpc_errors bytes_read bytes_written

Exit: 0 success; 1 command failure; 2 usage error.
`)
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	c := &client{addr: *addr, timeout: *timeout}
	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	switch cmd {
	case "health":
		os.Exit(c.printGet("/health"))
	case "metrics":
		os.Exit(c.printGet("/metrics"))
	case "cache-status":
		os.Exit(c.printGet("/cache-status"))
	case "offline":
		os.Exit(c.printGet("/offline"))
	case "offline-on":
		os.Exit(c.printPost("/offline?on=true", ""))
	case "offline-off":
		os.Exit(c.printPost("/offline?on=false", ""))
	case "sync":
		os.Exit(c.printPost("/sync", ""))
	case "reclaim":
		os.Exit(c.printPost("/reclaim", ""))
	case "cache-clear":
		fs := flag.NewFlagSet("cache-clear", flag.ExitOnError)
		keepPinned := fs.Bool("keep-pinned", false, "re-enqueue pinned files after clear")
		_ = fs.Parse(args)
		q := ""
		if *keepPinned {
			q = "?keep-pinned=true"
		}
		os.Exit(c.printPost("/cache-clear"+q, ""))
	case "self-test":
		fs := flag.NewFlagSet("self-test", flag.ExitOnError)
		force := fs.Bool("force", false, "rerun the probe instead of returning cached")
		_ = fs.Parse(args)
		if *force {
			os.Exit(c.printPost("/self-test", ""))
		}
		os.Exit(c.printGet("/self-test"))
	case "verify-pins":
		os.Exit(c.printPost("/verify-pins", ""))
	case "force-eject":
		os.Exit(c.printPost("/force-eject", ""))
	case "stop":
		os.Exit(c.printPost("/stop", ""))
	case "get-metric":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: jmctl get-metric RPC FIELD")
			os.Exit(2)
		}
		os.Exit(c.getMetric(args[0], args[1]))
	case "metric-counters":
		os.Exit(c.metricCounters())
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n", cmd)
		flag.Usage()
		os.Exit(2)
	}
}

type client struct {
	addr    string
	timeout time.Duration
}

func (c *client) url(path string) string {
	u := url.URL{Scheme: "http", Host: c.addr, Path: ""}
	if i := strings.Index(path, "?"); i >= 0 {
		u.Path = path[:i]
		u.RawQuery = path[i+1:]
	} else {
		u.Path = path
	}
	return u.String()
}

func (c *client) do(method, path, body string) ([]byte, int, error) {
	hc := &http.Client{Timeout: c.timeout}
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, c.url(path), bodyReader)
	if err != nil {
		return nil, 0, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, nil
}

func (c *client) printGet(path string) int {
	return c.printDo("GET", path)
}
func (c *client) printPost(path, body string) int {
	return c.printDo("POST", path)
}
func (c *client) printDo(method, path string) int {
	data, status, err := c.do(method, path, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "jmctl: %s %s: %v\n", method, path, err)
		return 1
	}
	if status >= 400 {
		fmt.Fprintf(os.Stderr, "jmctl: %s %s: HTTP %d\n%s\n", method, path, status, string(data))
		return 1
	}
	// Try to pretty-print JSON; fall back to raw if it's not JSON.
	var parsed any
	if err := json.Unmarshal(data, &parsed); err == nil {
		pretty, _ := json.MarshalIndent(parsed, "", "  ")
		fmt.Println(string(pretty))
	} else {
		os.Stdout.Write(data)
		if len(data) > 0 && data[len(data)-1] != '\n' {
			fmt.Println()
		}
	}
	return 0
}

func (c *client) getMetric(rpc, field string) int {
	data, status, err := c.do("GET", "/metrics", "")
	if err != nil || status >= 400 {
		fmt.Fprintf(os.Stderr, "jmctl: GET /metrics failed: status=%d err=%v\n", status, err)
		return 1
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		fmt.Fprintf(os.Stderr, "jmctl: parse metrics: %v\n", err)
		return 1
	}
	rpcs, ok := m["rpcs"].(map[string]any)
	if !ok {
		fmt.Fprintln(os.Stderr, "jmctl: no rpcs section in metrics")
		return 1
	}
	r, ok := rpcs[rpc].(map[string]any)
	if !ok {
		fmt.Fprintf(os.Stderr, "jmctl: RPC %q not present (no calls yet)\n", rpc)
		fmt.Println("0")
		return 0
	}
	v, ok := r[field]
	if !ok {
		fmt.Fprintf(os.Stderr, "jmctl: field %q not present on RPC %q\n", field, rpc)
		return 1
	}
	switch tv := v.(type) {
	case float64:
		// Print as integer when whole.
		if tv == float64(int64(tv)) {
			fmt.Println(int64(tv))
		} else {
			fmt.Println(tv)
		}
	default:
		fmt.Println(v)
	}
	return 0
}

func (c *client) metricCounters() int {
	data, status, err := c.do("GET", "/metrics", "")
	if err != nil || status >= 400 {
		fmt.Fprintf(os.Stderr, "jmctl: GET /metrics failed: status=%d err=%v\n", status, err)
		return 1
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		fmt.Fprintf(os.Stderr, "jmctl: parse metrics: %v\n", err)
		return 1
	}
	get := func(k string) int64 {
		if v, ok := m[k].(float64); ok {
			return int64(v)
		}
		return 0
	}
	fmt.Printf("uptime_sec=%d rpc_total=%d rpc_errors=%d bytes_read=%d bytes_written=%d\n",
		get("uptime_sec"), get("rpc_total"), get("rpc_errors"),
		get("bytes_read"), get("bytes_written"))
	return 0
}
