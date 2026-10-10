// Command jm-linkbench joins the tailnet and measures latency + throughput
// against a jm-linkserve peer, printing p50 latency and MB/s.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"tailscale.com/tsnet"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: jm-linkbench <control-url> <authkey> <peer-ip:port> [state-dir]")
		os.Exit(2)
	}
	srv := &tsnet.Server{
		Hostname:   "jm-bench-client",
		ControlURL: os.Args[1],
		AuthKey:    os.Args[2],
		Dir:        "/tmp/jm-bench-state",
	}
	if len(os.Args) > 4 {
		srv.Dir = os.Args[4]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := srv.Up(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "join failed:", err)
		os.Exit(1)
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return srv.Dial(ctx, network, addr)
		},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}
	peer := os.Args[3]

	// Latency: 20 sequential /pings
	var lats []time.Duration
	for i := 0; i < 20; i++ {
		t0 := time.Now()
		resp, err := client.Get("http://" + peer + "/ping")
		if err != nil {
			fmt.Fprintln(os.Stderr, "ping:", err)
			os.Exit(1)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		lats = append(lats, time.Since(t0))
		time.Sleep(50 * time.Millisecond)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p50 := lats[len(lats)/2]
	fmt.Printf("LATENCY p50=%v min=%v max=%v\n", p50.Round(100*time.Microsecond), lats[0].Round(100*time.Microsecond), lats[len(lats)-1].Round(100*time.Microsecond))

	// Throughput: 50 MiB download, best of 3
	best := 0.0
	for run := 0; run < 3; run++ {
		t0 := time.Now()
		resp, err := client.Get("http://" + peer + "/bench?bytes=52428800")
		if err != nil {
			fmt.Fprintln(os.Stderr, "bench:", err)
			os.Exit(1)
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		secs := time.Since(t0).Seconds()
		mbps := float64(n) / 1048576 / secs
		if mbps > best {
			best = mbps
		}
	}
	fmt.Printf("THROUGHPUT best=%.1f MiB/s\n", best)
	_ = strconv.Itoa // keep strconv import if unused paths change
}
