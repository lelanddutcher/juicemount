// Command jm-linkserve joins the JuiceMount tailnet and serves benchmark
// endpoints on its tailnet IP: /ping (latency) and /bench?bytes=N (throughput).
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"tailscale.com/tsnet"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: jm-linkserve <control-url> <authkey> [state-dir]")
		os.Exit(2)
	}
	srv := &tsnet.Server{
		Hostname:   "jm-nas-bench",
		ControlURL: os.Args[1],
		AuthKey:    os.Args[2],
		Dir:        "/tmp/jm-serve-state",
	}
	if len(os.Args) > 3 {
		srv.Dir = os.Args[3]
	}
	ln, err := srv.Listen("tcp", ":8090")
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong")) })
	http.HandleFunc("/bench", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
		if n <= 0 || n > 1<<30 {
			n = 1 << 20
		}
		buf := make([]byte, 65536)
		for n > 0 {
			c := int64(len(buf))
			if c > n {
				c = n
			}
			if _, err := w.Write(buf[:c]); err != nil {
				return
			}
			n -= c
		}
	})
	fmt.Fprintln(os.Stderr, "serving on tailnet :8090")
	http.Serve(ln, nil)
}
