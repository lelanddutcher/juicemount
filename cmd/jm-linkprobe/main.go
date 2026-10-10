// Command jm-linkprobe joins the JuiceMount tailnet exactly like the Mac
// app's embedded LinkNode does, for headless end-to-end testing of the
// pairing pipeline: pair -> preauth key -> control register -> listed node.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"tailscale.com/tsnet"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: jm-linkprobe <control-url> <authkey> <state-dir>")
		os.Exit(2)
	}
	srv := &tsnet.Server{
		Hostname:   "jm-cli-probe",
		ControlURL: os.Args[1],
		AuthKey:    os.Args[2],
		Dir:        os.Args[3],
		Ephemeral:  true,
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	st, err := srv.Up(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "join failed:", err)
		os.Exit(1)
	}
	fmt.Println("JOINED", st.TailscaleIPs)
}
