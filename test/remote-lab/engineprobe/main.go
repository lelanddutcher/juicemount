// Command engineprobe measures the JuiceFS engine called directly (no FUSE
// mount) against the same volume read through a normal FUSE mount, in the
// same session. It is the Step 0 probe of the macFUSE report: a lab tool, not
// product code. It runs on Linux (the remote lab's client container) and on
// macOS (the laptop lab).
//
//	engineprobe relay   -listen 127.0.0.1:16379 -to HOST:6379 -http 127.0.0.1:16380
//	engineprobe fixture -mnt /jfs-probe -dir engineprobe-fx -allow-write
//	engineprobe run     -arm direct -meta redis://USER@127.0.0.1:16379/1 -cache-dir DIR ...
//	engineprobe run     -arm fuse   -mnt /jfs-probe ...
//
// The relay forwards one TCP port to Redis (or to the RESP proxy) and counts
// round trips: a burst of client bytes answered by a burst of server bytes.
// Both arms connect to Redis through it, so both count the same way. A
// round trip's start and end times let the probe count how many happened one
// after another (the serial chain) inside a timed step.
//
// The direct arm builds the engine the way `juicefs mount` does (meta client,
// object storage, cached chunk store, vfs) with the app's mount flags, but
// serves no kernel mount. The password comes from META_PASSWORD, never argv.
//
// Every result is one JSON line on stdout. The tool reads by default. The
// writes (fixture, T4's peer edit, T5's creates) need -allow-write and go only
// into a folder whose name starts with "engineprobe-" and that holds the
// marker file the fixture command made.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const mib = 1 << 20

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "relay":
		err = cmdRelay(os.Args[2:])
	case "fixture":
		err = cmdFixture(os.Args[2:])
	case "run":
		err = cmdRun(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "engineprobe:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: engineprobe relay|fixture|run [flags] (see -h of each)")
	os.Exit(2)
}

// emit prints one result line. Every line carries the test, the arm and the
// label so lines from several runs can be concatenated.
func emit(v map[string]any) {
	v["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
