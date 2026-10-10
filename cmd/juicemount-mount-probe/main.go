// juicemount-mount-probe isolates Darwin's potentially uninterruptible
// getfsstat syscall from the JuiceMount app process. It only observes mounts.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/fusecustody"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/nfscustody"
)

const maxHelperJSONBytes = 4 << 20

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--nfs-provision-leaf" {
		os.Exit(nfscustody.ProvisionRun(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "--nfs-custodian-capability" {
		os.Exit(nfscustody.CapabilityRun(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "--nfs-custodian" {
		os.Exit(nfscustody.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "--fuse-custodian-capability" {
		os.Exit(fusecustody.CapabilityRun(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "--fuse-custodian" {
		os.Exit(fusecustody.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, mounttable.CaptureSnapshot))
}

// The capture dependency lets tests prove the executable's output protocol
// without calling the host kernel observer.
func run(args []string, stdout, stderr io.Writer, capture func() (mounttable.Snapshot, error)) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "mount probe takes no arguments")
		return 2
	}
	if capture == nil {
		fmt.Fprintln(stderr, "mount probe has no observer")
		return 1
	}
	snapshot, err := capture()
	if err != nil {
		fmt.Fprintf(stderr, "mount probe: %v\n", err)
		return 1
	}
	if snapshot.Version != 1 || strings.TrimSpace(snapshot.BootID) == "" || snapshot.Entries == nil {
		fmt.Fprintln(stderr, "mount probe: incomplete snapshot header")
		return 1
	}
	for i, entry := range snapshot.Entries {
		if entry.Path == "" || entry.Source == "" || entry.Type == "" {
			fmt.Fprintf(stderr, "mount probe: incomplete entry %d\n", i)
			return 1
		}
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Fprintf(stderr, "mount probe: encode: %v\n", err)
		return 1
	}
	if len(payload)+1 > maxHelperJSONBytes {
		fmt.Fprintln(stderr, "mount probe: snapshot exceeds output limit")
		return 1
	}
	payload = append(payload, '\n')
	if n, err := stdout.Write(payload); err != nil || n != len(payload) {
		fmt.Fprintf(stderr, "mount probe: write output: %v\n", err)
		return 1
	}
	return 0
}
