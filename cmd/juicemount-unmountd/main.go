// Command juicemount-unmountd is the privileged unmount helper: a root
// LaunchDaemon (SMAppService) whose only mutating power is the fsid-targeted
// sysctl vfs.generic.ctlbyfsid. The decision logic is in internal/unmountd.
//
// At launch it runs the sweep, arms the orphan window for every still-mounted
// receipt (its owner re-binds with "reclaim" over XPC), then serves the XPC
// transport (internal/unmountd/xpc_darwin.go) through serveTransport. In a
// build without a transport, still-mounted receipts are reported and left
// alone: arming orphan retirement with no transport would retire live mounts
// whose owners have no way to reclaim them.
//
// Two flags run without root and exit, for scripts/verify-build.sh:
//
//	-print-requirements          the peer code-signing requirements compiled in
//	-check-peer PATH [-check-peer-role service|status]
//	                             statically check signed code against one of them
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/lelanddutcher/juicemount/internal/unmountd"
)

const defaultRoot = "/Library/Application Support/JuiceMount/unmountd"

// serveTransport is set by the transport lane (a darwin-only file in this
// package). It must block for the daemon's lifetime.
var serveTransport func(*unmountd.Service, *log.Logger) error

// checkPeer statically checks the signed code at path against the
// requirement of the named listener ("service" or "status"). Set with
// serveTransport.
var checkPeer func(role, path string) error

func main() {
	os.Exit(run(os.Args[1:], os.Geteuid(), os.Stderr))
}

func run(args []string, euid int, stderr io.Writer) int {
	fs := flag.NewFlagSet("juicemount-unmountd", flag.ContinueOnError)
	fs.SetOutput(stderr)
	root := fs.String("root", defaultRoot, "root-owned receipt directory")
	printReq := fs.Bool("print-requirements", false, "print the compiled-in peer code-signing requirements and exit")
	checkPath := fs.String("check-peer", "", "statically check the signed code at this path against a listener requirement and exit")
	checkRole := fs.String("check-peer-role", "service", "listener for -check-peer: service or status")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *printReq {
		service, status := unmountd.PeerRequirements()
		fmt.Fprintf(stderr, "channel=%s\nservice=%s\nstatus=%s\n", unmountd.PeerChannel(), service, status)
		return 0
	}
	if *checkPath != "" {
		if checkPeer == nil {
			fmt.Fprintln(stderr, "check-peer: no transport compiled in")
			return 2
		}
		if err := checkPeer(*checkRole, *checkPath); err != nil {
			fmt.Fprintf(stderr, "check-peer %s %s: %v\n", *checkRole, *checkPath, err)
			return 1
		}
		fmt.Fprintf(stderr, "check-peer %s %s: satisfied\n", *checkRole, *checkPath)
		return 0
	}
	logger := log.New(stderr, "juicemount-unmountd: ", log.LstdFlags)
	if euid != 0 {
		logger.Print("refusing to run: must be root (launched by launchd as a LaunchDaemon)")
		return 1
	}
	if err := os.MkdirAll(filepath.Dir(*root), 0o755); err != nil {
		logger.Printf("receipt parent: %v", err)
		return 1
	}
	store, err := unmountd.OpenStore(*root, unmountd.DefaultReceiptLimit)
	if err != nil {
		logger.Printf("receipt store: %v", err)
		return 1
	}
	svc, err := unmountd.NewService(unmountd.Config{
		Store: store, Kernel: unmountd.DarwinKernel{}, Holders: unmountd.ProcHolders{},
		Policy: unmountd.DefaultPolicy(), Logf: logger.Printf,
	})
	if err != nil {
		logger.Printf("service: %v", err)
		return 1
	}
	defer svc.Close()
	rep, err := svc.SweepAtLaunch()
	if err != nil {
		logger.Printf("launch sweep: %v", err)
		return 1
	}
	logger.Printf("launch sweep: %s", rep)
	for _, e := range rep.Errors {
		logger.Printf("launch sweep error: %v", e)
	}
	if serveTransport == nil {
		for _, r := range rep.Pending {
			logger.Printf("pending receipt uid=%d op=%s fsid=%s left alone: no transport built", r.UID, r.OpID, r.FSID)
		}
		logger.Print("no transport compiled in; exiting after sweep")
		return 0
	}
	if n := unmountd.ArmLaunchOrphans(svc, rep, logger.Printf); n > 0 {
		logger.Printf("launch sweep: orphan window armed for %d still-mounted receipt(s)", n)
	}
	if err := serveTransport(svc, logger); err != nil {
		logger.Printf("transport: %v", err)
		return 1
	}
	return 0
}
