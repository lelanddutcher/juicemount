//go:build h3_acceptance

package main

import (
	"testing"
	"time"
)

// A different loopback NFS server can publish the same mount-table source.
// Source/type/path alone must never authorize a destructive unmount command.
func TestH3AcceptanceSameSourceForeignMountIsNeverUnmounted(t *testing.T) {
	const mountPoint = "/Volumes/JuiceMount"
	const foreignMountTable = "127.0.0.1:/ on /Volumes/JuiceMount (nfs, nodev, nosuid)\n"
	foreignMounted := true
	inspect := func(path string) nfsMountOwnership {
		if !foreignMounted {
			return nfsMountAbsent
		}
		return classifyConfiguredNFSMount(path, []byte(foreignMountTable), nil)
	}
	commands := 0
	run := func(time.Duration, string, ...string) error {
		commands++
		foreignMounted = false // The injected command removed the foreign mount.
		return nil
	}
	result := unmountNFSWith(mountPoint, inspect, run)
	if commands != 0 || !foreignMounted || result {
		t.Fatalf("foreign same-source mount: commands=%d mounted=%t success=%t; want refusal without a command",
			commands, foreignMounted, result)
	}
}

// An export generation distinguishes RPC namespaces, not kernel mount owners.
// No durable broker receipt is available on this path, even if source text
// exactly matches the current export. Stop must stay incomplete.
func TestH3AcceptanceGeneratedSourceWithoutReceiptIsNeverUnmounted(t *testing.T) {
	const mountPoint = "/Volumes/JuiceMount"
	const table = "127.0.0.1:/jm-0123456789abcdef0123456789abcdef on /Volumes/JuiceMount (nfs, nodev)\n"
	commands := 0
	if unmountNFSWith(mountPoint, func(path string) nfsMountOwnership {
		return classifyConfiguredNFSMount(path, []byte(table), nil)
	}, func(time.Duration, string, ...string) error { commands++; return nil }) || commands != 0 {
		t.Fatalf("unreceipted generated source dispatched %d commands or reported completion", commands)
	}
}
