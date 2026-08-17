package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The sidecar mount source, pinned.
//
// Sidecars live under .juicemount/derivatives — an INTERNAL namespace that is
// scan-filtered out of the metadata mirror the NFS server serves from. Over the
// NFS mount that directory is empty; under the FUSE mount every manifest is
// there. Measured 2026-08-17 after backfilling 26,447 sidecars: 0 derivative
// dirs visible via NFS, 37,255 via FUSE.
//
// A reconcile pointed at the NFS mount fails in the worst possible way: an
// unreadable sidecar returns (found=false, err=nil), so there is no error to
// log and no warning to find. /derivatives just answers exists:false forever
// and the consumer concludes the farm produced nothing.

// Every ReconcileOneSidecar call site must prefer the FUSE path. This is a
// source-level assertion because the failure mode is silent at runtime — there
// is no error, no log line, and no counter that would catch a regression here.
func TestAllSidecarReconcileCallSitesPreferTheFUSEPath(t *testing.T) {
	files := []string{"cbridge.go", "thumbs.go"}
	var offenders []string
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(".", f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		lines := strings.Split(string(b), "\n")
		for i, ln := range lines {
			if !strings.Contains(ln, "farm.ReconcileOneSidecar(") {
				continue
			}
			// Walk back for the mount assignment feeding this call.
			lo := i - 25
			if lo < 0 {
				lo = 0
			}
			// CODE ONLY. The first version of this scanned the raw window and
			// was fooled by its own documentation: the comment above the call
			// site explains why globalFUSEPath is required, so the string was
			// present even after the assignment was reverted to
			// globalMountPath — the test passed against the exact bug it
			// exists to catch. Strip comments and look for a real assignment.
			var code []string
			for _, w := range lines[lo:i] {
				t := strings.TrimSpace(w)
				if strings.HasPrefix(t, "//") {
					continue
				}
				code = append(code, w)
			}
			window := strings.Join(code, "\n")
			if !strings.Contains(window, "= globalFUSEPath") {
				offenders = append(offenders,
					f+":"+lines[i][:min(60, len(lines[i]))])
			}
		}
	}
	if len(offenders) > 0 {
		t.Errorf("ReconcileOneSidecar call site(s) not sourcing globalFUSEPath: %v\n"+
			"Sidecars are invisible over the NFS mount, and an invisible sidecar "+
			"returns found=false with NO error — so this regression would be "+
			"completely silent.", offenders)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
