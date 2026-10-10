//go:build h8_acceptance

package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// H-8 RED (pre-existing, documented by the drainer re-review probe P3): a file
// another Mac created at the name BEFORE this Mac's new-file drain opened is
// rewritten in place. The drainer's conflict copy only covers the window
// between its open and its rename. Closing this needs cross-writer leases or a
// base identity recorded per spool row.
func TestH8AcceptanceOtherWriterFileAtDrainStartNotOverwritten(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/y.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	macB := []byte("MAC B")
	if err := os.WriteFile(dest, macB, 0o644); err != nil {
		t.Fatal(err)
	}
	// Mac A opened its create OFFLINE: the name was new to Mac A. (An ONLINE
	// open takes its base from the backend, so a file already there is the
	// file the save replaces; see TestH8BMirrorMissOnlineSaveLandsInPlace.)
	withOffline(t, func() { writeSpoolEntry(t, spool, name, []byte("mac A offline create")) })
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, macB) {
		t.Errorf("H8: pre-existing other-writer file overwritten in place (no conflict copy): %q", got)
	}
}
