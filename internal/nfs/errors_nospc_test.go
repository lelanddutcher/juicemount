package nfs

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

// TestNFSStatusErrorFromENOSPC pins the Phase-2 item-4 mapping: any error
// carrying syscall.ENOSPC — in production, nfs.ErrSpoolFull from a full
// write spool, which this package cannot import (cycle) — must map to
// NFS3ERR_NOSPC, not the generic NFS3ERR_IO. Clients then show "disk
// full" (actionable) instead of "I/O error".
func TestNFSStatusErrorFromENOSPC(t *testing.T) {
	// Shape mirrors nfs.ErrSpoolFull (fmt.Errorf %w around the errno),
	// plus an extra wrapping layer like real call sites add.
	spoolFull := fmt.Errorf("spool: capacity full: %w", syscall.ENOSPC)
	wrapped := fmt.Errorf("write /edit/a.mov: %w", spoolFull)

	for name, in := range map[string]error{
		"bare errno":     syscall.ENOSPC,
		"sentinel shape": spoolFull,
		"double wrapped": wrapped,
	} {
		got := nfsStatusErrorFrom(in)
		var statusErr *NFSStatusError
		if !errors.As(got, &statusErr) {
			t.Fatalf("%s: got %T, want *NFSStatusError", name, got)
		}
		if statusErr.NFSStatus != NFSStatusNoSPC {
			t.Errorf("%s: status=%v, want NFSStatusNoSPC", name, statusErr.NFSStatus)
		}
	}

	// Non-ENOSPC errors keep the IO fallback.
	got := nfsStatusErrorFrom(errors.New("something else"))
	var statusErr *NFSStatusError
	if !errors.As(got, &statusErr) {
		t.Fatalf("fallback: got %T, want *NFSStatusError", got)
	}
	if statusErr.NFSStatus != NFSStatusIO {
		t.Errorf("fallback status=%v, want NFSStatusIO", statusErr.NFSStatus)
	}
}
