package manager

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestTrashRestoreTargetConfined is the regression test for the
// trash-restore target-path confinement fix. Before the fix,
// handleTrashRestore gated only the SOURCE entry (isInsideTrash) and
// checked the target merely for "not inside .trash" — so an
// authenticated caller could restore a caller-controlled trash entry to
// an arbitrary host path (e.g. /etc/cron.d/pwn) and get root code
// execution on the NAS. The gate now rejects any target outside the
// /jfs volume BEFORE any filesystem operation, so this test needs no
// on-disk fixture: the 403 fires before restoreTrash touches the disk.
func TestTrashRestoreTargetConfined(t *testing.T) {
	a := &API{fuseMount: t.TempDir(), destMount: "/jfs"}

	// The entry must pass the isInsideTrash source gate so we actually
	// reach the target gate — i.e. a path under /jfs/.trash/.
	const entry = "/jfs/.trash/2026-01-01-00/12345-Film|clip.mov"

	cases := []struct {
		name       string
		target     string
		wantStatus int // exact for the block; "not 403" for allowed
		wantBlock  bool
	}{
		{"escape to /etc", "/etc/cron.d/pwn", http.StatusForbidden, true},
		{"escape via traversal", "/jfs/../etc/pwn", http.StatusForbidden, true},
		{"absolute host path", "/root/.ssh/authorized_keys", http.StatusForbidden, true},
		// A legitimate in-volume target must NOT be blocked by the gate
		// (it will 500 later because the entry file doesn't exist on
		// disk in this unit test — that's fine; the point is it's not 403).
		{"in-volume target allowed through", "/jfs/restored/clip.mov", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"path":"` + entry + `","target_path":"` + tc.target + `"}`
			r := httptest.NewRequest(http.MethodPost, "/api/trash/restore", strings.NewReader(body))
			w := httptest.NewRecorder()
			a.handleTrashRestore(w, r)
			if tc.wantBlock {
				if w.Code != http.StatusForbidden {
					t.Fatalf("target %q: got status %d, want 403 (blocked)", tc.target, w.Code)
				}
			} else if w.Code == http.StatusForbidden {
				t.Fatalf("target %q: got 403, but an in-volume target must pass the confinement gate", tc.target)
			}
		})
	}
}
