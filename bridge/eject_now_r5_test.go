package main

// Review #5 end-to-end regression test on the real handler, the real
// nfscustody.HelperRetireGate and the real helper core.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/unmountd/client"
)

// V1-E2E (I1/I3): POST A is crossed by the user's DELETE. Before A reaches
// the helper, JuiceMount's own Stop runs an ordinary helper retire and gives
// up (CancelHelperRetire). A's late retire then starts a job; A answers 409
// "withdrawn", but its re-cancel is skipped because the Stop's request was
// "admitted after the DELETE". Once the apps let go, the volume ejects,
// although the user cancelled Quit and JuiceMount gave up its own attempt.
func TestReviewR5CrossedPostOrphanAfterInternalGiveUpE2E(t *testing.T) {
	k, core := raceCore(t)
	publishNFSController(t, core)
	if code, _ := doEjectNow(t, http.MethodPost, true); code != http.StatusAccepted {
		t.Fatalf("first POST: %d", code)
	}
	var delCode int
	var stopErr error
	n := 0
	core.beforeHelper = func(rr client.RetireRequest) {
		n++
		if n == 1 { // A, admitted, about to reach the helper
			delCode, _ = doEjectNow(t, http.MethodDelete, true)
			_, stopErr = core.RetireWithHelper(context.Background(), client.RetireRequest{Liveness: client.LivenessAlive}, 50*time.Millisecond)
			core.CancelHelperRetire() // the Stop gives up
		}
	}
	aCode, aBody := doEjectNow(t, http.MethodPost, true)
	core.beforeHelper = nil
	k.mu.Lock()
	k.busy = false
	k.mu.Unlock()
	time.Sleep(500 * time.Millisecond)
	_, present := k.state()
	t.Logf("DELETE -> %d; Stop retire -> %v; A -> %d %v; present after apps let go=%v", delCode, stopErr, aCode, aBody, present)
	if delCode != http.StatusOK || aCode != http.StatusConflict {
		t.Fatalf("scenario not reached: DELETE -> %d, A -> %d (want 200, 409)", delCode, aCode)
	}
	if !present {
		t.Fatal("STRANDED JOB: the user cancelled Quit (DELETE 200, A 409 withdrawn) and the Stop withdrew its own attempt, yet a helper job ejected the volume once the apps let go")
	}
}
