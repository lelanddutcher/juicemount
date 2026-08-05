package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/metadata"
)

// A SYNTHETIC inode must fail RETRYABLY (409), not as a missing file (404).
//
// MEASURED ON THE LIVE VOLUME (2026-08-05). For roughly 10-30s after a file is
// created, the inode a client observes is a synthetic pre-drain identifier with
// the high bit set (nfs/handler.go mints these as `x | (1 << 63)`), and `stat`
// and `READDIR` can report DIFFERENT synthetic values for the same file at the
// same instant. It resolves to the real backend inode afterwards — but only if
// something forces a fresh lookup: a probe that re-stat'd the same path with no
// intervening readdir held the synthetic value for 180s without converging.
//
// WHY THIS IS A DEFECT AND NOT A CURIOSITY. Both "unknown inode" and "not
// drained yet" hit the same `entry == nil` branch, and a bare 404 reads as
// permanent. The ClipLogger consumer retries 409 (the drain window) and treats
// 404 as fatal — which is correct behaviour against a route that cannot tell
// them apart. The result is that contributing a derivative for footage
// processed shortly after it lands fails with a fatal-looking error, which is
// the single most common shape of the whole contribute-back workflow.
//
// Registering under the synthetic value is NOT an acceptable alternative: the
// row would be keyed to an identifier that ceases to exist, orphaning the
// artifact permanently.
func TestRegisterSyntheticInodeIsRetryableNotFatal(t *testing.T) {
	tmp := t.TempDir()

	mstore, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer mstore.Close()
	dstore, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dstore.Close()

	globalMu.Lock()
	oldS, oldD, oldF := globalStore, globalDerivStore, globalFUSEPath
	globalStore, globalDerivStore, globalFUSEPath = mstore, dstore, tmp
	globalMu.Unlock()
	defer func() {
		globalMu.Lock()
		globalStore, globalDerivStore, globalFUSEPath = oldS, oldD, oldF
		globalMu.Unlock()
	}()

	post := func(inode uint64) *httptest.ResponseRecorder {
		b, _ := json.Marshal(map[string]any{
			"inode": inode, "kind": "thumbnail", "producer": "on-device",
			"source_size": 4096, "source_mtime": 1700000000,
			"blob_rel_path": "poster.jpg",
		})
		rr := httptest.NewRecorder()
		handleDerivativesRegisterHTTP(rr, httptest.NewRequest("POST", "/derivatives/register", bytes.NewReader(b)))
		return rr
	}

	// The two synthetic shapes actually observed on the volume: the counter form
	// (handler.go mints `inodeCounter.Add(1) | 1<<63`, seen over READDIR as
	// 2^63+44) and the fnv-hash form (seen over stat).
	for _, ino := range []uint64{1<<63 + 44, 1<<63 | 0x5f3a9c21d4e70b16, 1 << 63} {
		rr := post(ino)
		if rr.Code == 404 {
			t.Errorf("synthetic inode %d got 404 — indistinguishable from a genuinely "+
				"unknown file, so a consumer gives up on a source that was merely a few "+
				"seconds early", ino)
			continue
		}
		if rr.Code != 409 {
			t.Errorf("synthetic inode %d: status %d, want 409 (retryable); body %s",
				ino, rr.Code, rr.Body.String())
			continue
		}
		// The remedy has to name re-listing the parent. "Wait and retry" alone is
		// wrong advice: a client holding a cached synthetic stat never refreshes
		// it, so a patient consumer waits forever.
		if body := rr.Body.String(); !strings.Contains(body, "READDIR") &&
			!strings.Contains(body, "re-list") {
			t.Errorf("synthetic inode %d: 409 body does not tell the consumer to re-list "+
				"the parent to force a fresh lookup; got %q", ino, body)
		}
	}

	// A genuinely unknown REAL inode must still be 404 — the distinction is the
	// entire point, and a fix that turned every miss into 409 would make the
	// consumer retry forever against a file that does not exist.
	if rr := post(999404); rr.Code != 404 {
		t.Errorf("unknown real inode: status %d, want 404 — collapsing this into 409 "+
			"would make a consumer retry forever on a file that is genuinely absent",
			rr.Code)
	}
}
