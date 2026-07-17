package main

// GET /du (INSTANT-NAV #2) handler contract: user-path → volume-relative
// translation (mount root ⇒ "."), O(1) subtree totals from the metadata
// mirror's aggregates, du-style file answers, and the 400/404/503 edges.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// withDuGlobals installs a temp metadata store + mount path into the package
// globals the handler reads, restoring the previous values on cleanup so
// sibling tests are unaffected.
func withDuGlobals(t *testing.T) *metadata.Store {
	t.Helper()
	t.Setenv("JM_SUBTREE_SIZES", "1")
	store, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	globalMu.Lock()
	prevStore, prevMount := globalStore, globalMountPath
	globalStore = store
	globalMountPath = "/Volumes/zpool"
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStore, globalMountPath = prevStore, prevMount
		globalMu.Unlock()
		store.Close()
	})
	return store
}

func duGet(t *testing.T, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handleDuHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestDuHandler(t *testing.T) {
	store := withDuGlobals(t)
	now := time.Now()

	for i, d := range []string{"SFX", "SFX/Impacts"} {
		if err := store.Insert(metadata.MakeEntry(d, true, 0, now, uint64(1+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Insert(metadata.MakeEntry("SFX/Impacts/boom.wav", false, 2048, now, 10)); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(metadata.MakeEntry("SFX/whoosh.wav", false, 1000, now, 11)); err != nil {
		t.Fatal(err)
	}

	decode := func(rec *httptest.ResponseRecorder) duResponse {
		t.Helper()
		if rec.Code != 200 {
			t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
		}
		var resp duResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return resp
	}

	// Directory: recursive totals.
	resp := decode(duGet(t, "/du?path=/Volumes/zpool/SFX"))
	if resp.Bytes != 3048 || resp.Files != 2 {
		t.Fatalf("SFX = (%d, %d), want (3048, 2)", resp.Bytes, resp.Files)
	}
	if resp.Human != "3.0 KiB" {
		t.Fatalf("human = %q, want \"3.0 KiB\"", resp.Human)
	}

	// Mount root translates to "." (whole-volume totals).
	resp = decode(duGet(t, "/du?path=/Volumes/zpool"))
	if resp.Bytes != 3048 || resp.Files != 2 {
		t.Fatalf("root = (%d, %d), want (3048, 2)", resp.Bytes, resp.Files)
	}

	// File: du-style — its own size, files=1.
	resp = decode(duGet(t, "/du?path=/Volumes/zpool/SFX/Impacts/boom.wav"))
	if resp.Bytes != 2048 || resp.Files != 1 {
		t.Fatalf("file = (%d, %d), want (2048, 1)", resp.Bytes, resp.Files)
	}

	// Unknown path → 404; missing param → 400.
	if rec := duGet(t, "/du?path=/Volumes/zpool/NOPE"); rec.Code != 404 {
		t.Fatalf("unknown path status = %d, want 404", rec.Code)
	}
	if rec := duGet(t, "/du"); rec.Code != 400 {
		t.Fatalf("missing param status = %d, want 400", rec.Code)
	}
}

func TestDuHandlerGateOff(t *testing.T) {
	// Gate off ⇒ the store maintains no aggregates ⇒ /du must answer 503, not
	// a confidently-wrong zero.
	t.Setenv("JM_SUBTREE_SIZES", "0")
	store, err := metadata.Open(":memory:")
	if err != nil {
		t.Fatalf("metadata.Open: %v", err)
	}
	globalMu.Lock()
	prevStore, prevMount := globalStore, globalMountPath
	globalStore = store
	globalMountPath = "/Volumes/zpool"
	globalMu.Unlock()
	t.Cleanup(func() {
		globalMu.Lock()
		globalStore, globalMountPath = prevStore, prevMount
		globalMu.Unlock()
		store.Close()
	})

	if rec := duGet(t, "/du?path=/Volumes/zpool"); rec.Code != 503 {
		t.Fatalf("gate-off status = %d, want 503", rec.Code)
	}
}

func TestDuHuman(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{2048, "2.0 KiB"},
		{5 << 20, "5.0 MiB"},
		{int64(15_513_600_000), "14.4 GiB"},
		{3 << 40, "3.0 TiB"},
	}
	for _, c := range cases {
		if got := duHuman(c.in); got != c.want {
			t.Errorf("duHuman(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
