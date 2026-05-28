package nfs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestBuildSpoolStatusDisabled covers the nil-spool branch: the menu
// bar app polls /spool unconditionally and expects a clean "enabled:
// false" response with a hint message rather than an error.
func TestBuildSpoolStatusDisabled(t *testing.T) {
	resp, err := BuildSpoolStatus(nil, nil)
	if err != nil {
		t.Fatalf("err=%v, want nil", err)
	}
	if resp.Enabled {
		t.Errorf("Enabled=true, want false")
	}
	if resp.Error == "" {
		t.Errorf("Error should be populated with a hint message")
	}
}

// TestBuildSpoolStatusHappyPath populates a spool with a handful of
// entries in different drain states and checks the counters + entry
// list reflect them correctly.
func TestBuildSpoolStatusHappyPath(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	// 3 in-progress writers.
	for _, p := range []string{"/Movies/a.mov", "/Movies/b.mov", "/Movies/c.mov"} {
		e, _ := s.OpenWrite(p)
		if _, err := e.WriteAt(make([]byte, 1000), 0); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("close %s: %v", p, err)
		}
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !resp.Enabled {
		t.Errorf("Enabled=false")
	}
	if resp.PendingFiles != 3 {
		t.Errorf("PendingFiles=%d, want 3", resp.PendingFiles)
	}
	if resp.PendingBytes != 3000 {
		t.Errorf("PendingBytes=%d, want 3000", resp.PendingBytes)
	}
	if len(resp.Entries) != 3 {
		t.Fatalf("entries=%d, want 3", len(resp.Entries))
	}

	// Newest-first ordering: the LAST-inserted (/Movies/c.mov) should
	// be at index 0.
	if resp.Entries[0].Path != "/Movies/c.mov" {
		t.Errorf("Entries[0].Path=%q, want /Movies/c.mov", resp.Entries[0].Path)
	}
	for _, e := range resp.Entries {
		if e.DrainState != "ready" {
			t.Errorf("entry %q drain_state=%q, want ready", e.Path, e.DrainState)
		}
		if e.Size != 1000 {
			t.Errorf("entry %q size=%d, want 1000", e.Path, e.Size)
		}
	}
}

// TestBuildSpoolStatusEntryCap verifies the SpoolStatusEntryCap limit.
// We insert cap+50 entries and assert only the most recent cap rows
// come back.
func TestBuildSpoolStatusEntryCap(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	total := SpoolStatusEntryCap + 50
	for i := 0; i < total; i++ {
		e, _ := s.OpenWrite("/many" + string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-" + string(rune('a'+(i/100)%26)) + ".bin")
		if _, err := e.WriteAt([]byte("x"), 0); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := e.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	resp, err := BuildSpoolStatus(s, nil)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(resp.Entries) != SpoolStatusEntryCap {
		t.Errorf("entries=%d, want %d (capped)", len(resp.Entries), SpoolStatusEntryCap)
	}
}

// TestWriteSpoolStatusJSONStatusCodes verifies the HTTP wrapper picks
// the right status code on each branch.
func TestWriteSpoolStatusJSONStatusCodes(t *testing.T) {
	t.Run("disabled returns 503", func(t *testing.T) {
		rr := httptest.NewRecorder()
		WriteSpoolStatusJSON(rr, nil, nil)
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("code=%d, want 503", rr.Code)
		}
		var body SpoolStatusResponse
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Enabled {
			t.Errorf("Enabled should be false")
		}
	})

	t.Run("happy returns 200", func(t *testing.T) {
		s := newTestSpoolStore(t, 0)
		rr := httptest.NewRecorder()
		WriteSpoolStatusJSON(rr, s, nil)
		if rr.Code != http.StatusOK {
			t.Errorf("code=%d, want 200", rr.Code)
		}
		var body SpoolStatusResponse
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !body.Enabled {
			t.Errorf("Enabled should be true")
		}
	})
}

// TestBuildSpoolStatusWithDrainerCounters fakes a drainer and verifies
// the counters propagate.
func TestBuildSpoolStatusWithDrainerCounters(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	d, err := NewDrainer(s, DrainerConfig{FuseRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("new drainer: %v", err)
	}
	d.Metrics().DrainsSucceeded.Store(42)
	d.Metrics().DrainsFailed.Store(3)
	d.Metrics().Quarantined.Store(1)
	d.Metrics().InFlight.Store(2)

	resp, _ := BuildSpoolStatus(s, d)
	if resp.Succeeded != 42 {
		t.Errorf("Succeeded=%d, want 42", resp.Succeeded)
	}
	if resp.Failed != 3 {
		t.Errorf("Failed=%d, want 3", resp.Failed)
	}
	if resp.Quarantined != 1 {
		t.Errorf("Quarantined=%d, want 1", resp.Quarantined)
	}
	if resp.InProgress != 2 {
		t.Errorf("InProgress=%d, want 2", resp.InProgress)
	}
}
