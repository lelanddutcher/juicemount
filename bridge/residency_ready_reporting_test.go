package main

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Persisted byte counters cannot promise offline availability before the
// current service has admitted the corresponding local authority.
func TestResidencyReportingRequiresCurrentAdmission(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "durable-ready-without-admission"
		if failed {
			name = "failed-with-full-byte-counter"
		}
		t.Run(name, func(t *testing.T) {
			restore := seedControlPlane(t)
			defer restore()
			if failed {
				if err := globalPinStore.UpdateStatus(clip031, pin.StatusFailed, 880000000, "authority unavailable"); err != nil {
					t.Fatal(err)
				}
			}
			globalPinStore.RevokeReady(clip031)
			rr := httptest.NewRecorder()
			handleResidencyHTTP(rr, httptest.NewRequest("GET", "/residency?"+url.Values{"path": {clip031}}.Encode(), nil))
			var got residencyResponse
			if rr.Code != 200 || json.Unmarshal(rr.Body.Bytes(), &got) != nil {
				t.Fatalf("response %d: %s", rr.Code, rr.Body.String())
			}
			if !got.Exists || !got.Pinned || got.Resident || !got.Streaming || got.BytesCached != 0 {
				t.Fatalf("unadmitted pin advertised resident bytes: %+v", got)
			}
		})
	}
}

func TestResidencyReportingAdmittedGenerationAndRepin(t *testing.T) {
	restore := seedControlPlane(t)
	defer restore()
	read := func() residencyResponse {
		t.Helper()
		rr := httptest.NewRecorder()
		handleResidencyHTTP(rr, httptest.NewRequest("GET", "/residency?"+url.Values{"path": {clip031}}.Encode(), nil))
		var got residencyResponse
		if rr.Code != 200 || json.Unmarshal(rr.Body.Bytes(), &got) != nil {
			t.Fatalf("response %d: %s", rr.Code, rr.Body.String())
		}
		return got
	}
	if got := read(); !got.Exists || !got.Pinned || !got.Resident || got.Streaming || got.BytesCached != 880000000 {
		t.Fatalf("admitted fixture was not reported Ready: %+v", got)
	}
	if err := globalPinStore.Pin(clip031, 880000000, "/Volumes/zpool/Project_Foo"); err != nil {
		t.Fatal(err)
	}
	if got := read(); !got.Exists || !got.Pinned || got.Resident || !got.Streaming || got.BytesCached != 0 {
		t.Fatalf("repin inherited old generation's admission: %+v", got)
	}
}

func TestResidencyReportingRejectsReplacementDuringAdmission(t *testing.T) {
	var once sync.Once
	var replaceErr error
	restore := seedControlPlane(t, func(e pin.Entry) bool {
		once.Do(func() {
			replacement := metadata.MakeEntry("Project_Foo/clip_031.mov", false,
				880000000, time.Unix(seedMtime, 0), 1180999)
			replacement.MtimeNsecKnown = true
			replaceErr = globalStore.Insert(replacement)
		})
		return len(e.Authority) != 0
	})
	defer restore()
	initial, ok := globalStore.LookupCachedFileIdentity("Project_Foo/clip_031.mov")
	if !ok {
		t.Fatal("missing versioned fixture identity")
	}
	rr := httptest.NewRecorder()
	handleResidencyHTTP(rr, httptest.NewRequest("GET", "/residency?"+url.Values{"path": {clip031}}.Encode(), nil))
	if replaceErr != nil {
		t.Fatal(replaceErr)
	}
	var got residencyResponse
	if rr.Code != 200 || json.Unmarshal(rr.Body.Bytes(), &got) != nil {
		t.Fatalf("response %d: %s", rr.Code, rr.Body.String())
	}
	current, ok := globalStore.LookupCachedFileIdentity("Project_Foo/clip_031.mov")
	if !ok || current.Publication == initial.Publication || current.Inode == initial.Inode {
		t.Fatalf("validator did not publish same-size replacement: before=%+v after=%+v", initial, current)
	}
	if !got.Exists || !got.Pinned || got.Resident || !got.Streaming || got.BytesCached != 0 {
		t.Fatalf("old entry combined with successor admission: %+v", got)
	}
}
