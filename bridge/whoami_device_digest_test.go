package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

// /whoami carries this Mac's device digest so the Manager can flag one
// principal arriving from two Macs; it is omitted when unavailable.
func TestWhoamiCarriesDeviceDigest(t *testing.T) {
	const mac = "AAAAAAAA-0000-4000-8000-00000000000A"
	get := func() map[string]any {
		rr := httptest.NewRecorder()
		handleWhoamiHTTP(rr, httptest.NewRequest("GET", "/whoami", nil))
		var m map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	restore := deviceid.SetSourceForTest(func() (string, error) { return mac, nil })
	if got := get()["device_digest"]; got != deviceid.Digest(mac) {
		t.Fatalf("device_digest = %v, want %s", got, deviceid.Digest(mac))
	}
	restore()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "", deviceid.ErrUnavailable }))
	if _, present := get()["device_digest"]; present {
		t.Fatal("device_digest present with no fingerprint")
	}
}
