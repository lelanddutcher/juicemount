package manager

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

const (
	cloneNewInstanceID = "9A8B7C6D-5E4F-4A3B-8C2D-1E0F9A8B7C6D"
	digestMacA         = "0123456789abcdef0123456789abcdef"
	digestMacB         = "fedcba9876543210fedcba9876543210"
)

func snapshotPrincipal(t *testing.T, mux http.Handler, name string) MacPrincipal {
	t.Helper()
	rr := doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rr.Code, rr.Body)
	}
	var snap PrincipalsSnapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatal(err)
	}
	p := snap.byName(name)
	if p == nil {
		t.Fatalf("principal %s not listed", name)
	}
	return *p
}

// TestH8BEnrollRecordsCloneOrigin: a Mac whose identity was rotated after a
// clone sends its previous instance ID and its device digest; the admin view
// shows both, so the admin can tell the copy from the original.
func TestH8BEnrollRecordsCloneOrigin(t *testing.T) {
	mux, _, _, _ := newH8BAPI(t)
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{
		"instance_id": cloneNewInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret,
		"previous_instance_id": testInstanceID, "device_digest": digestMacB,
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("clone enroll: %d %s", rr.Code, rr.Body)
	}
	er := decodeEnroll(t, rr.Body.Bytes())
	p := snapshotPrincipal(t, mux, er.Principal)
	if p.PreviousInstanceID != testInstanceID || p.DeviceDigest != digestMacB {
		t.Fatalf("clone origin not recorded: previous=%q digest=%q", p.PreviousInstanceID, p.DeviceDigest)
	}
	if p.DeviceMismatch {
		t.Fatal("a first request is flagged as a device mismatch")
	}

	// Malformed optional fields are refused, never stored.
	for _, bad := range []map[string]string{
		{"instance_id": testInstanceID, "host": "x", "secret": testMacSecret, "previous_instance_id": "not-a-uuid"},
		{"instance_id": testInstanceID, "host": "x", "secret": testMacSecret, "previous_instance_id": testInstanceID},
		{"instance_id": testInstanceID, "host": "x", "secret": testMacSecret, "device_digest": "ZZ"},
	} {
		if rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", bad); rr.Code != http.StatusBadRequest {
			t.Fatalf("bad optional field %v: %d %s, want 400", bad, rr.Code, rr.Body)
		}
	}
}

// TestH8BEnrollFlagsSecondDevice: one principal used from two Macs (a copy
// of the identity made before the device binding existed) is flagged for the
// admin, both on a token poll and on a re-enroll with the secret.
func TestH8BEnrollFlagsSecondDevice(t *testing.T) {
	for _, viaSecret := range []bool{false, true} {
		mux, _, _, _ := newH8BAPI(t)
		rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{
			"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret, "device_digest": digestMacA,
		})
		if rr.Code != http.StatusAccepted {
			t.Fatalf("enroll: %d %s", rr.Code, rr.Body)
		}
		er := decodeEnroll(t, rr.Body.Bytes())

		// Same Mac polls: nothing flagged.
		poll := map[string]string{"instance_id": testInstanceID, "poll_token": er.PollToken, "device_digest": digestMacA}
		if rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", poll); rr.Code != http.StatusAccepted {
			t.Fatalf("poll: %d %s", rr.Code, rr.Body)
		}
		if snapshotPrincipal(t, mux, er.Principal).DeviceMismatch {
			t.Fatal("the enrolling Mac's own poll was flagged")
		}

		second := map[string]string{"instance_id": testInstanceID, "poll_token": er.PollToken, "device_digest": digestMacB}
		if viaSecret {
			second = map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret, "device_digest": digestMacB}
		}
		rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", second)
		if rr.Code != http.StatusAccepted {
			t.Fatalf("second-device request (secret=%v): %d %s", viaSecret, rr.Code, rr.Body)
		}
		p := snapshotPrincipal(t, mux, er.Principal)
		if !p.DeviceMismatch || p.MismatchedDigest != digestMacB {
			t.Fatalf("secret=%v: principal used from a second Mac is not flagged: %+v", viaSecret, p)
		}
		if strings.Contains(rr.Body.String(), "device") {
			t.Fatalf("the requester learns about the flag: %s", rr.Body)
		}
	}
}
