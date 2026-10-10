package main

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

// jm5's /whoami carries device_digest exactly as the GUI core does, and
// omits it when no fingerprint is available.
func TestJM5WhoamiCarriesDeviceDigest(t *testing.T) {
	const mac = "AAAAAAAA-0000-4000-8000-00000000000A"
	body := func() map[string]any {
		b, err := json.Marshal(jm5WhoAmI("ID", "/Volumes/zpool", "127.0.0.1:1", "/db/metadata.db", []string{"/whoami"}))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	restore := deviceid.SetSourceForTest(func() (string, error) { return mac, nil })
	if got := body()["device_digest"]; got != deviceid.Digest(mac) {
		t.Fatalf("jm5 device_digest = %v, want %s", got, deviceid.Digest(mac))
	}
	restore()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "", deviceid.ErrUnavailable }))
	if _, present := body()["device_digest"]; present {
		t.Fatal("jm5 device_digest present with no fingerprint")
	}
}
