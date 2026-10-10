package credstore

import (
	"errors"
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

func TestObjectKeyEncodingRoundTrips(t *testing.T) {
	stored, ok := EncodeObjectKey("jm-client", "s3cr/et+key=")
	if !ok || stored != "jm-client\ns3cr/et+key=" {
		t.Fatalf("EncodeObjectKey = %q, %v", stored, ok)
	}
	ak, sk, ok := DecodeObjectKey(stored)
	if !ok || ak != "jm-client" || sk != "s3cr/et+key=" {
		t.Fatalf("DecodeObjectKey = %q %q %v", ak, sk, ok)
	}
	for _, bad := range [][2]string{{"", "sk"}, {"ak", ""}, {"a\nk", "sk"}, {"ak", "s\nk"}, {"ak", "s\rk"}} {
		if _, ok := EncodeObjectKey(bad[0], bad[1]); ok {
			t.Errorf("EncodeObjectKey accepted %q", bad)
		}
	}
	for _, bad := range []string{"", "ak", "\nsk", "ak\n", "ak\nsk\nmore"} {
		if _, _, ok := DecodeObjectKey(bad); ok {
			t.Errorf("DecodeObjectKey accepted %q", bad)
		}
	}
}

// The object key is read from its own slot, for the Redis endpoint account,
// with the same device binding as the Redis password: a key copied from
// another Mac is refused.
func TestLoadObjectKeyIsSlotEndpointAndDeviceBound(t *testing.T) {
	if _, isolated := os.LookupEnv("JM_APP_ISOLATION"); isolated {
		t.Skip("isolation env set; this test covers the Keychain path")
	}
	const account = "redis://mac-0123456789ab@nas:30179"
	stamp := ""
	stored := "jm-client\nclientsecret"
	prev := keychainRead
	keychainRead = func(service, slot, acct string) (string, string, error) {
		if slot != string(ObjectKey) || acct != account {
			return "", "", ErrNotFound
		}
		return stored, stamp, nil
	}
	t.Cleanup(func() { keychainRead = prev })
	onMac(t, "BBBBBBBB-0000-4000-8000-00000000000B")

	ak, sk, err := LoadObjectKey(account)
	if err != nil || ak != "jm-client" || sk != "clientsecret" {
		t.Fatalf("LoadObjectKey = %q %q %v", ak, sk, err)
	}
	if _, _, err := LoadObjectKey("redis://mac-0123456789ab@other:30179"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("object key returned for another endpoint: %v", err)
	}
	stamp = DeviceStampPrefix + deviceid.Digest("AAAAAAAA-0000-4000-8000-00000000000A")
	if ak, _, err := LoadObjectKey(account); !errors.Is(err, ErrOtherDevice) || ak != "" {
		t.Fatalf("object key saved on another Mac returned: %q %v", ak, err)
	}
	stamp, stored = "", "garbage-without-separator"
	if _, _, err := LoadObjectKey(account); !errors.Is(err, ErrNotFound) {
		t.Fatalf("malformed object key item = %v, want ErrNotFound", err)
	}
}
