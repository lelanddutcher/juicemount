package linkconfig

import "testing"

func TestHostIsLANOnly(t *testing.T) {
	tests := map[string]bool{
		"192.168.0.197":     true,
		"10.0.0.4":          true,
		"100.100.12.3":      true,
		"fd7a:115c:a1e0::1": true,
		"127.0.0.1":         true,
		"nas.local":         true,
		"truenas":           true,
		"link.example.test": true,
		"nas.example.com":   false,
		"8.8.8.8":           false,
	}
	for host, want := range tests {
		if got := HostIsLANOnly(host); got != want {
			t.Errorf("HostIsLANOnly(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestValidateServerURL(t *testing.T) {
	tests := []struct {
		name, raw               string
		allowInsecure, allowLAN bool
		wantErr                 bool
	}{
		{"public HTTPS", "https://link.example.com:30193", false, false, false},
		{"private HTTPS rejected", "https://192.168.0.197:30193", false, false, true},
		{"private HTTPS explicit LAN", "https://192.168.0.197:30193", false, true, false},
		{"private HTTP needs both opts", "http://192.168.0.197:30193", true, false, true},
		{"private HTTP explicit dev", "http://192.168.0.197:30193", true, true, false},
		{"public HTTP always rejected", "http://link.example.com:30193", true, true, true},
		{"credentials rejected", "https://user:pass@link.example.com", false, false, true},
		{"path rejected", "https://link.example.com/headscale", false, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateServerURL(tc.raw, tc.allowInsecure, tc.allowLAN)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateServerURL error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestRemoteReady(t *testing.T) {
	if !RemoteReady("https://link.example.com:30193") {
		t.Fatal("public HTTPS URL must be remote-ready")
	}
	for _, raw := range []string{
		"http://link.example.com:30193",
		"https://192.168.0.197:30193",
		"https://nas.local:30193",
	} {
		if RemoteReady(raw) {
			t.Errorf("RemoteReady(%q) = true", raw)
		}
	}
}
