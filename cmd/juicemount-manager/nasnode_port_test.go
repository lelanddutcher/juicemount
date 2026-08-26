package main

import "testing"

func TestNASNodeUDPPort(t *testing.T) {
	t.Setenv("JM_NET_NAS_UDP_PORT", "")
	got, err := nasNodeUDPPort()
	if err != nil || got != defaultNASUDPPort {
		t.Fatalf("default port = %d, %v", got, err)
	}
	t.Setenv("JM_NET_NAS_UDP_PORT", "42424")
	got, err = nasNodeUDPPort()
	if err != nil || got != 42424 {
		t.Fatalf("override port = %d, %v", got, err)
	}
	for _, invalid := range []string{"0", "65536", "not-a-port"} {
		t.Run(invalid, func(t *testing.T) {
			t.Setenv("JM_NET_NAS_UDP_PORT", invalid)
			if _, err := nasNodeUDPPort(); err == nil {
				t.Fatalf("invalid port %q was accepted", invalid)
			}
		})
	}
}
