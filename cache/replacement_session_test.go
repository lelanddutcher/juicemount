package cache

import (
	"strings"
	"testing"
)

func TestReplacementSettlementSessionCheckNeverWaitsForCallbackLock(t *testing.T) {
	b := VolumeBinding{Version: 1, Backend: strings.Repeat("a", 64), UUID: "settlement", BlockSize: DefaultBlockSize}
	r := &Reader{}
	s := &Session{binding: b, reader: r, online: true, qualified: true, epoch: 7}
	if !s.TryMatchRemoteObservation(7, b, r) {
		t.Fatal("qualified observation refused")
	}
	s.mu.Lock()
	// Synchronous same-goroutine call deadlocks if this ever becomes Lock.
	if s.TryMatchRemoteObservation(7, b, r) {
		t.Fatal("held callback lock admitted")
	}
	s.mu.Unlock()
	if s.TryMatchRemoteObservation(6, b, r) || s.TryMatchRemoteObservation(7, b, &Reader{}) {
		t.Fatal("changed observation admitted")
	}
	for _, tc := range []struct {
		name   string
		change func()
	}{
		{"offline", func() { s.online = false }},
		{"unqualified", func() { s.qualified = false }},
		{"refused", func() { s.identityRefused = true }},
		{"transport", func() { s.transportResets = 1 }},
		{"stopped", func() { s.closed = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.online, s.qualified, s.identityRefused, s.closed, s.transportResets = true, true, false, false, 0
			tc.change()
			if s.TryMatchRemoteObservation(7, b, r) {
				t.Fatal("revoked observation admitted")
			}
		})
	}
}
