package cache

import (
	"strings"
	"testing"
)

func TestP79SavedSnapshotBindingSurvivesOfflineAndRefusesIdentity(t *testing.T) {
	binding := VolumeBinding{Version: 1, Backend: strings.Repeat("a", 64), UUID: "P79-test-volume", BlockSize: DefaultBlockSize}
	s := &Session{binding: binding}
	if got, ok := s.SnapshotBinding(); !ok || got != binding {
		t.Fatalf("saved binding=%+v,%v", got, ok)
	}
	s.RefuseIdentity(ErrVolumeBinding)
	if _, ok := s.SnapshotBinding(); ok {
		t.Fatal("positive volume refusal still authorizes snapshot")
	}
	s.mu.Lock()
	s.identityRefused = false // a matching fresh qualification's publication
	s.closed = true
	s.mu.Unlock()
	if _, ok := s.SnapshotBinding(); ok {
		t.Fatal("stopped session still authorizes snapshot")
	}
}
