package respproxy

import (
	"sync"
	"testing"
	"time"
)

// Snapshot (the /stats endpoint the HEALTHCHECK polls) and closing
// connections run together without deadlocking (lock order s.mu, cs.mu).
func TestStatsSnapshotWhileConnectionsClose(t *testing.T) {
	s := NewStats()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; i < 2000; i++ {
					cs := s.newConnStats()
					cs.command(Command{Name: "GET", Class: ClassRead}, "t")
					s.closeConnStats(cs)
				}
			}()
			go func() {
				defer wg.Done()
				for i := 0; i < 2000; i++ {
					_ = s.Snapshot()
					_ = s.Commands()
				}
			}()
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock: Snapshot and closeConnStats did not finish")
	}
}
