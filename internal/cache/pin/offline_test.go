package pin

import (
	"sync"
	"testing"
	"time"
)

// resetOfflineState clears all package-level state. Tests must call
// this in defer or t.Cleanup because the state is global.
func resetOfflineState() {
	userOfflineFlag.Store(0)
	autoOfflineFlag.Store(0)
	autoOfflineMu.Lock()
	autoOfflineReason = ""
	autoOfflineSince = time.Time{}
	autoOfflineMu.Unlock()
}

func TestOffline_UserOnly(t *testing.T) {
	t.Cleanup(resetOfflineState)
	SetOffline(true)
	if !IsOffline() {
		t.Errorf("IsOffline() = false; want true after user SetOffline(true)")
	}
	if !IsUserOffline() {
		t.Errorf("IsUserOffline() = false; want true")
	}
	if IsAutoOffline() {
		t.Errorf("IsAutoOffline() = true; want false (only user engaged)")
	}
	SetOffline(false)
	if IsOffline() {
		t.Errorf("IsOffline() = true; want false after user cleared")
	}
}

func TestOffline_AutoOnly(t *testing.T) {
	t.Cleanup(resetOfflineState)
	SetAutoOffline(true, "no route to host")
	if !IsOffline() {
		t.Errorf("IsOffline() = false; want true after auto-engage")
	}
	if IsUserOffline() {
		t.Errorf("IsUserOffline() = true; want false (only auto engaged)")
	}
	if !IsAutoOffline() {
		t.Errorf("IsAutoOffline() = false; want true")
	}
	s := State()
	if s.Reason != "no route to host" {
		t.Errorf("State().Reason = %q; want %q", s.Reason, "no route to host")
	}
	if s.Since.IsZero() {
		t.Errorf("State().Since = zero; want set")
	}
	SetAutoOffline(false, "")
	if IsOffline() {
		t.Errorf("IsOffline() = true; want false after auto cleared")
	}
	s = State()
	if s.Reason != "" {
		t.Errorf("State().Reason = %q; want empty after clear", s.Reason)
	}
	if !s.Since.IsZero() {
		t.Errorf("State().Since = %v; want zero after clear", s.Since)
	}
}

// TestOffline_BothEngaged confirms the OR semantics: clearing one
// source while the other is still engaged keeps IsOffline true.
func TestOffline_BothEngaged(t *testing.T) {
	t.Cleanup(resetOfflineState)

	SetOffline(true)
	SetAutoOffline(true, "i/o timeout")
	if !IsOffline() {
		t.Errorf("IsOffline() = false; want true after both engaged")
	}

	// Clear user; auto still engaged → still offline.
	SetOffline(false)
	if !IsOffline() {
		t.Errorf("IsOffline() = false after clearing only user; auto still engaged")
	}
	if IsUserOffline() {
		t.Errorf("IsUserOffline() = true; want false")
	}
	if !IsAutoOffline() {
		t.Errorf("IsAutoOffline() = false; want true")
	}

	// Re-engage user, clear auto → still offline.
	SetOffline(true)
	SetAutoOffline(false, "")
	if !IsOffline() {
		t.Errorf("IsOffline() = false after clearing only auto; user still engaged")
	}

	// Clear both → online.
	SetOffline(false)
	if IsOffline() {
		t.Errorf("IsOffline() = true after both cleared")
	}
}

// TestOffline_SinceStableAcrossUpdates verifies that Since is set
// on the first auto-engage and NOT reset by a subsequent
// SetAutoOffline(true, ...) call (which can happen if the monitor
// fires multiple transitions in a row).
func TestOffline_SinceStableAcrossUpdates(t *testing.T) {
	t.Cleanup(resetOfflineState)

	SetAutoOffline(true, "reason A")
	first := State().Since
	if first.IsZero() {
		t.Fatalf("expected Since to be set after first engage")
	}
	time.Sleep(10 * time.Millisecond)

	// Second engage call without going through false. Should NOT
	// reset Since.
	SetAutoOffline(true, "reason B")
	second := State().Since
	if !first.Equal(second) {
		t.Errorf("Since reset on repeat engage: first=%v second=%v", first, second)
	}

	// But the reason updates.
	if State().Reason != "reason B" {
		t.Errorf("Reason did not update on repeat engage")
	}

	// Now clear and re-engage. Since should reset.
	SetAutoOffline(false, "")
	SetAutoOffline(true, "reason C")
	third := State().Since
	if first.Equal(third) {
		t.Errorf("Since did not reset after clear-then-engage cycle")
	}
}

// TestOffline_RaceSafety hammers the API from many goroutines. Run
// under -race; should produce no detector reports.
func TestOffline_RaceSafety(t *testing.T) {
	t.Cleanup(resetOfflineState)

	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 200

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				switch (id + j) % 5 {
				case 0:
					SetOffline(true)
				case 1:
					SetOffline(false)
				case 2:
					SetAutoOffline(true, "test reason")
				case 3:
					SetAutoOffline(false, "")
				case 4:
					_ = State()
					_ = IsOffline()
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestOffline_StateSinceSec verifies the seconds-since field tracks
// monotonically while engaged.
func TestOffline_StateSinceSec(t *testing.T) {
	t.Cleanup(resetOfflineState)

	if s := State(); s.SinceSec != 0 {
		t.Errorf("expected SinceSec=0 when not engaged, got %d", s.SinceSec)
	}
	SetAutoOffline(true, "test")
	time.Sleep(1100 * time.Millisecond)
	if s := State(); s.SinceSec < 1 {
		t.Errorf("expected SinceSec >= 1 after 1.1s, got %d", s.SinceSec)
	}
}
