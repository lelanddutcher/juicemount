package nfs

import (
	"testing"
	"time"
)

// The in-place JUKEBOX-hold diagnostic. These assert the two things that were
// actually wrong or absent, not that logging "works".
//
// Context: the spool path has carried an in-flight-hole diagnostic since
// 57d320a; its in-place twin had NONE. Live on 2026-08-17 that produced
// JUKEBOX-RATE storms of up to 90 nfs.Read per 15s during Premiere exports with
// zero lines identifying the file, the offset, or the hole geometry — the
// storming path was the unlogged one.

// THE DISCRIMINATOR, and it was implemented backwards on the first attempt.
// An out-of-order-write hole fills in milliseconds as neighbouring writes land;
// a preallocation hole persists for the whole encode. They are identical in
// every other field, so getting this flag wrong makes the log actively
// misleading rather than merely thin.
func TestPreallocateHoleIsDistinguishedFromOutOfOrderWriteHole(t *testing.T) {
	t.Run("out-of-order write does NOT claim preallocation", func(t *testing.T) {
		var tr inPlaceTracker
		// base=0, write at [4096,8192) leaves a hole below it.
		tr.noteWrite("a.mov", 0, 4096, 8192)
		h := tr.m["a.mov"]
		if h == nil {
			t.Fatal("no hole recorded for an out-of-order write")
		}
		if h.fromTruncate {
			t.Error("fromTruncate=true for a hole opened by an out-of-order WRITE. " +
				"That inverts the field's whole purpose: the log would report a " +
				"transient mid-burst gap as an app preallocating and sitting on it.")
		}
	})

	t.Run("grow DOES claim preallocation", func(t *testing.T) {
		var tr inPlaceTracker
		tr.noteTruncate("b.mov", 0, 40<<20) // F_PREALLOCATE / ftruncate-to-grow
		h := tr.m["b.mov"]
		if h == nil {
			t.Fatal("no hole recorded for a grow")
		}
		if !h.fromTruncate {
			t.Error("fromTruncate=false for a hole opened by a GROW — the export " +
				"preallocation case is exactly what this field exists to name")
		}
	})

	t.Run("a grow after an out-of-order write still marks preallocation", func(t *testing.T) {
		var tr inPlaceTracker
		tr.noteWrite("c.mov", 0, 4096, 8192) // hole exists, not from truncate
		tr.noteTruncate("c.mov", 0, 40<<20)  // then the app preallocates
		h := tr.m["c.mov"]
		if h == nil {
			t.Fatal("hole vanished")
		}
		if !h.fromTruncate {
			t.Error("a grow that widened an EXISTING hole left fromTruncate=false; " +
				"once a preallocation is involved it is the fact that changes the " +
				"diagnosis, regardless of what opened the record first")
		}
	})
}

// The throttle is the other half: this fires from the read path, and a client
// retrying a held read hammers it. An unthrottled line per retry is the
// JM_LOOKUP_TRACE flood lesson.
func TestHoldLogIsThrottledPerPath(t *testing.T) {
	var tr inPlaceTracker
	tr.noteTruncate("d.mov", 0, 40<<20)

	tr.logHold("d.mov", 1024)
	first := tr.m["d.mov"].lastLog
	if first.IsZero() {
		t.Fatal("logHold did not record a log timestamp — it never emitted, so a " +
			"held read stays as invisible as it was before this existed")
	}

	tr.logHold("d.mov", 2048)
	if got := tr.m["d.mov"].lastLog; !got.Equal(first) {
		t.Error("a second logHold within the window updated lastLog — the throttle " +
			"is not holding, and a retrying client will flood the log")
	}

	// Age past the window and it must speak again: a throttle that never
	// releases is the same blindness with extra steps.
	tr.mu.Lock()
	tr.m["d.mov"].lastLog = time.Now().Add(-3 * time.Second)
	tr.mu.Unlock()
	tr.logHold("d.mov", 4096)
	if got := tr.m["d.mov"].lastLog; got.Before(first) || got.Equal(first) {
		t.Error("logHold stayed silent after the throttle window expired")
	}
}

// Cheap insurance for the hot-path gate: no tracked hole means no map lookup
// and certainly no log.
func TestHoldLogIsInertWhenNoHoleExists(t *testing.T) {
	var tr inPlaceTracker
	tr.logHold("nothing.mov", 0) // must not panic, must not create state
	if len(tr.m) != 0 {
		t.Errorf("logHold created tracker state for an untracked path: %v", tr.m)
	}
}
