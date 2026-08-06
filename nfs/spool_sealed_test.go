package nfs

import "testing"

// gib is declared in spool_freespace_test.go.
const mib = int64(1) << 20

// The sealed prefix decides which bytes may be sent to the backend and then
// PUNCHED OUT of the spool file. A punched range reads back as ZEROS with no
// error (measured on APFS), so an over-eager seal is not a performance bug — it
// is the silent-corruption class that contiguousEnd exists to prevent (black
// frames / corrupt RAW, 2026-06-15).
//
// Every case below therefore checks the same safety property in addition to its
// own expectation: the seal never crosses contiguousEnd.
func TestSealedEndNeverCrossesWrittenData(t *testing.T) {
	cases := []struct {
		name          string
		writtenEnd    int64
		contiguousEnd int64
		want          int64
	}{{
		name: "small file is never streamed — it cannot exhaust the disk alone " +
			"and the whole-file drain already verifies it at rest",
		writtenEnd: 100 * mib, contiguousEnd: 100 * mib, want: 0,
	}, {
		name:       "one byte below the threshold is not streamed",
		writtenEnd: spoolStreamMinSize - 1, contiguousEnd: spoolStreamMinSize - 1, want: 0,
	}, {
		name: "AT the threshold streaming is enabled — spoolStreamMinSize is a " +
			"minimum, so the boundary itself qualifies",
		writtenEnd: spoolStreamMinSize, contiguousEnd: spoolStreamMinSize,
		want: spoolStreamMinSize - spoolSealMargin,
	}, {
		name: "large but nothing contiguous yet (preallocated by ftruncate) — " +
			"sealing here would punch bytes that were never written",
		writtenEnd: 8 * gib, contiguousEnd: 0, want: 0,
	}, {
		name:       "large, contiguous, well past the head reserve",
		writtenEnd: 8 * gib, contiguousEnd: 8 * gib,
		want: 8*gib - spoolSealMargin,
	}, {
		name: "contiguous prefix far below the high-water mark (out-of-order " +
			"writer): the seal follows contiguousEnd, NOT writtenEnd",
		writtenEnd: 8 * gib, contiguousEnd: 2 * gib,
		want: 2*gib - spoolSealMargin,
	}, {
		name: "contiguous only just past the head reserve — nothing sealable " +
			"once the margin is taken off",
		writtenEnd: 4 * gib, contiguousEnd: spoolSealHeadReserve + spoolSealMargin,
		want: 0,
	}, {
		name: "head reserve is absolute: a seal landing inside it yields nothing",
		writtenEnd: 4 * gib, contiguousEnd: spoolSealHeadReserve,
		want: 0,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &SpoolEntry{writtenEnd: tc.writtenEnd, contiguousEnd: tc.contiguousEnd}
			got := e.sealedEndLocked()
			if got != tc.want {
				t.Errorf("sealedEnd = %d, want %d", got, tc.want)
			}
			// The invariant, checked on every case regardless of expectation.
			if got > tc.contiguousEnd {
				t.Errorf("SEAL CROSSED WRITTEN DATA: sealed %d > contiguousEnd %d — "+
					"punching there would serve zeros as file content", got, tc.contiguousEnd)
			}
			if got < 0 {
				t.Errorf("negative seal %d", got)
			}
			if got != 0 && got <= spoolSealHeadReserve {
				t.Errorf("seal %d is inside the head reserve %d — the rewrite-at-close "+
					"region must stay on the spool", got, spoolSealHeadReserve)
			}
		})
	}
}

// Property sweep: no combination of high-water and contiguous marks may produce
// a seal above the contiguous prefix. The table above encodes the cases I
// thought of; this covers the ones I did not.
func TestSealedEndInvariantHoldsAcrossRanges(t *testing.T) {
	steps := []int64{0, 1, mib, 64 * mib, 512 * mib, gib, 2 * gib, 8 * gib, 64 * gib}
	for _, w := range steps {
		for _, c := range steps {
			e := &SpoolEntry{writtenEnd: w, contiguousEnd: c}
			got := e.sealedEndLocked()
			if got > c {
				t.Fatalf("writtenEnd=%d contiguousEnd=%d -> sealed=%d exceeds contiguous", w, c, got)
			}
			if got > w {
				t.Fatalf("writtenEnd=%d contiguousEnd=%d -> sealed=%d exceeds high-water", w, c, got)
			}
			if got < 0 {
				t.Fatalf("writtenEnd=%d contiguousEnd=%d -> negative sealed=%d", w, c, got)
			}
		}
	}
}

// SealedEnd must take the lock. The predicate is read from the write path while
// WriteAt mutates the same fields; an unlocked read is a data race that -race
// would flag only under a timing window we might not hit in CI.
func TestSealedEndIsLockedAccessor(t *testing.T) {
	e := &SpoolEntry{writtenEnd: 8 * gib, contiguousEnd: 8 * gib}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			_ = e.SealedEnd()
		}
	}()
	for i := 0; i < 2000; i++ {
		e.mu.Lock()
		e.contiguousEnd += 4096
		e.writtenEnd += 4096
		e.mu.Unlock()
	}
	<-done
}
