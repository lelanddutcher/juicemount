package nfs

import "testing"

// THE INVARIANT THAT MATTERS MOST.
//
// A punched range reads as zeros with no error. If routing ever sends a read
// below punchedEnd at the spool file, the client gets a file of the right length
// full of zeros in that region — silent corruption, no diagnostic. This sweep
// asserts that NO combination of the other inputs can produce planSpool below
// punchedEnd. The table tests below cover the cases I thought of; this covers
// the ones I did not.
func TestNoOffsetBelowPunchedEndIsEverServedFromSpool(t *testing.T) {
	offsets := []int64{0, 1, 4095, 4096, 100 << 20, (1 << 30) - 1}
	punched := []int64{0, 4096, 1 << 20, 512 << 20, 1 << 30}
	cends := []int64{0, 4096, 1 << 20, 512 << 20, 1 << 30, 2 << 30}
	wends := []int64{0, 1 << 20, 1 << 30, 4 << 30}

	for _, off := range offsets {
		for _, pe := range punched {
			for _, ce := range cends {
				for _, we := range wends {
					for _, active := range []bool{true, false} {
						for _, ad := range []bool{true, false} {
							in := readPlanInputs{
								punchedEnd:    pe,
								contiguousEnd: ce,
								writtenEnd:    we,
								writerActive:  active,
								isAppleDouble: ad,
							}
							got := planReadAt(off, in)
							if off < pe && got != planDest {
								t.Fatalf("off=%d punchedEnd=%d cend=%d wend=%d active=%v ad=%v "+
									"-> %s; a punched offset MUST route to dest, anything else "+
									"serves zeros as file content", off, pe, ce, we, active, ad, got)
							}
						}
					}
				}
			}
		}
	}
}

func TestPlanReadAtCases(t *testing.T) {
	const mb = 1 << 20
	cases := []struct {
		name string
		off  int64
		in   readPlanInputs
		want readPlan
	}{{
		name: "nothing punched, inside the contiguous prefix -> spool",
		off:  5 * mb,
		in:   readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 10 * mb},
		want: planSpool,
	}, {
		name: "below punchedEnd -> dest, even though it is also below cend",
		off:  2 * mb,
		in:   readPlanInputs{punchedEnd: 4 * mb, contiguousEnd: 10 * mb, writtenEnd: 10 * mb},
		want: planDest,
	}, {
		name: "exactly AT punchedEnd is still in the spool (half-open range)",
		off:  4 * mb,
		in:   readPlanInputs{punchedEnd: 4 * mb, contiguousEnd: 10 * mb, writtenEnd: 10 * mb},
		want: planSpool,
	}, {
		name: "in-flight hole above cend with an active writer -> hold",
		off:  12 * mb,
		in:   readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 20 * mb, writerActive: true},
		want: planHold,
	}, {
		name: "same hole but the writer went silent -> EOF, take the partial",
		off:  12 * mb,
		in:   readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 20 * mb, writerActive: false},
		want: planEOF,
	}, {
		name: "past the high-water mark -> EOF",
		off:  30 * mb,
		in:   readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 20 * mb, writerActive: true},
		want: planEOF,
	}, {
		name: "._ sidecar reads up to writtenEnd and never holds (#100, the 73s stall)",
		off:  12 * mb,
		in: readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 20 * mb,
			writerActive: true, isAppleDouble: true},
		want: planSpool,
	}, {
		name: "._ sidecar past writtenEnd -> EOF, never a hold",
		off:  30 * mb,
		in: readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 20 * mb,
			writerActive: true, isAppleDouble: true},
		want: planEOF,
	}, {
		name: "._ sidecar does NOT bypass the punched check",
		off:  1 * mb,
		in: readPlanInputs{punchedEnd: 4 * mb, contiguousEnd: 10 * mb, writtenEnd: 20 * mb,
			writerActive: true, isAppleDouble: true},
		want: planDest,
	}, {
		name: "negative offset -> EOF rather than a spool read",
		off:  -1,
		in:   readPlanInputs{contiguousEnd: 10 * mb, writtenEnd: 10 * mb},
		want: planEOF,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := planReadAt(tc.off, tc.in); got != tc.want {
				t.Errorf("planReadAt(%d) = %s, want %s", tc.off, got, tc.want)
			}
		})
	}
}

// Publishing must precede punching, so a punch may never overtake the published
// boundary. A sealed end that has gone BACKWARDS (Truncate can do this) must not
// "un-punch" — those bytes are already gone from the spool and the routing that
// sends readers to the backend for them cannot be withdrawn.
func TestPunchSafeCeilingNeverRetreatsBelowPublished(t *testing.T) {
	const mb = 1 << 20
	cases := []struct {
		sealed, published, want int64
	}{
		{sealed: 10 * mb, published: 4 * mb, want: 10 * mb}, // normal advance
		{sealed: 4 * mb, published: 4 * mb, want: 4 * mb},   // no movement
		{sealed: 1 * mb, published: 4 * mb, want: 4 * mb},   // retreat -> clamped
		{sealed: 0, published: 8 * mb, want: 8 * mb},        // full retreat -> clamped
	}
	for _, tc := range cases {
		if got := punchSafeCeiling(tc.sealed, tc.published); got != tc.want {
			t.Errorf("punchSafeCeiling(sealed=%d, published=%d) = %d, want %d — a punch "+
				"that retreats below the published boundary would imply un-punching, "+
				"which is not possible", tc.sealed, tc.published, got, tc.want)
		}
	}
}
