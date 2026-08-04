package farm

import "testing"

// D5/D2 density ask (CONSUMER_STATUS 2026-07-21) + the short-clip frame floor
// (BACKLOG). These pin the DEFAULTS, which is the whole substance of the ask —
// the geometry shape was already correct.

func TestShortClipFrameFloor(t *testing.T) {
	cases := []struct {
		name   string
		durSec float64
		fps    float64
		want   int
	}{
		// The banding case the consumer reported: a short clip previously got a
		// flat 12 frames and repeated each one 3-6x across a wide strip.
		{"3s at 30fps has 90 frames available, so floor lifts to 32", 3, 30, 32},
		{"10s at 25fps", 10, 25, 32},
		// Never ask for more distinct frames than the clip contains.
		{"1s at 24fps can only offer ~24", 1, 24, 24},
		{"0.5s at 24fps -> 12 available, keeps the historical minimum", 0.5, 24, 12},
		// Unknown fps must not regress below the old behaviour.
		{"unknown fps keeps the flat floor", 3, 0, 12},
		// Long clips are unaffected by the floor and still cap at 144.
		{"120s clip is driven by duration, not the floor", 120, 30, 120},
		{"600s clip caps at 144", 600, 30, 144},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := frameTarget(tc.durSec, tc.fps); got != tc.want {
				t.Errorf("frameTarget(%.2fs, %.0ffps) = %d, want %d", tc.durSec, tc.fps, got, tc.want)
			}
		})
	}
}
