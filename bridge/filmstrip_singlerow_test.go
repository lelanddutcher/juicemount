package main

import (
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// A SINGLE-ROW strip is a legal grid, not a special case.
//
// WHY THIS TEST EXISTS: the ClipLogger agent read the grid contract as requiring
// a true 2-D pack and scoped "a day of work" to repack their 1xN strips before
// they could contribute the kind at all. That is a real cost paid for a
// misreading, and a contract that reads as stricter than it is costs adoption
// exactly like a contract that IS stricter.
//
// cols=frameCount, rows=1 satisfies every clause: the canvas is
// frameCount*cell_w x 1*cell_h, and frame i sits at (i/cols, i%cols) == (0, i),
// which is precisely a left-to-right single row. Pinned so nobody later adds a
// rows>1 or "must be squarish" rule and silently breaks a shipped producer.
func TestFilmstripSingleRowIsALegalGrid(t *testing.T) {
	for _, n := range []int{1, 2, 8, 120, maxStripGridDim} {
		geo := &derivatives.FilmstripGeo{
			Cols: n, Rows: 1,
			CellW: 160, CellH: 90,
			FrameCount: n,
			IntervalMS: 1000,
			DurationMS: n * 1000,
		}
		if err := validateFilmstripGeometry(geo); err != nil {
			t.Errorf("single-row strip of %d frames REJECTED: %v — 1xN is a legal grid "+
				"and a producer should not have to repack to contribute", n, err)
		}
	}
}

// The mirror case: a single COLUMN (cols=1, rows=N) is equally legal. Same
// reasoning, opposite axis — i%1 == 0 for every i, so every frame is in column
// 0 and the row index is i.
func TestFilmstripSingleColumnIsALegalGrid(t *testing.T) {
	geo := &derivatives.FilmstripGeo{
		Cols: 1, Rows: 24, CellW: 160, CellH: 90,
		FrameCount: 24, IntervalMS: 500, DurationMS: 12000,
	}
	if err := validateFilmstripGeometry(geo); err != nil {
		t.Errorf("single-column strip REJECTED: %v", err)
	}
}

// Guard the boundary that DOES matter, so the two tests above cannot be
// mistaken for "geometry is unchecked": a 1xN strip that declares more frames
// than it has cells is still a lie about the canvas and must be refused.
func TestFilmstripSingleRowStillRejectsOverpack(t *testing.T) {
	geo := &derivatives.FilmstripGeo{
		Cols: 8, Rows: 1, CellW: 160, CellH: 90,
		FrameCount: 9, IntervalMS: 1000,
	}
	err := validateFilmstripGeometry(geo)
	if err == nil {
		t.Fatal("9 frames in an 8x1 grid was accepted — the canvas cannot hold them")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected rejection reason: %v", err)
	}
}
