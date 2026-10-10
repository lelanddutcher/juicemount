package slicegraph

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestCanonicalEquivalentZerosAndSplitExtents(t *testing.T) {
	base := Graph{Version: Version, Length: ChunkSize + 10, Chunks: []Chunk{
		{Index: 0, Extents: []Extent{{Pos: 0, SliceID: 8, Size: ChunkSize, Len: 20}, {Pos: 30, SliceID: 9, Size: 100, Len: 10}}},
		{Index: 1},
	}}
	equivalent := base.Clone()
	equivalent.Chunks[0].Extents = []Extent{
		{Pos: 0, SliceID: 8, Size: ChunkSize, Len: 7},
		{Pos: 7, SliceID: 8, Size: ChunkSize, Off: 7, Len: 13},
		{Pos: 20, Len: 10},
		{Pos: 30, SliceID: 9, Size: 100, Len: 10},
		{Pos: 40, Len: ChunkSize - 40},
	}
	a, err := base.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	b, err := equivalent.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("equivalent logical extents encoded differently")
	}
	decoded, err := Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if encoded, err := decoded.Canonical(); err != nil || !bytes.Equal(encoded, a) {
		t.Fatalf("decode roundtrip: %v", err)
	}
	different := base.Clone()
	different.Chunks[1].Extents = []Extent{{Pos: 0, SliceID: 10, Size: 10, Len: 10}}
	c, err := different.Canonical()
	if err != nil || bytes.Equal(a, c) {
		t.Fatalf("changed chunk not detected: %v", err)
	}
}

func TestGraphRejectsMalformedAndMissingCoverage(t *testing.T) {
	cases := []Graph{
		{Version: Version, Length: 1},
		{Version: 2, Length: 0},
		{Version: Version, Length: 1, Chunks: []Chunk{{Index: 1}}},
		{Version: Version, Length: 4, Chunks: []Chunk{{Extents: []Extent{{Pos: 0, SliceID: 1, Size: 2, Len: 3}}}}},
		{Version: Version, Length: 4, Chunks: []Chunk{{Extents: []Extent{{Pos: 0, SliceID: 1, Size: 4, Len: 3}, {Pos: 2, Len: 2}}}}},
		{Version: Version, Length: 4, Chunks: []Chunk{{Extents: []Extent{{Pos: ChunkSize - 1, Len: 2}}}}},
	}
	for i, g := range cases {
		if _, err := g.Canonical(); !errors.Is(err, ErrInvalidGraph) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	g := Graph{Version: Version, Length: 0}
	encoded, err := g.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(append(encoded, 0)); !errors.Is(err, ErrInvalidGraph) {
		t.Fatalf("trailing data: %v", err)
	}
	if _, err := Decode(encoded[:len(encoded)-1]); !errors.Is(err, ErrInvalidGraph) {
		t.Fatalf("short data: %v", err)
	}
}

func TestResolveOverlayAndZeroCut(t *testing.T) {
	resolved, err := Resolve([]Extent{{SliceID: 3, Size: 20, Len: 20}, {Pos: 5, Len: 10}})
	if err != nil {
		t.Fatal(err)
	}
	want := []Extent{{SliceID: 3, Size: 20, Len: 5}, {Pos: 5, Len: 10}, {Pos: 15, SliceID: 3, Size: 20, Off: 15, Len: 5}}
	if len(resolved) != len(want) {
		t.Fatalf("resolved: %+v", resolved)
	}
	for i := range want {
		if resolved[i] != want[i] {
			t.Fatalf("resolved[%d]: %+v", i, resolved[i])
		}
	}
}

func TestCanonicalIgnoresBeyondEOFWithoutDroppingHoles(t *testing.T) {
	cases := []struct {
		name     string
		baseline []Extent
		withTail []Extent
	}{
		{
			name:     "all zero before first extent",
			baseline: nil,
			withTail: []Extent{{Pos: 10, SliceID: 1, Size: 4, Len: 4}},
		},
		{
			name:     "partial data then trailing hole",
			baseline: []Extent{{Pos: 0, SliceID: 2, Size: 4, Len: 4}},
			withTail: []Extent{{Pos: 0, SliceID: 2, Size: 4, Len: 4}, {Pos: 10, SliceID: 3, Size: 4, Len: 4}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := Graph{Version: Version, Length: 8, Chunks: []Chunk{{Index: 0, Extents: tc.baseline}}}
			withTail := Graph{Version: Version, Length: 8, Chunks: []Chunk{{Index: 0, Extents: tc.withTail}}}
			want, err := baseline.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			got, err := withTail.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("extent beyond EOF changed visible hole encoding")
			}
			wantDigest, err := baseline.Digest()
			if err != nil {
				t.Fatal(err)
			}
			gotDigest, err := withTail.Digest()
			if err != nil || gotDigest != wantDigest {
				t.Fatalf("digest differs: %v", err)
			}
			decoded, err := Decode(got)
			if err != nil {
				t.Fatal(err)
			}
			roundtrip, err := decoded.Canonical()
			if err != nil || !bytes.Equal(roundtrip, want) {
				t.Fatalf("decode roundtrip: %v", err)
			}
		})
	}
}

func TestDecodeRejectsMalformedBeforeExpansion(t *testing.T) {
	zero, err := (Graph{Version: Version, Length: 8, Chunks: []Chunk{{Index: 0}}}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	badCount := bytes.Clone(zero)
	binary.BigEndian.PutUint32(badCount[16:20], 1<<20)

	shortCoverage := bytes.Clone(zero)
	binary.BigEndian.PutUint32(shortCoverage[29:33], 7)

	adjacentZeros := append([]byte(nil), zero[:28]...)
	binary.BigEndian.PutUint32(adjacentZeros[24:28], 2)
	adjacentZeros = append(adjacentZeros, 0, 0, 0, 0, 4, 0, 0, 0, 0, 4)

	positive, err := (Graph{Version: Version, Length: 4, Chunks: []Chunk{{Index: 0,
		Extents: []Extent{{SliceID: 1, Size: 4, Len: 4}},
	}}}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	zeroIDPositive := bytes.Clone(positive)
	binary.BigEndian.PutUint64(zeroIDPositive[33:41], 0)
	outOfBoundsSource := bytes.Clone(positive)
	binary.BigEndian.PutUint32(outOfBoundsSource[41:45], 3)

	splitPositive, err := (Graph{Version: Version, Length: 8, Chunks: []Chunk{{Index: 0,
		Extents: []Extent{{SliceID: 1, Size: 8, Len: 4}, {Pos: 4, SliceID: 2, Size: 8, Off: 4, Len: 4}},
	}}}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint64(splitPositive[54:62], 1) // now mergeable with first source extent

	for name, encoded := range map[string][]byte{
		"excess chunk count":       badCount,
		"short coverage":           shortCoverage,
		"adjacent zeros":           adjacentZeros,
		"zero ID in positive kind": zeroIDPositive,
		"source bounds":            outOfBoundsSource,
		"adjacent source":          splitPositive,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(encoded); !errors.Is(err, ErrInvalidGraph) {
				t.Fatalf("Decode accepted malformed graph: %v", err)
			}
		})
	}
}
