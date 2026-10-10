package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/slicegraph"
)

func graphReader() (*Reader, ResidentAttr) {
	attr := ResidentAttr{Size: ChunkSize + 12, MtimeNS: 123, Valid: true}
	r := &Reader{
		binding: VolumeBinding{Version: 1, Backend: strings.Repeat("a", 64), UUID: "graph-test", BlockSize: 4 << 20},
		sliceCache: map[string][]SliceInfo{
			"7_0": {{Pos: 0, SliceID: 10, Size: ChunkSize, Len: ChunkSize}},
			"7_1": {{Pos: 4, SliceID: 0, Len: 4}, {Pos: 8, SliceID: 11, Size: 4, Len: 4}},
		},
		attrWitness: map[string]ResidentAttr{"7_0": attr, "7_1": attr},
		inodeAttr:   map[uint64]ResidentAttr{7: attr},
		inodeEpoch:  make(map[uint64]uint64),
	}
	return r, attr
}

func TestExportContentGraphCopiesCompleteUnpinnedMultiChunkGraph(t *testing.T) {
	r, attr := graphReader()
	export, err := r.ExportContentGraph(7, attr)
	if err != nil || !export.Token.Valid() {
		t.Fatalf("export: %v", err)
	}
	encoded := export.Canonical()
	if sum := sha256.Sum256(encoded); export.Digest != hex.EncodeToString(sum[:]) {
		t.Fatal("digest mismatch")
	}
	decoded, err := slicegraph.Decode(encoded)
	if err != nil || len(decoded.Chunks) != 2 {
		t.Fatalf("decoded: %v %+v", err, decoded)
	}
	if decoded.Chunks[1].Extents[0].SliceID != 0 {
		t.Fatal("explicit hole disappeared")
	}
	r.sliceMu.Lock()
	r.sliceCache["7_0"][0].SliceID = 99
	r.sliceCache["7_1"] = nil
	r.inodeEpoch[7]++
	r.sliceMu.Unlock()
	if export.Token.Valid() {
		t.Fatal("invalidated token remains live")
	}
	if !bytes.Equal(encoded, export.Canonical()) {
		t.Fatal("cache mutation changed captured graph")
	}
	graph := export.Graph()
	graph.Chunks[0].Extents[0].SliceID = 88
	encoded[0] = 0
	if export.Graph().Chunks[0].Extents[0].SliceID != 10 || export.Canonical()[0] != 'J' {
		t.Fatal("caller mutation changed export")
	}
}

func TestExportContentGraphCoverageAndWitnessRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Reader, *ResidentAttr)
	}{
		{"missing mapping", func(r *Reader, _ *ResidentAttr) { delete(r.sliceCache, "7_1") }},
		{"missing witness", func(r *Reader, _ *ResidentAttr) { delete(r.attrWitness, "7_1") }},
		{"wrong witness", func(r *Reader, _ *ResidentAttr) { r.attrWitness["7_1"] = ResidentAttr{Size: 3, Valid: true} }},
		{"wrong inode attribute", func(r *Reader, _ *ResidentAttr) { r.inodeAttr[7] = ResidentAttr{Size: 3, Valid: true} }},
		{"invalid source bounds", func(r *Reader, _ *ResidentAttr) { r.sliceCache["7_1"] = []SliceInfo{{SliceID: 42, Size: 2, Len: 4}} }},
		{"wrong expected size", func(_ *Reader, a *ResidentAttr) { a.Size++ }},
		{"invalid expected", func(_ *Reader, a *ResidentAttr) { a.Valid = false }},
		{"invalid binding", func(r *Reader, _ *ResidentAttr) { r.binding.UUID = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, attr := graphReader()
			tc.mutate(r, &attr)
			if _, err := r.ExportContentGraph(7, attr); err == nil {
				t.Fatal("invalid graph exported")
			}
		})
	}
}

func TestExportContentGraphObservedEmptyAndEmptyFile(t *testing.T) {
	r, attr := graphReader()
	r.sliceCache["7_1"] = nil // key exists: observed all-hole tail
	export, err := r.ExportContentGraph(7, attr)
	if err != nil || len(export.Graph().Chunks) != 2 {
		t.Fatalf("observed empty tail: %v", err)
	}
	r.sliceMu.Lock()
	delete(r.sliceCache, "7_1")
	r.sliceMu.Unlock()
	if _, err := r.ExportContentGraph(7, attr); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("unknown tail: %v", err)
	}
	empty := ResidentAttr{Size: 0, MtimeNS: 123, Valid: true}
	r.inodeAttr[7] = empty
	zero, err := r.ExportContentGraph(7, empty)
	if err != nil || len(zero.Graph().Chunks) != 0 {
		t.Fatalf("qualified zero length: %v", err)
	}
}

func TestExportContentGraphEmptyTokenRejectsFirstContradictoryAttr(t *testing.T) {
	r, _ := graphReader()
	delete(r.inodeAttr, 7)
	empty := ResidentAttr{Size: 0, MtimeNS: 123, Valid: true}
	export, err := r.ExportContentGraph(7, empty)
	if err != nil || !export.Token.Valid() {
		t.Fatalf("empty export: %v", err)
	}
	r.sliceMu.Lock()
	r.inodeAttr[7] = empty
	r.sliceMu.Unlock()
	if !export.Token.Valid() {
		t.Fatal("matching first observation rejected")
	}
	r.sliceMu.Lock()
	r.inodeAttr[7] = ResidentAttr{Size: 12, MtimeNS: 123, Valid: true}
	r.sliceMu.Unlock()
	if export.Token.Valid() {
		t.Fatal("first contradictory attribute kept empty export valid")
	}
}
