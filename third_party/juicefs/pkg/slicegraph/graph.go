// Package slicegraph provides the common, versioned representation of the
// visible JuiceFS slice graph. It does not read Redis, media, or metadata.
package slicegraph

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

const (
	ChunkSize       = 64 << 20
	Version         = 1
	MaxGraphBytes   = 64 << 20
	maxDecodedSlots = MaxGraphBytes / 32
)

var ErrInvalidGraph = errors.New("invalid slice graph")

// Extent is a resolved visible part of a JuiceFS chunk. SliceID zero is an
// explicit zero range; a gap between extents is also a zero range.
type Extent struct {
	Pos     uint32
	SliceID uint64
	Size    uint32
	Off     uint32
	Len     uint32
}

type Chunk struct {
	Index   uint32
	Extents []Extent
}

// Graph contains one observed chunk for every chunk intersecting [0, Length).
// A Chunk with no Extents represents an observed empty mapping, not an unknown
// mapping. Neither this type nor its digest supplies observation provenance.
type Graph struct {
	Version uint32
	Length  uint64
	Chunks  []Chunk
}

// Clone prevents subsequent mutations of a returned slice from changing a
// captured graph.
func (g Graph) Clone() Graph {
	copyGraph := Graph{Version: g.Version, Length: g.Length, Chunks: make([]Chunk, len(g.Chunks))}
	for i, chunk := range g.Chunks {
		copyGraph.Chunks[i] = Chunk{Index: chunk.Index, Extents: append([]Extent(nil), chunk.Extents...)}
	}
	return copyGraph
}

// Resolve applies append-order JuiceFS slice overlays without changing raw.
// Zero-length history entries remain no-ops, matching JuiceFS buildSlice.
func Resolve(raw []Extent) ([]Extent, error) {
	visible := make([]Extent, 0, len(raw))
	for _, next := range raw {
		if next.Len == 0 {
			continue
		}
		end := uint64(next.Pos) + uint64(next.Len)
		if end > ChunkSize {
			return nil, fmt.Errorf("%w: slice interval overflows chunk", ErrInvalidGraph)
		}
		if next.SliceID != 0 && uint64(next.Off)+uint64(next.Len) > uint64(next.Size) {
			return nil, fmt.Errorf("%w: slice extent exceeds source", ErrInvalidGraph)
		}
		updated := make([]Extent, 0, len(visible)+1)
		for _, old := range visible {
			oldStart := uint64(old.Pos)
			oldEnd := oldStart + uint64(old.Len)
			if oldEnd <= uint64(next.Pos) || oldStart >= end {
				updated = append(updated, old)
				continue
			}
			if oldStart < uint64(next.Pos) {
				left := old
				left.Len = next.Pos - old.Pos
				updated = append(updated, left)
			}
			if oldEnd > end {
				right := old
				delta := uint32(end - oldStart)
				right.Pos = uint32(end)
				right.Off += delta
				right.Len = uint32(oldEnd - end)
				updated = append(updated, right)
			}
		}
		updated = append(updated, next)
		sort.Slice(updated, func(i, j int) bool { return updated[i].Pos < updated[j].Pos })
		visible = updated
	}
	return visible, nil
}

type segment struct {
	length uint32
	id     uint64
	size   uint32
	off    uint32
}

func appendSegment(dst []segment, next segment) []segment {
	if next.length == 0 {
		return dst
	}
	if n := len(dst); n > 0 {
		last := &dst[n-1]
		if last.id == 0 && next.id == 0 ||
			last.id != 0 && last.id == next.id && last.size == next.size &&
				uint64(last.off)+uint64(last.length) == uint64(next.off) {
			last.length += next.length
			return dst
		}
	}
	return append(dst, next)
}

func chunkSegments(chunk Chunk, covered uint32) ([]segment, error) {
	var out []segment
	var validatedEnd uint64 // ordering and bounds include extents beyond EOF
	var visibleEnd uint64   // only bytes inside [0, covered) fill visible holes
	for _, extent := range chunk.Extents {
		end := uint64(extent.Pos) + uint64(extent.Len)
		if extent.Len == 0 || uint64(extent.Pos) < validatedEnd || end > ChunkSize ||
			extent.SliceID != 0 && uint64(extent.Off)+uint64(extent.Len) > uint64(extent.Size) {
			return nil, ErrInvalidGraph
		}
		validatedEnd = end
		if uint64(extent.Pos) >= uint64(covered) {
			continue
		}
		if uint64(extent.Pos) > visibleEnd {
			out = appendSegment(out, segment{length: uint32(uint64(extent.Pos) - visibleEnd)})
		}
		clippedEnd := min(end, uint64(covered))
		out = appendSegment(out, segment{length: uint32(clippedEnd - uint64(extent.Pos)), id: extent.SliceID, size: extent.Size, off: extent.Off})
		visibleEnd = clippedEnd
	}
	if visibleEnd < uint64(covered) {
		out = appendSegment(out, segment{length: uint32(uint64(covered) - visibleEnd)})
	}
	return out, nil
}

// Canonical encodes the logical extents, including implicit holes, on [0,L).
// Adjacent equivalent extents and explicit zero ranges have one encoding.
// The metadata cap prevents a corrupt witness from becoming an unbounded
// stage marker or an unbounded WATCH comparison.
func (g Graph) Canonical() ([]byte, error) {
	if g.Version != Version {
		return nil, ErrInvalidGraph
	}
	count := g.Length / ChunkSize
	if g.Length%ChunkSize != 0 {
		count++
	}
	if count > (MaxGraphBytes-20)/13 || count > maxDecodedSlots/2 || count != uint64(len(g.Chunks)) {
		return nil, ErrInvalidGraph
	}
	var buf bytes.Buffer
	buf.Grow(20 + len(g.Chunks)*13)
	buf.WriteString("JMSG")
	_ = binary.Write(&buf, binary.BigEndian, g.Version)
	_ = binary.Write(&buf, binary.BigEndian, g.Length)
	_ = binary.Write(&buf, binary.BigEndian, uint32(count))
	var totalSegments uint64
	for i, chunk := range g.Chunks {
		if uint64(chunk.Index) != uint64(i) {
			return nil, ErrInvalidGraph
		}
		if len(chunk.Extents) > MaxGraphBytes/21 {
			return nil, ErrInvalidGraph
		}
		covered := uint32(min(uint64(ChunkSize), g.Length-uint64(i)*ChunkSize))
		segments, err := chunkSegments(chunk, covered)
		totalSegments += uint64(len(segments))
		if err != nil || len(segments) > MaxGraphBytes/9 || count+totalSegments > maxDecodedSlots {
			return nil, ErrInvalidGraph
		}
		encodedSize := 8
		for _, seg := range segments {
			encodedSize += 5
			if seg.id != 0 {
				encodedSize += 16
			}
		}
		if buf.Len()+encodedSize > MaxGraphBytes {
			return nil, ErrInvalidGraph
		}
		_ = binary.Write(&buf, binary.BigEndian, chunk.Index)
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(segments)))
		for _, seg := range segments {
			kind := byte(0)
			if seg.id != 0 {
				kind = 1
			}
			buf.WriteByte(kind)
			_ = binary.Write(&buf, binary.BigEndian, seg.length)
			if kind == 1 {
				_ = binary.Write(&buf, binary.BigEndian, seg.id)
				_ = binary.Write(&buf, binary.BigEndian, seg.size)
				_ = binary.Write(&buf, binary.BigEndian, seg.off)
			}
		}
	}
	return buf.Bytes(), nil
}

func (g Graph) Digest() ([sha256.Size]byte, error) {
	encoded, err := g.Canonical()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// Decode validates the entire input before allocating the graph. The decoded
// representation uses substantially more memory than the wire format, so the
// combined chunk/extent budget is separate from MaxGraphBytes.
func Decode(encoded []byte) (Graph, error) {
	if _, err := scanCanonical(encoded, false); err != nil {
		return Graph{}, err
	}
	return scanCanonical(encoded, true)
}

func scanCanonical(encoded []byte, materialize bool) (Graph, error) {
	var g Graph
	if len(encoded) < 20 || len(encoded) > MaxGraphBytes || string(encoded[:4]) != "JMSG" {
		return g, ErrInvalidGraph
	}
	g.Version = binary.BigEndian.Uint32(encoded[4:8])
	g.Length = binary.BigEndian.Uint64(encoded[8:16])
	count := binary.BigEndian.Uint32(encoded[16:20])
	expected := g.Length / ChunkSize
	if g.Length%ChunkSize != 0 {
		expected++
	}
	// Chunk is 32 bytes and Extent is 32 bytes on supported Go targets.
	// Count both before making either slice; a 5-byte zero segment cannot
	// expand a small encoded graph into an unbounded allocation.
	if g.Version != Version || uint64(count) != expected ||
		uint64(count) > uint64(len(encoded)-20)/13 || count > maxDecodedSlots {
		return Graph{}, ErrInvalidGraph
	}
	if materialize {
		g.Chunks = make([]Chunk, count)
	}
	pos := 20
	var totalSegments uint64
	for i := uint32(0); i < count; i++ {
		if len(encoded)-pos < 8 {
			return Graph{}, ErrInvalidGraph
		}
		index := binary.BigEndian.Uint32(encoded[pos : pos+4])
		segments := binary.BigEndian.Uint32(encoded[pos+4 : pos+8])
		pos += 8
		if index != i || segments == 0 || uint64(segments) > uint64(len(encoded)-pos)/5 ||
			uint64(count)+totalSegments+uint64(segments) > maxDecodedSlots {
			return Graph{}, ErrInvalidGraph
		}
		totalSegments += uint64(segments)
		covered := uint64(min(uint64(ChunkSize), g.Length-uint64(i)*ChunkSize))
		if materialize {
			g.Chunks[i] = Chunk{Index: i, Extents: make([]Extent, segments)}
		}
		var offset uint64
		var prior Extent
		for j := uint32(0); j < segments; j++ {
			if len(encoded)-pos < 5 {
				return Graph{}, ErrInvalidGraph
			}
			kind := encoded[pos]
			length := binary.BigEndian.Uint32(encoded[pos+1 : pos+5])
			pos += 5
			if length == 0 || offset+uint64(length) > covered {
				return Graph{}, ErrInvalidGraph
			}
			extent := Extent{Pos: uint32(offset), Len: length}
			if kind == 1 {
				if len(encoded)-pos < 16 {
					return Graph{}, ErrInvalidGraph
				}
				extent.SliceID = binary.BigEndian.Uint64(encoded[pos : pos+8])
				extent.Size = binary.BigEndian.Uint32(encoded[pos+8 : pos+12])
				extent.Off = binary.BigEndian.Uint32(encoded[pos+12 : pos+16])
				pos += 16
				if extent.SliceID == 0 || uint64(extent.Off)+uint64(length) > uint64(extent.Size) {
					return Graph{}, ErrInvalidGraph
				}
			} else if kind != 0 {
				return Graph{}, ErrInvalidGraph
			}
			if j > 0 && (prior.SliceID == 0 && extent.SliceID == 0 ||
				prior.SliceID != 0 && prior.SliceID == extent.SliceID && prior.Size == extent.Size &&
					uint64(prior.Off)+uint64(prior.Len) == uint64(extent.Off)) {
				return Graph{}, ErrInvalidGraph // adjacent segments must be merged
			}
			if materialize {
				g.Chunks[i].Extents[j] = extent
			}
			prior = extent
			offset += uint64(length)
		}
		if offset != covered {
			return Graph{}, ErrInvalidGraph
		}
	}
	if pos != len(encoded) {
		return Graph{}, ErrInvalidGraph
	}
	return g, nil
}
