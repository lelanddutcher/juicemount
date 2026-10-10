package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/juicedata/juicefs/pkg/slicegraph"
)

// ContentGraphExport is a local, same-lock copy of every observed mapping
// intersecting the expected file length. The caller must bind this evidence
// to its accepted local namespace row and recheck Token immediately before
// that acceptance. A later cache invalidation cannot mutate the copied bytes.
type ContentGraphExport struct {
	Binding VolumeBinding
	Inode   uint64
	Attr    ResidentAttr
	Digest  string // SHA-256 of Canonical(), lowercase hex
	Token   ContentGraphToken

	graph     slicegraph.Graph
	canonical []byte
}

// ContentGraphToken also binds the expected attributes. A first observation
// after an empty-file export must not silently change its length authority.
type ContentGraphToken struct {
	token    ResidentToken
	expected ResidentAttr
}

func (t ContentGraphToken) Valid() bool {
	r := t.token.reader
	if r == nil {
		return false
	}
	r.sliceMu.RLock()
	defer r.sliceMu.RUnlock()
	if r.stopped.Load() || r.generationLocked(t.token.inode) != t.token.generation {
		return false
	}
	attr, known := r.inodeAttr[t.token.inode]
	return !known || attr == t.expected
}

// Graph returns a deep copy independent of the export and Reader's cache.
func (e ContentGraphExport) Graph() slicegraph.Graph { return e.graph.Clone() }

// Canonical returns the versioned graph encoding independently of the export.
func (e ContentGraphExport) Canonical() []byte { return append([]byte(nil), e.canonical...) }

// ExportContentGraph performs only local RAM work. An observed empty mapping
// key is a hole; a missing key is unknown and refuses export. Each nonempty
// chunk requires the joint same-observation attribute witness. For length zero
// the qualified expected attribute is the file-length authority, so no chunk
// mapping is inferred or required.
func (r *Reader) ExportContentGraph(inode uint64, expected ResidentAttr) (ContentGraphExport, error) {
	if r == nil || r.stopped.Load() || inode == 0 || !r.binding.valid() ||
		!expected.Valid || expected.Size < 0 {
		return ContentGraphExport{}, ErrCacheMiss
	}
	length := uint64(expected.Size)
	count := length / ChunkSize
	if length%ChunkSize != 0 {
		count++
	}
	// The graph encoding bounds both metadata and the number of observed keys.
	if count > (slicegraph.MaxGraphBytes-20)/13 {
		return ContentGraphExport{}, ErrSliceMapping
	}
	graph := slicegraph.Graph{Version: slicegraph.Version, Length: length, Chunks: make([]slicegraph.Chunk, 0, min(count, 1024))}
	r.sliceMu.RLock()
	if r.stopped.Load() || !r.binding.valid() {
		r.sliceMu.RUnlock()
		return ContentGraphExport{}, ErrCacheMiss
	}
	if known, ok := r.inodeAttr[inode]; ok && known != expected {
		r.sliceMu.RUnlock()
		return ContentGraphExport{}, ErrCacheMiss
	}
	var totalExtents uint64
	for i := uint64(0); i < count; i++ {
		key := fmt.Sprintf("%d_%d", inode, i)
		slices, known := r.sliceCache[key]
		if !known || r.attrWitness[key] != expected {
			r.sliceMu.RUnlock()
			return ContentGraphExport{}, ErrCacheMiss
		}
		totalExtents += uint64(len(slices))
		if totalExtents > slicegraph.MaxGraphBytes/21 {
			r.sliceMu.RUnlock()
			return ContentGraphExport{}, ErrSliceMapping
		}
		graph.Chunks = append(graph.Chunks, slicegraph.Chunk{Index: uint32(i), Extents: append([]slicegraph.Extent(nil), slices...)})
	}
	token := ContentGraphToken{token: ResidentToken{reader: r, inode: inode, generation: r.generationLocked(inode)}, expected: expected}
	r.sliceMu.RUnlock()

	canonical, err := graph.Canonical()
	if err != nil {
		return ContentGraphExport{}, fmt.Errorf("%w: %v", ErrSliceMapping, err)
	}
	if !token.Valid() {
		return ContentGraphExport{}, ErrCacheMiss
	}
	digest := sha256.Sum256(canonical)
	return ContentGraphExport{
		Binding: r.binding, Inode: inode, Attr: expected, Digest: hex.EncodeToString(digest[:]),
		Token: token, graph: graph, canonical: canonical,
	}, nil
}
