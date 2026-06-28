package derivatives

import "testing"

// TestProxyCodecRoundTrip: the PROXY-CODEC fields (codec/codec_string/blob_size)
// persist into the `extra` JSON column and read back on a proxy row; a non-proxy
// row never carries them (the schema forbids `codec` off-proxy, and omitempty +
// the kind-gated decode guarantee it).
func TestProxyCodecRoundTrip(t *testing.T) {
	s := openTest(t)
	const inode = uint64(1180621)
	if err := s.PutSource(inode, sp("9f2b1c0a4e7d8b30")); err != nil {
		t.Fatal(err)
	}
	codec, cs := "h264", "avc1.640028, mp4a.40.2"
	var bsz int64 = 20447232
	rel, mt := "proxy.mp4", "video/mp4"
	if err := s.PutDeriv(inode, DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, Hash: sp("9f2b1c0a4e7d8b30"),
		BlobRelPath: &rel, MediaType: &mt, Codec: &codec, CodecString: &cs, BlobSize: &bsz,
	}); err != nil {
		t.Fatal(err)
	}
	// A sibling tech row must NOT carry codec fields.
	if err := s.PutDeriv(inode, DerivRow{Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1}); err != nil {
		t.Fatal(err)
	}

	rows, err := s.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	var proxy, tech *DerivRow
	for i := range rows {
		switch rows[i].Kind {
		case "proxy":
			proxy = &rows[i]
		case "tech":
			tech = &rows[i]
		}
	}
	if proxy == nil || tech == nil {
		t.Fatalf("missing rows: %+v", rows)
	}
	if proxy.Codec == nil || *proxy.Codec != "h264" {
		t.Errorf("proxy.Codec = %v, want h264", proxy.Codec)
	}
	if proxy.CodecString == nil || *proxy.CodecString != cs {
		t.Errorf("proxy.CodecString = %v, want %q", proxy.CodecString, cs)
	}
	if proxy.BlobSize == nil || *proxy.BlobSize != bsz {
		t.Errorf("proxy.BlobSize = %v, want %d", proxy.BlobSize, bsz)
	}
	if tech.Codec != nil || tech.CodecString != nil || tech.BlobSize != nil {
		t.Errorf("tech row leaked codec fields: %+v", tech)
	}
}

// TestFilmstripStillDecodes: the new envelope encoding does not break filmstrip
// geometry round-trips (the `extra` column is now shared).
func TestFilmstripStillDecodes(t *testing.T) {
	s := openTest(t)
	const inode = uint64(7)
	_ = s.PutSource(inode, sp("h"))
	geo := &FilmstripGeo{FrameCount: 120, Cols: 12, Rows: 10, CellW: 160, CellH: 90, IntervalMS: 1000, DurationMS: 120000}
	if err := s.PutDeriv(inode, DerivRow{Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1, Filmstrip: geo}); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.Manifest(inode)
	if len(rows) != 1 || rows[0].Filmstrip == nil || rows[0].Filmstrip.Cols != 12 {
		t.Fatalf("filmstrip geo lost: %+v", rows)
	}
}
