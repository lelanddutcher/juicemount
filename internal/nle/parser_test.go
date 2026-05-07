package nle

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------
// DetectKind
// ---------------------------------------------------------------------

func TestDetectKind(t *testing.T) {
	cases := []struct {
		path string
		want ProjectKind
	}{
		{"/tmp/foo.prproj", KindPremiere},
		{"/tmp/Foo.PRPROJ", KindPremiere},
		{"/tmp/bar.drp", KindResolve},
		{"/tmp/baz.fcpxml", KindFCPX},
		{"/tmp/Library.fcpbundle", KindFCPX},
		{"/tmp/random.txt", KindUnknown},
		{"/tmp/no-extension", KindUnknown},
		{"", KindUnknown},
	}
	for _, c := range cases {
		got := DetectKind(c.path)
		if got != c.want {
			t.Errorf("DetectKind(%q) = %s, want %s", c.path, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------
// Premiere
// ---------------------------------------------------------------------

const premiereSampleXML = `<?xml version="1.0" encoding="UTF-8"?>
<PremiereData>
  <Project ObjectID="1">
    <RootProjectItem>
      <Media>
        <ActualMediaFilePath>/Volumes/zpool/Footage/A001_C001.r3d</ActualMediaFilePath>
        <pproUrl>file:///Volumes/zpool/Footage/Audio%20Files/take_01.wav</pproUrl>
      </Media>
      <Media>
        <FilePath>/Volumes/zpool/Footage/B-Roll/sunset.mp4</FilePath>
        <ProxyPath>/Volumes/zpool/Proxies/sunset_proxy.mov</ProxyPath>
      </Media>
      <Media>
        <ActualMediaFilePath>/Volumes/zpool/Footage/A001_C001.r3d</ActualMediaFilePath>
      </Media>
    </RootProjectItem>
  </Project>
</PremiereData>`

func writeGzip(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	gz := gzip.NewWriter(f)
	if _, err := gz.Write([]byte(content)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestParsePremiere(t *testing.T) {
	dir := t.TempDir()
	pj := filepath.Join(dir, "test.prproj")
	writeGzip(t, pj, premiereSampleXML)

	refs, err := Parse(pj)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	paths := pathSet(refs)
	want := []string{
		"/Volumes/zpool/Footage/A001_C001.r3d",
		"/Volumes/zpool/Footage/Audio Files/take_01.wav",
		"/Volumes/zpool/Footage/B-Roll/sunset.mp4",
		"/Volumes/zpool/Proxies/sunset_proxy.mov",
	}
	for _, w := range want {
		if !paths[w] {
			t.Errorf("missing path %q in refs %+v", w, refs)
		}
	}
	// Dedupe check: A001_C001.r3d appears twice, expect one entry.
	count := 0
	for _, r := range refs {
		if r.Path == "/Volumes/zpool/Footage/A001_C001.r3d" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected dedup of duplicate path, got %d entries", count)
	}

	// Format extraction.
	for _, r := range refs {
		if r.Path == "/Volumes/zpool/Footage/A001_C001.r3d" && r.Format != "r3d" {
			t.Errorf("Format = %q, want r3d", r.Format)
		}
		if r.Path == "/Volumes/zpool/Proxies/sunset_proxy.mov" && !r.Optional {
			t.Errorf("ProxyPath should be Optional=true")
		}
	}
}

// TestParsePremiereRejectsNumericIDs reproduces a real-world false
// positive seen in Premiere projects exported from Windows: the same
// <FilePath> tag is reused for object IDs ("1129270354") AND real
// paths. We must reject the bare-integer case.
func TestParsePremiereRejectsNumericIDs(t *testing.T) {
	const xml = `<?xml version="1.0" encoding="UTF-8"?>
<PremiereData>
  <FilePath>1129270354</FilePath>
  <FilePath>1112293707</FilePath>
  <FilePath>Z:\Film Projects\foo\bar.mp4</FilePath>
  <ActualMediaFilePath>/Volumes/zpool/clip.r3d</ActualMediaFilePath>
</PremiereData>`
	dir := t.TempDir()
	pj := filepath.Join(dir, "ids.prproj")
	writeGzip(t, pj, xml)
	refs, err := Parse(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("got %d refs, want 2 (numeric IDs rejected): %+v", len(refs), refs)
	}
	paths := pathSet(refs)
	if !paths[`Z:\Film Projects\foo\bar.mp4`] {
		t.Errorf("expected Windows path, got %+v", refs)
	}
	if !paths["/Volumes/zpool/clip.r3d"] {
		t.Errorf("expected POSIX path, got %+v", refs)
	}
}

func TestParsePremiereMixedFormats(t *testing.T) {
	const xml = `<?xml version="1.0" encoding="UTF-8"?>
<PremiereData>
  <Media><ActualMediaFilePath>/m/clip.r3d</ActualMediaFilePath></Media>
  <Media><ActualMediaFilePath>/m/audio.wav</ActualMediaFilePath></Media>
  <Media><ActualMediaFilePath>/m/render.mp4</ActualMediaFilePath></Media>
  <Media><ActualMediaFilePath>/m/photo.jpg</ActualMediaFilePath></Media>
</PremiereData>`
	dir := t.TempDir()
	pj := filepath.Join(dir, "mixed.prproj")
	writeGzip(t, pj, xml)

	refs, err := Parse(pj)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := map[string]bool{}
	for _, r := range refs {
		got[r.Format] = true
	}
	for _, f := range []string{"r3d", "wav", "mp4", "jpg"} {
		if !got[f] {
			t.Errorf("missing format %q in refs %+v", f, refs)
		}
	}
}

func TestParsePremiereMalformed(t *testing.T) {
	dir := t.TempDir()

	// Case 1: not gzipped at all.
	bad1 := filepath.Join(dir, "bad1.prproj")
	if err := os.WriteFile(bad1, []byte("not gzip"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(bad1); err == nil {
		t.Error("expected error on non-gzip file")
	}

	// Case 2: gzipped, but the inner content is not XML.
	bad2 := filepath.Join(dir, "bad2.prproj")
	writeGzip(t, bad2, "{json: not xml}")
	// The token-based parser tolerates non-XML by returning nothing
	// or a parse error. Either is acceptable behaviour as long as we
	// don't panic.
	refs, err := Parse(bad2)
	if err == nil && len(refs) > 0 {
		t.Errorf("non-XML inside gzip should yield error or empty, got %d refs", len(refs))
	}
}

// ---------------------------------------------------------------------
// FCPX
// ---------------------------------------------------------------------

const fcpxSampleXML = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE fcpxml>
<fcpxml version="1.10">
  <resources>
    <asset id="r1" name="clip1" src="file:///Volumes/zpool/A001_C001.r3d"/>
    <asset id="r2" name="audio" src="file:///Volumes/zpool/Audio%20Files/take_01.wav"/>
    <asset id="r3" name="vid" src="file:///Volumes/zpool/render.mp4"/>
    <asset id="r4" name="vid2">
      <media-rep src="file:///Volumes/zpool/extra.mov"/>
    </asset>
  </resources>
</fcpxml>`

func TestParseFCPXML(t *testing.T) {
	dir := t.TempDir()
	pj := filepath.Join(dir, "test.fcpxml")
	if err := os.WriteFile(pj, []byte(fcpxSampleXML), 0o644); err != nil {
		t.Fatal(err)
	}
	refs, err := Parse(pj)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	paths := pathSet(refs)
	for _, w := range []string{
		"/Volumes/zpool/A001_C001.r3d",
		"/Volumes/zpool/Audio Files/take_01.wav",
		"/Volumes/zpool/render.mp4",
		"/Volumes/zpool/extra.mov",
	} {
		if !paths[w] {
			t.Errorf("missing %q in %+v", w, refs)
		}
	}
}

func TestParseFCPBundle(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "Library.fcpbundle")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	xmlPath := filepath.Join(bundle, "CurrentVersion.fcpxml")
	if err := os.WriteFile(xmlPath, []byte(fcpxSampleXML), 0o644); err != nil {
		t.Fatal(err)
	}

	if k := DetectKind(bundle); k != KindFCPX {
		t.Errorf("DetectKind(bundle) = %s, want fcpx", k)
	}
	refs, err := Parse(bundle)
	if err != nil {
		t.Fatalf("Parse(bundle): %v", err)
	}
	if len(refs) == 0 {
		t.Error("expected refs from bundle, got 0")
	}
}

func TestParseFCPXMalformed(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.fcpxml")
	if err := os.WriteFile(bad, []byte("<<<not xml>>>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(bad); err == nil {
		t.Error("expected error on malformed xml")
	}
}

// ---------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------

// makeResolveSqliteDB writes a minimal SQLite database with a "media"
// table populated with rows. Returns the file path.
func makeResolveSqliteDB(t *testing.T, dir string, rows []MediaRef) string {
	t.Helper()
	dbPath := filepath.Join(dir, "Project.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE media (path TEXT, size INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO media (path, size) VALUES (?, ?)`,
			r.Path, r.Size); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return dbPath
}

// zipResolveDB wraps a Project.db file in a zip and writes it to drpPath.
func zipResolveDB(t *testing.T, drpPath, dbPath string) {
	t.Helper()
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("Project.db")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	if _, err := w.Write(dbData); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	if err := os.WriteFile(drpPath, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write drp: %v", err)
	}
}

func TestParseResolveZip(t *testing.T) {
	dir := t.TempDir()
	rows := []MediaRef{
		{Path: "/Volumes/zpool/A001_C001.r3d", Size: 12345},
		{Path: "/Volumes/zpool/audio.wav", Size: 678},
		{Path: "/Volumes/zpool/render.mp4", Size: 0},
	}
	dbPath := makeResolveSqliteDB(t, dir, rows)
	drpPath := filepath.Join(dir, "test.drp")
	zipResolveDB(t, drpPath, dbPath)

	refs, err := Parse(drpPath)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("got %d refs, want 3: %+v", len(refs), refs)
	}
	paths := pathSet(refs)
	for _, r := range rows {
		if !paths[r.Path] {
			t.Errorf("missing %q", r.Path)
		}
	}
	for _, r := range refs {
		if r.Path == "/Volumes/zpool/A001_C001.r3d" && r.Size != 12345 {
			t.Errorf("Size = %d, want 12345", r.Size)
		}
	}
}

func TestParseResolveRawSQLite(t *testing.T) {
	dir := t.TempDir()
	rows := []MediaRef{
		{Path: "/Volumes/zpool/clip.r3d", Size: 99},
	}
	dbPath := makeResolveSqliteDB(t, dir, rows)
	// Rename .db to .drp — the sniffer should accept the SQLite magic.
	drpPath := filepath.Join(dir, "raw.drp")
	if err := os.Rename(dbPath, drpPath); err != nil {
		t.Fatal(err)
	}
	refs, err := Parse(drpPath)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(refs) != 1 || refs[0].Path != "/Volumes/zpool/clip.r3d" {
		t.Errorf("unexpected refs: %+v", refs)
	}
	if refs[0].Size != 99 {
		t.Errorf("Size = %d, want 99", refs[0].Size)
	}
}

func TestParseResolveProprietary(t *testing.T) {
	dir := t.TempDir()
	drpPath := filepath.Join(dir, "proprietary.drp")
	// Plausible-looking opaque header that is neither zip nor SQLite.
	if err := os.WriteFile(drpPath, []byte("DRP\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Parse(drpPath)
	if !errors.Is(err, ErrUnsupportedResolveVariant) {
		t.Errorf("err = %v, want ErrUnsupportedResolveVariant", err)
	}
}

// ---------------------------------------------------------------------
// Cross-format mixed-media test
// ---------------------------------------------------------------------

func TestParseMixedMediaPremiere(t *testing.T) {
	// A single project referencing r3d + wav + mp4 — the typical
	// mix on a JuiceMount user's timeline.
	const xml = `<?xml version="1.0" encoding="UTF-8"?>
<PremiereData>
  <Media><pproUrl>file:///mnt/footage/RED/A001_C001.r3d</pproUrl></Media>
  <Media><pproUrl>file:///mnt/footage/Audio/master.wav</pproUrl></Media>
  <Media><pproUrl>file:///mnt/footage/B-Roll/drone.mp4</pproUrl></Media>
</PremiereData>`
	dir := t.TempDir()
	pj := filepath.Join(dir, "mixed.prproj")
	writeGzip(t, pj, xml)

	refs, err := Parse(pj)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("want 3 refs, got %d", len(refs))
	}
	formats := []string{}
	for _, r := range refs {
		formats = append(formats, r.Format)
	}
	sort.Strings(formats)
	want := []string{"mp4", "r3d", "wav"}
	if strings.Join(formats, ",") != strings.Join(want, ",") {
		t.Errorf("formats = %v, want %v", formats, want)
	}
}

// ---------------------------------------------------------------------
// Unknown kind
// ---------------------------------------------------------------------

func TestParseUnknown(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "thing.txt")
	if err := os.WriteFile(bad, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Parse(bad)
	if !errors.Is(err, ErrUnknownKind) {
		t.Errorf("err = %v, want ErrUnknownKind", err)
	}
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

func pathSet(refs []MediaRef) map[string]bool {
	m := make(map[string]bool, len(refs))
	for _, r := range refs {
		m[r.Path] = true
	}
	return m
}
