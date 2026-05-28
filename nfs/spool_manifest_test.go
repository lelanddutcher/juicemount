package nfs

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// readManifest parses the JSONL log into a slice of records.
func readManifest(t *testing.T, path string) []ManifestRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer f.Close()
	var out []ManifestRecord
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		var r ManifestRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("parse line %q: %v", line, err)
		}
		out = append(out, r)
	}
	if err := s.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

// TestManifestCreatedOnNewSpoolStore verifies the file is touched at
// construction so a downstream reader doesn't hit ENOENT.
func TestManifestCreatedOnNewSpoolStore(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	if s.Manifest() == nil {
		t.Fatalf("manifest writer should be initialized")
	}
	if _, err := os.Stat(filepath.Join(s.Root(), ManifestFile)); err != nil {
		t.Errorf("manifest file should exist: %v", err)
	}
}

// TestManifestAppendRecord exercises the write path end-to-end.
func TestManifestAppendRecord(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	mw := s.Manifest()
	if err := mw.Append(ManifestRecord{
		Event:     ManifestEventDrainDone,
		Path:      "/Movies/clip.mov",
		Size:      1024,
		SHA256Hex: "abc123",
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	recs := readManifest(t, mw.Path())
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if recs[0].Event != ManifestEventDrainDone || recs[0].Path != "/Movies/clip.mov" {
		t.Errorf("record fields wrong: %+v", recs[0])
	}
	if recs[0].TimestampRFC3339Nano == "" {
		t.Errorf("timestamp should be auto-populated")
	}
}

// TestManifestConcurrentAppends checks that parallel appends produce
// well-formed lines (no interleaving of partial JSON).
func TestManifestConcurrentAppends(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	mw := s.Manifest()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			_ = mw.Append(ManifestRecord{
				Event: ManifestEventDrainDone,
				Path:  "/f" + string(rune('a'+i%26)) + ".bin",
				Size:  int64(i),
			})
		}(i)
	}
	wg.Wait()

	recs := readManifest(t, mw.Path())
	if len(recs) != N {
		t.Errorf("got %d records, want %d (lost lines = JSONL framing broken)", len(recs), N)
	}
}

// TestManifestRecordsDrainDoneViaMarkDrainComplete verifies the
// integration with the disposition method: completing a drain appends
// a drain_done record.
func TestManifestRecordsDrainDoneViaMarkDrainComplete(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/integration.bin")
	if _, err := e.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, _ = s.Meta().MarkDraining(e.ID())

	if err := s.MarkDrainComplete(e.ID(), e.NFSPath(), e.SpoolFilePath(), 5); err != nil {
		t.Fatalf("MarkDrainComplete: %v", err)
	}

	recs := readManifest(t, s.Manifest().Path())
	if len(recs) != 1 {
		t.Fatalf("expected 1 manifest record, got %d", len(recs))
	}
	if recs[0].Event != ManifestEventDrainDone {
		t.Errorf("event=%q, want %q", recs[0].Event, ManifestEventDrainDone)
	}
	if recs[0].Path != "/integration.bin" {
		t.Errorf("path=%q", recs[0].Path)
	}
	if recs[0].SHA256Hex == "" {
		t.Errorf("sha should be populated from SQL row")
	}
}

// TestManifestRecordsQuarantineViaQuarantineDrain mirrors the
// drain_done test but for the quarantine path. The reason field
// should be populated.
func TestManifestRecordsQuarantineViaQuarantineDrain(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/badbits.bin")
	if _, err := e.WriteAt([]byte("corrupt"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, _ = s.Meta().MarkDraining(e.ID())

	if err := s.QuarantineDrain(e.ID(), e.NFSPath(), e.SpoolFilePath(), 7, "sha mismatch in test"); err != nil {
		t.Fatalf("QuarantineDrain: %v", err)
	}

	recs := readManifest(t, s.Manifest().Path())
	if len(recs) != 1 {
		t.Fatalf("expected 1 manifest record, got %d", len(recs))
	}
	if recs[0].Event != ManifestEventQuarantine {
		t.Errorf("event=%q, want %q", recs[0].Event, ManifestEventQuarantine)
	}
	if recs[0].Reason != "sha mismatch in test" {
		t.Errorf("reason=%q", recs[0].Reason)
	}
}

// TestManifestAppendAfterCloseErrors ensures the closed flag stops
// writes (used during shutdown).
func TestManifestAppendAfterCloseErrors(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	mw := s.Manifest()
	_ = mw.Close()
	if err := mw.Append(ManifestRecord{Event: "x"}); err == nil {
		t.Errorf("expected error appending to closed manifest")
	}
}

// TestManifestSurvivesRestart proves the JSONL log survives a
// SpoolStore restart on the same root — records from the first
// session are preserved, new records append after.
func TestManifestSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	manifestPath := filepath.Join(root, ManifestFile)

	// Write one record via a manifest writer pointed at the root.
	mw1, err := newManifestWriter(root)
	if err != nil {
		t.Fatalf("mw1: %v", err)
	}
	if err := mw1.Append(ManifestRecord{Event: "session1"}); err != nil {
		t.Fatalf("append1: %v", err)
	}
	_ = mw1.Close()
	_ = dbPath // db not actually required for this test

	// Open a fresh writer on the same root and append again.
	mw2, err := newManifestWriter(root)
	if err != nil {
		t.Fatalf("mw2: %v", err)
	}
	if err := mw2.Append(ManifestRecord{Event: "session2"}); err != nil {
		t.Fatalf("append2: %v", err)
	}
	_ = mw2.Close()

	recs := readManifest(t, manifestPath)
	if len(recs) != 2 {
		t.Fatalf("expected 2 records across restart, got %d", len(recs))
	}
	if recs[0].Event != "session1" || recs[1].Event != "session2" {
		t.Errorf("ordering wrong: %+v", recs)
	}
}
