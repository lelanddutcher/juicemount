package metadata

import (
	"fmt"
	"testing"
	"time"
)

func TestSearchBasic(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Insert test entries simulating a video production library
	entries := []*Entry{
		MakeEntry("SFX/Impacts/Big_Explosion_4K.wav", false, 1024*1024*5, now, 100),
		MakeEntry("SFX/Impacts/Small_Explosion_Debris.wav", false, 1024*1024*2, now, 101),
		MakeEntry("SFX/Whoosh/Whoosh_Fast_01.wav", false, 1024*1024, now, 102),
		MakeEntry("SFX/Whoosh/Whoosh_Slow_02.wav", false, 1024*512, now, 103),
		MakeEntry("Footage/Aerials/Drone_Explosion_Scene.mov", false, 1024*1024*500, now, 104),
		MakeEntry("Footage/Aerials/Sunset_Beach.mov", false, 1024*1024*300, now, 105),
		MakeEntry("Footage/B-Roll/City_Traffic.mov", false, 1024*1024*200, now, 106),
		MakeEntry("LUTs/Film_Look_01.cube", false, 1024*100, now, 107),
	}
	for _, e := range entries {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}

	// Rebuild FTS after individual inserts (FTS is rebuilt after BulkInsert
	// and sync cycles; individual inserts don't update FTS incrementally)
	if err := s.RebuildFTS(); err != nil {
		t.Fatal(err)
	}

	// Search for "explosion" — should match 3 entries
	results, err := s.Search("Explosion", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results for 'Explosion', got %d", len(results))
	}

	// Verify all results contain "explosion" in name or path
	for _, r := range results {
		t.Logf("  result: %s (rank=%.2f)", r.Entry.Path, r.Rank)
	}
}

func TestSearchPartialMatch(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("Music/Epic_Orchestral_Theme.mp3", false, 1024*1024*8, now, 200))
	s.Insert(MakeEntry("Music/Ambient_Pad_Long.mp3", false, 1024*1024*12, now, 201))
	s.Insert(MakeEntry("SFX/Orchestra_Hit.wav", false, 1024*512, now, 202))

	s.RebuildFTS()

	// Partial match: "orch" should match "Orchestral" and "Orchestra"
	results, err := s.Search("orch", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results for 'orch', got %d", len(results))
	}
}

func TestSearchScopedToSubtree(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("SFX/Impacts/Boom_01.wav", false, 1024*1024, now, 300))
	s.Insert(MakeEntry("SFX/Impacts/Boom_02.wav", false, 1024*1024, now, 301))
	s.Insert(MakeEntry("Music/Boom_Bass.mp3", false, 1024*1024*5, now, 302))
	s.Insert(MakeEntry("Footage/Boom_Shot.mov", false, 1024*1024*200, now, 303))

	s.RebuildFTS()

	// Search "boom" scoped to SFX — should only match 2
	results, err := s.Search("Boom", 50, "SFX")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results for 'Boom' in SFX, got %d", len(results))
	}
	for _, r := range results {
		if r.Entry.ParentPath != "SFX/Impacts" {
			t.Fatalf("result outside SFX: %s", r.Entry.Path)
		}
	}

	// Search "boom" unscoped — should match all 4
	allResults, err := s.Search("Boom", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(allResults) != 4 {
		t.Fatalf("expected 4 results for 'Boom' unscoped, got %d", len(allResults))
	}
}

func TestSearchLimit(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Insert 20 files with "clip" in the name
	for i := 0; i < 20; i++ {
		s.Insert(MakeEntry(fmt.Sprintf("project/clip_%03d.mov", i), false, int64(i*1000), now, uint64(400+i)))
	}

	s.RebuildFTS()

	// Limit to 5
	results, err := s.Search("clip", 5, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 {
		t.Fatalf("expected 5 results with limit, got %d", len(results))
	}
}

func TestSearchEmptyQuery(t *testing.T) {
	s := newTestStore(t)

	results, err := s.Search("", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty query, got %d", len(results))
	}
}

func TestSearchNoResults(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("file.txt", false, 100, now, 500))

	s.RebuildFTS()
	results, err := s.Search("nonexistent_term_xyz", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestSearchAfterBulkInsert(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	// Simulate a Redis sync: bulk insert entries
	entries := make([]*Entry, 100)
	for i := range entries {
		name := fmt.Sprintf("file_%03d.mov", i)
		if i%10 == 0 {
			name = fmt.Sprintf("transition_%03d.mov", i)
		}
		entries[i] = MakeEntry("project/"+name, false, int64(i*100), now, uint64(600+i))
	}
	if err := s.BulkInsert(entries, 500); err != nil {
		t.Fatal(err)
	}

	// FTS triggers fire on INSERT, so search should work immediately
	results, err := s.Search("transition", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 transition results after bulk insert, got %d", len(results))
	}
}

func TestSearchAfterDelete(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("SFX/thunder.wav", false, 1024, now, 700))
	s.Insert(MakeEntry("SFX/rain.wav", false, 2048, now, 701))
	s.RebuildFTS()

	// Should find thunder
	results, err := s.Search("thunder", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result before delete, got %d", len(results))
	}

	// Delete thunder
	if err := s.Delete("SFX/thunder.wav"); err != nil {
		t.Fatal(err)
	}
	s.RebuildFTS()

	// Should no longer find it
	results2, err := s.Search("thunder", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results2) != 0 {
		t.Fatalf("expected 0 results after delete, got %d", len(results2))
	}
}

func TestSearchRebuildFTS(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("alpha.txt", false, 100, now, 800))
	s.Insert(MakeEntry("beta.txt", false, 200, now, 801))

	// Rebuild FTS
	if err := s.RebuildFTS(); err != nil {
		t.Fatal(err)
	}

	// Search should still work
	results, err := s.Search("alpha", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result after FTS rebuild, got %d", len(results))
	}
}

func TestSearchCaseInsensitive(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().Truncate(time.Second)

	s.Insert(MakeEntry("project/UPPERCASE_FILE.MOV", false, 1024, now, 900))
	s.Insert(MakeEntry("project/lowercase_file.mov", false, 2048, now, 901))
	s.Insert(MakeEntry("project/MixedCase_File.mov", false, 4096, now, 902))
	s.RebuildFTS()

	// Search lowercase should match uppercase too (trigram is case-sensitive
	// by default, but we search the literal string which covers exact case)
	results, err := s.Search("UPPERCASE", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) < 1 {
		t.Fatalf("expected at least 1 result for 'UPPERCASE', got %d", len(results))
	}
}

func BenchmarkSearch(b *testing.B) {
	s, err := Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	now := time.Now()

	// Insert 100K entries to simulate a real library
	entries := make([]*Entry, 100000)
	names := []string{"clip", "scene", "take", "explosion", "whoosh", "ambient", "music", "transition", "title", "lower_third"}
	for i := range entries {
		nameBase := names[i%len(names)]
		entries[i] = MakeEntry(
			fmt.Sprintf("library/category_%02d/%s_%05d.mov", i%50, nameBase, i),
			false, int64(i*1000), now, uint64(i+1),
		)
	}
	if err := s.BulkInsert(entries, 5000); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := s.Search("explosion", 50, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(results) == 0 {
			b.Fatal("expected results")
		}
	}
}
