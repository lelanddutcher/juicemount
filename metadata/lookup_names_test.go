package metadata

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestLookupSiblingPathsIncludesEvictedDurableAndLocalNames(t *testing.T) {
	t.Setenv("JM_DURABLE_SERVE_FALLBACK", "0")
	s, err := OpenWithMaxCacheSize(":memory:", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entries := []*Entry{MakeEntry("d", true, 0, time.Now(), 10)}
	for i, name := range []string{"d/A\u030a", "d/Å", "d/other", "d/last"} {
		entries = append(entries, MakeEntry(name, false, 3, time.Unix(int64(i), 0), uint64(20+i)))
	}
	if err := s.BulkInsert(entries, 32); err != nil {
		t.Fatal(err)
	}
	if !s.cacheEvicted.Load() {
		t.Fatal("fixture did not evict")
	}
	s.InsertToCache(MakeEntry("d/local", false, 4, time.Now(), 40))
	s.mu.Lock()
	s.preparedCreates["d/reserved"] = PreparedCreate{Path: "d/reserved", ParentPath: "d"}
	s.mu.Unlock()
	got, err := s.LookupSiblingPaths("d")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{"d/A\u030a", "d/Å", "d/other", "d/last", "d/local", "d/reserved"}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory=%q, want %q", got, want)
	}
}

func TestLookupSiblingPathsQueryFailureDoesNotUsePartialRAM(t *testing.T) {
	s := newTestStore(t)
	s.InsertToCache(MakeEntry("d/Å", false, 1, time.Now(), 3))
	if _, err := s.DB().Exec("DROP TABLE prepared_creates"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.LookupSiblingPaths("d"); err == nil || got != nil {
		t.Fatalf("incomplete inventory=%q, err=%v", got, err)
	}
}
