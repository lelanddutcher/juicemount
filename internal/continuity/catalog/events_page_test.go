package catalog

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// largeRenameOperation renames testAsset back and forth with 4000-byte labels
// so each canonical request is close to MaxRequestBytes.
func largeRenameOperation(id string) Operation {
	labels := [2]string{strings.Repeat("a", 4000), strings.Repeat("b", 4000)}
	changes := make([]Change, 120)
	for i := range changes {
		changes[i] = Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: labels[i%2], Label: labels[(i+1)%2]}}
	}
	return testOperation(id, changes...)
}

// A page of large events stops at the request-byte budget instead of holding
// up to MaxPageSize megabyte requests; paging still reaches every event.
func TestCatalogEventsPageStopsAtByteBudget(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, testOperation("create", Change{CreateAsset: &CreateAsset{AssetID: testAsset, Label: strings.Repeat("a", 4000)}}))
	const large = 40
	for i := 0; i < large; i++ {
		applyTest(t, s, largeRenameOperation(fmt.Sprintf("large-%02d", i)))
	}
	_, request, _, err := freezeOperation(largeRenameOperation("large-00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(request) < 900_000 || len(request) > MaxRequestBytes {
		t.Fatalf("fixture request is %d bytes", len(request))
	}
	var after int64
	pages := 0
	for {
		page, err := s.Events(context.Background(), testLibrary, after, MaxPageSize)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		total := 0
		for _, event := range page {
			if event.Receipt.Sequence != after+1 {
				t.Fatalf("page skipped from %d to %d", after, event.Receipt.Sequence)
			}
			after = event.Receipt.Sequence
			_, request, _, err := freezeOperation(event.Operation)
			if err != nil {
				t.Fatal(err)
			}
			total += len(request)
		}
		if total > eventPageBytes {
			t.Fatalf("page %d holds %d request bytes, over the %d budget", pages, total, eventPageBytes)
		}
	}
	if after != large+1 || pages < 3 {
		t.Fatalf("paging reached sequence %d in %d pages", after, pages)
	}
	// Even a budget-sized single event is returned: a short page is not the end.
	page, err := s.Events(context.Background(), testLibrary, 1, 1)
	if err != nil || len(page) != 1 || page[0].Receipt.Sequence != 2 {
		t.Fatalf("single large event page: %d events, %v", len(page), err)
	}
}

// Reads select request/receipt blobs through the same length bounds as Export,
// so an oversized stored blob is refused without being materialized.
func TestCatalogEventReadsDoNotLoadOversizedBlobs(t *testing.T) {
	s, _ := newTestStore(t)
	applyTest(t, s, initialOperation())
	for _, q := range []string{`DROP TRIGGER events_no_update`, `PRAGMA ignore_check_constraints=ON`} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`UPDATE events SET request=zeroblob(64<<20) WHERE operation_id='initial'`); err != nil {
		t.Fatal(err)
	}
	for name, read := range map[string]func() error{
		"GetEvent": func() error { _, err := s.GetEvent(context.Background(), testLibrary, "initial"); return err },
		"Events":   func() error { _, err := s.Events(context.Background(), testLibrary, 0, 10); return err },
	} {
		var before, afterStats runtime.MemStats
		runtime.ReadMemStats(&before)
		err := read()
		runtime.ReadMemStats(&afterStats)
		if !errors.Is(err, ErrFormat) {
			t.Fatalf("%s of oversized request: %v", name, err)
		}
		if allocated := afterStats.TotalAlloc - before.TotalAlloc; allocated > 16<<20 {
			t.Fatalf("%s allocated %d bytes reading an oversized request", name, allocated)
		}
	}
}
