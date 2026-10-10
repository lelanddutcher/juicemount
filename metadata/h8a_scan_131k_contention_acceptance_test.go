//go:build h8aacceptance

package metadata

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const h8aScan131kEntries = 131000

func TestH8AScan131kContentionAndConvergence(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	paths := make([]string, h8aScan131kEntries)
	entries := make([]*Entry, h8aScan131kEntries)
	stamp := time.Unix(1700000000, 0)
	for i := range paths {
		paths[i] = fmt.Sprintf("media/%06d.mov", i)
		entries[i] = MakeEntry(paths[i], false, 4096, stamp, uint64(i+10))
		entries[i].LocalOnly = true
	}
	seedStart := time.Now()
	if err := store.BulkInsert(entries, scanMutationChunk); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d entries in %s", len(entries), time.Since(seedStart))

	upserts := make([]*Entry, len(entries))
	for i, entry := range entries {
		upserts[i] = MakeEntry(entry.Path, false, entry.Size+1024, stamp.Add(time.Second), entry.Inode+1)
		upserts[i].LocalOnly = true
	}
	measurements := map[string][]time.Duration{}

	// Each phase commits one 500-row batch, then holds a stale observation while
	// foreground writers run. After the expected epoch conflict, it resumes the
	// unaffected remainder with a fresh token and verifies full convergence.
	run := func(name string, batch func([]int, StoreEpoch) (StoreEpoch, error)) {
		t.Helper()
		epoch, err := store.CaptureStoreEpoch()
		if err != nil {
			t.Fatal(err)
		}
		first := make([]int, scanMutationChunk)
		for i := range first {
			first[i] = i
		}
		start := time.Now()
		epoch, err = batch(first, epoch)
		measurements[name] = append(measurements[name], time.Since(start))
		if err != nil {
			t.Fatalf("%s first batch: %v", name, err)
		}

		var writes atomic.Int64
		var wg sync.WaitGroup
		for worker := 0; worker < 2; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					path := fmt.Sprintf("foreground/%s-%d-%03d.mov", name, worker, i)
					e := MakeEntry(path, false, int64(i+1), stamp, uint64(9000000+worker*100+i))
					if err := store.Insert(e); err != nil {
						t.Errorf("foreground insert during %s: %v", name, err)
						return
					}
					if err := store.UpdateSize(path, int64(i+1024), stamp.Add(time.Second)); err != nil {
						t.Errorf("foreground UpdateSize during %s: %v", name, err)
						return
					}
					writes.Add(1)
				}
			}(worker)
		}
		for writes.Load() == 0 {
			time.Sleep(time.Microsecond)
		}
		for writes.Load() < 100 {
			start = time.Now()
			_, err := batch(indexRange(scanMutationChunk, 2*scanMutationChunk), epoch)
			measurements[name+"_conflict"] = append(measurements[name+"_conflict"], time.Since(start))
			if err != ErrMirrorConflict {
				t.Fatalf("%s stale batch = %v, want mirror conflict", name, err)
			}
		}
		wg.Wait()

		epoch, err = store.CaptureStoreEpoch()
		if err != nil {
			t.Fatal(err)
		}
		for offset := scanMutationChunk; offset < len(paths); offset += scanMutationChunk {
			end := offset + scanMutationChunk
			if end > len(paths) {
				end = len(paths)
			}
			start = time.Now()
			epoch, err = batch(indexRange(offset, end), epoch)
			measurements[name] = append(measurements[name], time.Since(start))
			if err != nil {
				t.Fatalf("%s batch [%d:%d]: %v", name, offset, end, err)
			}
		}
	}

	run("upsert", func(indices []int, epoch StoreEpoch) (StoreEpoch, error) {
		batch := make([]*Entry, len(indices))
		for i, index := range indices {
			batch[i] = upserts[index]
		}
		return store.ScanUpsertBatch(batch, epoch, nil)
	})
	run("clear", func(indices []int, epoch StoreEpoch) (StoreEpoch, error) {
		batch := make([]string, len(indices))
		for i, index := range indices {
			batch[i] = paths[index]
		}
		return store.ScanClearLocalOnlyBatch(batch, epoch)
	})
	run("prune", func(indices []int, epoch StoreEpoch) (StoreEpoch, error) {
		batch := make([]string, len(indices))
		for i, index := range indices {
			batch[i] = paths[index]
		}
		return store.ScanDeletePathsBatch(batch, epoch)
	})

	if got := store.LookupByPath(paths[0]); got != nil {
		t.Fatalf("pruned scan entry still present: %+v", got)
	}
	for worker := 0; worker < 2; worker++ {
		path := fmt.Sprintf("foreground/prune-%d-049.mov", worker)
		if got := store.LookupByPath(path); got == nil || got.Size != 1073 {
			t.Fatalf("foreground write did not survive scan prune for %q: %+v", path, got)
		}
	}
	for phase, samples := range measurements {
		t.Logf("%s batches=%d p50=%s p99=%s max=%s", phase, len(samples), percentileDuration(samples, 50), percentileDuration(samples, 99), percentileDuration(samples, 100))
	}
}

func indexRange(start, end int) []int {
	indices := make([]int, end-start)
	for i := range indices {
		indices[i] = start + i
	}
	return indices
}

func percentileDuration(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (percentile*len(sorted)+99)/100 - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}
