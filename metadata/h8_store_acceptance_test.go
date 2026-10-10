//go:build h8_acceptance

package metadata

import (
	"testing"
	"time"
)

// These release-blocking assertions deliberately stay red until Store owns
// durable incarnations and stale snapshot rejection. Run with:
// go test -tags h8_acceptance ./metadata -run '^TestH8StoreAcceptance' -count=1
func TestH8StoreAcceptanceDelayedSnapshotAfterRecreate(t *testing.T) {
	for _, method := range []string{"Insert", "BulkInsert"} {
		t.Run(method, func(t *testing.T) {
			s := newTestStore(t)
			const filename = "reel.mov"
			const inode = 901
			stamp := time.Unix(1700000000, 0)
			if err := s.Insert(MakeEntry(filename, false, 4096, stamp, inode)); err != nil {
				t.Fatal(err)
			}
			delayed := s.LookupByPath(filename)
			if delayed == nil {
				t.Fatal("initial row missing")
			}

			if err := s.Delete(filename); err != nil {
				t.Fatal(err)
			}
			// Neither inode nor second-resolution mtime proves incarnation.
			if err := s.Insert(MakeEntry(filename, false, 128, stamp, inode)); err != nil {
				t.Fatal(err)
			}
			var err error
			if method == "Insert" {
				err = s.Insert(delayed)
			} else {
				err = s.BulkInsert([]*Entry{delayed}, 500)
			}
			// Either a conflict error or a rejected no-op is acceptable. What
			// must never happen is publication into the replacement incarnation.
			if err != nil {
				t.Logf("delayed snapshot rejected: %v", err)
			}
			got := s.LookupByPath(filename)
			if got == nil || got.Size != 128 {
				t.Errorf("replacement RAM row overwritten by delayed snapshot: got %+v, want size 128", got)
			}
			var durableSize int64
			if err := s.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, filename).Scan(&durableSize); err != nil {
				t.Fatal(err)
			}
			if durableSize != 128 {
				t.Errorf("replacement SQLite row overwritten by delayed snapshot: got size %d, want 128", durableSize)
			}
		})
	}
}
