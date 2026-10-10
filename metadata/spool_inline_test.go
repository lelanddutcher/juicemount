package metadata

import (
	"fmt"
	"testing"
	"time"
)

func TestDrainCompletePublishesExactSize(t *testing.T) {
	for _, size := range []int64{0, 128, 8192} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store, spool := openStoreWithSpool(t)
			const name = "/Films/clip.mov"
			id := seedReadyRow(t, store, spool, name, size)
			if err := store.SetSizeExact(name, 4096, time.Unix(1700000000, 0)); err != nil {
				t.Fatal(err)
			}
			before := store.LookupByPath(name).Version
			stamp := time.Unix(1800000000, 0)
			result, err := store.DrainComplete(DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/clip.mov", Size: size, Mtime: stamp})
			if err != nil || !result.Done || result.SpoolID != id {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			got := store.LookupByPath(name)
			if got == nil || got.Size != size || !got.Mtime.Equal(stamp) || got.MtimeNsecKnown || got.Version.Incarnation != before.Incarnation || got.Version.Revision != before.Revision+1 {
				t.Fatalf("committed mirror=%+v before=%+v", got, before)
			}
			if durableSize(t, store, name) != size {
				t.Fatal("durable size did not match exact image")
			}
			token, err := store.CaptureMirror(name)
			if err != nil || token != got.Version {
				t.Fatalf("RAM/SQL version disagreement token=%+v err=%v", token, err)
			}
			row, err := spool.Get(id)
			if err != nil || row.DrainState != DrainDone {
				t.Fatalf("row=%+v err=%v", row, err)
			}
		})
	}
}

func TestDrainCompleteRejectedTuplePreservesReplacement(t *testing.T) {
	for _, scenario := range []string{"canceled", "requeued", "source", "path", "size", "punched", "already_done"} {
		t.Run(scenario, func(t *testing.T) {
			store, spool := openStoreWithSpool(t)
			const name = "/Films/clip.mov"
			id := seedReadyRow(t, store, spool, name, 4096)
			item := DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/clip.mov", Size: 4096, Mtime: time.Unix(1800000000, 0)}
			var err error
			switch scenario {
			case "canceled":
				_, err = spool.DeleteActiveByPath(name)
			case "requeued":
				err = spool.ResetToReady(id)
			case "source":
				item.SpoolFile += ".other"
			case "path":
				item.NFSPath += ".other"
			case "size":
				item.Size++
			case "punched":
				_, err = store.DB().Exec(`UPDATE spool_entries SET punched_end=1 WHERE id=?`, id)
			case "already_done":
				_, err = spool.MarkDone(id)
			}
			if err != nil {
				t.Fatal(err)
			}
			inode := store.LookupByPath(name).Inode
			if err := store.Delete(name); err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1700000000, 0)
			if err := store.Insert(MakeEntry(item.NFSPath, false, 128, stamp, inode)); err != nil {
				t.Fatal(err)
			}
			before := store.LookupByPath(item.NFSPath).Version
			result, err := store.DrainComplete(item)
			if err != nil || result.Done || result.SpoolID != id {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			got := store.LookupByPath(item.NFSPath)
			if got == nil || got.Size != 128 || got.Version != before || !got.Mtime.Equal(stamp) || durableSize(t, store, item.NFSPath) != 128 {
				t.Fatalf("replacement changed: %+v", got)
			}
		})
	}
}

func TestDrainCompleteUncertainMirrorRollsBackRow(t *testing.T) {
	for _, scenario := range []string{"missing_mirror", "metadata_error", "spool_error"} {
		t.Run(scenario, func(t *testing.T) {
			store, spool := openStoreWithSpool(t)
			const name = "/Films/clip.mov"
			id := seedReadyRow(t, store, spool, name, 4096)
			var err error
			switch scenario {
			case "missing_mirror":
				err = store.Delete(name)
			case "metadata_error":
				_, err = store.DB().Exec(`CREATE TRIGGER reject_inline BEFORE UPDATE ON entries BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END`)
			case "spool_error":
				_, err = store.DB().Exec(`CREATE TRIGGER reject_inline BEFORE UPDATE ON spool_entries BEGIN SELECT RAISE(ABORT, 'injected spool failure'); END`)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.DrainComplete(DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/clip.mov", Size: 4096, Mtime: time.Now()})
			if err == nil || result.Done {
				t.Fatalf("uncertain completion=%+v err=%v", result, err)
			}
			row, err := spool.Get(id)
			if err != nil || row.DrainState != DrainDraining {
				t.Fatalf("row escaped rollback=%+v err=%v", row, err)
			}
			got := store.LookupByPath(name)
			if scenario == "missing_mirror" {
				if got != nil {
					t.Fatal("missing mirror resurrected")
				}
			} else if got == nil || got.Size != 0 || durableSize(t, store, name) != 0 {
				t.Fatalf("mirror escaped rollback=%+v", got)
			}
		})
	}
}
