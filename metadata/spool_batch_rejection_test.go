package metadata

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestBatchDrainCompleteRejectedItemsPreserveMirror(t *testing.T) {
	for _, scenario := range []string{"canceled", "requeued", "path", "source", "size", "punched", "already_done"} {
		t.Run(scenario, func(t *testing.T) {
			store, spool := openStoreWithSpool(t)
			const name = "/Films/old.mov"
			id := seedReadyRow(t, store, spool, name, 4096)
			item := DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/old.mov", Size: 4096, Mtime: time.Unix(1800000000, 0)}
			var err error
			switch scenario {
			case "canceled":
				_, err = spool.DeleteActiveByPath(name)
			case "requeued":
				err = spool.ResetToReady(id)
			case "path":
				item.NFSPath = "/Films/replacement.mov"
			case "source":
				item.SpoolFile += ".replacement"
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
			// Use a replacement with the same inode as the old cache entry. The
			// canceled-row guard must not depend on an inode number changing.
			inode := store.LookupByPath(name).Inode
			if err := store.Delete(name); err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1700000000, 0)
			if err := store.Insert(MakeEntry(item.NFSPath, false, 128, stamp, inode)); err != nil {
				t.Fatal(err)
			}
			before := store.LookupByPath(item.NFSPath).Version
			keep := seedReadyRow(t, store, spool, "/Films/keep.mov", 8192)
			results, err := store.BatchDrainComplete([]DrainCommitItem{
				item,
				{SpoolID: keep, NFSPath: "/Films/keep.mov", SpoolFile: "/spool/files/keep.mov", Size: 8192, Mtime: item.Mtime},
			})
			if err != nil || len(results) != 2 || results[0].Done || !results[1].Done {
				t.Fatalf("results=%+v err=%v", results, err)
			}
			if got := store.LookupByPath(item.NFSPath); got == nil || got.Size != 128 || !got.Mtime.Equal(stamp) || got.Version != before {
				t.Fatalf("rejected item changed RAM replacement: %+v", got)
			}
			if durableSize(t, store, item.NFSPath) != 128 || durableSize(t, store, "/Films/keep.mov") != 8192 {
				t.Fatal("rejected item or accepted sibling has wrong durable size")
			}
			var stampSQL int64
			if err := store.DB().QueryRow(`SELECT mtime FROM entries WHERE path=?`, item.NFSPath).Scan(&stampSQL); err != nil || stampSQL != stamp.Unix() {
				t.Fatalf("replacement timestamp=%d err=%v", stampSQL, err)
			}
			// Reopening the actual database must preserve the replacement too.
			var seq int
			var label, dbPath string
			if err := store.DB().QueryRow(`PRAGMA database_list`).Scan(&seq, &label, &dbPath); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := reopened.LookupByPath(item.NFSPath); got == nil || got.Size != 128 || !got.Mtime.Equal(stamp) {
				t.Fatalf("reopened replacement=%+v", got)
			}
		})
	}
}

func TestBatchDrainCompleteFailureRollsBackEveryItem(t *testing.T) {
	for _, scenario := range []string{"metadata_error", "spool_error", "duplicate_id", "duplicate_path", "invalid_id", "invalid_path", "invalid_size"} {
		t.Run(scenario, func(t *testing.T) {
			store, spool := openStoreWithSpool(t)
			items := make([]DrainCommitItem, 2)
			for i, name := range []string{"/Films/a.mov", "/Films/b.mov"} {
				items[i] = DrainCommitItem{SpoolID: seedReadyRow(t, store, spool, name, 4096), NFSPath: name, SpoolFile: "/spool/files/" + filepath.Base(name), Size: 4096, Mtime: time.Now()}
			}
			var err error
			switch scenario {
			case "metadata_error":
				_, err = store.DB().Exec(`CREATE TRIGGER reject_metadata BEFORE UPDATE ON entries WHEN NEW.path='/Films/b.mov' BEGIN SELECT RAISE(ABORT, 'injected metadata failure'); END`)
			case "spool_error":
				_, err = store.DB().Exec(`CREATE TRIGGER reject_spool BEFORE UPDATE ON spool_entries WHEN NEW.nfs_path='/Films/b.mov' BEGIN SELECT RAISE(ABORT, 'injected spool failure'); END`)
			case "duplicate_id":
				items = append(items, items[0])
			case "duplicate_path":
				other := items[0]
				other.SpoolFile += ".second"
				// Legacy DBs can contain duplicate active rows. Seed that
				// historical shape directly; normal admission now refuses it.
				var result sql.Result
				result, err = store.DB().Exec(`INSERT INTO spool_entries
				 (nfs_path,spool_file,size,drain_state,created_at,updated_at)
				 VALUES (?,?,?,'draining',?,?)`, other.NFSPath, other.SpoolFile, other.Size, time.Now().Unix(), time.Now().Unix())
				if err == nil {
					other.SpoolID, err = result.LastInsertId()
				}
				items = append(items, other)
			case "invalid_id", "invalid_path", "invalid_size":
				other := items[0]
				switch scenario {
				case "invalid_id":
					other.SpoolID = 0
				case "invalid_path":
					other.NFSPath = ""
				case "invalid_size":
					other.Size = -1
				}
				items = append(items, other)
			}
			if err != nil {
				t.Fatal(err)
			}
			if results, err := store.BatchDrainComplete(items); err == nil || results != nil {
				t.Fatalf("failure returned results=%+v err=%v", results, err)
			}
			checkItems := items[:2]
			if scenario == "duplicate_path" {
				checkItems = items
			}
			for _, item := range checkItems {
				row, err := spool.Get(item.SpoolID)
				if err != nil || row.DrainState != DrainDraining {
					t.Fatalf("spool transition escaped rollback: row=%+v err=%v", row, err)
				}
				if got := store.LookupByPath(item.NFSPath); got == nil || got.Size != 0 || durableSize(t, store, item.NFSPath) != 0 {
					t.Fatalf("metadata escaped rollback: %+v", got)
				}
			}
		})
	}
}
