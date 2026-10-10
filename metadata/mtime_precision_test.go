package metadata

import (
	"context"
	"database/sql"
	"encoding/binary"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReconcilePreservesJuiceFSNanosecondMtime(t *testing.T) {
	const sec = int64(1779992968)
	const nsec = uint32(504304000)
	attr := encodeInodeAttr(sec, 4096)
	// JuiceFS v1.3.1 Attr.Marshal: seconds at23, nanoseconds at31.
	binary.BigEndian.PutUint32(attr[31:35], nsec)
	child := dirChild{inode: 227634, childPath: "video/._clip.mp4", mode: fs.FileMode(0644)}
	entries, _ := buildReconcileChildEntries([]dirChild{child}, map[uint64][]byte{child.inode: attr}, func(string) *Entry { return nil })
	if len(entries) != 1 {
		t.Fatalf("entries=%d", len(entries))
	}
	want := time.Unix(sec, int64(nsec))
	if !entries[0].Mtime.Equal(want) {
		t.Fatalf("production reconcile mtime=%s want=%s", entries[0].Mtime.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestReconcileDetectsSameSecondMetadataReplacement(t *testing.T) {
	old := MakeEntry("video/._clip.mp4", false, 4096, time.Unix(1779992968, 100), 227634)
	old.MtimeNsecKnown = true
	attr := encodeInodeAttr(old.Mtime.Unix(), old.Size)
	binary.BigEndian.PutUint32(attr[31:35], 200)
	child := dirChild{inode: old.Inode, childPath: old.Path, mode: old.Mode}
	entries, _ := buildReconcileChildEntries([]dirChild{child}, map[uint64][]byte{old.Inode: attr}, func(string) *Entry { return old })
	if len(entries) != 1 {
		t.Fatalf("same-second content metadata change produced %d updates; want1", len(entries))
	}
}

func TestPreciseSCANLuaPreservesNanosecondsAndColonName(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	ctx := context.Background()
	const name = "._clip:one.mov"
	attr := encodeInodeAttr(1779992968, 4096)
	binary.BigEndian.PutUint32(attr[31:35], 999999999)
	if err := rdb.HSet(ctx, "d1", name, string(encodeDirChild(42, 1))).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "i42", string(attr), 0).Err(); err != nil {
		t.Fatal(err)
	}
	rows, err := rdb.Eval(ctx, luaScanBatch, nil, "0", "500").StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%v", rows)
	}
	got, err := parseScanRawEntry(rows[1])
	if err != nil {
		t.Fatal(err)
	}
	if got.mtime != 1779992968 || got.mtimeNsec != 999999999 || !got.mtimeNsecKnown || got.name != name {
		t.Fatalf("precision/name lost: %+v", got)
	}
	legacy, err := parseScanRawEntry("1:1:1779992968:4096:42:1:._clip:one.mov")
	if err != nil || legacy.mtimeNsecKnown {
		t.Fatalf("legacy row minted precision: %+v %v", legacy, err)
	}
	for _, raw := range []string{"v2:1:1:1779992968:1000000000:4096:42:1:x", "v2:1:1:1779992968:-1:4096:42:1:x"} {
		if _, err := parseScanRawEntry(raw); err == nil {
			t.Fatalf("invalid fraction accepted %q", raw)
		}
	}
}

func TestMirrorNanosecondMigrationKeepsLegacyUnknown(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO entries(path,name,parent_path,is_dir,size,mtime,inode,mode) VALUES('._legacy','._legacy','.',0,4,1779992968,42,420)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	db.Close()
	store, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	legacy := store.LookupByPath("._legacy")
	if legacy == nil || legacy.MtimeNsecKnown {
		store.Close()
		t.Fatal("migration manufactured exact zero nanoseconds")
	}
	if _, ok := store.LookupCachedFileIdentity(legacy.Path); ok {
		store.Close()
		t.Fatal("legacy row authorized strong proof")
	}
	// An observed exact zero is different from unknown and survives restart.
	exact := MakeEntry("._exact", false, 4, time.Unix(1779992968, 0), 43)
	exact.MtimeNsecKnown = true
	if err := store.Insert(exact); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.Close()
	store, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got := store.LookupByPath(exact.Path)
	if got == nil || !got.MtimeNsecKnown || got.Mtime.Nanosecond() != 0 {
		t.Fatal("exact-zero proof lost on reopen")
	}
	if _, ok := store.LookupCachedFileIdentity(exact.Path); !ok {
		t.Fatal("observed exact-zero row ineligible")
	}
}

func TestMirrorNanosecondAllInsertAndReadPaths(t *testing.T) {
	for _, method := range []string{"insert", "bulk-incremental", "bulk-rebuild", "absent-incremental", "absent-rebuild"} {
		t.Run(method, func(t *testing.T) {
			s := newTestStore(t)
			e := MakeEntry("._precise", false, 4, time.Unix(1779992968, 504304000), 42)
			e.MtimeNsecKnown = true
			old := FTSFullRebuildThreshold
			if strings.HasSuffix(method, "rebuild") {
				FTSFullRebuildThreshold = 0
			}
			defer func() { FTSFullRebuildThreshold = old }()
			var err error
			switch method {
			case "insert":
				err = s.Insert(e)
			case "bulk-incremental", "bulk-rebuild":
				err = s.BulkInsert([]*Entry{e}, 500)
			default:
				err = s.BulkInsertAbsent([]*Entry{e}, 500)
			}
			if err != nil {
				t.Fatal(err)
			}
			assert := func(label string, got *Entry) {
				t.Helper()
				if got == nil || !got.MtimeNsecKnown || !got.Mtime.Equal(e.Mtime) {
					t.Fatalf("%s lost precise timestamp: %+v", label, got)
				}
			}
			assert("RAM", s.LookupByPath(e.Path))
			assert("SQL path", s.lookupByPathSQLite(e.Path))
			assert("SQL inode", s.lookupByInodeSQLite(e.Inode))
			rows, _, err := s.ListChildrenPage(".", "", 10)
			if err != nil || len(rows) != 1 {
				t.Fatalf("page=%d,%v", len(rows), err)
			}
			assert("page", rows[0])
			if err := s.RebuildFTS(); err != nil {
				t.Fatal(err)
			}
			results, err := s.Search("precise", 10, "")
			if err != nil || len(results) != 1 {
				t.Fatalf("search=%d,%v", len(results), err)
			}
			assert("search", results[0].Entry)
		})
	}
}

func TestMirrorGenericTimeMutationClearsPrecision(t *testing.T) {
	for _, method := range []string{"size-max", "size-exact", "size-cas"} {
		t.Run(method, func(t *testing.T) {
			s := newTestStore(t)
			e := MakeEntry("._precise", false, 4, time.Unix(1779992968, 100), 42)
			e.MtimeNsecKnown = true
			if err := s.Insert(e); err != nil {
				t.Fatal(err)
			}
			var err error
			switch method {
			case "size-max":
				err = s.UpdateSize(e.Path, 5, time.Unix(1779992969, 0))
			case "size-exact":
				err = s.SetSizeExact(e.Path, 5, time.Unix(1779992969, 0))
			case "size-cas":
				_, err = s.SetSizeExactExpected(s.LookupByPath(e.Path).Version, 5, time.Unix(1779992969, 0))
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, got := range []*Entry{s.LookupByPath(e.Path), s.lookupByPathSQLite(e.Path)} {
				if got == nil || got.MtimeNsecKnown {
					t.Fatal("generic mutation retained old exact fraction")
				}
			}
		})
	}
}

// Drive the production Lua, diff, and SQL publication together. A
// same-second replacement must invalidate the prior observation; a no-op must not.
func TestPreciseSCANPublishesSameSecondAndEpochTimes(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	ctx := context.Background()
	rc := newLiveRC(t, rdb)
	const name = "._clip.mov"
	if err := rdb.HSet(ctx, "d1", name, string(encodeDirChild(42, 1))).Err(); err != nil {
		t.Fatal(err)
	}
	var previous MirrorToken
	for _, stamp := range []time.Time{time.Unix(1779992968, 100), time.Unix(1779992968, 200), time.Unix(0, 300)} {
		attr := encodeInodeAttr(stamp.Unix(), 4096)
		binary.BigEndian.PutUint32(attr[31:35], uint32(stamp.Nanosecond()))
		if err := rdb.Set(ctx, "i42", string(attr), 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rc.SyncOnce(); err != nil {
			t.Fatal(err)
		}
		got := rc.store.LookupByPath(name)
		if got == nil || !got.MtimeNsecKnown || !got.Mtime.Equal(stamp) {
			t.Fatalf("SCAN lost exact time: %+v want %v", got, stamp)
		}
		if got.Version == previous {
			t.Fatal("same-second replacement retained prior version")
		}
		previous = got.Version
		if err := rc.SyncOnce(); err != nil {
			t.Fatal(err)
		}
		if got := rc.store.LookupByPath(name); got.Version != previous {
			t.Fatal("no-op SCAN rewrote exact observation")
		}
	}
}

func TestMirrorCompletionClearsPrecision(t *testing.T) {
	for _, method := range []string{"online", "bound-spool", "batch-spool"} {
		t.Run(method, func(t *testing.T) {
			s, sp := openStoreWithSpool(t)
			e := MakeEntry("reel.mov", false, 128, time.Unix(1779992968, 100), 901)
			e.MtimeNsecKnown = true
			if err := s.Insert(e); err != nil {
				t.Fatal(err)
			}
			v, err := s.CaptureMirror(e.Path)
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1779992969, 0)
			if method == "online" {
				m, err := s.BeginOnlineMutation(v, onlineTestMutation(e.Path, 128, 64))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.CompleteOnlineMutation(m, stamp); err != nil {
					t.Fatal(err)
				}
			} else {
				spoolFile := filepath.Join(t.TempDir(), "spool")
				var id int64
				if method == "bound-spool" {
					id, err = s.InsertBoundSpool(v, spoolFile)
				} else {
					id, err = sp.Insert(e.Path, spoolFile)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := sp.MarkReady(id, 256, nil); err != nil {
					t.Fatal(err)
				}
				if ok, err := sp.MarkDraining(id); err != nil || !ok {
					t.Fatalf("claim=%v,%v", ok, err)
				}
				row, err := sp.Get(id)
				if err != nil {
					t.Fatal(err)
				}
				if method == "bound-spool" {
					if err := s.BeginSpoolPublish(row, v, filepath.Join(t.TempDir(), "stage")); err != nil {
						t.Fatal(err)
					}
					if err := s.CompleteSpool(row, v, stamp); err != nil {
						t.Fatal(err)
					}
				} else {
					results, err := s.BatchDrainComplete([]DrainCommitItem{{SpoolID: id, NFSPath: e.Path, SpoolFile: spoolFile, Size: 256, Mtime: stamp}})
					if err != nil || len(results) != 1 || !results[0].Done {
						t.Fatalf("batch=%+v,%v", results, err)
					}
				}
			}
			for _, got := range []*Entry{s.LookupByPath(e.Path), s.lookupByPathSQLite(e.Path)} {
				if got == nil || got.MtimeNsecKnown {
					t.Fatal("completion retained old timestamp proof")
				}
			}
			if _, ok := s.LookupCachedFileIdentity(e.Path); ok {
				t.Fatal("completion authorized strong cache with generic time")
			}
		})
	}
}
