package metadata

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/juicedata/juicefs/pkg/slicegraph"
)

type replacementFixture struct {
	store   *Store
	spool   *SpoolStore
	dbPath  string
	file    string
	body    []byte
	w       SpoolReplacementWitness
	history [2]int64
}

func newReplacementFixture(t *testing.T, ready bool) *replacementFixture {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "mirror.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(s.DB())
	s.SetSpoolStore(spool)

	oldSize := int64(slicegraph.ChunkSize + 12)
	parent := cachePathTestEntry("a", true, 11, 1700000000)
	dest := cachePathTestEntry("a/clip.mov", false, 22, 1700000001)
	dest.Size = oldSize
	localParent := cachePathTestEntry("work", true, 31, 1700000002)
	localParent.LocalOnly = true
	source := cachePathTestEntry("work/local.mov", false, 32, 1700000003)
	source.LocalOnly = true
	source.Size = 8
	for _, e := range []*Entry{parent, dest, localParent, source} {
		if err := s.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := OpenCachePathEligibility(s)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := gate.CaptureCachePath(dest.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.ConfirmCachePath(testCachePathBinding, observed, cachePathTestProof(observed, testCachePathBinding.FormatUUID)); err != nil {
		t.Fatal(err)
	}
	id, ok := s.LookupCachedFileIdentity(dest.Path)
	if !ok {
		t.Fatal("missing destination identity")
	}
	destination, ok := gate.SnapshotEligibleCachePath(testCachePathBinding, id)
	if !ok {
		t.Fatal("destination not qualified in RAM")
	}
	parents, err := s.CaptureSpoolRenameParents(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.CaptureMirror(source.Path)
	if err != nil {
		t.Fatal(err)
	}

	body := []byte("new edit")
	file := filepath.Join(dir, "local.spool")
	if err := os.WriteFile(file, body, 0600); err != nil {
		t.Fatal(err)
	}
	// Old source and destination rows are inert history. Their retirement
	// must occur in the same transaction as the current writer's move.
	oldSource, err := spool.Insert(source.Path, filepath.Join(dir, "old-source"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec("UPDATE spool_entries SET drain_state='done' WHERE id=?", oldSource); err != nil {
		t.Fatal(err)
	}
	oldDest, err := spool.Insert(dest.Path, filepath.Join(dir, "old-dest"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec("UPDATE spool_entries SET drain_state='done' WHERE id=?", oldDest); err != nil {
		t.Fatal(err)
	}
	rowID, err := spool.InsertWithBase(source.Path, file, SpoolBase{Kind: SpoolBaseAbsent}, 7)
	if err != nil {
		t.Fatal(err)
	}
	imageHash := sha256.Sum256(body)
	if ready {
		if err := spool.MarkReady(rowID, int64(len(body)), imageHash[:]); err != nil {
			t.Fatal(err)
		}
	}
	fileInfo, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	graph := slicegraph.Graph{
		Version: slicegraph.Version, Length: uint64(oldSize),
		Chunks: []slicegraph.Chunk{
			{Index: 0, Extents: []slicegraph.Extent{{Pos: 4096, SliceID: 101, Size: 8192, Len: 8192}}},
			{Index: 1, Extents: []slicegraph.Extent{{Pos: 4, SliceID: 202, Size: 8, Len: 8}}},
		},
	}
	encoded, err := graph.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	opID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	return &replacementFixture{
		store: s, spool: spool, dbPath: dbPath, file: file, body: body, history: [2]int64{oldSource, oldDest},
		w: SpoolReplacementWitness{
			Version: 1, OperationID: opID, SourceRowID: rowID, SourceFile: file,
			SourceIdentity: "private-local-file", Source: token, SourceParents: parents,
			Image:              SpoolReplacementImage{Size: int64(len(body)), MtimeNS: fileInfo.ModTime().UnixNano(), SHA256: imageHash[:]},
			CurrentDestination: destination.Chain[len(destination.Chain)-1].Token,
			Destination:        destination, Binding: testCachePathBinding,
			Graph: encoded, GraphDigest: hex.EncodeToString(digest[:]),
			SourceEpoch: 7, AcceptedEpoch: 19,
		},
	}
}

type replacementSQLState struct {
	path, state                        string
	size                               int64
	sha                                []byte
	baseIno, baseCtime, epoch, retired int64
	witness                            []byte
	history                            [2]int64
	source, destination                *Entry
}

func (f *replacementFixture) sqlState(t *testing.T) replacementSQLState {
	t.Helper()
	var st replacementSQLState
	if err := f.store.DB().QueryRow(`SELECT nfs_path,drain_state,size,sha256,base_ino,base_ctime_ns,accepted_epoch,retired_by,replacement_witness FROM spool_entries WHERE id=?`, f.w.SourceRowID).
		Scan(&st.path, &st.state, &st.size, &st.sha, &st.baseIno, &st.baseCtime, &st.epoch, &st.retired, &st.witness); err != nil {
		t.Fatal(err)
	}
	for i, id := range f.history {
		if err := f.store.DB().QueryRow("SELECT retired_by FROM spool_entries WHERE id=?", id).Scan(&st.history[i]); err != nil {
			t.Fatal(err)
		}
	}
	st.source, _ = scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, f.w.Source.Path))
	st.destination, _ = scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, f.w.Destination.Path))
	return st
}

func (f *replacementFixture) assertUntouched(t *testing.T, before replacementSQLState, sourceRAM, destRAM *Entry) {
	t.Helper()
	after := f.sqlState(t)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("transaction changed durable state\nbefore=%+v\nafter=%+v", before, after)
	}
	if !sameReplacementEntry(sourceRAM, f.store.LookupByPath(f.w.Source.Path)) || !sameReplacementEntry(destRAM, f.store.LookupByPath(f.w.Destination.Path)) {
		t.Fatal("rejected replacement changed RAM namespace")
	}
	actual, err := os.ReadFile(f.file)
	if err != nil || !bytes.Equal(actual, f.body) {
		t.Fatalf("source bytes lost: %q, %v", actual, err)
	}
}

func TestAcceptSpoolReplacementWritingAndReadyDurableRestart(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "writing"
		if ready {
			name = "ready"
		}
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t, ready)
			before := f.sqlState(t)
			if got, err := f.spool.ReplacementWitness(&SpoolRow{ID: f.w.SourceRowID, NFSPath: before.path, SpoolFile: f.file}); err != nil || got != nil {
				t.Fatalf("unexpected preaccept witness %+v: %v", got, err)
			}
			checks := 0
			moved, err := f.store.AcceptSpoolReplacement(f.w, func() error {
				checks++
				actual, err := os.ReadFile(f.file)
				if err != nil || !bytes.Equal(actual, f.body) {
					return errors.New("source bytes changed")
				}
				return nil
			})
			if err != nil || moved == nil || checks != 2 {
				t.Fatalf("accept moved=%+v checks=%d err=%v", moved, checks, err)
			}
			if moved.Path != f.w.Destination.Path || moved.Inode != before.source.Inode ||
				moved.Version.Incarnation != f.w.Source.Incarnation || moved.Version.Revision != f.w.Source.Revision+1 {
				t.Fatalf("wrong moved mirror: %+v", moved)
			}
			after := f.sqlState(t)
			if after.path != f.w.Destination.Path || after.state != string(DrainReady) || after.size != f.w.Image.Size || !bytes.Equal(after.sha, f.w.Image.SHA256) || after.baseIno != spoolBaseInoAbsent ||
				after.baseCtime != spoolBaseCtimeAtomicConflict || after.epoch != int64(f.w.AcceptedEpoch) ||
				after.source != nil || !sameReplacementEntry(after.destination, moved) ||
				moved.Size != f.w.Image.Size || moved.Mtime.UnixNano() != f.w.Image.MtimeNS || !moved.MtimeNsecKnown ||
				after.history != [2]int64{f.w.SourceRowID, f.w.SourceRowID} {
				t.Fatalf("incomplete durable move: %+v", after)
			}
			row, err := f.spool.Get(f.w.SourceRowID)
			if err != nil {
				t.Fatal(err)
			}
			witness, err := f.spool.ReplacementWitness(row)
			if err != nil || !reflect.DeepEqual(witness, &f.w) {
				t.Fatalf("witness %+v err=%v", witness, err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(f.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := InitSpoolSchema(reopened.DB()); err != nil {
				t.Fatal(err)
			}
			reopenedSpool := NewSpoolStore(reopened.DB())
			reopened.SetSpoolStore(reopenedSpool)
			reopenedRow, err := reopenedSpool.Get(f.w.SourceRowID)
			if err != nil {
				t.Fatal(err)
			}
			reopenedWitness, err := reopenedSpool.ReplacementWitness(reopenedRow)
			if err != nil || !reflect.DeepEqual(reopenedWitness, &f.w) ||
				reopened.LookupByPath(f.w.Source.Path) != nil ||
				!sameReplacementEntry(reopened.LookupByPath(f.w.Destination.Path), moved) {
				t.Fatalf("reopen witness=%+v mirror=%+v err=%v", reopenedWitness, reopened.LookupByPath(f.w.Destination.Path), err)
			}
		})
	}
}

func TestAcceptSpoolReplacementRollsBackEverySQLCheckpoint(t *testing.T) {
	cases := []struct{ name, trigger string }{
		{"row-abort", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF nfs_path ON spool_entries WHEN OLD.nfs_path='work/local.mov' BEGIN SELECT RAISE(ABORT,'row'); END`},
		{"row-ignore", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF nfs_path ON spool_entries WHEN OLD.nfs_path='work/local.mov' BEGIN SELECT RAISE(IGNORE); END`},
		{"history-abort", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF retired_by ON spool_entries WHEN OLD.retired_by=0 BEGIN SELECT RAISE(ABORT,'history'); END`},
		{"history-ignore", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF retired_by ON spool_entries WHEN OLD.retired_by=0 BEGIN SELECT RAISE(IGNORE); END`},
		{"mirror-delete-abort", `CREATE TRIGGER reject_replacement BEFORE DELETE ON entries WHEN OLD.path='a/clip.mov' BEGIN SELECT RAISE(ABORT,'delete'); END`},
		{"mirror-delete-ignore", `CREATE TRIGGER reject_replacement BEFORE DELETE ON entries WHEN OLD.path='a/clip.mov' BEGIN SELECT RAISE(IGNORE); END`},
		{"mirror-upsert-abort", `CREATE TRIGGER reject_replacement BEFORE INSERT ON entries WHEN NEW.path='a/clip.mov' BEGIN SELECT RAISE(ABORT,'mirror'); END`},
		{"mirror-upsert-ignore", `CREATE TRIGGER reject_replacement BEFORE INSERT ON entries WHEN NEW.path='a/clip.mov' BEGIN SELECT RAISE(IGNORE); END`},
		{"identity-abort", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF incarnation ON entries WHEN OLD.path='a/clip.mov' BEGIN SELECT RAISE(ABORT,'identity'); END`},
		{"identity-ignore", `CREATE TRIGGER reject_replacement BEFORE UPDATE OF incarnation ON entries WHEN OLD.path='a/clip.mov' BEGIN SELECT RAISE(IGNORE); END`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			if _, err := f.store.DB().Exec(tc.trigger); err != nil {
				t.Fatal(err)
			}
			before := f.sqlState(t)
			sourceRAM, destRAM := f.store.LookupByPath(f.w.Source.Path), f.store.LookupByPath(f.w.Destination.Path)
			checks := 0
			if moved, err := f.store.AcceptSpoolReplacement(f.w, func() error { checks++; return nil }); err == nil || moved != nil || checks != 1 {
				t.Fatalf("rejected SQL checkpoint accepted: moved=%+v err=%v", moved, err)
			}
			f.assertUntouched(t, before, sourceRAM, destRAM)
		})
	}
}

func TestAcceptSpoolReplacementRejectsChangedEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, f *replacementFixture)
	}{
		{"atomic-conflict-base", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET base_ctime_ns=-4 WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"older-live-source", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET drain_state='failed' WHERE id=?", f.history[0]); err != nil {
				t.Fatal(err)
			}
		}},
		{"detached-publishing-destination", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation,phase,publish_path) VALUES(999,'detached','publishing',?)`, f.w.Destination.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"detached-birth-source", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation,birth_path,birth_id) VALUES(999,'detached',?,'retained')`, f.w.Source.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"birth-bound-source", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation,birth_path,birth_id) VALUES(?,'bound',?,'retained')`, f.w.SourceRowID, f.w.Source.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"empty-witness", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET replacement_witness=X'' WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown-base", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET base_ino=0 WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"source-sql", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE entries SET size=size+1 WHERE path=?", f.w.Source.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"destination-sql", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE entries SET inode=inode+1 WHERE path=?", f.w.Destination.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"source-ram", func(t *testing.T, f *replacementFixture) {
			f.store.mu.Lock()
			f.store.pathCache[f.w.Source.Path].Size++
			f.store.mu.Unlock()
		}},
		{"destination-ram", func(t *testing.T, f *replacementFixture) {
			f.store.mu.Lock()
			f.store.pathCache[f.w.Destination.Path].Size++
			f.store.mu.Unlock()
		}},
		{"ancestor-inode", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE entries SET inode=inode+1 WHERE path='work'"); err != nil {
				t.Fatal(err)
			}
		}},
		{"binding", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE cache_path_eligibility SET backend_digest='changed' WHERE path=?", f.w.Destination.Path); err != nil {
				t.Fatal(err)
			}
		}},
		{"failed-source", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET drain_state='failed' WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"clone-hold", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET clone_hold='other Mac' WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"qualification-hold", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET qualification_hold='unverified' WHERE id=?", f.w.SourceRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"active-destination", func(t *testing.T, f *replacementFixture) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET drain_state='failed' WHERE id=?", f.history[1]); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong-binding", func(t *testing.T, f *replacementFixture) {
			f.w.Binding.Digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
		{"wrong-epoch", func(t *testing.T, f *replacementFixture) { f.w.SourceEpoch++ }},
		{"negative-image-size", func(t *testing.T, f *replacementFixture) { f.w.Image.Size = -1 }},
		{"short-image-digest", func(t *testing.T, f *replacementFixture) { f.w.Image.SHA256 = []byte{1} }},
		{"ready-image-differs", func(t *testing.T, f *replacementFixture) {
			if err := f.spool.MarkReady(f.w.SourceRowID, f.w.Image.Size, f.w.Image.SHA256); err != nil {
				t.Fatal(err)
			}
			f.w.Image.SHA256[0] ^= 1
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			tc.mutate(t, f)
			before := f.sqlState(t)
			sourceRAM, destRAM := f.store.LookupByPath(f.w.Source.Path), f.store.LookupByPath(f.w.Destination.Path)
			checks := 0
			if moved, err := f.store.AcceptSpoolReplacement(f.w, func() error { checks++; return nil }); !errors.Is(err, ErrMirrorConflict) || moved != nil || checks != 0 {
				t.Fatalf("stale evidence accepted: moved=%+v checks=%d err=%v", moved, checks, err)
			}
			f.assertUntouched(t, before, sourceRAM, destRAM)
		})
	}
}

func TestAcceptSpoolReplacementPostcommitSourceCheckLeavesDurableCarrier(t *testing.T) {
	f := newReplacementFixture(t, false)
	oldSource, oldDest := f.store.LookupByPath(f.w.Source.Path), f.store.LookupByPath(f.w.Destination.Path)
	checks := 0
	moved, err := f.store.AcceptSpoolReplacement(f.w, func() error {
		checks++
		if checks == 2 {
			return errors.New("source descriptor changed after commit")
		}
		return nil
	})
	var pending *PendingSpoolReplacement
	if moved != nil || !errors.As(err, &pending) || checks != 2 {
		t.Fatalf("postcommit result moved=%+v checks=%d err=%v", moved, checks, err)
	}
	if !sameReplacementEntry(oldSource, f.store.LookupByPath(f.w.Source.Path)) ||
		!sameReplacementEntry(oldDest, f.store.LookupByPath(f.w.Destination.Path)) {
		t.Fatal("uncertain result published RAM state")
	}
	row, err := f.spool.Get(f.w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	if row.NFSPath != f.w.Destination.Path {
		t.Fatalf("carrier path=%q", row.NFSPath)
	}
	if witness, err := f.spool.ReplacementWitness(row); err != nil || !reflect.DeepEqual(witness, &f.w) {
		t.Fatalf("carrier witness=%+v err=%v", witness, err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.LookupByPath(f.w.Source.Path) != nil || reopened.LookupByPath(f.w.Destination.Path).Inode != oldSource.Inode {
		t.Fatal("committed mirror did not survive reopen")
	}
}

func TestReplacementWitnessDistinguishesNullFromMalformedEmptyBlob(t *testing.T) {
	f := newReplacementFixture(t, false)
	row, err := f.spool.Get(f.w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	if witness, err := f.spool.ReplacementWitness(row); err != nil || witness != nil {
		t.Fatalf("NULL witness=%+v err=%v", witness, err)
	}
	if _, err := f.store.DB().Exec("UPDATE spool_entries SET replacement_witness=X'' WHERE id=?", row.ID); err != nil {
		t.Fatal(err)
	}
	if witness, err := f.spool.ReplacementWitness(row); err == nil || witness != nil {
		t.Fatalf("malformed empty witness=%+v err=%v", witness, err)
	}
}

func successorReplacement(t *testing.T, f *replacementFixture) SpoolReplacementWitness {
	t.Helper()
	previous := f.w
	if _, err := f.store.AcceptSpoolReplacement(previous, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	priorRow, err := f.spool.Get(previous.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := f.spool.ReplacementWitness(priorRow)
	if err != nil || prior == nil {
		t.Fatalf("prior witness %+v: %v", prior, err)
	}
	source := cachePathTestEntry("work/second.mov", false, 41, 1700000004)
	source.LocalOnly = true
	source.Size = 17
	if err := f.store.Insert(source); err != nil {
		t.Fatal(err)
	}
	secondFile := filepath.Join(filepath.Dir(f.file), "second.spool")
	secondBody := []byte("second saved body")
	if err := os.WriteFile(secondFile, secondBody, 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(secondFile)
	if err != nil {
		t.Fatal(err)
	}
	id, err := f.spool.InsertWithBase(source.Path, secondFile, SpoolBase{Kind: SpoolBaseAbsent}, 23)
	if err != nil {
		t.Fatal(err)
	}
	token, err := f.store.CaptureMirror(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	parents, err := f.store.CaptureSpoolRenameParents(source.Path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.store.CaptureMirror(previous.Destination.Path)
	if err != nil {
		t.Fatal(err)
	}
	opID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(secondBody)
	w := *prior
	w.OperationID, w.SourceRowID, w.SourceFile, w.SourceIdentity = opID, id, secondFile, "second-private-image"
	w.Source, w.SourceParents, w.SourceEpoch = token, parents, 23
	w.Image = SpoolReplacementImage{Size: int64(len(secondBody)), MtimeNS: info.ModTime().UnixNano(), SHA256: hash[:]}
	w.CurrentDestination, w.PredecessorRowID = current, previous.SourceRowID
	return w
}

func TestAcceptSpoolReplacementSuccessorInheritsOriginalGraph(t *testing.T) {
	f := newReplacementFixture(t, false)
	w := successorReplacement(t, f)
	if w.Image.Size == int64(f.w.Destination.Chain[len(f.w.Destination.Chain)-1].Size) {
		t.Fatal("fixture does not distinguish new image from original graph")
	}
	moved, err := f.store.AcceptSpoolReplacement(w, func() error { return nil })
	if err != nil || moved == nil || moved.Size != w.Image.Size || moved.Mtime.UnixNano() != w.Image.MtimeNS {
		t.Fatalf("successor moved=%+v err=%v", moved, err)
	}
	prior, err := f.spool.Get(w.PredecessorRowID)
	if err != nil || prior.RetiredBy != w.SourceRowID {
		t.Fatalf("predecessor retirement %+v: %v", prior, err)
	}
	row, err := f.spool.Get(w.SourceRowID)
	if err != nil || row.DrainState != DrainReady || row.Size != w.Image.Size || !bytes.Equal(row.SHA256, w.Image.SHA256) {
		t.Fatalf("sealed successor %+v: %v", row, err)
	}
	got, err := f.spool.ReplacementWitness(row)
	if err != nil || !reflect.DeepEqual(got, &w) || !sameOriginalReplacement(*got, f.w) {
		t.Fatalf("successor witness %+v: %v", got, err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	reopenedSpool := NewSpoolStore(reopened.DB())
	reopenedRow, err := reopenedSpool.Get(w.SourceRowID)
	if err != nil {
		t.Fatal(err)
	}
	reopenedWitness, err := reopenedSpool.ReplacementWitness(reopenedRow)
	if err != nil || !reflect.DeepEqual(reopenedWitness, &w) ||
		reopenedRow.DrainState != DrainReady || reopenedRow.Size != w.Image.Size ||
		!bytes.Equal(reopenedRow.SHA256, w.Image.SHA256) ||
		reopened.LookupByPath(w.Destination.Path).Size != w.Image.Size {
		t.Fatalf("reopened successor row=%+v witness=%+v err=%v", reopenedRow, reopenedWitness, err)
	}
}

func TestAcceptSpoolReplacementRejectsStaleSuccessor(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *replacementFixture, *SpoolReplacementWitness)
	}{
		{"wrong-predecessor", func(_ *testing.T, _ *replacementFixture, w *SpoolReplacementWitness) { w.PredecessorRowID-- }},
		{"changed-original-epoch", func(_ *testing.T, _ *replacementFixture, w *SpoolReplacementWitness) { w.AcceptedEpoch++ }},
		{"changed-new-source-epoch", func(_ *testing.T, _ *replacementFixture, w *SpoolReplacementWitness) { w.SourceEpoch++ }},
		{"stale-current-token", func(_ *testing.T, _ *replacementFixture, w *SpoolReplacementWitness) { w.CurrentDestination.Revision-- }},
		{"predecessor-failed", func(t *testing.T, f *replacementFixture, w *SpoolReplacementWitness) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET drain_state='failed' WHERE id=?", w.PredecessorRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"predecessor-hold", func(t *testing.T, f *replacementFixture, w *SpoolReplacementWitness) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET clone_hold='pending' WHERE id=?", w.PredecessorRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"newer-failed-destination", func(t *testing.T, f *replacementFixture, w *SpoolReplacementWitness) {
			if _, err := f.store.DB().Exec(`INSERT INTO spool_entries(nfs_path,spool_file,size,drain_state,drain_attempts,created_at,updated_at)
 VALUES(?,?,0,'failed',0,1,1)`, w.Destination.Path, filepath.Join(filepath.Dir(f.file), "competing.spool")); err != nil {
				t.Fatal(err)
			}
		}},
		{"predecessor-witness-mutated", func(t *testing.T, f *replacementFixture, w *SpoolReplacementWitness) {
			if _, err := f.store.DB().Exec("UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?", w.PredecessorRowID); err != nil {
				t.Fatal(err)
			}
		}},
		{"original-ancestor-aba", func(t *testing.T, f *replacementFixture, _ *SpoolReplacementWitness) {
			if _, err := f.store.DB().Exec("UPDATE entries SET revision=revision+1 WHERE path='a'"); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			w := successorReplacement(t, f)
			tc.mutate(t, f, &w)
			checks := 0
			if moved, err := f.store.AcceptSpoolReplacement(w, func() error { checks++; return nil }); !errors.Is(err, ErrMirrorConflict) || moved != nil || checks != 0 {
				t.Fatalf("stale successor moved=%+v checks=%d err=%v", moved, checks, err)
			}
		})
	}
}

func TestReplacementWitnessRejectsMutatedSealedRow(t *testing.T) {
	for _, mutation := range []struct{ name, sql string }{
		{"digest", "UPDATE spool_entries SET sha256=X'00' WHERE id=?"},
		{"epoch", "UPDATE spool_entries SET accepted_epoch=accepted_epoch+1 WHERE id=?"},
		{"negative-epoch", "UPDATE spool_entries SET accepted_epoch=-1 WHERE id=?"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			if _, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			row, err := f.spool.Get(f.w.SourceRowID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(mutation.sql, row.ID); err != nil {
				t.Fatal(err)
			}
			if witness, err := f.spool.ReplacementWitness(row); !errors.Is(err, ErrMirrorConflict) || witness != nil {
				t.Fatalf("mutated row witness=%+v err=%v", witness, err)
			}
		})
	}
}

func TestAcceptSpoolReplacementSuccessorRetirementRollback(t *testing.T) {
	for _, tc := range []struct{ name, trigger string }{
		{"abort", `CREATE TRIGGER reject_successor_retire BEFORE UPDATE OF retired_by ON spool_entries
 WHEN OLD.nfs_path='a/clip.mov' AND OLD.retired_by=0 BEGIN SELECT RAISE(ABORT,'retire'); END`},
		{"ignore", `CREATE TRIGGER reject_successor_retire BEFORE UPDATE OF retired_by ON spool_entries
 WHEN OLD.nfs_path='a/clip.mov' AND OLD.retired_by=0 BEGIN SELECT RAISE(IGNORE); END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t, false)
			w := successorReplacement(t, f)
			priorBefore, err := f.spool.Get(w.PredecessorRowID)
			if err != nil {
				t.Fatal(err)
			}
			sourceBefore, err := f.spool.Get(w.SourceRowID)
			if err != nil {
				t.Fatal(err)
			}
			priorWitnessBefore, err := f.spool.ReplacementWitness(priorBefore)
			if err != nil || priorWitnessBefore == nil {
				t.Fatalf("prior witness %+v: %v", priorWitnessBefore, err)
			}
			sourceWitnessBefore, err := f.spool.ReplacementWitness(sourceBefore)
			if err != nil || sourceWitnessBefore != nil {
				t.Fatalf("new source witness %+v: %v", sourceWitnessBefore, err)
			}
			sourceRAM := f.store.LookupByPath(w.Source.Path)
			destinationRAM := f.store.LookupByPath(w.Destination.Path)
			sourceSQL, err := scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Source.Path))
			if err != nil {
				t.Fatal(err)
			}
			destinationSQL, err := scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Destination.Path))
			if err != nil {
				t.Fatal(err)
			}
			sourceBytes, err := os.ReadFile(w.SourceFile)
			if err != nil {
				t.Fatal(err)
			}
			priorBytes, err := os.ReadFile(priorBefore.SpoolFile)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(tc.trigger); err != nil {
				t.Fatal(err)
			}
			checks := 0
			if moved, err := f.store.AcceptSpoolReplacement(w, func() error { checks++; return nil }); err == nil || moved != nil || checks != 1 {
				t.Fatalf("retirement failure moved=%+v checks=%d err=%v", moved, checks, err)
			}
			priorAfter, err := f.spool.Get(w.PredecessorRowID)
			if err != nil || !reflect.DeepEqual(priorAfter, priorBefore) {
				t.Fatalf("predecessor changed before=%+v after=%+v err=%v", priorBefore, priorAfter, err)
			}
			sourceAfter, err := f.spool.Get(w.SourceRowID)
			if err != nil || !reflect.DeepEqual(sourceAfter, sourceBefore) {
				t.Fatalf("source changed before=%+v after=%+v err=%v", sourceBefore, sourceAfter, err)
			}
			priorWitnessAfter, err := f.spool.ReplacementWitness(priorAfter)
			if err != nil || !reflect.DeepEqual(priorWitnessAfter, priorWitnessBefore) {
				t.Fatalf("predecessor witness changed %+v: %v", priorWitnessAfter, err)
			}
			sourceWitnessAfter, err := f.spool.ReplacementWitness(sourceAfter)
			if err != nil || sourceWitnessAfter != nil {
				t.Fatalf("new source witness changed %+v: %v", sourceWitnessAfter, err)
			}
			if !sameReplacementEntry(sourceRAM, f.store.LookupByPath(w.Source.Path)) ||
				!sameReplacementEntry(destinationRAM, f.store.LookupByPath(w.Destination.Path)) {
				t.Fatal("RAM mirror changed on retirement refusal")
			}
			sourceSQLAfter, err := scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Source.Path))
			if err != nil || !sameReplacementEntry(sourceSQL, sourceSQLAfter) {
				t.Fatalf("source SQL mirror changed %+v: %v", sourceSQLAfter, err)
			}
			destinationSQLAfter, err := scanEntry(f.store.DB().QueryRow(`SELECT `+serveColumns+` FROM entries WHERE path=?`, w.Destination.Path))
			if err != nil || !sameReplacementEntry(destinationSQL, destinationSQLAfter) {
				t.Fatalf("destination SQL mirror changed %+v: %v", destinationSQLAfter, err)
			}
			if got, err := os.ReadFile(w.SourceFile); err != nil || !bytes.Equal(got, sourceBytes) {
				t.Fatalf("new source private bytes changed: %v", err)
			}
			if got, err := os.ReadFile(priorBefore.SpoolFile); err != nil || !bytes.Equal(got, priorBytes) {
				t.Fatalf("predecessor private bytes changed: %v", err)
			}
		})
	}
}
