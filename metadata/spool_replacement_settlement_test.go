package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/juicedata/juicefs/pkg/slicegraph"
)

func committedSettlementFixture(t *testing.T) (*replacementFixture, *SpoolRow, SpoolReplacementPublication, *CachePathEligibility, CachePathObservation, RedisPathProof, []byte) {
	t.Helper()
	f, row, intent := publicationIntent(t)
	r := publicationRecord(t, intent, f.w)
	arming := publicationNext(t, intent, ReplacementPublicationArming, r)
	armed := publicationNext(t, arming, ReplacementPublicationArmed, r)
	r.Phase = "committed"
	committed := publicationNext(t, armed, ReplacementPublicationCommitted, r)
	for _, pair := range [][2]SpoolReplacementPublication{{intent, arming}, {arming, armed}, {armed, committed}} {
		got, err := f.store.AdvanceSpoolReplacementPublication(pair[0], pair[1], func() error { return nil })
		if err != nil || got == nil || *got != pair[1] {
			t.Fatalf("advance %s: %v", pair[1].Phase, err)
		}
	}
	gate, err := OpenCachePathEligibility(f.store)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := gate.CaptureReplacementSettlement(committed)
	if err != nil {
		t.Fatal(err)
	}
	target := captured
	target.Chain = append([]CachePathNode(nil), captured.Chain...)
	leaf := len(target.Chain) - 1
	target.Chain[leaf].Inode = r.Stage.Inode
	target.Chain[leaf].Size = f.w.Image.Size
	target.Chain[leaf].MtimeSec = f.w.Image.MtimeNS / 1_000_000_000
	target.Chain[leaf].MtimeNsec = uint32(f.w.Image.MtimeNS % 1_000_000_000)
	proof := cachePathTestProof(target, f.w.Binding.FormatUUID)
	graph, err := (slicegraph.Graph{Version: slicegraph.Version, Length: uint64(f.w.Image.Size), Chunks: []slicegraph.Chunk{{Index: 0, Extents: []slicegraph.Extent{{Pos: 0, SliceID: 303, Size: uint32(f.w.Image.Size), Len: uint32(f.w.Image.Size)}}}}}).Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return f, row, committed, gate, captured, proof, graph
}

func TestReplacementSettlementAtomicReceiptAndHistoricalCleanup(t *testing.T) {
	f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	checks := 0
	settled, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { checks++; return nil })
	if err != nil || settled == nil || settled.Phase != ReplacementPublicationSettled || checks != 3 {
		t.Fatalf("settle=%+v checks=%d err=%v", settled, checks, err)
	}
	rec, err := settled.SettlementRecord()
	if err != nil || rec.Published.Revision != committed.Expected.Revision+1 || rec.Chain[len(rec.Chain)-1].Inode != proof.Nodes[len(proof.Nodes)-1].Inode {
		t.Fatalf("settlement record=%+v err=%v", rec, err)
	}
	got, err := f.spool.Get(row.ID)
	if err != nil || got.DrainState != DrainDone || got.RetiredBy != 0 {
		t.Fatalf("settled row=%+v err=%v", got, err)
	}
	if body, err := os.ReadFile(f.file); err != nil || !bytes.Equal(body, f.body) {
		t.Fatalf("private source changed: %v", err)
	}
	if err := f.spool.CheckReplacementUnder("a"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("settled path unguarded: %v", err)
	}
	cleaning, err := f.store.BeginReplacementCleanup(*settled, "root-device:inode", "files-device:inode", func() error { return nil })
	if err != nil || cleaning == nil || cleaning.Phase != ReplacementPublicationCleaning {
		t.Fatalf("cleaning=%+v err=%v", cleaning, err)
	}
	cleaned, err := f.store.FinishReplacementCleanup(*cleaning, func() error { return nil })
	if err != nil || cleaned == nil || cleaned.Phase != ReplacementPublicationCleaned {
		t.Fatalf("cleaned=%+v err=%v", cleaned, err)
	}
	if err := f.spool.CheckReplacementUnder("a"); err != nil {
		t.Fatalf("cleaned path stayed guarded: %v", err)
	}
	if _, err := f.store.DB().Exec(`DELETE FROM entries WHERE path=?`, cleaned.Expected.Path); err != nil {
		t.Fatal(err)
	}
	if p, _, _, err := readReplacementPublication(f.store.DB(), row.ID); err != nil || p == nil || *p != *cleaned {
		t.Fatalf("history depended on mirror: %+v %v", p, err)
	}
	altered := *cleaned
	alteredRec, err := altered.SettlementRecord()
	if err != nil {
		t.Fatal(err)
	}
	alteredRec.Chain[len(alteredRec.Chain)-1].Inode++
	encodedRecord, err := json.Marshal(alteredRec)
	if err != nil {
		t.Fatal(err)
	}
	altered.Settlement = string(encodedRecord)
	raw, err := encodeReplacementPublication(altered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_replacement_publications SET payload=? WHERE spool_id=?`, raw, row.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.spool.CheckReplacementUnder("a"); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("corrupt cleaned bypassed guard: %v", err)
	}
}

func TestReplacementSettlementRollbackAndProofFences(t *testing.T) {
	f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	before := f.sqlState(t)
	wrong := proof
	wrong.Nodes = append([]RedisPathNode(nil), proof.Nodes...)
	wrong.Nodes[len(wrong.Nodes)-1].Inode++
	if got, err := gate.SettleSpoolReplacement(committed, captured, wrong, wrong, graph, func() error { return nil }); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("wrong stage admitted: %+v %v", got, err)
	}
	if got, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return errors.New("session lost") }); got != nil || err == nil {
		t.Fatalf("failed callback admitted: %+v %v", got, err)
	}
	if !reflect.DeepEqual(before, f.sqlState(t)) {
		t.Fatal("rejected settlement changed row/mirror")
	}
	gotRow, err := f.spool.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.spool.ReplacementPublication(gotRow)
	if err != nil || p == nil || *p != committed {
		t.Fatalf("receipt changed after rejection: %+v %v", p, err)
	}
}

func TestReplacementSettlementSQLTriggerRollback(t *testing.T) {
	cases := []struct{ name, ddl string }{
		{"ignore-eligibility", `CREATE TRIGGER ignore_replacement_eligibility BEFORE INSERT ON cache_path_eligibility BEGIN SELECT RAISE(IGNORE); END`},
		{"ignore-done", `CREATE TRIGGER reject_replacement_done BEFORE UPDATE OF drain_state ON spool_entries WHEN NEW.drain_state='done' BEGIN SELECT RAISE(IGNORE); END`},
		{"abort-journal", `CREATE TRIGGER reject_replacement_journal BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='settled' BEGIN SELECT RAISE(ABORT,'settlement blocked'); END`},
		{"after-mirror-rewind", `CREATE TRIGGER rewind_replacement_mirror AFTER UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='settled' BEGIN UPDATE entries SET inode=inode+1 WHERE path=(SELECT nfs_path FROM spool_entries WHERE id=NEW.spool_id); END`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
			before := f.sqlState(t)
			if _, err := f.store.DB().Exec(tc.ddl); err != nil {
				t.Fatal(err)
			}
			got, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return nil })
			if got != nil || err == nil {
				t.Fatalf("fault accepted: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, f.sqlState(t)) {
				t.Fatal("rejected settlement changed durable row or mirror")
			}
			if body, err := os.ReadFile(f.file); err != nil || !bytes.Equal(body, f.body) {
				t.Fatalf("private bytes lost: %v", err)
			}
			current, err := f.spool.Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			p, err := f.spool.ReplacementPublication(current)
			if err != nil || p == nil || *p != committed {
				t.Fatalf("receipt changed: %+v %v", p, err)
			}
		})
	}
}

func TestReplacementSettlementRefusesProofDriftAndStaleCapture(t *testing.T) {
	f, _, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	changed := proof
	changed.Nodes = append([]RedisPathNode(nil), proof.Nodes...)
	changed.Nodes[0].RawAttr = []byte("different exact Redis attr")
	if got, err := gate.SettleSpoolReplacement(committed, captured, proof, changed, graph, func() error { return nil }); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("proof drift accepted: %+v %v", got, err)
	}
	if _, err := f.store.DB().Exec(`UPDATE entries SET revision=revision+1 WHERE path=?`, captured.Chain[0].Path); err != nil {
		t.Fatal(err)
	}
	if got, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return nil }); got != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale ancestor accepted: %+v %v", got, err)
	}
}

func TestReplacementSettlementVerifierFailureBeforeAndAfterCommit(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
			calls := 0
			got, settleErr := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error {
				calls++
				if calls == failAt {
					return errors.New("descriptor changed")
				}
				return nil
			})
			if got != nil || settleErr == nil || calls != failAt {
				t.Fatalf("verifier failure: got=%+v calls=%d err=%v", got, calls, settleErr)
			}
			current, err := f.spool.Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			p, err := f.spool.ReplacementPublication(current)
			if err != nil || p == nil {
				t.Fatalf("journal read: %+v %v", p, err)
			}
			if failAt == 2 {
				if *p != committed || current.DrainState != DrainReady || f.store.LookupByPath(committed.Expected.Path).LocalOnly != true {
					t.Fatalf("precommit failure published: %+v %+v", p, current)
				}
			} else {
				var pending *PendingSpoolReplacement
				if !errors.As(settleErr, &pending) || p.Phase != ReplacementPublicationSettled || current.DrainState != DrainDone || f.store.LookupByPath(committed.Expected.Path).LocalOnly {
					t.Fatalf("postcommit uncertainty lost settlement: %+v %+v %v", p, current, settleErr)
				}
			}
		})
	}
}

func TestReplacementSettlementMigratesSixPhaseCommittedReceipt(t *testing.T) {
	f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	var original []byte
	if err := f.store.DB().QueryRow(`SELECT payload FROM spool_replacement_publications WHERE spool_id=?`, row.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`DROP TABLE spool_replacement_publications`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`CREATE TABLE spool_replacement_publications (
 spool_id INTEGER PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE,
 phase TEXT NOT NULL CHECK(phase IN ('queued','intent','arming','armed','committed','superseded')), payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`INSERT INTO spool_replacement_publications(rowid,spool_id,operation_id,phase,payload) VALUES(?,?,?,?,?)`, row.ID, row.ID, committed.OperationID, committed.Phase, original); err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(f.store.DB()); err != nil {
		t.Fatal(err)
	}
	var rowid int64
	var migrated []byte
	if err := f.store.DB().QueryRow(`SELECT rowid,payload FROM spool_replacement_publications WHERE spool_id=?`, row.ID).Scan(&rowid, &migrated); err != nil || rowid != row.ID || !bytes.Equal(migrated, original) {
		t.Fatalf("migration changed rowid/payload: %d %v", rowid, err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_entries SET retired_by=999 WHERE id=?`, row.ID); err == nil {
		t.Fatal("migration lost active retirement guard")
	}
	got, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return nil })
	if err != nil || got == nil || got.Phase != ReplacementPublicationSettled {
		t.Fatalf("migrated receipt did not settle: %+v %v", got, err)
	}
}

func TestReplacementSettlementCaptureAllowsExactSyntheticLeaf(t *testing.T) {
	f, _, committed, gate, _, _, _ := committedSettlementFixture(t)
	synthetic := uint64(1<<63) | 45
	if _, err := f.store.DB().Exec(`UPDATE entries SET inode=? WHERE path=?`, int64(synthetic), committed.Expected.Path); err != nil {
		t.Fatal(err)
	}
	// This fixture changes only inode representation. The ordinary UPDATE
	// trigger advanced revision, so restore the accepted token explicitly.
	if _, err := f.store.DB().Exec(`UPDATE entries SET revision=? WHERE path=?`, committed.Expected.Revision, committed.Expected.Path); err != nil {
		t.Fatal(err)
	}
	f.store.mu.Lock()
	f.store.pathCache[committed.Expected.Path].Inode = synthetic
	f.store.mu.Unlock()
	captured, err := gate.CaptureReplacementSettlement(committed)
	if err != nil || captured.Chain[len(captured.Chain)-1].Inode != synthetic {
		t.Fatalf("synthetic local leaf rejected: %+v %v", captured, err)
	}
	if validCachePathChain(captured.Path, captured.Chain) {
		t.Fatal("ordinary path validator accepted synthetic leaf")
	}
}

func TestReplacementSettlementRecoveryGuardRequiresCanonicalJournal(t *testing.T) {
	f, row, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	settled, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := f.spool.CheckDrainCompletionRecovery(); err != nil {
		t.Fatalf("settled receipt held as legacy: %v", err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_replacement_publications SET payload='{}' WHERE spool_id=?`, row.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.spool.CheckDrainCompletionRecovery(); err == nil {
		t.Fatal("corrupt journal bypassed recovery guard")
	}
	raw, err := encodeReplacementPublication(*settled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_replacement_publications SET payload=? WHERE spool_id=?`, raw, row.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`INSERT INTO spool_completion_holds(completion_id,spool_id,nfs_path,spool_file,size) VALUES('retained-hold',?,?,?,?)`, row.ID, row.NFSPath, row.SpoolFile, row.Size); err != nil {
		t.Fatal(err)
	}
	var pending *PendingDrainCompletion
	if err := f.spool.CheckDrainCompletionRecovery(); !errors.As(err, &pending) {
		t.Fatalf("replacement bypassed explicit hold: %v", err)
	}
}
