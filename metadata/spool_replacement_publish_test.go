package metadata

import (
	"bytes"
	"encoding/hex"
	"errors"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/replaceguard"
)

func publicationRecord(t *testing.T, p SpoolReplacementPublication, w SpoolReplacementWitness) replaceguard.Record {
	t.Helper()
	parents := []replaceguard.Parent{{Inode: 1}}
	for _, node := range w.Destination.Chain[:len(w.Destination.Chain)-1] {
		parents = append(parents, replaceguard.Parent{Name: path.Base(node.Path), Inode: node.Inode})
	}
	leaf := w.Destination.Chain[len(w.Destination.Chain)-1]
	return replaceguard.Record{
		Version: 1, OperationID: w.OperationID, Owner: "owner-a", Proxy: "proxy-a",
		BackendUUID: w.Binding.FormatUUID, BackendDigest: w.Binding.Digest,
		Project: parents[len(parents)-1].Inode, AcceptedEpoch: w.AcceptedEpoch, Epoch: w.AcceptedEpoch,
		Parents: parents, GraphDigest: w.GraphDigest,
		ImageSHA256: hex.EncodeToString(w.Image.SHA256), Phase: "prepared",
		Stage: replaceguard.File{Name: path.Base(p.StagePath), Parent: parents[len(parents)-1].Inode,
			Inode: 333, Length: uint64(w.Image.Size), MtimeNS: w.Image.MtimeNS, CtimeNS: 7},
		Destination: replaceguard.File{Name: path.Base(w.Destination.Path), Parent: parents[len(parents)-1].Inode,
			Inode: leaf.Inode, Length: uint64(leaf.Size), MtimeNS: leaf.MtimeSec*1_000_000_000 + int64(leaf.MtimeNsec), CtimeNS: 8},
	}
}

func publicationNext(t *testing.T, p SpoolReplacementPublication, phase string, r replaceguard.Record) SpoolReplacementPublication {
	t.Helper()
	encoded, err := replaceguard.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	p.Phase, p.Revision, p.Record = phase, p.Revision+1, string(encoded)
	return p
}

func publicationIntent(t *testing.T) (*replacementFixture, *SpoolRow, SpoolReplacementPublication) {
	t.Helper()
	f, row, queued := acceptedPublicationFixture(t)
	intent, err := f.store.BeginSpoolReplacementPublication(row.ID, queued.Expected, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return f, row, *intent
}

func TestReplacementPublicationReceiptAdvanceAndReopen(t *testing.T) {
	f, row, intent := publicationIntent(t)
	before := f.sqlState(t)
	r := publicationRecord(t, intent, f.w)
	arming := publicationNext(t, intent, ReplacementPublicationArming, r)
	checks := 0
	advance := func(a, b SpoolReplacementPublication) {
		t.Helper()
		got, err := f.store.AdvanceSpoolReplacementPublication(a, b, func() error { checks++; return nil })
		if err != nil || got == nil || *got != b {
			t.Fatalf("advance %s->%s: got=%+v err=%v", a.Phase, b.Phase, got, err)
		}
	}
	advance(intent, arming)
	r.Epoch++
	armingAdvanced := publicationNext(t, arming, ReplacementPublicationArming, r)
	advance(arming, armingAdvanced)
	r.Owner, r.Proxy = "normalized-owner", "normalized-proxy"
	armed := publicationNext(t, armingAdvanced, ReplacementPublicationArmed, r)
	advance(armingAdvanced, armed)
	r.Proxy, r.Epoch = "proxy-restart", r.Epoch+1
	refreshed := publicationNext(t, armed, ReplacementPublicationArmed, r)
	advance(armed, refreshed)
	for name, mutate := range map[string]func(*replaceguard.Record){
		"stage-inode": func(v *replaceguard.Record) { v.Stage.Inode++ },
		"stage-ctime": func(v *replaceguard.Record) { v.Stage.CtimeNS++ },
		"old-ctime":   func(v *replaceguard.Record) { v.Destination.CtimeNS++ },
	} {
		t.Run(name, func(t *testing.T) {
			bad := r
			mutate(&bad)
			bad.Phase = "committed"
			candidate := publicationNext(t, refreshed, ReplacementPublicationCommitted, bad)
			if got, err := f.store.AdvanceSpoolReplacementPublication(refreshed, candidate, func() error { return nil }); !errors.Is(err, ErrMirrorConflict) || got != nil {
				t.Fatalf("changed committed receipt=%+v err=%v", got, err)
			}
		})
	}
	r.Phase = "committed"
	committed := publicationNext(t, refreshed, ReplacementPublicationCommitted, r)
	advance(refreshed, committed)
	if checks != 10 {
		t.Fatalf("source checks=%d", checks)
	}
	if got, err := f.store.AdvanceSpoolReplacementPublication(refreshed, committed, func() error { return nil }); !errors.Is(err, ErrMirrorConflict) || got != nil {
		t.Fatalf("stale CAS accepted: %+v %v", got, err)
	}
	if got, err := f.store.AdvanceSpoolReplacementPublication(committed, committed, func() error { return nil }); !errors.Is(err, ErrMirrorConflict) || got != nil {
		t.Fatalf("terminal receipt changed: %+v %v", got, err)
	}
	after := f.sqlState(t)
	if !reflect.DeepEqual(before, after) || after.state != string(DrainReady) || after.retired != 0 {
		t.Fatalf("receipt changed accepted row/mirror: before=%+v after=%+v", before, after)
	}
	if ok, err := f.spool.MarkDraining(row.ID); ok || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("committed row entered generic drain: %v %v", ok, err)
	}
	if ok, err := f.spool.MarkDone(row.ID); ok || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("committed row entered generic done: %v %v", ok, err)
	}
	if err := f.spool.Delete(row.ID); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("committed row deleted: %v", err)
	}
	if _, err := f.store.DB().Exec(`DELETE FROM spool_entries WHERE id=?`, row.ID); err == nil {
		t.Fatal("committed row bypassed SQL delete guard")
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
	spool := NewSpoolStore(reopened.DB())
	fresh, err := spool.Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := spool.ReplacementPublication(fresh)
	if err != nil || got == nil || *got != committed {
		t.Fatalf("reopened receipt=%+v err=%v", got, err)
	}
}

func TestReplacementPublicationArmingReconcilesHigherEpochReply(t *testing.T) {
	f, row, intent := publicationIntent(t)
	r := publicationRecord(t, intent, f.w)
	arming := publicationNext(t, intent, ReplacementPublicationArming, r)
	if _, err := f.store.AdvanceSpoolReplacementPublication(intent, arming, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	r.Epoch += 2
	r.Owner, r.Proxy = "normalized-owner", "new-proxy"
	armed := publicationNext(t, arming, ReplacementPublicationArmed, r)
	if got, err := f.store.AdvanceSpoolReplacementPublication(arming, armed, func() error { return nil }); err != nil || got == nil || *got != armed {
		t.Fatalf("higher-epoch canonical reply=%+v err=%v", got, err)
	}
	if got, err := f.spool.ReplacementPublication(row); err != nil || got == nil || *got != armed {
		t.Fatalf("higher-epoch stored reply=%+v err=%v", got, err)
	}
}

func TestReplacementPublicationRejectsImmutableRecordChanges(t *testing.T) {
	f, row, intent := publicationIntent(t)
	base := publicationRecord(t, intent, f.w)
	cases := map[string]func(*replaceguard.Record){
		"binding": func(r *replaceguard.Record) { r.BackendDigest = strings.Repeat("b", 64) },
		"graph":   func(r *replaceguard.Record) { r.GraphDigest = strings.Repeat("b", 64) },
		"image":   func(r *replaceguard.Record) { r.ImageSHA256 = strings.Repeat("b", 64) },
		"parent": func(r *replaceguard.Record) {
			r.Parents[1].Inode++
			r.Project++
			r.Stage.Parent++
			r.Destination.Parent++
		},
		"old-length":    func(r *replaceguard.Record) { r.Destination.Length++ },
		"old-mtime":     func(r *replaceguard.Record) { r.Destination.MtimeNS++ },
		"stage-length":  func(r *replaceguard.Record) { r.Stage.Length++ },
		"stage-mtime":   func(r *replaceguard.Record) { r.Stage.MtimeNS++ },
		"missing-ctime": func(r *replaceguard.Record) { r.Destination.CtimeNS = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := base
			r.Parents = append([]replaceguard.Parent(nil), base.Parents...)
			mutate(&r)
			next := publicationNext(t, intent, ReplacementPublicationArming, r)
			if got, err := f.store.AdvanceSpoolReplacementPublication(intent, next, func() error { return nil }); !errors.Is(err, ErrMirrorConflict) || got != nil {
				t.Fatalf("accepted changed record: %+v %v", got, err)
			}
		})
	}
	got, err := f.spool.ReplacementPublication(row)
	if err != nil || got == nil || *got != intent {
		t.Fatalf("failed advance changed journal: %+v %v", got, err)
	}
}

func TestReplacementPublicationAdvanceRollback(t *testing.T) {
	for _, action := range []string{"ABORT", "IGNORE", "REWIND"} {
		t.Run(action, func(t *testing.T) {
			f, row, intent := publicationIntent(t)
			next := publicationNext(t, intent, ReplacementPublicationArming, publicationRecord(t, intent, f.w))
			trigger := `CREATE TRIGGER reject_publication BEFORE UPDATE OF phase ON spool_replacement_publications
 WHEN NEW.phase='arming' BEGIN SELECT RAISE(` + action + `,'reject'); END`
			if action == "IGNORE" {
				trigger = `CREATE TRIGGER reject_publication BEFORE UPDATE OF phase ON spool_replacement_publications
 WHEN NEW.phase='arming' BEGIN SELECT RAISE(IGNORE); END`
			}
			if action == "REWIND" {
				trigger = `CREATE TRIGGER reject_publication AFTER UPDATE OF phase ON spool_replacement_publications
 WHEN NEW.phase='arming' BEGIN UPDATE spool_replacement_publications SET phase=OLD.phase,payload=OLD.payload WHERE spool_id=OLD.spool_id; END`
			}
			if _, err := f.store.DB().Exec(trigger); err != nil {
				t.Fatal(err)
			}
			if got, err := f.store.AdvanceSpoolReplacementPublication(intent, next, func() error { return nil }); err == nil || got != nil {
				t.Fatalf("trigger accepted: %+v %v", got, err)
			}
			got, err := f.spool.ReplacementPublication(row)
			if err != nil || got == nil || *got != intent {
				t.Fatalf("rollback lost intent: %+v %v", got, err)
			}
		})
	}
}

func TestReplacementPublicationAdvanceSourceVerification(t *testing.T) {
	f, row, intent := publicationIntent(t)
	next := publicationNext(t, intent, ReplacementPublicationArming, publicationRecord(t, intent, f.w))
	checks := 0
	precheck := errors.New("source changed before commit")
	if got, err := f.store.AdvanceSpoolReplacementPublication(intent, next, func() error {
		checks++
		return precheck
	}); got != nil || !errors.Is(err, precheck) || checks != 1 {
		t.Fatalf("precheck result=%+v err=%v checks=%d", got, err, checks)
	}
	if got, err := f.spool.ReplacementPublication(row); err != nil || got == nil || *got != intent {
		t.Fatalf("precheck changed journal=%+v err=%v", got, err)
	}
	checks = 0
	postcheck := errors.New("source changed after commit")
	if got, err := f.store.AdvanceSpoolReplacementPublication(intent, next, func() error {
		checks++
		if checks == 2 {
			return postcheck
		}
		return nil
	}); got != nil || !errors.Is(err, postcheck) || checks != 2 {
		t.Fatalf("postcheck result=%+v err=%v checks=%d", got, err, checks)
	} else {
		var pending *PendingSpoolReplacement
		if !errors.As(err, &pending) {
			t.Fatalf("postcommit source failure was not pending: %v", err)
		}
	}
	if got, err := f.spool.ReplacementPublication(row); err != nil || got == nil || *got != next {
		t.Fatalf("postcommit journal=%+v err=%v", got, err)
	}
}

func TestReplacementPublicationLegacyPhaseMigrationPreservesPayload(t *testing.T) {
	for _, phase := range []string{ReplacementPublicationQueued, ReplacementPublicationIntent} {
		t.Run(phase, func(t *testing.T) {
			f, row, before := acceptedPublicationFixture(t)
			if phase == ReplacementPublicationIntent {
				p, err := f.store.BeginSpoolReplacementPublication(row.ID, before.Expected, func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				before = p
			}
			var raw []byte
			if err := f.store.DB().QueryRow(`SELECT payload FROM spool_replacement_publications WHERE spool_id=?`, row.ID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(`DROP TABLE spool_replacement_publications`); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(`CREATE TABLE spool_replacement_publications (
 spool_id INTEGER PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE,
 phase TEXT NOT NULL CHECK(phase IN ('queued','intent','superseded')), payload BLOB NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(`INSERT INTO spool_replacement_publications(spool_id,operation_id,phase,payload) VALUES(?,?,?,?)`,
				row.ID, before.OperationID, phase, raw); err != nil {
				t.Fatal(err)
			}
			if err := InitSpoolSchema(f.store.DB()); err != nil {
				t.Fatal(err)
			}
			var after []byte
			if err := f.store.DB().QueryRow(`SELECT payload FROM spool_replacement_publications WHERE spool_id=?`, row.ID).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, after) {
				t.Fatal("migration changed canonical payload")
			}
			got, err := f.spool.ReplacementPublication(row)
			if err != nil || got == nil || *got != *before {
				t.Fatalf("migrated journal=%+v err=%v", got, err)
			}
			if err := InitSpoolSchema(f.store.DB()); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.DB().Exec(`DELETE FROM spool_entries WHERE id=?`, row.ID); err == nil {
				t.Fatal("migrated journal lost delete guard")
			}
			if _, err := f.store.DB().Exec(`UPDATE spool_entries SET retired_by=999 WHERE id=?`, row.ID); err == nil {
				t.Fatal("migrated journal lost retirement guard")
			}
		})
	}
}

// A malformed saved seconds value must not wrap to the legitimate record's
// timestamp when converted to int64 nanoseconds during recovery.
func TestReplacementPublicationRejectsOverflowedWitnessTime(t *testing.T) {
	f, _, intent := publicationIntent(t)
	r := publicationRecord(t, intent, f.w)
	p := publicationNext(t, intent, ReplacementPublicationArming, r)
	w := f.w
	w.Destination.Chain = append([]CachePathNode(nil), w.Destination.Chain...)
	leaf := &w.Destination.Chain[len(w.Destination.Chain)-1]
	const wrappedSeconds = int64((1 << 64) / 1_000_000_000)
	const wrappedNanos = int64((1 << 64) % 1_000_000_000)
	ns := r.Destination.MtimeNS%1_000_000_000 + wrappedNanos
	leaf.MtimeSec = r.Destination.MtimeNS/1_000_000_000 + wrappedSeconds + ns/1_000_000_000
	leaf.MtimeNsec = uint32(ns % 1_000_000_000)
	if err := validateReplacementRecord(p, w); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("overflowed witness time accepted: %v", err)
	}
}
