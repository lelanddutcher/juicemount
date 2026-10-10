package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBoundEffectsDurableBodyAndModeLineage(t *testing.T) {
	s, sp := newBoundCompletionStore(t)
	f := bornDrainingFixture(t, s, sp, "._effects.mov", 4, 8)
	r, err := s.CompleteBoundSpool(f.item)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(f.item.SpoolFile)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if err := s.SettleBoundEffects(r, digest, body, true); err != nil {
		t.Fatal(err)
	}
	image, exists, valid, err := s.BoundSidecarBody(r.Path)
	if err != nil || !exists || valid {
		t.Fatalf("pre-retirement body exposed %v %v %v", exists, valid, err)
	}
	// Retirement requires exact physical custody; a mutable receipt alone fails.
	if err := s.RetireBoundEffects(r, nil); err == nil {
		t.Fatal("bare receipt retired source")
	}
	var phase string
	if err := s.DB().QueryRow(`SELECT phase FROM bound_completion_effects WHERE spool_id=?`, r.SpoolID).Scan(&phase); err != nil || phase != "ready" {
		t.Fatalf("phase %q %v", phase, err)
	}
	_ = image
	if pending, err := s.ListPendingBoundReconcile(1); err != nil || len(pending) != 1 || pending[0].Path != r.Path {
		t.Fatalf("pending hint %+v %v", pending, err)
	}
	if n, err := s.DispatchPendingBoundReconcile(1, func(MetadataEvent) error { return errors.New("offline") }); err == nil || n != 0 {
		t.Fatalf("dispatch failure %d %v", n, err)
	}
	if pending, err := s.ListPendingBoundReconcile(1); err != nil || len(pending) != 1 {
		t.Fatalf("lost pending hint %+v %v", pending, err)
	}
	if n, err := s.DispatchPendingBoundReconcile(1, func(ev MetadataEvent) error {
		if ev.Op != "reconcile" || ev.Path != r.Path {
			t.Fatalf("hint %+v", ev)
		}
		return nil
	}); err != nil || n != 1 {
		t.Fatalf("dispatch %d %v", n, err)
	}
	// A content-equivalent body is present in the durable row before source unlink.
	var durable []byte
	if err := s.DB().QueryRow(`SELECT sidecar_body FROM bound_completion_effects WHERE spool_id=?`, r.SpoolID).Scan(&durable); err != nil || !bytes.Equal(durable, body) {
		t.Fatalf("durable body %v", err)
	}
}

func TestBoundEffectsRetirementRejectsWrongNamespace(t *testing.T) {
	s, sp := newBoundCompletionStore(t)
	f := bornDrainingFixture(t, s, sp, "._wrong.mov", 4, 8)
	r, err := s.CompleteBoundSpool(f.item)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(filepath.Dir(f.item.SpoolFile))
	if _, err := PrepareBoundRetirement(r, f.f, root); err == nil {
		t.Fatal("non-P38 source accepted as retirement custody")
	}
}

// A failed transport dispatch survives Store closure. A new pending identity
// created during retry is not acknowledged by the earlier snapshot's CAS.
func TestBoundEffectsReconcileDispatchReopenAndIdentityAck(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	sp := NewSpoolStore(s.DB())
	s.SetSpoolStore(sp)
	first := bornDrainingFixture(t, s, sp, "._first", 4, 8)
	r1, err := s.CompleteBoundSpool(first.item)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(first.item.SpoolFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SettleBoundEffects(r1, sha256.Sum256(body), body, true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	sp2 := NewSpoolStore(fresh.DB())
	fresh.SetSpoolStore(sp2)
	pending, err := fresh.ListPendingBoundReconcile(8)
	if err != nil || len(pending) != 1 || pending[0].CompletionID != r1.CompletionID {
		t.Fatalf("reopen pending %+v %v", pending, err)
	}
	if n, err := fresh.DispatchPendingBoundReconcile(8, func(MetadataEvent) error { return errors.New("transport down") }); n != 0 || err == nil {
		t.Fatalf("failed dispatch %d %v", n, err)
	}
	pending, err = fresh.ListPendingBoundReconcile(8)
	if err != nil || len(pending) != 1 || pending[0].CompletionID != r1.CompletionID {
		t.Fatalf("failed dispatch lost pending %+v %v", pending, err)
	}
	var r2 BoundDrainCommitResult
	count := 0
	n, err := fresh.DispatchPendingBoundReconcile(8, func(ev MetadataEvent) error {
		count++
		if ev.Op != "reconcile" || ev.Path != r1.Path {
			t.Fatalf("wrong dispatch %+v", ev)
		}
		second := bornDrainingFixture(t, fresh, sp2, "._second", 4, 8)
		var e error
		r2, e = fresh.CompleteBoundSpool(second.item)
		if e != nil {
			return e
		}
		b, e := os.ReadFile(second.item.SpoolFile)
		if e != nil {
			return e
		}
		return fresh.SettleBoundEffects(r2, sha256.Sum256(b), b, true)
	})
	if err != nil || n != 1 || count != 1 || r2.CompletionID == "" || r2.CompletionID == r1.CompletionID {
		t.Fatalf("dispatch n=%d count=%d second=%+v err=%v", n, count, r2, err)
	}
	pending, err = fresh.ListPendingBoundReconcile(8)
	if err != nil || len(pending) != 1 || pending[0].CompletionID != r2.CompletionID {
		t.Fatalf("new hint was incorrectly acked %+v %v", pending, err)
	}
}

const legacyBoundEffectsDDL = `CREATE TABLE bound_completion_effects (
 completion_id TEXT PRIMARY KEY, spool_id INTEGER NOT NULL UNIQUE,
 birth_id TEXT NOT NULL, nfs_path TEXT NOT NULL, incarnation TEXT NOT NULL,
 published_revision INTEGER NOT NULL, original_inode INTEGER NOT NULL,
 size INTEGER NOT NULL CHECK(size >= 0), source_sha256 BLOB NOT NULL,
 sidecar_body BLOB CHECK(sidecar_body IS NULL OR length(sidecar_body)<=131072),
 phase TEXT NOT NULL CHECK(phase IN ('ready','retired')),
 reconcile_pending INTEGER NOT NULL DEFAULT 1)`

func TestBoundEffectsLegacyPhaseMigrationPreservesRowsAndOrder(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(legacyBoundEffectsDDL); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id           string
		spool, rowid int64
		phase        string
		body         []byte
	}{
		{"ready-id", 7, 19, "ready", []byte("exact-ready")},
		{"retired-id", 8, 23, "retired", []byte("historical")},
	} {
		_, err := s.DB().Exec(`INSERT INTO bound_completion_effects
		 (rowid,completion_id,spool_id,birth_id,nfs_path,incarnation,published_revision,original_inode,
		 size,source_sha256,sidecar_body,phase,reconcile_pending) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			row.rowid, row.id, row.spool, "birth", "clip.mov", "inc", 3, 91, len(row.body), bytes.Repeat([]byte{7}, 32),
			row.body, row.phase, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	rows, err := s.DB().Query(`SELECT rowid,completion_id,phase,sidecar_body FROM bound_completion_effects ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for _, want := range []struct {
		rowid     int64
		id, phase string
		body      []byte
	}{
		{19, "ready-id", "ready", []byte("exact-ready")},
		{23, "retired-id", "retired", []byte("historical")},
	} {
		if !rows.Next() {
			t.Fatal("migration dropped row")
		}
		var rowid int64
		var id, phase string
		var body []byte
		if err := rows.Scan(&rowid, &id, &phase, &body); err != nil {
			t.Fatal(err)
		}
		if rowid != want.rowid || id != want.id || phase != want.phase || !bytes.Equal(body, want.body) {
			t.Fatalf("migrated row %d %s %s %q", rowid, id, phase, body)
		}
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("extra rows or scan error: %v", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase='awaiting_publication' WHERE completion_id='ready-id' AND phase='ready'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase='unknown' WHERE completion_id='ready-id'`); err == nil {
		t.Fatal("new CHECK allowed unknown phase")
	}
}

func TestBoundEffectsLegacyMigrationRefusesUnexpectedObject(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(legacyBoundEffectsDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO bound_completion_effects
	 (completion_id,spool_id,birth_id,nfs_path,incarnation,published_revision,original_inode,
	 size,source_sha256,phase) VALUES('retain',1,'birth','clip.mov','inc',1,1,1,zeroblob(32),'ready')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`CREATE TRIGGER preserve_bound_effects BEFORE DELETE ON bound_completion_effects BEGIN SELECT RAISE(ABORT,'protected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err == nil {
		t.Fatal("unexpected object silently dropped")
	}
	var phase string
	if err := s.DB().QueryRow(`SELECT phase FROM bound_completion_effects WHERE completion_id='retain'`).Scan(&phase); err != nil || phase != "ready" {
		t.Fatalf("row changed after refusal %q %v", phase, err)
	}
	if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase='awaiting_publication' WHERE completion_id='retain'`); err == nil {
		t.Fatal("legacy CHECK changed on failed migration")
	}
}

func TestBoundEffectsLegacyMigrationRejectsAlteredDefinition(t *testing.T) {
	for _, changed := range []struct{ name, from, to string }{
		{"size_constraint", "CHECK(size >= 0)", "CHECK(size >= -1)"},
		{"phase_literal", "'ready'", "'READY'"},
	} {
		t.Run(changed.name, func(t *testing.T) {
			s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			altered := strings.Replace(legacyBoundEffectsDDL, changed.from, changed.to, 1)
			if _, err := s.DB().Exec(altered); err != nil {
				t.Fatal(err)
			}
			if err := InitSpoolSchema(s.DB()); err == nil {
				t.Fatal("altered same-column definition accepted")
			}
			var ddl string
			if err := s.DB().QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='bound_completion_effects'`).Scan(&ddl); err != nil || !strings.Contains(ddl, changed.to) {
				t.Fatalf("failed migration changed original definition: %v %q", err, ddl)
			}
		})
	}
}

func TestBoundEffectsRecoveryRejectsMismatchedIdentityAndPhase(t *testing.T) {
	s, sp := newBoundCompletionStore(t)
	f := bornDrainingFixture(t, s, sp, "._strict.mov", 4, 8)
	r, err := s.CompleteBoundSpool(f.item)
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(f.item.SpoolFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SettleBoundEffects(r, sha256.Sum256(body), body, true); err != nil {
		t.Fatal(err)
	}
	var pending *PendingBoundEffectsRecovery
	if err := sp.CheckBoundEffectsRecovery(); !errors.As(err, &pending) || len(pending.Effects) != 1 || pending.Effects[0].Phase != "ready" {
		t.Fatalf("ready recovery: %v", err)
	}
	if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase='awaiting_publication' WHERE spool_id=?`, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	if err := sp.CheckBoundEffectsRecovery(); !errors.As(err, &pending) || len(pending.Effects) != 1 || pending.Effects[0].Phase != "awaiting_publication" {
		t.Fatalf("awaiting recovery: %v", err)
	}
	if _, err := s.DB().Exec(`UPDATE spool_completion_holds SET birth_identity='wrong' WHERE spool_id=?`, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	if err := sp.CheckBoundEffectsRecovery(); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("mismatched identity: %v", err)
	}
	if _, err := s.DB().Exec(`UPDATE spool_completion_holds SET birth_identity=? WHERE spool_id=?`, r.FileIdentity, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase='retired' WHERE spool_id=?`, r.SpoolID); err != nil {
		t.Fatal(err)
	}
	if err := sp.CheckBoundEffectsRecovery(); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("retired with active hold: %v", err)
	}
}

type modeState struct {
	mode, ramMode               uint32
	revision, ramRevision       int64
	incarnation, ramIncarnation string
}

func captureModeState(t *testing.T, s *Store, path string) modeState {
	t.Helper()
	var state modeState
	if err := s.DB().QueryRow(`SELECT mode,revision,incarnation FROM entries WHERE path=?`, path).
		Scan(&state.mode, &state.revision, &state.incarnation); err != nil {
		t.Fatal(err)
	}
	e := s.LookupByPath(path)
	if e == nil {
		t.Fatal("missing RAM entry")
	}
	state.ramMode, state.ramRevision, state.ramIncarnation = uint32(e.Mode), e.Version.Revision, e.Version.Incarnation
	return state
}

func insertModeFixture(t *testing.T, s *Store, path string) {
	t.Helper()
	e := MakeEntry(path, false, 8, time.Unix(1700000000, 0), 121)
	if err := s.Insert(e); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateModeMetadataOnlyWithoutSpoolSchema(t *testing.T) {
	for _, path := range []string{"plain.mov", "._plain.mov"} {
		t.Run(path, func(t *testing.T) {
			s := newTestStore(t)
			insertModeFixture(t, s, path)
			before := captureModeState(t, s, path)
			if err := s.UpdateMode(path, 0600); err != nil {
				t.Fatal(err)
			}
			after := captureModeState(t, s, path)
			if after.mode&0777 != 0600 || after.ramMode&0777 != 0600 || after.revision != before.revision+1 || after.ramRevision != before.ramRevision+1 ||
				after.incarnation != before.incarnation || after.ramIncarnation != before.ramIncarnation {
				t.Fatalf("metadata-only mode before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestUpdateModeInitializedLegacyWithoutEffects(t *testing.T) {
	s, _ := newBoundCompletionStore(t)
	insertModeFixture(t, s, "unbound.mov")
	before := captureModeState(t, s, "unbound.mov")
	if err := s.UpdateMode("unbound.mov", 0600); err != nil {
		t.Fatal(err)
	}
	after := captureModeState(t, s, "unbound.mov")
	if after.mode&0777 != 0600 || after.ramMode&0777 != 0600 || after.revision != before.revision+1 || after.ramRevision != before.ramRevision+1 {
		t.Fatalf("initialized legacy before=%+v after=%+v", before, after)
	}
}

func TestUpdateModeRejectsIncompleteOrAttachedSpoolSchema(t *testing.T) {
	for _, scenario := range []string{"partial", "case_variant", "wrong_type", "attached_store", "invalid_column", "query_failure"} {
		t.Run(scenario, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "mirror.db")
			s, err := Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			const path = "guarded.mov"
			insertModeFixture(t, s, path)
			before := captureModeState(t, s, path)
			switch scenario {
			case "partial":
				if _, err := s.DB().Exec(`CREATE TABLE spool_entries (id INTEGER)`); err != nil {
					t.Fatal(err)
				}
			case "case_variant":
				if _, err := s.DB().Exec(`CREATE TABLE SPOOL_ENTRIES (id INTEGER)`); err != nil {
					t.Fatal(err)
				}
			case "wrong_type":
				if _, err := s.DB().Exec(`CREATE VIEW spool_entries AS SELECT 1 AS id`); err != nil {
					t.Fatal(err)
				}
			case "attached_store":
				s.SetSpoolStore(NewSpoolStore(s.DB()))
			case "invalid_column":
				if err := InitSpoolSchema(s.DB()); err != nil {
					t.Fatal(err)
				}
				if _, err := s.DB().Exec(`DROP TABLE bound_completion_effects; CREATE TABLE bound_completion_effects (spool_id INTEGER,nfs_path TEXT)`); err != nil {
					t.Fatal(err)
				}
			case "query_failure":
				if err := s.DB().Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.UpdateMode(path, 0600); err == nil {
				t.Fatal("unsafe mode update acknowledged")
			}
			if scenario != "query_failure" {
				if after := captureModeState(t, s, path); after != before {
					t.Fatalf("failed mode changed SQL or RAM: before=%+v after=%+v", before, after)
				}
			} else {
				if e := s.lookupByPathRAM(path); e == nil || uint32(e.Mode) != before.ramMode || e.Version.Revision != before.ramRevision {
					t.Fatalf("failed query changed RAM: %+v", e)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("close failed Store before SQL observation: %v", err)
				}
				uri := url.URL{Scheme: "file", Path: dbPath}
				options := uri.Query()
				options.Set("mode", "ro")
				options.Add("_pragma", "busy_timeout(1000)")
				uri.RawQuery = options.Encode()
				check, err := sql.Open("sqlite", uri.String())
				if err != nil {
					t.Fatal(err)
				}
				defer check.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				var mode uint32
				var revision int64
				if err := check.QueryRowContext(ctx, `SELECT mode,revision FROM entries WHERE path=?`, path).Scan(&mode, &revision); err != nil {
					t.Fatalf("fresh read-only SQL observation failed: %v", err)
				}
				if mode != before.mode || revision != before.revision {
					t.Fatalf("failed mode update changed SQL: mode=%o revision=%d before=%+v", mode, revision, before)
				}
			}
		})
	}
}

func TestUpdateModeFencesPendingBoundEffectsAndHold(t *testing.T) {
	for _, phase := range []string{"ready", "awaiting_publication", "retired"} {
		for _, detached := range []bool{false, true} {
			label := phase
			if detached {
				label += "_detached"
			}
			t.Run(label, func(t *testing.T) {
				s, sp := newBoundCompletionStore(t)
				f := bornDrainingFixture(t, s, sp, "._guarded.mov", 4, 8)
				r, err := s.CompleteBoundSpool(f.item)
				if err != nil {
					t.Fatal(err)
				}
				body, err := os.ReadFile(f.item.SpoolFile)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.SettleBoundEffects(r, sha256.Sum256(body), body, true); err != nil {
					t.Fatal(err)
				}
				if phase != "ready" {
					if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase=? WHERE completion_id=?`, phase, r.CompletionID); err != nil {
						t.Fatal(err)
					}
				}
				before := captureModeState(t, s, r.Path)
				var holdID, holdPath, holdFile, effectPhase string
				var holdHash, effectBody []byte
				if err := s.DB().QueryRow(`SELECT completion_id,nfs_path,spool_file,source_sha256 FROM spool_completion_holds WHERE spool_id=?`, r.SpoolID).
					Scan(&holdID, &holdPath, &holdFile, &holdHash); err != nil {
					t.Fatal(err)
				}
				if err := s.DB().QueryRow(`SELECT phase,sidecar_body FROM bound_completion_effects WHERE spool_id=?`, r.SpoolID).
					Scan(&effectPhase, &effectBody); err != nil {
					t.Fatal(err)
				}
				if detached {
					s.SetSpoolStore(nil)
				}
				if err := s.UpdateMode(r.Path, 0600); !errors.Is(err, ErrMirrorConflict) {
					t.Fatalf("pending %s mode error %v", phase, err)
				}
				if after := captureModeState(t, s, r.Path); after != before {
					t.Fatalf("failed mode changed SQL or RAM: before=%+v after=%+v", before, after)
				}
				var gotHoldID, gotHoldPath, gotHoldFile, gotPhase string
				var gotHoldHash, gotBody []byte
				if err := s.DB().QueryRow(`SELECT completion_id,nfs_path,spool_file,source_sha256 FROM spool_completion_holds WHERE spool_id=?`, r.SpoolID).
					Scan(&gotHoldID, &gotHoldPath, &gotHoldFile, &gotHoldHash); err != nil {
					t.Fatal(err)
				}
				if err := s.DB().QueryRow(`SELECT phase,sidecar_body FROM bound_completion_effects WHERE spool_id=?`, r.SpoolID).
					Scan(&gotPhase, &gotBody); err != nil {
					t.Fatal(err)
				}
				if gotHoldID != holdID || gotHoldPath != holdPath || gotHoldFile != holdFile || !bytes.Equal(gotHoldHash, holdHash) ||
					gotPhase != effectPhase || !bytes.Equal(gotBody, effectBody) || !bytes.Equal(gotBody, body) {
					t.Fatal("pending hold or sidecar body changed")
				}
			})
		}
	}
}

func TestUpdateModeFencesBoundFullHoldBeforeEffects(t *testing.T) {
	s, sp := newBoundCompletionStore(t)
	f := bornDrainingFixture(t, s, sp, "held.mov", 4, 8)
	r, err := s.CompleteBoundSpool(f.item)
	if err != nil {
		t.Fatal(err)
	}
	before := captureModeState(t, s, r.Path)
	var completionID, spoolFile string
	var digest []byte
	if err := s.DB().QueryRow(`SELECT completion_id,spool_file,source_sha256 FROM spool_completion_holds WHERE spool_id=? AND kind='bound'`, r.SpoolID).
		Scan(&completionID, &spoolFile, &digest); err != nil {
		t.Fatal(err)
	}
	var effects int
	if err := s.DB().QueryRow(`SELECT count(*) FROM bound_completion_effects WHERE spool_id=?`, r.SpoolID).Scan(&effects); err != nil || effects != 0 {
		t.Fatalf("expected hold before effects: %d %v", effects, err)
	}
	s.SetSpoolStore(nil)
	if err := s.UpdateMode(r.Path, 0600); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("hold-only mode error %v", err)
	}
	if after := captureModeState(t, s, r.Path); after != before {
		t.Fatalf("hold-only changed SQL or RAM: before=%+v after=%+v", before, after)
	}
	var gotCompletionID, gotSpoolFile string
	var gotDigest []byte
	if err := s.DB().QueryRow(`SELECT completion_id,spool_file,source_sha256 FROM spool_completion_holds WHERE spool_id=?`, r.SpoolID).
		Scan(&gotCompletionID, &gotSpoolFile, &gotDigest); err != nil {
		t.Fatal(err)
	}
	if gotCompletionID != completionID || gotSpoolFile != spoolFile || !bytes.Equal(gotDigest, digest) {
		t.Fatal("hold changed after refusal")
	}
}

func TestUpdateModeFencesPendingEffectWithoutHold(t *testing.T) {
	for _, phase := range []string{"ready", "awaiting_publication"} {
		t.Run(phase, func(t *testing.T) {
			s, sp := newBoundCompletionStore(t)
			f := bornDrainingFixture(t, s, sp, "._effect_only.mov", 4, 8)
			r, err := s.CompleteBoundSpool(f.item)
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(f.item.SpoolFile)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(body)
			if err := s.SettleBoundEffects(r, digest, body, true); err != nil {
				t.Fatal(err)
			}
			if _, err := s.DB().Exec(`UPDATE bound_completion_effects SET phase=? WHERE completion_id=?`, phase, r.CompletionID); err != nil {
				t.Fatal(err)
			}
			// Simulate an inconsistent recovered database: an effect without its
			// FULL hold must still fence chmod before SQL or RAM can change.
			if _, err := s.DB().Exec(`DELETE FROM spool_completion_holds WHERE completion_id=?`, r.CompletionID); err != nil {
				t.Fatal(err)
			}
			before := captureModeState(t, s, r.Path)
			s.SetSpoolStore(nil)
			if err := s.UpdateMode(r.Path, 0600); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("effect-only %s mode error %v", phase, err)
			}
			if after := captureModeState(t, s, r.Path); after != before {
				t.Fatalf("effect-only changed SQL or RAM: before=%+v after=%+v", before, after)
			}
			var gotPhase string
			var gotBody, gotHash []byte
			if err := s.DB().QueryRow(`SELECT phase,sidecar_body,source_sha256 FROM bound_completion_effects WHERE completion_id=?`, r.CompletionID).
				Scan(&gotPhase, &gotBody, &gotHash); err != nil {
				t.Fatal(err)
			}
			if gotPhase != phase || !bytes.Equal(gotBody, body) || !bytes.Equal(gotHash, digest[:]) {
				t.Fatal("effect-only identity or durable body changed")
			}
		})
	}
}
