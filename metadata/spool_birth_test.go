package metadata

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func birthRequest(t *testing.T, s *Store, entry *Entry, file string) (SpoolBirthReceipt, *Entry, error) {
	t.Helper()
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	expected, err := s.CaptureMirror(entry.Path)
	if err != nil {
		t.Fatal(err)
	}
	birthID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	return s.InsertBoundSpoolBirth(expected, entry, file, birthID)
}

func TestBoundSpoolBirthPositiveSeedRequiresExactImage(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(s.DB())
	s.SetSpoolStore(spool)
	file := filepath.Join(t.TempDir(), "seed")
	if err := os.WriteFile(file, []byte("seed body"), 0600); err != nil {
		t.Fatal(err)
	}
	expected, err := s.CaptureMirror("seed.mov")
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	r, born, err := s.InsertBoundSpoolBirth(expected, MakeEntry("seed.mov", false, 9, time.Now(), 100), file, nonce)
	if err != nil || born == nil || born.Size != 9 || r.Outcome != SpoolBirthCommitted {
		t.Fatalf("positive birth receipt=%+v entry=%+v err=%v", r, born, err)
	}
	row, err := spool.Get(r.SpoolID)
	if err != nil || row.Size != 9 {
		t.Fatalf("seed row=%+v err=%v", row, err)
	}
}

func TestInsertBoundSpoolBirthAtomicAndNeverAdoptsLegacy(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(s.DB())
	s.SetSpoolStore(spool)
	entry := MakeEntry("new.mov", false, 0, time.Now(), 101)
	entry.LocalOnly = true
	receipt, born, err := birthRequest(t, s, entry, filepath.Join(t.TempDir(), "new.spool"))
	if err != nil || born == nil || born.Version.Incarnation == "" || receipt.Outcome != SpoolBirthCommitted {
		t.Fatalf("birth receipt=%+v entry=%+v err=%v", receipt, born, err)
	}
	row, err := spool.Get(receipt.SpoolID)
	if err != nil || row.NFSPath != entry.Path || row.DrainState != DrainWriting {
		t.Fatalf("birth row=%+v err=%v", row, err)
	}
	var incarnation string
	if err := s.DB().QueryRow(`SELECT incarnation FROM spool_bindings WHERE spool_id=?`, receipt.SpoolID).Scan(&incarnation); err != nil || incarnation != born.Version.Incarnation {
		t.Fatalf("birth binding=%q err=%v", incarnation, err)
	}
	if got := s.LookupByPath(entry.Path); got == nil || got.Version != born.Version {
		t.Fatalf("birth mirror=%+v", got)
	}
	// A second birth cannot replace the still-live first writer or its mirror.
	other := MakeEntry(entry.Path, false, 0, time.Now(), 102)
	if _, _, err := birthRequest(t, s, other, filepath.Join(t.TempDir(), "other.spool")); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("duplicate birth err=%v", err)
	}
	if got := s.LookupByPath(entry.Path); got == nil || got.Version != born.Version {
		t.Fatalf("duplicate birth changed mirror=%+v", got)
	}
	if err := spool.Delete(receipt.SpoolID); err != nil {
		t.Fatal(err)
	}
	_, reborn, err := birthRequest(t, s, MakeEntry(entry.Path, false, 0, time.Now(), entry.Inode), filepath.Join(t.TempDir(), "reborn.spool"))
	if err != nil || reborn == nil || reborn.Version.Incarnation == born.Version.Incarnation {
		t.Fatalf("same-inode birth reused predecessor: entry=%+v err=%v", reborn, err)
	}
	// A row created by the legacy API must never receive this fresh writer's
	// binding, even when its path happens to match a future CREATE.
	legacyID, err := spool.Insert("legacy.mov", filepath.Join(t.TempDir(), "legacy.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := birthRequest(t, s, MakeEntry("legacy.mov", false, 0, time.Now(), 103), filepath.Join(t.TempDir(), "future.spool")); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy adoption err=%v", err)
	}
	var bindings int
	if err := s.DB().QueryRow(`SELECT count(*) FROM spool_bindings WHERE spool_id=?`, legacyID).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatalf("legacy row adopted binding count=%d err=%v", bindings, err)
	}
	if got := s.LookupByPath("legacy.mov"); got != nil {
		t.Fatalf("legacy adoption created mirror=%+v", got)
	}
	if _, err := s.DB().Exec(`CREATE TRIGGER refuse_birth_binding BEFORE INSERT ON spool_bindings BEGIN SELECT RAISE(ABORT, 'injected binding failure'); END`); err != nil {
		t.Fatal(err)
	}
	rollback, _, err := birthRequest(t, s, MakeEntry("rollback.mov", false, 0, time.Now(), 104), filepath.Join(t.TempDir(), "rollback.spool"))
	if err == nil || rollback.Outcome != SpoolBirthNotCommitted {
		t.Fatalf("failed binding insert accepted birth or ambiguous rollback: receipt=%+v err=%v", rollback, err)
	}
	if got := s.LookupByPath("rollback.mov"); got != nil {
		t.Fatalf("failed birth published mirror=%+v", got)
	}
	var rows int
	if err := s.DB().QueryRow(`SELECT count(*) FROM spool_entries WHERE nfs_path='rollback.mov'`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("failed birth left spool rows=%d err=%v", rows, err)
	}
	if outcome, _, err := s.ResolveBoundSpoolBirth(rollback); outcome != SpoolBirthUncertain || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("reopening inferred durable negative from rolled-back birth: outcome=%s err=%v", outcome, err)
	}
	if _, err := os.Stat(rollback.SpoolFile); err != nil {
		t.Fatalf("precommit error destroyed caller's private file: %v", err)
	}
}

func TestBoundSpoolBirthRefusesPublishingTombstone(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(s.DB())
	s.SetSpoolStore(spool)
	const name = "blocked.mov"
	if err := s.Insert(MakeEntry(name, false, 0, time.Now(), 201)); err != nil {
		t.Fatal(err)
	}
	v, err := s.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.InsertBoundSpool(v, filepath.Join(t.TempDir(), "old.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`UPDATE spool_bindings SET phase='publishing',publish_path=? WHERE spool_id=?`, name, id); err != nil {
		t.Fatal(err)
	}
	if err := spool.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := birthRequest(t, s, MakeEntry(name, false, 0, time.Now(), 202), filepath.Join(t.TempDir(), "new.spool")); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("birth crossed publishing tombstone: %v", err)
	}
	if got := s.LookupByPath(name); got == nil || got.Version != v {
		t.Fatalf("publisher mirror changed: %+v", got)
	}
}

func TestBoundSpoolBirthRejectsNoncanonicalEntry(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	e := MakeEntry("a/../clip.mov", false, 0, time.Now(), 203)
	if _, _, err := birthRequest(t, s, e, filepath.Join(t.TempDir(), "bad.spool")); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("noncanonical birth accepted: %v", err)
	}
}

func TestBoundSpoolBirthLostReplyCanBeDiscriminated(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	birthID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), birthID)
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	expected, err := s.CaptureMirror("lost.mov")
	if err != nil {
		t.Fatal(err)
	}
	receipt, entry, err := s.insertBoundSpoolBirth(expected, MakeEntry("lost.mov", false, 0, time.Now(), 204), file, birthID, func() error {
		return errors.New("injected lost reply after actual commit")
	})
	var pending *PendingSpoolBirth
	if !errors.As(err, &pending) || receipt.Outcome != SpoolBirthUncertain || entry != nil || pending.Receipt != receipt {
		t.Fatalf("postcommit reply loss: receipt=%+v entry=%+v err=%v", receipt, entry, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	resolved, born, err := reopened.ResolveBoundSpoolBirth(receipt)
	if err != nil || resolved != SpoolBirthCommitted || born == nil || born.Version.Incarnation != receipt.Incarnation {
		t.Fatalf("reopened lost reply: outcome=%s entry=%+v err=%v", resolved, born, err)
	}
	flipped := receipt
	flipped.Outcome = SpoolBirthNotCommitted
	t.Run("flipped committed outcome", func(t *testing.T) {
		if outcome, born, err := reopened.ResolveBoundSpoolBirth(flipped); err != nil || outcome != SpoolBirthCommitted || born == nil || born.Version.Incarnation != receipt.Incarnation {
			t.Fatalf("caller outcome overrode durable commit: outcome=%s entry=%+v err=%v", outcome, born, err)
		}
	})
	malformed := SpoolBirthReceipt{Outcome: SpoolBirthNotCommitted}
	t.Run("malformed negative outcome", func(t *testing.T) {
		if outcome, born, err := reopened.ResolveBoundSpoolBirth(malformed); outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("malformed outcome claimed negative: outcome=%s entry=%+v err=%v", outcome, born, err)
		}
	})
	stale := flipped
	stale.Path = "other.mov"
	t.Run("stale negative outcome", func(t *testing.T) {
		if outcome, born, err := reopened.ResolveBoundSpoolBirth(stale); outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("stale outcome claimed negative: outcome=%s entry=%+v err=%v", outcome, born, err)
		}
	})
	wrongNonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	forged := receipt
	forged.BirthID = wrongNonce
	if resolved, _, err := reopened.ResolveBoundSpoolBirth(forged); resolved != SpoolBirthUncertain || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("wrong nonce adopted row: outcome=%s err=%v", resolved, err)
	}
	replacement := filepath.Join(t.TempDir(), "different-private-inode")
	if err := os.WriteFile(replacement, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, file); err != nil {
		t.Fatal(err)
	}
	if resolved, _, err := reopened.ResolveBoundSpoolBirth(receipt); resolved != SpoolBirthUncertain || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("replaced private file adopted: outcome=%s err=%v", resolved, err)
	}
}

func TestBoundSpoolBirthStaleExpectedLeafRefusesOverwrite(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	const name = "stale.mov"
	expected, err := s.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry(name, false, 17, time.Now(), 301)); err != nil {
		t.Fatal(err)
	}
	newer := s.LookupByPath(name)
	file := filepath.Join(t.TempDir(), "stale-private")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	birthID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	receipt, _, err := s.InsertBoundSpoolBirth(expected, MakeEntry(name, false, 0, time.Now(), 302), file, birthID)
	if !errors.Is(err, ErrMirrorConflict) || receipt.Outcome != SpoolBirthNotCommitted {
		t.Fatalf("stale leaf accepted: receipt=%+v err=%v", receipt, err)
	}
	if got := s.LookupByPath(name); got == nil || got.Version != newer.Version || got.Size != newer.Size {
		t.Fatalf("stale leaf changed successor: %+v", got)
	}
}

func TestResolveBoundSpoolBirthRefusesSuccessorIncarnation(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	receipt, _, err := birthRequest(t, s, MakeEntry("successor.mov", false, 0, time.Now(), 401), filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("successor.mov"); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("successor.mov", false, 7, time.Now(), 402)); err != nil {
		t.Fatal(err)
	}
	if outcome, _, err := s.ResolveBoundSpoolBirth(receipt); outcome != SpoolBirthUncertain || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("successor incarnation adopted: outcome=%s err=%v", outcome, err)
	}
}

func TestBirthReceiptMigratesOldBindingRows(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB().Exec(`CREATE TABLE spool_bindings (
 spool_id INTEGER PRIMARY KEY, incarnation TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'bound',
 stage_path TEXT NOT NULL DEFAULT '', publish_path TEXT NOT NULL DEFAULT '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO spool_bindings(spool_id,incarnation) VALUES (1,'legacy')`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := InitSpoolSchema(s.DB()); err != nil {
			t.Fatalf("migration run %d: %v", i, err)
		}
	}
	var id, name, file, identity string
	if err := s.DB().QueryRow(`SELECT birth_id,birth_path,birth_file,birth_identity FROM spool_bindings WHERE spool_id=1`).Scan(&id, &name, &file, &identity); err != nil || id != "" || name != "" || file != "" || identity != "" {
		t.Fatalf("legacy binding adopted during migration: id=%q name=%q file=%q identity=%q err=%v", id, name, file, identity, err)
	}
}

func TestBoundSpoolBirthRefusesCacheOnlyLeaf(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	const name = "shadow.mov"
	expected, err := s.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	shadow := MakeEntry(name, false, 9, time.Now(), 901)
	s.InsertToCache(shadow)
	file := filepath.Join(t.TempDir(), "shadow.spool")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	nonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.InsertBoundSpoolBirth(expected, MakeEntry(name, false, 0, time.Now(), 902), file, nonce)
	if !errors.Is(err, ErrMirrorConflict) || r.Outcome != SpoolBirthNotCommitted {
		t.Fatalf("birth over cache-only leaf receipt=%+v err=%v", r, err)
	}
	if got := s.LookupByPath(name); got == nil || got.Inode != shadow.Inode || got.Size != shadow.Size {
		t.Fatalf("cache-only leaf overwritten: %+v", got)
	}
}

func TestBoundSpoolBirthRefusesPreparedCreate(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	const name = "reserved.mov"
	expected, err := s.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PrepareCreate(expected, "test-mount"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "reserved.spool")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	nonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.InsertBoundSpoolBirth(expected, MakeEntry(name, false, 0, time.Now(), 903), file, nonce)
	if !errors.Is(err, ErrMirrorConflict) || r.Outcome != SpoolBirthNotCommitted {
		t.Fatalf("birth over prepared create receipt=%+v err=%v", r, err)
	}
}

func TestBoundSpoolBirthCheckSpoolRecoveryRefusesWriting(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	r, _, err := birthRequest(t, s, MakeEntry("boot.mov", false, 0, time.Now(), 904), filepath.Join(t.TempDir(), "boot.spool"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckSpoolRecovery(); err == nil {
		t.Fatalf("boot preflight admitted unresolved born writing row %+v", r)
	}
	row, err := NewSpoolStore(s.DB()).Get(r.SpoolID)
	if err != nil || row.DrainState != DrainWriting {
		t.Fatalf("preflight mutated writing row: row=%+v err=%v", row, err)
	}
}

func TestBoundSpoolBirthNoReturnByNonceReopens(t *testing.T) {
	const name = "no-return.mov"
	if os.Getenv("JM_BIRTH_CRASH_CHILD") == "1" {
		dbPath, file, nonce := os.Getenv("JM_BIRTH_DB"), os.Getenv("JM_BIRTH_FILE"), os.Getenv("JM_BIRTH_NONCE")
		s, err := Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := InitSpoolSchema(s.DB()); err != nil {
			t.Fatal(err)
		}
		s.SetSpoolStore(NewSpoolStore(s.DB()))
		expected, err := s.CaptureMirror(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = s.insertBoundSpoolBirth(expected, MakeEntry(name, false, 0, time.Now(), 905), file, nonce, func() error {
			os.Exit(47) // a real COMMIT happened, but no receipt reaches the caller
			return nil
		})
		t.Fatalf("post-commit crash seam did not run: %v", err)
	}
	dir := t.TempDir()
	dbPath, file := filepath.Join(dir, "mirror.db"), filepath.Join(dir, "private.spool")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	identity, err := privateBirthFileIdentity(file)
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	intent := SpoolBirthIntent{BirthID: nonce, Path: name, SpoolFile: file, FileIdentity: identity}
	cmd := exec.Command(os.Args[0], "-test.run=^TestBoundSpoolBirthNoReturnByNonceReopens$")
	cmd.Env = append(os.Environ(), "JM_BIRTH_CRASH_CHILD=1", "JM_BIRTH_DB="+dbPath, "JM_BIRTH_FILE="+file, "JM_BIRTH_NONCE="+nonce)
	err = cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 47 {
		t.Fatalf("child exit=%v, want exact post-COMMIT exit 47", err)
	}
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := InitSpoolSchema(reopened.DB()); err != nil {
		t.Fatal(err)
	}
	r, born, err := reopened.ResolveBoundSpoolBirthByNonce(intent)
	if err != nil || r.Outcome != SpoolBirthCommitted || r.SpoolID <= 0 || born == nil || born.Version.Incarnation != r.Incarnation {
		t.Fatalf("by-nonce recovery receipt=%+v entry=%+v err=%v", r, born, err)
	}
	if err := reopened.CheckSpoolRecovery(); err == nil {
		t.Fatal("reopened birth admitted boot scrub before reconciliation")
	}
}

func TestResolveBoundSpoolBirthByNonceRefusesAmbiguity(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	s.SetSpoolStore(NewSpoolStore(s.DB()))
	file := filepath.Join(t.TempDir(), "source")
	r, _, err := birthRequest(t, s, MakeEntry("owned.mov", false, 0, time.Now(), 906), file)
	if err != nil {
		t.Fatal(err)
	}
	intent := SpoolBirthIntent{BirthID: r.BirthID, Path: r.Path, SpoolFile: r.SpoolFile, FileIdentity: r.FileIdentity}
	if got, born, err := s.ResolveBoundSpoolBirthByNonce(intent); err != nil || got.Outcome != SpoolBirthCommitted || born == nil || got.SpoolID != r.SpoolID {
		t.Fatalf("exact discovery=(%+v,%+v,%v)", got, born, err)
	}
	otherNonce, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign")
	if err := os.WriteFile(foreign, nil, 0600); err != nil {
		t.Fatal(err)
	}
	foreignIdentity, err := privateBirthFileIdentity(foreign)
	if err != nil {
		t.Fatal(err)
	}
	legacyFile := filepath.Join(t.TempDir(), "legacy-source")
	if err := os.WriteFile(legacyFile, []byte("legacy bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSpoolStore(s.DB()).Insert("legacy.mov", legacyFile); err != nil {
		t.Fatal(err)
	}
	legacyIdentity, err := privateBirthFileIdentity(legacyFile)
	if err != nil {
		t.Fatal(err)
	}
	legacyIntent := SpoolBirthIntent{BirthID: otherNonce, Path: "legacy.mov", SpoolFile: legacyFile, FileIdentity: legacyIdentity}
	if got, born, err := s.ResolveBoundSpoolBirthByNonce(legacyIntent); got.Outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("legacy row adopted: receipt=%+v entry=%+v err=%v", got, born, err)
	}
	for label, changed := range map[string]SpoolBirthIntent{
		"unknown nonce":   {BirthID: otherNonce, Path: intent.Path, SpoolFile: intent.SpoolFile, FileIdentity: intent.FileIdentity},
		"other path":      {BirthID: intent.BirthID, Path: "other.mov", SpoolFile: intent.SpoolFile, FileIdentity: intent.FileIdentity},
		"foreign file":    {BirthID: intent.BirthID, Path: intent.Path, SpoolFile: foreign, FileIdentity: foreignIdentity},
		"forged identity": {BirthID: intent.BirthID, Path: intent.Path, SpoolFile: intent.SpoolFile, FileIdentity: foreignIdentity},
	} {
		got, born, err := s.ResolveBoundSpoolBirthByNonce(changed)
		if got.Outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
			t.Errorf("%s adopted: receipt=%+v entry=%+v err=%v", label, got, born, err)
		}
	}
	if err := s.Delete("owned.mov"); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(MakeEntry("owned.mov", false, 8, time.Now(), 907)); err != nil {
		t.Fatal(err)
	}
	if got, born, err := s.ResolveBoundSpoolBirthByNonce(intent); got.Outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("successor adopted: receipt=%+v entry=%+v err=%v", got, born, err)
	}
	if _, err := s.DB().Exec(`DELETE FROM spool_bindings WHERE birth_id=?`, intent.BirthID); err != nil {
		t.Fatal(err)
	}
	if got, born, err := s.ResolveBoundSpoolBirthByNonce(intent); got.Outcome != SpoolBirthUncertain || born != nil || !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("missing binding inferred no commit: receipt=%+v entry=%+v err=%v", got, born, err)
	}
}

func TestBoundSpoolBirthRecoveryFencesAllBindingPhases(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := InitSpoolSchema(s.DB()); err != nil {
		t.Fatal(err)
	}
	spool := NewSpoolStore(s.DB())
	s.SetSpoolStore(spool)
	r, _, err := birthRequest(t, s, MakeEntry("phase.mov", false, 0, time.Now(), 908), filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []struct {
		name, sql  string
		rowPresent bool
	}{
		{"writing", "", true},
		{"ready", `UPDATE spool_entries SET drain_state='ready' WHERE id=?`, true},
		{"completed", `UPDATE spool_bindings SET phase='completed' WHERE spool_id=?`, true},
		{"row missing", `DELETE FROM spool_entries WHERE id=?`, false},
	} {
		if phase.sql != "" {
			if _, err := s.DB().Exec(phase.sql, r.SpoolID); err != nil {
				t.Fatal(err)
			}
		}
		var pending *PendingSpoolBirthRecovery
		if err := spool.CheckSpoolBirthRecovery(); !errors.As(err, &pending) || len(pending.Births) != 1 || pending.Births[0].BirthID != r.BirthID || pending.Births[0].DrainState.Valid != phase.rowPresent {
			t.Fatalf("%s guard=%v pending=%+v", phase.name, err, pending)
		}
	}
}
