package metadata

import (
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func boundModeFixture(t *testing.T) (*Store, *SpoolStore, BoundModeObservation, *os.File) {
	t.Helper()
	s, sp := newBoundCompletionStore(t)
	file := filepath.Join(t.TempDir(), "private")
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	body := []byte("exact private bytes")
	if _, err = f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	path := "mode.mov"
	expected, err := s.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	birthID, err := NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	e := MakeEntry(path, false, int64(len(body)), time.Unix(1700000000, 0), 510)
	e.Mode = 0644
	r, _, err := s.InsertBoundSpoolBirth(expected, e, file, birthID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if err = sp.MarkReady(r.SpoolID, int64(len(body)), digest[:]); err != nil {
		t.Fatal(err)
	}
	ok, err := sp.MarkDraining(r.SpoolID)
	if err != nil || !ok {
		t.Fatalf("draining %v %v", ok, err)
	}
	obs, err := s.CaptureBoundMode(r.SpoolID, r.BirthID, path, file, r.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	return s, sp, obs, f
}

func TestBoundModeExactCASAndFullHoldFence(t *testing.T) {
	s, _, obs, f := boundModeFixture(t)
	if obs.Mode.Perm() != 0644 || obs.FullSize != int64(len("exact private bytes")) {
		t.Fatalf("observation %+v", obs)
	}
	next, err := s.ApplyBoundMode(obs, 0444)
	if err != nil || next.Token.Revision != obs.Token.Revision+1 || next.Mode.Perm() != 0444 {
		t.Fatalf("CAS %+v %v", next, err)
	}
	if got := s.LookupByPath(obs.Path); got == nil || got.Version != next.Token || got.Mode.Perm() != 0444 {
		t.Fatalf("RAM %+v", got)
	}
	var mode uint32
	if err = s.DB().QueryRow("SELECT mode FROM entries WHERE path=?", obs.Path).Scan(&mode); err != nil || fs.FileMode(mode).Perm() != 0444 {
		t.Fatalf("SQL mode %o %v", mode, err)
	}
	if _, err = s.ApplyBoundMode(obs, 0644); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("stale CAS %v", err)
	}
	item := BoundDrainCommitItem{SpoolID: obs.SpoolID, BirthID: obs.BirthID, Path: obs.Path, SpoolFile: obs.SpoolFile, FileIdentity: obs.FileIdentity,
		BackingIdentity: "backing:1", Source: f, FullSize: obs.FullSize, Expected: next.Token, Mtime: time.Unix(1800000000, 0)}
	// The FULL hold itself is written by P40; a lost response still fences mode.
	_, err = s.completeBoundSpools([]BoundDrainCommitItem{item}, func() error { return errors.New("lost hold reply") }, nil)
	var pending *PendingBoundDrainCompletion
	if !errors.As(err, &pending) {
		t.Fatalf("hold reply %v", err)
	}
	var held *PendingDrainCompletion
	if _, err = s.CaptureBoundMode(obs.SpoolID, obs.BirthID, obs.Path, obs.SpoolFile, obs.FileIdentity); !errors.As(err, &held) {
		t.Fatalf("held capture %v", err)
	}
	if _, err = s.ApplyBoundMode(next, 0644); !errors.As(err, &held) {
		t.Fatalf("held SETATTR %v", err)
	}
}

func TestBoundModeLostReplyRetainsExactToken(t *testing.T) {
	s, _, obs, _ := boundModeFixture(t)
	_, err := s.applyBoundMode(obs, 0444, func() error { return errors.New("lost reply") })
	var pending *PendingBoundMode
	if !errors.As(err, &pending) || pending.Observation.Token.Revision != obs.Token.Revision+1 {
		t.Fatalf("pending %v", err)
	}
	current, err := s.CaptureBoundMode(obs.SpoolID, obs.BirthID, obs.Path, obs.SpoolFile, obs.FileIdentity)
	if err != nil || current != pending.Observation {
		t.Fatalf("durable %+v %v", current, err)
	}
}

func TestBoundModeSQLFailureCannotAcknowledge(t *testing.T) {
	s, _, obs, _ := boundModeFixture(t)
	if _, err := s.DB().Exec(`CREATE TRIGGER fail_bound_mode BEFORE UPDATE OF mode ON entries
		BEGIN SELECT RAISE(ABORT, 'forced mode failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyBoundMode(obs, 0444); err == nil {
		t.Fatal("failed Store mode update acknowledged")
	}
	current, err := s.CaptureBoundMode(obs.SpoolID, obs.BirthID, obs.Path, obs.SpoolFile, obs.FileIdentity)
	if err != nil || current != obs {
		t.Fatalf("failed CAS mutated token/mode: %+v %v", current, err)
	}
}
