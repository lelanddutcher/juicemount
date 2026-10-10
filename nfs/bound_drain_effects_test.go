package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestBoundEffectsPrivateRetirementFreshBody(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "mirror.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(meta)
	spool, err := NewSpoolStore(filepath.Join(base, "spool"), 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	name := "._exact.mov"
	seed := []byte("seed")
	custody, err := spool.PreparePrivateBirth(name, seed)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := db.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(name, false, int64(len(seed)), time.Unix(1700000000, 0), 901)
	birth, _, err := custody.CommitPrivateBirth(db, expected, e)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("full completed Finder body")
	if err := custody.image.Truncate(int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := custody.image.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	if err := custody.image.Sync(); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(body)
	if err := spool.meta.MarkReady(birth.SpoolID, int64(len(body)), hash[:]); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.meta.MarkDraining(birth.SpoolID); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := db.CaptureBoundMode(birth.SpoolID, birth.BirthID, name, birth.SpoolFile, birth.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backing")
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := dst.Write(body); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := drainer.boundPhases.begin(db, obs, custody.image, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1800000000, 0)
	obs, err = lease.settle(mt, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	obs, backing, err := lease.transferBatch()
	if err != nil {
		t.Fatal(err)
	}
	item := metadata.BoundDrainCommitItem{SpoolID: birth.SpoolID, BirthID: birth.BirthID, Path: name, SpoolFile: birth.SpoolFile, FileIdentity: birth.FileIdentity, BackingIdentity: backing, Source: custody.image, FullSize: int64(len(body)), Expected: obs.Token, Mtime: mt}
	results, err := db.BatchCompleteBoundSpools([]metadata.BoundDrainCommitItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainer.boundPhases.acceptBatchResults([]*boundFDPhase{lease}, results); err != nil {
		t.Fatal(err)
	}
	effects, err := lease.claimEffects(spool)
	if err != nil {
		t.Fatal(err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop dropped effects owner")
	}
	if err := effects.apply(nil, nil); err != nil {
		t.Fatal(err)
	}
	receipt := results[0]
	root := spool.root
	wrong := filepath.Join(t.TempDir(), "wrong")
	if err := os.MkdirAll(filepath.Join(wrong, "files"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.PrepareBoundRetirement(receipt, custody.image, wrong); err == nil {
		t.Fatal("wrong root accepted")
	}
	var pending *metadata.PendingBoundEffects
	if err := effects.retire(); !errors.As(err, &pending) || !errors.Is(err, metadata.ErrBoundPublicationPending) {
		t.Fatalf("local settlement must remain pending: %v", err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop released unpublished effects")
	}
	if _, err := os.Stat(birth.SpoolFile); err != nil {
		t.Fatalf("retained image: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "birth-intents", birth.BirthID+".json")); err != nil {
		t.Fatalf("retained intent: %v", err)
	}
	var phase string
	if err := db.DB().QueryRow(`SELECT phase FROM bound_completion_effects WHERE spool_id=?`, birth.SpoolID).Scan(&phase); err != nil || phase != "awaiting_publication" {
		t.Fatalf("settled phase %q %v", phase, err)
	}
	if err := effects.retire(); !errors.Is(err, metadata.ErrBoundPublicationPending) {
		t.Fatalf("exact awaiting retry must remain pending: %v", err)
	}
	if _, err := db.DB().Exec(`UPDATE bound_completion_effects SET source_sha256=zeroblob(32) WHERE spool_id=?`, birth.SpoolID); err != nil {
		t.Fatal(err)
	}
	if err := effects.retire(); err == nil || errors.Is(err, metadata.ErrBoundPublicationPending) {
		t.Fatalf("mismatched awaiting digest accepted: %v", err)
	}
	if _, err := db.DB().Exec(`UPDATE bound_completion_effects SET source_sha256=? WHERE spool_id=?`, hash[:], birth.SpoolID); err != nil {
		t.Fatal(err)
	}
	// The remaining assertions cover historical retired sidecar readers. Only
	// this test fixture synthesizes that old database state; production has no
	// qualified receipt or retirement path in this slice.
	for _, q := range []string{
		`UPDATE bound_completion_effects SET phase='retired' WHERE spool_id=?`,
		`DELETE FROM spool_completion_holds WHERE spool_id=?`,
		`DELETE FROM spool_bindings WHERE spool_id=?`,
		`DELETE FROM spool_entries WHERE id=?`,
	} {
		if _, err := db.DB().Exec(q, birth.SpoolID); err != nil {
			t.Fatal(err)
		}
	}
	image, exists, valid, err := db.BoundSidecarBody(name)
	if err != nil || !exists || !valid || !bytes.Equal(image.Body, body) || image.Token != receipt.Published {
		t.Fatalf("body %+v %v %v %v", image, exists, valid, err)
	}
	h := &JuiceMountHandler{store: db, sidecar: newSidecarCache(), activeWriters: make(map[string]int)}
	served := make([]byte, len(body))
	if n, ok := h.boundSidecarServe(name, served, 0); !ok || n != len(body) || !bytes.Equal(served, body) {
		t.Fatalf("exact serve (%d,%v) %q", n, ok, served)
	}
	seeded, seedToken, ok := h.offlineBoundMetadataSeed(name)
	if !ok || !bytes.Equal(seeded, body) || seedToken != receipt.Published {
		t.Fatalf("exact seed %q %+v %v", seeded, seedToken, ok)
	}
	h.activeWriters[name] = 1
	if n, ok := h.boundSidecarServe(name, served, 0); ok || n != 0 {
		t.Fatal("served during active write")
	}
	if _, _, ok := h.offlineBoundMetadataSeed(name); ok {
		t.Fatal("seeded during active write")
	}
	delete(h.activeWriters, name)
	if err := db.UpdateMode(name, 0600); err != nil {
		t.Fatal(err)
	}
	modeBody, exists, valid, err := db.BoundSidecarBody(name)
	if err != nil || !exists || !valid || !bytes.Equal(modeBody.Body, body) || modeBody.Token.Revision != receipt.Published.Revision+1 {
		t.Fatalf("mode-only body %+v %v %v %v", modeBody, exists, valid, err)
	}
	spool.Stop()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	got, exists, valid, err := fresh.BoundSidecarBody(name)
	if err != nil || !exists || !valid || !bytes.Equal(got.Body, body) || got.Token != modeBody.Token {
		t.Fatalf("fresh body %+v %v %v %v", got, exists, valid, err)
	}
	freshHandler := &JuiceMountHandler{store: fresh, sidecar: newSidecarCache()}
	other := bytes.Repeat([]byte{'S'}, len(body))
	freshHandler.sidecar.put(name, body, mt.Unix(), int64(len(body))) // weak fallback must not qualify bound generation
	successor := metadata.MakeEntry(name, false, int64(len(body)), mt, 902)
	successor.Mode = 0644
	if err := fresh.Insert(successor); err != nil {
		t.Fatal(err)
	}
	current := fresh.LookupByPath(name)
	if current == nil || current.Version.Incarnation == got.Token.Incarnation || current.Size != int64(len(body)) || current.Mtime.Unix() != mt.Unix() || current.Mode != 0644 {
		t.Fatalf("successor %+v", current)
	}
	if n, ok := freshHandler.boundSidecarServe(name, served, 0); ok || n != 0 {
		t.Fatal("served predecessor after same-tag successor")
	}
	if _, _, ok := freshHandler.offlineBoundMetadataSeed(name); ok {
		t.Fatal("seeded predecessor after same-tag successor")
	}
	if _, err := fresh.PublishExpected(got.Token, metadata.MakeEntry(name, false, int64(len(other)), mt, 903)); err == nil {
		t.Fatal("stale seed token passed producer CAS")
	}
}

func TestBoundEffectsTerminalSQLFailureRetainsLease(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "mirror.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(meta)
	spool, err := NewSpoolStore(filepath.Join(base, "spool"), 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	name := "._exact.mov"
	seed := []byte("seed")
	custody, err := spool.PreparePrivateBirth(name, seed)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := db.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(name, false, int64(len(seed)), time.Unix(1700000000, 0), 901)
	birth, _, err := custody.CommitPrivateBirth(db, expected, e)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("full completed Finder body")
	if err := custody.image.Truncate(int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := custody.image.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	if err := custody.image.Sync(); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(body)
	if err := spool.meta.MarkReady(birth.SpoolID, int64(len(body)), hash[:]); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.meta.MarkDraining(birth.SpoolID); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := db.CaptureBoundMode(birth.SpoolID, birth.BirthID, name, birth.SpoolFile, birth.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backing")
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := dst.Write(body); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := drainer.boundPhases.begin(db, obs, custody.image, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1800000000, 0)
	obs, err = lease.settle(mt, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	obs, backing, err := lease.transferBatch()
	if err != nil {
		t.Fatal(err)
	}
	item := metadata.BoundDrainCommitItem{SpoolID: birth.SpoolID, BirthID: birth.BirthID, Path: name, SpoolFile: birth.SpoolFile, FileIdentity: birth.FileIdentity, BackingIdentity: backing, Source: custody.image, FullSize: int64(len(body)), Expected: obs.Token, Mtime: mt}
	results, err := db.BatchCompleteBoundSpools([]metadata.BoundDrainCommitItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainer.boundPhases.acceptBatchResults([]*boundFDPhase{lease}, results); err != nil {
		t.Fatal(err)
	}
	effects, err := lease.claimEffects(spool)
	if err != nil {
		t.Fatal(err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop dropped effects owner")
	}
	if err := effects.apply(nil, nil); err != nil {
		t.Fatal(err)
	}
	receipt := results[0]
	_ = receipt
	before, _ := spool.Capacity()
	if _, err := db.DB().Exec(`CREATE TRIGGER refuse_bound_settle BEFORE UPDATE OF phase ON bound_completion_effects WHEN NEW.phase='awaiting_publication' BEGIN SELECT RAISE(ABORT,'settle denied'); END`); err != nil {
		t.Fatal(err)
	}
	err = effects.retire()
	var pending *metadata.PendingBoundEffects
	if !errors.As(err, &pending) {
		t.Fatalf("expected typed pending, got %v", err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop dropped uncertain source")
	}
	after, _ := spool.Capacity()
	if after != before {
		t.Fatalf("capacity released %d -> %d", before, after)
	}
	if _, err := custody.image.Stat(); err != nil {
		t.Fatalf("source FD lost %v", err)
	}
	if info, err := custody.image.Stat(); err != nil {
		t.Fatal(err)
	} else if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 {
		t.Fatalf("source not retained linked: %+v", info.Sys())
	}
	if _, err := os.Stat(birth.SpoolFile); err != nil {
		t.Fatalf("image path missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(spool.root, "birth-intents", birth.BirthID+".json")); err != nil {
		t.Fatalf("intent path missing: %v", err)
	}
	row, err := spool.meta.Get(birth.SpoolID)
	if err != nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row %+v %v", row, err)
	}
	var holds int
	if err := db.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, birth.SpoolID).Scan(&holds); err != nil || holds != 1 {
		t.Fatalf("hold %d %v", holds, err)
	}
	var phase string
	var durable []byte
	if err := db.DB().QueryRow(`SELECT phase,sidecar_body FROM bound_completion_effects WHERE spool_id=?`, birth.SpoolID).Scan(&phase, &durable); err != nil || phase != "ready" || !bytes.Equal(durable, body) {
		t.Fatalf("ready effects %q %q %v", phase, durable, err)
	}
	if err := effects.retire(); !errors.As(err, &pending) {
		t.Fatalf("failed local transition retry claimed settlement: %v", err)
	}
	if drainer.Stop(0) {
		t.Fatal("retry released uncertain lease")
	}
}

func TestBoundEffectsDelayedInvalidationPreservesSuccessor(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "mirror.db")
	db, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(meta)
	spool, err := NewSpoolStore(filepath.Join(base, "spool"), 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	name := "._exact.mov"
	seed := []byte("seed")
	custody, err := spool.PreparePrivateBirth(name, seed)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := db.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(name, false, int64(len(seed)), time.Unix(1700000000, 0), 901)
	birth, _, err := custody.CommitPrivateBirth(db, expected, e)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("full completed Finder body")
	if err := custody.image.Truncate(int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := custody.image.WriteAt(body, 0); err != nil {
		t.Fatal(err)
	}
	if err := custody.image.Sync(); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(body)
	if err := spool.meta.MarkReady(birth.SpoolID, int64(len(body)), hash[:]); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.meta.MarkDraining(birth.SpoolID); err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := db.CaptureBoundMode(birth.SpoolID, birth.BirthID, name, birth.SpoolFile, birth.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backing")
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if _, err := dst.Write(body); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := drainer.boundPhases.begin(db, obs, custody.image, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1800000000, 0)
	obs, err = lease.settle(mt, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	obs, backing, err := lease.transferBatch()
	if err != nil {
		t.Fatal(err)
	}
	item := metadata.BoundDrainCommitItem{SpoolID: birth.SpoolID, BirthID: birth.BirthID, Path: name, SpoolFile: birth.SpoolFile, FileIdentity: birth.FileIdentity, BackingIdentity: backing, Source: custody.image, FullSize: int64(len(body)), Expected: obs.Token, Mtime: mt}
	results, err := db.BatchCompleteBoundSpools([]metadata.BoundDrainCommitItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if err := drainer.boundPhases.acceptBatchResults([]*boundFDPhase{lease}, results); err != nil {
		t.Fatal(err)
	}
	effects, err := lease.claimEffects(spool)
	if err != nil {
		t.Fatal(err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop dropped effects owner")
	}
	receipt := results[0]
	_ = receipt
	mb := NewMemoryBuffer(1<<20, 8<<20)
	defer mb.Stop()
	fuseFile := filepath.Join(t.TempDir(), "sidecar-source")
	if err := os.WriteFile(fuseFile, body, 0600); err != nil {
		t.Fatal(err)
	}
	waitBody := func(want []byte) {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if got := mb.Get(name, int64(len(want)), fuseFile); bytes.Equal(got, want) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("memory buffer never loaded %q", want)
	}
	mb.Get(name, int64(len(body)), fuseFile)
	waitBody(body)
	h := &JuiceMountHandler{store: db, memBuf: mb, sidecar: newSidecarCache()}
	h.sidecar.put(name, body, mt.Unix(), int64(len(body)))
	beforeMem, beforeSide := mb.captureEntry(name), h.sidecar.captureEntry(name)
	paused := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	effects.testBeforeCacheInvalidate = func() { close(paused); <-resume }
	go func() { done <- effects.apply(h, nil) }()
	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("apply returned before invalidation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("effects did not reach invalidation barrier")
	}
	successorBody := bytes.Repeat([]byte{'S'}, len(body))
	successor := metadata.MakeEntry(name, false, int64(len(body)), mt, 902)
	successor.Mode = 0600
	if err := db.Insert(successor); err != nil {
		t.Fatal(err)
	}
	current := db.LookupByPath(name)
	if current == nil || current.Version.Incarnation == receipt.Published.Incarnation || current.Size != int64(len(body)) || current.Mtime.Unix() != mt.Unix() || current.Mode != 0600 {
		t.Fatalf("successor %+v", current)
	}
	h.sidecar.put(name, successorBody, mt.Unix(), int64(len(body)))
	mb.Invalidate(name)
	if err := os.WriteFile(fuseFile, successorBody, 0600); err != nil {
		t.Fatal(err)
	}
	mb.Get(name, int64(len(body)), fuseFile)
	waitBody(successorBody)
	afterMem, afterSide := mb.captureEntry(name), h.sidecar.captureEntry(name)
	if afterMem == nil || afterSide == nil || afterMem == beforeMem || afterSide == beforeSide {
		t.Fatal("successor cache objects not published")
	}
	close(resume)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("apply %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("effects did not finish")
	}
	if got := db.LookupByPath(name); got == nil || got.Version != current.Version || got.Mode != 0600 {
		t.Fatalf("successor mirror lost %+v", got)
	}
	if got, ok := h.sidecar.get(name, mt.Unix(), int64(len(body))); !ok || !bytes.Equal(got, successorBody) {
		t.Fatalf("successor sidecar %q %v", got, ok)
	}
	if got := mb.Get(name, int64(len(body)), fuseFile); !bytes.Equal(got, successorBody) {
		t.Fatalf("successor mem %q", got)
	}
	if mb.captureEntry(name) != afterMem || h.sidecar.captureEntry(name) != afterSide {
		t.Fatal("predecessor invalidated successor object")
	}
}

// P54 runs two explicit local-settlement modes in separate persistent fixtures.
// The child exits with open source and database descriptors after SQL.
type p53Config struct {
	Case        string `json:"case"`
	FixtureRoot string `json:"fixture_root"`
	Nonce       string `json:"nonce"`
	ReceiptDir  string `json:"receipt_dir"`
}

type p53ChildReceipt struct {
	Case              string      `json:"case"`
	ExpectedPhase     string      `json:"expected_phase"`
	RetirementPending bool        `json:"retirement_pending"`
	SQLCause          string      `json:"sql_cause"`
	SpoolRootIdentity string      `json:"spool_root_identity"`
	FilesIdentity     string      `json:"files_identity"`
	IntentsIdentity   string      `json:"intents_identity"`
	IntentIdentity    string      `json:"intent_identity"`
	IntentSHA256      string      `json:"intent_sha256"`
	IntentSize        int64       `json:"intent_size"`
	Nonce             string      `json:"nonce"`
	PID               int         `json:"pid"`
	ExecutableSHA256  string      `json:"executable_sha256"`
	FixtureIdentity   string      `json:"fixture_identity"`
	SpoolID           int64       `json:"spool_id"`
	BirthID           string      `json:"birth_id"`
	CompletionID      string      `json:"completion_id"`
	Incarnation       string      `json:"incarnation"`
	PublishedRevision int64       `json:"published_revision"`
	SpoolFile         string      `json:"spool_file"`
	FileIdentity      string      `json:"file_identity"`
	BackingIdentity   string      `json:"backing_identity"`
	SourceSHA256      string      `json:"source_sha256"`
	SourceSize        int64       `json:"source_size"`
	FDNlink           uint64      `json:"fd_nlink"`
	ImagePresent      bool        `json:"image_present"`
	IntentPresent     bool        `json:"intent_present"`
	UnlinkObserved    bool        `json:"unlink_observed"`
	CapacityBefore    int64       `json:"capacity_before"`
	CapacityAfter     int64       `json:"capacity_after"`
	DestinationSHA256 string      `json:"destination_sha256"`
	SQL               p53Snapshot `json:"sql"`
}

type p53Snapshot struct {
	SpoolID            int64  `json:"spool_id"`
	PathRows           int    `json:"path_rows"`
	BindingRows        int    `json:"binding_rows"`
	HoldRows           int    `json:"hold_rows"`
	EffectsRows        int    `json:"effects_rows"`
	NonNullBodies      int    `json:"nonnull_sql_bodies"`
	Path               string `json:"path"`
	SpoolFile          string `json:"spool_file"`
	Size               int64  `json:"size"`
	SHA256             string `json:"sha256"`
	DrainState         string `json:"drain_state"`
	PunchedEnd         int64  `json:"punched_end"`
	BirthID            string `json:"birth_id"`
	FileIdentity       string `json:"file_identity"`
	BindingPhase       string `json:"binding_phase"`
	BindingIncarnation string `json:"binding_incarnation"`
	BindingPath        string `json:"binding_path"`
	BindingFile        string `json:"binding_file"`
	CompletionID       string `json:"completion_id"`
	HoldKind           string `json:"hold_kind"`
	HoldSHA256         string `json:"hold_sha256"`
	HoldIncarnation    string `json:"hold_incarnation"`
	HoldRevision       int64  `json:"hold_revision"`
	HoldBirthID        string `json:"hold_birth_id"`
	HoldPath           string `json:"hold_path"`
	HoldFile           string `json:"hold_file"`
	HoldIdentity       string `json:"hold_identity"`
	HoldBacking        string `json:"hold_backing"`
	HoldSize           int64  `json:"hold_size"`
	EffectsPhase       string `json:"effects_phase"`
	EffectsBodyNull    bool   `json:"effects_body_null"`
	EffectsReconcile   int    `json:"effects_reconcile_pending"`
	EffectsBirthID     string `json:"effects_birth_id"`
	EffectsPath        string `json:"effects_path"`
	EffectsIncarnation string `json:"effects_incarnation"`
	EffectsRevision    int64  `json:"effects_revision"`
	EffectsSHA256      string `json:"effects_sha256"`
	EffectsSize        int64  `json:"effects_size"`
	MirrorIncarnation  string `json:"mirror_incarnation"`
	MirrorRevision     int64  `json:"mirror_revision"`
}

func p53DirectoryIdentity(name string) (string, error) {
	fi, err := os.Lstat(name)
	if err != nil || !fi.IsDir() {
		return "", fmt.Errorf("fixture directory: %v", err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("fixture directory has no device/inode")
	}
	return fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino), nil
}

func p53Body() []byte {
	body := make([]byte, 64<<10)
	for i := range body {
		body[i] = byte((i*131 + 17) % 251)
	}
	return body
}

func p53WriteJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	err = enc.Encode(value)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = d.Sync()
	closeErr = d.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func p53ReadConfig(t *testing.T) p53Config {
	t.Helper()
	name := os.Getenv("JM_P54_CONFIG")
	if name == "" || !filepath.IsAbs(name) || filepath.Clean(name) != name {
		t.Fatal("JM_P54_CONFIG must be an absolute clean path")
	}
	fi, err := os.Lstat(name)
	if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != 0600 {
		t.Fatalf("config must be regular mode 0600: %v", err)
	}
	raw, err := os.ReadFile(name)
	if err != nil || len(raw) > 4096 {
		t.Fatalf("read bounded config: %v", err)
	}
	var c p53Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if c.Case != "sql_failure" && c.Case != "local_success" {
		t.Fatal("case must be sql_failure or local_success")
	}
	if b, err := hex.DecodeString(c.Nonce); err != nil || len(b) != 16 || len(c.Nonce) != 32 || strings.ToLower(c.Nonce) != c.Nonce {
		t.Fatal("nonce must be 128-bit lowercase hex")
	}
	for _, dir := range []string{c.FixtureRoot, c.ReceiptDir} {
		if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
			t.Fatal("fixture and receipt paths must be absolute and clean")
		}
		fi, err := os.Lstat(dir)
		if err != nil || !fi.IsDir() || fi.Mode().Perm() != 0700 {
			t.Fatalf("directory must exist with mode 0700: %s: %v", dir, err)
		}
		resolved, err := filepath.EvalSymlinks(dir)
		if err != nil || resolved != dir {
			t.Fatalf("directory must be canonical: %s: %v", dir, err)
		}
	}
	if c.FixtureRoot == c.ReceiptDir || strings.HasPrefix(c.ReceiptDir, c.FixtureRoot+string(os.PathSeparator)) ||
		strings.HasPrefix(c.FixtureRoot, c.ReceiptDir+string(os.PathSeparator)) {
		t.Fatal("fixture and receipts must be separate")
	}
	return c
}

func p53FileHash(name string) (string, int64, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		return "", 0, fmt.Errorf("not a regular file: %s: %v", name, err)
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

func p53InspectSQL(db *metadata.Store, receipt p53ChildReceipt) (p53Snapshot, error) {
	var s p53Snapshot
	var rowHash, holdHash []byte
	var bodyNull int
	err := db.DB().QueryRow(`SELECT id,nfs_path,spool_file,size,sha256,drain_state,punched_end
	 FROM spool_entries WHERE id=?`, receipt.SpoolID).
		Scan(&s.SpoolID, &s.Path, &s.SpoolFile, &s.Size, &rowHash, &s.DrainState, &s.PunchedEnd)
	if err != nil {
		return s, fmt.Errorf("spool row: %v", err)
	}
	s.SHA256 = hex.EncodeToString(rowHash)
	if err := db.DB().QueryRow(`SELECT count(*) FROM spool_entries WHERE nfs_path='clip.mov'`).Scan(&s.PathRows); err != nil {
		return s, fmt.Errorf("path row count: %v", err)
	}
	err = db.DB().QueryRow(`SELECT birth_id,birth_identity,phase,incarnation,birth_path,birth_file
	 FROM spool_bindings WHERE spool_id=?`, receipt.SpoolID).
		Scan(&s.BirthID, &s.FileIdentity, &s.BindingPhase, &s.BindingIncarnation, &s.BindingPath, &s.BindingFile)
	if err != nil {
		return s, fmt.Errorf("bound binding: %v", err)
	}
	err = db.DB().QueryRow(`SELECT completion_id,kind,source_sha256,incarnation,published_revision,
	 birth_id,nfs_path,spool_file,birth_identity,backing_identity,size
	 FROM spool_completion_holds WHERE spool_id=?`, receipt.SpoolID).
		Scan(&s.CompletionID, &s.HoldKind, &holdHash, &s.HoldIncarnation, &s.HoldRevision,
			&s.HoldBirthID, &s.HoldPath, &s.HoldFile, &s.HoldIdentity, &s.HoldBacking, &s.HoldSize)
	if err != nil {
		return s, fmt.Errorf("completion hold: %v", err)
	}
	s.HoldSHA256 = hex.EncodeToString(holdHash)
	var effectsHash []byte
	err = db.DB().QueryRow(`SELECT phase,sidecar_body IS NULL,reconcile_pending,birth_id,nfs_path,
	 incarnation,published_revision,source_sha256,size
	 FROM bound_completion_effects WHERE completion_id=? AND spool_id=?`, receipt.CompletionID, receipt.SpoolID).
		Scan(&s.EffectsPhase, &bodyNull, &s.EffectsReconcile, &s.EffectsBirthID, &s.EffectsPath,
			&s.EffectsIncarnation, &s.EffectsRevision, &effectsHash, &s.EffectsSize)
	if err != nil {
		return s, fmt.Errorf("bound effects: %v", err)
	}
	s.EffectsBodyNull = bodyNull == 1
	s.EffectsSHA256 = hex.EncodeToString(effectsHash)
	for _, q := range []struct {
		sql  string
		dest *int
	}{
		{`SELECT count(*) FROM spool_bindings WHERE spool_id=?`, &s.BindingRows},
		{`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, &s.HoldRows},
		{`SELECT count(*) FROM bound_completion_effects WHERE spool_id=?`, &s.EffectsRows},
	} {
		if err := db.DB().QueryRow(q.sql, receipt.SpoolID).Scan(q.dest); err != nil {
			return s, fmt.Errorf("bound tuple count: %v", err)
		}
	}
	if err := db.DB().QueryRow(`SELECT count(*) FROM bound_completion_effects WHERE sidecar_body IS NOT NULL`).Scan(&s.NonNullBodies); err != nil {
		return s, fmt.Errorf("SQL body backup count: %v", err)
	}
	m := db.LookupByPath("clip.mov")
	if m == nil {
		return s, errors.New("missing clip.mov mirror")
	}
	s.MirrorIncarnation, s.MirrorRevision = m.Version.Incarnation, m.Version.Revision
	if s.SpoolID != receipt.SpoolID || s.PathRows != 1 || s.BindingRows != 1 || s.HoldRows != 1 ||
		s.EffectsRows != 1 || s.NonNullBodies != 0 || s.Path != "clip.mov" || s.SpoolFile != receipt.SpoolFile ||
		s.Size != receipt.SourceSize || s.SHA256 != receipt.SourceSHA256 || s.DrainState != string(metadata.DrainDone) ||
		s.PunchedEnd != 0 || s.BirthID != receipt.BirthID || s.FileIdentity != receipt.FileIdentity ||
		s.BindingPhase != "bound" || s.BindingIncarnation != receipt.Incarnation ||
		s.BindingPath != "clip.mov" || s.BindingFile != receipt.SpoolFile ||
		s.CompletionID != receipt.CompletionID || s.HoldKind != "bound" ||
		s.HoldSHA256 != receipt.SourceSHA256 || s.HoldIncarnation != receipt.Incarnation ||
		s.HoldRevision != receipt.PublishedRevision || s.HoldBirthID != receipt.BirthID ||
		s.HoldPath != "clip.mov" || s.HoldFile != receipt.SpoolFile ||
		s.HoldIdentity != receipt.FileIdentity || s.HoldBacking != receipt.BackingIdentity ||
		s.HoldSize != receipt.SourceSize || s.EffectsPhase != receipt.ExpectedPhase || !s.EffectsBodyNull ||
		s.EffectsReconcile != 1 || s.EffectsBirthID != receipt.BirthID ||
		s.EffectsPath != "clip.mov" || s.EffectsIncarnation != receipt.Incarnation ||
		s.EffectsRevision != receipt.PublishedRevision || s.EffectsSHA256 != receipt.SourceSHA256 ||
		s.EffectsSize != receipt.SourceSize || s.MirrorIncarnation != receipt.Incarnation ||
		s.MirrorRevision != receipt.PublishedRevision {
		return s, fmt.Errorf("uncertain SQL tuple changed: %+v", s)
	}
	return s, nil
}

func p53Child(t *testing.T, c p53Config) {
	t.Helper()
	if os.Getenv("JM_P54_CHILD_NONCE") != c.Nonce {
		t.Fatal("child nonce mismatch")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exeHash, _, err := p53FileHash(exe)
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := p53DirectoryIdentity(c.FixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	db, err := metadata.Open(filepath.Join(c.FixtureRoot, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(meta)
	spool, err := NewSpoolStore(filepath.Join(c.FixtureRoot, "spool"), 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	body := p53Body()
	hash := sha256.Sum256(body)
	name := "clip.mov"
	custody, err := spool.PreparePrivateBirth(name, body)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := db.CaptureMirror(name)
	if err != nil {
		t.Fatal(err)
	}
	entry := metadata.MakeEntry(name, false, int64(len(body)), time.Unix(1700000000, 0), 901)
	birth, _, err := custody.CommitPrivateBirth(db, expected, entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.meta.MarkReady(birth.SpoolID, int64(len(body)), hash[:]); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.meta.MarkDraining(birth.SpoolID); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	destDir := filepath.Join(c.FixtureRoot, "destination")
	if err := os.Mkdir(destDir, 0700); err != nil {
		t.Fatal(err)
	}
	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: destDir})
	if err != nil {
		t.Fatal(err)
	}
	obs, err := db.CaptureBoundMode(birth.SpoolID, birth.BirthID, name, birth.SpoolFile, birth.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(destDir, name)
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := dst.Sync(); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := drainer.boundPhases.begin(db, obs, custody.image, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	mt := time.Unix(1800000000, 0)
	obs, err = lease.settle(mt, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	obs, backing, err := lease.transferBatch()
	if err != nil {
		t.Fatal(err)
	}
	item := metadata.BoundDrainCommitItem{SpoolID: birth.SpoolID, BirthID: birth.BirthID, Path: name,
		SpoolFile: birth.SpoolFile, FileIdentity: birth.FileIdentity, BackingIdentity: backing,
		Source: custody.image, FullSize: int64(len(body)), Expected: obs.Token, Mtime: mt}
	results, err := db.BatchCompleteBoundSpools([]metadata.BoundDrainCommitItem{item})
	if err != nil || len(results) != 1 {
		t.Fatalf("batch completion: %v", err)
	}
	if err := drainer.boundPhases.acceptBatchResults([]*boundFDPhase{lease}, results); err != nil {
		t.Fatal(err)
	}
	effects, err := lease.claimEffects(spool)
	if err != nil {
		t.Fatal(err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop released live effects owner")
	}
	if err := effects.apply(nil, nil); err != nil {
		t.Fatal(err)
	}
	before, _ := spool.Capacity()
	if before != int64(len(body)) {
		t.Fatalf("capacity before retire: %d", before)
	}
	expectedPhase := "ready"
	if c.Case == "sql_failure" {
		if _, err := db.DB().Exec(`CREATE TRIGGER refuse_bound_settle
		 BEFORE UPDATE OF phase ON bound_completion_effects
		 WHEN NEW.phase='awaiting_publication'
		 BEGIN SELECT RAISE(ABORT,'local phase denied'); END`); err != nil {
			t.Fatal(err)
		}
	} else {
		expectedPhase = "awaiting_publication"
	}
	err = effects.retire()
	var pending *metadata.PendingBoundEffects
	if !errors.As(err, &pending) || pending.SpoolID != birth.SpoolID || pending.Cause == nil {
		t.Fatalf("local settlement must be typed pending: %v", err)
	}
	if c.Case == "sql_failure" && !strings.Contains(pending.Cause.Error(), "local phase denied") {
		t.Fatalf("original SQL cause missing: %v", err)
	}
	if c.Case == "local_success" && !errors.Is(err, metadata.ErrBoundPublicationPending) {
		t.Fatalf("successful local phase cannot acknowledge publication: %v", err)
	}
	if drainer.Stop(0) {
		t.Fatal("Stop released uncertain source")
	}
	after, _ := spool.Capacity()
	if after != before {
		t.Fatalf("capacity changed: %d -> %d", before, after)
	}
	imagePresent, err := p53PathPresent(birth.SpoolFile)
	if err != nil {
		t.Fatalf("image path observation: %v", err)
	}
	intentPresent, err := p53PathPresent(filepath.Join(spool.root, "birth-intents", birth.BirthID+".json"))
	if err != nil {
		t.Fatalf("intent path observation: %v", err)
	}
	fi, err := custody.image.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || fi.Size() != int64(len(body)) {
		t.Fatalf("source FD identity/links: %+v", fi.Sys())
	}
	fdHash := sha256.New()
	if n, err := io.Copy(fdHash, io.NewSectionReader(custody.image, 0, int64(len(body)))); err != nil || n != int64(len(body)) {
		t.Fatalf("FD hash read: %d %v", n, err)
	}
	if !bytes.Equal(fdHash.Sum(nil), hash[:]) {
		t.Fatal("retained FD hash changed")
	}
	gotDestHash, gotDestSize, err := p53FileHash(dest)
	if err != nil || gotDestHash != hex.EncodeToString(hash[:]) || gotDestSize != int64(len(body)) {
		t.Fatalf("local destination hash/size: %s %d %v", gotDestHash, gotDestSize, err)
	}
	intentPath := filepath.Join(spool.root, "birth-intents", birth.BirthID+".json")
	intentHash, intentSize, err := p53FileHash(intentPath)
	if err != nil {
		t.Fatal(err)
	}
	intentID, _, err := metadataPrivateFileState(intentPath)
	if err != nil {
		t.Fatal(err)
	}
	spoolRootID, err := p53DirectoryIdentity(spool.root)
	if err != nil {
		t.Fatal(err)
	}
	filesID, err := p53DirectoryIdentity(filepath.Join(spool.root, "files"))
	if err != nil {
		t.Fatal(err)
	}
	intentsID, err := p53DirectoryIdentity(filepath.Join(spool.root, "birth-intents"))
	if err != nil {
		t.Fatal(err)
	}
	r := p53ChildReceipt{Case: c.Case, ExpectedPhase: expectedPhase,
		RetirementPending: true, SQLCause: pending.Cause.Error(),
		SpoolRootIdentity: spoolRootID, FilesIdentity: filesID, IntentsIdentity: intentsID,
		IntentIdentity: intentID, IntentSHA256: intentHash, IntentSize: intentSize,
		Nonce: c.Nonce, PID: os.Getpid(), ExecutableSHA256: exeHash,
		FixtureIdentity: rootID, SpoolID: birth.SpoolID, BirthID: birth.BirthID,
		CompletionID: results[0].CompletionID, Incarnation: results[0].Published.Incarnation,
		PublishedRevision: results[0].Published.Revision, SpoolFile: birth.SpoolFile,
		FileIdentity: birth.FileIdentity, BackingIdentity: results[0].BackingIdentity,
		SourceSHA256: hex.EncodeToString(hash[:]),
		SourceSize:   int64(len(body)), FDNlink: uint64(st.Nlink), CapacityBefore: before,
		CapacityAfter: after, DestinationSHA256: gotDestHash, ImagePresent: imagePresent,
		IntentPresent: intentPresent, UnlinkObserved: !imagePresent && !intentPresent && st.Nlink == 0}
	r.SQL, err = p53InspectSQL(db, r)
	if err != nil {
		t.Fatal(err)
	}
	if err := p53WriteJSON(filepath.Join(c.ReceiptDir, "child.json"), r); err != nil {
		t.Fatal(err)
	}
	os.Exit(47)
}

type p53ChildState struct {
	PID      int  `json:"pid"`
	ExitCode int  `json:"exit_code"`
	Reaped   bool `json:"reaped"`
	TimedOut bool `json:"timed_out"`
}

type p53SourceState struct {
	ImagePresent          bool     `json:"image_present"`
	ImageIdentity         string   `json:"image_identity"`
	ImageSize             int64    `json:"image_size"`
	ImageSHA256           string   `json:"image_sha256"`
	IntentPresent         bool     `json:"intent_present"`
	InventoryEntries      int      `json:"inventory_entries"`
	InventoryRegularBytes int64    `json:"inventory_regular_bytes"`
	MatchingPrivatePaths  []string `json:"matching_private_paths"`
}

type p53DestinationState struct {
	Present bool   `json:"present"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type p53Result struct {
	Case                        string              `json:"case"`
	ExpectedPhase               string              `json:"expected_phase"`
	RetirementPending           bool                `json:"retirement_pending"`
	SQLCause                    string              `json:"sql_cause"`
	Nonce                       string              `json:"nonce"`
	Verdict                     string              `json:"verdict"`
	Child                       p53ChildState       `json:"child"`
	RefusalPass                 bool                `json:"refusal_pass"`
	CustodyPass                 bool                `json:"custody_pass"`
	UnlinkObserved              bool                `json:"unlink_observed"`
	QualifiedPublicationReceipt bool                `json:"qualified_publication_receipt"`
	SourceSize                  int64               `json:"source_size"`
	SourceSHA256                string              `json:"source_sha256"`
	Source                      p53SourceState      `json:"source"`
	Destination                 p53DestinationState `json:"destination"`
	RecoveryAttempts            int                 `json:"recovery_attempts"`
	Before                      *p53Snapshot        `json:"sql_before,omitempty"`
	After                       *p53Snapshot        `json:"sql_after,omitempty"`
	SetupError                  string              `json:"setup_error,omitempty"`
}

func p53ExactIntent(path string, child p53ChildReceipt) (bool, error) {
	present, err := p53PathPresent(path)
	if err != nil || !present {
		return false, err
	}
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() || fi.Size() <= 0 || fi.Size() > 8192 {
		return false, fmt.Errorf("bounded intent shape: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 8192 {
		return false, fmt.Errorf("bounded intent read: %v", err)
	}
	var record struct {
		Version         int                       `json:"version"`
		Intent          metadata.SpoolBirthIntent `json:"intent"`
		RootIdentity    string                    `json:"root_identity"`
		FilesIdentity   string                    `json:"files_identity"`
		IntentsIdentity string                    `json:"intents_identity"`
		Size            int64                     `json:"size"`
		SHA256          [32]byte                  `json:"sha256"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return false, err
	}
	id, _, err := metadataPrivateFileState(path)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(raw)
	rootID, err := p53DirectoryIdentity(filepath.Dir(filepath.Dir(path)))
	if err != nil {
		return false, err
	}
	filesID, err := p53DirectoryIdentity(filepath.Join(filepath.Dir(filepath.Dir(path)), "files"))
	if err != nil {
		return false, err
	}
	intentsID, err := p53DirectoryIdentity(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	wantSeed, err := hex.DecodeString(child.SourceSHA256)
	if err != nil || len(wantSeed) != 32 {
		return false, errors.New("bad seed digest")
	}
	return record.Version == 1 && record.Size == child.SourceSize &&
		bytes.Equal(record.SHA256[:], wantSeed) &&
		record.RootIdentity == child.SpoolRootIdentity && record.RootIdentity == rootID &&
		record.FilesIdentity == child.FilesIdentity && record.FilesIdentity == filesID &&
		record.IntentsIdentity == child.IntentsIdentity && record.IntentsIdentity == intentsID &&
		record.Intent.BirthID == child.BirthID && record.Intent.Path == "clip.mov" &&
		record.Intent.SpoolFile == child.SpoolFile && record.Intent.FileIdentity == child.FileIdentity &&
		id == child.IntentIdentity && int64(len(raw)) == child.IntentSize &&
		hex.EncodeToString(digest[:]) == child.IntentSHA256, nil
}

func p53PathPresent(name string) (bool, error) {
	_, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func p53Inventory(root, expectedHash string, expectedSize int64) (p53SourceState, error) {
	var out p53SourceState
	out.MatchingPrivatePaths = []string{}
	err := filepath.WalkDir(root, func(name string, de os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		out.InventoryEntries++
		if out.InventoryEntries > 128 {
			return errors.New("spool inventory exceeds 128 entries")
		}
		if de.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in private spool: %s", name)
		}
		if de.IsDir() {
			return nil
		}
		if !de.Type().IsRegular() {
			return fmt.Errorf("nonregular spool entry: %s", name)
		}
		fi, err := de.Info()
		if err != nil {
			return err
		}
		out.InventoryRegularBytes += fi.Size()
		if out.InventoryRegularBytes > 8<<20 {
			return errors.New("spool inventory exceeds 8 MiB")
		}
		if fi.Size() == expectedSize {
			h, size, err := p53FileHash(name)
			if err != nil {
				return err
			}
			if size == expectedSize && h == expectedHash {
				rel, err := filepath.Rel(root, name)
				if err != nil {
					return err
				}
				out.MatchingPrivatePaths = append(out.MatchingPrivatePaths, rel)
			}
		}
		return nil
	})
	return out, err
}

func p53WriteResult(t *testing.T, c p53Config, result p53Result) {
	t.Helper()
	if err := p53WriteJSON(filepath.Join(c.ReceiptDir, "result.json"), result); err != nil {
		t.Fatalf("fsync result receipt: %v", err)
	}
}

func TestBoundEffectsLocalSettlementFreshProcessRecovery(t *testing.T) {
	if os.Getenv("JM_P54_CONFIG") == "" {
		t.Skip("private opt-in fixture requires JM_P54_CONFIG")
	}
	c := p53ReadConfig(t)
	if os.Getenv("JM_P54_CHILD_NONCE") != "" {
		p53Child(t, c)
		return
	}
	body := p53Body()
	hash := sha256.Sum256(body)
	wantHash := hex.EncodeToString(hash[:])
	expectedPhase := "ready"
	if c.Case == "local_success" {
		expectedPhase = "awaiting_publication"
	}
	result := p53Result{Case: c.Case, ExpectedPhase: expectedPhase, Nonce: c.Nonce, Verdict: "SETUP_ERROR", SourceSize: int64(len(body)), SourceSHA256: wantHash}
	failSetup := func(msg string) {
		result.SetupError = msg
		p53WriteResult(t, c, result)
		t.Fatal(msg)
	}
	entries, err := os.ReadDir(c.FixtureRoot)
	if err != nil || len(entries) != 0 {
		failSetup(fmt.Sprintf("fixture root must start empty: %v, %d entries", err, len(entries)))
	}
	rootID, err := p53DirectoryIdentity(c.FixtureRoot)
	if err != nil {
		failSetup(fmt.Sprintf("fixture identity: %v", err))
	}
	exe, err := os.Executable()
	if err != nil {
		failSetup(fmt.Sprintf("executable path: %v", err))
	}
	exeHash, _, err := p53FileHash(exe)
	if err != nil {
		failSetup(fmt.Sprintf("executable hash: %v", err))
	}
	command := []string{exe, "-test.run=^TestBoundEffectsLocalSettlementFreshProcessRecovery$", "-test.count=1"}
	if err := p53WriteJSON(filepath.Join(c.ReceiptDir, "launch.json"), map[string]any{
		"case": c.Case, "nonce": c.Nonce, "executable": exe, "executable_sha256": exeHash, "argv": command,
		"fixture_root": c.FixtureRoot, "fixture_identity": rootID,
	}); err != nil {
		failSetup(fmt.Sprintf("prelaunch receipt: %v", err))
	}
	stdout, err := os.OpenFile(filepath.Join(c.ReceiptDir, "child.stdout"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		failSetup(fmt.Sprintf("child stdout: %v", err))
	}
	stderr, err := os.OpenFile(filepath.Join(c.ReceiptDir, "child.stderr"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		_ = stdout.Close()
		failSetup(fmt.Sprintf("child stderr: %v", err))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Env = append(os.Environ(), "JM_P54_CHILD_NONCE="+c.Nonce)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		failSetup(fmt.Sprintf("child Start: %v", err))
	}
	result.Child.PID = cmd.Process.Pid
	startedErr := p53WriteJSON(filepath.Join(c.ReceiptDir, "started.json"), map[string]any{
		"case": c.Case, "nonce": c.Nonce, "pid": cmd.Process.Pid, "executable_sha256": exeHash,
	})
	if startedErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait() // exact child is fully reaped before any DB open
	result.Child.Reaped = cmd.ProcessState != nil
	if cmd.ProcessState != nil {
		result.Child.ExitCode = cmd.ProcessState.ExitCode()
	}
	result.Child.TimedOut = ctx.Err() == context.DeadlineExceeded
	_ = stdout.Sync()
	_ = stderr.Sync()
	_ = stdout.Close()
	_ = stderr.Close()
	if startedErr != nil {
		failSetup(fmt.Sprintf("started receipt: %v", startedErr))
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 47 || result.Child.TimedOut {
		failSetup(fmt.Sprintf("child exit setup failure: %v", waitErr))
	}
	childBytes, err := os.ReadFile(filepath.Join(c.ReceiptDir, "child.json"))
	if err != nil || len(childBytes) > 8192 {
		failSetup(fmt.Sprintf("child receipt: %v", err))
	}
	var child p53ChildReceipt
	if err := json.Unmarshal(childBytes, &child); err != nil {
		failSetup(fmt.Sprintf("decode child receipt: %v", err))
	}
	postWaitRootID, err := p53DirectoryIdentity(c.FixtureRoot)
	if err != nil || postWaitRootID != rootID {
		failSetup(fmt.Sprintf("fixture root changed after Wait: %v", err))
	}
	if child.Case != c.Case || child.ExpectedPhase != expectedPhase || !child.RetirementPending ||
		child.IntentIdentity == "" || child.IntentSHA256 == "" || child.IntentSize <= 0 ||
		child.SpoolRootIdentity == "" || child.FilesIdentity == "" || child.IntentsIdentity == "" ||
		child.FDNlink != 1 || !child.ImagePresent || !child.IntentPresent || child.UnlinkObserved ||
		(c.Case == "sql_failure" && !strings.Contains(child.SQLCause, "local phase denied")) ||
		(c.Case == "local_success" && child.SQLCause != metadata.ErrBoundPublicationPending.Error()) ||
		child.Nonce != c.Nonce || child.PID != result.Child.PID ||
		child.ExecutableSHA256 != exeHash || child.FixtureIdentity != rootID ||
		child.SourceSize != int64(len(body)) ||
		child.SourceSHA256 != wantHash || child.CapacityBefore != int64(len(body)) ||
		child.CapacityAfter != child.CapacityBefore || child.DestinationSHA256 != wantHash ||
		child.SpoolID <= 0 || child.BirthID == "" || child.CompletionID == "" || child.Incarnation == "" ||
		child.BackingIdentity == "" ||
		child.PublishedRevision <= 0 || child.SpoolFile != filepath.Join(c.FixtureRoot, "spool", "files", "birth-"+child.BirthID) ||
		child.UnlinkObserved != (!child.ImagePresent && !child.IntentPresent && child.FDNlink == 0) {
		failSetup("child receipt identity or source proof mismatch")
	}
	result.UnlinkObserved = child.UnlinkObserved
	result.RetirementPending = child.RetirementPending
	result.SQLCause = child.SQLCause
	// No parent DB handle exists before this point.
	db, err := metadata.Open(filepath.Join(c.FixtureRoot, "mirror.db"))
	if err != nil {
		failSetup(fmt.Sprintf("fresh metadata.Open: %v", err))
	}
	defer db.Close()
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		failSetup(fmt.Sprintf("fresh schema migration: %v", err))
	}
	meta := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(meta)
	spool, err := NewSpoolStore(filepath.Join(c.FixtureRoot, "spool"), 0, meta)
	if err != nil {
		failSetup(fmt.Sprintf("fresh NewSpoolStore: %v", err))
	}
	defer spool.Stop()
	before, err := p53InspectSQL(db, child)
	if err != nil {
		failSetup(fmt.Sprintf("fresh SQL tuple: %v", err))
	}
	if !reflect.DeepEqual(before, child.SQL) {
		failSetup("child/parent fresh SQL snapshot mismatch")
	}
	result.Before = &before
	intentPath := filepath.Join(c.FixtureRoot, "spool", "birth-intents", child.BirthID+".json")
	intentPresentBefore, err := p53PathPresent(intentPath)
	if err != nil {
		failSetup(fmt.Sprintf("intent path before recovery: %v", err))
	}
	exactIntent, err := p53ExactIntent(intentPath, child)
	if err != nil || (intentPresentBefore && !exactIntent) {
		failSetup(fmt.Sprintf("retained intent is not exact: %v", err))
	}
	const sentinel = int64(131071)
	spool.used.Store(sentinel)
	for i := 0; i < 2; i++ {
		var pending *metadata.PendingBoundEffectsRecovery
		if err := db.CheckSpoolRecovery(); !errors.As(err, &pending) || len(pending.Effects) != 1 ||
			pending.Effects[0].CompletionID != child.CompletionID ||
			pending.Effects[0].SpoolID != child.SpoolID || pending.Effects[0].Phase != expectedPhase {
			failSetup(fmt.Sprintf("preflight refusal attempt %d: %v", i, err))
		}
		report, err := spool.RecoverOnBoot(context.Background())
		var bootPending *metadata.PendingBoundEffectsRecovery
		if report != (RecoveryReport{}) || !errors.As(err, &bootPending) ||
			len(bootPending.Effects) != 1 || bootPending.Effects[0] != pending.Effects[0] {
			failSetup(fmt.Sprintf("boot refusal attempt %d: %v, report %+v", i, err, report))
		}
		if used, _ := spool.Capacity(); used != sentinel {
			failSetup(fmt.Sprintf("capacity changed after refusal attempt %d: %d", i, used))
		}
		after, err := p53InspectSQL(db, child)
		if err != nil {
			failSetup(fmt.Sprintf("recovery SQL tuple: %v", err))
		}
		if currentRootID, err := p53DirectoryIdentity(c.FixtureRoot); err != nil || currentRootID != rootID {
			failSetup(fmt.Sprintf("fixture root changed on recovery attempt %d: %v", i, err))
		}
		if !reflect.DeepEqual(before, after) {
			failSetup(fmt.Sprintf("SQL tuple changed after refusal attempt %d", i))
		}
		currentIntent, err := p53ExactIntent(intentPath, child)
		if err != nil || currentIntent != exactIntent {
			failSetup(fmt.Sprintf("intent changed after refusal attempt %d: %v", i, err))
		}
		result.RecoveryAttempts++
	}
	after, err := p53InspectSQL(db, child)
	if err != nil {
		failSetup(fmt.Sprintf("final SQL tuple: %v", err))
	}
	result.After = &after
	result.RefusalPass = true
	imagePresent, err := p53PathPresent(child.SpoolFile)
	if err != nil {
		failSetup(fmt.Sprintf("image path: %v", err))
	}
	intentPresent, err := p53PathPresent(intentPath)
	if err != nil {
		failSetup(fmt.Sprintf("intent path: %v", err))
	}
	source, err := p53Inventory(filepath.Join(c.FixtureRoot, "spool"), wantHash, int64(len(body)))
	if err != nil {
		failSetup(fmt.Sprintf("bounded private inventory: %v", err))
	}
	source.ImagePresent, source.IntentPresent = imagePresent, intentPresent
	if imagePresent {
		source.ImageIdentity, source.ImageSize, err = metadataPrivateFileState(child.SpoolFile)
		if err != nil {
			failSetup(fmt.Sprintf("bound image identity: %v", err))
		}
		source.ImageSHA256, _, err = p53FileHash(child.SpoolFile)
		if err != nil {
			failSetup(fmt.Sprintf("bound image hash: %v", err))
		}
	}
	result.Source = source
	dest := filepath.Join(c.FixtureRoot, "destination", "clip.mov")
	destPresent, err := p53PathPresent(dest)
	if err != nil {
		failSetup(fmt.Sprintf("destination path: %v", err))
	}
	result.Destination.Present = destPresent
	if destPresent {
		result.Destination.SHA256, result.Destination.Size, err = p53FileHash(dest)
		if err != nil {
			failSetup(fmt.Sprintf("destination hash: %v", err))
		}
	}
	if !destPresent || result.Destination.Size != int64(len(body)) || result.Destination.SHA256 != wantHash {
		failSetup("local destination changed during fresh recovery")
	}
	// This fixture has no qualified remote publication receipt. The local
	// destination hash is a separate observation and cannot satisfy custody.
	result.CustodyPass = exactIntent && source.IntentPresent && source.ImagePresent &&
		child.FDNlink == 1 && len(source.MatchingPrivatePaths) == 1 &&
		source.MatchingPrivatePaths[0] == filepath.Join("files", "birth-"+child.BirthID) &&
		source.ImageIdentity == child.FileIdentity &&
		source.ImageSize == int64(len(body)) && source.ImageSHA256 == wantHash
	if result.CustodyPass {
		result.Verdict = "PASS"
	} else {
		result.Verdict = "CUSTODY_RED"
	}
	p53WriteResult(t, c, result)
	if !result.CustodyPass {
		t.Errorf("required restart-discoverable exact private source missing; refusal passed and local destination remained exact")
	}
}
