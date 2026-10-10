//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type statusNoRemoteDoer struct{}

func (*statusNoRemoteDoer) Do(context.Context, ...any) *redis.Cmd {
	panic("/spool attempted remote metadata I/O")
}

func replacementStatusFor(t *testing.T, f *replacementResumeFixture) (SpoolStatusResponse, string) {
	t.Helper()
	status, err := BuildSpoolStatus(f.spool, f.d)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range status.Entries {
		if entry.Path == f.row.NFSPath {
			raw, err := json.Marshal(status)
			if err != nil {
				t.Fatal(err)
			}
			const schemaID = "https://juicemount.contract/v1/spool-status.schema.json"
			schemaBytes, err := os.ReadFile("../contract/spec/schema/spool-status.schema.json")
			if err != nil {
				t.Fatal(err)
			}
			compiler := jsonschema.NewCompiler()
			compiler.Draft = jsonschema.Draft2020
			if err := compiler.AddResource(schemaID, bytes.NewReader(schemaBytes)); err != nil {
				t.Fatal(err)
			}
			schema, err := compiler.Compile(schemaID)
			if err != nil {
				t.Fatal(err)
			}
			var actual any
			if err := json.Unmarshal(raw, &actual); err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(actual); err != nil {
				t.Fatalf("actual /spool status violates schema: %v", err)
			}
			t.Logf("replacement_availability_status=%s", raw)
			return status, entry.ReplacementState
		}
	}
	t.Fatal("accepted replacement absent from /spool")
	return status, ""
}

func TestReplacementAvailabilityLocalStatesAndNoRemoteStatusRead(t *testing.T) {
	f := newReplacementResumeFixtureBirth(t, []byte("accepted bytes stay local until publication"))
	hold := errors.New("retain intent for status")
	if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID,
		func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) { return nil, nil, hold }, f.binding); !errors.Is(err, hold) {
		t.Fatalf("prepare intent: %v", err)
	}
	q := f.d.replacementDispatch
	q.resume = func(context.Context, int64) error { return nil } // wired, never started
	status, state := replacementStatusFor(t, f)
	if state != "recovery_disabled" || status.RecoveryEnabled == nil || *status.RecoveryEnabled {
		t.Fatalf("disabled status=%q enabled=%v", state, status.RecoveryEnabled)
	}
	q.enabled = true
	_, state = replacementStatusFor(t, f)
	if state != "queued" {
		t.Fatalf("enabled pending=%q", state)
	}
	p, err := f.spool.meta.ReplacementDispatchCandidate(context.Background(), f.row.ID)
	if err != nil {
		t.Fatal(err)
	}
	q.noteUnavailable(*p)
	_, state = replacementStatusFor(t, f)
	if state != "provider_unavailable" {
		t.Fatalf("recent provider failure=%q", state)
	}
	q.availabilityMu.Lock()
	v := q.unavailable[f.row.ID]
	v.revision++
	q.unavailable[f.row.ID] = v
	q.availabilityMu.Unlock()
	_, state = replacementStatusFor(t, f)
	if state != "queued" {
		t.Fatalf("stale revision leaked=%q", state)
	}
	q.noteUnavailable(*p)
	q.availabilityMu.Lock()
	v = q.unavailable[f.row.ID]
	v.at = time.Now().Add(-replacementUnavailableWindow - time.Second)
	q.unavailable[f.row.ID] = v
	q.availabilityMu.Unlock()
	_, state = replacementStatusFor(t, f)
	if state != "queued" {
		t.Fatalf("expired capability reason leaked=%q", state)
	}
	q.noteUnavailable(*p)
	if err := f.d.dispatchReplacement(context.Background(), f.row.ID, true); err != nil {
		t.Fatal(err)
	}
	_, state = replacementStatusFor(t, f)
	if state != "provider_unavailable" {
		t.Fatalf("local-only scan erased remote reason=%q", state)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = f.d.dispatchReplacement(canceled, f.row.ID, false)
	_, state = replacementStatusFor(t, f)
	if state != "queued" {
		t.Fatalf("early remote retry retained stale reason=%q", state)
	}
	q.noteUnavailable(*p)
	if err := f.spool.meta.RecordReplacementDisposition(*p, metadata.ReplacementWaitingWriter, "mac-peer", 2, projectOf(f.row.NFSPath)); err != nil {
		t.Fatal(err)
	}
	_, state = replacementStatusFor(t, f)
	if state != metadata.ReplacementWaitingWriter {
		t.Fatalf("writer disposition lost priority=%q", state)
	}
	lease := f.d.leasesPtr.Load()
	lease.Stop() // quiesce heartbeat before substituting the remote I/O tripwire
	original := lease.rdb
	lease.rdb = &statusNoRemoteDoer{}
	defer func() { lease.rdb = original }()
	_, _ = replacementStatusFor(t, f)
}

func TestReplacementAvailabilityOffProviderRetainsStageAndReason(t *testing.T) {
	f := newReplacementResumeFixtureVolume(t, []byte("accepted image remains readable"), "12345678-1234-abcd-5678-123456789abc", false)
	d := f.d
	d.replacementDispatch.enabled = true
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		return f.x.jfs.handler.resumeQueuedReplacement(ctx, d, id, f.factory)
	}
	before, _ := f.spool.Capacity()
	if err := d.dispatchReplacement(context.Background(), f.row.ID, false); !errors.Is(err, errReplacementCapabilityUnavailable) {
		t.Fatalf("off provider attempt: %v", err)
	}
	_, state := replacementStatusFor(t, f)
	if state != "provider_unavailable" {
		t.Fatalf("off provider status=%q", state)
	}
	if _, err := f.backend.Stat(replaceguard.BirthStageName(f.backend.w.OperationID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("off provider created a stage: %v", err)
	}
	if after, _ := f.spool.Capacity(); after != before {
		t.Fatalf("capacity changed: %d to %d", before, after)
	}
}

func TestReplacementAvailabilityCommittedFinishingWithRecoveryOff(t *testing.T) {
	f := newReplacementResumeFixtureBirth(t, []byte("committed save finishing"))
	stop := errors.New("stop at committed")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == metadata.ReplacementPublicationCommitted {
			return stop
		}
		return nil
	}
	if err := f.run(context.Background()); !errors.Is(err, stop) {
		t.Fatalf("commit: %v", err)
	}
	f.spool.replacementResumePhase = nil
	f.d.replacementDispatch.enabled = false
	_, state := replacementStatusFor(t, f)
	if state != "finishing" {
		t.Fatalf("committed status=%q", state)
	}
}

func TestReplacementAvailabilityStaleListedRowAfterSettlement(t *testing.T) {
	f := newReplacementResumeFixtureBirth(t, []byte("settlement changes the displayed row state"))
	oldList, err := f.spool.meta.ListForStatus(time.Now().Add(-5 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop after settlement")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == metadata.ReplacementPublicationSettled {
			return stop
		}
		return nil
	}
	if err := f.run(context.Background()); !errors.Is(err, stop) {
		t.Fatalf("settle: %v", err)
	}
	f.spool.replacementResumePhase = nil
	if phase := cleanupPhase(t, f.spool, f.row.ID); phase != metadata.ReplacementPublicationSettled {
		t.Fatalf("phase=%s", phase)
	}
	stale, err := f.spool.meta.ReplacementPublicationsForStatus(oldList)
	if err != nil {
		t.Fatalf("stale status list caused an error: %v", err)
	}
	if _, ok := stale[f.row.ID]; ok {
		t.Fatal("stale ready list displayed settled publication")
	}
	_, state := replacementStatusFor(t, f)
	if state != "finishing" {
		t.Fatalf("fresh settled status=%q", state)
	}
}

func TestReplacementAvailabilitySameOperationRecoversAfterProviderEnablesBirth(t *testing.T) {
	body := []byte("same accepted save survives an unavailable provider")
	x, spool, d, row, witness, _ := replacementNestedFixtureBodyVolume(t, body, "12345678-1234-abcd-5678-123456789abc")
	prior := newReplacementRuntimeWithBirth(t, x, t.TempDir(), false, time.Hour, false)
	client, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: prior.url, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Stop)
	d.SetProjectLeases(client)
	if err := x.session.Qualify(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := &replacementResumeFixture{x: x, spool: spool, d: d, row: row, proxyURL: prior.url,
		backend: &replacementSDKIO{fs: prior.fs, m: prior.m, w: *witness, ctx: jfsmeta.Background(), parent: 4}}
	before, _ := spool.Capacity()
	if err := f.run(context.Background()); !errors.Is(err, errReplacementCapabilityUnavailable) {
		t.Fatalf("off provider attempt: %v", err)
	}
	pending, err := spool.meta.ReplacementPublication(row)
	if err != nil || pending == nil || pending.OperationID != witness.OperationID || pending.Phase != metadata.ReplacementPublicationIntent {
		t.Fatalf("accepted operation changed: %+v %v", pending, err)
	}
	if _, err := f.backend.Stat(replaceguard.BirthStageName(witness.OperationID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("off provider created stage: %v", err)
	}
	if used, _ := spool.Capacity(); used != before {
		t.Fatalf("off provider released charge: %d != %d", used, before)
	}
	client.Stop()
	prior.stop(t)
	next := newReplacementRuntimeWithBirth(t, x, prior.objectRoot, true, time.Hour, true)
	f.proxyURL = next.url
	f.reopen(t, true)
	f.backend = &replacementSDKIO{fs: next.fs, m: next.m, w: *witness, ctx: jfsmeta.Background(), parent: 4}
	q := f.d.replacementDispatch
	q.enabled = true
	done := make(chan error, 4)
	q.resume = func(ctx context.Context, id int64) error {
		err := f.x.jfs.handler.resumeQueuedReplacement(ctx, f.d, id, f.factory)
		done <- err
		return err
	}
	f.d.Start()
	t.Cleanup(func() {
		if !f.d.Stop(5 * time.Second) {
			t.Error("queue did not join")
		}
	})
	awaitReplacementQueueCleaned(t, f, done)
	if phase := cleanupPhase(t, f.spool, f.row.ID); phase != metadata.ReplacementPublicationCleaned {
		t.Fatalf("phase=%s", phase)
	}
	if destination, err := f.backend.Stat("clip.mov"); err != nil || destination.Length != uint64(len(body)) || f.backend.renames != 1 {
		t.Fatalf("same operation not published: %+v %v", destination, err)
	}
	if used, _ := f.spool.Capacity(); used != before-f.row.Size {
		t.Fatalf("charge after recovery=%d", used)
	}
	if !f.d.Stop(5 * time.Second) {
		t.Fatal("queue did not join before offline readback")
	}
	f.d.leasesPtr.Load().Stop()
	assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, before-f.row.Size, false)
}
