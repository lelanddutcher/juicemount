//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type replacementIncarnationOverrideDoer struct {
	leaseDoer
	queries int
}

func (d *replacementIncarnationOverrideDoer) Do(ctx context.Context, args ...any) *redis.Cmd {
	if len(args) == 2 && args[0] == "JM.LEASE" && args[1] == "REPLACEMENT_CAPABILITIES" {
		d.queries++
		cmd := redis.NewCmd(ctx, args...)
		cmd.SetVal([]any{int64(1), int64(3), "mac-test", "wrong-incarnation", int64(1)})
		return cmd
	}
	return d.leaseDoer.Do(ctx, args...)
}

func TestReplacementBornRefusesSnapshotIncarnationMismatchBeforeUpload(t *testing.T) {
	body := []byte("never upload to a stage born under a different incarnation")
	f := newReplacementResumeFixtureBirth(t, body)
	client := f.d.leasesPtr.Load()
	override := &replacementIncarnationOverrideDoer{leaseDoer: client.rdb}
	client.rdb = override
	usedBefore, _ := f.spool.Capacity()
	if err := f.run(context.Background()); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("incarnation mismatch: %v", err)
	}
	if override.queries != 1 || f.backend.renames != 0 {
		t.Fatalf("capability queries=%d renames=%d", override.queries, f.backend.renames)
	}
	stage, err := f.backend.Stat(replaceguard.BirthStageName(f.backend.w.OperationID))
	if err != nil || stage.Length != 0 {
		t.Fatalf("unwritable stage: %+v %v", stage, err)
	}
	if got, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("accepted image changed: %v", err)
	}
	if usedAfter, _ := f.spool.Capacity(); usedAfter != usedBefore {
		t.Fatalf("charged bytes changed: %d -> %d", usedBefore, usedAfter)
	}
}

func TestReplacementBornUploadResumesAfterReopen(t *testing.T) {
	testReplacementBornUploadReopen(t, false)
}

func TestReplacementBornResumeWithCreationOffAndLocalCleanup(t *testing.T) {
	body := []byte("private born-stage recovery survives provider creation off")
	f, prior := newReplacementFreshFixture(t, body)
	ctx, cancel := context.WithCancel(context.Background())
	fault := &replacementInterruptedUploadIO{replacementPublishIO: f.backend, at: "after-create", cancel: cancel}
	factory := func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return fault, func() {}, nil
	}
	if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, factory, f.binding); !errors.Is(err, context.Canceled) {
		t.Fatalf("seed born stage: %v", err)
	}
	before, err := f.spool.meta.ReplacementPublication(f.row)
	if err != nil || before == nil || before.Phase != "intent" {
		t.Fatalf("born intent: %+v %v", before, err)
	}
	stage, err := f.backend.Stat(path.Base(before.StagePath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.x.rdb.Get(context.Background(), replaceguard.BirthOperationKey(f.backend.w.OperationID)).Bytes(); err != nil {
		t.Fatalf("born receipt missing: %v", err)
	}
	f.d.leasesPtr.Load().Stop()
	prior.stop(t)
	next := newReplacementRuntimeWithBirth(t, f.x, prior.objectRoot, true, time.Hour, false)
	if next.proxy.Incarnation() == prior.proxy.Incarnation() {
		t.Fatal("provider incarnation did not change")
	}
	f.proxyURL = next.url
	f.reopen(t, true)
	f.backend = &replacementSDKIO{fs: next.fs, m: next.m, w: f.backend.w, ctx: jfsmeta.Background(), parent: 4}
	if _, err := f.d.leasesPtr.Load().replacementStageCapability(context.Background()); !errors.Is(err, errReplacementCapabilityUnavailable) {
		t.Fatalf("creation-off provider advertised capability: %v", err)
	}
	stopAt := errors.New("stop after committed")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == "committed" {
			return stopAt
		}
		return nil
	}
	if err := f.run(context.Background()); !errors.Is(err, stopAt) {
		t.Fatalf("born reopen with creation off: %v", err)
	}
	f.spool.replacementResumePhase = nil
	if committed, err := f.backend.Stat("clip.mov"); err != nil || committed.Inode != stage.Inode {
		t.Fatalf("born stage not committed: %+v %v", committed, err)
	}
	stopAt = errors.New("stop after settled")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == "settled" {
			return stopAt
		}
		return nil
	}
	if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, nil, nil); !errors.Is(err, stopAt) {
		t.Fatalf("settlement under creation-off provider: %v", err)
	}
	f.spool.replacementResumePhase = nil
	f.reopen(t, false)
	if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, nil, nil); err != nil {
		t.Fatalf("local cleanup without provider: %v", err)
	}
	if got := cleanupPhase(t, f.spool, f.row.ID); got != metadata.ReplacementPublicationCleaned {
		t.Fatalf("phase=%s", got)
	}
}

func TestReplacementBornCreationOffRetainsAcceptedImage(t *testing.T) {
	body := []byte("accepted save waits for provider birth support")
	f := newReplacementResumeFixtureVolume(t, body, "12345678-1234-abcd-5678-123456789abc", false)
	usedBefore, _ := f.spool.Capacity()
	rowBefore := *f.row
	witnessBefore := f.backend.w
	if err := f.run(context.Background()); !errors.Is(err, errReplacementCapabilityUnavailable) {
		t.Fatalf("creation-off refusal: %v", err)
	}
	client := f.d.leasesPtr.Load()
	if client == nil || !client.unsupported.IsZero() {
		t.Fatal("capability refusal changed ordinary lease support")
	}
	if lease, err := client.acquire(context.Background(), projectOf(f.row.NFSPath), "auto", 0); err != nil || !lease.held {
		t.Fatalf("ordinary lease after capability refusal: %+v %v", lease, err)
	}
	if _, err := f.backend.Stat(replaceguard.BirthStageName(witnessBefore.OperationID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("creation-off provider made a stage: %v", err)
	}
	rowAfter, err := f.spool.meta.Get(f.row.ID)
	if err != nil || !reflect.DeepEqual(rowAfter, &rowBefore) || !reflect.DeepEqual(f.backend.w, witnessBefore) {
		t.Fatalf("accepted row or witness changed: %+v %v", rowAfter, err)
	}
	if got, err := os.ReadFile(rowAfter.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("accepted image changed: %v", err)
	}
	if usedAfter, _ := f.spool.Capacity(); usedAfter != usedBefore {
		t.Fatalf("charged bytes changed: %d -> %d", usedBefore, usedAfter)
	}
}

func testReplacementBornUploadReopen(t *testing.T, fresh bool) {
	for _, tc := range []struct {
		name, at string
		empty    bool
	}{{"after-create", "after-create", false}, {"after-first-chunk", "after-first-chunk", false}, {"lost-create-reply", "lost-create-reply", false}, {"before-identity-commit", "before-identity-commit", false}, {"empty-after-create", "after-create", true}, {"empty-before-identity", "before-identity-commit", true}} {
		t.Run(tc.name, func(t *testing.T) {
			at := tc.at
			body := bytes.Repeat([]byte("interrupted-sdk-save-"), 160003)
			if tc.empty {
				body = nil
			}
			var f *replacementResumeFixture
			var runtime *replacementRuntime
			if fresh {
				f, runtime = newReplacementFreshFixture(t, body)
			} else {
				f = newReplacementResumeFixtureBirth(t, body)
			}
			tracked := &replacementTrackedIO{replacementPublishIO: f.backend}
			original, err := f.backend.Stat("clip.mov")
			if err != nil {
				t.Fatal(err)
			}
			originalAttr, err := f.x.rdb.Get(context.Background(), "i3").Bytes()
			if err != nil {
				t.Fatal(err)
			}
			originalGraph, err := f.x.rdb.LRange(context.Background(), "c3_0", 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			originalName, err := f.x.rdb.HGet(context.Background(), "d4", "clip.mov").Bytes()
			if err != nil {
				t.Fatal(err)
			}
			used, _ := f.spool.Capacity()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			fault := &replacementInterruptedUploadIO{replacementPublishIO: tracked, at: at, cancel: cancel, lost: errors.New("controlled lost create acknowledgement")}
			if at == "before-identity-commit" {
				if _, err := f.x.store.DB().Exec(`CREATE TRIGGER upload_identity_fault BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='arming' BEGIN SELECT RAISE(ABORT,'upload identity fault'); END`); err != nil {
					t.Fatal(err)
				}
			}
			factory := func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return fault, func() {}, nil
			}
			err = f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, factory, f.binding)
			if err == nil {
				t.Fatal("interruption did not stop publication")
			}
			if (at == "after-create" || at == "after-first-chunk") && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if at == "lost-create-reply" && !errors.Is(err, fault.lost) {
				t.Fatalf("create reply: %v", err)
			}
			if at == "before-identity-commit" {
				if _, err := f.x.store.DB().Exec(`DROP TRIGGER upload_identity_fault`); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || before == nil || before.Phase != "intent" || before.Record != "" {
				t.Fatalf("missing unowned intent: %+v %v", before, err)
			}
			stage, err := f.backend.Stat(path.Base(before.StagePath))
			if err != nil {
				t.Fatal(err)
			}
			wantSize := uint64(0)
			if at == "after-first-chunk" {
				wantSize = 1 << 20
			}
			if at == "before-identity-commit" {
				wantSize = uint64(len(body))
			}
			if stage.Length != wantSize || stage.Inode == original.Inode || f.backend.renames != 0 {
				t.Fatalf("unexpected partial stage: %+v", stage)
			}
			if at == "after-first-chunk" && fault.writes != 1 {
				t.Fatalf("writes=%d", fault.writes)
			}
			current, err := f.backend.Stat("clip.mov")
			if err != nil || current != original || f.backend.renames != 0 {
				t.Fatalf("original changed: %+v %v", current, err)
			}
			currentStage, err := f.backend.Stat(path.Base(before.StagePath))
			if err != nil || currentStage != stage {
				t.Fatalf("unowned stage changed: %+v %v", currentStage, err)
			}
			gotAttr, attrErr := f.x.rdb.Get(context.Background(), "i3").Bytes()
			gotGraph, graphErr := f.x.rdb.LRange(context.Background(), "c3_0", 0, -1).Result()
			gotName, nameErr := f.x.rdb.HGet(context.Background(), "d4", "clip.mov").Bytes()
			if attrErr != nil || graphErr != nil || nameErr != nil || !bytes.Equal(gotAttr, originalAttr) || !reflect.DeepEqual(gotGraph, originalGraph) || !bytes.Equal(gotName, originalName) {
				t.Fatal("original remote namespace or content mapping changed")
			}
			if got, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("private save changed: %v", err)
			}
			if after, _ := f.spool.Capacity(); after != used {
				t.Fatalf("capacity changed: %d != %d", after, used)
			}
			receiptKey := replaceguard.BirthOperationKey(f.backend.w.OperationID)
			birthBefore, err := f.x.rdb.Get(context.Background(), receiptKey).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			born, err := replaceguard.DecodeBirth(birthBefore)
			if err != nil || born.Stage.Inode != stage.Inode {
				t.Fatalf("birth: %+v %v", born, err)
			}
			tracked.assertClosed(t)
			if fresh {
				runtime = replaceReplacementRuntime(t, f, runtime)
			} else {
				f.reopen(t, true)
			}
			resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer resumeCancel()
			if err := f.run(resumeCtx); err != nil {
				t.Fatalf("resume owned stage: %v", err)
			}
			if got := cleanupPhase(t, f.spool, f.row.ID); got != metadata.ReplacementPublicationCleaned {
				t.Fatalf("phase=%s", got)
			}
			destination, err := f.backend.Stat("clip.mov")
			if err != nil || destination.Inode != stage.Inode || destination.Length != uint64(len(body)) || f.backend.renames != 1 {
				t.Fatalf("published same stage: %+v %v renames=%d", destination, err, f.backend.renames)
			}
			birthAfter, err := f.x.rdb.Get(context.Background(), receiptKey).Bytes()
			if err != nil || !bytes.Equal(birthBefore, birthAfter) {
				t.Fatal("birth custody changed")
			}
			t.Logf("interruption=%s resumed_inode=%d accepted_bytes=%d phase=cleaned", at, stage.Inode, len(body))
			assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, used-f.row.Size, false)
		})
	}
}

func TestReplacementBornUploadRefusesChangedCustody(t *testing.T) {
	for _, fault := range []string{"missing-operation", "missing-inode", "malformed", "owner", "database", "uuid", "mode", "parent-name", "oversized", "linked", "missing-stage", "swapped-name", "sealed"} {
		t.Run(fault, func(t *testing.T) {
			body := []byte("private bytes survive every refused recovery")
			f := newReplacementResumeFixtureBirth(t, body)
			ctx, cancel := context.WithCancel(context.Background())
			wrapper := &replacementInterruptedUploadIO{replacementPublishIO: f.backend, at: "after-create", cancel: cancel}
			factory := func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return wrapper, func() {}, nil
			}
			if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, factory, f.binding); !errors.Is(err, context.Canceled) {
				t.Fatalf("seed interruption: %v", err)
			}
			defer cancel()
			before, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || before == nil || before.Phase != "intent" {
				t.Fatalf("intent: %+v %v", before, err)
			}
			stage, err := f.backend.Stat(path.Base(before.StagePath))
			if err != nil {
				t.Fatal(err)
			}
			opKey := replaceguard.BirthOperationKey(f.backend.w.OperationID)
			inodeKey := replaceguard.BirthInodeKey(stage.Inode)
			raw, err := f.x.rdb.Get(context.Background(), opKey).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			b, err := replaceguard.DecodeBirth(raw)
			if err != nil {
				t.Fatal(err)
			}
			set := func(key string, value any) {
				t.Helper()
				if err := f.x.rdb.Set(context.Background(), key, value, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			switch fault {
			case "missing-operation":
				err = f.x.rdb.Del(context.Background(), opKey).Err()
			case "missing-inode":
				err = f.x.rdb.Del(context.Background(), inodeKey).Err()
			case "malformed":
				set(opKey, "bad receipt")
			case "owner":
				b.Owner = "mac-peer"
			case "database":
				b.Database++
			case "uuid":
				b.BackendUUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
			case "mode":
				b.Mode = 0600
			case "parent-name":
				b.Parents[1].Name = "elsewhere"
			case "oversized", "linked":
				key := "i" + fmt.Sprint(stage.Inode)
				attr, e := f.x.rdb.Get(context.Background(), key).Bytes()
				if e != nil {
					t.Fatal(e)
				}
				if fault == "oversized" {
					binary.BigEndian.PutUint64(attr[51:59], uint64(len(body)+1))
				} else {
					binary.BigEndian.PutUint32(attr[47:51], 2)
				}
				set(key, attr)
			case "missing-stage":
				err = f.x.rdb.HDel(context.Background(), "d4", stage.Name).Err()
			case "swapped-name":
				edge := make([]byte, 9)
				edge[0] = 1
				binary.BigEndian.PutUint64(edge[1:], 3)
				err = f.x.rdb.HSet(context.Background(), "d4", stage.Name, edge).Err()
			case "sealed":
				set(replaceguard.Key(stage.Inode), "prepared record already exists")
			}
			if err != nil {
				t.Fatal(err)
			}
			if fault == "owner" || fault == "database" || fault == "uuid" || fault == "mode" || fault == "parent-name" {
				changed, err := replaceguard.EncodeBirth(b)
				if err != nil {
					t.Fatal(err)
				}
				set(opKey, changed)
				set(inodeKey, changed)
			}
			keys := []string{"i3", "c3_0", "d4", "i" + fmt.Sprint(stage.Inode), "c" + fmt.Sprint(stage.Inode) + "_0", opKey, inodeKey, replaceguard.Key(stage.Inode)}
			snapshot := func() []string {
				t.Helper()
				out := make([]string, len(keys))
				for i, k := range keys {
					v, e := f.x.rdb.Dump(context.Background(), k).Result()
					if e != nil && e != redis.Nil {
						t.Fatal(e)
					}
					out[i] = v
				}
				return out
			}
			remote := snapshot()
			used, _ := f.spool.Capacity()
			f.reopen(t, true)
			resumeCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			if err := f.run(resumeCtx); err == nil {
				t.Fatal("unproven stage resumed")
			}
			after, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(remote, snapshot()) || f.backend.renames != 0 {
				t.Fatalf("refusal changed custody: %+v %v", after, err)
			}
			got, err := os.ReadFile(f.row.SpoolFile)
			if err != nil || !bytes.Equal(got, body) {
				t.Fatalf("private bytes: %v", err)
			}
			if after, _ := f.spool.Capacity(); after != used {
				t.Fatal("capacity released on refusal")
			}
		})
	}
}
