//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/metadata"
)

// These faults happen after real SDK effects. They do not emulate a process
// kill or claim automatic recovery of a stage without durable ownership.
type replacementInterruptedUploadIO struct {
	replacementPublishIO
	at     string
	cancel context.CancelFunc
	writes int
	lost   error
}

func (b *replacementInterruptedUploadIO) Create(name string, mode os.FileMode) (replacementPublishFile, error) {
	f, err := b.replacementPublishIO.Create(name, mode)
	if err != nil {
		return f, err
	}
	if b.at == "lost-create-reply" {
		if err := f.Close(); err != nil {
			return nil, err
		}
		return nil, b.lost
	}
	if b.at == "after-create" {
		b.cancel()
	}
	return &replacementInterruptedUploadFile{replacementPublishFile: f, owner: b}, nil
}

type replacementInterruptedUploadFile struct {
	replacementPublishFile
	owner *replacementInterruptedUploadIO
}

func (f *replacementInterruptedUploadFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.replacementPublishFile.WriteAt(p, off)
	if n > 0 {
		f.owner.writes++
	}
	if f.owner.at == "after-first-chunk" && f.owner.writes == 1 {
		f.owner.cancel()
	}
	return n, err
}

func TestReplacementInterruptedUploadRetainsUnownedStage(t *testing.T) {
	for _, at := range []string{"after-create", "after-first-chunk", "lost-create-reply", "before-identity-commit"} {
		t.Run(at, func(t *testing.T) {
			body := bytes.Repeat([]byte("interrupted-sdk-save-"), 160003)
			f := newReplacementResumeFixture(t, body)
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
			fault := &replacementInterruptedUploadIO{replacementPublishIO: f.backend, at: at, cancel: cancel, lost: errors.New("controlled lost create acknowledgement")}
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
			// Seed the historical unreceipted stage state explicitly. A new
			// client cannot create this state now, but must still refuse its
			// filename on recovery without adopting or deleting it.
			opKey := replaceguard.BirthOperationKey(f.backend.w.OperationID)
			inodeKey := replaceguard.BirthInodeKey(stage.Inode)
			raw, err := f.x.rdb.Get(context.Background(), opKey).Bytes()
			if err != nil {
				t.Fatal(err)
			}
			reverse, err := f.x.rdb.Get(context.Background(), inodeKey).Bytes()
			if err != nil || !bytes.Equal(raw, reverse) {
				t.Fatalf("seed birth pair mismatch: %v", err)
			}
			birth, err := replaceguard.DecodeBirth(raw)
			if err != nil || birth.OperationID != f.backend.w.OperationID || birth.Stage.Inode != stage.Inode || birth.Stage.Parent != stage.Parent || birth.Stage.Name != stage.Name || birth.Owner != "mac-test" || birth.Database != 3 {
				t.Fatalf("seed birth identity: %+v %v", birth, err)
			}
			if n, err := f.x.rdb.Del(context.Background(), opKey, inodeKey).Result(); err != nil || n != 2 {
				t.Fatalf("seed historical stage: removed=%d error=%v", n, err)
			}
			f.reopen(t, true)
			resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer resumeCancel()
			if err := f.run(resumeCtx); !errors.Is(err, os.ErrExist) {
				t.Fatalf("unowned stage must refuse with EEXIST: %v", err)
			}
			row, err := f.spool.meta.Get(f.row.ID)
			if err != nil {
				t.Fatal(err)
			}
			after, err := f.spool.meta.ReplacementPublication(row)
			if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(row, f.row) {
				t.Fatalf("recovery changed accepted custody: %+v %v", after, err)
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
			if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("private save changed: %v", err)
			}
			if after, _ := f.spool.Capacity(); after != used {
				t.Fatalf("capacity changed: %d != %d", after, used)
			}
			t.Logf("interruption=%s durable_phase=intent unowned_stage_bytes=%d accepted_bytes=%d resume=EEXIST original_graph=unchanged", at, stage.Length, len(body))
			assertReplacementOfflineNFSRoute(t, f.x, f.spool, row, body, used, true, true)
		})
	}
}
