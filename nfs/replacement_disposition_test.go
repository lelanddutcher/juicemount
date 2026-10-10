//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func TestReplacementDispositionForeignObservationSurvivesNewClient(t *testing.T) {
	prior := &metadata.ReplacementDisposition{State: metadata.ReplacementWaitingWriter,
		ObservedPrincipal: "mac-peer@peer-host", ObservedEpoch: 3}
	// A fresh owner can be granted after the peer released, but its in-memory
	// foreignBefore may be zero. The saved authenticated peer observation wins.
	epoch, principal := replacementForeignObservation(prior, leaseDecision{held: true, epoch: 5, foreignBefore: 0}, 0)
	if epoch != 3 || principal != prior.ObservedPrincipal {
		t.Fatalf("lost saved peer tenure: epoch=%d principal=%q", epoch, principal)
	}
	epoch, principal = replacementForeignObservation(prior, leaseDecision{held: true, epoch: 5, foreignBefore: 5}, 0)
	if epoch != 5 || principal != prior.ObservedPrincipal {
		t.Fatalf("lost current foreign boundary: epoch=%d principal=%q", epoch, principal)
	}
	epoch, principal = replacementForeignObservation(nil, leaseDecision{held: true, epoch: 5, foreignBefore: 5}, 0)
	if epoch != 5 || principal != "" {
		t.Fatalf("current foreign boundary: epoch=%d principal=%q", epoch, principal)
	}
	if !errors.Is(&metadata.ReplacementRefusalError{State: metadata.ReplacementWriterChanged}, metadata.ErrMirrorConflict) {
		t.Fatal("typed refusal lost conflict identity")
	}
}

// An armed journal with an exact committed server receipt is historical
// evidence. Even a later refusal on the same retained row must be settled
// read-only before the writer lease or a pending schedule hold is considered.
func TestReplacementDispositionArmedCommittedReceipt(t *testing.T) {
	for _, state := range []string{metadata.ReplacementWaitingWriter, metadata.ReplacementWriterChanged} {
		t.Run(state, func(t *testing.T) {
			root := t.TempDir()
			raw := redistestfixture.Start(t)
			opts, err := redisurl.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			opts.ContextTimeoutEnabled, opts.MaxRetries = true, -1
			rdb := redis.NewClient(opts)
			t.Cleanup(func() { _ = rdb.Close() })
			seed := launchReplacementProcess(t, root, raw, "seed", replacementProcessCommittedRename)
			ready := awaitReplacementProcessReady(t, root, seed, replacementProcessCommittedRename)
			verifyReplacementProcessCut(t, root, rdb, ready)
			originalGraph := verifyReplacementCommittedRemote(t, root, rdb, ready)
			if err := seed.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-seed.done:
			case <-time.After(10 * time.Second):
				t.Fatal("killed seed did not join")
			}
			var exited *exec.ExitError
			if !errors.As(seed.err, &exited) {
				t.Fatalf("seed not signaled: %v", seed.err)
			}
			status, ok := exited.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("seed signal: %v", seed.err)
			}
			checkReplacementProcessTranscript(t, seed, "seed")
			deadline := time.Now().Add(15 * time.Second)
			for {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				value, err := rdb.Get(ctx, respproxy.SingletonKey).Result()
				cancel()
				if err == redis.Nil {
					break
				}
				if err != nil || value != ready.ProxyIncarnation || time.Now().After(deadline) {
					t.Fatalf("old singleton: %q %v", value, err)
				}
				time.Sleep(20 * time.Millisecond)
			}
			store, err := metadata.Open(filepath.Join(root, "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			if err := metadata.InitSpoolSchema(store.DB()); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			index := metadata.NewSpoolStore(store.DB())
			store.SetSpoolStore(index)
			row, err := index.Get(ready.RowID)
			if err != nil || row == nil {
				_ = store.Close()
				t.Fatalf("armed row: %+v %v", row, err)
			}
			armed, err := index.ReplacementPublication(row)
			if err != nil || armed == nil || armed.Phase != metadata.ReplacementPublicationArmed {
				_ = store.Close()
				t.Fatalf("armed journal: %+v %v", armed, err)
			}
			project := ""
			if state == metadata.ReplacementWaitingWriter {
				project = projectOf(row.NFSPath)
			}
			if err := index.RecordReplacementDisposition(*armed, state, "mac-peer@peer-host", 2, project); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			before, err := index.ReplacementDisposition(row.ID)
			if err != nil || before == nil || before.State != state {
				_ = store.Close()
				t.Fatalf("bound armed disposition: %+v %v", before, err)
			}
			if state == metadata.ReplacementWaitingWriter {
				wait, err := index.WaitProject(row.ID)
				if err != nil || wait != project {
					_ = store.Close()
					t.Fatalf("waiting armed schedule: %q %v", wait, err)
				}
			}
			snapshot := func(db *sql.DB) string {
				var values [9]any
				args := make([]any, len(values))
				for i := range values {
					args[i] = &values[i]
				}
				if err := db.QueryRow(`SELECT spool_id,operation_id,witness_sha256,journal_revision,journal_payload,state,observed_principal,observed_epoch,updated_at FROM spool_replacement_dispositions WHERE spool_id=?`, ready.RowID).Scan(args...); err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(values)
				if err != nil {
					t.Fatal(err)
				}
				return string(data)
			}
			historyBefore := snapshot(store.DB())
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			resume := launchReplacementProcess(t, root, raw, "resume", replacementProcessCommittedRename)
			select {
			case <-resume.done:
			case <-time.After(60 * time.Second):
				t.Fatalf("committed receipt did not settle: %s", resume.diagnostic(t))
			}
			if resume.err != nil {
				t.Fatalf("resume: %v\n%s", resume.err, resume.diagnostic(t))
			}
			checkReplacementProcessTranscript(t, resume, "resume")
			data, err := os.ReadFile(filepath.Join(root, "recovered.json"))
			if err != nil {
				t.Fatal(err)
			}
			var recovered replacementProcessRecovered
			if err := json.Unmarshal(data, &recovered); err != nil {
				t.Fatal(err)
			}
			if recovered.PID != resume.cmd.Process.Pid || recovered.PID == ready.PID || recovered.Phase != metadata.ReplacementPublicationCleaned || recovered.Renames != 0 || recovered.MutationCalls != 0 || recovered.ServerRecordSHA256 != ready.ServerRecordSHA256 || recovered.StageInode != ready.StageInode || recovered.ByteCount != len(replacementProcessBody()) || recovered.BodySHA256 != ready.BodySHA256 {
				t.Fatalf("historical receipt settlement: %+v", recovered)
			}
			if verifyReplacementCommittedRemote(t, root, rdb, ready) != originalGraph {
				t.Fatal("remote committed graph changed")
			}
			if _, err := os.Lstat(ready.SpoolPath); !os.IsNotExist(err) {
				t.Fatalf("private image not retired: %v", err)
			}
			uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.db"), RawQuery: "mode=ro"}
			db, err := sql.Open("sqlite", uri.String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if snapshot(db) != historyBefore {
				t.Fatal("historical disposition changed during settlement")
			}

		})
	}
}
