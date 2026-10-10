//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// An already committed server receipt permits local reconciliation only.
// Any attempt to reopen/recreate a stage, inspect a new destination tuple, or
// repeat the rename fails this oracle before it can mutate the fixture.
type replacementRecoveryIO struct {
	replacementPublishIO
	checks, opens, stats, renames int
}

func (b *replacementRecoveryIO) Check() error { b.checks++; return b.replacementPublishIO.Check() }
func (b *replacementRecoveryIO) Open(string, bool) (replacementPublishFile, error) {
	b.opens++
	return nil, errors.New("recovery attempted stage I/O")
}
func (b *replacementRecoveryIO) Stat(string) (replaceguard.File, error) {
	b.stats++
	return replaceguard.File{}, errors.New("recovery attempted new destination evidence")
}
func (b *replacementRecoveryIO) Rename(string, string) error {
	b.renames++
	return errors.New("recovery attempted a second rename")
}

// The client is synchronous and never starts heartbeats in this fixture.
// Observe actual forwarded lease commands; do not replace server decisions.
type replacementRecoveryLeaseDoer struct {
	leaseDoer
	acquires, arms int
}

func (d *replacementRecoveryLeaseDoer) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	if len(args) >= 2 && args[0] == "JM.LEASE" {
		switch args[1] {
		case "ACQUIRE":
			d.acquires++
		case "ARM_REPLACE":
			d.arms++
		}
	}
	return d.leaseDoer.Do(ctx, args...)
}

func replacementPeerTenure(t *testing.T, original *ProjectLeaseClient, proxyURL, project string, history bool) {
	t.Helper()
	// Use authenticated wire transitions to model a legal later
	// writer tenure. The public Release method defers for pending
	// local rows, so fixture setup calls the underlying wire path.
	status, err := original.releaseNow(context.Background(), project)
	if err != nil || status != "released" {
		t.Fatalf("original wire release: %s %v", status, err)
	}
	peerURL, err := url.Parse(proxyURL)
	if err != nil {
		t.Fatal(err)
	}
	peerURL.User = url.UserPassword("mac-peer", "mac-secret")
	opts, err := redis.ParseURL(peerURL.String())
	if err != nil {
		t.Fatal(err)
	}
	opts.Protocol, opts.DisableIdentity, opts.MaxRetries = 2, true, -1
	peer := redis.NewClient(opts)
	t.Cleanup(func() { _ = peer.Close() })
	grant, err := peer.Do(context.Background(), "JM.LEASE", "ACQUIRE", "path:"+project, "auto").Slice()
	if err != nil || len(grant) != 5 || grant[0] != "GRANTED" || grant[1] != int64(3) {
		t.Fatalf("peer authenticated grant: %v %v", grant, err)
	}
	if history {
		released, err := peer.Do(context.Background(), "JM.LEASE", "RELEASE", "path:"+project, "3").Slice()
		if err != nil || len(released) != 2 || released[0] != "RELEASED" || released[1] != int64(4) {
			t.Fatalf("peer authenticated release: %v %v", released, err)
		}
	}
}

func TestReplacementPublishCommittedReceiptAfterReopen(t *testing.T) {
	for _, receiptMode := range []string{"exact", "missing", "changed", "foreign-holder", "foreign-history"} {
		t.Run(receiptMode, func(t *testing.T) {
			x, spool, d, row, witness, body := replacementNestedFixture(t)
			fs, proxyURL := replacementProxyAndFS(t, x, spool, d)
			usedBefore, _ := spool.Capacity()
			const fault = "replacement-local-commit-injected"
			if _, err := x.store.DB().Exec(`CREATE TRIGGER replacement_test_commit_fault
                BEFORE UPDATE OF phase ON spool_replacement_publications
                WHEN NEW.phase='committed'
                BEGIN SELECT RAISE(ABORT,'replacement-local-commit-injected'); END`); err != nil {
				t.Fatal(err)
			}
			binding := func(got metadata.CachePathBinding) error {
				if got != witness.Binding {
					return metadata.ErrMirrorConflict
				}
				return nil
			}
			initial := &replacementSDKIO{fs: fs, m: fs.Meta(), w: *witness, ctx: jfsmeta.Background(), parent: 4}
			err := d.publishReplacement(context.Background(), row.ID, func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				if w.OperationID != witness.OperationID {
					return nil, nil, metadata.ErrMirrorConflict
				}
				return initial, func() {}, nil
			}, binding)
			if err == nil || strings.Count(err.Error(), fault) != 1 || initial.renames != 1 {
				t.Fatalf("required exact post-rename SQL fault: err=%v renames=%d", err, initial.renames)
			}
			beforeRow, err := spool.Meta().Get(row.ID)
			if err != nil || !reflect.DeepEqual(beforeRow, row) {
				t.Fatalf("initial publication changed accepted row: %+v %v", beforeRow, err)
			}
			armed, err := spool.Meta().ReplacementPublication(beforeRow)
			if err != nil || armed == nil || armed.Phase != metadata.ReplacementPublicationArmed {
				t.Fatalf("durable armed journal=%+v err=%v", armed, err)
			}
			prepared, err := replaceguard.Decode([]byte(armed.Record))
			if err != nil {
				t.Fatal(err)
			}
			server, err := d.leasesPtr.Load().replacementRead(context.Background(), prepared.Stage.Inode)
			if err != nil || server == nil || server.Phase != "committed" || !sameReplacementEvidence(prepared, *server, false) {
				t.Fatalf("actual committed server receipt=%+v err=%v", server, err)
			}
			published, st := fs.Open(jfsmeta.Background(), "/media/project/clip.mov", jfsmeta.MODE_MASK_R)
			if st != 0 {
				t.Fatal(st)
			}
			got := make([]byte, len(body))
			n, readErr := published.Pread(jfsmeta.Background(), got, 0)
			closeErr := published.Close(jfsmeta.Background())
			if n != len(body) || readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != 0 || !bytes.Equal(got, body) {
				t.Fatalf("published bytes before restart n=%d read=%v close=%v", n, readErr, closeErr)
			}
			if _, err := x.store.DB().Exec(`DROP TRIGGER replacement_test_commit_fault`); err != nil {
				t.Fatal(err)
			}
			unchanged, err := spool.Meta().ReplacementPublication(beforeRow)
			if err != nil || unchanged == nil || *unchanged != *armed {
				t.Fatalf("removing fixture fault changed journal: %+v %v", unchanged, err)
			}
			if strings.HasPrefix(receiptMode, "foreign-") {
				replacementPeerTenure(t, d.leasesPtr.Load(), proxyURL, projectOf(row.NFSPath), receiptMode == "foreign-history")
			}
			d.leasesPtr.Load().Stop()
			store, recovered := reopenReplacementPublicationFixture(t, x, spool.root)
			if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
				t.Fatal(err)
			}
			next, err := NewDrainer(recovered, DrainerConfig{FuseRoot: x.fuseRoot})
			if err != nil {
				t.Fatal(err)
			}
			handler := NewHandler(store, x.fuseRoot)
			t.Cleanup(func() { handler.StopHandler() })
			handler.SetSpool(recovered, next)
			client, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: proxyURL, Spool: recovered})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Stop)
			leaseCommands := &replacementRecoveryLeaseDoer{leaseDoer: client.rdb}
			client.rdb = leaseCommands
			next.SetProjectLeases(client)
			reopenedRow, err := recovered.Meta().Get(row.ID)
			if err != nil || !reflect.DeepEqual(reopenedRow, beforeRow) {
				t.Fatalf("reopened row changed: %+v %v", reopenedRow, err)
			}
			reopened, err := recovered.Meta().ReplacementPublication(reopenedRow)
			if err != nil || reopened == nil || *reopened != *armed {
				t.Fatalf("reopened journal changed: %+v %v", reopened, err)
			}
			key := replaceguard.Key(prepared.Stage.Inode)
			switch receiptMode {
			case "missing":
				if removed, err := x.rdb.Del(context.Background(), key).Result(); err != nil || removed != 1 {
					t.Fatalf("remove exact fixture receipt: %d %v", removed, err)
				}
			case "changed":
				changed := *server
				changed.ImageSHA256 = strings.Repeat("a", 64)
				if changed.ImageSHA256 == server.ImageSHA256 {
					t.Fatal("mutation did not change image hash")
				}
				encoded, err := replaceguard.Encode(changed)
				if err != nil {
					t.Fatal(err)
				}
				if err := x.rdb.Set(context.Background(), key, encoded, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			beforeReceipt, beforeReceiptErr := x.rdb.Get(context.Background(), key).Result()
			if beforeReceiptErr != nil && beforeReceiptErr != redis.Nil {
				t.Fatal(beforeReceiptErr)
			}
			leaseKey, epochKey := respproxy.LeaseKeyPrefix+"4", respproxy.EpochKeyPrefix+"4"
			beforeLease, err := x.rdb.HGetAll(context.Background(), leaseKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeEpoch, err := x.rdb.Get(context.Background(), epochKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			if receiptMode == "foreign-holder" && (beforeLease["holder"] != "mac-peer" || beforeEpoch != "3") {
				t.Fatalf("missing live peer tenure: %v %s", beforeLease, beforeEpoch)
			}
			if receiptMode == "foreign-history" && (beforeLease["holder"] != "" || beforeEpoch != "4") {
				t.Fatalf("missing ended peer tenure: %v %s", beforeLease, beforeEpoch)
			}
			recovery := &replacementRecoveryIO{replacementPublishIO: &replacementSDKIO{fs: fs, m: fs.Meta(), w: *witness, ctx: jfsmeta.Background(), parent: 4}}
			factoryCalls := 0
			err = next.publishReplacement(context.Background(), row.ID, func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				factoryCalls++
				if w.OperationID != witness.OperationID {
					return nil, nil, metadata.ErrMirrorConflict
				}
				return recovery, func() {}, nil
			}, binding)
			if receiptMode == "exact" || strings.HasPrefix(receiptMode, "foreign-") {
				if err != nil {
					t.Fatalf("reconcile exact committed receipt: %v", err)
				}
			} else if !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatalf("unqualified receipt must refuse: %v", err)
			}
			if factoryCalls != 1 || recovery.checks == 0 || recovery.opens != 0 || recovery.stats != 0 || recovery.renames != 0 || initial.renames != 1 {
				t.Fatalf("recovery repeated publication: factories=%d backend=%+v initial_renames=%d", factoryCalls, recovery, initial.renames)
			}
			afterReceipt, afterReceiptErr := x.rdb.Get(context.Background(), key).Result()
			if afterReceipt != beforeReceipt || afterReceiptErr != beforeReceiptErr {
				t.Fatalf("recovery modified remote receipt: before_err=%v after_err=%v", beforeReceiptErr, afterReceiptErr)
			}
			afterLease, leaseErr := x.rdb.HGetAll(context.Background(), leaseKey).Result()
			afterEpoch, epochErr := x.rdb.Get(context.Background(), epochKey).Result()
			if leaseErr != nil || epochErr != nil || !reflect.DeepEqual(beforeLease, afterLease) || beforeEpoch != afterEpoch {
				t.Fatalf("receipt recovery changed writer authority: before=%v/%s after=%v/%s errors=%v/%v", beforeLease, beforeEpoch, afterLease, afterEpoch, leaseErr, epochErr)
			}
			if leaseCommands.acquires != 0 || leaseCommands.arms != 0 {
				t.Fatalf("receipt-only recovery issued writer commands: acquire=%d arm=%d", leaseCommands.acquires, leaseCommands.arms)
			}
			finalRow, err := recovered.Meta().Get(row.ID)
			if err != nil || !reflect.DeepEqual(finalRow, beforeRow) {
				t.Fatalf("receipt recovery changed accepted row: %+v %v", finalRow, err)
			}
			final, err := recovered.Meta().ReplacementPublication(finalRow)
			if err != nil || final == nil {
				t.Fatalf("final journal=%+v err=%v", final, err)
			}
			if receiptMode == "exact" || strings.HasPrefix(receiptMode, "foreign-") {
				if final.Phase != metadata.ReplacementPublicationCommitted || final.Record != replacementRecordBytes(*server) || final.Revision != armed.Revision+1 {
					t.Fatalf("committed journal did not bind server receipt: %+v", final)
				}
			} else if *final != *armed {
				t.Fatalf("refused receipt changed durable journal: %+v", final)
			}
			mirror := store.LookupByPath(row.NFSPath)
			if mirror == nil || !mirror.LocalOnly || mirror.Version != armed.Expected || mirror.Size != int64(len(body)) {
				t.Fatalf("local mirror custody changed: %+v", mirror)
			}
			if usedAfter, _ := recovered.Capacity(); usedAfter != usedBefore {
				t.Fatalf("capacity changed across recovery: %d -> %d", usedBefore, usedAfter)
			}
			if private, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(private, body) {
				t.Fatalf("private image lost: %v", err)
			}
			pin.SetOffline(true)
			local, err := (&juiceFS{handler: handler}).OpenFile(row.NFSPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			got = make([]byte, len(body))
			n, readErr = local.ReadAt(got, 0)
			localClose := local.Close()
			if n != len(body) || readErr != nil && !errors.Is(readErr, io.EOF) || localClose != nil || !bytes.Equal(got, body) {
				t.Fatalf("offline reads after receipt reconciliation n=%d read=%v close=%v", n, readErr, localClose)
			}
		})
	}
}

// Stop after durable ARM but before namespace publication. A prepared receipt
// still needs writer authority; the historical-commit path must not accept it.
type replacementBeforeRenameIO struct {
	replacementPublishIO
	attempts int
}

func (b *replacementBeforeRenameIO) Rename(string, string) error {
	b.attempts++
	return errors.New("replacement-before-rename-injected")
}
func TestReplacementPreparedRecoveryRequiresWriterLease(t *testing.T) {
	x, spool, d, row, w, body := replacementNestedFixture(t)
	fs, proxyURL := replacementProxyAndFS(t, x, spool, d)
	initial := &replacementBeforeRenameIO{replacementPublishIO: &replacementSDKIO{fs: fs, m: fs.Meta(), w: *w, ctx: jfsmeta.Background(), parent: 4}}
	binding := func(got metadata.CachePathBinding) error {
		if got != w.Binding {
			return metadata.ErrMirrorConflict
		}
		return nil
	}
	err := d.publishReplacement(context.Background(), row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return initial, func() {}, nil
	}, binding)
	if err == nil || strings.Count(err.Error(), "replacement-before-rename-injected") != 1 || initial.attempts != 1 {
		t.Fatalf("before-rename fault not reached: %v attempts=%d", err, initial.attempts)
	}
	armed, err := spool.Meta().ReplacementPublication(row)
	if err != nil || armed == nil || armed.Phase != metadata.ReplacementPublicationArmed {
		t.Fatalf("prepared journal: %+v %v", armed, err)
	}
	record, err := replaceguard.Decode([]byte(armed.Record))
	if err != nil {
		t.Fatal(err)
	}
	server, err := d.leasesPtr.Load().replacementRead(context.Background(), record.Stage.Inode)
	if err != nil || server == nil || replacementRecordBytes(*server) != armed.Record {
		t.Fatalf("server not exactly prepared: %+v %v", server, err)
	}
	replacementPeerTenure(t, d.leasesPtr.Load(), proxyURL, projectOf(row.NFSPath), false)
	d.leasesPtr.Load().Stop()
	fresh, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: proxyURL, Spool: spool})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fresh.Stop)
	commands := &replacementRecoveryLeaseDoer{leaseDoer: fresh.rdb}
	fresh.rdb = commands
	d.SetProjectLeases(fresh)
	leaseKey, epochKey := respproxy.LeaseKeyPrefix+"4", respproxy.EpochKeyPrefix+"4"
	before, err := x.rdb.HGetAll(context.Background(), leaseKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if before["holder"] != "mac-peer" || before["epoch"] != "3" {
		t.Fatalf("peer lease absent: %v", before)
	}
	beforeRow, err := spool.Meta().Get(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prior, err := spool.Meta().ReplacementDisposition(row.ID); err != nil || prior != nil {
		t.Fatalf("unexpected prior disposition: %+v %v", prior, err)
	}
	used, _ := spool.Capacity()
	recovery := &replacementRecoveryIO{replacementPublishIO: initial.replacementPublishIO}
	err = d.publishReplacement(context.Background(), row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return recovery, func() {}, nil
	}, binding)
	if !errors.Is(err, metadata.ErrMirrorConflict) || commands.acquires != 1 || commands.arms != 0 || recovery.opens != 0 || recovery.stats != 0 || recovery.renames != 0 {
		t.Fatalf("prepared recovery bypassed writer: err=%v acquire=%d arm=%d io=%+v", err, commands.acquires, commands.arms, recovery)
	}
	var refusal *metadata.ReplacementRefusalError
	if !errors.As(err, &refusal) || refusal.State != metadata.ReplacementWaitingWriter || refusal.ObservedPrincipal != "mac-peer@peer-host" || refusal.ObservedEpoch != 3 {
		t.Fatalf("wrong prepared refusal: %#v %v", refusal, err)
	}
	wantedDisposition := metadata.ReplacementDisposition{RowID: row.ID, OperationID: armed.OperationID, WitnessSHA256: armed.WitnessSHA256, JournalRevision: armed.Revision, State: metadata.ReplacementWaitingWriter, ObservedPrincipal: "mac-peer@peer-host", ObservedEpoch: 3}
	if disposition, err := spool.Meta().ReplacementDisposition(row.ID); err != nil || disposition == nil || *disposition != wantedDisposition {
		t.Fatalf("unbound waiting disposition: %+v %v", disposition, err)
	}
	if wait, err := spool.Meta().WaitProject(row.ID); err != nil || wait != projectOf(row.NFSPath) {
		t.Fatalf("wrong project wait: %q %v", wait, err)
	}
	after, err := x.rdb.HGetAll(context.Background(), leaseKey).Result()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("foreign lease mutated: %v %v", after, err)
	}
	if epoch, err := x.rdb.Get(context.Background(), epochKey).Result(); err != nil || epoch != "3" {
		t.Fatalf("foreign epoch changed: %s %v", epoch, err)
	}
	got, err := x.rdb.Get(context.Background(), replaceguard.Key(record.Stage.Inode)).Result()
	if err != nil || got != armed.Record {
		t.Fatalf("prepared receipt mutated: %v", err)
	}
	current, err := spool.Meta().Get(row.ID)
	if err != nil || current == nil {
		t.Fatalf("prepared row missing: %+v %v", current, err)
	}
	const waitingMessage = "Your save is kept on this Mac while another Mac holds the project. It will be checked again when the project is available."
	if current.LastError != waitingMessage || current.UpdatedAt.Before(beforeRow.UpdatedAt) {
		t.Fatalf("wrong waiting explanation: %+v", current)
	}
	// Only the durable waiting explanation may change; preserve every saved
	// image, state, attempt, custody and accounting field in the row.
	expectedRow := *beforeRow
	expectedRow.LastError, expectedRow.UpdatedAt = waitingMessage, current.UpdatedAt
	if !reflect.DeepEqual(current, &expectedRow) {
		t.Fatalf("prepared row changed beyond waiting explanation: %+v", current)
	}
	journal, err := spool.Meta().ReplacementPublication(current)
	if err != nil || journal == nil || *journal != *armed {
		t.Fatalf("prepared journal mutated: %+v %v", journal, err)
	}
	if after, _ := spool.Capacity(); after != used {
		t.Fatalf("prepared capacity released: %d -> %d", used, after)
	}
	if local, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(local, body) {
		t.Fatalf("prepared private bytes lost: %v", err)
	}
}
