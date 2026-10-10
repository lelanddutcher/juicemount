package metadata

import (
	"errors"
	"path"
	"sync"
	"testing"
	"time"
)

func TestSubscriptionEventPreservesAcceptedReplacement(t *testing.T) {
	f, _, _ := acceptedPublicationFixture(t)
	before := f.store.LookupByPath(f.w.Destination.Path).snapshot()
	parentPath := f.w.Destination.Chain[0].Path
	parent := f.store.LookupByPath(parentPath).snapshot()
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	var content, names int
	f.store.SetOnContentInvalidated(func(string, bool) { content++ })
	f.store.SetOnPathInvalidated(func(string, bool) { names++ })
	for _, evt := range []MetadataEvent{
		{Op: "create", Path: before.Path, Inode: 8001, Size: 1},
		{Op: "update", Path: before.Path, Inode: 8002, Size: 1},
		{Op: "delete", Path: before.Path},
		{Op: "rename", OldPath: before.Path, Path: "other.mov", Inode: 8003},
		{Op: "rename", OldPath: "other.mov", Path: before.Path, Inode: 8004},
		{Op: "update", Path: parentPath, Inode: 8005},
		{Op: "create", Path: "alias.mov", Inode: before.Inode, Size: 1},
		{Op: "create", Path: "unrelated-dir", IsDir: true, Inode: 8006},
	} {
		rc.applyEvent(evt)
		if content != 0 || names != 0 {
			t.Fatalf("protected event %+v fired callbacks content=%d names=%d", evt, content, names)
		}
		if got := f.store.LookupByPath(before.Path); !sameReplacementEntry(got, before) {
			t.Fatalf("protected event %+v changed destination: %+v", evt, got)
		}
		if got := f.store.LookupByPath(parentPath); !sameReplacementEntry(got, parent) {
			t.Fatalf("protected event %+v changed ancestor: %+v", evt, got)
		}
	}
	if f.store.LookupByPath("other.mov") != nil || f.store.LookupByPath("alias.mov") != nil || f.store.LookupByPath("unrelated-dir") != nil {
		t.Fatal("protected event created an alias or directory")
	}
	// An unrelated file remains on the immediate subscription path while a
	// replacement is held; the guard does not stall the whole event stream.
	rc.applyEvent(MetadataEvent{Op: "create", Path: "ordinary.mov", Inode: 9001, Size: 19, Mtime: time.Now().Unix()})
	if got := f.store.LookupByPath("ordinary.mov"); got == nil || got.Size != 19 || content != 1 {
		t.Fatalf("ordinary file event got=%+v callbacks=%d", got, content)
	}
	sibling := path.Join(path.Dir(before.Path), "sibling.mov")
	rc.applyEvent(MetadataEvent{Op: "create", Path: sibling, Inode: 9004, Size: 20, Mtime: time.Now().Unix()})
	if got := f.store.LookupByPath(sibling); got == nil || got.Size != 20 || content != 2 {
		t.Fatalf("same-project sibling event got=%+v callbacks=%d", got, content)
	}
}

func TestSubscriptionEventLegacyAndMalformedWitnessFailClosed(t *testing.T) {
	for _, variant := range []string{"legacy", "missing", "malformed"} {
		t.Run(variant, func(t *testing.T) {
			f, row, _ := acceptedPublicationFixture(t)
			before := f.store.LookupByPath(f.w.Destination.Path).snapshot()
			if variant == "legacy" {
				if _, err := f.store.DB().Exec(`DELETE FROM spool_replacement_publications WHERE spool_id=?`, row.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.DB().Exec(`UPDATE spool_entries SET replacement_publication_required=0 WHERE id=?`, row.ID); err != nil {
					t.Fatal(err)
				}
			} else if variant == "missing" {
				if _, err := f.store.DB().Exec(`DELETE FROM spool_replacement_publications WHERE spool_id=?`, row.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.store.DB().Exec(`UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?`, row.ID); err != nil {
					t.Fatal(err)
				}
			}
			rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
			var callbacks int
			f.store.SetOnContentInvalidated(func(string, bool) { callbacks++ })
			rc.applyEvent(MetadataEvent{Op: "update", Path: before.Path, Inode: 7001, Size: 1})
			if callbacks != 0 || !sameReplacementEntry(f.store.LookupByPath(before.Path), before) {
				t.Fatalf("%s changed accepted mirror or invalidated content", variant)
			}
			if variant != "legacy" {
				rc.applyEvent(MetadataEvent{Op: "create", Path: "other.mov", Inode: 7002})
				if f.store.LookupByPath("other.mov") != nil || callbacks != 0 {
					t.Fatal("malformed ownership evidence granted an unrelated event")
				}
			}
		})
	}
}

func TestSubscriptionEventAfterReplacementCleanup(t *testing.T) {
	f, _, p := settledCleanupMetadataFixture(t)
	cleaning, err := f.store.BeginReplacementCleanup(p, "1:2", "1:3", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.FinishReplacementCleanup(*cleaning, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	wantSize := f.store.LookupByPath(f.w.Destination.Path).Size + 1
	rc.applyEvent(MetadataEvent{Op: "update", Path: f.w.Destination.Path, Inode: 9002, Size: wantSize, Mtime: time.Now().Unix()})
	if got := f.store.LookupByPath(f.w.Destination.Path); got == nil || got.Inode != 9002 || got.Size != wantSize {
		t.Fatalf("cleaned replacement still blocked ordinary event: %+v", got)
	}
}

func TestSubscriptionAcceptanceAndEventAreSerialized(t *testing.T) {
	f := newReplacementFixture(t, false)
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	wantSize := f.store.LookupByPath(f.w.Destination.Path).Size + 1
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	eventHeldGate := make(chan bool, 1)
	f.store.SetOnContentInvalidated(func(string, bool) {
		if f.store.replacementObservationMu.TryLock() {
			f.store.replacementObservationMu.Unlock()
			eventHeldGate <- false
		} else {
			eventHeldGate <- true
		}
		close(entered)
		<-release
	})
	eventDone := make(chan struct{})
	go func() {
		rc.applyEvent(MetadataEvent{Op: "update", Path: f.w.Destination.Path, Inode: 7001, Size: wantSize})
		close(eventDone)
	}()
	<-entered
	acceptDone := make(chan error, 1)
	go func() {
		_, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil })
		acceptDone <- err
	}()
	releaseOnce.Do(func() { close(release) })
	<-eventDone
	if !<-eventHeldGate {
		t.Fatal("event callback ran without observation gate")
	}
	if err := <-acceptDone; !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("event-first acceptance=%v, want changed-mirror conflict", err)
	}
	// Now hold acceptance at its caller-owned source check, which runs under
	// the observation gate. The event can only inspect ownership after both
	// the SQLite acceptance and RAM move have completed.
	f2 := newReplacementFixture(t, false)
	rc2 := &RedisClient{store: f2.store, stopCh: make(chan struct{})}
	acceptedCheck, releaseAccept := make(chan struct{}), make(chan struct{})
	var releaseAcceptOnce sync.Once
	defer releaseAcceptOnce.Do(func() { close(releaseAccept) })
	acceptHeldGate := make(chan bool, 1)
	var once sync.Once
	acceptedDone := make(chan error, 1)
	go func() {
		_, err := f2.store.AcceptSpoolReplacement(f2.w, func() error {
			once.Do(func() {
				if f2.store.replacementObservationMu.TryLock() {
					f2.store.replacementObservationMu.Unlock()
					acceptHeldGate <- false
				} else {
					acceptHeldGate <- true
				}
				close(acceptedCheck)
				<-releaseAccept
			})
			return nil
		})
		acceptedDone <- err
	}()
	<-acceptedCheck
	remoteDone := make(chan struct{})
	go func() {
		rc2.applyEvent(MetadataEvent{Op: "update", Path: f2.w.Destination.Path, Inode: 9003, Size: f2.w.Image.Size + 1})
		close(remoteDone)
	}()
	releaseAcceptOnce.Do(func() { close(releaseAccept) })
	if err := <-acceptedDone; err != nil {
		t.Fatalf("acceptance-first=%v", err)
	}
	if !<-acceptHeldGate {
		t.Fatal("acceptance verifier ran without observation gate")
	}
	<-remoteDone
	if got := f2.store.LookupByPath(f2.w.Destination.Path); got == nil || !got.LocalOnly || got.Size != f2.w.Image.Size {
		t.Fatalf("event raced accepted RAM publication: %+v", got)
	}
}

func TestSubscriptionSettlementAliasTransitionIsSerialized(t *testing.T) {
	f, _, committed, gate, captured, proof, graph := committedSettlementFixture(t)
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	precommit, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	settleHeldGate := make(chan bool, 1)
	var checks int
	settled := make(chan error, 1)
	go func() {
		_, err := gate.SettleSpoolReplacement(committed, captured, proof, proof, graph, func() error {
			checks++
			if checks == 2 { // inside the transaction with the observation gate held
				if f.store.replacementObservationMu.TryLock() {
					f.store.replacementObservationMu.Unlock()
					settleHeldGate <- false
				} else {
					settleHeldGate <- true
				}
				close(precommit)
				<-release
			}
			return nil
		})
		settled <- err
	}()
	<-precommit
	// Lookup serves from RAM while settlement holds its Store writer locks.
	if got := f.store.lookupByPathRAM(f.w.Destination.Path); got == nil || got.Inode == 0 {
		t.Fatalf("cached lookup stalled or disappeared: %+v", got)
	}
	remoteDone := make(chan struct{})
	go func() {
		// The stage inode becomes the destination's current RAM identity at
		// settlement; a remote alias event cannot publish it elsewhere.
		rc.applyEvent(MetadataEvent{Op: "create", Path: "alias.mov", Inode: proof.Nodes[len(proof.Nodes)-1].Inode, Size: f.w.Image.Size})
		close(remoteDone)
	}()
	releaseOnce.Do(func() { close(release) })
	if err := <-settled; err != nil {
		t.Fatalf("settlement=%v", err)
	}
	if !<-settleHeldGate {
		t.Fatal("settlement verifier ran without observation gate")
	}
	<-remoteDone
	if got := f.store.LookupByPath("alias.mov"); got != nil {
		t.Fatalf("event published protected settled inode alias: %+v", got)
	}
}

func TestSubscriptionLegacyIntentUpgradeIsSerialized(t *testing.T) {
	f, row, _ := acceptedPublicationFixture(t)
	if _, err := f.store.DB().Exec(`DELETE FROM spool_replacement_publications WHERE spool_id=?`, row.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.DB().Exec(`UPDATE spool_entries SET replacement_publication_required=0 WHERE id=?`, row.ID); err != nil {
		t.Fatal(err)
	}
	before := f.store.LookupByPath(f.w.Destination.Path).snapshot()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	intentHeldGate := make(chan bool, 1)
	var once sync.Once
	intentDone := make(chan error, 1)
	go func() {
		_, err := f.store.BeginSpoolReplacementPublication(row.ID, before.Version, func() error {
			once.Do(func() {
				if f.store.replacementObservationMu.TryLock() {
					f.store.replacementObservationMu.Unlock()
					intentHeldGate <- false
				} else {
					intentHeldGate <- true
				}
				close(entered)
				<-release
			})
			return nil
		})
		intentDone <- err
	}()
	<-entered
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	eventDone := make(chan struct{})
	go func() {
		rc.applyEvent(MetadataEvent{Op: "update", Path: before.Path, Inode: 9005, Size: before.Size + 1})
		close(eventDone)
	}()
	releaseOnce.Do(func() { close(release) })
	if err := <-intentDone; err != nil {
		t.Fatalf("legacy intent upgrade=%v", err)
	}
	if !<-intentHeldGate {
		t.Fatal("legacy intent verifier ran without observation gate")
	}
	<-eventDone
	if got := f.store.LookupByPath(before.Path); !sameReplacementEntry(got, before) {
		t.Fatalf("event clobbered upgraded legacy intent: %+v", got)
	}
}

func TestSubscriptionHighBitAcceptedInodeAliasAfterRAMEviction(t *testing.T) {
	f := newReplacementFixture(t, false)
	source := f.store.LookupByPath(f.w.Source.Path).snapshot()
	highInode := uint64(1)<<63 | 0x12345
	source.Inode = highInode
	source.Version = MirrorToken{}
	if err := f.store.Insert(source); err != nil {
		t.Fatal(err)
	}
	f.w.Source = f.store.LookupByPath(source.Path).Version
	if _, err := f.store.AcceptSpoolReplacement(f.w, func() error { return nil }); err != nil {
		t.Fatalf("accept high-bit local inode: %v", err)
	}
	f.store.DeleteFromCache(f.w.Destination.Path)
	if got := f.store.lookupByPathRAM(f.w.Destination.Path); got != nil {
		t.Fatalf("destination remained in RAM: %+v", got)
	}
	if got := f.store.lookupByPathSQLite(f.w.Destination.Path); got == nil || got.Inode != highInode {
		t.Fatalf("durable high-bit destination: %+v", got)
	}
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	var invalidations int
	f.store.SetOnContentInvalidated(func(string, bool) { invalidations++ })
	alias := path.Join(path.Dir(f.w.Destination.Path), "alias.mov")
	rc.applyEvent(MetadataEvent{Op: "create", Path: alias, Inode: highInode, Size: 1})
	if got := f.store.LookupByPath(alias); got != nil || invalidations != 0 {
		t.Fatalf("evicted high-bit alias admitted: %+v invalidations=%d", got, invalidations)
	}
	sibling := path.Join(path.Dir(f.w.Destination.Path), "sibling.mov")
	rc.applyEvent(MetadataEvent{Op: "create", Path: sibling, Inode: 9006, Size: 2})
	if got := f.store.LookupByPath(sibling); got == nil || got.Size != 2 || invalidations != 1 {
		t.Fatalf("ordinary same-directory sibling blocked: %+v invalidations=%d", got, invalidations)
	}
}
