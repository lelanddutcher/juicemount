//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

func startReplacementQueue(t *testing.T, f *replacementResumeFixture, factory func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error)) <-chan error {
	t.Helper()
	done := make(chan error, 8)
	h, d := f.x.jfs.handler, f.d
	d.replacementDispatch.enabled = true
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		err := h.resumeQueuedReplacement(ctx, d, id, factory)
		done <- err
		return err
	}
	t.Cleanup(func() {
		if !d.Stop(5 * time.Second) {
			t.Error("queue did not join")
		}
	})
	d.Start()
	return done
}
func awaitReplacementQueue(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("replacement queue did not finish")
		return nil
	}
}

func awaitReplacementQueueCleaned(t *testing.T, f *replacementResumeFixture, done <-chan error) {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			phase := cleanupPhase(t, f.spool, f.row.ID)
			if err == nil {
				if phase != metadata.ReplacementPublicationCleaned {
					t.Fatalf("successful queue attempt stopped at %s", phase)
				}
				return
			}
			// The local loop can observe settlement before the remote loop
			// releases its row claim. Only that duplicate claim may wait for
			// the next wake; every other error is a real failed recovery.
			if !errors.Is(err, metadata.ErrMirrorConflict) ||
				(phase != metadata.ReplacementPublicationSettled && phase != metadata.ReplacementPublicationCleaning && phase != metadata.ReplacementPublicationCleaned) {
				t.Fatalf("queue attempt at %s: %v", phase, err)
			}
		case <-timer.C:
			t.Fatalf("queue did not clean accepted row; phase=%s", cleanupPhase(t, f.spool, f.row.ID))
		}
	}
}

func TestReplacementDispatchSDKFreshRecovery(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "content"
		if empty {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			body := bytes.Repeat([]byte("queue-recovery-"), 240003)
			if empty {
				body = nil
			}
			f, rt := newReplacementFreshFixture(t, body)
			used, _ := f.spool.Capacity()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			at := "after-first-chunk"
			if empty {
				at = "after-create"
			}
			tracked := &replacementTrackedIO{replacementPublishIO: f.backend}
			fault := &replacementInterruptedUploadIO{replacementPublishIO: tracked, at: at, cancel: cancel}
			if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return fault, func() {}, nil
			}, f.binding); !errors.Is(err, context.Canceled) {
				t.Fatalf("interruption: %v", err)
			}
			tracked.assertClosed(t)
			replaceReplacementRuntime(t, f, rt)
			done := startReplacementQueue(t, f, f.factory)
			awaitReplacementQueueCleaned(t, f, done)
			if !f.d.Stop(5 * time.Second) {
				t.Fatal("queue did not stop")
			}
			for {
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, metadata.ErrMirrorConflict) {
						t.Fatalf("joined queue attempt failed after cleanup: %v", err)
					}
				default:
					goto joined
				}
			}
		joined:
			if cleanupPhase(t, f.spool, f.row.ID) != metadata.ReplacementPublicationCleaned || f.backend.renames != 1 {
				t.Fatal("queue did not finish exactly once")
			}
			assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, used-f.row.Size, false)
		})
	}
}

func TestReplacementDispatchDisabledOrUnconfigured(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "default-off"
		if missing {
			name = "missing-callback"
		}
		t.Run(name, func(t *testing.T) {
			f, _ := newReplacementFreshFixture(t, []byte("preserved"))
			var calls atomic.Int32
			f.d.replacementDispatch.resume = func(context.Context, int64) error { calls.Add(1); return nil }
			if missing {
				f.d.replacementDispatch.enabled = true
				f.d.replacementDispatch.resume = nil
			}
			f.d.Start()
			if !f.d.Stop(5 * time.Second) {
				t.Fatal("stop")
			}
			q := f.d.replacementDispatch
			if calls.Load() != 0 {
				t.Fatal("disabled queue called recovery")
			}
			if missing && (q.done != nil || q.localDone != nil) {
				t.Fatal("queue started without callback")
			}
			if !missing {
				if q.done == nil || q.localDone == nil {
					t.Fatal("disabled local cleanup worker missing")
				}
				select {
				case <-q.done:
				default:
					t.Fatal("disabled remote worker did not close synchronously")
				}
				select {
				case <-q.localDone:
				default:
					t.Fatal("disabled local worker did not join")
				}
			}
			if cleanupPhase(t, f.spool, f.row.ID) != metadata.ReplacementPublicationQueued {
				t.Fatal("row changed")
			}
		})
	}
}

func TestReplacementDispatchDisabledStillCleansSettledLocally(t *testing.T) {
	f, rt := newReplacementFreshFixture(t, []byte("settled bytes survive disabled remote recovery"))
	replaceReplacementRuntime(t, f, rt)
	injected := errors.New("stop after settlement")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == metadata.ReplacementPublicationSettled {
			return injected
		}
		return nil
	}
	if err := f.run(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("prepare settled: %v", err)
	}
	f.spool.replacementResumePhase = nil
	d := f.d
	q := d.replacementDispatch
	q.enabled = false
	done := make(chan error, 1)
	q.resume = func(ctx context.Context, id int64) error {
		err := f.x.jfs.handler.resumeQueuedReplacement(ctx, d, id, f.factory)
		done <- err
		return err
	}
	before, _ := f.spool.Capacity()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })
	d.Start()
	if err := awaitReplacementQueue(t, done); err != nil {
		t.Fatalf("disabled local cleanup: %v", err)
	}
	if !d.Stop(5 * time.Second) {
		t.Fatal("disabled worker did not join")
	}
	if phase := cleanupPhase(t, f.spool, f.row.ID); phase != metadata.ReplacementPublicationCleaned {
		t.Fatalf("phase=%s", phase)
	}
	after, _ := f.spool.Capacity()
	if after != before-f.row.Size {
		t.Fatalf("capacity=%d expected=%d", after, before-f.row.Size)
	}
	select {
	case <-q.done:
	default:
		t.Fatal("disabled remote channel did not close")
	}
	select {
	case <-q.localDone:
	default:
		t.Fatal("local worker did not join")
	}
}

func TestReplacementDispatchMissingLeaseAndBinding(t *testing.T) {
	for _, which := range []string{"lease", "binding", "binding-during-factory", "old-attachment"} {
		t.Run(which, func(t *testing.T) {
			f, _ := newReplacementFreshFixture(t, []byte("retained private edit"))
			h, d := f.x.jfs.handler, f.d
			var called, closed int
			factory := func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				called++
				if which == "binding-during-factory" {
					h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
				}
				return f.backend, func() { closed++ }, nil
			}
			switch which {
			case "lease":
				d.SetProjectLeases(nil)
			case "binding":
				h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
			case "old-attachment":
				h.drainer.Store(&Drainer{spool: f.spool})
				defer h.drainer.Store(d)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.resumeQueuedReplacement(ctx, d, f.row.ID, factory); err == nil {
				t.Fatal("unqualified publication accepted")
			}
			if which == "binding-during-factory" {
				if called != 1 || closed != 1 {
					t.Fatal("opened adapter not retired")
				}
			} else if called != 0 {
				t.Fatal("opened unqualified backend")
			}
			if f.backend.renames != 0 {
				t.Fatal("refusal published")
			}
			if which == "old-attachment" {
				if err := h.resumeReplacementAttached(ctx, d, f.row.ID, factory, f.binding); err == nil {
					t.Fatal("coordinator adopted a new attachment")
				}
			}
		})
	}
}

func TestReplacementDispatchOfflineCleanupBypassesRemoteCapacity(t *testing.T) {
	for _, gate := range []string{"media", "slow", "offline"} {
		t.Run(gate, func(t *testing.T) {
			f, rt := newReplacementFreshFixture(t, []byte("cleanup despite unavailable upload capacity"))
			// Settlement requires persisted resident chunks, unlike the upload-only first generation.
			replaceReplacementRuntime(t, f, rt)
			d := f.d
			class := netprofile.ClassSlow
			netprofile.Default().ForceClass(&class)
			t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
			if !drainLinkIsSlow() {
				t.Fatal("slow-link fixture not applied")
			}
			t.Setenv("JM_DRAIN_CLASS_GATE", "1")
			if gate == "media" {
				for i := 0; i < cap(d.sem); i++ {
					d.sem <- struct{}{}
				}
				defer func() {
					for len(d.sem) > 0 {
						<-d.sem
					}
				}()
			}
			if gate == "slow" {
				d.slowGate <- struct{}{}
				defer func() { <-d.slowGate }()
			}
			if gate == "offline" {
				pin.SetOffline(true)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// The direct coordinator prepares a settled row. The actual Start
			// loop must clean it without either remote slot or network access.
			pin.SetOffline(false)
			injected := errors.New("stop after settlement")
			f.spool.replacementResumePhase = func(phase string) error {
				if phase == metadata.ReplacementPublicationSettled {
					return injected
				}
				return nil
			}
			if err := f.run(ctx); !errors.Is(err, injected) {
				t.Fatalf("settle: %v", err)
			}
			f.spool.replacementResumePhase = nil
			pin.SetOffline(true)
			if _, err := f.spool.meta.Insert(f.row.NFSPath, f.spool.root+"/newer-unused"); err != nil {
				t.Fatal(err)
			}
			done := startReplacementQueue(t, f, f.factory)
			if err := awaitReplacementQueue(t, done); err != nil {
				t.Fatalf("offline cleanup: %v", err)
			}
			if !d.Stop(5*time.Second) || cleanupPhase(t, f.spool, f.row.ID) != metadata.ReplacementPublicationCleaned {
				t.Fatal("cleanup stranded")
			}
		})
	}
}

func TestReplacementDispatchLocalStartCleansWhileRemoteCapacityWaits(t *testing.T) {
	f, rt := newReplacementFreshFixture(t, []byte("private cleanup may outrun queued media"))
	replaceReplacementRuntime(t, f, rt) // settlement needs persisted resident blocks
	d := f.d
	for i := 0; i < cap(d.sem); i++ {
		d.sem <- struct{}{}
	}
	defer func() {
		for len(d.sem) > 0 {
			<-d.sem
		}
	}()
	var resumed atomic.Int32
	done := make(chan error, 8)
	d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
		resumed.Add(1)
		err := f.x.jfs.handler.resumeQueuedReplacement(ctx, d, id, f.factory)
		done <- err
		return err
	}
	capacityEntry := make(chan int64, 1)
	d.replacementDispatch.beforeCapacity = func(id int64) { capacityEntry <- id }
	remoteCtx, cancelRemote := context.WithCancel(context.Background())
	remoteDone, remoteExited := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(remoteExited)
		remoteDone <- d.dispatchReplacement(remoteCtx, f.row.ID, false)
	}()
	defer func() {
		cancelRemote()
		select {
		case <-remoteExited:
		case <-time.After(5 * time.Second):
			t.Error("direct remote capacity waiter did not join")
		}
	}()
	select {
	case id := <-capacityEntry:
		if id != f.row.ID {
			t.Fatalf("wrong capacity candidate: %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote never reached capacity after canonical check")
	}
	select {
	case err := <-remoteDone:
		t.Fatalf("remote completed with media capacity held: %v", err)
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	injected := errors.New("stop after settlement")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == metadata.ReplacementPublicationSettled {
			return injected
		}
		return nil
	}
	if err := f.run(ctx); !errors.Is(err, injected) {
		t.Fatalf("prepare settled row: %v", err)
	}
	f.spool.replacementResumePhase = nil
	before, _ := f.spool.Capacity()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(false) })
	if _, err := f.spool.meta.Insert(f.row.NFSPath, f.spool.root+"/newer-unused"); err != nil {
		t.Fatal(err)
	}
	d.replacementDispatch.enabled = true
	t.Cleanup(func() {
		if !d.Stop(5 * time.Second) {
			t.Error("local queue did not join")
		}
	})
	d.Start()
	if err := awaitReplacementQueue(t, done); err != nil {
		t.Fatalf("local cleanup behind remote waiter: %v", err)
	}
	if phase := cleanupPhase(t, f.spool, f.row.ID); phase != metadata.ReplacementPublicationCleaned {
		t.Fatalf("local cleanup stopped at %s", phase)
	}
	after, _ := f.spool.Capacity()
	if after != before-f.row.Size || len(d.sem) != cap(d.sem) || resumed.Load() != 1 {
		t.Fatalf("cleanup/refund while remote waits: before=%d after=%d slots=%d resumes=%d", before, after, len(d.sem), resumed.Load())
	}
	if err := d.dispatchReplacement(ctx, f.row.ID, true); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("cleaned history redispatched: %v", err)
	}
	if used, _ := f.spool.Capacity(); used != after {
		t.Fatal("duplicate cleanup refunded capacity twice")
	}
	cancelRemote()
	if err := <-remoteDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("remote capacity waiter exit: %v", err)
	}
	if !d.Stop(5 * time.Second) {
		t.Fatal("two-loop Stop did not join")
	}
}

func TestReplacementDispatchSettledRoleHandoffAfterCapacityGate(t *testing.T) {
	f, rt := newReplacementFreshFixture(t, []byte("settled work belongs to local queue"))
	replaceReplacementRuntime(t, f, rt)
	d := f.d
	for i := 0; i < cap(d.sem); i++ {
		d.sem <- struct{}{}
	}
	defer func() {
		for len(d.sem) > 0 {
			<-d.sem
		}
	}()
	var queuedResumes atomic.Int32
	d.replacementDispatch.resume = func(context.Context, int64) error {
		queuedResumes.Add(1)
		return nil
	}
	capacityEntry := make(chan int64, 1)
	d.replacementDispatch.beforeCapacity = func(id int64) { capacityEntry <- id }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, exited := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(exited)
		result <- d.dispatchReplacement(ctx, f.row.ID, false)
	}()
	defer func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("role-handoff capacity waiter did not join")
		}
	}()
	select {
	case id := <-capacityEntry:
		if id != f.row.ID {
			t.Fatalf("wrong capacity candidate: %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote never reached capacity after canonical check")
	}
	injected := errors.New("stop after settlement")
	f.spool.replacementResumePhase = func(phase string) error {
		if phase == metadata.ReplacementPublicationSettled {
			return injected
		}
		return nil
	}
	if err := f.run(ctx); !errors.Is(err, injected) {
		t.Fatalf("prepare settled row: %v", err)
	}
	f.spool.replacementResumePhase = nil
	<-d.sem
	select {
	case err := <-result:
		if err != nil || queuedResumes.Load() != 0 {
			t.Fatalf("remote failed safe handoff: %v resumes=%d", err, queuedResumes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remote did not hand settled row to local queue")
	}
	if phase := cleanupPhase(t, f.spool, f.row.ID); phase != metadata.ReplacementPublicationSettled {
		t.Fatalf("remote changed settled row: %s", phase)
	}
}

func TestReplacementDispatchChangedAuthorityAfterCapacityGate(t *testing.T) {
	for _, change := range []string{"hold", "canonical", "binding", "attachment"} {
		t.Run(change, func(t *testing.T) {
			f, _ := newReplacementFreshFixture(t, []byte("post-gate refusal keeps remote original"))
			d, h := f.d, f.x.jfs.handler
			for i := 0; i < cap(d.sem); i++ {
				d.sem <- struct{}{}
			}
			defer func() {
				for len(d.sem) > 0 {
					<-d.sem
				}
			}()
			var factoryCalls atomic.Int32
			d.replacementDispatch.resume = func(ctx context.Context, id int64) error {
				return h.resumeQueuedReplacement(ctx, d, id, func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
					factoryCalls.Add(1)
					return f.factory(w)
				})
			}
			capacityEntry := make(chan int64, 1)
			d.replacementDispatch.beforeCapacity = func(id int64) { capacityEntry <- id }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result, exited := make(chan error, 1), make(chan struct{})
			go func() {
				defer close(exited)
				result <- d.dispatchReplacement(ctx, f.row.ID, false)
			}()
			defer func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("capacity waiter did not join")
				}
			}()
			select {
			case id := <-capacityEntry:
				if id != f.row.ID {
					t.Fatalf("wrong capacity candidate: %d", id)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("remote never reached capacity after canonical check")
			}
			switch change {
			case "hold":
				if _, err := h.store.DB().Exec(`UPDATE spool_entries SET wait_project='media/project' WHERE id=?`, f.row.ID); err != nil {
					t.Fatal(err)
				}
			case "canonical":
				if _, err := h.store.DB().Exec(`UPDATE spool_entries SET replacement_witness=X'00' WHERE id=?`, f.row.ID); err != nil {
					t.Fatal(err)
				}
			case "binding":
				h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
			case "attachment":
				h.drainer.Store(&Drainer{spool: f.spool})
				t.Cleanup(func() { h.drainer.Store(d) })
			}
			<-d.sem // release one ordinary slot for the queued attempt
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("changed authority admitted after capacity wait")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("changed-authority waiter did not finish")
			}
			if factoryCalls.Load() != 0 || f.backend.renames != 0 {
				t.Fatal("changed authority reached remote stage")
			}
		})
	}
}

func TestReplacementDispatchWakeCooldownAndStop(t *testing.T) {
	f, _ := newReplacementFreshFixture(t, []byte("pending on every wake"))
	var calls atomic.Int32
	attempted := make(chan struct{}, 8)
	d := f.d
	d.replacementDispatch.enabled = true
	d.replacementDispatch.resume = func(context.Context, int64) error {
		calls.Add(1)
		attempted <- struct{}{}
		return metadata.ErrMirrorConflict
	}
	t.Cleanup(func() {
		if !d.Stop(5 * time.Second) {
			t.Error("queue join")
		}
	})
	d.Start()
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("initial wake missing")
	}
	for i := 0; i < 1000; i++ {
		d.wakeNonBlocking()
	}
	select {
	case <-attempted:
		t.Fatal("wake bypassed cooldown")
	case <-time.After(100 * time.Millisecond):
	}
	if !d.Stop(5*time.Second) || !d.Stop(0) {
		t.Fatal("repeated Stop did not join")
	}
	d.Start()
	if calls.Load() != 1 {
		t.Fatal("stopped queue restarted")
	}
}

func TestReplacementDispatchStopOwnsBlockedBackend(t *testing.T) {
	f, _ := newReplacementFreshFixture(t, []byte("private bytes must survive bounded Stop"))
	held := &replacementShutdownIO{entered: make(chan struct{}), release: make(chan struct{}), failure: errors.New("held backend released")}
	var once sync.Once
	done := startReplacementQueue(t, f, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		return held, func() {}, nil
	})
	t.Cleanup(func() { once.Do(func() { close(held.release) }) })
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("backend was not entered")
	}
	if f.d.Stop(20 * time.Millisecond) {
		t.Fatal("Stop claimed a held backend was gone")
	}
	once.Do(func() { close(held.release) })
	if err := awaitReplacementQueue(t, done); err == nil {
		t.Fatal("interrupted operation succeeded")
	}
	if !f.d.Stop(5*time.Second) || !f.d.Stop(0) {
		t.Fatal("original queue did not join")
	}
	if cleanupPhase(t, f.spool, f.row.ID) != metadata.ReplacementPublicationIntent {
		t.Fatal("Stop lost durable intent")
	}
}

func TestReplacementDispatchStopJoinsBothLoopsAtCapacityGates(t *testing.T) {
	for _, gate := range []string{"media", "slow"} {
		t.Run(gate, func(t *testing.T) {
			f, _ := newReplacementFreshFixture(t, []byte("stop owns both queue loops"))
			d, q := f.d, f.d.replacementDispatch
			class := netprofile.ClassSlow
			netprofile.Default().ForceClass(&class)
			t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
			t.Setenv("JM_DRAIN_CLASS_GATE", "1")
			q.enabled = true
			var resumed atomic.Int32
			q.resume = func(context.Context, int64) error { resumed.Add(1); return nil }
			capacityEntry := make(chan int64, 1)
			q.beforeCapacity = func(id int64) { capacityEntry <- id }
			if gate == "media" {
				for i := 0; i < cap(d.sem); i++ {
					d.sem <- struct{}{}
				}
			} else {
				d.slowGate <- struct{}{}
			}
			t.Cleanup(func() {
				if !d.Stop(5 * time.Second) {
					t.Error("capacity queue did not join")
				}
				for len(d.sem) > 0 {
					<-d.sem
				}
				for len(d.slowGate) > 0 {
					<-d.slowGate
				}
			})
			d.Start()
			select {
			case id := <-capacityEntry:
				if id != f.row.ID {
					t.Fatalf("wrong capacity candidate: %d", id)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("queue did not reach remote capacity gate")
			}
			if gate == "slow" {
				deadline := time.After(5 * time.Second)
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for len(d.sem) == 0 {
					select {
					case <-tick.C:
					case <-deadline:
						t.Fatal("slow waiter never acquired media slot")
					}
				}
			}
			if !d.Stop(5 * time.Second) {
				t.Fatal("Stop did not cancel both queue loops")
			}
			select {
			case <-q.done:
			default:
				t.Fatal("remote loop not joined")
			}
			select {
			case <-q.localDone:
			default:
				t.Fatal("local loop not joined")
			}
			if resumed.Load() != 0 {
				t.Fatal("cancelled capacity wait resumed queued row")
			}
			if gate == "media" && (len(d.sem) != cap(d.sem) || len(d.slowGate) != 0) ||
				gate == "slow" && (len(d.sem) != 0 || len(d.slowGate) != 1) {
				t.Fatalf("Stop changed external gate tokens: media=%d slow=%d", len(d.sem), len(d.slowGate))
			}
		})
	}
}

func TestReplacementDispatchStopBeforeStart(t *testing.T) {
	f, _ := newReplacementFreshFixture(t, []byte("not started"))
	f.d.replacementDispatch.enabled = true
	if !f.d.Stop(time.Second) {
		t.Fatal("unstarted Stop")
	}
	f.d.Start()
	if f.d.replacementDispatch.done != nil || f.d.replacementDispatch.localDone != nil {
		t.Fatal("queue started after Stop")
	}
}

func TestReplacementDispatchPeerAndRevokedOwner(t *testing.T) {
	for _, which := range []string{"peer-held", "revoked-session"} {
		t.Run(which, func(t *testing.T) {
			body := bytes.Repeat([]byte("queue-held-private-"), 170003)
			f, rt := newReplacementFreshFixture(t, body)
			used, _ := f.spool.Capacity()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tracked := &replacementTrackedIO{replacementPublishIO: f.backend}
			fault := &replacementInterruptedUploadIO{replacementPublishIO: tracked, at: "after-first-chunk", cancel: cancel}
			if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				return fault, func() {}, nil
			}, f.binding); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			tracked.assertClosed(t)
			prior, err := f.x.rdb.Dump(context.Background(), "d4").Result()
			if err != nil {
				t.Fatal(err)
			}
			journal, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil {
				t.Fatal(err)
			}
			if which == "revoked-session" {
				revokeReplacementRuntimeOwner(t, rt)
			} else {
				if status, err := f.d.ProjectLeases().releaseNow(context.Background(), "media/project"); err != nil || status != "released" {
					t.Fatalf("release: %s %v", status, err)
				}
				u, err := url.Parse(rt.url)
				if err != nil {
					t.Fatal(err)
				}
				u.User = url.UserPassword("mac-peer", "mac-secret")
				peer, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: u.String()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(peer.Stop)
				if grant, err := peer.EnsureForDrain("media/project"); err != nil || !grant.held {
					t.Fatalf("peer: %+v %v", grant, err)
				}
			}
			done := startReplacementQueue(t, f, f.factory)
			if err := awaitReplacementQueue(t, done); err == nil {
				t.Fatal("unauthorized queue published")
			}
			if !f.d.Stop(5 * time.Second) {
				t.Fatal("queue join")
			}
			after, err := f.x.rdb.Dump(context.Background(), "d4").Result()
			if err != nil || after != prior {
				t.Fatal("remote names changed")
			}
			current, err := f.spool.meta.ReplacementPublication(f.row)
			if err != nil || current == nil || *current != *journal || f.backend.renames != 0 {
				t.Fatal("refusal changed publication")
			}
			assertReplacementQueuedBytesOffline(t, f, body, used)
		})
	}
}

// Queued edits read through spoolReadFile, whose final positive read need not
// carry EOF. Assert the concrete route, all bytes, and exact/past-end EOF.
func assertReplacementQueuedBytesOffline(t *testing.T, f *replacementResumeFixture, body []byte, used int64) {
	t.Helper()
	assertReplacementOfflineNFSRoute(t, f.x, f.spool, f.row, body, used, true, true)
}
