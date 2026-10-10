package nfs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// An ordinary worker can refill the shared media channel repeatedly. The
// replacement waits as one sender and proceeds once finite competing work
// yields capacity; it cannot invent a private upload slot.
func TestReplacementDispatchCapacityWaitsBehindOrdinaryRefill(t *testing.T) {
	t.Setenv("JM_DRAIN_CLASS_GATE", "0")
	synctest.Test(t, func(t *testing.T) {
		d := &Drainer{sem: make(chan struct{}, 1), slowGate: make(chan struct{}, 1)}
		d.sem <- struct{}{} // ordinary worker owns the only media slot
		result := make(chan error, 1)
		var resumed int
		var refills atomic.Int32
		go func() {
			for i := 0; i < 128; i++ {
				d.sem <- struct{}{} // ordinary-style competing refill
				refills.Add(1)
				<-d.sem
			}
		}()
		synctest.Wait() // ordinary refill is first in the channel wait queue
		go func() {
			result <- d.withReplacementCapacity(context.Background(), func() error {
				resumed++
				if len(d.sem) != 1 || refills.Load() != 1 {
					return errors.New("replacement lost its turn after first ordinary refill")
				}
				return nil
			})
		}()
		synctest.Wait() // replacement is parked behind that ordinary refill
		select {
		case err := <-result:
			t.Fatalf("replacement bypassed occupied slot: %v", err)
		default:
		}
		<-d.sem
		synctest.Wait()
		if err := <-result; err != nil || resumed != 1 || refills.Load() != 128 || len(d.sem) != 0 {
			t.Fatalf("capacity progress err=%v resumed=%d refills=%d slots=%d", err, resumed, refills.Load(), len(d.sem))
		}
	})
}

func TestReplacementDispatchCapacitySlowGateAndCancellationReleaseSlots(t *testing.T) {
	t.Setenv("JM_DRAIN_CLASS_GATE", "1")
	class := netprofile.ClassSlow
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	if !drainLinkIsSlow() {
		t.Fatal("slow class not selected")
	}
	for _, blocked := range []string{"media", "slow"} {
		t.Run(blocked, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := &Drainer{sem: make(chan struct{}, 1), slowGate: make(chan struct{}, 1)}
				if blocked == "media" {
					d.sem <- struct{}{}
				} else {
					d.slowGate <- struct{}{}
				}
				ctx, cancel := context.WithCancel(context.Background())
				result := make(chan error, 1)
				called := false
				go func() {
					result <- d.withReplacementCapacity(ctx, func() error { called = true; return nil })
				}()
				synctest.Wait()
				if called || (blocked == "slow" && len(d.sem) != 1) {
					t.Fatal("capacity wait did not park at requested gate")
				}
				cancel()
				synctest.Wait()
				if err := <-result; !errors.Is(err, context.Canceled) || called {
					t.Fatalf("cancelled wait err=%v callback=%t", err, called)
				}
				if blocked == "media" {
					if len(d.sem) != 1 || len(d.slowGate) != 0 {
						t.Fatal("cancelled media wait changed held slots")
					}
					<-d.sem
				} else {
					if len(d.sem) != 0 || len(d.slowGate) != 1 {
						t.Fatal("cancelled slow wait retained media slot")
					}
					<-d.slowGate
				}
				if err := d.withReplacementCapacity(context.Background(), func() error { called = true; return nil }); err != nil || !called || len(d.sem) != 0 || len(d.slowGate) != 0 {
					t.Fatalf("slots not reusable after cancellation: %v media=%d slow=%d", err, len(d.sem), len(d.slowGate))
				}
			})
		})
	}
}

func TestReplacementDispatchCapacityHonorsSlowEnvelopeAfterMediaAdmission(t *testing.T) {
	t.Setenv("JM_DRAIN_CLASS_GATE", "1")
	class := netprofile.ClassSlow
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	if !drainLinkIsSlow() {
		t.Fatal("slow class not selected")
	}
	synctest.Test(t, func(t *testing.T) {
		d := &Drainer{sem: make(chan struct{}, 2), slowGate: make(chan struct{}, 1)}
		d.sem <- struct{}{}
		d.slowGate <- struct{}{}
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- d.withReplacementCapacity(context.Background(), func() error {
				if len(d.sem) > 2 || len(d.slowGate) != 1 {
					return errors.New("shared envelope exceeded")
				}
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
		synctest.Wait() // replacement owns second media slot, waits for slow
		if len(d.sem) != 2 || len(d.slowGate) != 1 {
			t.Fatalf("parked capacity media=%d slow=%d", len(d.sem), len(d.slowGate))
		}
		select {
		case <-entered:
			t.Fatal("replacement bypassed occupied slow gate")
		default:
		}
		<-d.slowGate
		synctest.Wait()
		<-entered
		close(release)
		synctest.Wait()
		if err := <-result; err != nil || len(d.sem) != 1 || len(d.slowGate) != 0 {
			t.Fatalf("envelope release err=%v media=%d slow=%d", err, len(d.sem), len(d.slowGate))
		}
		<-d.sem // original ordinary owner
	})
}

func TestReplacementDispatchCapacitySlowRefillProgress(t *testing.T) {
	t.Setenv("JM_DRAIN_CLASS_GATE", "1")
	class := netprofile.ClassSlow
	netprofile.Default().ForceClass(&class)
	t.Cleanup(func() { netprofile.Default().ForceClass(nil) })
	if !drainLinkIsSlow() {
		t.Fatal("slow class not selected")
	}
	synctest.Test(t, func(t *testing.T) {
		d := &Drainer{sem: make(chan struct{}, 2), slowGate: make(chan struct{}, 1)}
		d.sem <- struct{}{}      // ordinary worker occupies one media slot
		d.slowGate <- struct{}{} // ordinary worker owns slow-link gate
		result := make(chan error, 1)
		var resumed int
		var refills atomic.Int32
		go func() {
			for i := 0; i < 128; i++ {
				d.slowGate <- struct{}{}
				refills.Add(1)
				<-d.slowGate
			}
		}()
		synctest.Wait() // ordinary slow refill queues first
		go func() {
			result <- d.withReplacementCapacity(context.Background(), func() error {
				resumed++
				if len(d.sem) != 2 || len(d.slowGate) != 1 || refills.Load() != 1 {
					return errors.New("replacement lost its turn after first ordinary slow refill")
				}
				return nil
			})
		}()
		synctest.Wait()
		if len(d.sem) != 2 {
			t.Fatalf("replacement did not retain media slot while waiting: %d", len(d.sem))
		}
		<-d.slowGate
		synctest.Wait()
		if err := <-result; err != nil || resumed != 1 || refills.Load() != 128 || len(d.sem) != 1 || len(d.slowGate) != 0 {
			t.Fatalf("slow refill progress err=%v resumed=%d refills=%d media=%d slow=%d", err, resumed, refills.Load(), len(d.sem), len(d.slowGate))
		}
		<-d.sem
	})
}
