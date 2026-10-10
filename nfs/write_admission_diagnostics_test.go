package nfs

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// These controls exercise the real producers, not a metrics helper. Counts are
// process cumulative, so each assertion compares only its own branch delta.
func TestWriteAdmissionDiagnosticBranches(t *testing.T) {
	reg := metrics.Default()
	delta := func(name string, before, after uint64) {
		t.Helper()
		if after <= before {
			t.Fatalf("%s counter did not advance: %d -> %d", name, before, after)
		}
	}
	t.Run("continuation_entry_expired", func(t *testing.T) {
		var g continuationGate
		before := reg.AdmissionRefusals().ContinuationEntryExpired
		_, err := g.acquire("clip.mov", time.Now().Add(-time.Second), nil)
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("expired: %v", err)
		}
		delta("continuation_entry_expired", before, reg.AdmissionRefusals().ContinuationEntryExpired)
	})
	t.Run("continuation_stop", func(t *testing.T) {
		var g continuationGate
		g.beginStop()
		before := reg.AdmissionRefusals().ContinuationStopped
		_, err := g.acquire("clip.mov", time.Now().Add(-time.Second), nil)
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("stopped: %v", err)
		}
		delta("continuation_stop", before, reg.AdmissionRefusals().ContinuationStopped)
	})
	t.Run("continuation_queued_stopped", func(t *testing.T) {
		var g continuationGate
		holder, err := g.acquire("clip.mov", time.Now().Add(time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holder()
		queued := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := g.acquire("clip.mov", time.Now().Add(time.Second), func() { close(queued) })
			done <- err
		}()
		select {
		case <-queued:
		case <-time.After(time.Second):
			t.Fatal("waiter did not queue")
		}
		before := reg.AdmissionRefusals().ContinuationStopped
		g.beginStop()
		select {
		case err := <-done:
			if !errors.Is(err, errFUSETimeout) {
				t.Fatalf("queued stop: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("stopped waiter did not settle")
		}
		delta("continuation_queued_stopped", before, reg.AdmissionRefusals().ContinuationStopped)
	})
	t.Run("continuation_queued_expired", func(t *testing.T) {
		var g continuationGate
		holder, err := g.acquire("clip.mov", time.Now().Add(time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holder()
		queued := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := g.acquire("clip.mov", time.Now().Add(30*time.Millisecond), func() { close(queued) })
			done <- err
		}()
		select {
		case <-queued:
		case <-time.After(time.Second):
			t.Fatal("waiter did not queue")
		}
		before := reg.AdmissionRefusals().ContinuationQueuedExpired
		select {
		case err := <-done:
			if !errors.Is(err, errFUSETimeout) {
				t.Fatalf("queued expiry: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not settle")
		}
		delta("continuation_queued_expired", before, reg.AdmissionRefusals().ContinuationQueuedExpired)
	})
	t.Run("namespace_overlap_and_serving_blocked", func(t *testing.T) {
		h := &JuiceMountHandler{}
		held := h.namespace.begin([]string{"clip.mov"})
		if held == nil {
			t.Fatal("no original owner")
		}
		defer held.finish(nil)
		before := reg.AdmissionRefusals().NamespaceOverlap
		_, err := runNamespaceOperationUntil(h, []string{"clip.mov"}, time.Now().Add(time.Second), func(*namespaceOperation) (struct{}, error) { return struct{}{}, nil }, nil)
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("overlap: %v", err)
		}
		delta("namespace_overlap", before, reg.AdmissionRefusals().NamespaceOverlap)
		held.blockReads()
		before = reg.AdmissionRefusals().NamespaceServingBlocked
		if err := h.namespace.servingError("clip.mov"); !errors.Is(err, errFUSETimeout) {
			t.Fatalf("serving block: %v", err)
		}
		delta("namespace_serving_blocked", before, reg.AdmissionRefusals().NamespaceServingBlocked)
	})
	t.Run("namespace_active_cap", func(t *testing.T) {
		var c namespaceCoordinator
		owners := make([]*namespaceOperation, 0, 24)
		defer func() {
			for _, op := range owners {
				op.finish(nil)
			}
		}()
		for i := 0; i < 24; i++ {
			op := c.begin([]string{fmt.Sprintf("file%d", i)})
			if op == nil {
				t.Fatalf("owner %d", i)
			}
			owners = append(owners, op)
		}
		before := reg.AdmissionRefusals().NamespaceActiveCap
		if op := c.begin([]string{"other"}); op != nil {
			op.finish(nil)
			t.Fatal("25th admitted")
		}
		delta("namespace_active_cap", before, reg.AdmissionRefusals().NamespaceActiveCap)
	})
	t.Run("namespace_stop", func(t *testing.T) {
		c := namespaceCoordinator{stopping: true}
		before := reg.AdmissionRefusals().NamespaceStopping
		if op := c.begin([]string{"clip.mov"}); op != nil {
			op.finish(nil)
			t.Fatal("stopped namespace admitted")
		}
		delta("namespace_stop", before, reg.AdmissionRefusals().NamespaceStopping)
	})
	t.Run("pre_deadline", func(t *testing.T) {
		h := &JuiceMountHandler{}
		before := reg.AdmissionRefusals().NamespacePreDeadline
		_, err := runNamespaceOperationUntil(h, []string{"clip.mov"}, time.Now().Add(-time.Second), func(*namespaceOperation) (struct{}, error) { return struct{}{}, nil }, nil)
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("pre deadline: %v", err)
		}
		delta("pre_deadline", before, reg.AdmissionRefusals().NamespacePreDeadline)
	})
	t.Run("worker_deadline_retains_owner", func(t *testing.T) {
		h := &JuiceMountHandler{}
		entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		defer func() {
			close(release)
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Error("original worker not joined")
			}
			if !h.namespace.stop(time.Second) {
				t.Error("original namespace worker did not retire")
			}
		}()
		before := reg.AdmissionRefusals().NamespaceWorkerDeadline
		result := make(chan error, 1)
		go func() {
			_, err := runNamespaceOperationUntil(h, []string{"clip.mov"}, time.Now().Add(time.Second), func(*namespaceOperation) (struct{}, error) {
				close(entered)
				<-release
				close(finished)
				return struct{}{}, nil
			}, nil)
			result <- err
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("worker never entered")
		}
		select {
		case err := <-result:
			if !errors.Is(err, errFUSETimeout) {
				t.Fatalf("worker deadline: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("bounded runner did not settle")
		}
		if op := h.namespace.begin([]string{"clip.mov"}); op != nil {
			op.finish(nil)
			t.Fatal("timed-out worker lost authority")
		}
		delta("worker_deadline", before, reg.AdmissionRefusals().NamespaceWorkerDeadline)
	})
}
