//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// No mount is involved: Check and the retained backend close are separate
// deterministic barriers in the real coordinator's ownership lifetime.
type replacementShutdownIO struct {
	replacementPublishIO
	entered chan struct{}
	release chan struct{}
	failure error
}

func (b *replacementShutdownIO) Check() error {
	close(b.entered)
	<-b.release
	return b.failure
}

func TestReplacementPublishShutdownLifetime(t *testing.T) {
	for _, mode := range []string{"unstarted", "started", "started-late"} {
		t.Run(mode, func(t *testing.T) {
			x, spool, d, row, witness, body := replacementNestedFixture(t)
			usedBefore, _ := spool.Capacity()
			mirrorBefore := x.store.LookupByPath(row.NFSPath)
			fault := errors.New("replacement-shutdown-controlled-exit")
			backend := &replacementShutdownIO{entered: make(chan struct{}), release: make(chan struct{}), failure: fault}
			closeEntered, closeRelease := make(chan struct{}), make(chan struct{})
			done := make(chan struct{})
			var workRelease, parentRelease sync.Once
			var calls atomic.Int32
			var publishErr error
			t.Cleanup(func() {
				workRelease.Do(func() { close(backend.release) })
				parentRelease.Do(func() { close(closeRelease) })
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("replacement worker did not join")
				}
				if !d.Stop(5 * time.Second) {
					t.Error("replacement drainer did not join")
				}
			})
			factory := func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
				if calls.Add(1) != 1 {
					return nil, nil, errors.New("unexpected replacement factory admission")
				}
				return backend, func() { close(closeEntered); <-closeRelease }, nil
			}
			binding := func(got metadata.CachePathBinding) error {
				if got != witness.Binding {
					return metadata.ErrMirrorConflict
				}
				return nil
			}
			if mode == "started" {
				d.Start()
			}
			go func() { publishErr = d.publishReplacement(context.Background(), row.ID, factory, binding); close(done) }()
			select {
			case <-backend.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("replacement did not enter owned backend")
			}
			if mode == "started-late" {
				d.Start()
			}
			// Duplicate exclusion must remain intact while the first worker owns it.
			if err := d.publishReplacement(context.Background(), row.ID, factory, binding); !errors.Is(err, metadata.ErrMirrorConflict) || calls.Load() != 1 {
				t.Fatalf("duplicate publication admitted: err=%v calls=%d", err, calls.Load())
			}
			journal, err := spool.Meta().ReplacementPublication(row)
			if err != nil || journal == nil || journal.Phase != metadata.ReplacementPublicationIntent {
				t.Fatalf("intent journal=%+v err=%v", journal, err)
			}
			started := time.Now()
			if d.Stop(20 * time.Millisecond) {
				t.Error("Stop claimed completion while replacement backend was held")
			}
			if time.Since(started) > 500*time.Millisecond {
				t.Error("Stop exceeded its bounded deadline")
			}
			if mode != "unstarted" {
				select {
				case <-d.done:
				case <-time.After(5 * time.Second):
					t.Fatal("dispatcher did not join independently")
				}
			}
			if d.Stop(20 * time.Millisecond) {
				t.Error("repeated Stop claimed completion while replacement backend was held")
			}
			workRelease.Do(func() { close(backend.release) })
			select {
			case <-closeEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("replacement did not enter retained-parent close")
			}
			if d.Stop(20 * time.Millisecond) {
				t.Error("Stop claimed completion while replacement parent close was held")
			}
			parentRelease.Do(func() { close(closeRelease) })
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("replacement original worker did not complete")
			}
			if !errors.Is(publishErr, fault) {
				t.Fatalf("unexpected coordinator outcome: %v", publishErr)
			}
			if !d.Stop(5*time.Second) || !d.Stop(0) {
				t.Fatal("Stop did not retain completed shutdown")
			}
			// Start cannot reopen admission after Stop, including on an unstarted drainer.
			d.Start()
			if err := d.publishReplacement(context.Background(), row.ID, factory, binding); !errors.Is(err, metadata.ErrMirrorConflict) || calls.Load() != 1 {
				t.Errorf("post-stop publication admitted: err=%v calls=%d", err, calls.Load())
			}
			gotRow, err := spool.Meta().Get(row.ID)
			if err != nil || !reflect.DeepEqual(gotRow, row) {
				t.Fatalf("shutdown changed accepted row: %+v %v", gotRow, err)
			}
			gotJournal, err := spool.Meta().ReplacementPublication(gotRow)
			if err != nil || gotJournal == nil || *gotJournal != *journal {
				t.Fatalf("shutdown changed durable intent: %+v %v", gotJournal, err)
			}
			if !reflect.DeepEqual(x.store.LookupByPath(row.NFSPath), mirrorBefore) {
				t.Fatal("shutdown changed local mirror")
			}
			if usedAfter, _ := spool.Capacity(); usedAfter != usedBefore {
				t.Fatalf("shutdown released capacity: %d -> %d", usedBefore, usedAfter)
			}
			if private, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(private, body) {
				t.Fatalf("private save changed: %v", err)
			}
			pin.SetOffline(true)
			local, err := x.jfs.OpenFile(row.NFSPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(body))
			n, readErr := local.ReadAt(got, 0)
			closeErr := local.Close()
			if n != len(body) || readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil || !bytes.Equal(got, body) {
				t.Fatalf("offline read after shutdown n=%d read=%v close=%v", n, readErr, closeErr)
			}
		})
	}
}
