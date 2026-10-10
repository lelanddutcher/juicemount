//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/lelanddutcher/juicemount/metadata"
)

type replacementResumeFixture struct {
	x         *boundCacheFixture
	spool     *SpoolStore
	d         *Drainer
	row       *metadata.SpoolRow
	backend   *replacementSDKIO
	proxyURL  string
	factories int
}

func newReplacementResumeFixture(t *testing.T, body []byte) *replacementResumeFixture {
	return newReplacementResumeFixtureBirth(t, body)
}

func newReplacementResumeFixtureBirth(t *testing.T, body []byte) *replacementResumeFixture {
	return newReplacementResumeFixtureVolume(t, body, "12345678-1234-abcd-5678-123456789abc", true)
}

func newReplacementResumeFixtureVolume(t *testing.T, body []byte, uuid string, birth bool) *replacementResumeFixture {
	t.Helper()
	x, spool, d, row, w, _ := replacementNestedFixtureBodyVolume(t, body, uuid)
	fs, proxyURL := replacementProxyAndFSConfigBirth(t, x, spool, d, func(c *chunk.Config) { configureReplacementResidentCacheVolume(t, c, uuid) }, birth)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	return &replacementResumeFixture{x: x, spool: spool, d: d, row: row, proxyURL: proxyURL,
		backend: &replacementSDKIO{fs: fs, m: fs.Meta(), w: *w, ctx: jfsmeta.Background(), parent: 4}}
}

func (f *replacementResumeFixture) factory(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
	f.factories++
	if w.OperationID != f.backend.w.OperationID {
		return nil, nil, metadata.ErrMirrorConflict
	}
	return f.backend, func() {}, nil
}
func (f *replacementResumeFixture) binding(b metadata.CachePathBinding) error {
	if b != f.backend.w.Binding {
		return metadata.ErrMirrorConflict
	}
	return nil
}
func (f *replacementResumeFixture) run(ctx context.Context) error {
	return f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, f.factory, f.binding)
}

// Reopen ordinary DB/spool/handler objects. Only phases needing publication or
// settlement receive a new online Session/metadata/lease client; terminal
// cleanup is deliberately exercised with none of those remote capabilities.
func (f *replacementResumeFixture) reopen(t *testing.T, online bool) {
	t.Helper()
	f.d.leasesPtr.Load().Stop()
	store, spool := reopenReplacementPublicationFixture(t, f.x, f.spool.root)
	if _, err := spool.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: f.x.fuseRoot})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, f.x.fuseRoot)
	h.SetSpool(spool, d)
	old := *f.x
	old.store, old.jfs, old.session, old.rc, old.paths = store, &juiceFS{handler: h}, nil, nil, nil
	f.x, f.spool, f.d = &old, spool, d
	if online {
		paths, err := metadata.OpenCachePathEligibility(store)
		if err != nil {
			t.Fatal(err)
		}
		rc, err := metadata.NewRedisClientDeferred(f.x.raw, store)
		if err != nil {
			t.Fatal(err)
		}
		opts := *f.x.rdb.Options()
		session, err := cache.NewSession(cache.SessionConfig{Store: store, LogicalURL: f.x.raw, Options: &opts, StateDir: f.x.state,
			VerifyReader: func(*cache.Reader) error { return nil }, OnReader: h.SetCacheReader, OnQualified: rc.SetBackendQualified, OnStatus: h.SetCacheBackendStatus})
		if err != nil {
			t.Fatal(err)
		}
		h.SetBoundCacheSession(session, rc, paths)
		h.SetCacheReader(session.Reader())
		f.x.session, f.x.rc, f.x.paths = session, rc, paths
		t.Cleanup(func() { h.StopHandler(); session.Stop(); rc.Stop() })
		session.SetOnline(true)
		pin.SetOffline(false)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := session.Qualify(ctx); err != nil {
			t.Fatal(err)
		}
		lease, err := NewProjectLeaseClient(ProjectLeaseConfig{RedisURL: f.proxyURL, Spool: spool})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(lease.Stop)
		d.SetProjectLeases(lease)
	} else {
		t.Cleanup(func() { h.StopHandler() })
	}
}

func TestReplacementResumeSDKOfflineReopen(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{{"three_blocks", bytes.Repeat([]byte("sdk-settlement-"), 700003)}, {"empty", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementResumeFixture(t, tc.body)
			before, _ := f.spool.Capacity()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := f.run(ctx); err != nil {
				t.Fatal(err)
			}
			if got := cleanupPhase(t, f.spool, f.row.ID); got != metadata.ReplacementPublicationCleaned {
				t.Fatalf("phase %s", got)
			}
			if f.factories != 1 || f.backend.renames != 1 {
				t.Fatalf("publication repeated: factory=%d rename=%d", f.factories, f.backend.renames)
			}
			assertReplacementOfflineNFS(t, f.x, f.spool, f.row, tc.body, before-f.row.Size, false)
		})
	}
}

func TestReplacementResumeEveryDurablePhaseAfterReopen(t *testing.T) {
	for _, phase := range []string{"queued", "intent", "arming", "armed", "committed", "settled", "cleaning", "cleaned"} {
		t.Run(phase, func(t *testing.T) {
			body := []byte("one save must survive each durable phase")
			f := newReplacementResumeFixture(t, body)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			injected := errors.New("controlled phase interruption")
			switch phase {
			case "queued":
			case "intent":
				if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, func(metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
					return nil, nil, injected
				}, f.binding); !errors.Is(err, injected) {
					t.Fatalf("intent interruption: %v", err)
				}
			case "arming", "armed":
				target := "armed"
				if phase == "armed" {
					target = "committed"
				}
				if _, err := f.x.store.DB().Exec(`CREATE TRIGGER resume_fault BEFORE UPDATE OF phase ON spool_replacement_publications WHEN NEW.phase='` + target + `' BEGIN SELECT RAISE(ABORT,'resume phase fault'); END`); err != nil {
					t.Fatal(err)
				}
				if err := f.run(ctx); err == nil {
					t.Fatal("SQL interruption not reached")
				}
				if _, err := f.x.store.DB().Exec(`DROP TRIGGER resume_fault`); err != nil {
					t.Fatal(err)
				}
			case "cleaning":
				f.spool.replacementCleanupFault = func(at string) error {
					if at == "after-intent" {
						return injected
					}
					return nil
				}
				if err := f.run(ctx); !errors.Is(err, injected) {
					t.Fatalf("cleaning interruption: %v", err)
				}
				f.spool.replacementCleanupFault = nil
			default:
				f.spool.replacementResumePhase = func(at string) error {
					if at == phase {
						return injected
					}
					return nil
				}
				if err := f.run(ctx); !errors.Is(err, injected) {
					t.Fatalf("phase interruption %s: %v", phase, err)
				}
				f.spool.replacementResumePhase = nil
			}
			if got := cleanupPhase(t, f.spool, f.row.ID); got != phase {
				t.Fatalf("phase %s != %s", got, phase)
			}
			initialFactories := f.factories
			online := phase == "queued" || phase == "intent" || phase == "arming" || phase == "armed" || phase == "committed"
			f.reopen(t, online)
			if phase == "committed" || !online {
				if err := f.x.jfs.handler.resumeReplacement(ctx, f.row.ID, nil, nil); err != nil {
					t.Fatal(err)
				}
			} else if err := f.run(ctx); err != nil {
				t.Fatal(err)
			}
			if got := cleanupPhase(t, f.spool, f.row.ID); got != "cleaned" {
				t.Fatalf("final phase %s", got)
			}
			if (phase == "committed" || !online) && f.factories != initialFactories {
				t.Fatal("terminal recovery reopened backend")
			}
			if f.backend.renames != 1 {
				t.Fatalf("rename count %d", f.backend.renames)
			}
			assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, 0, false)
		})
	}
}

func TestReplacementResumeCancellationRetainsDurableOutcome(t *testing.T) {
	for _, phase := range []string{"queued", "committed", "settled", "cleaned"} {
		t.Run(phase, func(t *testing.T) {
			body := []byte("cancel cannot discard an accepted save")
			f := newReplacementResumeFixture(t, body)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "queued" {
				cancel()
			} else {
				f.spool.replacementResumePhase = func(at string) error {
					if at == phase {
						cancel()
					}
					return nil
				}
			}
			if err := f.run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			f.spool.replacementResumePhase = nil
			if got := cleanupPhase(t, f.spool, f.row.ID); got != phase {
				t.Fatalf("cancellation phase %s != %s", got, phase)
			}
			if phase != "cleaned" {
				if got, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(got, body) {
					t.Fatalf("cancel discarded private save: %v", err)
				}
			}
			if err := f.run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.backend.renames != 1 {
				t.Fatal("cancel retry repeated remote replacement")
			}
			assertReplacementOfflineNFS(t, f.x, f.spool, f.row, body, 0, false)
		})
	}
}

func TestReplacementResumeStopAndDuplicateOwnWholeWorker(t *testing.T) {
	for _, phase := range []string{"committed", "settled"} {
		t.Run(phase, func(t *testing.T) {
			f := newReplacementResumeFixture(t, []byte("owned through phase boundary"))
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var once sync.Once
			var calls atomic.Int32
			f.spool.replacementResumePhase = func(at string) error {
				if at == phase {
					calls.Add(1)
					close(entered)
					<-release
				}
				return nil
			}
			t.Cleanup(func() {
				once.Do(func() { close(release) })
				if !f.d.Stop(5 * time.Second) {
					t.Error("worker not joined")
				}
			})
			go func() { done <- f.run(context.Background()) }()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("worker exited early: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("boundary not reached")
			}
			if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, nil, nil); !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatalf("duplicate admitted: %v", err)
			}
			if f.d.Stop(20 * time.Millisecond) {
				t.Fatal("Stop forgot worker between phases")
			}
			once.Do(func() { close(release) })
			select {
			case err := <-done:
				if !errors.Is(err, metadata.ErrMirrorConflict) {
					t.Fatalf("stopped worker: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("worker not joined")
			}
			if !f.d.Stop(5*time.Second) || calls.Load() != 1 {
				t.Fatal("Stop did not retain completion")
			}
			if got := cleanupPhase(t, f.spool, f.row.ID); got != phase {
				t.Fatalf("post-stop phase advanced: %s", got)
			}
			if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, nil, nil); !errors.Is(err, metadata.ErrMirrorConflict) {
				t.Fatal("stopped drainer reopened")
			}
		})
	}
}

func TestReplacementResumeCommittedPeerDoesNotGrantSettlement(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged"
		if changed {
			name = "changed-destination"
		}
		t.Run(name, func(t *testing.T) {
			f := newReplacementResumeFixture(t, []byte("private save retained on peer change"))
			stop := errors.New("published boundary")
			f.spool.replacementResumePhase = func(phase string) error {
				if phase == "committed" {
					return stop
				}
				return nil
			}
			if err := f.run(context.Background()); !errors.Is(err, stop) {
				t.Fatalf("publication: %v", err)
			}
			f.spool.replacementResumePhase = nil
			replacementPeerTenure(t, f.d.leasesPtr.Load(), f.proxyURL, projectOf(f.row.NFSPath), false)
			if changed {
				// Inject a later remote-content change under the authenticated
				// peer tenure. This is a metadata fault, not a second-Mac UI test.
				p, err := f.spool.meta.ReplacementPublication(f.row)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := replaceguard.Decode([]byte(p.Record))
				if err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("i%d", receipt.Stage.Inode)
				raw, err := f.x.rdb.Get(context.Background(), key).Bytes()
				if err != nil {
					t.Fatal(err)
				}
				var attr jfsmeta.Attr
				attr.Unmarshal(raw)
				attr.Length++
				if err := f.x.rdb.Set(context.Background(), key, attr.Marshal(), 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			leaseBefore, err := f.x.rdb.HGetAll(context.Background(), respproxy.LeaseKeyPrefix+"4").Result()
			if err != nil {
				t.Fatal(err)
			}
			if leaseBefore["holder"] != "mac-peer" {
				t.Fatalf("peer fixture missing: %v", leaseBefore)
			}
			err = f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, nil, nil)
			if changed {
				if err == nil || cleanupPhase(t, f.spool, f.row.ID) != "committed" {
					t.Fatalf("peer change settled: %v", err)
				}
				if _, err := os.Stat(f.row.SpoolFile); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			leaseAfter, readErr := f.x.rdb.HGetAll(context.Background(), respproxy.LeaseKeyPrefix+"4").Result()
			if readErr != nil || !reflect.DeepEqual(leaseBefore, leaseAfter) || f.backend.renames != 1 {
				t.Fatal("recovery changed peer lease or renamed twice")
			}
		})
	}
}

type replacementResumePreparedIO struct{ replacementPublishIO }

func (b *replacementResumePreparedIO) Rename(string, string) error {
	return errors.New("prepared operation interrupted before rename")
}

func TestReplacementResumePreparedPeerPreservesSave(t *testing.T) {
	body := []byte("a competing writer cannot discard this private save")
	f := newReplacementResumeFixture(t, body)
	factory := func(w metadata.SpoolReplacementWitness) (replacementPublishIO, func(), error) {
		b, close, err := f.factory(w)
		return &replacementResumePreparedIO{b}, close, err
	}
	if err := f.x.jfs.handler.resumeReplacement(context.Background(), f.row.ID, factory, f.binding); err == nil {
		t.Fatal("prepared interruption not reached")
	}
	if got := cleanupPhase(t, f.spool, f.row.ID); got != "armed" {
		t.Fatalf("phase %s", got)
	}
	before, err := f.spool.meta.ReplacementPublication(f.row)
	if err != nil {
		t.Fatal(err)
	}
	used, _ := f.spool.Capacity()
	original, err := f.backend.Stat("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	replacementPeerTenure(t, f.d.leasesPtr.Load(), f.proxyURL, projectOf(f.row.NFSPath), false)
	if err := f.run(context.Background()); err == nil {
		t.Fatal("peer-held prepared operation published")
	}
	row, err := f.spool.meta.Get(f.row.ID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := f.spool.meta.ReplacementPublication(row)
	if err != nil || after == nil || *after != *before {
		t.Fatalf("peer refusal changed journal: %+v %v", after, err)
	}
	current, err := f.backend.Stat("clip.mov")
	if err != nil || current != original || f.backend.renames != 0 {
		t.Fatal("peer refusal changed original")
	}
	if got, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("private save changed: %v", err)
	}
	if after, _ := f.spool.Capacity(); after != used {
		t.Fatal("peer refusal released custody")
	}
}
