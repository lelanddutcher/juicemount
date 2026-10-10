package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBackendAdmissionHeldScanLossAndRecovery(t *testing.T) {
	s := openTestStore(t)
	rc := &RedisClient{store: s, stopCh: make(chan struct{})}
	rc.SetBackendQualified(false)
	called := false
	rc.testScanBatch = func(_ context.Context, _, _ string) ([]string, error) {
		called = true
		return []string{"0", "1:1:1700000100:8192:43:1:foreign.mov"}, nil
	}
	if err := rc.SyncOnce(); !errors.Is(err, ErrBackendUnqualified) || called {
		t.Fatalf("unqualified SCAN = %v, read=%v", err, called)
	}

	rc.SetBackendQualified(true)
	read := make(chan struct{})
	release := make(chan struct{})
	rc.testAfterScanTokenCheck = func() {
		close(read)
		<-release
	}
	done := make(chan error, 1)
	go func() { done <- rc.SyncOnce() }()
	<-read
	rc.SetBackendQualified(false)
	rc.SetBackendQualified(true)
	close(release)
	if err := <-done; err != nil && !errors.Is(err, errScanPublicationInvalidated) {
		t.Fatalf("held SCAN result = %v", err)
	}
	if got := s.LookupByPath("foreign.mov"); got != nil {
		t.Fatalf("pre-loss SCAN published after recovery: %+v", got)
	}
	rc.testAfterScanTokenCheck = nil
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("new qualified SCAN: %v", err)
	}
	if got := s.LookupByPath("foreign.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("fresh qualified SCAN was not admitted: %+v", got)
	}
}

func TestBackendAdmissionHeldKeyspaceLossAndRecovery(t *testing.T) {
	s := openTestStore(t)
	rc := &RedisClient{store: s, stopCh: make(chan struct{})}
	rc.SetBackendQualified(false)
	if err := rc.reconcileDir(1); !errors.Is(err, ErrBackendUnqualified) {
		t.Fatalf("unqualified keyspace reconcile = %v", err)
	}
	rc.SetBackendQualified(true)
	var value [9]byte
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	read := make(chan struct{})
	release := make(chan struct{})
	var readOnce sync.Once
	rc.testReadDirRaw = func(uint64) (map[string]string, error) {
		readOnce.Do(func() { close(read) })
		<-release
		return map[string]string{"clip.mov": string(value[:])}, nil
	}
	rc.testReadAttrs = func([]string) ([]interface{}, error) {
		return []interface{}{string(encodeTestAttr(1700000000, 4096))}, nil
	}
	done := make(chan error, 1)
	go func() { done <- rc.reconcileDir(1) }()
	<-read
	rc.SetBackendQualified(false)
	rc.SetBackendQualified(true)
	close(release)
	if err := <-done; !errors.Is(err, errScanPublicationInvalidated) {
		t.Fatalf("held keyspace reconcile = %v, want invalidation", err)
	}
	if got := s.LookupByPath("clip.mov"); got != nil {
		t.Fatalf("pre-loss keyspace row published: %+v", got)
	}
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
	rc = &RedisClient{store: s, stopCh: make(chan struct{})}
	rc.SetBackendQualified(true)
	rc.testReadDirRaw = func(uint64) (map[string]string, error) {
		return map[string]string{"clip.mov": string(value[:])}, nil
	}
	rc.testReadAttrs = func([]string) ([]interface{}, error) {
		return []interface{}{string(encodeTestAttr(1700000000, 4096))}, nil
	}
	if err := rc.reconcileDir(1); err != nil {
		t.Fatalf("fresh keyspace reconcile: %v", err)
	}
	if got := s.LookupByPath("clip.mov"); got == nil || got.Inode != 42 {
		t.Fatalf("fresh keyspace row missing: %+v", got)
	}
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
}

func TestBackendAdmissionStaleSubscriptionAndLocalProgress(t *testing.T) {
	s := openTestStore(t)
	local := MakeEntry("local.mov", false, 12, time.Unix(1700000000, 0), 11)
	if err := s.Insert(local); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: s}
	rc.SetBackendQualified(true)
	old, _, _ := rc.backendAdmission()
	delivered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(delivered)
		<-release
		rc.applyEventFromSubscription(MetadataEvent{Op: "create", Path: "foreign.mov", Inode: 42}, old)
	}()
	<-delivered
	rc.SetBackendQualified(false)
	rc.applyEvent(MetadataEvent{Op: "create", Path: "blocked.mov", Inode: 99})
	if got := s.LookupByPath("blocked.mov"); got != nil {
		t.Fatalf("unqualified pub/sub row published: %+v", got)
	}
	// Local RAM/SQLite operations must remain independent of remote admission.
	localDone := make(chan error, 1)
	go func() {
		if got := s.LookupByPath("local.mov"); got == nil || got.Inode != 11 {
			localDone <- errors.New("local RAM read failed")
			return
		}
		if rows, err := s.ListChildren("."); err != nil || len(rows) == 0 {
			localDone <- errors.New("local SQLite read failed")
			return
		}
		localDone <- s.Insert(MakeEntry("spooled.mov", false, 1, time.Now(), 12))
	}()
	select {
	case err := <-localDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("local metadata work stalled under unqualified backend")
	}
	rc.SetBackendQualified(true)
	close(release)
	<-done
	if got := s.LookupByPath("foreign.mov"); got != nil {
		t.Fatalf("old subscription event published after recovery: %+v", got)
	}
	current, _, _ := rc.backendAdmission()
	rc.applyEventFromSubscription(MetadataEvent{Op: "create", Path: "fresh.mov", Inode: 43}, current)
	if got := s.LookupByPath("fresh.mov"); got == nil || got.Inode != 43 {
		t.Fatalf("new qualified event missing: %+v", got)
	}
}

func TestBackendAdmissionRejectsCompletedOldScan(t *testing.T) {
	s := openTestStore(t)
	rc := &RedisClient{store: s}
	rc.SetBackendQualified(true)
	token := scanTokenForPublicationTest(t, rc)
	rc.SetBackendQualified(false)
	rc.SetBackendQualified(true)
	completed, err := rc.completeScanPublication(token, time.Now())
	if err != nil || completed {
		t.Fatalf("old SCAN completion = %v, %v", completed, err)
	}
	if _, ok, err := s.GetMeta(metaKeyLastSyncTime); err != nil || ok {
		t.Fatalf("old SCAN persisted last sync: ok=%v err=%v", ok, err)
	}
}

func TestBackendAdmissionRetryEnqueueLossIsAtomic(t *testing.T) {
	rc := &RedisClient{stopCh: make(chan struct{}), testTriggerSync: func() {}}
	defer func() {
		close(rc.stopCh)
		rc.attrRetryWG.Wait()
	}()
	rc.SetBackendQualified(true)
	old, _, _ := rc.backendAdmission()
	entered := make(chan struct{})
	release := make(chan struct{})
	rc.testBeforeAttrRetryEnqueue = func() {
		close(entered)
		<-release
	}
	oldDone := make(chan struct{})
	go func() {
		rc.scheduleAttrRetryWithBackend(42, old)
		close(oldDone)
	}()
	<-entered // old enqueue owns lifecycleMu, but has not entered attrRetryMu
	lossStarted := make(chan struct{})
	lossDone := make(chan struct{})
	go func() {
		close(lossStarted)
		rc.SetBackendQualified(false)
		close(lossDone)
	}()
	<-lossStarted
	close(release)
	<-lossDone // must serialize after old enqueue and clear it
	<-oldDone
	rc.testBeforeAttrRetryEnqueue = nil
	rc.SetBackendQualified(true)
	current, _, _ := rc.backendAdmission()
	if current == old {
		t.Fatal("loss/recovery did not advance backend generation")
	}
	rc.attrRetryMu.Lock()
	oldState := rc.attrRetries[42]
	rc.attrRetryMu.Unlock()
	if oldState != nil {
		t.Fatalf("old retry survived loss: %+v", oldState)
	}
	// Even an orphaned stale state must not swallow this generation's retry.
	rc.attrRetryMu.Lock()
	rc.attrRetries = map[uint64]*attrRetryState{
		42: &attrRetryState{needed: true, due: time.Now().Add(time.Hour), delay: time.Second, backend: old},
	}
	rc.attrRetryMu.Unlock()
	executed := make(chan uint64, 1)
	rc.testReconcileDir = func(inode uint64) error {
		executed <- inode
		return nil
	}
	rc.scheduleAttrRetryWithBackend(42, old) // stale caller cannot re-enqueue
	rc.scheduleAttrRetryWithBackend(42, current)
	rc.attrRetryMu.Lock()
	state := rc.attrRetries[42]
	rc.attrRetryMu.Unlock()
	if state == nil || state.backend != current {
		t.Fatalf("fresh retry was lost behind old state: %+v", state)
	}
	select {
	case inode := <-executed:
		if inode != 42 {
			t.Fatalf("retried inode = %d", inode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fresh retry did not execute")
	}
}

func TestBackendAdmissionRunningRetryLossStopJoins(t *testing.T) {
	rc := &RedisClient{stopCh: make(chan struct{}), testTriggerSync: func() {}}
	rc.rdb.Store(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}))
	rc.SetBackendQualified(true)
	generation, _, _ := rc.backendAdmission()
	entered := make(chan struct{})
	release := make(chan struct{})
	var queuedCalls atomic.Int32
	rc.testReconcileDir = func(inode uint64) error {
		if inode == 42 {
			close(entered)
			<-release
		} else {
			queuedCalls.Add(1)
		}
		return nil
	}
	rc.scheduleAttrRetryWithBackend(42, generation)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("running retry never entered")
	}
	rc.scheduleAttrRetryWithBackend(43, generation) // queued when loss arrives
	rc.SetBackendQualified(false)
	stopDone := make(chan struct{})
	go func() {
		rc.Stop()
		close(stopDone)
	}()
	select {
	case <-rc.stopCh: // Stop has begun joining the held worker
	case <-time.After(time.Second):
		t.Fatal("Stop did not close stopCh")
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned before running retry completed")
	default:
	}
	close(release)
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join running/queued retry")
	}
	if got := queuedCalls.Load(); got != 0 {
		t.Fatalf("queued retry executed after loss/Stop: %d", got)
	}
}

func TestBackendAdmissionConfigBackoffCancelsOnGenerationChange(t *testing.T) {
	for _, holdProbe := range []bool{false, true} {
		name := "sleep"
		if holdProbe {
			name = "probe"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("JM_KEYSPACE_AUTOCONFIG", "0")
			rc := &RedisClient{redisURL: "redis://127.0.0.1:6379/0", stopCh: make(chan struct{})}
			rc.SetBackendQualified(true)
			entered := make(chan struct{})
			var probes atomic.Int32
			rc.testProbeKeyspaceConfig = func(ctx context.Context) (bool, string) {
				probes.Add(1)
				if holdProbe {
					close(entered)
					<-ctx.Done()
				}
				return false, ""
			}
			if !holdProbe {
				rc.testBeforeConfigBackoff = func() { close(entered) }
			}
			done := make(chan struct{})
			go func() {
				rc.runKeyspaceSubscribe()
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("CONFIG fallback was not reached")
			}
			rc.SetBackendQualified(false)
			rc.SetBackendQualified(true)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("old CONFIG backoff survived qualification change")
			}
			// A new generation must be able to enter the CONFIG probe promptly.
			rc.testProbeKeyspaceConfig = func(context.Context) (bool, string) {
				probes.Add(1)
				return false, ""
			}
			rc.testBeforeConfigBackoff = nil
			close(rc.stopCh)
			rc.runKeyspaceSubscribe()
			if got := probes.Load(); got != 2 {
				t.Fatalf("CONFIG probes after recovery = %d, want 2", got)
			}
		})
	}
}
