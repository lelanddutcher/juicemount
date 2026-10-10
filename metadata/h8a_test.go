package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestH8AReconcileDirSQLFailureRetriesDurably(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 9)
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	newClient := func(s *Store) *RedisClient {
		return &RedisClient{
			store: s, stopCh: make(chan struct{}),
			testReadDirRaw: func(uint64) (map[string]string, error) {
				return map[string]string{"media.mov": string(value)}, nil
			},
			testReadAttrs: func([]string) ([]interface{}, error) {
				return []interface{}{string(encodeTestAttr(1700000000, 4096))}, nil
			},
		}
	}
	rc := newClient(store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rc.reconcileDir(1); err == nil {
		t.Fatal("closed SQLite did not fail reconcileDir")
	}
	if got := store.LookupByPath("media.mov"); got != nil {
		t.Fatalf("failed reconcile published cache row: %+v", got)
	}
	rc.attrRetryMu.Lock()
	pending := len(rc.attrRetries)
	rc.attrRetryMu.Unlock()
	if pending != 1 {
		t.Fatalf("failed reconcile pending parents = %d", pending)
	}
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc = newClient(store)
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := store.LookupByPath("media.mov"); got == nil || got.Size != 4096 || got.Inode != 42 {
		t.Fatalf("reopened retry row = %+v", got)
	}
}

func TestH8AGapFillEngagement(t *testing.T) {
	rc := &RedisClient{backstopChangedCh: make(chan struct{}, 1)}
	if !rc.noteGapFillResult(errIncompleteInodeAttrs) || keyspaceEngagement(rc.engaged.Load()) != keyspaceDegraded {
		t.Fatal("incomplete SCAN closed subscription or marked it fresh")
	}
	if rc.noteGapFillResult(errors.New("transport failed")) {
		t.Fatal("transport failure kept synchronous subscription")
	}
	if !rc.noteGapFillResult(nil) || keyspaceEngagement(rc.engaged.Load()) != keyspaceEnabled {
		t.Fatal("complete SCAN did not enable push")
	}
}

func TestH8AFailedBaselinePersistenceJoinsSubscription(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	if err := rdb.ConfigSet(context.Background(), "notify-keyspace-events", keyspaceNotifyWant).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := NewRedisClient(metadataLiveTestRedisURL(), store)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)

	rc.background.Add(1)
	subscriptionDone := make(chan struct{})
	go func() {
		defer rc.background.Done()
		rc.runKeyspaceSubscribe()
		close(subscriptionDone)
	}()
	select {
	case <-subscriptionDone:
	case <-time.After(3 * time.Second):
		_ = rc.redisDB().Close()
		t.Fatal("failed baseline persistence stranded subscription startup")
	}
	stopDone := make(chan struct{})
	go func() { rc.Stop(); close(stopDone) }()
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join failed subscription startup")
	}
}

func TestH8ABootSkipDoesNotErasePumpedEventDebt(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	ctx := context.Background()
	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", keyspaceNotifyWant).Err(); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	for key, value := range map[string]string{
		metaKeyPushSessionDirty: "0", metaKeyIncompleteBaseline: "0",
		metaKeyPushLastAlive: stamp, metaKeyLastSyncTime: stamp,
	} {
		if err := store.SetMeta(key, value); err != nil {
			t.Fatal(err)
		}
	}
	rc, err := NewRedisClient(metadataLiveTestRedisURL(), store)
	if err != nil {
		t.Fatal(err)
	}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)
	paused := make(chan struct{})
	release := make(chan struct{})
	rc.testBeforeBootSkipPromotion = func() { close(paused); <-release }
	rc.background.Add(1)
	go func() { defer rc.background.Done(); rc.runKeyspaceSubscribe() }()
	select {
	case <-paused:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("boot-skip promotion was not reached")
	}
	if err := rdb.HSet(ctx, "d1", "race.mov", string(encodeDirChild(101, jfsTypeFile))).Err(); err != nil {
		close(release)
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		rc.lifecycleMu.Lock()
		seen := rc.eventGeneration > 0
		rc.lifecycleMu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("pump did not observe Redis event")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	rc.Stop()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted := &RedisClient{store: store}
	if !restarted.baselineIncomplete() || restarted.ShouldSkipBootSync() {
		t.Fatal("pumped event was certified fresh after Stop and SQLite reopen")
	}
	bootGapFillEvaluated.Store(false)
	if restarted.shouldSkipBootGapFill() {
		t.Fatal("pumped event allowed boot gap-fill skip")
	}
}

func TestH8AReconcileInsertFailureDoesNotPublishCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store}
	e := MakeEntry("dir/media.mov", false, 4096, time.Unix(1700000000, 0), 42)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rc.applyReconcileUpserts([]*Entry{e}); err == nil {
		t.Fatal("closed SQLite accepted reconcile insert")
	}
	if got := store.LookupByPath(e.Path); got != nil {
		t.Fatalf("failed SQL write published RAM row: %+v", got)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc.store = store
	if err := rc.applyReconcileUpserts([]*Entry{e}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := store.LookupByPath(e.Path); got == nil || got.Size != 4096 || got.Inode != 42 {
		t.Fatalf("reopened durable row = %+v", got)
	}
}

func TestH8AIncompleteBaselineVetoesRestartSkip(t *testing.T) {
	store := openTestStore(t)
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	if err := store.SetMeta(metaKeyPushLastAlive, stamp); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMeta(metaKeyLastSyncTime, stamp); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)
	if !rc.shouldSkipBootGapFill() {
		t.Fatal("fresh baseline should skip")
	}
	if err := rc.markBaselineIncomplete(); err != nil {
		t.Fatal(err)
	}
	bootGapFillEvaluated.Store(false)
	if rc.shouldSkipBootGapFill() {
		t.Fatal("incomplete baseline skipped gap-fill on restart")
	}
	if rc.ShouldSkipBootSync() {
		t.Fatal("incomplete baseline skipped C1 full SCAN")
	}
	if err := rc.clearBaselineIncomplete(); err != nil {
		t.Fatal(err)
	}
	bootGapFillEvaluated.Store(false)
	if !rc.shouldSkipBootGapFill() {
		t.Fatal("completed baseline did not restore fresh skip")
	}
}

func TestH8AAttrRetryOverflowMarkerSurvivesSQLiteReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, stopCh: make(chan struct{}), testTriggerSync: func() {}}
	rc.testReconcileDir = func(uint64) error { <-rc.stopCh; return context.Canceled }
	for i := 1; i <= maxAttrRetryParents+1; i++ {
		rc.scheduleAttrRetry(uint64(i))
	}
	if !rc.baselineIncomplete() {
		t.Fatal("retry overflow did not persist incomplete-baseline debt")
	}
	close(rc.stopCh)
	rc.attrRetryMu.Lock()
	rc.attrRetryStopped = true
	rc.attrRetryMu.Unlock()
	rc.attrRetryWG.Wait()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc = &RedisClient{store: store}
	if !rc.baselineIncomplete() {
		t.Fatal("reopened SQLite lost overflow's incomplete-baseline debt")
	}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)
	if rc.shouldSkipBootGapFill() {
		t.Fatal("reopened overflow debt allowed boot gap-fill skip")
	}
	if rc.ShouldSkipBootSync() {
		t.Fatal("reopened overflow debt allowed C1 boot sync skip")
	}
}

func TestH8AUnreadableSymlinkDeferredUntilTypedAttr(t *testing.T) {
	mode, isDir := modeForFiletype(jfsTypeSymlink)
	child := dirChild{inode: 91, childPath: "dir/link", mode: mode, isDir: isDir}
	lookup := func(string) *Entry { return nil }
	upsert, _ := buildReconcileChildEntries([]dirChild{child}, nil, lookup)
	if len(upsert) != 0 {
		t.Fatalf("unreadable symlink published: %+v", upsert)
	}
	upsert, _ = buildReconcileChildEntries([]dirChild{child}, map[uint64][]byte{91: encodeTestAttr(1, 0)}, lookup)
	if len(upsert) != 1 || upsert[0].Mode&fs.ModeSymlink == 0 || upsert[0].Size != 0 {
		t.Fatalf("decoded symlink classification lost: %+v", upsert)
	}
}

func TestH8AScanRawAttrValidity(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
		size  int64
	}{
		{"1:0:0:0:42:1:missing", false, 0},
		{"1:1:1700000000:0:43:1:empty", true, 0},
		{"3:1:1700000000:91:44:1:link:name", true, 91},
	} {
		e, err := parseScanRawEntry(tc.raw)
		if err != nil || e.attrValid != tc.valid || e.size != tc.size {
			t.Fatalf("parseScanRawEntry(%q) = %+v, %v", tc.raw, e, err)
		}
	}
	if _, err := parseScanRawEntry("1:bogus:0:0:42:1:x"); err == nil {
		t.Fatal("invalid validity accepted")
	}
}

func TestH8AActualLuaScanPreservesUnreadableRows(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	ctx := context.Background()
	for _, child := range []struct {
		name  string
		inode uint64
		ft    byte
	}{
		{"missing.mov", 101, jfsTypeFile},
		{"new.mov", 107, jfsTypeFile},
		{"short.mov", 102, jfsTypeFile},
		{"empty.mov", 103, jfsTypeFile},
		{"replaced.mov", 104, jfsTypeFile},
		{"folder", 105, jfsTypeDir},
		{"link", 106, jfsTypeSymlink},
	} {
		if err := rdb.HSet(ctx, "d1", child.name, string(encodeDirChild(child.inode, child.ft))).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.Set(ctx, "i102", string([]byte{1, 2, 3}), 0).Err(); err != nil {
		t.Fatal(err)
	}
	for inode, size := range map[uint64]int64{103: 0, 104: 16384, 105: 0, 106: 12} {
		if err := rdb.Set(ctx, "i"+strconv.FormatUint(inode, 10), string(encodeInodeAttr(1700000000, size)), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rdb.LPush(ctx, "delfiles", "unrelated").Err(); err != nil {
		t.Fatal(err)
	}
	res, err := rdb.Eval(ctx, luaScanBatch, nil, "0", "500").StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 8 || res[0] != "0" {
		t.Fatalf("Lua SCAN returned %v; want cursor 0 and seven children", res)
	}
	got := make(map[string]scanRawEntry)
	for _, raw := range res[1:] {
		entry, err := parseScanRawEntry(raw)
		if err != nil {
			t.Fatal(err)
		}
		got[entry.name] = entry
	}
	for _, tc := range []struct {
		name  string
		inode uint64
		valid bool
		size  int64
	}{
		{"missing.mov", 101, false, 0},
		{"new.mov", 107, false, 0},
		{"short.mov", 102, false, 0},
		{"empty.mov", 103, true, 0},
		{"replaced.mov", 104, true, 16384},
		{"folder", 105, true, 0},
		{"link", 106, true, 12},
	} {
		entry, ok := got[tc.name]
		if !ok || entry.inode != tc.inode || entry.attrValid != tc.valid || entry.size != tc.size {
			t.Fatalf("Lua %s = %+v found=%v", tc.name, entry, ok)
		}
	}

	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	known := MakeEntry("missing.mov", false, 8192, time.Unix(1600000000, 0), 88)
	known.LocalOnly = true
	if err := store.Insert(known); err != nil {
		t.Fatal(err)
	}
	old := MakeEntry("replaced.mov", false, 8192, time.Unix(1600000000, 0), 99)
	if err := store.Insert(old); err != nil {
		t.Fatal(err)
	}
	rc, err := NewRedisClient(metadataLiveTestRedisURL(), store)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()
	if err := rc.SyncOnce(); !errors.Is(err, errIncompleteInodeAttrs) {
		t.Fatalf("incomplete SCAN error=%v", err)
	}
	if entry := store.LookupByPath("new.mov"); entry != nil {
		t.Fatalf("unreadable new file published: %+v", entry)
	}
	if entry := store.LookupByPath("short.mov"); entry != nil {
		t.Fatalf("short-attr new file published: %+v", entry)
	}
	if entry := store.LookupByPath("empty.mov"); entry == nil || entry.Size != 0 || entry.Inode != 103 {
		t.Fatalf("valid zero file = %+v", entry)
	}
	if entry := store.LookupByPath("replaced.mov"); entry == nil || entry.Size != 16384 || entry.Inode != 104 {
		t.Fatalf("valid replacement = %+v", entry)
	}
	rc.Stop()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if entry := store.LookupByPath("missing.mov"); entry == nil || entry.Inode != 88 ||
		entry.Size != 8192 || entry.Mtime.Unix() != 1600000000 || !entry.LocalOnly {
		t.Fatalf("reopened unreadable replacement clobbered known row: %+v", entry)
	}
}

func TestH8AAttrRetryDeduplicatesAndJoins(t *testing.T) {
	rc := &RedisClient{stopCh: make(chan struct{})}
	called := make(chan uint64, 2)
	rc.testReconcileDir = func(inode uint64) error { called <- inode; return nil }
	rc.scheduleAttrRetry(42)
	rc.scheduleAttrRetry(42)
	rc.attrRetryMu.Lock()
	if len(rc.attrRetries) != 1 {
		t.Fatalf("retry workers = %d, want 1", len(rc.attrRetries))
	}
	rc.attrRetryMu.Unlock()
	select {
	case got := <-called:
		if got != 42 {
			t.Fatalf("retried inode %d", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retry did not run")
	}
	close(rc.stopCh)
	rc.attrRetryMu.Lock()
	rc.attrRetryStopped = true
	rc.attrRetryMu.Unlock()
	rc.attrRetryWG.Wait()
}

func TestH8AAttrRetryPopulationBounded(t *testing.T) {
	store := openTestStore(t)
	rc := &RedisClient{store: store, stopCh: make(chan struct{}), testTriggerSync: func() {}}
	rc.testReconcileDir = func(uint64) error { <-rc.stopCh; return context.Canceled }
	for i := 1; i <= maxAttrRetryParents+200; i++ {
		rc.scheduleAttrRetry(uint64(i))
	}
	rc.attrRetryMu.Lock()
	count, overflow, running := len(rc.attrRetries), rc.attrRetryOverflow, rc.attrRetryRunning
	rc.attrRetryMu.Unlock()
	if count != maxAttrRetryParents || !overflow || !running {
		t.Fatalf("queue=%d overflow=%v running=%v", count, overflow, running)
	}
	if !rc.baselineIncomplete() {
		t.Fatal("overflow did not persist incomplete baseline")
	}
	close(rc.stopCh)
	rc.attrRetryMu.Lock()
	rc.attrRetryStopped = true
	rc.attrRetryMu.Unlock()
	rc.attrRetryWG.Wait()
}

func TestH8AAttrRetryConcurrentWorkBound(t *testing.T) {
	rc := &RedisClient{stopCh: make(chan struct{})}
	started := make(chan struct{}, attrRetryWorkers+1)
	release := make(chan struct{})
	rc.testReconcileDir = func(uint64) error {
		started <- struct{}{}
		<-release
		return nil
	}
	for i := 1; i <= attrRetryWorkers*2; i++ {
		rc.scheduleAttrRetry(uint64(i))
	}
	for i := 0; i < attrRetryWorkers; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			close(release)
			close(rc.stopCh)
			rc.attrRetryWG.Wait()
			t.Fatal("worker pool did not fill")
		}
	}
	select {
	case <-started:
		t.Fatal("more than four retry reconciles active")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
}

func TestH8APeriodicCompletionRestoresSameSession(t *testing.T) {
	store := openTestStore(t)
	rc := &RedisClient{store: store, stopCh: make(chan struct{}), backstopChangedCh: make(chan struct{}, 1)}
	session, err := rc.openPushSession()
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.recordBaselineDebt(); err != nil {
		t.Fatal(err)
	}
	rc.setEngagement(keyspaceDegraded)
	token := rc.scanToken()
	if err := rc.finishScanBaseline(token); err != nil {
		t.Fatal(err)
	}
	if got := keyspaceEngagement(rc.engaged.Load()); got != keyspaceEnabled {
		t.Fatalf("same-session recovery engagement = %v", got)
	}
	if got := rc.currentBackstop(); got != backstopForClass(currentLinkClass()) {
		t.Fatalf("recovery backstop = %v", got)
	}
	if !rc.baselineIncomplete() {
		t.Fatal("live session became restart-fresh")
	}
	rc.closePushSession(session, true)
	rc.sealStoppedPushSession()
	if rc.baselineIncomplete() {
		t.Fatal("clean joined session stayed dirty")
	}
}

func TestH8ANewerOverflowFencesScanCompletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, stopCh: make(chan struct{}), testTriggerSync: func() {}}
	rc.testReconcileDir = func(uint64) error { <-rc.stopCh; return context.Canceled }
	if err := rc.markBaselineIncomplete(); err != nil {
		t.Fatal(err)
	}
	token := rc.scanToken()
	for i := 1; i <= maxAttrRetryParents+2; i++ {
		rc.scheduleAttrRetry(uint64(i))
	}
	if err := rc.finishScanBaseline(token); err != nil {
		t.Fatal(err)
	}
	close(rc.stopCh)
	rc.attrRetryWG.Wait()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted := &RedisClient{store: store}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)
	if !restarted.baselineIncomplete() || restarted.shouldSkipBootGapFill() || restarted.ShouldSkipBootSync() {
		t.Fatal("new overflow was cleared by stale SCAN or permitted restart skip")
	}
}

func TestH8AStaleResumedScanCannotClobberRecoveredMirror(t *testing.T) {
	liveRedisOrSkip(t)
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := NewRedisClient(metadataLiveTestRedisURL(), store)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Stop()
	newer := MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43)
	if err := store.Insert(newer); err != nil {
		t.Fatal(err)
	}
	old := scanRawEntry{ft: int(jfsTypeFile), attrValid: true, mtime: 1700000000,
		size: 4096, inode: 42, parentInode: "1", name: "media.mov"}
	rc.saveScanResumeWithToken([]scanRawEntry{old}, map[string][2]string{"42": {"1", "media.mov"}},
		"after-old", rc.scanToken())
	if err := rc.recordBaselineDebt(); err != nil {
		t.Fatal(err)
	}
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor == "0" {
			return []string{"0", "1:1:1700000100:8192:43:1:media.mov"}, nil
		}
		if cursor == "after-old" {
			return []string{"0"}, nil
		}
		t.Fatalf("unexpected SCAN cursor %q", cursor)
		return nil, nil
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatal(err)
	}
	if entry := store.LookupByPath("media.mov"); entry == nil || entry.Inode != 43 || entry.Size != 8192 {
		t.Fatalf("stale resumed SCAN clobbered recovered mirror: %+v", entry)
	}
	rc.Stop()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if entry := store.LookupByPath("media.mov"); entry == nil || entry.Inode != 43 || entry.Size != 8192 {
		t.Fatalf("reopened mirror contains stale resumed row: %+v", entry)
	}
}

func TestH8AMidApplyDebtCannotClobberRecoveredMirror(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}

	checked := make(chan struct{})
	release := make(chan struct{})
	rc.testScanBatch = func(_ context.Context, cursor, _ string) ([]string, error) {
		if cursor != "0" {
			t.Fatalf("unexpected SCAN cursor %q", cursor)
		}
		return []string{"0", "1:1:1700000000:4096:42:1:media.mov"}, nil
	}
	rc.testAfterScanTokenCheck = func() {
		close(checked)
		<-release
	}

	scanDone := make(chan error, 1)
	go func() { scanDone <- rc.SyncOnce() }()
	select {
	case <-checked:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("SCAN did not reach the post-token-check pause")
	}

	if err := rc.recordBaselineDebt(); err != nil {
		close(release)
		t.Fatal(err)
	}
	newer := MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43)
	if err := rc.applyReconcileUpserts([]*Entry{newer}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-scanDone; err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 || got.Size != 8192 {
		t.Fatalf("mid-apply SCAN clobbered recovered cache row: %+v", got)
	}
	if !rc.baselineIncomplete() || rc.ShouldSkipBootSync() {
		t.Fatal("invalidated SCAN cleared baseline debt or allowed restart skip")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc.store = store
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 || got.Size != 8192 {
		t.Fatalf("reopened mirror contains stale mid-apply SCAN row: %+v", got)
	}
	if !rc.baselineIncomplete() || rc.ShouldSkipBootSync() {
		t.Fatal("reopened mirror lost invalidated-SCAN debt or allowed restart skip")
	}
}

func TestH8ALadderPersistDoesNotBlockBaselineDebt(t *testing.T) {
	store := openTestStore(t)
	rc := &RedisClient{store: store}

	paused := make(chan struct{})
	release := make(chan struct{})
	rc.testBeforePruneLadderPersist = func() {
		close(paused)
		<-release
	}

	token := scanTokenForPublicationTest(t, rc)
	publishDone := make(chan error, 1)
	go func() {
		_, err := rc.completeScanPublication(token, time.Now())
		publishDone <- err
	}()
	select {
	case <-paused:
	case <-time.After(2 * time.Second):
		close(release)
		select {
		case <-publishDone:
		case <-time.After(2 * time.Second):
			t.Fatal("publication harness did not join after missing ladder-persist hook")
		}
		t.Fatal("publication did not reach the ladder-persist pause")
	}

	started := make(chan struct{})
	debtDone := make(chan error, 1)
	go func() {
		close(started)
		debtDone <- rc.recordBaselineDebt()
	}()
	<-started
	debtBeforeRelease := false
	select {
	case err := <-debtDone:
		if err != nil {
			close(release)
			<-publishDone
			t.Fatalf("recordBaselineDebt failed: %v", err)
		}
		debtBeforeRelease = true
	case <-time.After(250 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("publication failed after hook release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publication harness did not join after hook release")
	}
	if !debtBeforeRelease {
		select {
		case err := <-debtDone:
			if err != nil {
				t.Fatalf("recordBaselineDebt failed after hook release: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("recordBaselineDebt harness did not join after hook release")
		}
		t.Fatal("RED: recordBaselineDebt could not complete while final ladder persistence held lifecycleMu")
	}
}

func TestH8ALadderPersistFailureDoesNotCompleteScan(t *testing.T) {
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "1")
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, pruneAbsent: map[string]int{"missing.mov": 1}}
	if err := rc.recordBaselineDebt(); err != nil {
		t.Fatal(err)
	}
	const priorSync = "1700000000"
	if err := store.SetMeta(metaKeyLastSyncTime, priorSync); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_prune_ladder BEFORE INSERT ON prune_ladder_stage BEGIN SELECT RAISE(FAIL, 'injected ladder failure'); END`); err != nil {
		t.Fatal(err)
	}

	completed, err := rc.completeScanPublication(scanTokenForPublicationTest(t, rc), time.Now())
	if err == nil {
		t.Fatal("ladder persistence failure was not returned")
	}
	if completed {
		t.Fatal("SCAN was certified complete after ladder persistence failed")
	}
	if !rc.baselineIncomplete() {
		t.Fatal("ladder persistence failure cleared incomplete-baseline debt")
	}
	if got, ok, err := store.GetMeta(metaKeyLastSyncTime); err != nil || !ok || got != priorSync {
		t.Fatalf("failed SCAN sync timestamp = %q, %v, %v; want prior %q", got, ok, err, priorSync)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc.store = store
	if !rc.baselineIncomplete() {
		t.Fatal("reopened SQLite lost incomplete-baseline debt after ladder persistence failure")
	}
	if got, ok, err := store.GetMeta(metaKeyLastSyncTime); err != nil || !ok || got != priorSync {
		t.Fatalf("reopened failed SCAN sync timestamp = %q, %v, %v; want prior %q", got, ok, err, priorSync)
	}
}

func TestH8AActiveLadderUpdatesSurviveLaterScan(t *testing.T) {
	t.Setenv("JM_PRUNE_LADDER_DURABLE", "1")
	dbPath := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first := map[string]int{"missing.mov": 1}
	generation, err := store.StagePruneLadderSnapshot(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ActivatePruneLadderStage(generation, time.Unix(1700000000, 0)); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, pruneAbsent: map[string]int{"missing.mov": 2}, ladderPersisted: first}
	completed, err := rc.completeScanPublication(scanTokenForPublicationTest(t, rc), time.Unix(1700000100, 0))
	if err != nil || !completed {
		t.Fatalf("later SCAN completed=%v err=%v", completed, err)
	}
	waitForPruneLadderStageGone(t, store, generation)
	var stagedRows, manifests int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage`).Scan(&stagedRows); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM prune_ladder_stage_state`).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if stagedRows != 1 || manifests != 1 {
		t.Fatalf("completed SCAN retained %d staged rows and %d manifests; want only active generation", stagedRows, manifests)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loaded, err := store.LoadPruneLadder()
	if err != nil || loaded["missing.mov"] != 2 {
		t.Fatalf("reopened ladder after later SCAN = %v, %v; want count 2", loaded, err)
	}
}

func TestH8AStaleRetryCannotClobberNewerScan(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc := &RedisClient{store: store}
	staleToken := rc.scanToken() // retry captured before its Redis observation

	newer := MakeEntry("media.mov", false, 8192, time.Unix(1700000100, 0), 43)
	if err := rc.applyReconcileUpserts([]*Entry{newer}); err != nil {
		t.Fatal(err)
	}
	stale := MakeEntry("media.mov", false, 4096, time.Unix(1700000000, 0), 42)
	if err := rc.applyReconcileUpsertsWithToken([]*Entry{stale}, &staleToken); !errors.Is(err, errScanPublicationInvalidated) {
		t.Fatalf("stale retry error = %v, want publication invalidation", err)
	}
	if got := store.LookupByPath("media.mov"); got == nil || got.Inode != 43 || got.Size != 8192 {
		t.Fatalf("stale retry clobbered newer SCAN row: %+v", got)
	}
}

func TestH8ACrashedSessionVetoesFreshHeartbeat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mirror.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store}
	if _, err := rc.openPushSession(); err != nil {
		t.Fatal(err)
	}
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	if err := store.SetMeta(metaKeyPushLastAlive, stamp); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMeta(metaKeyLastSyncTime, stamp); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted := &RedisClient{store: store}
	bootGapFillEvaluated.Store(false)
	defer bootGapFillEvaluated.Store(false)
	if restarted.shouldSkipBootGapFill() || restarted.ShouldSkipBootSync() {
		t.Fatal("crashed dirty session trusted fresh heartbeat or SCAN timestamp")
	}
}

func TestH8AStopRetainsPostScanEventDebt(t *testing.T) {
	store := openTestStore(t)
	rc := &RedisClient{store: store}
	session, err := rc.openPushSession()
	if err != nil {
		t.Fatal(err)
	}
	token := rc.scanToken()
	rc.notePushEvent()
	if err := rc.finishScanBaseline(token); err != nil {
		t.Fatal(err)
	}
	rc.closePushSession(session, true)
	rc.sealStoppedPushSession()
	if !rc.baselineIncomplete() {
		t.Fatal("post-SCAN event was discarded and session sealed fresh")
	}
}
