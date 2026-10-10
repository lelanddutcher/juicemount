//go:build redisoptionsacceptance

package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestVolumeSessionColdBootAndOfflineHits(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	var qualified atomic.Bool
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }, OnQualified: qualified.Store})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if s.Reader() != nil || qualified.Load() {
		t.Fatal("cold session starts qualified")
	}
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !qualified.Load() || !s.Qualified() {
		t.Fatal("qualified backend did not open admission")
	}
	r := s.Reader()
	got := make([]byte, 4096)
	if _, err := r.ReadBlock(ctx, 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("online hit = %v", err)
	}
	s.SetOnline(false)
	if qualified.Load() || s.Qualified() {
		t.Fatal("offline transition did not close admission")
	}
	// Cached bytes remain available; an unknown mapping cannot use the still
	// reachable Redis server to fetch new data while offline admission is shut.
	if _, err := r.ReadBlock(context.Background(), 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("offline hit = %v", err)
	}
	if _, err := r.ReadBlock(context.Background(), 4, 0, got); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("offline miss = %v", err)
	}
}

type heldVolumeReply struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *heldVolumeReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *heldVolumeReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *heldVolumeReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "eval" && err == nil {
			held := false
			h.once.Do(func() { held = true; close(h.entered) })
			if held {
				<-h.release
			}
		}
		return err
	}
}

func TestVolumeSessionSuspensionRejectsEnteredRemoteReply(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	hook := &heldVolumeReply{entered: make(chan struct{}), release: make(chan struct{})}
	rdb.AddHook(hook)
	r, err := NewVolumeReader(binding, rdb)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(hook.release) }) }
	defer releaseHeld()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	got := make([]byte, 4096)
	go func() { _, err := r.ReadBlock(ctx, 3, 0, got); done <- err }()
	select {
	case <-hook.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Redis reply did not reach held boundary")
	}
	r.SetRemoteAvailable(false)
	r.SetRemoteAvailable(true)
	r.SetRemoteAvailable(false)
	releaseHeld()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("pre-outage reply = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reply did not settle")
	}
	if !bytes.Equal(got, make([]byte, 4096)) || len(r.sliceCache) != 0 {
		t.Fatal("old remote reply published after outage")
	}
	r.SetRemoteAvailable(true)
	if _, err := r.ReadBlock(ctx, 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("fresh online reply refused: %v", err)
	}
}

func TestVolumeSessionRetriesWithoutReachabilityEdge(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+foreignTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{}, 1)
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }, OnStatus: func(ok bool, _ error) {
		if ok {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	if err := s.Qualify(ctx); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("foreign volume = %v", err)
	}
	if s.Qualified() {
		t.Fatal("foreign volume opened admission")
	}
	select {
	case <-s.kick:
	default:
	} // require the retry ticker, not an old kick
	s.Start()
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+activeTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	// No second SetOnline call. The session must recover from its own retry.
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("recovery required another reachability edge")
	}
	if !s.Qualified() {
		t.Fatal("retry did not qualify expected volume")
	}
}

func TestVolumeSessionLateQualificationCannotUndoOutage(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	var opened atomic.Int64
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { close(entered); <-release; return nil }, OnQualified: func(v bool) {
		if v {
			opened.Add(1)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	defer releaseHeld()
	s.SetOnline(true)
	done := make(chan error, 1)
	go func() { done <- s.Qualify(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("qualification did not reach held reader")
	}
	s.SetOnline(false)
	releaseHeld()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stale qualification = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("qualification did not settle")
	}
	if s.Reader() != nil || s.Qualified() || opened.Load() != 0 {
		t.Fatal("stale qualification published after newer outage")
	}
}

func TestVolumeSessionStopJoinsActualQualification(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	var opened atomic.Int64
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { close(entered); <-release; return nil }, OnQualified: func(v bool) {
		if v {
			opened.Add(1)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { releaseHeld(); s.Stop() }()
	s.SetOnline(true)
	qdone := make(chan error, 1)
	go func() { qdone <- s.Qualify(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("qualification did not enter")
	}
	stopped := make(chan struct{})
	go func() { s.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop abandoned entered qualification")
	case <-time.After(25 * time.Millisecond):
	}
	releaseHeld()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not join released work")
	}
	if err := <-qdone; err == nil {
		t.Fatal("stopped qualification reported success")
	}
	if s.Qualified() || opened.Load() != 0 {
		t.Fatal("Stop permitted late admission")
	}
	if err := s.Qualify(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-Stop admission = %v", err)
	}
	s.Start() // cannot add work after Stop's actual join
}

func TestVolumeSessionSavedOfflineSnapshotBoot(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	store := newVolumeTestStore()
	state := t.TempDir()
	cfg := SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: state, VerifyReader: func(*Reader) error { return nil }}
	first, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := first.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reader().ReadBlock(ctx, 3, 0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	first.Stop()
	// The new session's runtime endpoint is deliberately dead. Its original
	// logical selector is unchanged, as during a proxy restart/offline boot.
	opts := *rdb.Options()
	opts.Addr = "127.0.0.1:1"
	cfg.Options = &opts
	second, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	if second.Qualified() {
		t.Fatal("saved binding authorized live remote metadata")
	}
	got := make([]byte, 4096)
	if second.Reader() == nil {
		t.Fatal("qualified offline reader absent")
	}
	if _, err := second.Reader().ReadBlock(context.Background(), 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("offline saved boot = %v", err)
	}
}

func TestVolumeSessionSettingsMismatchRefusesBeforeReader(t *testing.T) {
	raw, rdb, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	var verifyCalls atomic.Int64
	_, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Selection: []string{"different-route"}, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { verifyCalls.Add(1); return nil }})
	if !errors.Is(err, ErrVolumeBinding) || verifyCalls.Load() != 0 {
		t.Fatalf("different saved selection = %v, reader calls=%d", err, verifyCalls.Load())
	}
}

func TestVolumeSessionCanceledQualifierDoesNotWaitForOtherWork(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { close(entered); <-release; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { releaseHeld(); s.Stop() }()
	s.SetOnline(true)
	first := make(chan error, 1)
	go func() { first <- s.Qualify(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first qualifier not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- s.Qualify(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued qualifier = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("canceled caller waited behind actual work")
	}
	select {
	case <-first:
		t.Fatal("canceling queued caller canceled current qualifier")
	default:
	}
	releaseHeld()
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first did not finish")
	}
}

func TestVolumeSessionCorruptMapRecoversAfterIdentityProof(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	r, err := NewVolumeReader(binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetMappingSnapshotPath(r.MappingSnapshotPath(state)); err != nil {
		t.Fatal(err)
	}
	r.sliceCache["3_0"] = []SliceInfo{{SliceID: 77, Size: 4096, Len: 4096}}
	r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, "c3_0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "c3_0", "not-a-list", 0).Err(); err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: state, VerifyReader: func(*Reader) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	if err := s.Qualify(ctx); err != nil {
		t.Fatalf("qualified format + corrupt map stayed offline: %v", err)
	}
	if !s.Qualified() {
		t.Fatal("ordinary corrupt map held whole library offline")
	}
	s.Reader().sliceMu.RLock()
	count := len(s.Reader().sliceCache)
	s.Reader().sliceMu.RUnlock()
	if count != 0 {
		t.Fatal("corrupt maps retained after online admission")
	}
}

func TestVolumeSessionSuspensionPreservesEnteredLocalRead(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	_, _, binding := volumeFixture(t)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	r, err := NewVolumeReader(binding, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	r.sliceCache["3_0"] = []SliceInfo{{SliceID: 77, Size: 4096, Len: 4096}}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHeld()
	// Buffer acquisition follows the real read's mapping-generation capture.
	// Holding the pool allocation gives the outage a deterministic interleave.
	r.bufferPool.New = func() any { close(entered); <-release; return make([]byte, 4096) }
	done := make(chan error, 1)
	got := make([]byte, 4096)
	go func() { _, err := r.ReadBlock(context.Background(), 3, 0, got); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("local read did not enter")
	}
	r.SetRemoteAvailable(false)
	r.SetRemoteAvailable(false) // a failed recovery retry is not a cache mutation
	releaseHeld()
	select {
	case err := <-done:
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("outage invalidated cached local read: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("local read did not finish")
	}
}

func TestVolumeCacheUsesSelectedExecutable(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	for _, tc := range []struct {
		name, body string
		wantOK     bool
	}{
		{"supported", "#!/bin/sh\nprintf 'juicefs version 1.3.1\\n'\n", true},
		{"unsupported", "#!/bin/sh\nprintf 'juicefs version 1.4.0\\n'\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "selected-juicefs")
			if err := os.WriteFile(binary, []byte(tc.body), 0700); err != nil {
				t.Fatal(err)
			}
			err := r.VerifyWithBinary(context.Background(), binary)
			if (err == nil) != tc.wantOK {
				t.Fatalf("selected executable qualification = %v", err)
			}
		})
	}
	if err := r.VerifyWithBinary(context.Background(), filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("missing selected executable fell back to another installation")
	}
}

func TestVolumeCacheVersionProbeHonorsDeadline(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	binary := filepath.Join(t.TempDir(), "held-version")
	// exec transfers ownership to sleep; the deadline kills and reaps that
	// exact version child, without leaving shell descendants or retained pipes.
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec /bin/sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := r.VerifyWithBinary(ctx, binary); err == nil {
		t.Fatal("held version probe passed")
	}
	if ctx.Err() == nil || time.Since(started) > time.Second {
		t.Fatal("version probe ignored caller deadline")
	}
}

func TestVolumeSessionUnsupportedReaderPreservesQualifiedBackend(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, _ := volumeFixture(t)
	var admitted atomic.Bool
	var statusErr error
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return errors.New("unsupported layout") }, OnQualified: admitted.Store, OnStatus: func(ok bool, err error) {
		if ok {
			statusErr = err
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.Qualified() || !admitted.Load() || s.Reader() != nil || !errors.Is(s.ReaderError(), ErrReaderUnavailable) || !errors.Is(statusErr, ErrReaderUnavailable) {
		t.Fatal("unsupported acceleration blocked or misreported qualified backend")
	}
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+foreignTestUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Qualify(ctx); !errors.Is(err, ErrVolumeBinding) {
		t.Fatalf("identity mismatch softened: %v", err)
	}
	if s.Qualified() || admitted.Load() {
		t.Fatal("unsupported-reader policy reopened mismatched backend")
	}
}

func TestVolumeSessionSavedUnsupportedReaderCanStartOffline(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	opts := *rdb.Options()
	opts.Addr = "127.0.0.1:1"
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: &opts, StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return errors.New("unavailable executable") }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if s.Reader() != nil || s.Qualified() || !errors.Is(s.ReaderError(), ErrReaderUnavailable) {
		t.Fatal("offline unsupported accelerator misclassified")
	}
}

func TestVolumeSessionTransportFencesHeldQualification(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	seedVolumeSlice(t, rdb)
	want := bytes.Repeat([]byte{'B'}, 4096)
	seedVolumeBytes(t, activeTestUUID, want)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := s.Reader().ReadBlock(ctx, 3, 0, got); err != nil {
		t.Fatal(err)
	}
	oldProbe, oldPool := s.probe, s.Reader().rdb.Load()
	hook := &heldVolumeReply{entered: make(chan struct{}), release: make(chan struct{})}
	oldProbe.AddHook(hook)
	var once sync.Once
	release := func() { once.Do(func() { close(hook.release) }) }
	defer release()
	done := make(chan error, 1)
	go func() { done <- s.Qualify(ctx) }()
	select {
	case <-hook.entered:
	case <-ctx.Done():
		t.Fatal("qualification reply not held")
	}
	if !s.TransportChanged() {
		t.Fatal("transport reset refused")
	}
	if s.Qualified() || s.probe == oldProbe || s.Reader().rdb.Load() == oldPool {
		t.Fatal("transport reset did not retire pools/admission")
	}
	if _, err := s.Reader().ReadBlock(context.Background(), 3, 0, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("transport change lost local hit: %v", err)
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stale reply = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("stale qualification did not settle")
	}
	if s.Qualified() {
		t.Fatal("old transport reply reopened admission")
	}
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.Qualified() {
		t.Fatal("fresh transport did not qualify")
	}
	s.Stop()
	if s.TransportChanged() {
		t.Fatal("closed session created new transport pool")
	}
}

func TestVolumeSessionQualificationWaitsForActualTransportReset(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Bool
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }, OnQualified: admitted.Store})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseReset := func() { once.Do(func() { close(release) }) }
	defer releaseReset()
	oldPool := s.Reader().rdb.Load()
	s.resetReader = func(r *Reader) bool { close(entered); <-release; return r.ResetRedisConnections() }
	done := make(chan bool, 1)
	go func() { done <- s.TransportChanged() }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("transport reset not held")
	}
	if s.Reader().rdb.Load() != oldPool {
		t.Fatal("old pool retired before held boundary")
	}
	if err := s.Qualify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("qualification crossed pending reset: %v", err)
	}
	if s.Qualified() || admitted.Load() {
		t.Fatal("metadata admitted before actual pool retirement")
	}
	releaseReset()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("transport reset refused")
		}
	case <-ctx.Done():
		t.Fatal("reset did not finish")
	}
	if s.Reader().rdb.Load() == oldPool {
		t.Fatal("actual cache pool was not replaced")
	}
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.Qualified() || !admitted.Load() {
		t.Fatal("new transport remained unqualified")
	}
}

func TestVolumeSessionOverlappingTransportResetsStayClosed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, rdb, binding := volumeFixture(t)
	store := newVolumeTestStore()
	if err := SaveVolumeBinding(store, binding); err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: rdb.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	entered := make(chan int, 2)
	releases := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	release := func(i int) { once[i].Do(func() { close(releases[i]) }) }
	defer release(0)
	defer release(1)
	var calls atomic.Int32
	s.resetReader = func(r *Reader) bool {
		i := int(calls.Add(1)) - 1
		entered <- i
		<-releases[i]
		return r.ResetRedisConnections()
	}
	done := make(chan bool, 2)
	go func() { done <- s.TransportChanged() }()
	go func() { done <- s.TransportChanged() }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("overlapping resets not held")
		}
	}
	release(0)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("first reset not finished")
	}
	if err := s.Qualify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("one completion reopened while second reset held: %v", err)
	}
	if s.Qualified() {
		t.Fatal("one completion authorized remaining reset")
	}
	release(1)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("second reset not finished")
	}
	if err := s.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.Qualified() {
		t.Fatal("joined resets did not recover")
	}
}
