package cache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrReaderUnavailable = errors.New("direct SSD reader is unavailable")

// SessionConfig carries the saved selector separately from runtime connection
// options: loopback proxy ports must not change an offline library's identity.
// Callbacks must not call back into Session. They run under its publication
// lock, so qualification/loss/Stop are ordered with metadata admission.
type SessionConfig struct {
	Store       BindingStore
	LogicalURL  string
	Selection   []string
	Options     *redis.Options
	StateDir    string
	JuiceFSBin  string
	OnReader    func(*Reader)
	OnQualified func(bool)
	// A true status with ErrReaderUnavailable means the backend is qualified
	// while unsupported local acceleration remains detached.
	OnStatus func(bool, error)
	// VerifyReader is normally Reader.Verify; tests may isolate coordinator
	// behavior from the host's installed JuiceFS executable.
	VerifyReader func(*Reader) error
}

// Session owns one selected library's cache and its reconnect qualification.
// It never closes the metadata store. The server must retire its reads before
// calling Stop, then close metadata only after Stop has joined actual work.
type Session struct {
	mu                sync.Mutex
	qualificationSlot chan struct{}
	cfg               SessionConfig
	binding           VolumeBinding
	reader            *Reader
	readerError       error
	transportResets   int
	resetReader       func(*Reader) bool
	probe             *redis.Client
	online            bool
	qualified         bool
	identityRefused   bool
	closed            bool
	epoch             uint64
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	kick              chan struct{}
	startOnce         sync.Once
	stopOnce          sync.Once
}

func NewSession(cfg SessionConfig) (*Session, error) {
	if cfg.Store == nil || cfg.Options == nil || !filepath.IsAbs(cfg.StateDir) {
		return nil, ErrVolumeBinding
	}
	binding, exists, err := LoadVolumeBinding(cfg.Store, cfg.LogicalURL, cfg.Selection...)
	if err != nil {
		return nil, err
	}
	if cfg.VerifyReader == nil {
		cfg.VerifyReader = func(r *Reader) error {
			if cfg.JuiceFSBin == "" {
				return r.Verify()
			}
			return r.VerifyWithBinary(context.Background(), cfg.JuiceFSBin)
		}
	}
	// Freeze configuration; callers may reuse their original options/slice.
	opts := *cfg.Options
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	opts.Dialer = nil
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	cfg.Options = &opts
	cfg.Selection = append([]string(nil), cfg.Selection...)
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{resetReader: (*Reader).ResetRedisConnections, cfg: cfg, binding: binding, probe: redis.NewClient(&opts), ctx: ctx, cancel: cancel, kick: make(chan struct{}, 1), qualificationSlot: make(chan struct{}, 1)}
	if exists {
		s.reader, err = s.makeReader(binding)
		if errors.Is(err, ErrReaderUnavailable) {
			s.readerError = err
		} else if err != nil {
			cancel()
			_ = s.probe.Close()
			return nil, err
		}
	}
	if cfg.OnQualified != nil {
		cfg.OnQualified(false)
	}
	return s, nil
}

func (s *Session) makeReader(binding VolumeBinding) (*Reader, error) {
	opts := *s.cfg.Options
	client := redis.NewClient(&opts)
	r, err := NewVolumeReader(binding, client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	r.SetRemoteAvailable(false)
	if err = s.cfg.VerifyReader(r); err != nil {
		r.Stop()
		return nil, fmt.Errorf("%w: %v", ErrReaderUnavailable, err)
	}
	if err = r.SetMappingSnapshotPath(r.MappingSnapshotPath(s.cfg.StateDir)); err != nil {
		// A qualified filename should never contain a different identity.
		// Preserve it for diagnosis rather than silently replacing it.
		if errors.Is(err, ErrMappingIdentity) {
			r.Stop()
			return nil, err
		}
		// Malformed disposable mappings were removed by the Reader. Its new
		// working set still persists and will be rebuilt from checked Redis.
	}
	return r, nil
}

// SnapshotBinding returns a saved selected volume even when transport or the
// optional direct Reader is unavailable. Positive identity refusal and Stop
// revoke admission under the same publication lock as qualification.
func (s *Session) SnapshotBinding() (VolumeBinding, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binding, !s.closed && !s.identityRefused && s.binding.valid()
}

// SnapshotRefused distinguishes known wrong/stopped scope from an ordinary
// offline or not-yet-qualified saved binding. It performs no external work.
func (s *Session) SnapshotRefused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed || s.identityRefused
}

func (s *Session) Reader() *Reader    { s.mu.Lock(); defer s.mu.Unlock(); return s.reader }
func (s *Session) ReaderError() error { s.mu.Lock(); defer s.mu.Unlock(); return s.readerError }
func (s *Session) Qualified() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.online && s.qualified
}

// SetOnline(false) invalidates an in-flight qualification and remote mapping
// fetches, while keeping qualified local mappings available for offline reads.
func (s *Session) SetOnline(online bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if s.online != online || !online {
		s.epoch++
		s.online = online
		s.qualified = false
		if s.reader != nil {
			s.reader.SetRemoteAvailable(false)
		}
		if s.cfg.OnQualified != nil {
			s.cfg.OnQualified(false)
		}
	}
	s.mu.Unlock()
	if online {
		s.Kick()
	}
}

func (s *Session) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Qualify is also used synchronously before boot metadata sync. Every publish
// checks the outage epoch and actual cancellation; a late success cannot undo
// a later disconnect or Stop. No Redis or filesystem I/O holds s.mu.
func (s *Session) Qualify(caller context.Context) error {
	s.mu.Lock()
	if s.closed || !s.online || s.transportResets != 0 {
		s.mu.Unlock()
		return context.Canceled
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	ctx, cancel := context.WithTimeout(caller, 20*time.Second)
	stopCancellation := context.AfterFunc(s.ctx, cancel)
	defer stopCancellation()
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return context.Canceled
	case s.qualificationSlot <- struct{}{}:
	}
	defer func() { <-s.qualificationSlot }()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	if s.closed || !s.online || s.transportResets != 0 {
		s.mu.Unlock()
		return context.Canceled
	}
	epoch, binding, reader, probe := s.epoch, s.binding, s.reader, s.probe
	s.mu.Unlock()
	live, err := readVolumeBindingWithDeadline(ctx, probe, s.cfg.LogicalURL, false, s.cfg.Selection...)
	if err == nil && binding != (VolumeBinding{}) && binding != live {
		err = ErrVolumeBinding
	}
	var created *Reader
	var readerError error
	if err == nil && reader == nil {
		created, err = s.makeReader(live)
		if errors.Is(err, ErrReaderUnavailable) {
			readerError, err = err, nil
		}
		reader = created
	}
	if err == nil && reader != nil {
		_, _, err = reader.revalidateMappingsForSession(ctx)
	}
	discardMappings := false
	if errors.Is(err, ErrSliceMapping) {
		// A malformed slice list is disposable cache metadata. It may only
		// be discarded after a fresh positive proof of this same volume.
		err = reader.validateVolumeWithDeadline(ctx)
		discardMappings = err == nil
	}
	if err == nil && ctx.Err() == nil {
		// SQL persistence belongs to this joined qualification, but never
		// holds the publication lock or blocks SetOnline(false).
		err = SaveVolumeBinding(s.cfg.Store, live)
	}
	s.mu.Lock()
	if s.closed || !s.online || s.transportResets != 0 || s.epoch != epoch || ctx.Err() != nil {
		s.mu.Unlock()
		if created != nil {
			created.Stop()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return context.Canceled
	}
	if err != nil {
		if errors.Is(err, ErrVolumeBinding) {
			s.identityRefused = true
		}
		s.qualified = false
		if s.reader != nil {
			s.reader.SetRemoteAvailable(false)
		}
		if s.cfg.OnQualified != nil {
			s.cfg.OnQualified(false)
		}
		if s.cfg.OnStatus != nil {
			s.cfg.OnStatus(false, err)
		}
		s.mu.Unlock()
		if created != nil {
			created.Stop()
		}
		return err
	}
	if discardMappings {
		reader.InvalidateAll()
	}
	s.binding = live
	s.identityRefused = false
	if created != nil {
		s.reader = created
		if s.cfg.OnReader != nil {
			s.cfg.OnReader(created)
		}
	}
	if s.reader != nil {
		s.reader.SetRemoteAvailable(true)
	}
	s.readerError = readerError
	s.qualified = true
	if s.cfg.OnQualified != nil {
		s.cfg.OnQualified(true)
	}
	if s.cfg.OnStatus != nil {
		s.cfg.OnStatus(true, readerError)
	}
	s.mu.Unlock()
	return nil
}

// TransportChanged retires both cache-owned pools after a verified Link
// handoff. Cached local bytes remain readable; remote admission reopens only
// after qualification on the new pools. Stop joins the actual close/reset work.
func (s *Session) TransportChanged() bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.wg.Add(1)
	s.transportResets++
	s.epoch++
	s.qualified = false
	old, reader := s.probe, s.reader
	s.probe = redis.NewClient(s.cfg.Options)
	if reader != nil {
		reader.SetRemoteAvailable(false)
	}
	if s.cfg.OnQualified != nil {
		s.cfg.OnQualified(false)
	}
	s.mu.Unlock()
	defer s.wg.Done()
	_ = old.Close()
	if reader != nil {
		s.resetReader(reader)
	}
	s.mu.Lock()
	s.transportResets--
	s.mu.Unlock()
	s.Kick()
	return true
}

// Start retries failed qualification while TCP reachability stays true. A
// one-shot reachability edge is insufficient after a transient Redis error.
func (s *Session) Start() {
	s.startOnce.Do(func() {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-s.kick:
				case <-ticker.C:
				}
				s.mu.Lock()
				needs := !s.closed && s.online && !s.qualified
				s.mu.Unlock()
				if needs {
					_ = s.Qualify(s.ctx)
				}
			}
		}()
	})
}

func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.epoch++
		s.qualified = false
		if s.reader != nil {
			s.reader.SetRemoteAvailable(false)
		}
		if s.cfg.OnQualified != nil {
			s.cfg.OnQualified(false)
		}
		s.cancel()
		s.mu.Unlock()
		s.wg.Wait()
		if s.reader != nil {
			s.reader.Stop()
		}
		_ = s.probe.Close()
	})
}

// RemoteObservation fences remote proof work without holding Session.mu during
// network or SQLite I/O. Already-qualified local reads do not use this epoch.
func (s *Session) RemoteObservation() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epoch, !s.closed && s.online && s.qualified && s.transportResets == 0
}

func (s *Session) MatchesRemoteObservation(epoch uint64) bool {
	current, ready := s.RemoteObservation()
	return ready && current == epoch
}

// TryMatchRemoteObservation is a nonblocking check for callers that already
// hold metadata publication locks. Session callbacks can wait for those locks;
// waiting for Session.mu here would invert that order. Contention refuses the
// observation without changing the session or weakening its identity checks.
func (s *Session) TryMatchRemoteObservation(epoch uint64, binding VolumeBinding, reader *Reader) bool {
	if s == nil || !s.mu.TryLock() {
		return false
	}
	defer s.mu.Unlock()
	return !s.closed && s.online && s.qualified && !s.identityRefused && s.transportResets == 0 &&
		s.epoch == epoch && s.binding == binding && s.reader == reader && reader != nil
}

// RefuseIdentity is called when a request positively detects a different
// volume. TCP reachability cannot clear it; only successful qualification can.
func (s *Session) RefuseIdentity(err error) {
	if !errors.Is(err, ErrVolumeBinding) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.epoch++
	s.qualified = false
	s.identityRefused = true
	if s.reader != nil {
		s.reader.SetRemoteAvailable(false)
	}
	if s.cfg.OnQualified != nil {
		s.cfg.OnQualified(false)
	}
	if s.cfg.OnStatus != nil {
		s.cfg.OnStatus(false, err)
	}
}
