package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// ownerFacade is only published for a selected, productive serving-engine
// claim. The owner, rather than this process, holds Link and the FUSE manager.
type ownerFacade struct {
	mu                                        sync.Mutex
	intent                                    volumeOwnerIntent
	client                                    *volumeOwnerClient
	claimed                                   volumeOwnerReceipt
	transportSnapshot                         volumeOwnerReceipt
	token                                     string
	epoch, costSequence, offlineSequence      uint64
	costKnown, costExpensive, costConstrained bool
	offlineKnown, userOffline, autoOffline    bool
	costPending                               *ownerCostPending
	offlinePending                            *ownerOfflinePending
	policyCall                                func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error)
	// logicalRedisURL is the Redis endpoint this owner was claimed for
	// (before any Link rewrite); "" for a facade rebuilt for recovery.
	logicalRedisURL string
}

// linkRedisLoopback returns the owner's loopback Redis proxy address when
// the owner runs JuiceMount Link and was claimed for redisURL's endpoint:
// the route the mount's metadata traffic takes.
func (f *ownerFacade) linkRedisLoopback(redisURL string) (string, bool) {
	if f == nil {
		return "", false
	}
	f.mu.Lock()
	claimed, logical := f.claimed, f.logicalRedisURL
	f.mu.Unlock()
	if !claimed.LinkEnabled || logical == "" || !sameRedisEndpoint(logical, redisURL) {
		return "", false
	}
	opts, err := redisurl.Parse(claimed.RedisURL)
	if err != nil || !isLoopbackHostPort(opts.Addr) {
		return "", false
	}
	return opts.Addr, true
}

// enrollHTTP carries a single Manager request through the selected owner's
// retained Link. A lost owner reply is ambiguous, so callers must never
// repeat this request through another route in the same enrollment step.
func (f *ownerFacade) enrollHTTP(method, rawURL, body string) enrollHTTPAnswer {
	fail := func() enrollHTTPAnswer {
		return enrollHTTPAnswer{Route: "link", Error: "the selected owner Manager route is unavailable"}
	}
	if f == nil || f.client == nil {
		return fail()
	}
	f.mu.Lock()
	epoch, token, claimed, logical := f.epoch, f.token, f.claimed, f.logicalRedisURL
	f.mu.Unlock()
	if epoch == 0 || token == "" || !claimed.LinkEnabled ||
		enrollHTTPTargetAllowed(rawURL, logical) != "" {
		return fail()
	}
	ctx, cancel := context.WithTimeout(context.Background(), enrollHTTPTimeout+time.Second)
	defer cancel()
	resp, err := f.callPolicy(ctx, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: enrollHTTPMethod,
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root, token, method, rawURL, body},
		Ints:    []int{int(epoch)},
	})
	if err != nil || resp.Error != "" {
		return fail()
	}
	var r volumeOwnerReceipt
	if json.Unmarshal([]byte(resp.Text), &r) != nil || r.Instance != f.intent.Instance ||
		r.ConfigDigest != f.intent.ConfigDigest || r.IsolationRoot != f.intent.Root ||
		r.Build != version.Commit || r.AttachmentEpoch != epoch || r.AttachmentToken != token ||
		r.RedisURL != claimed.RedisURL || r.BucketURL != claimed.BucketURL ||
		!r.LinkEnabled || r.EnrollHTTP == nil || r.EnrollHTTP.Route != "link" ||
		len(r.EnrollHTTP.Body) > enrollHTTPMaxResponse {
		return fail()
	}
	return *r.EnrollHTTP
}

type ownerCostPending struct {
	expected               uint64
	expensive, constrained bool
}
type ownerOfflinePending struct {
	expected   uint64
	user, auto bool
}

// The per-facade call seam lets controls drop a reply after real dispatch
// without mutating a process-wide transport hook.
func (f *ownerFacade) callPolicy(ctx context.Context, req servicecontrol.Request) (servicecontrol.Response, error) {
	if f.policyCall != nil {
		return f.policyCall(ctx, f.client.path, req)
	}
	return servicecontrol.Call(ctx, f.client.path, req)
}

// fuseEvidence asks only the selected owner for its original child's Wait
// and complete local mount snapshot. An unavailable or changed attachment
// is unknown; callers must not substitute a process-title search.
func (f *ownerFacade) fuseEvidence() health.FUSESteadyEvidence {
	if f == nil || f.client == nil {
		return health.FUSESteadyEvidence{}
	}
	if !f.mu.TryLock() {
		return health.FUSESteadyEvidence{}
	}
	epoch, token, claimed := f.epoch, f.token, f.claimed
	f.mu.Unlock()
	if epoch == 0 || token == "" || claimed.MountPath == "" {
		return health.FUSESteadyEvidence{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := f.callPolicy(ctx, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "fuse-health",
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root, token},
		Ints:    []int{int(epoch)},
	})
	if err != nil || resp.Error != "" {
		return health.FUSESteadyEvidence{}
	}
	var r volumeOwnerReceipt
	if json.Unmarshal([]byte(resp.Text), &r) != nil || r.Instance != f.intent.Instance ||
		r.ConfigDigest != f.intent.ConfigDigest || r.IsolationRoot != f.intent.Root ||
		r.Build != version.Commit || r.AttachmentEpoch != epoch || r.AttachmentToken != token ||
		r.RedisURL != claimed.RedisURL || r.BucketURL != claimed.BucketURL ||
		r.MountPath != claimed.MountPath || r.FUSEEvidence == nil {
		return health.FUSESteadyEvidence{}
	}
	if !f.mu.TryLock() {
		return health.FUSESteadyEvidence{}
	}
	current := f.epoch == epoch && f.token == token && f.claimed.MountPath == claimed.MountPath &&
		f.claimed.RedisURL == claimed.RedisURL && f.claimed.BucketURL == claimed.BucketURL
	f.mu.Unlock()
	if !current || r.FUSEEvidence.Ready && (!r.FUSEEvidence.ChildProven || !r.FUSEEvidence.ChildAlive) {
		return health.FUSESteadyEvidence{}
	}
	return *r.FUSEEvidence
}

var selectServingOwner = selectServingOwnerImpl

func selectServingOwnerImpl(cfg ServerConfig) (*ownerFacade, error) {
	intent, err := selectVolumeOwner(cfg)
	if err != nil {
		return nil, err
	}
	return claimServingOwner(cfg, intent)
}

// Recovery never enters selectVolumeOwner: that path can create a successor
// after an authenticated retirement. Only the fixed original may be claimed.
func claimServingOwner(cfg ServerConfig, intent volumeOwnerIntent) (*ownerFacade, error) {
	path, err := ownerSocketPath()
	if err != nil {
		return nil, err
	}
	f := &ownerFacade{intent: intent, client: &volumeOwnerClient{
		path: path, instance: intent.Instance, digest: intent.ConfigDigest, root: intent.Root}}
	var pinned int64
	if ps, err := pin.Open(pinStorePath(cfg.DBPath)); err == nil {
		if stats, err := ps.AggregateStats(); err == nil {
			pinned = stats.TotalBytes
		}
		_ = ps.Close()
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return f, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := selectedOwnerOperation(ctx, path, intent, "claim", intent.ClaimID,
		[]string{string(raw)}, []int{int(pinned), int(netprofile.Default().Class())})
	if err != nil {
		return f, fmt.Errorf("selected owner claim retained; original ID %s: %w", intent.ClaimID, err)
	}
	if r.MountPath == "" || r.RedisURL == "" || r.JuiceFSBin == "" || r.MetricsAddr == "" {
		return f, errors.New("selected owner claim lacks effective FUSE or proxy configuration")
	}
	f.client.redis, f.client.bucket = r.RedisURL, r.BucketURL
	f.claimed = r
	f.logicalRedisURL = cfg.RedisURL
	f.transportSnapshot = r
	f.token, err = randomOwnerID()
	if err != nil {
		return f, err
	}
	attached, err := f.attach()
	if err != nil {
		return f, err
	}
	f.epoch, f.costSequence, f.offlineSequence = attached.AttachmentEpoch, attached.CostSequence, attached.OfflineSequence
	f.costKnown, f.costExpensive, f.costConstrained = attached.CostSequence != 0, attached.CostExpensive, attached.CostConstrained
	f.offlineKnown, f.userOffline, f.autoOffline = attached.OfflineSequence != 0, attached.UserOffline, attached.AutoOffline
	return f, nil
}

func (f *ownerFacade) attach() (volumeOwnerReceipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	before, err := f.inspect()
	if err != nil {
		return before, err
	}
	resp, err := servicecontrol.Call(ctx, f.client.path, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "attach",
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root, f.token},
		Ints:    []int{int(before.AttachmentEpoch)}})
	if err != nil {
		// A lost reply can follow the original attachment. Inspect its token;
		// never rebase the stale expected epoch into another request.
		got, inspectErr := f.inspect()
		if inspectErr == nil && got.AttachmentToken == f.token {
			return got, nil
		}
		return volumeOwnerReceipt{}, err
	}
	if resp.Error != "" {
		return volumeOwnerReceipt{}, errors.New(resp.Error)
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return r, err
	}
	if r.Instance != f.intent.Instance || r.ConfigDigest != f.intent.ConfigDigest ||
		r.IsolationRoot != f.intent.Root || r.Build != version.Commit || r.AttachmentEpoch == 0 || r.AttachmentToken != f.token {
		return r, errors.New("selected owner attachment changed identity")
	}
	return r, nil
}

func (f *ownerFacade) policyReceipt() (volumeOwnerReceipt, error) {
	r, err := f.readReceipt() // caller holds f.mu; readReceipt never takes it
	if err != nil {
		return r, err
	}
	if r.AttachmentEpoch != f.epoch || r.AttachmentToken != f.token {
		return r, errors.New("selected owner attachment superseded")
	}
	return r, nil
}

func (f *ownerFacade) reconcileCostPending(replay bool) error {
	p := f.costPending
	if p == nil {
		return nil
	}
	r, err := f.policyReceipt()
	if err != nil {
		return err
	}
	if r.CostSequence == p.expected && replay {
		e, c := 0, 0
		if p.expensive {
			e = 1
		}
		if p.constrained {
			c = 1
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, sendErr := f.callPolicy(ctx, servicecontrol.Request{
			Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "cost",
			Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root},
			Ints:    []int{e, c, int(f.epoch), int(p.expected)}})
		cancel()
		// The delayed original may win the CAS while this identical replay is
		// in transit. Inspect the single resulting publication either way.
		r, err = f.policyReceipt()
		if err != nil {
			return err
		}
		if r.CostSequence == p.expected && sendErr == nil && resp.Error != "" {
			return errors.New(resp.Error)
		}
	}
	if r.CostSequence == p.expected {
		return errors.New("original owner cost update remains unconfirmed")
	}
	if r.CostSequence != p.expected+1 || r.CostExpensive != p.expensive || r.CostConstrained != p.constrained {
		return errors.New("original owner cost sequence or payload was superseded")
	}
	f.costSequence, f.costKnown = r.CostSequence, true
	f.costExpensive, f.costConstrained = p.expensive, p.constrained
	f.costPending = nil
	return nil
}

func (f *ownerFacade) setCost(expensive, constrained bool) error {
	if f == nil {
		return errors.New("selected owner unavailable")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.reconcileCostPending(true); err != nil {
		return err
	}
	if f.costKnown && f.costExpensive == expensive && f.costConstrained == constrained {
		return nil
	}
	p := &ownerCostPending{expected: f.costSequence, expensive: expensive, constrained: constrained}
	f.costPending = p // retain original payload before the socket can lose its reply
	e, c := 0, 0
	if expensive {
		e = 1
	}
	if constrained {
		c = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := f.callPolicy(ctx, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "cost",
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root},
		Ints:    []int{e, c, int(f.epoch), int(p.expected)}})
	cancel()
	if err != nil {
		return f.reconcileCostPending(false)
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return err
	}
	if r.Instance != f.intent.Instance || r.ConfigDigest != f.intent.ConfigDigest || r.Build != version.Commit ||
		r.IsolationRoot != f.intent.Root || r.AttachmentEpoch != f.epoch || r.AttachmentToken != f.token ||
		r.CostSequence != p.expected+1 || r.CostExpensive != expensive || r.CostConstrained != constrained {
		return errors.New("selected owner cost response changed identity, sequence, or payload")
	}
	f.costSequence, f.costKnown = r.CostSequence, true
	f.costExpensive, f.costConstrained = expensive, constrained
	f.costPending = nil
	return nil
}

func (f *ownerFacade) reconcileOfflinePending(replay bool) error {
	p := f.offlinePending
	if p == nil {
		return nil
	}
	r, err := f.policyReceipt()
	if err != nil {
		return err
	}
	if r.OfflineSequence == p.expected && replay {
		u, a := 0, 0
		if p.user {
			u = 1
		}
		if p.auto {
			a = 1
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, sendErr := f.callPolicy(ctx, servicecontrol.Request{
			Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "offline",
			Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root},
			Ints:    []int{u, a, int(f.epoch), int(p.expected)}})
		cancel()
		r, err = f.policyReceipt()
		if err != nil {
			return err
		}
		if r.OfflineSequence == p.expected && sendErr == nil && resp.Error != "" {
			return errors.New(resp.Error)
		}
	}
	if r.OfflineSequence == p.expected {
		return errors.New("original owner offline update remains unconfirmed")
	}
	if r.OfflineSequence != p.expected+1 || r.UserOffline != p.user || r.AutoOffline != p.auto {
		return errors.New("original owner offline sequence or payload was superseded")
	}
	f.offlineSequence, f.offlineKnown = r.OfflineSequence, true
	f.userOffline, f.autoOffline = p.user, p.auto
	f.offlinePending = nil
	return nil
}

func (f *ownerFacade) syncOffline() error {
	if f == nil {
		return errors.New("selected owner unavailable")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.reconcileOfflinePending(true); err != nil {
		return err
	}
	user, auto := pin.IsUserOffline(), pin.IsAutoOffline()
	if f.offlineKnown && f.userOffline == user && f.autoOffline == auto {
		return nil
	}
	p := &ownerOfflinePending{expected: f.offlineSequence, user: user, auto: auto}
	f.offlinePending = p
	u, a := 0, 0
	if user {
		u = 1
	}
	if auto {
		a = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := f.callPolicy(ctx, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "offline",
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest, f.intent.Root},
		Ints:    []int{u, a, int(f.epoch), int(p.expected)}})
	cancel()
	if err != nil {
		return f.reconcileOfflinePending(false)
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return err
	}
	if r.Instance != f.intent.Instance || r.ConfigDigest != f.intent.ConfigDigest || r.Build != version.Commit ||
		r.IsolationRoot != f.intent.Root || r.AttachmentEpoch != f.epoch || r.AttachmentToken != f.token ||
		r.OfflineSequence != p.expected+1 || r.UserOffline != user || r.AutoOffline != auto {
		return errors.New("selected owner offline response changed identity, sequence, or payload")
	}
	f.offlineSequence, f.offlineKnown = r.OfflineSequence, true
	f.userOffline, f.autoOffline = user, auto
	f.offlinePending = nil
	return nil
}

func (f *ownerFacade) activate(inline bool) (volumeOwnerReceipt, error) {
	if f == nil {
		return volumeOwnerReceipt{}, errors.New("selected owner unavailable")
	}
	mode := 0
	if inline {
		mode = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := selectedOwnerOperation(ctx, f.client.path, f.intent, "mount", f.intent.MountID, nil, []int{mode})
	return r, err
}

func (f *ownerFacade) startupProbe() (volumeOwnerReceipt, error) {
	if f == nil {
		return volumeOwnerReceipt{}, errors.New("selected owner unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := selectedOwnerOperation(ctx, f.client.path, f.intent, "probe-startup", f.intent.StartupProbeID, nil, nil)
	return r, err
}

func (f *ownerFacade) readReceipt() (volumeOwnerReceipt, error) {
	if f == nil {
		return volumeOwnerReceipt{}, errors.New("selected owner unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := f.callPolicy(ctx, servicecontrol.Request{
		Version: servicecontrol.ProtocolVersion, Build: version.Commit, Method: "inspect",
		Strings: []string{f.intent.Instance, f.intent.ConfigDigest}})
	if err != nil {
		return volumeOwnerReceipt{}, err
	}
	if resp.Error != "" {
		return volumeOwnerReceipt{}, errors.New(resp.Error)
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return r, err
	}
	if r.Instance != f.intent.Instance || r.ConfigDigest != f.intent.ConfigDigest || r.IsolationRoot != f.intent.Root || r.Build != version.Commit {
		return r, errors.New("selected owner inspection changed identity")
	}
	return r, nil
}

func (f *ownerFacade) inspect() (volumeOwnerReceipt, error) {
	r, err := f.readReceipt()
	if err != nil {
		return r, err
	}
	f.mu.Lock()
	f.transportSnapshot = r
	f.mu.Unlock()
	return r, nil
}

func (f *ownerFacade) transportStatus() (string, uint64, uint64) {
	if f == nil {
		return "system-direct", 0, 0
	}
	f.mu.Lock()
	r := f.transportSnapshot
	f.mu.Unlock()
	if !r.LinkEnabled {
		return "system-direct", 0, 0
	}
	if r.TransportMode == "" {
		return "selected-owner-unknown", 0, 0
	}
	return r.TransportMode, r.DirectLANConnections, r.EncryptedConnections
}

func (f *ownerFacade) stop(id string) (volumeOwnerReceipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	return selectedOwnerOperation(ctx, f.client.path, f.intent, "stop", id, nil, nil)
}

func cachePolicyFromOwner(r volumeOwnerReceipt) {
	mb, err := strconv.ParseInt(r.CacheSizeMB, 10, 64)
	if err != nil || mb <= 0 || mb > (1<<63-1)>>20 || !r.CachePolicyKnown ||
		r.CacheFreeFloorBytes <= 0 || r.CacheVolumePath == "" || r.CacheVolumeDev == 0 {
		pin.SetCachePolicy(0, 0, "", 0, false)
		return
	}
	pin.SetCachePolicy(mb<<20, r.CacheFreeFloorBytes, r.CacheVolumePath, r.CacheVolumeDev, true)
}

// The owner publishes only its latest completed remount generation. The
// replaceable handler applies that version locally and joins its callback
// before closing any handler/store dependencies.
type ownerCallbackLoop struct {
	stopCh chan struct{}
	done   chan struct{}
	once   sync.Once
}

func startOwnerCallbackLoop(f *ownerFacade, apply func() bool, transport func(string)) *ownerCallbackLoop {
	l := &ownerCallbackLoop{stopCh: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(l.done)
		var applied uint64
		var transportApplied uint64
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-l.stopCh:
				return
			default:
			}
			_ = f.syncOffline() // a refused update never promotes a local recovery verdict
			r, err := f.inspect()
			if err == nil && r.TransportVersion > transportApplied {
				transport(r.TransportMode)
				transportApplied = r.TransportVersion
			}
			if err == nil && r.RemountVersion > applied {
				cachePolicyFromOwner(r)
				if apply() {
					applied = r.RemountVersion
				}
			}
			select {
			case <-l.stopCh:
				return
			case <-ticker.C:
			}
		}
	}()
	return l
}

func (l *ownerCallbackLoop) stopWithin(deadline time.Duration) bool {
	if l == nil {
		return true
	}
	l.once.Do(func() { close(l.stopCh) })
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case <-l.done:
		return true
	case <-timer.C:
		return false
	}
}
