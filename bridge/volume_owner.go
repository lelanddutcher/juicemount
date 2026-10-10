package main

// This process owns the original FUSE manager and Link listeners. Its fixed
// operations are independent of a caller's socket: losing a reply never
// cancels a mount or licenses a replacement owner.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

type volumeOwnerClient struct {
	path, instance, digest, root, redis, bucket string
}

// Attach requires a selected owner instance. A caller may use inspect for
// discovery, but discovery alone never authorizes Mount, Stop, or adoption.
func attachVolumeOwner(ctx context.Context, path, instance, requestedDigest, isolationRoot, redis, bucket string) (*volumeOwnerClient, volumeOwnerReceipt, error) {
	if instance == "" || requestedDigest == "" || isolationRoot == "" {
		return nil, volumeOwnerReceipt{}, errors.New("missing selected volume owner identity")
	}
	resp, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "inspect", Strings: []string{instance, requestedDigest}})
	if err != nil {
		return nil, volumeOwnerReceipt{}, err
	}
	if resp.Error != "" {
		return nil, volumeOwnerReceipt{}, errors.New(resp.Error)
	}
	var receipt volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &receipt); err != nil {
		return nil, volumeOwnerReceipt{}, err
	}
	if receipt.Build != version.Commit || receipt.Instance != instance || receipt.ConfigDigest != requestedDigest ||
		receipt.IsolationRoot != isolationRoot || receipt.RedisURL != redis || receipt.BucketURL != bucket {
		return nil, volumeOwnerReceipt{}, errors.New("selected owner build, configuration, isolation, or effective endpoints changed")
	}
	return &volumeOwnerClient{path: path, instance: instance, digest: requestedDigest,
		root: isolationRoot, redis: redis, bucket: bucket}, receipt, nil
}

func (c *volumeOwnerClient) call(ctx context.Context, method, id string) (volumeOwnerReceipt, error) {
	if c == nil || id == "" {
		return volumeOwnerReceipt{}, errors.New("missing original owner operation")
	}
	resp, err := servicecontrol.Call(ctx, c.path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: method, Strings: []string{c.instance, id, c.digest}})
	if err != nil {
		return volumeOwnerReceipt{}, err
	} // ambiguous: query this ID, never replay with a new ID
	var receipt volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &receipt); err != nil {
		return volumeOwnerReceipt{}, err
	}
	if receipt.Instance != c.instance || receipt.ConfigDigest != c.digest || receipt.IsolationRoot != c.root ||
		receipt.RedisURL != c.redis || receipt.BucketURL != c.bucket || receipt.Build != version.Commit || receipt.Operation != id {
		return volumeOwnerReceipt{}, errors.New("original owner operation response identity changed")
	}
	if resp.Error != "" {
		return receipt, errors.New(resp.Error)
	}
	return receipt, nil
}

func (c *volumeOwnerClient) probe(ctx context.Context) (volumeOwnerReceipt, error) {
	if c == nil || ctx == nil {
		return volumeOwnerReceipt{}, errors.New("missing selected owner probe")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return volumeOwnerReceipt{}, errors.New("owner probe requires deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return volumeOwnerReceipt{}, context.DeadlineExceeded
	}
	ms := int(remaining.Milliseconds())
	if ms > 10000 {
		ms = 10000
	}
	if ms < 1 {
		ms = 1
	}
	resp, err := servicecontrol.Call(ctx, c.path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
		Build: version.Commit, Method: "probe", Strings: []string{c.instance, c.digest, c.root}, Ints: []int{ms}})
	if err != nil {
		return volumeOwnerReceipt{}, err
	}
	var r volumeOwnerReceipt
	if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
		return volumeOwnerReceipt{}, err
	}
	if r.Instance != c.instance || r.ConfigDigest != c.digest || r.IsolationRoot != c.root ||
		r.RedisURL != c.redis || r.BucketURL != c.bucket || r.Build != version.Commit {
		return volumeOwnerReceipt{}, errors.New("selected owner probe identity changed")
	}
	if resp.Error != "" {
		return r, errors.New(resp.Error)
	}
	if !r.BackendUp || r.ProbeRTTNanos <= 0 {
		return r, errors.New("owner Link did not prove remote Redis")
	}
	return r, nil
}

// probe-startup is retained under one original ID because its encrypted bulk
// benchmark can outlive a caller deadline. A later engine queries that ID.
func (c *volumeOwnerClient) probeStartup(ctx context.Context, id string) (volumeOwnerReceipt, error) {
	return c.call(ctx, "probe-startup", id)
}

// selectedOwnerOperation discovers the original ID before sending anything.
// A reconnect may have different pins or reachability; those values must not
// rebind an already accepted claim or MountID.
func selectedOwnerOperation(ctx context.Context, path string, intent volumeOwnerIntent, method, id string, extra []string, ints []int) (volumeOwnerReceipt, error) {
	query := func() (volumeOwnerReceipt, bool, error) {
		resp, err := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "query", Strings: []string{intent.Instance, id, intent.ConfigDigest}})
		if err != nil {
			return volumeOwnerReceipt{}, false, err
		}
		if resp.Error == "unknown original volume owner operation" {
			return volumeOwnerReceipt{}, false, nil
		}
		var r volumeOwnerReceipt
		if err := json.Unmarshal([]byte(resp.Text), &r); err != nil {
			return r, false, err
		}
		if r.Instance != intent.Instance || r.Build != version.Commit || r.ConfigDigest != intent.ConfigDigest ||
			r.IsolationRoot != intent.Root || r.Operation != id {
			return r, false, errors.New("original owner operation changed identity")
		}
		if resp.Error != "" {
			return r, true, errors.New(resp.Error)
		}
		if r.OperationState == "error" {
			return r, true, errors.New("original owner operation failed")
		}
		return r, true, nil
	}
	first, found, err := query()
	if err != nil {
		return first, err
	}
	if found && first.OperationState == "complete" {
		return first, nil
	}
	strings := []string{intent.Instance, id}
	if method == "claim" {
		strings = append(strings, extra...)
	} else {
		strings = append(strings, intent.ConfigDigest)
	}
	if !found {
		resp, sendErr := servicecontrol.Call(ctx, path, servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: method, Strings: strings, Ints: ints})
		if sendErr == nil {
			if resp.Error != "" {
				return volumeOwnerReceipt{}, errors.New(resp.Error)
			}
			var sent volumeOwnerReceipt
			if err := json.Unmarshal([]byte(resp.Text), &sent); err != nil {
				return sent, err
			}
			if sent.Instance != intent.Instance || sent.Build != version.Commit || sent.ConfigDigest != intent.ConfigDigest ||
				sent.IsolationRoot != intent.Root || sent.Operation != id {
				return sent, errors.New("selected owner operation response identity changed")
			}
			if sent.OperationState == "complete" {
				return sent, nil
			}
		}
	}
	// An ambiguous transport result can follow accepted physical work. Query
	// the original ID; never submit another payload under it.
	for {
		got, exists, err := query()
		if err != nil {
			if exists && got.OperationState == "error" {
				// A definitive failure carries its typed receipt (StopResult).
				return got, err
			}
			return volumeOwnerReceipt{}, fmt.Errorf("original %s outcome unknown; retain owner: %w", method, err)
		}
		if !exists {
			return volumeOwnerReceipt{}, errors.New("original owner operation not admitted; retain selected owner")
		}
		if got.OperationState == "complete" {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return got, fmt.Errorf("original %s remains pending; retain owner: %w", method, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

type volumeOwnerReceipt struct {
	Instance             string                       `json:"instance"`
	Build                string                       `json:"build"`
	IsolationRoot        string                       `json:"isolation_root"`
	ConfigDigest         string                       `json:"config_digest"`
	Operation            string                       `json:"operation,omitempty"`
	OperationState       string                       `json:"operation_state,omitempty"`
	MountPath            string                       `json:"mount_path,omitempty"`
	RedisURL             string                       `json:"redis_url,omitempty"`
	BucketURL            string                       `json:"bucket_url,omitempty"`
	JuiceFSBin           string                       `json:"juicefs_bin,omitempty"`
	LinkEnabled          bool                         `json:"link_enabled,omitempty"`
	CacheSizeMB          string                       `json:"cache_size_mb,omitempty"`
	CacheFreeFloorBytes  int64                        `json:"cache_free_floor_bytes"`
	CacheVolumePath      string                       `json:"cache_volume_path"`
	CacheVolumeDev       uint64                       `json:"cache_volume_dev"`
	CachePolicyKnown     bool                         `json:"cache_policy_known"`
	MetricsAddr          string                       `json:"metrics_addr,omitempty"`
	Attempted            bool                         `json:"attempted"`
	RemountVersion       uint64                       `json:"remount_version"`
	TransportVersion     uint64                       `json:"transport_version,omitempty"`
	TransportMode        string                       `json:"transport_mode,omitempty"`
	DirectLANConnections uint64                       `json:"direct_lan_connections,omitempty"`
	EncryptedConnections uint64                       `json:"encrypted_connections,omitempty"`
	StopResult           string                       `json:"stop_result,omitempty"`
	BackendUp            bool                         `json:"backend_up,omitempty"`
	ProbeRTTMillis       int64                        `json:"probe_rtt_ms,omitempty"`
	ProbeRTTNanos        int64                        `json:"probe_rtt_ns,omitempty"`
	UploadMbps           float64                      `json:"upload_mbps,omitempty"`
	DownloadMbps         float64                      `json:"download_mbps,omitempty"`
	CostSequence         uint64                       `json:"cost_sequence,omitempty"`
	AttachmentEpoch      uint64                       `json:"attachment_epoch,omitempty"`
	AttachmentToken      string                       `json:"attachment_token,omitempty"`
	CostExpensive        bool                         `json:"cost_expensive,omitempty"`
	CostConstrained      bool                         `json:"cost_constrained,omitempty"`
	OfflineSequence      uint64                       `json:"offline_sequence,omitempty"`
	UserOffline          bool                         `json:"user_offline,omitempty"`
	AutoOffline          bool                         `json:"auto_offline,omitempty"`
	EnrollHTTP           *enrollHTTPAnswer            `json:"enroll_http,omitempty"`
	FUSEEvidence         *health.FUSESteadyEvidence   `json:"fuse_evidence,omitempty"`
	Operations           []volumeOwnerOperationStatus `json:"operations,omitempty"`
}

type volumeOwnerOperationStatus struct {
	ID, Method, State string
}

type volumeOwnerOperation struct {
	method, payload string
	done            chan struct{}
	result          volumeOwnerReceipt
	err             error
}

type ownerSnapshotSource interface {
	OwnerSnapshot() health.FUSEOwnerSnapshot
}

type ownerManagedFUSE interface {
	ownerSnapshotSource
	Mount() error
	StartMonitor()
	Stop() health.FUSEStopResult
}

type ownerSteadyFUSE interface {
	SteadyEvidence() health.FUSESteadyEvidence
}

type volumeOwner struct {
	mu                   sync.Mutex
	mutationMu           sync.Mutex
	instance             string
	root                 string
	config               string // SHA-256 of requested config, before runtime proxy rewrites
	claimDigest          string // selected immutable config while the original claim is pending
	intentBound          bool   // production claims require a pre-birth durable selection
	intent               volumeOwnerIntent
	logical              string
	endpoint             string
	bucket               string
	juiceBin             string
	manager              ownerManagedFUSE
	status               ownerSnapshotSource // original manager in production; sampled outside o.mu
	link                 *jmnfs.LinkNode
	releaseLink          func() // closes the original Link only after physical FUSE Stop completes
	linkReleasePending   bool
	probeActive          int
	enrollActive         int         // accepted HTTP work retains Link through Stop
	enrollDial           contextDial // test transport; production uses the retained node
	probeWork            func(*jmnfs.LinkNode, string, time.Duration, bool) (time.Duration, float64, float64, error)
	snapshot             health.FUSEOwnerSnapshot
	version              uint64
	transportVersion     uint64
	transportMode        string
	directLANConnections uint64
	encryptedConnections uint64
	costSequence         uint64
	attachmentEpoch      uint64
	attachmentToken      string
	costExpensive        bool
	costConstrained      bool
	offlineSequence      uint64
	userOffline          bool
	autoOffline          bool
	retired              bool
	terminalRequested    bool
	stopRequested        bool
	mountActive          bool
	ops                  map[string]*volumeOwnerOperation
}

func newVolumeOwner(root string) (*volumeOwner, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	return &volumeOwner{instance: hex.EncodeToString(nonce[:]), root: root,
		ops: make(map[string]*volumeOwnerOperation)}, nil
}

func ownerSocketPath() (string, error) {
	service, err := servicecontrol.SocketPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(service), "o.sock"), nil
}

func (o *volumeOwner) receiptLocked() volumeOwnerReceipt {
	digest := o.config
	if digest == "" {
		digest = o.claimDigest
	}
	r := volumeOwnerReceipt{Instance: o.instance, Build: version.Commit,
		IsolationRoot: o.root, ConfigDigest: digest, RedisURL: o.endpoint,
		BucketURL: o.bucket, JuiceFSBin: o.juiceBin, LinkEnabled: o.link != nil,
		RemountVersion: o.version, TransportVersion: o.transportVersion, TransportMode: o.transportMode,
		DirectLANConnections: o.directLANConnections, EncryptedConnections: o.encryptedConnections,
		MountPath: o.snapshot.MountPath, CacheSizeMB: o.snapshot.CacheSizeMB,
		CacheFreeFloorBytes: o.snapshot.CacheFreeFloorBytes,
		CacheVolumePath:     o.snapshot.CacheVolumePath, CacheVolumeDev: o.snapshot.CacheVolumeDev,
		CachePolicyKnown: o.snapshot.CachePolicyKnown,
		MetricsAddr:      o.snapshot.MetricsAddr, Attempted: o.snapshot.Attempted,
		CostSequence: o.costSequence, AttachmentEpoch: o.attachmentEpoch, AttachmentToken: o.attachmentToken,
		CostExpensive: o.costExpensive, CostConstrained: o.costConstrained,
		OfflineSequence: o.offlineSequence, UserOffline: o.userOffline, AutoOffline: o.autoOffline}
	for id, op := range o.ops {
		state := "pending"
		select {
		case <-op.done:
			if op.err == nil {
				state = "complete"
			} else {
				state = "error"
			}
		default:
		}
		r.Operations = append(r.Operations, volumeOwnerOperationStatus{ID: id, Method: op.method, State: state})
	}
	return r
}

func (o *volumeOwner) publishSnapshot(source ownerSnapshotSource, remount bool) {
	if source == nil {
		return
	}
	snapshot := source.OwnerSnapshot() // may wait for fm.mu; never hold o.mu here
	o.mu.Lock()
	if o.status != source || o.stopRequested || o.retired {
		o.mu.Unlock()
		return
	}
	o.snapshot = snapshot
	if remount {
		o.version++
	}
	o.mu.Unlock()
}

func (o *volumeOwner) publishTransport(node *jmnfs.LinkNode, mode string) {
	status := node.ProxyTransportStatus()
	o.mu.Lock()
	if o.link == node && !o.stopRequested && !o.retired {
		o.transportMode = mode
		o.directLANConnections, o.encryptedConnections = status.DirectLANConns, status.LinkConns
		o.transportVersion++
	}
	o.mu.Unlock()
}

func (o *volumeOwner) inspect(instance, digest string) (volumeOwnerReceipt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	selected := o.config
	if selected == "" {
		selected = o.claimDigest
	}
	if instance != "" && instance != o.instance || digest != "" && digest != selected {
		return volumeOwnerReceipt{}, errors.New("different volume owner instance or requested configuration")
	}
	return o.receiptLocked(), nil
}

// fuseHealth reads the original manager without holding the owner lock over
// its bounded local mount snapshot. Both lock acquisitions are nonblocking;
// a concurrent lifecycle transition makes the observation unproven.
func (o *volumeOwner) fuseHealth(values []string, epoch int) (volumeOwnerReceipt, error) {
	if !o.mu.TryLock() {
		return volumeOwnerReceipt{}, errors.New("selected owner health busy")
	}
	selected := values[0] == o.instance && values[1] == o.config && values[2] == o.root &&
		values[3] == o.attachmentToken && epoch == int(o.attachmentEpoch) && o.attachmentEpoch != 0 &&
		o.config != "" && o.snapshot.MountPath != "" && !o.stopRequested && !o.retired && !o.terminalRequested
	manager, source := o.manager, o.snapshot.MountPath
	o.mu.Unlock()
	probe, ok := manager.(ownerSteadyFUSE)
	if !selected || !ok {
		return volumeOwnerReceipt{}, errors.New("selected owner health identity or source unavailable")
	}
	evidence := probe.SteadyEvidence()
	if !o.mu.TryLock() {
		return volumeOwnerReceipt{}, errors.New("selected owner health changed during observation")
	}
	if o.manager != manager || o.snapshot.MountPath != source || o.stopRequested || o.retired || o.terminalRequested ||
		values[0] != o.instance || values[1] != o.config || values[2] != o.root ||
		values[3] != o.attachmentToken || epoch != int(o.attachmentEpoch) {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("selected owner health changed during observation")
	}
	r := o.receiptLocked()
	o.mu.Unlock()
	r.FUSEEvidence = &evidence
	return r, nil
}

// A completed or pending operation is forever identified by its original ID
// and exact payload. Query never executes it a second time.
func (o *volumeOwner) begin(method, id, payload string, work func() (volumeOwnerReceipt, error)) (volumeOwnerReceipt, error) {
	if len(id) != 32 {
		return volumeOwnerReceipt{}, errors.New("invalid volume owner operation ID")
	}
	decoded, decodeErr := hex.DecodeString(id)
	if decodeErr != nil || hex.EncodeToString(decoded) != id {
		return volumeOwnerReceipt{}, errors.New("invalid volume owner operation ID")
	}
	o.mu.Lock()
	if prior := o.ops[id]; prior != nil {
		if prior.method != method || prior.payload != payload {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, errors.New("volume owner operation ID rebound")
		}
		select {
		case <-prior.done:
			r, err := prior.result, prior.err
			o.mu.Unlock()
			return r, err
		default:
			r := o.receiptLocked()
			r.Operation, r.OperationState = id, "pending"
			o.mu.Unlock()
			return r, nil
		}
	}
	if o.terminalRequested {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("terminal owner shutdown admitted; new mutation refused")
	}
	if (method == "claim" || method == "mount") && (o.stopRequested || o.retired) {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("new owner work refused after Stop admission")
	}
	for _, prior := range o.ops {
		if prior.method != method {
			continue
		}
		if method == "claim" || method == "mount" || method == "probe-startup" {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, errors.New("original owner claim, mount, or startup probe ID already selected")
		}
		if method == "stop" {
			select {
			case <-prior.done:
				if prior.err == nil {
					o.mu.Unlock()
					return volumeOwnerReceipt{}, errors.New("original volume already stopped")
				}
			default:
				o.mu.Unlock()
				return volumeOwnerReceipt{}, errors.New("original stop pending; query its ID")
			}
		}
	}
	if len(o.ops) >= 128 {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("volume owner operation table full; original operations retained")
	}
	if method == "claim" {
		canonical, _, found := strings.Cut(payload, "\x00")
		if !found || canonical == "" {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, errors.New("invalid selected claim payload")
		}
		var requested ServerConfig
		if err := json.Unmarshal([]byte(canonical), &requested); err != nil {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, fmt.Errorf("invalid selected claim configuration: %w", err)
		}
		selected, err := servingConfigDigest(requested)
		if err != nil {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, err
		}
		if o.claimDigest != "" && o.claimDigest != selected || o.intentBound && o.intent.ConfigDigest != selected {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, errors.New("different pending owner claim")
		}
		o.claimDigest = selected
	}
	if method == "stop" {
		o.stopRequested = true
	}
	op := &volumeOwnerOperation{method: method, payload: payload, done: make(chan struct{})}
	o.ops[id] = op
	o.mu.Unlock()
	go func() {
		r, err := work()
		o.mu.Lock()
		if r.Instance == "" {
			published := o.receiptLocked()
			r.Instance, r.Build, r.IsolationRoot = published.Instance, published.Build, published.IsolationRoot
			r.ConfigDigest, r.RedisURL, r.BucketURL = published.ConfigDigest, published.RedisURL, published.BucketURL
			r.JuiceFSBin, r.LinkEnabled = published.JuiceFSBin, published.LinkEnabled
			if r.MountPath == "" {
				r.MountPath = published.MountPath
			}
			if r.CacheSizeMB == "" {
				r.CacheSizeMB = published.CacheSizeMB
			}
			if !r.CachePolicyKnown {
				r.CacheFreeFloorBytes, r.CacheVolumePath = published.CacheFreeFloorBytes, published.CacheVolumePath
				r.CacheVolumeDev, r.CachePolicyKnown = published.CacheVolumeDev, published.CachePolicyKnown
			}
			if r.MetricsAddr == "" {
				r.MetricsAddr = published.MetricsAddr
			}
			r.Attempted, r.RemountVersion = published.Attempted, published.RemountVersion
		}
		r.Operation = id
		if err != nil {
			r.OperationState = "error"
		} else {
			r.OperationState = "complete"
		}
		op.result, op.err = r, err
		close(op.done)
		o.mu.Unlock()
	}()
	r, _ := o.inspect("", "")
	r.Operation, r.OperationState = id, "pending"
	return r, nil
}

func (o *volumeOwner) query(instance, id, digest string) (volumeOwnerReceipt, error) {
	if _, err := o.inspect(instance, digest); err != nil {
		return volumeOwnerReceipt{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	op := o.ops[id]
	if op == nil {
		return volumeOwnerReceipt{}, errors.New("unknown original volume owner operation")
	}
	select {
	case <-op.done:
		return op.result, op.err
	default:
		r := o.receiptLocked()
		r.Operation, r.OperationState = id, "pending"
		return r, nil
	}
}

func (o *volumeOwner) claim(raw string, pinnedBytes int64, policy netprofile.LinkClass) (volumeOwnerReceipt, error) {
	if !o.intentBound {
		return volumeOwnerReceipt{}, errors.New("owner claim requires pre-birth durable selection")
	}
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return volumeOwnerReceipt{}, err
	}
	if err := validateIsolatedServerConfig(cfg); err != nil {
		return volumeOwnerReceipt{}, err
	}
	if pinnedBytes < 0 || policy < netprofile.ClassMetered || policy > netprofile.ClassFast {
		return volumeOwnerReceipt{}, errors.New("invalid owner cache or network policy")
	}
	o.mutationMu.Lock()
	defer o.mutationMu.Unlock()
	want, err := servingConfigDigest(cfg)
	if err != nil {
		return volumeOwnerReceipt{}, err
	}
	o.mu.Lock()
	if o.retired || o.stopRequested {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("volume owner stop admitted; no new claim")
	}
	if o.intentBound && want != o.intent.ConfigDigest {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("selected owner configuration differs from pre-birth intent")
	}
	if o.config != "" {
		if o.config != want || o.logical != cfg.RedisURL {
			o.mu.Unlock()
			return volumeOwnerReceipt{}, errors.New("different volume owner configuration while resources retained")
		}
		r := o.receiptLocked()
		o.mu.Unlock()
		return r, nil
	}
	o.mu.Unlock()
	// A measured class is not a host OS cost signal. The explicit versioned
	// cost operation below conveys expensive/constrained independently.
	_ = policy
	cfg.RedisURL, _ = redisURLsAtConfigIngest(cfg.RedisURL)
	logical := cfg.RedisURL
	node, owned, err := startLinkIfConfigured(&cfg)
	if err != nil {
		return volumeOwnerReceipt{}, err
	}
	if bin, binErr := ownerLaunchJuiceFS(cfg.JuiceFSBin); binErr != nil {
		if owned && node != nil {
			stopLinkNode(node)
		}
		return volumeOwnerReceipt{}, binErr
	} else {
		cfg.JuiceFSBin = bin
	}
	privatePaths, _, pathErr := appisolation.ResolvePaths()
	if pathErr != nil {
		if owned && node != nil {
			stopLinkNode(node)
		}
		return volumeOwnerReceipt{}, pathErr
	}
	fm, err := health.NewGenerationFUSEManager(health.FUSEConfig{
		JuiceFSBin: cfg.JuiceFSBin, RedisURL: cfg.RedisURL, MountPoint: cfg.FUSEPath,
		CacheSize: cfg.CacheSize, CacheDir: privatePaths.CacheBase, FreeSpaceRatio: "0.01", BucketOverride: cfg.BucketOverride,
		PinnedBytes: pinnedBytes, FUSEMetricsAddr: cfg.FUSEMetricsAddr, OpenCacheTTL: cfg.OpenCacheTTL,
	})
	if err != nil {
		if owned && node != nil {
			stopLinkNode(node)
		}
		return volumeOwnerReceipt{}, err
	}
	fm.SetOnRemount(func() { o.publishSnapshot(fm, true) })
	snapshot := fm.OwnerSnapshot()
	o.mu.Lock()
	o.config, o.logical, o.endpoint, o.bucket = want, logical, cfg.RedisURL, cfg.BucketOverride
	o.juiceBin = cfg.JuiceFSBin
	o.manager, o.status, o.link = fm, fm, node
	if node != nil {
		o.releaseLink = func() { stopLinkNode(node) }
	}
	o.snapshot = snapshot
	if node != nil {
		transport := node.ProxyTransportStatus()
		o.transportMode = transport.Mode
		o.directLANConnections, o.encryptedConnections = transport.DirectLANConns, transport.LinkConns
		o.transportVersion++
	}
	r := o.receiptLocked()
	stopRequested := o.stopRequested
	o.mu.Unlock()
	if node != nil {
		node.SetOnProxyTransportChanged(func(mode string) { o.publishTransport(node, mode) })
	}
	if stopRequested {
		return r, errors.New("stop admitted during claim; reserved original resources retained")
	}
	return r, nil
}

// mount is the one selected activation. A deferred boot reserves the original
// generation and starts its monitor without asking the offline backend to
// launch JuiceFS synchronously. The choice is bound to MountID by begin.
func (o *volumeOwner) mount() (volumeOwnerReceipt, error) { return o.mountWithMode(true) }

func (o *volumeOwner) mountWithMode(inline bool) (volumeOwnerReceipt, error) {
	o.mu.Lock()
	fm := o.manager
	source := o.status
	if o.stopRequested || o.retired || o.mountActive {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("owner mount refused after stop or while original mount active")
	}
	o.mountActive = true
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.mountActive = false; o.mu.Unlock() }()
	if fm == nil {
		return volumeOwnerReceipt{}, errors.New("volume owner has no reserved FUSE manager")
	}
	var err error
	if inline {
		err = fm.Mount()
	}
	o.mu.Lock()
	stillCurrent := o.manager == fm && !o.stopRequested && !o.retired
	o.mu.Unlock()
	if stillCurrent {
		// FUSEManager's stopping fence prevents a monitor started after Stop.
		fm.StartMonitor()
	}
	// A Stop may have completed while Mount returned. Never overwrite its
	// terminal publication with this older operation's manager snapshot.
	o.mu.Lock()
	stillCurrent = o.manager == fm && !o.stopRequested && !o.retired
	o.mu.Unlock()
	if stillCurrent {
		o.publishSnapshot(source, false)
	}
	o.mu.Lock()
	r := o.receiptLocked()
	stopRequested := o.stopRequested
	stillCurrent = o.manager == fm && !stopRequested && !o.retired
	o.mu.Unlock()
	if !stillCurrent {
		return r, errors.New("original mount settled after owner stop; terminal state retained")
	}
	if err == nil && inline && !stopRequested {
		if mb, parseErr := strconv.ParseInt(r.CacheSizeMB, 10, 64); parseErr == nil && mb > 0 {
			pin.SetCacheBudgetBytes(mb << 20)
		}
	}
	return r, err
}

func (o *volumeOwner) stop() (volumeOwnerReceipt, error) {
	o.mu.Lock()
	o.stopRequested = true // admission fence before any physical Stop work
	fm := o.manager
	o.mu.Unlock()
	if fm == nil {
		o.mu.Lock()
		defer o.mu.Unlock()
		r := o.receiptLocked()
		if o.manager != nil || o.config != "" || o.status != nil || o.link != nil || o.releaseLink != nil ||
			o.linkReleasePending || o.mountActive || o.probeActive != 0 || o.enrollActive != 0 {
			return r, errors.New("original claim or owner resources still retained; stop retained for retry")
		}
		for _, op := range o.ops {
			if op.method == "stop" {
				continue
			}
			select {
			case <-op.done:
			default:
				return r, errors.New("original owner operation still active; stop retained for retry")
			}
		}
		o.retired = true
		r.StopResult = (health.FUSEStopResult{State: health.FUSEStopped}).String()
		return r, nil
	}
	result := fm.Stop()
	o.mu.Lock()
	r := o.receiptLocked()
	r.StopResult = result.String()
	if !result.Complete() || o.mountActive || o.probeActive != 0 || o.enrollActive != 0 || o.manager != fm {
		o.mu.Unlock()
		return r, fmt.Errorf("original FUSE stop or Link work incomplete (%s); Link listeners retained", r.StopResult)
	}
	release := o.releaseLink
	o.linkReleasePending = release != nil
	o.manager, o.status, o.link = nil, nil, nil
	o.releaseLink = nil
	o.retired = true
	o.mu.Unlock()
	if release != nil {
		release()
		o.mu.Lock()
		o.linkReleasePending = false
		o.mu.Unlock()
	}
	return r, nil
}

// shutdown is a terminal admission decision. It never tears down live
// resources: the original Stop must have physically completed first.
func (o *volumeOwner) shutdown() (volumeOwnerReceipt, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.manager != nil || o.link != nil || o.releaseLink != nil || o.linkReleasePending || o.mountActive || o.probeActive != 0 || o.enrollActive != 0 {
		return o.receiptLocked(), errors.New("original FUSE manager or Link still retained")
	}
	for _, op := range o.ops {
		if op.method == "shutdown" {
			continue
		}
		select {
		case <-op.done:
		default:
			return o.receiptLocked(), errors.New("original owner operation still active")
		}
	}
	o.terminalRequested = true
	return o.receiptLocked(), nil
}

// probe executes the real RESP PING through the retained Link, never a
// loopback connect to the proxy. Startup additionally verifies bulk traffic.
func (o *volumeOwner) probe(timeout time.Duration, startup bool) (volumeOwnerReceipt, error) {
	o.mu.Lock()
	node, logical := o.link, o.logical
	r := o.receiptLocked()
	if node == nil || logical == "" || o.stopRequested || o.retired {
		o.mu.Unlock()
		return r, errors.New("selected owner has no available retained Link probe")
	}
	o.probeActive++
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.probeActive--; o.mu.Unlock() }()
	work := o.probeWork
	if work == nil {
		work = ownerLinkProbe
	}
	rtt, up, down, err := work(node, logical, timeout, startup)
	if err != nil {
		return r, err
	}
	o.mu.Lock()
	current := o.link == node && o.logical == logical && !o.stopRequested && !o.retired
	o.mu.Unlock()
	if !current {
		return r, errors.New("owner stopped during original Link proof")
	}
	netprofile.Default().ObserveRTT(rtt)
	if startup && down > 0 {
		observeLinkDataPlaneDownload(netprofile.Default(), linkDataPlaneBenchmarkBytes, down)
	}
	r.BackendUp, r.ProbeRTTMillis, r.ProbeRTTNanos = true, rtt.Milliseconds(), int64(rtt)
	r.UploadMbps, r.DownloadMbps = up, down
	return r, nil
}

func ownerLinkProbe(node *jmnfs.LinkNode, logical string, timeout time.Duration, startup bool) (time.Duration, float64, float64, error) {
	rtt, err := linkRedisPingRTT(node, logical, timeout)
	if err != nil {
		return 0, 0, 0, err
	}
	if !startup {
		return rtt, 0, 0, nil
	}
	up, down, err := linkRedisDataPlaneBenchmark(node, logical, linkDataPlaneBenchmarkBytes, 15*time.Second)
	return rtt, up, down, err
}

// enrollHTTP admits one fixed Manager request under the current attachment.
// Admission and the active count share o.mu with Stop's release decision.
func (o *volumeOwner) enrollHTTP(strings []string, epoch int) (volumeOwnerReceipt, error) {
	o.mu.Lock()
	selected := strings[0] == o.instance && strings[1] == o.config && strings[2] == o.root &&
		o.config != "" && strings[3] == o.attachmentToken && epoch == int(o.attachmentEpoch) &&
		o.attachmentEpoch != 0 && !o.stopRequested && !o.retired && !o.terminalRequested
	node, logical, dial := o.link, o.logical, o.enrollDial
	if !selected || node == nil || logical == "" {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("selected owner Link attachment or lifecycle changed")
	}
	method, rawURL, body := strings[4], strings[5], strings[6]
	if method != "GET" && method != "POST" || method == "GET" && body != "" || len(body) > enrollHTTPMaxRequest ||
		enrollHTTPTargetAllowed(rawURL, logical) != "" {
		o.mu.Unlock()
		return volumeOwnerReceipt{}, errors.New("selected owner Manager request refused")
	}
	o.enrollActive++
	o.mu.Unlock()
	defer func() { o.mu.Lock(); o.enrollActive--; o.mu.Unlock() }()
	if dial == nil {
		dial = node.AdaptiveDialContext
	}
	a := sendEnrollHTTP(method, rawURL, body, "link", dial)
	o.mu.Lock()
	r := o.receiptLocked()
	o.mu.Unlock()
	r.EnrollHTTP = &a
	return r, nil
}

func (o *volumeOwner) dispatch(req servicecontrol.Request) servicecontrol.Response {
	resp := servicecontrol.Response{Version: servicecontrol.ProtocolVersion}
	if req.Version != servicecontrol.ProtocolVersion || req.Build != version.Commit {
		resp.Error = "volume owner build/protocol mismatch"
		return resp
	}
	var r volumeOwnerReceipt
	var err error
	switch req.Method {
	case "inspect":
		if len(req.Strings) != 2 || len(req.Ints) != 0 {
			err = errors.New("invalid inspect shape")
			break
		}
		r, err = o.inspect(req.Strings[0], req.Strings[1])
	case "fuse-health":
		if len(req.Strings) != 4 || len(req.Ints) != 1 {
			err = errors.New("invalid selected owner health shape")
			break
		}
		r, err = o.fuseHealth(req.Strings, req.Ints[0])
	case "claim":
		if len(req.Strings) != 3 || len(req.Ints) != 2 {
			err = errors.New("invalid claim shape")
			break
		}
		if req.Strings[0] == "" {
			err = errors.New("missing selected owner instance")
			break
		}
		if o.intentBound && req.Strings[1] != o.intent.ClaimID {
			err = errors.New("claim original ID differs from pre-birth intent")
			break
		}
		if _, err = o.inspect(req.Strings[0], ""); err != nil {
			break
		}
		var requested ServerConfig
		if err = json.Unmarshal([]byte(req.Strings[2]), &requested); err != nil {
			break
		}
		var canonical []byte
		canonical, err = json.Marshal(canonicalServerConfig(requested))
		if err != nil {
			break
		}
		policy := netprofile.LinkClass(req.Ints[1])
		payload := string(canonical) + "\x00" + strconv.Itoa(req.Ints[0]) + "\x00" + strconv.Itoa(req.Ints[1])
		r, err = o.begin("claim", req.Strings[1], payload, func() (volumeOwnerReceipt, error) {
			return o.claim(string(canonical), int64(req.Ints[0]), policy)
		})
	case "mount", "stop":
		if len(req.Strings) != 3 || (req.Method == "stop" && len(req.Ints) != 0) ||
			(req.Method == "mount" && (len(req.Ints) > 1 || (len(req.Ints) == 1 && req.Ints[0] != 0 && req.Ints[0] != 1))) {
			err = errors.New("invalid mutation shape")
			break
		}
		if req.Strings[0] == "" || req.Strings[2] == "" {
			err = errors.New("missing selected owner identity")
			break
		}
		if req.Method == "mount" && o.intentBound && req.Strings[1] != o.intent.MountID {
			err = errors.New("mount original ID differs from pre-birth intent")
			break
		}
		if _, err = o.inspect(req.Strings[0], req.Strings[2]); err != nil {
			break
		}
		method := req.Method
		payload := req.Strings[2]
		inline := true
		if method == "mount" {
			if len(req.Ints) == 1 {
				inline = req.Ints[0] == 1
			}
			payload += "\x00" + strconv.FormatBool(inline)
		}
		r, err = o.begin(method, req.Strings[1], payload, func() (volumeOwnerReceipt, error) {
			if method == "mount" {
				return o.mountWithMode(inline)
			}
			return o.stop()
		})
	case "shutdown":
		if len(req.Strings) != 3 || len(req.Ints) != 0 || req.Strings[0] == "" {
			err = errors.New("invalid selected shutdown shape")
			break
		}
		if o.intentBound && req.Strings[1] != ownerShutdownID(o.intent) {
			err = errors.New("shutdown ID differs from selected owner terminal identity")
			break
		}
		o.mu.Lock()
		selected := o.config
		if selected == "" {
			selected = o.claimDigest
		}
		authorized := req.Strings[0] == o.instance && req.Strings[2] == selected
		o.mu.Unlock()
		if !authorized {
			err = errors.New("different volume owner instance or requested configuration")
			break
		}
		r, err = o.begin("shutdown", req.Strings[1], req.Strings[2], o.shutdown)
		if err == nil {
			o.mu.Lock()
			op := o.ops[req.Strings[1]]
			o.mu.Unlock()
			<-op.done // bounded in-memory eligibility check; original ID is retained
			r, err = o.query(o.instance, req.Strings[1], req.Strings[2])
		}
	case "probe":
		if len(req.Strings) != 3 || len(req.Ints) != 1 || req.Strings[0] == "" || req.Strings[1] == "" ||
			req.Ints[0] < 1 || req.Ints[0] > 10000 {
			err = errors.New("invalid selected Link probe shape")
			break
		}
		if _, err = o.inspect(req.Strings[0], req.Strings[1]); err != nil {
			break
		}
		o.mu.Lock()
		selected := o.config != "" && req.Strings[1] == o.config && req.Strings[2] == o.root && !o.retired && !o.stopRequested
		o.mu.Unlock()
		if !selected {
			err = errors.New("Link probe identity or owner state changed")
			break
		}
		r, err = o.probe(time.Duration(req.Ints[0])*time.Millisecond, false)
	case "probe-startup":
		if len(req.Strings) != 3 || len(req.Ints) != 0 || req.Strings[0] == "" || req.Strings[2] == "" {
			err = errors.New("invalid selected startup probe shape")
			break
		}
		if o.intentBound && req.Strings[1] != o.intent.StartupProbeID {
			err = errors.New("startup probe original ID differs from pre-birth intent")
			break
		}
		o.mu.Lock()
		selected := req.Strings[0] == o.instance && req.Strings[2] == o.config && o.config != "" && !o.stopRequested && !o.retired
		o.mu.Unlock()
		if !selected {
			err = errors.New("startup Link probe identity changed")
			break
		}
		r, err = o.begin("probe-startup", req.Strings[1], req.Strings[2], func() (volumeOwnerReceipt, error) {
			return o.probe(1500*time.Millisecond, true)
		})
	case enrollHTTPMethod:
		if len(req.Strings) != 7 || len(req.Ints) != 1 {
			err = errors.New("invalid selected owner Manager request shape")
			break
		}
		r, err = o.enrollHTTP(req.Strings, req.Ints[0])
	case "cost":
		if len(req.Strings) != 3 || req.Strings[0] == "" || req.Strings[1] == "" || len(req.Ints) != 4 ||
			(req.Ints[0] != 0 && req.Ints[0] != 1) || (req.Ints[1] != 0 && req.Ints[1] != 1) {
			err = errors.New("invalid owner network cost shape")
			break
		}
		if _, err = o.inspect(req.Strings[0], req.Strings[1]); err != nil {
			break
		}
		o.mu.Lock()
		if req.Strings[2] != o.root || o.config == "" || o.stopRequested || o.retired ||
			req.Ints[2] != int(o.attachmentEpoch) || req.Ints[3] != int(o.costSequence) {
			o.mu.Unlock()
			err = errors.New("owner cost attachment or sequence changed")
			break
		}
		exp, con := req.Ints[0] == 1, req.Ints[1] == 1
		if o.costSequence == 0 || o.costExpensive != exp || o.costConstrained != con {
			if class, costly := networkCostClass(exp, con); costly {
				netprofile.Default().SetNetworkCostClass(&class)
			} else {
				netprofile.Default().SetNetworkCostClass(nil)
			}
			netprofile.Default().ResetForTransportChange()
			o.costExpensive, o.costConstrained = exp, con
			o.costSequence++
		}
		r = o.receiptLocked()
		o.mu.Unlock()
	case "attach":
		if len(req.Strings) != 4 || len(req.Ints) != 1 || req.Strings[0] == "" || req.Strings[1] == "" || len(req.Strings[3]) != 32 {
			err = errors.New("invalid selected owner attachment")
			break
		}
		if _, err = o.inspect(req.Strings[0], req.Strings[1]); err != nil {
			break
		}
		o.mu.Lock()
		if req.Strings[2] != o.root || o.config == "" || o.stopRequested || o.retired {
			o.mu.Unlock()
			err = errors.New("owner attachment identity or lifecycle changed")
			break
		}
		if req.Strings[3] != o.attachmentToken {
			if req.Ints[0] != int(o.attachmentEpoch) {
				o.mu.Unlock()
				err = errors.New("stale owner attachment epoch")
				break
			}
			o.attachmentEpoch++
			o.attachmentToken = req.Strings[3]
		}
		r = o.receiptLocked()
		o.mu.Unlock()
	case "offline":
		if len(req.Strings) != 3 || req.Strings[0] == "" || req.Strings[1] == "" || len(req.Ints) != 4 ||
			(req.Ints[0] != 0 && req.Ints[0] != 1) || (req.Ints[1] != 0 && req.Ints[1] != 1) {
			err = errors.New("invalid owner offline state shape")
			break
		}
		if _, err = o.inspect(req.Strings[0], req.Strings[1]); err != nil {
			break
		}
		o.mu.Lock()
		if req.Strings[2] != o.root || o.config == "" || o.stopRequested || o.retired ||
			req.Ints[2] != int(o.attachmentEpoch) || req.Ints[3] != int(o.offlineSequence) {
			o.mu.Unlock()
			err = errors.New("owner offline attachment or sequence changed")
			break
		}
		user, auto := req.Ints[0] == 1, req.Ints[1] == 1
		if o.offlineSequence == 0 || o.userOffline != user || o.autoOffline != auto {
			// The owner does not configure a persistence path. The serving
			// engine remains the single writer of user offline intent.
			pin.SetOffline(user)
			pin.SetAutoOffline(auto, "serving engine offline state")
			o.userOffline, o.autoOffline = user, auto
			o.offlineSequence++
		}
		r = o.receiptLocked()
		o.mu.Unlock()
	case "query":
		if len(req.Strings) != 3 || len(req.Ints) != 0 {
			err = errors.New("invalid query shape")
			break
		}
		r, err = o.query(req.Strings[0], req.Strings[1], req.Strings[2])
	default:
		err = errors.New("unsupported volume owner operation")
	}
	if err != nil {
		resp.Error = err.Error()
	}
	if data, encodeErr := json.Marshal(r); encodeErr == nil {
		resp.Text = string(data)
	} else if err == nil {
		resp.Error = encodeErr.Error()
	}
	return resp
}

func runVolumeOwnerProcess() error {
	if serviceClientBuild {
		return errors.New("desktop control archive cannot own volume resources")
	}
	if os.Geteuid() == 0 {
		return errors.New("volume owner must not run as root")
	}
	scope, _, err := selectedOwnerScope()
	if err != nil {
		return err
	}
	path, err := ownerSocketPath()
	if err != nil {
		return err
	}
	owner, err := newVolumeOwner(scope.Root)
	if err != nil {
		return err
	}
	bootstrapFD := os.Getenv("JM_VOLUME_OWNER_BOOTSTRAP_FD")
	bootstrap, bootstrapErr := originalOwnerBootstrap(bootstrapFD)
	if bootstrapErr != nil {
		return bootstrapErr
	}
	intent, bound, bindErr := authorizeOriginalOwner(scope, bootstrapFD, bootstrap)
	if bindErr != nil {
		return bindErr
	}
	if bound {
		owner.instance, owner.claimDigest, owner.intentBound, owner.intent = intent.Instance, intent.ConfigDigest, true, intent
	}
	var lifetime *ownerLifetime
	if bound {
		lifetime, err = acquireOwnerLifetime(scope)
		if err != nil {
			return err
		}
		defer lifetime.close()
		current, err := readOwnerIntent(scope)
		if err != nil || current != intent {
			return errors.New("owner intent changed before lifetime admission")
		}
		if _, err := readOwnerRetirement(scope, intent); !errors.Is(err, os.ErrNotExist) {
			return errors.New("selected owner already has a retirement record or uncertain terminal state")
		}
	}
	server, err := servicecontrol.Listen(path)
	if err != nil {
		return fmt.Errorf("volume owner already active or unavailable: %w", err)
	}
	defer server.Close()
	shutdownClose := make(chan error, 1)
	err = server.ServeWithReplyHook(owner.dispatch, func(req servicecontrol.Request, resp servicecontrol.Response, writeErr error) {
		if req.Method == "shutdown" && resp.Error == "" {
			closeErr := server.Close() // only after the original reply was written
			select {
			case shutdownClose <- closeErr:
			default:
			}
		}
	})
	// Serve ending does not join the last reply hook or a concurrently admitted
	// handler. Close admission and join both before inspecting owner operations
	// or the hook's terminal notification. Otherwise an accepted claim could
	// appear empty, or a successful shutdown could be mistaken for a lost reply.
	closeErr := server.Close()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 15*time.Second)
	joinErr := server.WaitClosed(joinCtx)
	joinCancel()
	owner.mu.Lock()
	live := owner.manager != nil || owner.link != nil || owner.releaseLink != nil || owner.linkReleasePending
	terminal := owner.terminalRequested
	for _, op := range owner.ops {
		select {
		case <-op.done:
		default:
			live = true
		}
	}
	owner.mu.Unlock()
	if terminal && !live && err == nil && closeErr == nil && joinErr == nil {
		select {
		case replyCloseErr := <-shutdownClose:
			if replyCloseErr == nil {
				if lifetime != nil {
					// The original terminal decision is irrevocable even if its
					// reply was lost. Retain the fence until this same publication
					// is durable; never repeat physical Stop on a sync failure.
					for lifetime.publishRetired(intent, ownerShutdownID(intent)) != nil {
						time.Sleep(time.Second)
					}
				}
				return nil
			}
		default:
			// Every reply hook is joined, so absence is evidence, not a race.
		}
	}
	live = live || terminal || closeErr != nil || joinErr != nil
	if live {
		// An owner control failure cannot transfer the original waiter or
		// release its proxy ports. Keep the process and all resources held.
		for {
			time.Sleep(time.Hour)
		}
	}
	return err
}

// Explicit owner mode is separate from the serving engine. The engine's
// production caller must be routed before this becomes a recovery path.
func volumeOwnerMode(args []string) bool { return len(args) == 1 && args[0] == "--volume-owner" }
