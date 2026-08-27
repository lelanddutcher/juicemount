package nfs

// Presence (Tier-1 #3): "who has this file open" across every Mac client.
//
// Each JuiceMount client's NFS handler already funnels every write-handle
// open/close through incActiveWriter/decActiveWriter — the same choke points
// the phantom-purge gate uses. This tracker mirrors those transitions into
// Redis so ALL clients (and the NAS-side manager) see one live presence view:
//
//	jum:presence:<host>  HASH { <mount-path> : <firstOpenUnix> }  EXPIRE 45s
//
// A 15s refresher re-EXPIREs the key while any file is open, so a crashed
// client's entries self-expire within ~45s — no explicit offline message
// needed. v1 tracks WRITE opens only: NLE project files are the
// collaboration signal that matters (Premiere/Resolve hold project handles
// for the whole session), and the choke point is already exact. Read/QL
// presence can ride the same structure later via the fdPool open/close
// paths.

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/redis/go-redis/v9"
)

// presenceStore abstracts the four Redis operations presence needs so the
// tracker logic is unit-testable without a live server.
type presenceStore interface {
	hset(ctx context.Context, key, field string, val string) error
	hdel(ctx context.Context, key string, fields ...string) error
	expire(ctx context.Context, key string, ttl time.Duration) error
	scanKeys(ctx context.Context, pattern string) ([]string, error)
	hgetall(ctx context.Context, key string) (map[string]string, error)
}

type redisPresenceStore struct{ rdb *redis.Client }

func (r *redisPresenceStore) hset(ctx context.Context, key, field, val string) error {
	return r.rdb.HSet(ctx, key, field, val).Err()
}
func (r *redisPresenceStore) hdel(ctx context.Context, key string, fields ...string) error {
	return r.rdb.HDel(ctx, key, fields...).Err()
}
func (r *redisPresenceStore) expire(ctx context.Context, key string, ttl time.Duration) error {
	return r.rdb.Expire(ctx, key, ttl).Err()
}
func (r *redisPresenceStore) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	return r.rdb.Keys(ctx, pattern).Result()
}
func (r *redisPresenceStore) hgetall(ctx context.Context, key string) (map[string]string, error) {
	return r.rdb.HGetAll(ctx, key).Result()
}

const (
	presenceKeyPrefix = "jum:presence:"
	presenceTTL       = 45 * time.Second
	presenceRefresh   = 15 * time.Second
	presenceWriteWait = 750 * time.Millisecond
	presenceQueueSize = 256
)

type presenceUpdate struct {
	path  string
	since int64
	open  bool
}

// PresenceTracker mirrors write-handle open/close into Redis and keeps the
// local snapshot in memory for zero-cost local reads.
type PresenceTracker struct {
	host  string
	store presenceStore

	mu    sync.Mutex
	opens map[string]int64 // mount-path -> firstOpenUnix

	updates chan presenceUpdate
	stopCh  chan struct{}
	once    sync.Once
}

// NewPresenceTracker builds a tracker for this host. Nil-safe construction is
// the caller's job (handler only wires it when a Redis client exists).
func NewPresenceTracker(rdb *redis.Client) *PresenceTracker {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-mac"
	}
	return newPresenceTracker(host, &redisPresenceStore{rdb: rdb})
}

func newPresenceTracker(host string, store presenceStore) *PresenceTracker {
	p := &PresenceTracker{
		host:    host,
		store:   store,
		opens:   make(map[string]int64),
		updates: make(chan presenceUpdate, presenceQueueSize),
		stopCh:  make(chan struct{}),
	}
	go p.updateLoop()
	go p.refreshLoop()
	return p
}

// Open records a write-handle open for path. Fire-and-forget: presence is
// best-effort telemetry and must never add latency to the write hot path. The
// local snapshot changes synchronously; Redis work goes through a bounded
// background queue and is skipped while offline. A saturated queue may drop
// telemetry, but Redis TTL expiry removes stale presence and file custody never
// waits behind it.
func (p *PresenceTracker) Open(path string) {
	if p == nil {
		return
	}
	now := time.Now().Unix()
	p.mu.Lock()
	p.opens[path] = now
	p.mu.Unlock()
	p.enqueue(presenceUpdate{path: path, since: now, open: true})
}

// Close records a write-handle close. Same non-blocking contract as Open.
func (p *PresenceTracker) Close(path string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	delete(p.opens, path)
	p.mu.Unlock()
	p.enqueue(presenceUpdate{path: path})
}

func (p *PresenceTracker) enqueue(update presenceUpdate) {
	if pin.IsOffline() {
		return
	}
	select {
	case p.updates <- update:
	case <-p.stopCh:
	default:
		// Best-effort only. Unprocessed fields disappear via presenceTTL.
	}
}

func (p *PresenceTracker) updateLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		case update := <-p.updates:
			// The route can disappear after enqueue. Do not spend even the
			// short telemetry deadline once the data path is offline.
			if pin.IsOffline() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), presenceWriteWait)
			if update.open {
				if p.store.hset(ctx, p.key(), update.path, strconv.FormatInt(update.since, 10)) == nil {
					_ = p.store.expire(ctx, p.key(), presenceTTL)
				}
			} else {
				_ = p.store.hdel(ctx, p.key(), update.path)
			}
			cancel()
		}
	}
}

// Host returns this tracker's hostname.
func (p *PresenceTracker) Host() string { return p.host }

// key is the Redis key for this host's presence hash.
func (p *PresenceTracker) key() string { return presenceKeyPrefix + p.host }

// LocalSnapshot returns this host's currently-open write paths.
func (p *PresenceTracker) LocalSnapshot() []PresenceEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	return snapshotFromMap(p.opens)
}

// AllSnapshots returns presence for every host that has reported recently
// (SCAN jum:presence:*). Keys expired server-side simply don't appear.
func (p *PresenceTracker) AllSnapshots(ctx context.Context) map[string][]PresenceEntry {
	out := make(map[string][]PresenceEntry)
	keys, err := p.store.scanKeys(ctx, presenceKeyPrefix+"*")
	if err != nil {
		return out
	}
	for _, k := range keys {
		host := strings.TrimPrefix(k, presenceKeyPrefix)
		fields, err := p.store.hgetall(ctx, k)
		if err != nil {
			continue
		}
		m := make(map[string]int64, len(fields))
		for path, v := range fields {
			if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
				m[path] = ts
			}
		}
		out[host] = snapshotFromMap(m)
	}
	return out
}

// PresenceEntry is one open file in a snapshot.
type PresenceEntry struct {
	Path       string `json:"path"`
	OpenSince  int64  `json:"open_since_unix"`
	OpenForSec int64  `json:"open_for_sec"`
}

func snapshotFromMap(m map[string]int64) []PresenceEntry {
	out := make([]PresenceEntry, 0, len(m))
	now := time.Now().Unix()
	for path, since := range m {
		out = append(out, PresenceEntry{Path: path, OpenSince: since, OpenForSec: now - since})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// refreshLoop re-EXPIREs this host's key while anything is open. A crashed
// process stops refreshing; Redis expires the whole key within presenceTTL.
func (p *PresenceTracker) refreshLoop() {
	t := time.NewTicker(presenceRefresh)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
			p.mu.Lock()
			any := len(p.opens) > 0
			p.mu.Unlock()
			if !any {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = p.store.expire(ctx, p.key(), presenceTTL)
			cancel()
		}
	}
}

// Stop halts the refresher. Process-exit teardown; best-effort.
func (p *PresenceTracker) Stop() { p.once.Do(func() { close(p.stopCh) }) }
