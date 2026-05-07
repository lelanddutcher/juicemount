package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

const (
	// Channel used for real-time metadata events between JuiceMount clients.
	SubscribeChannel = "juicemount:metadata"

	// Default batch reconciliation interval.
	DefaultReconcileInterval = 30 * time.Second

	// PruneThreshold is the number of consecutive reconciliation cycles an entry
	// must be absent from Redis before it is deleted from SQLite.
	// At 30s intervals, 2 cycles = ~60s minimum before any prune fires.
	// This prevents transient Redis scan misses from dropping legitimate entries.
	PruneThreshold = 2
)

// MetadataEvent represents a real-time metadata change published via Redis SUBSCRIBE.
type MetadataEvent struct {
	Op    string `json:"op"`    // "create", "update", "rename", "delete"
	Path  string `json:"path"`
	Size  int64  `json:"size,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	Inode uint64 `json:"inode,omitempty"`
	IsDir bool   `json:"is_dir,omitempty"`

	// For rename operations
	OldPath string `json:"old_path,omitempty"`
}

// RedisClient manages Redis connections for metadata sync.
// It provides two sync mechanisms:
//  1. SUBSCRIBE: event-driven, <100ms propagation for real-time changes
//  2. Batch reconciliation: periodic full tree pull, catches anything SUBSCRIBE missed
type RedisClient struct {
	rdb      *redis.Client
	redisURL string // stored for reconnect
	store    *Store

	reconcileInterval time.Duration
	stopCh            chan struct{}
	stopOnce          sync.Once
	syncNowCh         chan struct{} // signals reconcileLoop to run immediately

	mu               sync.RWMutex
	lastSyncDuration time.Duration
	lastSyncTime     time.Time
	lastSyncEntries  int
	connected        bool
	lastDisconnect   time.Time
	lastReconnect    time.Time

	// pruneAbsent tracks how many consecutive reconciliation cycles each path
	// has been absent from Redis. Only paths absent for PruneThreshold+ cycles
	// are actually deleted from SQLite. Guarded by mu.
	pruneAbsent map[string]int
}

// ParseRedisURL extracts host, port, and db from a redis:// URL.
func ParseRedisURL(rawURL string) (addr string, db int, err error) {
	if !strings.HasPrefix(rawURL, "redis://") {
		rawURL = "redis://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", 0, fmt.Errorf("parse redis URL: %w", err)
	}
	host := u.Hostname()
	port := "6379"
	if p := u.Port(); p != "" {
		port = p
	}
	db = 0
	if len(u.Path) > 1 {
		db, _ = strconv.Atoi(u.Path[1:])
	}
	return host + ":" + port, db, nil
}

// NewRedisClient connects to Redis and prepares for syncing.
// It does NOT start background goroutines — call Start() for that.
func NewRedisClient(redisURL string, store *Store) (*RedisClient, error) {
	addr, db, err := ParseRedisURL(redisURL)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialTimeout:  10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisClient{
		rdb:               rdb,
		redisURL:          redisURL,
		store:             store,
		reconcileInterval: DefaultReconcileInterval,
		stopCh:            make(chan struct{}),
		syncNowCh:         make(chan struct{}, 1),
		connected:         true,
		pruneAbsent:       make(map[string]int),
	}, nil
}

// Store returns the underlying metadata store.
func (rc *RedisClient) Store() *Store { return rc.store }

// Connected returns whether the Redis client is currently connected.
func (rc *RedisClient) Connected() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.connected
}

// LastDisconnect returns when the client last lost connectivity.
func (rc *RedisClient) LastDisconnect() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastDisconnect
}

// LastReconnect returns when the client last regained connectivity.
func (rc *RedisClient) LastReconnect() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastReconnect
}

// Reconnect tears down and re-establishes the Redis connection.
// Called when a network interface change is detected so that TCP connections
// bound to the old interface are replaced with connections on the new one.
func (rc *RedisClient) Reconnect() error {
	addr, db, err := ParseRedisURL(rc.redisURL)
	if err != nil {
		return err
	}

	// Close the old connection (ignore errors — it may already be broken)
	rc.rdb.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Second,
		DialTimeout:  10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rc.mu.Lock()
		rc.connected = false
		rc.lastDisconnect = time.Now()
		rc.mu.Unlock()
		return fmt.Errorf("redis reconnect ping: %w", err)
	}

	rc.rdb = rdb
	rc.mu.Lock()
	rc.connected = true
	rc.lastReconnect = time.Now()
	rc.mu.Unlock()

	jmlog.Info("redis reconnected")
	return nil
}

// TriggerSync signals the reconcile loop to run an immediate sync cycle.
// Non-blocking: if a signal is already pending it does nothing.
func (rc *RedisClient) TriggerSync() {
	select {
	case rc.syncNowCh <- struct{}{}:
	default:
	}
}

// LastSyncDuration returns the duration of the most recent batch sync.
func (rc *RedisClient) LastSyncDuration() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncDuration
}

// LastSyncTime returns when the most recent batch sync completed.
func (rc *RedisClient) LastSyncTime() time.Time {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncTime
}

// LastSyncEntries returns the entry count from the most recent batch sync.
func (rc *RedisClient) LastSyncEntries() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastSyncEntries
}

// SyncOnce performs a single batch reconciliation (Lua tree pull → SQLite).
func (rc *RedisClient) SyncOnce() error {
	return rc.syncMetadata()
}

// Start begins both the SUBSCRIBE listener and periodic batch reconciliation.
func (rc *RedisClient) Start() {
	go rc.subscribeLoop()
	go rc.reconcileLoop()
}

// Stop halts all background goroutines and closes the Redis connection.
func (rc *RedisClient) Stop() {
	rc.stopOnce.Do(func() {
		close(rc.stopCh)
		rc.rdb.Close()
	})
}

// PublishEvent sends a metadata event to all subscribed clients.
func (rc *RedisClient) PublishEvent(ctx context.Context, evt MetadataEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return rc.rdb.Publish(ctx, SubscribeChannel, string(data)).Err()
}

// subscribeLoop listens for real-time metadata events via Redis SUBSCRIBE.
func (rc *RedisClient) subscribeLoop() {
	for {
		select {
		case <-rc.stopCh:
			return
		default:
		}

		rc.runSubscribe()

		// If we get here, subscription was lost. Wait briefly then retry.
		select {
		case <-rc.stopCh:
			return
		case <-time.After(2 * time.Second):
			jmlog.Info("redis SUBSCRIBE reconnecting")
		}
	}
}

func (rc *RedisClient) runSubscribe() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Monitor stopCh to cancel the subscription context
	go func() {
		select {
		case <-rc.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	sub := rc.rdb.Subscribe(ctx, SubscribeChannel)
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		var evt MetadataEvent
		if err := json.Unmarshal([]byte(msg.Payload), &evt); err != nil {
			log.Printf("subscribe: invalid event: %v", err)
			continue
		}
		rc.applyEvent(evt)
	}
}

// applyEvent applies a single real-time metadata event to the local store.
// Updates the in-memory cache immediately (never blocked), then writes to
// SQLite with retry (may be briefly blocked by BulkInsert transactions).
func (rc *RedisClient) applyEvent(evt MetadataEvent) {
	var mode fs.FileMode = 0644
	if evt.IsDir {
		mode = 0755 | fs.ModeDir
	}

	switch evt.Op {
	case "create", "update":
		e := &Entry{
			Path:       evt.Path,
			Name:       path.Base(evt.Path),
			ParentPath: path.Dir(evt.Path),
			IsDir:      evt.IsDir,
			Size:       evt.Size,
			Mtime:      time.Unix(evt.Mtime, 0),
			Inode:      evt.Inode,
			Mode:       mode,
		}
		// In-memory cache first (instant, never blocked)
		rc.store.InsertToCache(e)
		// SQLite write (serialized by writeMu, no retry needed)
		if err := rc.store.Insert(e); err != nil {
			log.Printf("subscribe apply create/update: %v", err)
		}

	case "delete":
		rc.store.DeleteFromCache(evt.Path)
		if err := rc.store.Delete(evt.Path); err != nil {
			log.Printf("subscribe apply delete: %v", err)
		}

	case "rename":
		if evt.OldPath != "" {
			rc.store.DeleteFromCache(evt.OldPath)
			rc.store.Delete(evt.OldPath)
		}
		e := &Entry{
			Path:       evt.Path,
			Name:       path.Base(evt.Path),
			ParentPath: path.Dir(evt.Path),
			IsDir:      evt.IsDir,
			Size:       evt.Size,
			Mtime:      time.Unix(evt.Mtime, 0),
			Inode:      evt.Inode,
			Mode:       mode,
		}
		rc.store.InsertToCache(e)
		if err := rc.store.Insert(e); err != nil {
			log.Printf("subscribe apply rename: %v", err)
		}
	}
}

// reconcileLoop runs periodic batch reconciliation with exponential backoff
// on failure (network transitions, Redis unreachable). It also listens for
// immediate sync requests via syncNowCh (triggered by network changes).
func (rc *RedisClient) reconcileLoop() {
	baseInterval := rc.reconcileInterval
	backoff := baseInterval
	maxBackoff := 5 * time.Minute
	consecutiveFailures := 0

	ticker := time.NewTicker(backoff)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rc.doReconcile(&consecutiveFailures, &backoff, baseInterval, maxBackoff, ticker)
		case <-rc.syncNowCh:
			jmlog.Info("reconciliation triggered by network change")
			rc.doReconcile(&consecutiveFailures, &backoff, baseInterval, maxBackoff, ticker)
		case <-rc.stopCh:
			return
		}
	}
}

func (rc *RedisClient) doReconcile(consecutiveFailures *int, backoff *time.Duration, baseInterval, maxBackoff time.Duration, ticker *time.Ticker) {
	if err := rc.syncMetadata(); err != nil {
		*consecutiveFailures++
		*backoff = baseInterval * time.Duration(1<<min(*consecutiveFailures, 6))
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
		rc.mu.Lock()
		if rc.connected {
			rc.connected = false
			rc.lastDisconnect = time.Now()
		}
		rc.mu.Unlock()
		jmlog.Warn("reconciliation failed",
			"attempt", *consecutiveFailures,
			"next_in_sec", int64(backoff.Round(time.Second).Seconds()),
			"error", err.Error(),
		)
		ticker.Reset(*backoff)
	} else {
		rc.mu.Lock()
		wasDisconnected := !rc.connected
		rc.connected = true
		if wasDisconnected {
			rc.lastReconnect = time.Now()
		}
		rc.mu.Unlock()
		if *consecutiveFailures > 0 {
			jmlog.Info("reconciliation recovered", "previous_failures", *consecutiveFailures)
			*consecutiveFailures = 0
			*backoff = baseInterval
			ticker.Reset(*backoff)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// luaScript is the JuiceFS Redis metadata tree pull script.
// It SCANs all d* keys (directory entries), builds an inode→path reverse map,
// resolves full paths, and fetches inode attributes (i{inode}) for mtime and size.
const luaScript = `
local function gi(v) if #v~=9 then return nil end
local a,b,c,d=string.byte(v,6,9) return a*16777216+b*65536+c*256+d end
local function u32(s,o) local a,b,c,d=string.byte(s,o,o+3) return a*16777216+b*65536+c*256+d end
local function u64(s,o) return u32(s,o)*4294967296+u32(s,o+4) end
local cursor='0' local rev={} local all={}
repeat local r=redis.call('SCAN',cursor,'MATCH','d*','COUNT',1000) cursor=r[1]
for _,key in ipairs(r[2]) do local pi=string.sub(key,2)
local ent=redis.call('HGETALL',key)
for i=1,#ent,2 do local nm=ent[i] local val=ent[i+1]
local ci=gi(val) local ft=string.byte(val,1)
if ci then rev[tostring(ci)]=pi..'\t'..nm
table.insert(all,{inode=tostring(ci),parent=pi,name=nm,ft=ft}) end
end end until cursor=='0'
local results={}
for _,e in ipairs(all) do
local parts={e.name} local cur=e.parent
for d=1,50 do if cur=='1' then break end
local mp=rev[cur] if not mp then break end
local sp=string.find(mp,'\t',1,true)
cur=string.sub(mp,1,sp-1)
table.insert(parts,1,string.sub(mp,sp+1)) end
if cur=='1' or e.parent=='1' then
local path=table.concat(parts,'/')
local mt=0 local sz=0
local attr=redis.call('GET','i'..e.inode)
if attr and #attr>=59 then
mt=u64(attr,24)
sz=u64(attr,52)
end
table.insert(results,tostring(e.ft)..':'..tostring(mt)..':'..tostring(sz)..':'..e.inode..':'..path)
end
end
return results
`

// syncMetadata performs a full batch reconciliation: Lua tree pull → SQLite sync.
func (rc *RedisClient) syncMetadata() error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	result, err := rc.rdb.Eval(ctx, luaScript, nil).StringSlice()
	if err != nil {
		return fmt.Errorf("redis EVAL: %w", err)
	}

	// Parse entries from Lua output
	redisEntries := make([]*Entry, 0, len(result))
	redisPaths := make(map[string]struct{}, len(result))

	for _, raw := range result {
		// Format: "fileType:mtime:fileSize:inode:relative/path"
		parts := strings.SplitN(raw, ":", 5)
		if len(parts) != 5 || parts[4] == "" {
			continue
		}
		ft, _ := strconv.Atoi(parts[0])
		mtimeEpoch, _ := strconv.ParseInt(parts[1], 10, 64)
		fileSize, _ := strconv.ParseInt(parts[2], 10, 64)
		inode, _ := strconv.ParseUint(parts[3], 10, 64)
		entryPath := parts[4]

		isDir := ft == 2
		var mtime time.Time
		if mtimeEpoch > 0 {
			mtime = time.Unix(mtimeEpoch, 0)
		}

		var mode fs.FileMode = 0644
		if isDir {
			mode = 0755 | fs.ModeDir
		}

		redisEntries = append(redisEntries, &Entry{
			Path:       entryPath,
			Name:       path.Base(entryPath),
			ParentPath: path.Dir(entryPath),
			IsDir:      isDir,
			Size:       fileSize,
			Mtime:      mtime,
			Inode:      inode,
			Mode:       mode,
		})
		redisPaths[entryPath] = struct{}{}
	}

	// Incremental upsert: only insert entries that are new or changed.
	// Comparing with the in-memory cache avoids rewriting all 131K entries
	// to SQLite on every sync cycle (reduces ~6s to <500ms steady-state).
	var toUpsert []*Entry
	for _, e := range redisEntries {
		existing := rc.store.LookupByPath(e.Path)
		if existing == nil ||
			existing.Mtime.Unix() != e.Mtime.Unix() ||
			existing.Size != e.Size ||
			existing.Inode != e.Inode {
			toUpsert = append(toUpsert, e)
		}
	}

	if len(toUpsert) > 0 {
		if err := rc.store.BulkInsert(toUpsert, 500); err != nil {
			return fmt.Errorf("bulk insert: %w", err)
		}
	}

	// Clear local_only flag only for entries that are actually local_only.
	// No need to update all 131K entries — typically only 0-10 are local_only.
	localOnly, _ := rc.store.LocalOnlyEntries()
	if len(localOnly) > 0 {
		var clearPaths []string
		for _, e := range localOnly {
			if _, inRedis := redisPaths[e.Path]; inRedis {
				clearPaths = append(clearPaths, e.Path)
			}
		}
		if len(clearPaths) > 0 {
			if err := rc.store.BulkClearLocalOnly(clearPaths); err != nil {
				return fmt.Errorf("bulk clear local_only: %w", err)
			}
		}
	}

	// Prune entries absent from Redis for PruneThreshold+ consecutive cycles.
	// A single-cycle absence (transient scan miss) does NOT delete the entry —
	// it must be absent PruneThreshold times in a row before removal fires.
	existingPaths, err := rc.store.AllPaths()
	if err != nil {
		return fmt.Errorf("all paths: %w", err)
	}

	rc.mu.Lock()
	// Increment absence counter for paths not in Redis; clear for paths that returned.
	for p := range existingPaths {
		if _, inRedis := redisPaths[p]; !inRedis {
			rc.pruneAbsent[p]++
		} else {
			delete(rc.pruneAbsent, p)
		}
	}
	// Remove stale entries from pruneAbsent (paths already deleted from SQLite).
	for p := range rc.pruneAbsent {
		if _, exists := existingPaths[p]; !exists {
			delete(rc.pruneAbsent, p)
		}
	}
	// Collect paths that have been absent long enough to prune.
	var toDelete []string
	for p, count := range rc.pruneAbsent {
		if count >= PruneThreshold {
			toDelete = append(toDelete, p)
			delete(rc.pruneAbsent, p)
		}
	}
	rc.mu.Unlock()

	if len(toDelete) > 0 {
		if err := rc.store.DeletePaths(toDelete); err != nil {
			return fmt.Errorf("delete pruned: %w", err)
		}
	}

	duration := time.Since(start)
	rc.mu.Lock()
	rc.lastSyncDuration = duration
	rc.lastSyncTime = time.Now()
	rc.lastSyncEntries = len(redisEntries)
	pendingPrune := len(rc.pruneAbsent)
	rc.mu.Unlock()

	jmlog.Info("metadata sync complete",
		"entries", len(redisEntries),
		"upserted", len(toUpsert),
		"pruned", len(toDelete),
		"pending_prune", pendingPrune,
		"duration_ms", duration.Round(time.Millisecond).Milliseconds(),
	)
	return nil
}
