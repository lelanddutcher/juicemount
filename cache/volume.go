package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

var ErrVolumeBinding = errors.New("cache volume identity is not qualified")

const volumeBindingKey = "cache_volume_binding_v1"

// VolumeBinding binds inode mappings to a selected Redis database and a
// JuiceFS volume. It is not a dataset-incarnation proof: restoring a backend
// with the same UUID can reproduce this identity.
type VolumeBinding struct {
	Version   uint32
	Backend   string // digest only; no credentials or private endpoint in snapshots
	UUID      string
	BlockSize int64 // bytes (the JuiceFS format stores KiB)
}

// BindingStore is implemented by the metadata store. The caller serializes
// startup qualification with that store's lifecycle; it must not replace a
// binding while old readers or metadata consumers remain active.
type BindingStore interface {
	GetMeta(string) (string, bool, error)
	SetMeta(string, string) error
}

// BackendIdentity uses the saved logical endpoint BEFORE any runtime proxy
// rewrite. Authentication and timeout changes do not select another database.
// Parse deliberately supports only the standalone, unprefixed Redis namespace
// used by the metadata client and by the setting/cINODE_CHUNK keys below.
func BackendIdentity(logicalURL string, selection ...string) (string, error) {
	opts, err := redisurl.Parse(logicalURL)
	if err != nil {
		return "", err
	}
	identity := fmt.Sprintf("standalone\x00%s\x00%d\x00tls=%t", strings.ToLower(opts.Addr), opts.DB, opts.TLSConfig != nil)
	for _, selected := range selection {
		identity += fmt.Sprintf("\x00%d:%s", len(selected), selected)
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:]), nil
}

func validVolumeUUID(uuid string) bool {
	if len(uuid) == 0 || len(uuid) > 128 || uuid == "." || uuid == ".." {
		return false
	}
	for _, c := range uuid {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func (b VolumeBinding) valid() bool {
	digest, err := hex.DecodeString(b.Backend)
	return b.Version == 1 && err == nil && len(digest) == sha256.Size && validVolumeUUID(b.UUID) &&
		b.BlockSize >= 64<<10 && b.BlockSize <= 16<<20 && b.BlockSize&(b.BlockSize-1) == 0
}

// LoadVolumeBinding performs no network access. An absent binding is different
// from a mismatched or corrupt binding; neither authorizes old offline maps.
func LoadVolumeBinding(store BindingStore, logicalURL string, selection ...string) (VolumeBinding, bool, error) {
	backend, err := BackendIdentity(logicalURL, selection...)
	if err != nil {
		return VolumeBinding{}, false, err
	}
	raw, exists, err := store.GetMeta(volumeBindingKey)
	if err != nil || !exists {
		return VolumeBinding{}, false, err
	}
	var binding VolumeBinding
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &binding) != nil || !binding.valid() || binding.Backend != backend {
		return VolumeBinding{}, false, ErrVolumeBinding
	}
	return binding, true, nil
}

// ReadVolumeBinding requires a deadline. The bounded Lua observation refuses
// oversized formats without downloading them. No setting contents appear in
// errors: a JuiceFS format can contain object-store credentials.
func ReadVolumeBinding(ctx context.Context, rdb *redis.Client, logicalURL string, selection ...string) (VolumeBinding, error) {
	return readVolumeBindingWithDeadline(ctx, rdb, logicalURL, true, selection...)
}

// readVolumeBindingWithDeadline shares the exact identity, database and format
// checks with the public probe. Session qualification already has one finite
// caller-capped deadline for all of its cold Redis connections and validation
// steps; it must not restart a shorter total timer for each step.
func readVolumeBindingWithDeadline(ctx context.Context, rdb *redis.Client, logicalURL string, capProbe bool, selection ...string) (VolumeBinding, error) {
	backend, err := BackendIdentity(logicalURL, selection...)
	if err != nil {
		return VolumeBinding{}, err
	}
	if _, ok := ctx.Deadline(); !ok || rdb == nil || !rdb.Options().ContextTimeoutEnabled {
		return VolumeBinding{}, ErrVolumeBinding
	}
	if capProbe {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	// Check the runtime effective DB as well. Its host can legitimately be a
	// loopback proxy, but a proxy rewrite must never change the selected DB.
	opts, _ := redisurl.Parse(logicalURL)
	if rdb.Options().DB != opts.DB {
		return VolumeBinding{}, ErrVolumeBinding
	}
	raw, err := rdb.Eval(ctx, `
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local n = redis.call('STRLEN', KEYS[1])
if n == 0 or n > 65536 then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
return redis.call('GET', KEYS[1])`, []string{"setting"}).Text()
	if err != nil {
		return VolumeBinding{}, fmt.Errorf("observe cache volume: %w", classifyVolumeError(err))
	}
	var format struct {
		UUID      string
		BlockSize int64
	}
	if json.Unmarshal([]byte(raw), &format) != nil || format.BlockSize > 16384 || format.BlockSize < 64 {
		return VolumeBinding{}, ErrVolumeBinding
	}
	binding := VolumeBinding{Version: 1, Backend: backend, UUID: format.UUID, BlockSize: format.BlockSize * 1024}
	if !binding.valid() {
		return VolumeBinding{}, ErrVolumeBinding
	}
	return binding, nil
}

// SaveVolumeBinding records a qualified online observation without overwriting
// another library's binding. Migrating an existing metadata namespace to a
// different volume requires a separate, explicit lifecycle transition.
func SaveVolumeBinding(store BindingStore, binding VolumeBinding) error {
	if !binding.valid() {
		return ErrVolumeBinding
	}
	if raw, exists, err := store.GetMeta(volumeBindingKey); err != nil {
		return err
	} else if exists {
		var old VolumeBinding
		if len(raw) > 4096 || json.Unmarshal([]byte(raw), &old) != nil || old != binding {
			return ErrVolumeBinding
		}
		return nil
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	return store.SetMeta(volumeBindingKey, string(raw))
}

// VolumeCacheDir selects the exact UUID directory, including before its first
// block exists. Never scan sibling UUIDs to substitute a warm foreign cache.
// An explicit override must name this volume root or its raw/chunks directory.
func VolumeCacheDir(uuid string) (string, error) {
	if !validVolumeUUID(uuid) {
		return "", ErrVolumeBinding
	}
	paths, isolated, err := appisolation.ResolvePaths()
	if err != nil {
		return "", errors.Join(ErrVolumeBinding, err)
	}
	if configured := strings.TrimSpace(os.Getenv("JM_CACHE_DIR")); configured != "" {
		root := filepath.Clean(configured)
		if filepath.Base(root) == "chunks" && filepath.Base(filepath.Dir(root)) == "raw" {
			root = filepath.Dir(filepath.Dir(root))
		}
		if !filepath.IsAbs(root) || filepath.Base(root) != uuid || isolated && filepath.Dir(root) != paths.CacheBase {
			return "", ErrVolumeBinding
		}
		selected := filepath.Join(root, "raw", "chunks")
		if err := appisolation.ValidateCachePath(selected, true); err != nil {
			return "", errors.Join(ErrVolumeBinding, err)
		}
		return selected, nil
	}
	if isolated {
		selected := filepath.Join(paths.CacheBase, uuid, "raw", "chunks")
		if err := appisolation.ValidateCachePath(selected, true); err != nil {
			return "", errors.Join(ErrVolumeBinding, err)
		}
		return selected, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".juicefs", "cache", uuid, "raw", "chunks"), nil
}

// NewVolumeReader is the production constructor. NewReader remains the raw
// reader for explicitly owned fixtures; its snapshots cannot authorize a
// VolumeReader. Binding qualification belongs to startup, before publishing
// metadata or serving requests, rather than to the hot local read path.
func NewVolumeReader(binding VolumeBinding, rdb *redis.Client) (*Reader, error) {
	if !binding.valid() {
		return nil, ErrVolumeBinding
	}
	cacheDir, err := VolumeCacheDir(binding.UUID)
	if err != nil {
		return nil, err
	}
	r := NewReader(cacheDir, binding.BlockSize, rdb)
	r.binding = binding
	return r, nil
}

// ValidateVolume checks identity even when there are no saved mappings. It
// never relabels an existing reader to use another volume's bytes.
func (r *Reader) ValidateVolume(ctx context.Context) error {
	if r.binding == (VolumeBinding{}) {
		return nil // preserve the public low-level reader behavior
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrVolumeBinding
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return r.validateVolumeWithDeadline(ctx)
}

// validateVolumeWithDeadline preserves the public identity check while using
// the already-bounded Session qualification deadline for cold connections.
func (r *Reader) validateVolumeWithDeadline(ctx context.Context) error {
	if r.binding == (VolumeBinding{}) {
		return nil // explicit low-level readers have no selected-volume contract
	}
	if _, ok := ctx.Deadline(); !ok {
		return ErrVolumeBinding
	}
	rdb := r.rdb.Load()
	if rdb == nil || !rdb.Options().ContextTimeoutEnabled {
		return ErrVolumeBinding
	}
	return classifyVolumeError(rdb.Eval(ctx, boundMappingLua, []string{"setting"}, r.binding.UUID, r.binding.BlockSize/1024).Err())
}

// The format comparison and LRANGE share one Redis execution. A separate GET
// followed by LRANGE could combine old-volume authorization with new slices.
const boundMappingLua = `
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local n = redis.call('STRLEN', KEYS[1])
if n == 0 or n > 65536 then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local ok, f = pcall(cjson.decode, redis.call('GET', KEYS[1]))
if not ok or type(f) ~= 'table' or f.UUID ~= ARGV[1] or tonumber(f.BlockSize) ~= tonumber(ARGV[2]) then
  return redis.error_reply('JM_CACHE_VOLUME_MISMATCH')
end
if #KEYS == 1 then return 1 end
return redis.call('LRANGE', KEYS[2], 0, -1)
`

// The inode attr and slice list must be one Redis observation. A separately
// fetched size can describe an older mapping and falsely authorize terminal
// EOF after an overwrite. Empty/malformed attrs retain ordinary byte serving
// but cannot create a resident terminal witness.
const boundMappingAttrLua = `
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local n = redis.call('STRLEN', KEYS[1])
if n == 0 or n > 65536 then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local ok, f = pcall(cjson.decode, redis.call('GET', KEYS[1]))
if not ok or type(f) ~= 'table' or f.UUID ~= ARGV[1] or tonumber(f.BlockSize) ~= tonumber(ARGV[2]) then
  return redis.error_reply('JM_CACHE_VOLUME_MISMATCH')
end
local a = ''
if redis.call('TYPE', KEYS[3]).ok == 'string' then
  local an = redis.call('STRLEN', KEYS[3])
  if an == 72 or an == 80 then a = redis.call('GET', KEYS[3]) end
end
local s = redis.call('LRANGE', KEYS[2], 0, -1)
table.insert(s, 1, a)
return s
`

// Metadata preparation never transfers an unbounded overwritten-slice list.
// This is separate from the unchanged foreground mapping request: a large
// history declines speculative work without changing foreground read policy.
const prepareMappingMaxRecords = 256
const boundedPrepareMappingAttrLua = `
if redis.call('TYPE', KEYS[1]).ok ~= 'string' then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local n = redis.call('STRLEN', KEYS[1])
if n == 0 or n > 65536 then return redis.error_reply('JM_CACHE_FORMAT_INVALID') end
local ok, f = pcall(cjson.decode, redis.call('GET', KEYS[1]))
if not ok or type(f) ~= 'table' or f.UUID ~= ARGV[1] or tonumber(f.BlockSize) ~= tonumber(ARGV[2]) then
  return redis.error_reply('JM_CACHE_VOLUME_MISMATCH')
end
if redis.call('TYPE', KEYS[2]).ok ~= 'list' and redis.call('TYPE', KEYS[2]).ok ~= 'none' then
  return redis.error_reply('JM_CACHE_PREPARE_INVALID')
end
local count = redis.call('LLEN', KEYS[2])
if count > tonumber(ARGV[3]) then return redis.error_reply('JM_CACHE_PREPARE_LIMIT') end
local a = ''
if redis.call('TYPE', KEYS[3]).ok == 'string' then
  local an = redis.call('STRLEN', KEYS[3])
  if an == 72 or an == 80 then a = redis.call('GET', KEYS[3]) end
end
local out = {a}
for i = 0, count-1 do
  local record = redis.call('LINDEX', KEYS[2], i)
  if not record or #record ~= 24 then return redis.error_reply('JM_CACHE_PREPARE_INVALID') end
  out[#out+1] = record
end
return out
`

func (r *Reader) fetchMappingWithAttrBounded(ctx context.Context, client *redis.Client, key string, inode uint64) ([]string, ResidentAttr, error) {
	if r.binding == (VolumeBinding{}) {
		return nil, ResidentAttr{}, ErrVolumeBinding
	}
	items, err := client.Eval(ctx, boundedPrepareMappingAttrLua,
		[]string{"setting", key, "i" + strconv.FormatUint(inode, 10)},
		r.binding.UUID, r.binding.BlockSize/1024, prepareMappingMaxRecords).StringSlice()
	if err != nil {
		return nil, ResidentAttr{}, classifyMappingError(err)
	}
	if len(items) == 0 || len(items) > prepareMappingMaxRecords+1 {
		return nil, ResidentAttr{}, ErrSliceMapping
	}
	return items[1:], decodeResidentAttr(items[0]), nil
}

func (r *Reader) fetchMappingWithAttr(ctx context.Context, client *redis.Client, key string, inode uint64) ([]string, ResidentAttr, error) {
	if r.binding == (VolumeBinding{}) {
		items, err := client.LRange(ctx, key, 0, -1).Result()
		return items, ResidentAttr{}, err
	}
	items, err := client.Eval(ctx, boundMappingAttrLua,
		[]string{"setting", key, "i" + strconv.FormatUint(inode, 10)},
		r.binding.UUID, r.binding.BlockSize/1024).StringSlice()
	if err != nil {
		return nil, ResidentAttr{}, classifyMappingError(err)
	}
	if len(items) == 0 {
		return nil, ResidentAttr{}, ErrSliceMapping
	}
	return items[1:], decodeResidentAttr(items[0]), nil
}

func (r *Reader) fetchSliceRecords(ctx context.Context, client *redis.Client, key string) ([]string, error) {
	if r.binding == (VolumeBinding{}) {
		return client.LRange(ctx, key, 0, -1).Result()
	}
	items, err := client.Eval(ctx, boundMappingLua, []string{"setting", key}, r.binding.UUID, r.binding.BlockSize/1024).StringSlice()
	return items, classifyMappingError(err)
}

// Definite identity failures must never be handled as transport failures that
// authorize offline fallback or clear-and-reopen recovery.
func classifyVolumeError(err error) error {
	if redis.HasErrorPrefix(err, "JM_CACHE_VOLUME_MISMATCH") || redis.HasErrorPrefix(err, "JM_CACHE_FORMAT_INVALID") {
		return ErrVolumeBinding
	}
	return err
}

// MappingSnapshotPath leaves unqualified legacy maps untouched and enables
// durable working-set publication for this exact library and cache location.
func (r *Reader) MappingSnapshotPath(stateDir string) string {
	data, _ := json.Marshal(r.binding)
	data = append(data, []byte("\x00"+filepath.Clean(r.cacheDir))...)
	sum := sha256.Sum256(data)
	return filepath.Join(stateDir, "cache-mappings-v2-"+hex.EncodeToString(sum[:])+".gob")
}

func classifyMappingError(err error) error {
	if classified := classifyVolumeError(err); classified != err {
		return classified
	}
	if redis.HasErrorPrefix(err, "WRONGTYPE") {
		return fmt.Errorf("%w: %w", ErrSliceMapping, err)
	}
	return err
}

// VolumeBinding returns immutable selected-volume identity. Raw fixture
// readers report false and cannot supply a production library binding.
func (r *Reader) VolumeBinding() (VolumeBinding, bool) {
	if r == nil {
		return VolumeBinding{}, false
	}
	return r.binding, r.binding.valid()
}
