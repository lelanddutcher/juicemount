package mutationbackend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

// SDKConfig binds a preexisting Redis format and every additional provider
// input. FormatSHA256 hashes the exact bytes stored at Redis key "setting".
// No format/repair command or ambient provider credentials are accepted.
type SDKConfig struct {
	Redis                      RedisConfig `json:"redis"`
	FormatSHA256               string      `json:"format_sha256"`
	Region                     string      `json:"region,omitempty"`
	PathStyle                  *bool       `json:"path_style,omitempty"`
	EncryptionPassphraseFile   string      `json:"encryption_passphrase_file,omitempty"`
	EncryptionPassphraseSHA256 string      `json:"encryption_passphrase_sha256,omitempty"`
}

var sdkProcessStarted atomic.Bool

// SDKAttempted prevents a caller from starting a custody-only fallback in a
// process that has already attempted to acquire SDK capabilities.
func SDKAttempted() bool { return sdkProcessStarted.Load() }

// SDK is opaque: its filesystem and credentials never escape to a gateway
// client. Attaching a session does not authorize any media operation or receipt.
// JuiceFS has unjoinable workers; this runtime requires a dedicated process.
type SDK struct {
	mu             sync.Mutex
	fence          *Fence
	reader         *redis.Client
	metadata       meta.Meta
	filesystem     *fs.FileSystem
	objects        object.ObjectStorage
	formatHash     string
	changed        atomic.Bool
	closed         bool
	closing        bool
	activeStages   int
	publishing     bool
	stageUncertain bool
	media          *sdkMedia
	closeErr       error
}

// ReadSDKConfig reads a private, bounded configuration file without acquiring
// a backend capability. The constructor pins authority before reading secrets.
func ReadSDKConfig(path string) (*SDKConfig, error) {
	b, err := privateRead(path, 16<<10)
	if err != nil {
		return nil, fmt.Errorf("SDK configuration: %w", mutationauthority.ErrInvalid)
	}
	var cfg SDKConfig
	if err := strictJSON(b, &cfg); err != nil || !validSDKConfig(cfg) {
		return nil, fmt.Errorf("SDK configuration: %w", mutationauthority.ErrInvalid)
	}
	return &cfg, nil
}

func validSDKConfig(c SDKConfig) bool {
	return lowerHex(c.FormatSHA256, 64) &&
		(c.EncryptionPassphraseFile == "" && c.EncryptionPassphraseSHA256 == "" ||
			c.EncryptionPassphraseFile != "" && lowerHex(c.EncryptionPassphraseSHA256, 64))
}

func strictJSON(b []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return mutationauthority.ErrInvalid
	}
	return nil
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func sdkError(stage string) error {
	// Library/provider errors can include credentials or endpoint URLs.
	return fmt.Errorf("SDK %s: %w", stage, ErrUnavailable)
}

// OpenSDK consumes the sole SDK attachment attempt in this process. The SAME
// whole-volume journal is irreversibly pinned before any secret or writable
// capability is read. Any failure requires process exit, even before NewSession.
func OpenSDK(ctx context.Context, j *mutationauthority.Journal, volume string, cfg SDKConfig) (_ *SDK, err error) {
	if j == nil {
		return nil, mutationauthority.ErrInvalid
	}
	if err := j.PinProcessLifetime(); err != nil {
		return nil, err
	}
	if !sdkProcessStarted.CompareAndSwap(false, true) {
		return nil, mutationauthority.ErrProcessLifetime
	}
	// This dedicated process owns the SDK's global logger/transport settings.
	// Surface only static stage errors; never echo provider credentials or URLs.
	utils.SetOutput(io.Discard)
	if !validSDKConfig(cfg) || !cleanSDKEnvironment() {
		return nil, sdkError("configuration")
	}
	if _, err := PreflightStageRecovery(ctx, j, volume, cfg); err != nil {
		return nil, sdkError("stage preflight")
	}
	binding := sdkBinding(volume, cfg)
	if err := j.CheckBackendConfiguration(ctx, volume, binding); err != nil {
		return nil, sdkError("configuration binding")
	}
	fence, credentials, err := Open(ctx, j, volume, cfg.Redis)
	if err != nil {
		return nil, sdkError("authority")
	}
	r := &SDK{fence: fence, formatHash: cfg.FormatSHA256}
	defer func() {
		if err != nil {
			fence.Freeze()
			if r.reader != nil {
				_ = r.reader.Close()
			}
			// Do not retry, release the pin, or infer a join after partial startup.
		}
	}()
	r.reader = redis.NewClient(&redis.Options{Network: "unix", Addr: credentials.socket,
		Username: credentials.username, Password: credentials.password, Protocol: 2,
		DisableIdentity: true, MaxRetries: -1, PoolSize: 1, MaxActiveConns: 1,
		ContextTimeoutEnabled: true, ReadTimeout: time.Second, WriteTimeout: time.Second})
	body, err := r.readSetting(ctx)
	if err != nil {
		return nil, sdkError("existing format")
	}
	var format meta.Format
	if err := strictJSON(body, &format); err != nil || format.UUID != volume || !validSDKFormat(format) {
		return nil, sdkError("existing format")
	}
	if err := j.BindBackendConfiguration(ctx, volume, binding); err != nil {
		return nil, sdkError("configuration binding")
	}
	r.media = &sdkMedia{journal: j, volume: volume, format: format, binding: binding}
	objects, err := sdkStorageRecorded(ctx, format, cfg, r.media)
	if err != nil {
		return nil, sdkError("storage configuration")
	}
	r.objects = objects
	mconf := meta.DefaultConf()
	mconf.NoBGJob, mconf.MaxDeletes, mconf.Retries = true, 0, 1
	if err := fence.Check(ctx); err != nil {
		return nil, sdkError("metadata admission")
	}
	r.metadata = meta.NewClient(credentials.MetadataURL(), mconf)
	loaded, err := r.metadata.Load(true)
	if err != nil || !reflect.DeepEqual(loaded, &format) {
		return nil, sdkError("metadata format")
	}
	r.metadata.OnReload(func(next *meta.Format) {
		if !reflect.DeepEqual(next, &format) {
			r.changed.Store(true)
		}
	})
	if err := r.Check(ctx); err != nil {
		return nil, sdkError("session admission")
	}
	if err := r.metadata.NewSession(true); err != nil {
		return nil, sdkError("session startup")
	}
	cconf := chunk.Config{CacheDir: "memory", CacheSize: 0, Writeback: false,
		BlockSize: format.BlockSize * 1024, Compress: format.Compression, HashPrefix: format.HashPrefix,
		MaxUpload: 4, MaxRetries: 1, BufferSize: 64 << 20,
		GetTimeout: 10 * time.Second, PutTimeout: 10 * time.Second,
		UploadLimit: format.UploadLimit * 1_000_000 / 8, DownloadLimit: format.DownloadLimit * 1_000_000 / 8}
	if err := r.Check(ctx); err != nil {
		return nil, sdkError("filesystem admission")
	}
	store := chunk.NewCachedStore(objects, cconf, nil)
	r.filesystem, err = fs.NewFileSystem(&vfs.Config{Meta: mconf, Format: format, Chunk: &cconf}, &mediaMetadata{Meta: r.metadata, media: r.media}, store, nil)
	if err != nil {
		return nil, sdkError("filesystem startup")
	}
	if err := r.Check(ctx); err != nil {
		return nil, sdkError("final admission")
	}
	if err := r.retainPriorStages(ctx); err != nil {
		return nil, sdkError("stage recovery")
	}
	return r, nil
}

func (r *SDK) readSetting(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	// Fetch one byte beyond the permitted format size to detect overflow without
	// receiving or allocating an arbitrarily large Redis value.
	body, err := r.reader.GetRange(ctx, "setting", 0, 64<<10).Bytes()
	if err != nil || len(body) == 0 || len(body) > 64<<10 || digest(body) != r.formatHash {
		return nil, ErrUnavailable
	}
	return body, nil
}

// Check verifies the fixed format independently of the SDK reload callback,
// which runs only after the SDK has loaded a new format. Failure is terminal.
func (r *SDK) Check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.closing || r.changed.Load() {
		return ErrUnavailable
	}
	if err := r.fence.Check(ctx); err != nil {
		r.changed.Store(true)
		return ErrUnavailable
	}
	if _, err := r.readSetting(ctx); err != nil {
		r.changed.Store(true)
		r.fence.Freeze()
		return ErrUnavailable
	}
	return nil
}

// Close invokes library cleanup at most once. A nil result is NOT a worker
// join. The caller's journal remains irreversibly pinned until OS process exit.
func (r *SDK) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closing = true
	if r.activeStages != 0 {
		return mutationauthority.ErrInFlight
	}
	r.closed = true
	var errs []error
	if r.filesystem != nil {
		errs = append(errs, r.filesystem.Close())
	}
	if r.metadata != nil {
		errs = append(errs, r.metadata.CloseSession())
		errs = append(errs, r.metadata.Shutdown())
	}
	if r.reader != nil {
		errs = append(errs, r.reader.Close())
	}
	r.fence.Freeze()
	if errors.Join(errs...) != nil {
		r.closeErr = sdkError("close")
	}
	return r.closeErr
}
