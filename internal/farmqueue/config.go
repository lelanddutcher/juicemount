package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Farm-config channel (FARM-NODE-CONFIG spec): the manager publishes a
// FarmConfig document at ConfigKey; workers poll it every drain-loop tick and
// apply hot settings, reporting restart-class drift via their heartbeat.

// ValidFarmConfigKeys is the whitelist of config fields the manager may write.
// Anything else is rejected at PUT time — these values become argv/env on
// worker processes, so no free-form keys are allowed.
var ValidFarmConfigKeys = map[string]bool{
	"crf":               true, // int 1-51 (proxy quality)
	"preset":            true, // x264 preset name (validated list)
	"vcodec":            true, // encoder name (validated list)
	"model":             true, // whisper model name/path pattern [A-Za-z0-9._/-]
	"workers":           true, // int 1-256
	"proxy_workers":     true, // int 0-256
	"ffmpeg_threads":    true, // int 0-64 (0 = auto)
	"transcript_device": true, // cpu|vulkan|cuda|sycl
	"nice":              true, // int 0-19 (restart-class on workers)
	"ionice":            true, // int 0-7 or -1 to disable (restart-class)
	"watch_enabled":     true, // bool (hot-stoppable)
}

// RestartClassKeys cannot be applied by a running process; the worker reports
// them in PendingRestart instead of applying. watch_enabled is intentionally
// NOT here: it's hot-stoppable.
var RestartClassKeys = map[string]bool{
	"nice":   true,
	"ionice": true,
}

var validPresets = map[string]bool{
	"ultrafast": true, "superfast": true, "veryfast": true, "faster": true,
	"fast": true, "medium": true, "slow": true, "slower": true, "veryslow": true,
}

var validVCodecs = map[string]bool{
	"libx264": true, "h264_nvenc": true, "h264_qsv": true, "h264_vaapi": true,
	"libx265": true, "hevc_nvenc": true, "hevc_qsv": true, "hevc_vaapi": true,
}

var validDevices = map[string]bool{
	"cpu": true, "vulkan": true, "cuda": true, "sycl": true,
}

// ValidateFarmConfig checks a merged patch (one flat map of key→value) against
// the whitelist + ranges. Returns a human-readable error listing every
// violation, or nil. Used by the manager's PUT handler; the worker trusts but
// clamps defensively anyway.
func ValidateFarmConfig(patch map[string]any) error {
	var bad []string
	for k, v := range patch {
		if !ValidFarmConfigKeys[k] {
			bad = append(bad, fmt.Sprintf("unknown key %q", k))
			continue
		}
		switch k {
		case "crf":
			f, ok := numField(v)
			if !ok || f < 1 || f > 51 {
				bad = append(bad, fmt.Sprintf("crf must be int 1-51"))
			}
		case "preset":
			s, _ := v.(string)
			if !validPresets[s] {
				bad = append(bad, fmt.Sprintf("preset %q not allowed", s))
			}
		case "vcodec":
			s, _ := v.(string)
			if !validVCodecs[s] {
				bad = append(bad, fmt.Sprintf("vcodec %q not allowed", s))
			}
		case "model":
			s, _ := v.(string)
			if s == "" || len(s) > 200 || strings.ContainsAny(s, ";|&$`\"'\\ \n") {
				bad = append(bad, fmt.Sprintf("model %q invalid", s))
			}
		case "workers":
			f, ok := numField(v)
			if !ok || f < 1 || f > 256 {
				bad = append(bad, fmt.Sprintf("workers must be int 1-256"))
			}
		case "proxy_workers":
			f, ok := numField(v)
			if !ok || f < 0 || f > 256 {
				bad = append(bad, fmt.Sprintf("proxy_workers must be int 0-256"))
			}
		case "ffmpeg_threads":
			f, ok := numField(v)
			if !ok || f < 0 || f > 64 {
				bad = append(bad, fmt.Sprintf("ffmpeg_threads must be int 0-64"))
			}
		case "transcript_device":
			s, _ := v.(string)
			if !validDevices[s] {
				bad = append(bad, fmt.Sprintf("transcript_device %q not allowed", s))
			}
		case "nice":
			f, ok := numField(v)
			if !ok || f < 0 || f > 19 {
				bad = append(bad, fmt.Sprintf("nice must be int 0-19"))
			}
		case "ionice":
			f, ok := numField(v)
			if !ok || f < 0 || f > 7 {
				bad = append(bad, fmt.Sprintf("ionice must be int 0-7 (omit to disable)"))
			}
		case "watch_enabled":
			if _, ok := v.(bool); !ok {
				bad = append(bad, "watch_enabled must be bool")
			}
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("invalid farm config: %s", strings.Join(bad, "; "))
	}
	return nil
}

func numField(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// GetConfig reads the manager-published config doc. Returns (nil, nil) when
// unset — a totally normal cold-start state meaning "unmanaged".
func (c *Client) GetConfig(ctx context.Context) (*FarmConfig, error) {
	raw, err := c.rdb.Get(ctx, ConfigKey).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var fc FarmConfig
	if err := json.Unmarshal([]byte(raw), &fc); err != nil {
		return nil, err
	}
	return &fc, nil
}

// StoreConfig atomically publishes cfg with revision = max(existing)+1 (or the
// caller-provided revision when forceRevision ≥ 1). Uses WATCH on ConfigKey so
// two writers can't interleave revisions.
func (c *Client) StoreConfig(ctx context.Context, cfg *FarmConfig, forceRevision int64) (int64, error) {
	return c.storeConfigRetry(ctx, cfg, forceRevision, 3)
}

func (c *Client) storeConfigRetry(ctx context.Context, cfg *FarmConfig, forceRevision int64, attempts int) (int64, error) {
	for attempt := 0; attempt < attempts; attempt++ {
		err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
			raw, err := tx.Get(ctx, ConfigKey).Result()
			var cur int64
			if err != redis.Nil {
				if err != nil {
					return err
				}
				var existing FarmConfig
				if json.Unmarshal([]byte(raw), &existing) == nil {
					cur = existing.Revision
				}
			}
			if forceRevision >= 1 {
				cfg.Revision = forceRevision
			} else {
				cfg.Revision = cur + 1
			}
			cfg.UpdatedAt = nowISO()
			blob, err := json.Marshal(cfg)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, ConfigKey, blob, 0)
				return nil
			})
			return err
		}, ConfigKey)
		if err == redis.TxFailedErr {
			continue // optimistic-lock retry
		}
		return cfg.Revision, err
	}
	return 0, fmt.Errorf("config store: contention after %d attempts", attempts)
}

// ResolveFor merges the config for one worker: defaults deep-merged with
// overrides[name]. Returns the flat effective patch plus which restart-class
// keys were requested (the caller decides whether it can apply them).
func (fc *FarmConfig) ResolveFor(name string) (effective map[string]any, restartKeys []string) {
	eff := map[string]any{}
	for k, v := range fc.Defaults {
		eff[k] = v
	}
	if name != "" {
		for k, v := range fc.Overrides[name] {
			eff[k] = v
		}
	}
	for k := range eff {
		if RestartClassKeys[k] {
			restartKeys = append(restartKeys, k)
		}
	}
	return eff, restartKeys
}

// DeleteConfig removes the config doc (operator reset). Workers keep their
// last-good revision in memory and surface the drift on the next heartbeat.
func (c *Client) DeleteConfig(ctx context.Context) error {
	return c.rdb.Del(ctx, ConfigKey).Err()
}

// HeartbeatFull is Heartbeat with the enriched worker payload (same key/TTL).
func (c *Client) HeartbeatFull(ctx context.Context, w Worker) error { return c.Heartbeat(ctx, w) }

var _ = time.Now // keep time import if helpers shrink
