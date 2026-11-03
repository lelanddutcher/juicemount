package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxExactPermitInteger = uint64(1<<53 - 1)

// StoragePermit is an expiring, observable proof of backend write headroom.
// Its TTL is the admission boundary; CheckedAt and byte counts are diagnostics,
// never trusted in place of Redis expiry.
type StoragePermit struct {
	Source         string `json:"source"`
	CheckedAt      string `json:"checked_at"`
	CheckedAtUnix  int64  `json:"checked_at_unix"`
	TotalBytes     uint64 `json:"total_bytes,omitempty"`
	AvailableBytes uint64 `json:"available_bytes"`
	RequiredBytes  uint64 `json:"required_bytes"`
}

// PublishStoragePermit refreshes safe claim admission. Only a Manager/server
// process with a local StatFS view of the physical backend should call it.
func (c *Client) PublishStoragePermit(ctx context.Context, permit StoragePermit) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("publish storage permit: queue unavailable")
	}
	if permit.AvailableBytes < permit.RequiredBytes {
		return fmt.Errorf("publish storage permit: available bytes are below required reserve")
	}
	if permit.TotalBytes > maxExactPermitInteger || permit.AvailableBytes > maxExactPermitInteger ||
		permit.RequiredBytes > maxExactPermitInteger {
		return fmt.Errorf("publish storage permit: byte counts exceed exact JSON integer range")
	}
	if permit.Source == "" {
		permit.Source = "storage-guard"
	}
	// Stamp with Redis TIME, the clock the claim script judges freshness by.
	// The publisher's own clock is not that clock: a server host 5 s ahead made
	// every permit look future-dated and blocked every render claim.
	redisNow, err := c.rdb.Time(ctx).Result()
	if err != nil {
		return fmt.Errorf("publish storage permit: redis time: %w", err)
	}
	checkedAt := redisNow.UTC().Truncate(time.Second)
	permit.CheckedAt = checkedAt.Format(time.RFC3339)
	permit.CheckedAtUnix = checkedAt.Unix()
	raw, err := json.Marshal(permit)
	if err != nil {
		return fmt.Errorf("publish storage permit: %w", err)
	}
	if err := c.rdb.Set(ctx, StoragePermitKey, raw, StoragePermitTTL).Err(); err != nil {
		return fmt.Errorf("publish storage permit: %w", err)
	}
	return nil
}

// RevokeStoragePermit closes remote claim admission immediately. The TTL still
// provides the fail-closed path if Redis cannot accept this deletion.
func (c *Client) RevokeStoragePermit(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("revoke storage permit: queue unavailable")
	}
	return c.rdb.Del(ctx, StoragePermitKey).Err()
}

// GetStoragePermit returns only a currently-live permit. Redis expiration is
// authoritative, so a missing key is reported distinctly from malformed data.
func (c *Client) GetStoragePermit(ctx context.Context) (StoragePermit, error) {
	if c == nil || c.rdb == nil {
		return StoragePermit{}, fmt.Errorf("get storage permit: queue unavailable")
	}
	// Read the permit, its expiry and Redis TIME together. Freshness is judged
	// against Redis TIME, as the claim script judges it, so a render host whose
	// clock is skewed agrees with Redis about whether it may claim.
	var getCmd *redis.StringCmd
	var ttlCmd *redis.DurationCmd
	var timeCmd *redis.TimeCmd
	_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		getCmd = pipe.Get(ctx, StoragePermitKey)
		ttlCmd = pipe.PTTL(ctx, StoragePermitKey)
		timeCmd = pipe.Time(ctx)
		return nil
	})
	if err != nil && err != redis.Nil {
		return StoragePermit{}, fmt.Errorf("get storage permit: %w", err)
	}
	raw, err := getCmd.Bytes()
	if err == redis.Nil {
		return StoragePermit{}, fmt.Errorf("storage permit unavailable")
	}
	if err != nil {
		return StoragePermit{}, fmt.Errorf("get storage permit: %w", err)
	}
	redisNow, err := timeCmd.Result()
	if err != nil {
		return StoragePermit{}, fmt.Errorf("get storage permit: redis time: %w", err)
	}
	// checked_at is only a display field. Decode authority fields directly so
	// missing or malformed diagnostics cannot veto valid numeric authority.
	var authority struct {
		Source         string `json:"source"`
		CheckedAtUnix  *int64 `json:"checked_at_unix"`
		LegacyChecked  any    `json:"checked_at"`
		TotalBytes     uint64 `json:"total_bytes"`
		AvailableBytes uint64 `json:"available_bytes"`
		RequiredBytes  uint64 `json:"required_bytes"`
	}
	if err := json.Unmarshal(raw, &authority); err != nil {
		return StoragePermit{}, fmt.Errorf("decode storage permit: %w", err)
	}
	var checkedUnix int64
	if authority.CheckedAtUnix != nil {
		checkedUnix = *authority.CheckedAtUnix
	} else {
		checkedUnix = legacyPermitCheckedUnix(authority.LegacyChecked)
	}
	permit := StoragePermit{Source: authority.Source, CheckedAtUnix: checkedUnix,
		TotalBytes: authority.TotalBytes, AvailableBytes: authority.AvailableBytes,
		RequiredBytes: authority.RequiredBytes}
	if permit.CheckedAtUnix > 0 {
		permit.CheckedAt = time.Unix(permit.CheckedAtUnix, 0).UTC().Format(time.RFC3339)
	}
	if permit.Source == "" || permit.CheckedAtUnix <= 0 ||
		uint64(permit.CheckedAtUnix) > maxExactPermitInteger ||
		permit.TotalBytes > maxExactPermitInteger || permit.AvailableBytes > maxExactPermitInteger ||
		permit.RequiredBytes > maxExactPermitInteger || permit.AvailableBytes < permit.RequiredBytes {
		return StoragePermit{}, fmt.Errorf("storage permit is invalid")
	}
	age := redisNow.Unix() - permit.CheckedAtUnix
	if age < -2 || age >= int64(StoragePermitTTL/time.Second) {
		return StoragePermit{}, fmt.Errorf("storage permit is stale")
	}
	ttl, err := ttlCmd.Result()
	if err != nil || ttl <= 0 || ttl > StoragePermitTTL {
		return StoragePermit{}, fmt.Errorf("storage permit expiry is invalid")
	}
	return permit, nil
}

// legacyPermitCheckedUnix reads the freshness of a permit published before
// checked_at_unix existed. Those publishers (pre-H-2 server workers) write only
// a whole-second UTC RFC3339 checked_at. Without this, a render worker upgraded
// ahead of the server worker rejects every valid permit and never claims work.
// It accepts only the exact canonical form and returns 0 (invalid) otherwise;
// the claim script applies the same rule.
func legacyPermitCheckedUnix(value any) int64 {
	text, ok := value.(string)
	if !ok {
		return 0
	}
	checked, err := time.Parse(time.RFC3339, text)
	if err != nil || checked.UTC().Format(time.RFC3339) != text {
		return 0
	}
	return checked.Unix()
}
