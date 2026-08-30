package farmqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// StoragePermit is an expiring, observable proof of backend write headroom.
// Its TTL is the admission boundary; CheckedAt and byte counts are diagnostics,
// never trusted in place of Redis expiry.
type StoragePermit struct {
	Source         string `json:"source"`
	CheckedAt      string `json:"checked_at"`
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
	if permit.Source == "" {
		permit.Source = "storage-guard"
	}
	permit.CheckedAt = nowISO()
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
	raw, err := c.rdb.Get(ctx, StoragePermitKey).Bytes()
	if err == redis.Nil {
		return StoragePermit{}, fmt.Errorf("storage permit unavailable")
	}
	if err != nil {
		return StoragePermit{}, fmt.Errorf("get storage permit: %w", err)
	}
	var permit StoragePermit
	if err := json.Unmarshal(raw, &permit); err != nil {
		return StoragePermit{}, fmt.Errorf("decode storage permit: %w", err)
	}
	if permit.CheckedAt == "" || permit.AvailableBytes < permit.RequiredBytes {
		return StoragePermit{}, fmt.Errorf("storage permit is invalid")
	}
	if checkedAt, err := time.Parse(time.RFC3339, permit.CheckedAt); err != nil ||
		time.Since(checkedAt) > StoragePermitTTL {
		return StoragePermit{}, fmt.Errorf("storage permit is stale")
	}
	return permit, nil
}
