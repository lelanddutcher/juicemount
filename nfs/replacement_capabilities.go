package nfs

import (
	"context"
	"errors"
	"fmt"
)

var errReplacementCapabilityUnavailable = errors.New("replacement stage creation capability unavailable")

// replacementStageCapability is queried only when a saved operation has no
// birth. Unknown commands and malformed replies are local retryable failures;
// neither may change the ordinary lease client's unsupported state.
func (c *ProjectLeaseClient) replacementStageCapability(parent context.Context) (string, error) {
	if c == nil || c.rdb == nil {
		return "", errReplacementCapabilityUnavailable
	}
	ctx, cancel := c.ctx(parent)
	defer cancel()
	v, err := c.rdb.Do(ctx, "JM.LEASE", "REPLACEMENT_CAPABILITIES").Result()
	if err != nil {
		return "", fmt.Errorf("%w: %w", errReplacementCapabilityUnavailable, err)
	}
	a, ok := v.([]any)
	if !ok || len(a) != 5 {
		return "", errReplacementCapabilityUnavailable
	}
	version, vok := a[0].(int64)
	db, dok := a[1].(int64)
	principal, pok := a[2].(string)
	incarnation, iok := a[3].(string)
	ready, rok := a[4].(int64)
	if !vok || version != 1 || !dok || db != int64(c.database) || !pok || len(principal) == 0 || len(principal) > 256 || principal != c.principal ||
		!iok || len(incarnation) == 0 || len(incarnation) > 256 || !rok || ready != 1 {
		return "", errReplacementCapabilityUnavailable
	}
	return incarnation, nil
}
