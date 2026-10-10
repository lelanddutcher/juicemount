package nfs

import (
	"context"
	"errors"
	"math"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A successful check is one current authority snapshot, never permission to
// publish. Do not cache it or downgrade the entire lease service if this
// additive command is unavailable on an older server.
func (c *ProjectLeaseClient) replacementHistory(ctx context.Context, w metadata.SpoolReplacementWitness, current uint64) (uint64, error) {
	project := projectOf(w.Destination.Path)
	var expected uint64
	for _, node := range w.Destination.Chain {
		if node.Path == project {
			expected = node.Inode
			break
		}
	}
	if c == nil || c.rdb == nil || expected <= 1 || expected > math.MaxInt64 || current == 0 || current > math.MaxInt64 || w.AcceptedEpoch > current {
		return 0, metadata.ErrMirrorConflict
	}
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	value, err := c.rdb.Do(ctx, "JM.LEASE", "CHECK_REPLACE", "path:"+project, w.AcceptedEpoch, current).Result()
	if err != nil {
		return 0, errors.Join(metadata.ErrMirrorConflict, err)
	}
	return decodeReplacementHistory(value, expected, w.AcceptedEpoch, current)
}

func decodeReplacementHistory(value any, project, accepted, current uint64) (uint64, error) {
	arr, ok := value.([]any)
	if !ok || len(arr) != 4 {
		return 0, metadata.ErrMirrorConflict
	}
	status, statusOK := arr[0].(string)
	// RESP integer fields are required; accepting decimal strings would hide a
	// different protocol or a malformed reply from the authority service.
	p, pOK := arr[1].(int64)
	epoch, eOK := arr[2].(int64)
	foreign, fOK := arr[3].(int64)
	if !statusOK || !pOK || !eOK || !fOK || p <= 1 || uint64(p) != project || epoch <= 0 || uint64(epoch) != current ||
		foreign < 0 || uint64(foreign) > current || (status != "CLEAN" && status != "FOREIGN") || (status == "FOREIGN") != (uint64(foreign) > accepted) {
		return 0, metadata.ErrMirrorConflict
	}
	return uint64(foreign), nil
}
