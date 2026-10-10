package nfs

import (
	"errors"
	"io"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// cachedReadTrace belongs to one synchronous ReadAt caller. Cache reader phases
// are nested inside CacheReadNS, not additional portions of TotalNS.
type cachedReadTrace struct {
	CachePathInitiallyEligible bool
	QualificationNS            int64
	CacheReadNS                int64
	DescriptorAcquireNS        int64
	FUSEReadNS                 int64
	RetryNS                    int64
	UnaccountedNS              int64
	TotalNS                    int64
	FinalRoute                 string
	ReturnedBytes              int
	FinalResultClass           string
	Reader                     cache.ReadTrace
	logEligible                bool
}

func (tr *cachedReadTrace) finish(start time.Time, n int, err error) {
	if tr == nil {
		return
	}
	tr.TotalNS = time.Since(start).Nanoseconds()
	tr.ReturnedBytes = n
	tr.FinalResultClass = cachedReadResultClass(err)
	accounted := tr.QualificationNS + tr.CacheReadNS + tr.DescriptorAcquireNS + tr.FUSEReadNS + tr.RetryNS
	if tr.TotalNS > accounted {
		tr.UnaccountedNS = tr.TotalNS - accounted
	}
}

func cachedReadResultClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, syscall.EAGAIN):
		return "retryable"
	case errors.Is(err, errFUSETimeout):
		return "retryable_timeout"
	case pin.IsOfflineNotAvailable(err):
		return "offline_unavailable"
	default:
		return "error"
	}
}

func (f *cachedFile) ensureFuseFDTraced(trace *cachedReadTrace) error {
	if trace == nil {
		return f.ensureFuseFD()
	}
	start := time.Now()
	err := f.ensureFuseFD()
	trace.DescriptorAcquireNS += time.Since(start).Nanoseconds()
	return err
}
