package nfs

import (
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
)

type retryReadResult struct {
	n   int
	err error
}

type retryReadScript struct {
	results []retryReadResult
	calls   int
}

func (r *retryReadScript) boundedFUSEReadAt(p []byte, _ int64) (int, error) {
	r.calls++
	if r.calls > len(r.results) {
		panic("retry launched an unplanned pread")
	}
	result := r.results[r.calls-1]
	if result.n > 0 {
		copy(p, "recovered")
	}
	return result.n, result.err
}

func TestColdReadRetryStopsAtLaterTimeout(t *testing.T) {
	for _, tc := range []struct {
		name       string
		initialErr error
		timeoutErr error
	}{
		{name: "zero EIO plain", initialErr: syscall.EIO, timeoutErr: errFUSETimeout},
		{name: "zero EIO wrapped", initialErr: syscall.EIO, timeoutErr: errors.Join(errFUSETimeout, syscall.ETIMEDOUT)},
		{name: "premature EOF plain", initialErr: io.EOF, timeoutErr: errFUSETimeout},
		{name: "premature EOF wrapped", initialErr: io.EOF, timeoutErr: errors.Join(errFUSETimeout, syscall.ETIMEDOUT)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := metrics.Default().Snapshot()
			read := &retryReadScript{results: []retryReadResult{
				{err: tc.timeoutErr},
				{n: 9}, // must never start while the timed-out pread may still run
			}}
			n, err := retryColdFUSERead(read, make([]byte, 16), 0, 100, "clip.mov", 0, tc.initialErr)
			after := metrics.Default().Snapshot()
			if n != 0 || !errors.Is(err, errFUSETimeout) || read.calls != 1 ||
				after.ReadRetries-before.ReadRetries != 1 || after.ReadFails != before.ReadFails {
				t.Fatalf("retry = (n=%d, err=%v, calls=%d), want timeout after exactly one retry", n, err, read.calls)
			}
		})
	}
}

func TestColdReadRetryPreservesOrdinaryRecovery(t *testing.T) {
	read := &retryReadScript{results: []retryReadResult{
		{err: syscall.EIO},
		{n: 9},
	}}
	p := make([]byte, 16)
	n, err := retryColdFUSERead(read, p, 0, 100, "clip.mov", 0, syscall.EIO)
	if n != 9 || err != nil || read.calls != 2 || string(p[:n]) != "recovered" {
		t.Fatalf("ordinary retry = (n=%d, err=%v, calls=%d, data=%q)", n, err, read.calls, p[:n])
	}
}

func TestColdReadInitialTimeoutNeverRetries(t *testing.T) {
	read := &retryReadScript{results: []retryReadResult{{n: 9}}}
	n, err := retryColdFUSERead(read, make([]byte, 16), 0, 100, "clip.mov", 0, errFUSETimeout)
	if n != 0 || !errors.Is(err, errFUSETimeout) || read.calls != 0 {
		t.Fatalf("initial timeout = (n=%d, err=%v, calls=%d)", n, err, read.calls)
	}
}

func TestColdReadRetryLaterOfflineRefusalStops(t *testing.T) {
	before := metrics.Default().Snapshot()
	read := &retryReadScript{results: []retryReadResult{
		{err: pin.ErrOfflineNotAvailable},
		{n: 9},
	}}
	n, err := retryColdFUSERead(read, make([]byte, 16), 0, 100, "clip.mov", 0, syscall.EIO)
	after := metrics.Default().Snapshot()
	if n != 0 || !errors.Is(err, pin.ErrOfflineNotAvailable) || read.calls != 1 ||
		after.ReadRetries-before.ReadRetries != 1 || after.ReadFails != before.ReadFails {
		t.Fatalf("offline retry = (n=%d, err=%v, calls=%d), want one attempt and no hard fail", n, err, read.calls)
	}
}

func TestColdReadInitialOfflineAndEOFNeverRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		n   int
		err error
		off int64
	}{
		{name: "offline", err: pin.ErrOfflineNotAvailable},
		{name: "genuine EOF", err: io.EOF, off: 100},
		{name: "partial premature EOF", n: 9, err: io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := &retryReadScript{results: []retryReadResult{{n: 9}}}
			n, err := retryColdFUSERead(read, make([]byte, 16), tc.off, 100, "clip.mov", tc.n, tc.err)
			if n != tc.n || !errors.Is(err, tc.err) || read.calls != 0 {
				t.Fatalf("initial result = (n=%d, err=%v, calls=%d), want unchanged", n, err, read.calls)
			}
			if tc.name == "partial premature EOF" && !prematureFUSEEOF(n, err, tc.off, 100) {
				t.Fatal("partial EOF no longer reaches existing postprocessing")
			}
		})
	}
}

func TestColdReadPersistentEIOStillExhausts(t *testing.T) {
	before := metrics.Default().Snapshot()
	read := &retryReadScript{results: make([]retryReadResult, fuseReadMaxRetries)}
	for i := range read.results {
		read.results[i].err = syscall.EIO
	}
	n, err := retryColdFUSERead(read, make([]byte, 16), 0, 100, "clip.mov", 0, syscall.EIO)
	after := metrics.Default().Snapshot()
	if n != 0 || !errors.Is(err, syscall.EIO) || read.calls != fuseReadMaxRetries ||
		after.ReadRetries-before.ReadRetries != fuseReadMaxRetries ||
		after.ReadFails-before.ReadFails != 1 {
		t.Fatalf("persistent EIO = (n=%d, err=%v, calls=%d, retry_delta=%d, fail_delta=%d)",
			n, err, read.calls, after.ReadRetries-before.ReadRetries, after.ReadFails-before.ReadFails)
	}
}
