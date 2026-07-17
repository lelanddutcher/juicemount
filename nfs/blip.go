package nfs

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"

	"github.com/lelanddutcher/juicemount/internal/metrics"
)

// Backend-blip park/retry (#9, INSTANT-NAV / task #36). A Redis restart (or
// a short backend outage) used to fail in-flight DATA ops hard: a juicefs
// FUSE read/write blocked on the dead backend surfaces EIO, our handler
// mapped it to NFS3ERR_IO, and the client aborted — Finder errors, NLEs
// SIGBUS on mmap, copies die — for a blip that heals in seconds.
//
// Fix, scoped deliberately:
//   - DATA plane only (READ/WRITE tails). Mutations (MKDIR/REMOVE/RENAME/
//     CREATE) keep today's hard semantics — JUKEBOX-retrying non-idempotent
//     ops risks duplicate-execution confusion and the "error 100060"
//     retry-storm class (see nfs_onmkdir.go's warning).
//   - Error-CLASSIFICATION only: when a data op fails with a transport-class
//     errno (EIO/ETIMEDOUT/ENOTCONN) while the Redis connection is inside
//     the blip window (RecentlyDegraded — down now, or restored within it),
//     the error is wrapped in ErrBackendBlip, which conn.handle maps to the
//     RETRYABLE NFS3ERR_JUKEBOX. The client retries; after the reconnect the
//     retry succeeds. Ops keep failing-fast in every other respect.
//   - BOUNDED: past blipParkWindow the wrap stops and hard errors surface
//     exactly as before — a real outage still reports, no infinite tarpit.
//   - NOT the watchdog/remount machinery (explicitly off-limits this
//     sprint); this never restarts anything, it only re-maps an error for
//     ops that were already in flight during a blip.
//
// Kill switch: JM_BLIP_PARK=0.
const blipParkWindow = 90 * time.Second

var (
	errBackendBlip = nfslib.ErrBackendBlip
	// blipParkEnabled: JM_BLIP_PARK=0 kills the re-mapping (env read once
	// at init, launchctl-set before process start like the other JM_ flags).
	blipParkEnabled = os.Getenv("JM_BLIP_PARK") != "0"
)

// backendBlipActive reports whether we are inside a backend blip window.
// blipHook overrides for tests.
func (h *JuiceMountHandler) backendBlipActive() bool {
	if h == nil {
		return false
	}
	if h.blipHook != nil {
		return h.blipHook()
	}
	rc := h.redisClient
	if rc == nil {
		return false
	}
	return rc.RecentlyDegraded(blipParkWindow)
}

// isBlipErrno classifies transport-class errnos a juicefs FUSE op surfaces
// while its backend is down. Deliberately narrow: io.EOF / ErrUnexpectedEOF
// (the torn-read guards), pin offline errors, and everything else pass
// through untouched.
func isBlipErrno(err error) bool {
	return errors.Is(err, syscall.EIO) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENOTCONN)
}

// classifyBlipError re-maps a blip-class data-plane error to the retryable
// ErrBackendBlip while inside the blip window; every other (or nil) error is
// returned unchanged.
func (h *JuiceMountHandler) classifyBlipError(err error) error {
	if err == nil || !blipParkEnabled {
		return err
	}
	if !isBlipErrno(err) {
		return err
	}
	if h == nil || !h.backendBlipActive() {
		return err
	}
	metrics.Default().IncBackendBlipParked()
	return fmt.Errorf("%w: %v", errBackendBlip, err)
}
