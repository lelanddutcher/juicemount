package health

// NFS-layer absent-mount auto-recovery (#93).
//
// The P0 hole this closes: when the user-visible NFS mount (e.g.
// /Volumes/zpool) DISAPPEARS — user ran umount, or macOS dropped it — while
// juicefs/FUSE stays alive and healthy, nothing ever put it back. The
// legacy handleNFSAutoRemount treats every unhealthy-NFS tick the same and,
// while juicefs is alive, defers to the FUSE watchdog:
//
//	"nfs mount stale but juicefs alive — deferring to the fuse watchdog,
//	 not remounting"
//
// …but the FUSE watchdog never acts (FUSE is fine — there is nothing for it
// to recover), and the legacy path's own alive-case backstop is the
// 18-tick/180s HARD remount that runs `sudo umount -f` + the passwordless-
// sudo-ONLY mount tier — which fails forever on machines without the scoped
// sudoers entry. Net effect in the field: the volume stays down until the
// app is restarted.
//
// The fix is a SEPARATE decision path that engages only when the situation
// is unambiguous and a remount is provably safe:
//
//	(a) the mountpoint is genuinely ABSENT from the kernel mount table —
//	    checkNFS's "not mounted" verdict, which comes from reading the
//	    mount table (or the mountpoint directory being gone), NEVER from a
//	    probe timeout. Stale/wedged/slow mounts stay the FUSE watchdog's
//	    territory and this path stands down (Absent=false resets it).
//	    "Something else mounted on the mountpoint" is structurally
//	    excluded too: any filesystem mounted at the path keeps it in the
//	    mount table, so Absent stays false — and the wired mount routine
//	    (mountNFSWithPrompt) independently refuses a busy mountpoint.
//	(b) juicefs/FUSE is healthy (raw FUSE probe passed + the juicefs
//	    process tree is alive), and
//	(c) our own NFS listener (127.0.0.1:<port>) accepts TCP — a remount
//	    against a dead server would just wedge the kernel client.
//
// After NFSAbsentRemountTicks consecutive such ticks (default 3 ≈ 30s;
// env JM_NFS_AUTOREMOUNT_TICKS) it re-runs the SAME two-tier mount
// machinery boot uses (passwordless sudo → bounded 180s admin prompt;
// wired from bridge via EnableNFSAbsentRemount — no new mount routine).
// Guards:
//
//   - never while the user is in offline mode (pin.IsOffline());
//   - kill switch: JM_NFS_AUTOREMOUNT=0 disables this path entirely
//     (the legacy juicefs-dead remount path is NOT affected);
//   - exponential backoff after failures (1m → 2m → … → 30m cap) so the
//     kernel-haunted-mountpoint EBUSY case can never hot-loop or spam
//     admin prompts — each failure logs a clear "will retry in X" line;
//   - single-flight: while one attempt is in flight (possibly parked on
//     the 180s-bounded prompt) no second attempt can start.
//
// Log lines from this path all carry the "nfs-layer auto-remount" prefix so
// they can never be confused with the legacy "nfs auto-remount" (juicefs-
// dead) path or the FUSE watchdog.

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Tunables for the absent-mount recovery. Vars so tests can override.
var (
	// NFSAbsentRemountTicks is the number of consecutive qualifying ticks
	// (10s apart) before the recovery fires. Env JM_NFS_AUTOREMOUNT_TICKS
	// overrides it at monitor construction (New).
	NFSAbsentRemountTicks = 3

	// NFSAbsentRemountBackoffBase/Cap shape the exponential backoff after
	// failed attempts: base<<(failures-1), capped. The cap keeps a
	// permanently-failing mountpoint (haunted EBUSY) down to one quiet,
	// clearly-logged attempt per half hour instead of a hot loop.
	NFSAbsentRemountBackoffBase = 1 * time.Minute
	NFSAbsentRemountBackoffCap  = 30 * time.Minute

	// nfsServerUpFn probes our own NFS listener. Indirected so the
	// decision tests inject a deterministic answer; production never
	// reassigns it.
	nfsServerUpFn = nfsServerUp
)

// nfsMsgNotMounted is checkNFS's genuine-absence verdict (mountpoint absent
// from the kernel mount table, or the mountpoint directory itself gone).
// The Swift side keys on this exact string for the "Mount Now" remedy
// (LB-2), and the absent-mount recovery keys on it here — it is the ONLY
// NFS failure message that means "nothing is mounted" rather than
// "something is mounted but sick".
const nfsMsgNotMounted = "not mounted"

// nfsAutoRemountEnabled is the kill switch: JM_NFS_AUTOREMOUNT=0 disables
// the absent-mount recovery path (and only it). Read per tick so tests can
// flip it with t.Setenv.
func nfsAutoRemountEnabled() bool {
	return os.Getenv("JM_NFS_AUTOREMOUNT") != "0"
}

// absentRemountTicksFromEnv resolves the consecutive-tick threshold:
// JM_NFS_AUTOREMOUNT_TICKS when set to a positive integer, else the
// NFSAbsentRemountTicks default.
func absentRemountTicksFromEnv() int {
	if v := os.Getenv("JM_NFS_AUTOREMOUNT_TICKS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return NFSAbsentRemountTicks
}

// nfsAbsentInputs is one tick's evidence, fully resolved by the caller so
// the decision itself is pure and table-testable.
type nfsAbsentInputs struct {
	Absent       bool // mountpoint verified absent from the mount table (never a timeout)
	Healthy      bool // the NFS component probe passed outright
	JuiceFSAlive bool // raw FUSE probe healthy AND juicefs process tree alive
	ServerUp     bool // our own NFS listener accepts TCP
	Offline      bool // user/auto offline mode engaged
	Enabled      bool // kill switch JM_NFS_AUTOREMOUNT != "0"
	Now          time.Time
}

// nfsAbsentState is the recovery's decision state. All fields are guarded
// by the owning HealthMonitor's mu; tick itself takes no locks.
type nfsAbsentState struct {
	streak      int       // consecutive fully-qualifying ticks
	failures    int       // consecutive failed remount attempts (drives backoff)
	nextAttempt time.Time // earliest next attempt; zero = no backoff pending
	inProgress  bool      // an attempt is in flight (single-flight)
}

// tick folds one tick of evidence into the state and answers whether to
// fire the remount now. The reason string is for logging only.
//
// Truth table (threshold = N):
//
//	Healthy                          → full reset (streak+failures+backoff), no fire
//	!Absent (stale/wedged/slow)      → streak reset, no fire  (FUSE watchdog territory)
//	Absent && !Enabled               → streak reset, no fire  (kill switch)
//	Absent && Offline                → streak reset, no fire
//	Absent && !JuiceFSAlive          → streak reset, no fire  (legacy dead-remount path owns it)
//	Absent && !ServerUp              → streak reset, no fire
//	Absent && all-of-the-above OK    → streak++
//	    streak < N                   → no fire (arming)
//	    attempt in flight            → no fire (single-flight)
//	    Now < nextAttempt            → no fire (backoff)
//	    else                         → FIRE
func (s *nfsAbsentState) tick(in nfsAbsentInputs, threshold int) (fire bool, reason string) {
	if in.Healthy {
		s.streak, s.failures, s.nextAttempt = 0, 0, time.Time{}
		return false, "healthy"
	}
	if !in.Absent {
		// Unhealthy but PRESENT in the mount table (stale/wedged/slow) — a
		// different disease. Only reset the streak; keep failure backoff so
		// an absent↔stale flap can't bypass it.
		s.streak = 0
		return false, "not absent (stale/wedged — fuse watchdog territory)"
	}
	if !in.Enabled {
		s.streak = 0
		return false, "disabled (JM_NFS_AUTOREMOUNT=0)"
	}
	if in.Offline {
		s.streak = 0
		return false, "offline mode"
	}
	if !in.JuiceFSAlive {
		s.streak = 0
		return false, "juicefs/fuse not healthy (legacy remount path owns the dead case)"
	}
	if !in.ServerUp {
		s.streak = 0
		return false, "nfs server listener down"
	}
	s.streak++
	if s.streak < threshold {
		return false, "arming (streak below threshold)"
	}
	if s.inProgress {
		return false, "attempt already in flight"
	}
	if !s.nextAttempt.IsZero() && in.Now.Before(s.nextAttempt) {
		return false, "backing off after failure"
	}
	return true, "fire"
}

// absentBackoff returns the wait before the next attempt after `failures`
// consecutive failed remounts: base<<(failures-1), capped. Shift-overflow
// safe for absurd failure counts.
func absentBackoff(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	// 2^20 × base is astronomically past any sane cap — clamp the shift so
	// the multiplication below can't overflow into a negative Duration.
	if failures > 20 {
		return NFSAbsentRemountBackoffCap
	}
	d := NFSAbsentRemountBackoffBase << (failures - 1)
	if d <= 0 || d > NFSAbsentRemountBackoffCap {
		return NFSAbsentRemountBackoffCap
	}
	return d
}

// isMountBusyError reports whether a mount failure is the EBUSY /
// "Resource busy" family — the kernel-haunted-mountpoint case that gets
// the dedicated "mountpoint busy" log line.
func isMountBusyError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.EBUSY) ||
		strings.Contains(strings.ToLower(err.Error()), "busy")
}

// nfsServerUp is the production listener probe: one bounded loopback TCP
// connect to the NFS server's listen address. Runs only on already-
// qualifying ticks (mount absent), never on the happy path.
func nfsServerUp(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
