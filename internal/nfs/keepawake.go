package nfs

import (
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

// keepAwake prevents macOS system idle sleep while the NFS mount is actively in
// use, so a closed-lid / idle Mac can't suspend this process mid-operation.
//
// WHY: with the laptop LID CLOSED (clamshell), macOS idle-sleeps after its timer
// and SUSPENDS the JuiceMount process. The loopback NFS server then goes silent
// and the kernel client reports the mount gone — Finder shows "device
// disappeared" and ABORTS an in-flight copy. Observed 2026-06-15 on a real 500GB
// copy: a 131-second freeze in the app's heartbeat killed the copy at ~70GB,
// with `clamshelled: YES` in the system log and only DISPLAY sleep blocked (not
// system idle sleep). "We can't lose photos."
//
// We hold a `caffeinate -i` child (kIOPMAssertionTypePreventUserIdleSystemSleep)
// whenever there has been keep-awake-worthy activity recently — NFS DATA
// TRANSFER (an active copy/read-back) OR spool DRAIN PROGRESS (a post-copy /
// post-reconnect upload still flushing to the backend; see NoteDrainProgress) —
// and release it after a couple minutes of quiet so the Mac can still sleep when
// nothing is moving. The drain-tail hold is what lets a user reconnect, close
// the lid, and walk away while the backlog finishes uploading. `-w <ourpid>`
// makes caffeinate exit if we die (no orphan on SIGKILL).
//
// NOTE: the trigger is DataXferActivity() (READ/WRITE byte movement), NOT total
// RPC count. The macOS NFS client emits a steady low-rate trickle of liveness/
// attr-refresh RPCs (GETATTR/FSSTAT) even on a fully idle mount — measured
// ~1 RPC/8s with zero user activity — so a total-RPC trigger would never go
// flat and would pin the Mac awake for as long as the mount exists. Keying off
// byte movement releases correctly on a genuinely idle mount while still
// holding the whole time a copy is in flight.

const (
	keepAwakeTick        = 15 * time.Second
	keepAwakeIdleRelease = 8 // 8 * 15s = 2 min of no data transfer → release
)

var keepAwakeOnce sync.Once

// startKeepAwake launches the activity-driven keep-awake loop exactly once per
// process. Safe to call from every Serve(); subsequent calls are no-ops.
func startKeepAwake() {
	keepAwakeOnce.Do(func() { go keepAwakeLoop() })
}

func keepAwakeLoop() {
	var caf *exec.Cmd
	// Pre-sample the baseline so the FIRST tick can already detect activity.
	// Initializing to a -1 sentinel would burn the first tick just priming the
	// baseline (active stays false because lastCount<0), pushing the earliest
	// possible acquisition to the 2nd tick (~30s) — an avoidable unprotected
	// head for a copy that starts within ~15s of process launch (auto-mount-
	// then-immediate-copy, server restart mid-copy). Sampling now means a copy
	// already in flight at startup acquires on tick 1 (~15s worst case).
	lastCount := DataXferActivity()
	idleTicks := 0
	pid := strconv.Itoa(os.Getpid())

	release := func() {
		if caf != nil && caf.Process != nil {
			_ = caf.Process.Kill()
			_ = caf.Wait()
		}
		caf = nil
	}
	defer release()

	t := time.NewTicker(keepAwakeTick)
	defer t.Stop()
	for range t.C {
		xfer := DataXferActivity()
		active := xfer > lastCount // counter advanced since last tick → bytes moved
		lastCount = xfer
		if active {
			idleTicks = 0
			if caf == nil {
				// -i: prevent system idle sleep. -w <pid>: exit when we exit.
				c := exec.Command("/usr/bin/caffeinate", "-i", "-w", pid)
				if err := c.Start(); err == nil {
					caf = c
					Log.Infof("keep-awake: holding (NFS copy/read-back active)")
				}
			}
		} else {
			idleTicks++
			if idleTicks >= keepAwakeIdleRelease && caf != nil {
				release()
				Log.Infof("keep-awake: released (no data transfer ~2m)")
			}
		}
	}
}
