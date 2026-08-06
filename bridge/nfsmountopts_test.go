package main

import (
	"strings"
	"testing"
)

// mutejukebox must be present in the mount options.
//
// We return NFS3ERR_JUKEBOX deliberately on several paths — the FUSE stat
// budget (errFUSETimeout), the FUSE data ceiling shedding at its ceiling, and
// the spool in-flight hole hold. Retrying is the correct client behaviour there
// and failing is not. But JUKEBOX is also the documented macOS trigger for the
// "server is not responding" alert (`man mount_nfs`), so without mutejukebox
// every deliberate hold ALSO pops "connection interrupted" at the user — the
// symptom chased twice already.
func TestNFSMountOptsMutesJukeboxAlert(t *testing.T) {
	opts := nfsMountOpts("12049")
	if !hasOpt(opts, "mutejukebox") {
		t.Errorf("mount options lack mutejukebox — every deliberate JUKEBOX hold "+
			"will surface as a Finder 'connection interrupted' alert.\ngot: %s", opts)
	}
}

// The options that carry a documented incident behind them must not be dropped
// by a careless edit to the format string. Each of these was set in response to
// a specific failure; a regression here is silent until a user hits it again.
func TestNFSMountOptsKeepsIncidentDrivenOptions(t *testing.T) {
	opts := nfsMountOpts("12049")
	required := map[string]string{
		"hard":       "a soft mount turns a timed-out mmap PAGEIN into SIGBUS, crashing NLEs (2026-06-15)",
		"intr":       "interruptible, so a wedged RPC does not make the process unkillable",
		"timeo=400":  "QA-36: a CREATE stall past the ~60s budget aborted Finder copies with error 100060",
		"retrans=2":  "bounds dead-server detection; inert under hard but load-bearing if hard is ever revisited",
		"nolocks":    "we do not serve NLM",
		"locallocks": "locking is handled client-side",
		"vers=3":     "the server speaks NFSv3",
		"tcp":        "UDP is not supported by this server",
	}
	for opt, why := range required {
		if !hasOpt(opts, opt) {
			t.Errorf("mount options lost %q\n  why it exists: %s\n  got: %s", opt, why, opts)
		}
	}
}

// The comment block above nfsMountOpts drifted from the code (it described
// timeo=200 long after QA-36 moved it to 400) and cost a re-read. This pins the
// value the format string actually carries, so the next change to it is
// deliberate rather than accidental.
func TestNFSMountOptsTimeoIsTheDocumentedValue(t *testing.T) {
	opts := nfsMountOpts("12049")
	if !hasOpt(opts, "timeo=400") {
		t.Errorf("timeo changed; update the policy comment above nfsMountOpts in the "+
			"SAME commit, and say what incident moved it.\ngot: %s", opts)
	}
}

// hasOpt matches a whole comma-separated option, so "timeo=400" cannot be
// satisfied by "timeo=4000" and "hard" cannot be satisfied by "hardlinks".
func hasOpt(opts, want string) bool {
	for _, o := range strings.Split(opts, ",") {
		if o == want {
			return true
		}
	}
	return false
}
