package nfs

import (
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
)

// deriv_clobber_guard.go — a contributed derivative may not silently replace
// another producer's derivative.
//
// THE INCIDENT (live, 2026-08-11). Fourteen ClipLogger derivative uploads sat
// in the spool retrying `permission denied`. The cause was a directory-mode bug
// (see inheritDirOwnership in internal/derivatives/openat.go) and the obvious
// fix was to make the directory writable. That fix ALONE would have been a
// disaster, because every one of the fourteen was a NAME COLLISION with an
// artifact the farm had already produced, and the two artifacts were not the
// same thing:
//
//	waveform.json, inode 2313833   farm            ClipLogger
//	  samples_per_pixel            1,024           12,730
//	  length                       149,166         2,000
//	  data points                  298,332         4,000
//	  size                         670,104 B       11,541 B
//
// Both are valid audiowaveform v2 with identical schema. The client's is a
// 2,000-pixel preview; the farm's is full resolution. Same filename. Granting
// the write permission by itself would have replaced up to 12,016
// full-resolution waveforms with previews, silently, and downsampling
// 149k -> 2k is not reversible.
//
// WHY THERE WAS NOTHING TO STOP IT. Writing through the mount bypasses every
// guard on POST /derivatives/register — internal/farm/sidecar.go:129 documents
// that bypass as a CRITICAL finding, but only for manifest ROWS. Blobs had no
// equivalent check at all. So the permission error was, accidentally, the only
// thing protecting the data.
//
// THE RULE. Inside the derivatives tree only, a drain that would overwrite an
// existing blob belonging to a DIFFERENT owner is refused permanently, with the
// collision named. Everywhere else on the volume, overwriting is ordinary file
// behaviour and is untouched.
//
// Owner is the discriminator because it is exactly what distinguishes the two
// producers here — the farm writes as root from a container, the client writes
// as the logged-in user — and it costs one Lstat on a path that is already
// doing a create. It is not a security control: anything that can write the
// tree can still write a name nobody else has taken. It is a data-loss guard,
// and it fails toward keeping what is already there.
//
// A producer replacing its OWN blob is untouched: re-running the farm still
// updates farm artifacts, and a client still updates its own.

// derivTreePrefix is the mount-relative root of the shared derivative tree.
const derivTreePrefix = ".juicemount/derivatives/"

// derivClobberEUID resolves "who am I". A func var only so a test can stand in
// for the OTHER producer without needing root to create a root-owned file —
// the collision this guards is between a container running as root and a
// desktop app running as a user, which no unprivileged test can stage for real.
var derivClobberEUID = os.Geteuid

// isDerivativeBlobPath reports whether an NFS-relative path names a blob inside
// the shared derivatives tree.
func isDerivativeBlobPath(nfsPath string) bool {
	clean := strings.TrimPrefix(path.Clean("/"+nfsPath), "/")
	return strings.HasPrefix(clean, derivTreePrefix)
}

// errDerivClobber reports a refused overwrite. Permanent, not retryable:
// retrying cannot make the collision go away, and burning the attempt budget
// would bury the reason under a generic "failed".
type errDerivClobber struct {
	path      string
	existing  int
	incoming  int
	existSize int64
	newSize   int64
}

// DeclinedPrefix marks a terminal disposition that is a POLICY DECISION rather
// than a failure, so the control plane can report it as such.
//
// WHY THIS EXISTS. A clobber refusal is the guard working: it preserved a farm
// artifact instead of letting a client preview replace it. But it takes the
// same failPermanent path as a genuine error, so it lands in `failed` and
// `failed_files` and is indistinguishable from breakage. Measured 2026-08-17:
// after the legacy directory-ownership repair removed the permission barrier,
// the guard became the visible one and the spool reported failed=333 /
// failed_files=111 — every one of them a deliberate "kept the farm's 171 KB
// waveform rather than the client's preview". The founder reasonably read that
// as uploads still failing.
//
// A PREFIX on the reason rather than a new drain_state: the schema pins
// drain_state with a CHECK constraint (spool_schema.go) and several queries
// filter on the state set, so adding one is a migration plus an audit of every
// filter. The disposition, retry behaviour and capacity accounting are all
// already correct — only the REPORTING is wrong, so only the reporting changes.
const DeclinedPrefix = "declined: "

// isDeclinedReason reports whether a terminal drain reason is a POLICY DECISION
// rather than a failure. Extracted so the classification is testable on its own:
// inline in the status walk it had no coverage, and an attempt to neuter it
// there broke the build instead of the assertion — an inconclusive check.
func isDeclinedReason(reason string) bool {
	return strings.HasPrefix(reason, DeclinedPrefix)
}

func (e *errDerivClobber) Error() string {
	return DeclinedPrefix + fmt.Sprintf("refusing to overwrite %s: it belongs to uid %d (%d bytes) and this "+
		"contribution is from uid %d (%d bytes). Two producers write this tree and a "+
		"derivative of the same kind is not necessarily the same artifact — the farm's "+
		"waveform is full resolution where a client's is a preview. Preserving the "+
		"existing blob; give the contribution its own kind name if both should exist",
		e.path, e.existing, e.existSize, e.incoming, e.newSize)
}

// checkDerivClobber reports an error when writing dest would replace an
// existing derivative blob owned by someone else.
//
// Returns nil for every case that is not a cross-owner collision inside the
// derivatives tree: a path elsewhere on the volume, a destination that does not
// exist yet, a destination this same producer owns, or any stat we cannot
// resolve. Unresolvable ownership deliberately ALLOWS the write — this guard
// exists to stop a known, measured data downgrade, not to become a new way for
// drains to fail on a filesystem that reports ownership differently.
func checkDerivClobber(nfsPath, dest, incomingPath string, incomingSize int64) error {
	if !isDerivativeBlobPath(nfsPath) {
		return nil
	}
	fi, err := os.Lstat(dest)
	if err != nil {
		return nil // does not exist (the normal case) or unreadable — let the write proceed
	}
	if fi.IsDir() {
		return nil
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	mine := derivClobberEUID()
	if int(st.Uid) == mine {
		return nil // our own earlier blob — updating it is not a clobber
	}
	// A cross-owner collision on a waveform is decided by RESOLUTION, not by
	// which producer got there first. Ownership was only ever a proxy for "the
	// farm's is full resolution and the client's is a preview", and that proxy
	// is false for any clip under ~43 s (see deriv_waveform_quality.go). Fail
	// CLOSED: unreadable, unparseable, or not-the-same-audio all keep the
	// existing blob, because losing detail is the unrecoverable direction.
	if isWaveformBlob(nfsPath) && incomingPath != "" {
		if finer, ok := waveformIsFiner(incomingPath, dest); ok && finer {
			return nil
		}
	}
	return &errDerivClobber{
		path:      nfsPath,
		existing:  int(st.Uid),
		incoming:  mine,
		existSize: fi.Size(),
		newSize:   incomingSize,
	}
}
