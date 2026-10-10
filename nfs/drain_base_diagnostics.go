package nfs

import (
	"fmt"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// exactBaseMismatchFields describes only the four fields persisted for a
// PRESENT base. Mode was never saved with that base; current mode is useful
// context, but cannot be treated as a fifth identity check.
func exactBaseMismatchFields(base metadata.SpoolBase, current *unix.Stat_t) string {
	var changed []string
	if uint64(current.Ino) != base.Ino {
		changed = append(changed, "inode")
	}
	if current.Mtim.Nano() != base.MtimeNs {
		changed = append(changed, "mtime_ns")
	}
	if current.Ctim.Nano() != base.CtimeNs {
		changed = append(changed, "ctime_ns")
	}
	if current.Size != base.Len {
		changed = append(changed, "length")
	}
	return strings.Join(changed, ",")
}

// logExactBaseRefusal uses the Lstat already made by openDrainTarget. It does
// no new backend I/O and changes neither the decision nor the saved row.
func logExactBaseRefusal(row *metadata.SpoolRow, base metadata.SpoolBase, current *unix.Stat_t,
	acceptedEpoch uint64, lease leaseDecision, reason string) {
	jmlog.Warn("drain: exact base refused in-place write",
		"row_id", row.ID, "path", row.NFSPath, "reason", reason,
		"base_kind", base.Kind.String(), "mismatch_fields", exactBaseMismatchFields(base, current),
		"saved_inode", base.Ino, "saved_mtime_ns", base.MtimeNs,
		"saved_ctime_ns", base.CtimeNs, "saved_length", base.Len,
		"saved_mode", "unrecorded",
		"current_inode", uint64(current.Ino), "current_mtime_ns", current.Mtim.Nano(),
		"current_ctime_ns", current.Ctim.Nano(), "current_length", current.Size,
		"current_mode", fmt.Sprintf("%#o", current.Mode&0o7777),
		"accepted_epoch", acceptedEpoch, "lease_epoch", lease.epoch,
		"foreign_before_epoch", lease.foreignBefore, "lease_tainted", lease.taints(acceptedEpoch))
}
