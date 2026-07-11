package nfs

import (
	"os"
)

// nfsTrace (JM_NFS_TRACE=1) turns on high-volume per-RPC and per-attr
// diagnostics for chasing client-side vnode invalidation (fileid flaps,
// type flips). Diagnostic-only: keep every call site behind this flag —
// the attr path runs per READDIRPLUS entry, so an unconditional log there
// would melt a directory listing.
var nfsTrace = os.Getenv("JM_NFS_TRACE") == "1"
