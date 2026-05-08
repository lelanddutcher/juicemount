package metrics

import "time"

// NFS program/procedure constants. Mirrored from internal/nfs to avoid
// pulling that package into a circular import.
const (
	nfsProgram = 100003

	procGetAttr     = 1
	procSetAttr     = 2
	procLookup      = 3
	procAccess      = 4
	procRead        = 6
	procWrite       = 7
	procCreate      = 8
	procMkdir       = 9
	procRemove      = 12
	procRmdir       = 13
	procRename      = 14
	procReadDir     = 16
	procReadDirPlus = 17
	procFSStat      = 18
	procFSInfo      = 19
	procPathConf    = 20
	procCommit      = 21
)

// rpcTypeFor maps a (program, procedure) pair to a tracked RPCType.
// Anything we don't explicitly map is bucketed under RPCOther, which
// keeps the histogram bounded and avoids polluting /metrics with noise.
func rpcTypeFor(program, proc uint32) RPCType {
	if program != nfsProgram {
		return RPCOther
	}
	switch proc {
	case procGetAttr:
		return RPCGetAttr
	case procSetAttr:
		return RPCSetAttr
	case procLookup:
		return RPCLookup
	case procAccess:
		return RPCAccess
	case procRead:
		return RPCRead
	case procWrite:
		return RPCWrite
	case procCreate:
		return RPCCreate
	case procMkdir:
		return RPCMkdir
	case procRemove:
		return RPCRemove
	case procRmdir:
		return RPCRmdir
	case procRename:
		return RPCRename
	case procReadDir:
		return RPCReadDir
	case procReadDirPlus:
		return RPCReadDirPlus
	case procFSStat:
		return RPCFSStat
	case procFSInfo:
		return RPCFSInfo
	case procPathConf:
		return RPCPathConf
	case procCommit:
		return RPCCommit
	default:
		return RPCOther
	}
}

// ObserveRPC is the function that should be wired into
// nfslib.SetObserver(). It records per-RPC timing into the default
// registry. Callers wanting a custom registry can build their own
// closure by using rpcTypeFor + Registry.Observe.
func ObserveRPC(program, proc uint32, elapsed time.Duration, err error) {
	Default().Observe(rpcTypeFor(program, proc), elapsed, err)
}
