package nfs

import (
	"fmt"
	"os"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// nfsLogBridge routes the go-nfs library's logger into jmlog.
//
// WHY THIS EXISTS (2026-08-03). The library's DefaultLogger writes via
// log.Printf → stderr. A GUI app started by LaunchServices has no stderr, so
// every line the NFS layer logged went to /dev/null — including the
// JM_NFS_TRACE per-RPC diagnostics and, worse, the library's own Errorf calls.
// The same trap is called out at health/fuse.go:1489 for the FUSE side.
//
// Consequence: JM_NFS_TRACE=1 was dead tooling in app builds. It only ever
// produced output for CLI runs, which is why chasing a Finder-visible bug with
// it appeared to emit nothing at all. Verified live before this fix: the flag
// was present in the process environment and TRACE lines still never reached
// juicemount.log or the macOS unified log.
//
// Level policy: default WarnLevel so the library's errors and warnings finally
// land in the real log without adding per-RPC volume; JM_NFS_TRACE=1 opens it
// to InfoLevel, which is the level the trace call sites use. Panic/Fatal are
// deliberately left to the embedded DefaultLogger so they keep their
// process-killing behaviour.
type nfsLogBridge struct {
	*nfslib.DefaultLogger
}

func (l *nfsLogBridge) Errorf(format string, args ...interface{}) {
	if l.GetLevel() < nfslib.ErrorLevel {
		return
	}
	jmlog.Error(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Warnf(format string, args ...interface{}) {
	if l.GetLevel() < nfslib.WarnLevel {
		return
	}
	jmlog.Warn(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Infof(format string, args ...interface{}) {
	if l.GetLevel() < nfslib.InfoLevel {
		return
	}
	jmlog.Info(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Debugf(format string, args ...interface{}) {
	if l.GetLevel() < nfslib.DebugLevel {
		return
	}
	jmlog.Debug(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Tracef(format string, args ...interface{}) {
	if l.GetLevel() < nfslib.TraceLevel {
		return
	}
	jmlog.Debug(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Printf(format string, args ...interface{}) {
	jmlog.Info(fmt.Sprintf(format, args...))
}

func (l *nfsLogBridge) Error(args ...interface{}) {
	if l.GetLevel() < nfslib.ErrorLevel {
		return
	}
	jmlog.Error(fmt.Sprint(args...))
}

func (l *nfsLogBridge) Warn(args ...interface{}) {
	if l.GetLevel() < nfslib.WarnLevel {
		return
	}
	jmlog.Warn(fmt.Sprint(args...))
}

func (l *nfsLogBridge) Info(args ...interface{}) {
	if l.GetLevel() < nfslib.InfoLevel {
		return
	}
	jmlog.Info(fmt.Sprint(args...))
}

func (l *nfsLogBridge) Debug(args ...interface{}) {
	if l.GetLevel() < nfslib.DebugLevel {
		return
	}
	jmlog.Debug(fmt.Sprint(args...))
}

func (l *nfsLogBridge) Trace(args ...interface{}) {
	if l.GetLevel() < nfslib.TraceLevel {
		return
	}
	jmlog.Debug(fmt.Sprint(args...))
}

func (l *nfsLogBridge) Print(args ...interface{}) {
	jmlog.Info(fmt.Sprint(args...))
}

// installNFSLogBridge points the library logger at jmlog. Safe to call more
// than once; the last call wins.
func installNFSLogBridge() {
	base := &nfslib.DefaultLogger{}
	if os.Getenv("JM_NFS_TRACE") == "1" {
		base.SetLevel(nfslib.InfoLevel)
	} else {
		base.SetLevel(nfslib.WarnLevel)
	}
	nfslib.SetLogger(&nfsLogBridge{DefaultLogger: base})
}
