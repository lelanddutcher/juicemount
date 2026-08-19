package main

import (
	"os"
	"runtime"
	"strconv"

	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// enableContentionProfilers turns on the mutex and block profilers when asked.
//
// net/http/pprof publishes /debug/pprof/mutex and /debug/pprof/block
// unconditionally, but the RUNTIME samples neither unless it is told to, so
// both endpoints answer every request with "sampling period=0" and no samples.
// Nothing in this binary ever told it to, which made the only instrument that
// can see lock contention permanently blind.
//
// Both default to OFF because both are not free: the mutex profiler adds work
// to contended unlocks, and the block profiler to every blocking operation
// including channel sends and network waits. A shipped mount daemon should not
// pay that to answer a question nobody is asking.
//
//	JM_MUTEX_PROFILE_FRACTION=1   sample every contention event (5 or 10 is
//	                              plenty on a busy path and much cheaper)
//	JM_BLOCK_PROFILE_RATE=10000   sample one blocking event per 10us of delay;
//	                              1 samples everything, 0 disables
func enableContentionProfilers() {
	if v := os.Getenv("JM_MUTEX_PROFILE_FRACTION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			runtime.SetMutexProfileFraction(n)
			jmlog.Info("mutex profiler enabled", "fraction", n)
		}
	}
	if v := os.Getenv("JM_BLOCK_PROFILE_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			runtime.SetBlockProfileRate(n)
			jmlog.Info("block profiler enabled", "rate_ns", n)
		}
	}
}
