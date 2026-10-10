package health

// FUSEOwnerSnapshot is a coherent, value-only view of the original manager.
// A remote owner publishes it after lifecycle work, then serves queries from
// that publication while Mount or Stop may be holding fm.mu.
type FUSEOwnerSnapshot struct {
	MountPath           string
	CacheSizeMB         string
	MetricsAddr         string
	Attempted           bool
	CacheFreeFloorBytes int64
	CacheVolumePath     string
	CacheVolumeDev      uint64
	CachePolicyKnown    bool
}

// FUSESteadyEvidence is a local observation of the retained child and its
// exact kernel mount. A missing receipt or busy manager lock is unproven.
type FUSESteadyEvidence struct {
	Ready       bool
	ChildAlive  bool
	ChildProven bool
}

// SteadyEvidence never opens the FUSE root or a remote backend. The manager's
// existing steady probe checks its original Wait and complete mount snapshot.
func (fm *FUSEManager) SteadyEvidence() FUSESteadyEvidence {
	if fm == nil || !fm.mu.TryLock() {
		return FUSESteadyEvidence{}
	}
	defer fm.mu.Unlock()
	r := fm.activeReceipt
	if r == nil || r.owner != fm || r.process == nil || r.process.Pid <= 1 || r.waitDone == nil ||
		r.generation != fm.generation {
		return FUSESteadyEvidence{}
	}
	e := FUSESteadyEvidence{ChildProven: true, ChildAlive: !waitFUSEChannel(r.waitDone, 0)}
	if e.ChildAlive && !fm.stopping.Load() {
		e.Ready = fm.isMountedSteadyLocked()
	}
	return e
}

func (fm *FUSEManager) OwnerSnapshot() FUSEOwnerSnapshot {
	if fm == nil {
		return FUSEOwnerSnapshot{}
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	metrics := fm.cfg.FUSEMetricsAddr
	if metrics == "" {
		metrics = DefaultFUSEMetricsAddr
	}
	return FUSEOwnerSnapshot{
		MountPath: fm.cfg.MountPoint, CacheSizeMB: fm.cfg.CacheSize,
		MetricsAddr: metrics, Attempted: fm.generation != nil && fm.generation.spent,
		CacheFreeFloorBytes: fm.cacheFloorBytes, CacheVolumePath: fm.cacheVolumePath,
		CacheVolumeDev: fm.cacheVolumeDev, CachePolicyKnown: fm.cachePolicyKnown,
	}
}
