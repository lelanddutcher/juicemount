package main

import (
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// A read-only admission for the automatic NFS sentinel. It is not authority
// for ordinary user writes or for creating a mount. The only globalMu section
// copies pointers; Session and FUSE observations run after releasing it.
type nfsWriteProbeSource struct {
	owner    *bridgeCacheOwner
	session  *cache.Session
	fusePath string
}

type nfsWriteProbeAdmission struct {
	nfsWriteProbeSource
	epoch uint64
}

type nfsWriteProbeAdmissionDeps struct {
	current func() nfsWriteProbeSource
	offline func() bool
	remote  func(*cache.Session) (uint64, bool)
	fuse    func(string) bool
}

func productionNFSWriteProbeAdmissionDeps() nfsWriteProbeAdmissionDeps {
	return nfsWriteProbeAdmissionDeps{
		current: func() nfsWriteProbeSource {
			globalMu.Lock()
			defer globalMu.Unlock()
			o := globalCacheOwner
			var s *cache.Session
			if o != nil {
				s = o.session
			}
			return nfsWriteProbeSource{owner: o, session: s, fusePath: globalFUSEPath}
		},
		offline: pin.IsOffline,
		remote:  (*cache.Session).RemoteObservation,
		fuse: func(path string) bool {
			if path == "" {
				return false
			}
			// Fresh avoids a prior-generation two-second cached success. Inert
			// and disabled gates are not positive mount-identity evidence.
			ok, reason := pin.FUSEIdentityFresh()
			return ok && reason == "ok"
		},
	}
}

func (d nfsWriteProbeAdmissionDeps) capture(generation uint64) (nfsWriteProbeAdmission, bool) {
	if !nfsWriteVerifierCurrent(generation) || d.offline() {
		return nfsWriteProbeAdmission{}, false
	}
	source := d.current()
	if source.owner == nil || source.session == nil || source.fusePath == "" || source.owner.session != source.session {
		return nfsWriteProbeAdmission{}, false
	}
	epoch, ready := d.remote(source.session)
	if !ready || !d.fuse(source.fusePath) || !nfsWriteVerifierCurrent(generation) || d.offline() {
		return nfsWriteProbeAdmission{}, false
	}
	current := d.current()
	if current != source {
		return nfsWriteProbeAdmission{}, false
	}
	currentEpoch, ready := d.remote(source.session)
	if !ready || currentEpoch != epoch || !nfsWriteVerifierCurrent(generation) || d.offline() {
		return nfsWriteProbeAdmission{}, false
	}
	return nfsWriteProbeAdmission{nfsWriteProbeSource: source, epoch: epoch}, true
}

func (d nfsWriteProbeAdmissionDeps) stillCurrent(generation uint64, original nfsWriteProbeAdmission) bool {
	current, ok := d.capture(generation)
	return ok && current.nfsWriteProbeSource == original.nfsWriteProbeSource && current.epoch == original.epoch
}
