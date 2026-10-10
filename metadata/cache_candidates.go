package metadata

import (
	"path"
)

// CacheCandidate is a post-commit hint, never path or content authority.
// Its generation prevents a buffered boot hint from targeting a later backend.
type CacheCandidate struct {
	Identity          CachedFileIdentity
	origin            *Store
	BackendGeneration uint64
}

const cacheCandidateMaxSize = 128 << 10
const cacheCandidatePerBatch = 16

type cacheCandidateSink struct{ offer func(CacheCandidate) }

// SetCacheCandidateSink is wired before any boot reconciliation. The callback
// must be nonblocking: reconciliation never waits for metadata preparation.
func (rc *RedisClient) SetCacheCandidateSink(offer func(CacheCandidate)) {
	rc.cacheCandidateMu.Lock()
	rc.cacheCandidateSink = cacheCandidateSink{offer: offer}
	rc.cacheCandidateMu.Unlock()
}

// MatchesStore fences a buffered hint to its originating Store instance.
// Present-row MirrorTokens deliberately do not carry a StoreID.
func (c CacheCandidate) MatchesStore(s *Store) bool { return c.origin != nil && c.origin == s }

// CacheCandidateCurrent is a local generation fence, not remote proof.
func (rc *RedisClient) CacheCandidateCurrent(c CacheCandidate) bool {
	rc.lifecycleMu.Lock()
	current := c.MatchesStore(rc.store) && rc.backendAdmittedLocked(c.BackendGeneration) && rc.backendGeneration == c.BackendGeneration
	rc.lifecycleMu.Unlock()
	if !current {
		return false
	}
	live, ok := rc.store.LookupCachedFileIdentity(c.Identity.Path)
	return ok && live == c.Identity
}

func (rc *RedisClient) offerCacheCandidates(entries []*Entry, token scanBaselineToken) {
	rc.cacheCandidateMu.RLock()
	offer := rc.cacheCandidateSink.offer
	rc.cacheCandidateMu.RUnlock()
	if offer == nil {
		return
	}
	n := 0
	for _, e := range entries {
		if n >= cacheCandidatePerBatch {
			return
		}
		if e == nil || e.IsDir || e.LocalOnly || !e.Mode.IsRegular() || e.Size <= 0 || e.Size > cacheCandidateMaxSize ||
			!(len(e.Name) > 2 && e.Name[:2] == "._" || path.Base(e.Path) == ".DS_Store") {
			continue
		}
		id, ok := rc.store.LookupCachedFileIdentity(e.Path)
		if !ok || id.Inode != e.Inode || id.Size != e.Size || id.MtimeNS != e.Mtime.UnixNano() {
			continue
		}
		c := CacheCandidate{Identity: id, origin: rc.store, BackendGeneration: token.backend}
		if rc.CacheCandidateCurrent(c) {
			offer(c)
		}
		n++
	}
}
