package metadata

import (
	"iter"
	"log"
	"maps"
	"sort"
	"time"
)

// Capture, apply and cleanup all yield mu after a fixed amount of map work.
// In particular, one full-catalog RLock can also block readers behind a queued
// writer, so selection may not hold it for the entire map walk.
const evictionChunk = 256

type evictionCandidate struct {
	entry *Entry
	value Entry
}

type evictionAlias struct {
	inode     uint64
	candidate evictionCandidate
}

func (s *Store) markCachePublicationLocked(e *Entry) {
	s.nextCacheIdentity++
	e.cacheIdentity = s.nextCacheIdentity
}

func evictionOlder(a, b evictionCandidate) bool {
	if a.value.Mtime.Equal(b.value.Mtime) {
		return a.value.Path < b.value.Path
	}
	return a.value.Mtime.Before(b.value.Mtime)
}

// A max heap retains only the oldest excess entries. A small overage at the
// default 500k limit must not sort/copy/rebuild the entire serving catalog.
type evictionHeap []evictionCandidate

func (h *evictionHeap) retain(candidate evictionCandidate, limit int) {
	if len(*h) < limit {
		*h = append(*h, candidate)
		i := len(*h) - 1
		for i > 0 {
			p := (i - 1) / 2
			if !evictionOlder((*h)[p], (*h)[i]) {
				break
			}
			(*h)[p], (*h)[i] = (*h)[i], (*h)[p]
			i = p
		}
		return
	}
	if limit == 0 || !evictionOlder(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	for i := 0; ; {
		child := 2*i + 1
		if child >= len(*h) {
			break
		}
		if child+1 < len(*h) && evictionOlder((*h)[child], (*h)[child+1]) {
			child++
		}
		if !evictionOlder((*h)[i], (*h)[child]) {
			break
		}
		(*h)[i], (*h)[child] = (*h)[child], (*h)[i]
		i = child
	}
}

// selectEvictionCandidates never holds the publication gate or reads mutable
// Entry fields without mu. Map iteration is resumed only under RLock. Mutations
// between iterations are allowed; the initial length caps visits under churn.
// Open is the only whole-map replacement path and runs before Store is exposed.
func (s *Store) selectEvictionCandidates(pinned map[string]struct{}) evictionHeap {
	s.mu.RLock()
	paths, count, budget := s.pathCache, len(s.pathCache), s.maxCacheSize
	s.mu.RUnlock()
	if budget <= 0 || count <= budget {
		return nil
	}
	limit := count - budget
	chosen := make(evictionHeap, 0, limit)
	batch := make([]evictionCandidate, 0, evictionChunk)
	next, stop := iter.Pull2(maps.All(paths))
	defer stop()
	done := false
	for visited := 0; visited < count && !done; {
		batch = batch[:0]
		s.mu.RLock()
		for n := 0; n < evictionChunk && visited < count; n++ {
			_, e, ok := next()
			if !ok {
				done = true
				break
			}
			visited++
			if e != nil && !e.IsDir && !isPinned(pinned, e.Path) {
				batch = append(batch, evictionCandidate{e, *e})
			}
		}
		s.mu.RUnlock()
		for _, candidate := range batch {
			chosen.retain(candidate, limit)
		}
	}
	sort.Slice(chosen, func(i, j int) bool { return evictionOlder(chosen[i], chosen[j]) })
	return chosen
}

func (s *Store) captureEvictionAliases() []evictionAlias {
	s.mu.RLock()
	inodes, count := s.inodeCache, len(s.inodeCache)
	s.mu.RUnlock()
	next, stop := iter.Pull2(maps.All(inodes))
	defer stop()
	batch := make([]evictionAlias, 0, evictionChunk)
	var aliases []evictionAlias
	done := false
	for visited := 0; visited < count && !done; {
		batch = batch[:0]
		s.mu.RLock()
		for n := 0; n < evictionChunk && visited < count; n++ {
			inode, e, ok := next()
			if !ok {
				done = true
				break
			}
			visited++
			if e != nil && (inode != e.Inode || s.pathCache[e.Path] != e) {
				batch = append(batch, evictionAlias{inode, evictionCandidate{e, *e}})
			}
		}
		s.mu.RUnlock()
		// Slice growth and copying happen outside the serving lock.
		aliases = append(aliases, batch...)
	}
	return aliases
}

func (s *Store) applyEvictionCandidates(candidates evictionHeap, pinned map[string]struct{}) {
	for i := 0; i < len(candidates); i += evictionChunk {
		end := min(i+evictionChunk, len(candidates))
		s.cachePublicationMu.Lock()
		s.mu.Lock()
		for _, candidate := range candidates[i:end] {
			if len(s.pathCache) <= s.maxCacheSize {
				break
			}
			old := candidate.value
			current := s.pathCache[old.Path]
			// Direct legacy writers bypass the publication gate. Pointer AND complete
			// scalar/identity equality protect replacements and in-place mutations.
			if current != candidate.entry || current == nil || *current != old || s.inodeCache[old.Inode] != current || current.IsDir || isPinned(pinned, old.Path) {
				continue
			}
			s.cacheEvicted.Store(true) // before exposing any partial RAM children index
			s.parkEvictedShadowLocked(current, time.Now())
			s.subtreeRemoveLocked(current)
			s.removeFromChildrenIdx(current)
			delete(s.inodeCache, old.Inode)
			delete(s.pathCache, old.Path)
		}
		s.mu.Unlock()
		s.cachePublicationMu.Unlock()
		if s.evictionAfterApplyChunkHook != nil {
			s.evictionAfterApplyChunkHook()
		}
	}
}

func (s *Store) removeEvictionAliases(aliases []evictionAlias) {
	for i := 0; i < len(aliases); i += evictionChunk {
		end := min(i+evictionChunk, len(aliases))
		s.cachePublicationMu.Lock()
		s.mu.Lock()
		for _, alias := range aliases[i:end] {
			current := s.inodeCache[alias.inode]
			if current == alias.candidate.entry && current != nil && *current == alias.candidate.value && (alias.inode != current.Inode || s.pathCache[current.Path] != current) {
				delete(s.inodeCache, alias.inode)
			}
		}
		s.mu.Unlock()
		s.cachePublicationMu.Unlock()
	}
}

// A shared cursor makes cleanup bounded for every shadow producer, including
// direct Delete/Update paths while a pressure pass is yielding its locks.
type shadowExpiryCursor struct {
	next      func() (uint64, evictedShadow, bool)
	stop      func()
	remaining int
	deadline  time.Time
}

// Caller holds mu; true means the finite captured pass has more work.
func (s *Store) sweepShadowChunkLocked(now time.Time) bool {
	if s.shadowSweep == nil {
		if len(s.recentlyEvicted) <= 1024 || s.shadowSweepAfter.After(now) {
			return false
		}
		next, stop := iter.Pull2(maps.All(s.recentlyEvicted))
		s.shadowSweep = &shadowExpiryCursor{next, stop, len(s.recentlyEvicted), now.Add(ShadowTTL)}
	}
	cursor := s.shadowSweep
	for n := 0; n < evictionChunk && cursor.remaining > 0; n++ {
		inode, record, ok := cursor.next()
		if !ok {
			cursor.remaining = 0
			break
		}
		cursor.remaining--
		if record.ExpiresAt.Before(now) {
			delete(s.recentlyEvicted, inode)
		} else if record.ExpiresAt.Before(cursor.deadline) {
			cursor.deadline = record.ExpiresAt
		}
	}
	if cursor.remaining > 0 {
		return true
	}
	cursor.stop()
	deadline := cursor.deadline
	if minimum := now.Add(shadowSweepInterval); deadline.Before(minimum) {
		deadline = minimum
	}
	s.shadowSweepAfter = deadline
	s.shadowSweep = nil
	return false
}

func (s *Store) sweepEvictionShadows(now time.Time) {
	for {
		s.mu.Lock()
		more := s.sweepShadowChunkLocked(now)
		s.mu.Unlock()
		if !more {
			return
		}
	}
}

// Pressure eviction is conservative under concurrent mutation: changed or
// newly published entries survive until a later pass. Without churn it retains
// all directories/pins and exactly the newest files that fit the entry budget.
// Existing map bucket allocations are retained; this is not a hard byte cap.
func (s *Store) evictCacheChunks() {
	s.mu.RLock()
	needed := s.maxCacheSize > 0 && len(s.pathCache) > s.maxCacheSize
	s.mu.RUnlock()
	if !needed {
		return
	}
	pinned, err := s.pinnedSetPublic()
	if err != nil {
		log.Printf("[metadata] eviction: pin-checker error, skipping cycle: %v", err)
		return
	}
	aliases := s.captureEvictionAliases()
	candidates := s.selectEvictionCandidates(pinned)
	if s.evictionAfterSelectHook != nil {
		s.evictionAfterSelectHook()
	}
	// Refresh outside both serving locks after potentially lengthy selection.
	pinned, err = s.pinnedSetPublic()
	if err != nil {
		log.Printf("[metadata] eviction: pin-checker error, skipping cycle: %v", err)
		return
	}
	s.sweepEvictionShadows(time.Now())
	s.applyEvictionCandidates(candidates, pinned)
	s.removeEvictionAliases(aliases)
}
