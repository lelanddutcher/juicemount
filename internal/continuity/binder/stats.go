package binder

import "time"

// Stats is the binder's observation state. Complete is always false in
// this slice: changes made before ObservedSince (the Manager was down) or
// lost to Dropped, Discarded, Failed, Unresolved, TooManyLinks or
// Resubscribes are missing
// from the catalog, and only a rescan (a later slice) can find them.
type Stats struct {
	ObservedSince time.Time `json:"observed_since,omitzero"` // first subscription acknowledged
	Connected     bool      `json:"connected"`               // the volume is open
	Disabled      bool      `json:"disabled"`
	Queued        int       `json:"queued"`
	QueueSize     int       `json:"queue_size"`
	Received      uint64    `json:"received"`
	Handled       uint64    `json:"handled"`        // taken off the queue and finished
	Applied       uint64    `json:"applied"`        // catalog operations committed
	Ignored       uint64    `json:"ignored"`        // not a visible regular file, malformed, or gone again
	Dropped       uint64    `json:"dropped"`        // queue full or stopped: change missed
	Discarded     uint64    `json:"discarded"`      // still queued at stop: change missed
	Failed        uint64    `json:"failed"`         // volume or catalog error: change missed
	Retried       uint64    `json:"retried"`        // events re-run after an unknown volume state
	Unresolved    uint64    `json:"unresolved"`     // delete naming no inode the binder knew
	TooManyLinks  uint64    `json:"too_many_links"` // over MaxLinks names to list: left unrecorded
	Resubscribes  uint64    `json:"resubscribes"`
	// PathCacheEvictions: recent paths forgotten, so a file later replaced
	// at one of them (rename over it) is not noticed.
	PathCacheEvictions uint64 `json:"path_cache_evictions"`
	// Continued: replaced files whose asset the new file continued (a save
	// by rename); Merged: of those, the new file's provisional asset was
	// retired into it. NotContinued: replacements left as two assets (the
	// new file is hard-linked or has an established asset of its own).
	// NotContinuedNoRename: of those, no rename onto the path was seen (a
	// new file where one vanished). PairsExhausted: restored files left
	// out of the catalog, their inode having used all MaxGenerations pairs.
	Continued            uint64 `json:"continued"`
	Merged               uint64 `json:"merged"`
	NotContinued         uint64 `json:"not_continued"`
	NotContinuedNoRename uint64 `json:"not_continued_no_rename"`
	PairsExhausted       uint64 `json:"pairs_exhausted"`
	// Touched: update events that moved a file's hint sequence (its
	// recency marker, touch.go); TouchesSkipped: coalesced or over the
	// rate limit.
	Touched        uint64 `json:"touched"`
	TouchesSkipped uint64 `json:"touches_skipped"`
	Complete       bool   `json:"complete"`
}

func (b *Binder) Stats() Stats {
	s := Stats{
		Connected:    b.vol.Load() != nil,
		Disabled:     b.disabled.Load(),
		Queued:       len(b.queue),
		QueueSize:    cap(b.queue),
		Received:     b.received.Load(),
		Handled:      b.handled.Load(),
		Applied:      b.applied.Load(),
		Ignored:      b.ignored.Load(),
		Dropped:      b.dropped.Load(),
		Discarded:    b.discarded.Load(),
		Retried:      b.retried.Load(),
		Failed:       b.failed.Load(),
		Unresolved:   b.unresolved.Load(),
		TooManyLinks: b.tooManyLinks.Load(),
		Resubscribes: b.resubscribes.Load(),

		PathCacheEvictions:   b.paths.evictions.Load(),
		Continued:            b.continued.Load(),
		Merged:               b.merged.Load(),
		NotContinued:         b.notContinued.Load(),
		NotContinuedNoRename: b.noRename.Load(),
		PairsExhausted:       b.pairsExhausted.Load(),
		Touched:              b.touched.Load(),
		TouchesSkipped:       b.touchSkipped.Load(),
	}
	if ns := b.observedSince.Load(); ns != 0 {
		s.ObservedSince = time.Unix(0, ns).UTC()
	}
	return s
}
