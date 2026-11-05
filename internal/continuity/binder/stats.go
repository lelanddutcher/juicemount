package binder

import "time"

// Stats is the binder's observation state. Complete is always false in this
// slice: changes made before ObservedSince (the Manager was down) or lost to
// Dropped, Failed, Unresolved or Resubscribes are missing from the catalog,
// and only a rescan (a later slice) can find them.
type Stats struct {
	ObservedSince time.Time `json:"observed_since,omitzero"` // first subscription acknowledged
	Connected     bool      `json:"connected"`               // the volume is open
	Disabled      bool      `json:"disabled"`
	Queued        int       `json:"queued"`
	QueueSize     int       `json:"queue_size"`
	Received      uint64    `json:"received"`
	Handled       uint64    `json:"handled"`    // taken off the queue and finished
	Applied       uint64    `json:"applied"`    // catalog operations committed
	Ignored       uint64    `json:"ignored"`    // not a visible regular file, malformed, or gone again
	Dropped       uint64    `json:"dropped"`    // queue full: change missed
	Failed        uint64    `json:"failed"`     // volume or catalog error: change missed
	Unresolved    uint64    `json:"unresolved"` // delete naming no inode the binder knew
	Resubscribes  uint64    `json:"resubscribes"`
	Complete      bool      `json:"complete"`
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
		Failed:       b.failed.Load(),
		Unresolved:   b.unresolved.Load(),
		Resubscribes: b.resubscribes.Load(),
	}
	if ns := b.observedSince.Load(); ns != 0 {
		s.ObservedSince = time.Unix(0, ns).UTC()
	}
	return s
}
