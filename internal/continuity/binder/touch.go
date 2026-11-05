package binder

import (
	"context"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

// Recency marker. A file saved in place keeps its inode and path, so no
// record changes and its hint's catalog sequence would only say when it was
// bound or last moved. Saved queries pick their candidates by that sequence
// (most recent first), so an "update" event for a bound file whose hint did
// not change re-puts the hint with its own value ("touch"): the catalog
// moves the hint's UpdatedSequence, and nothing else changes.
//
// Each touch is a permanent catalog event, so they are kept few: only
// paths Config.Touch accepts (the Manager: files under some saved query's
// folder), an inode bound, moved or touched within TouchInterval (default
// 10 min) is not touched again, and touches are limited to TouchRate a
// second (default 5, bursts of 50); the last two skips are counted
// (touches_skipped). The recent-inode memory holds at most
// defaultTouchMemory inodes.
const (
	defaultTouchInterval = 10 * time.Minute
	defaultTouchRate     = 5
	defaultTouchBurst    = 50
	defaultTouchMemory   = 65536
)

// recentInodes: inodes whose hint sequence moved recently (worker-only).
type recentInodes struct {
	at  map[uint64]time.Time
	max int
}

func (r *recentInodes) seen(ino uint64, now time.Time, within time.Duration) bool {
	t, ok := r.at[ino]
	return ok && now.Sub(t) < within
}

func (r *recentInodes) put(ino uint64, now time.Time, within time.Duration) {
	if len(r.at) >= r.max {
		for k, t := range r.at {
			if now.Sub(t) >= within {
				delete(r.at, k)
			}
		}
		if len(r.at) >= r.max {
			r.at = map[uint64]time.Time{}
		}
	}
	r.at[ino] = now
}

// touchAllowed takes a token from the touch budget (worker-only).
func (b *Binder) touchAllowed(now time.Time) bool {
	if !b.touchAt.IsZero() {
		b.touchTokens += now.Sub(b.touchAt).Seconds() * b.cfg.TouchRate
	}
	b.touchAt = now
	if b.touchTokens > float64(b.cfg.TouchBurst) {
		b.touchTokens = float64(b.cfg.TouchBurst)
	}
	if b.touchTokens < 1 {
		return false
	}
	b.touchTokens--
	return true
}

// touch moves the hint's sequence of a bound inode, if it was not moved
// within TouchInterval and the rate allows.
func (b *Binder) touch(ctx context.Context, v *volume, ino uint64) error {
	now := b.cfg.Now()
	if b.recent.seen(ino, now, b.cfg.TouchInterval) {
		b.touchSkipped.Add(1)
		return nil
	}
	if !b.touchAllowed(now) {
		b.touchSkipped.Add(1)
		return nil
	}
	err := b.converge(ctx, func() (*catalog.Operation, error) {
		s, err := b.records(ctx, v, ino)
		if err != nil || s.ino == nil || s.ino.Removed || s.hint == nil || s.hint.Removed {
			return nil, err
		}
		return &catalog.Operation{
			Version:   catalog.OperationVersion,
			LibraryID: b.library,
			ID:        operationID(v, ino, s.kind("touch"), seq(s.ino), seq(s.hint)),
			Actor:     Actor,
			Changes:   []catalog.Change{{PutLocation: &catalog.PutLocation{ID: s.hint.ID, Expected: expected(s.hint), Value: s.hint.Value}}},
		}, nil
	})
	if err == nil {
		b.touched.Add(1)
		b.recent.put(ino, now, b.cfg.TouchInterval)
	}
	return err
}
