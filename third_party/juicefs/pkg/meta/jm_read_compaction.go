/*
 * JuiceMount: read compaction can be turned off (patch 0036).
 *
 * Upstream, a Read that finds 5 or more slices in a chunk starts a
 * compaction of that chunk in the background (base.go Read). The
 * compaction downloads every slice of the chunk, uploads the merged
 * result as one new slice, and then commits the new list in one metadata
 * transaction that first checks the head of the list did not change
 * (redis.go doCompactChunk). A new write appends to the list and passes
 * that check; another client's compaction of the same chunk replaces the
 * head and fails it. Then the result is "wasted" and deleted (base.go
 * compactChunk).
 *
 * A Mac abroad reads files that teammates edit. On a remote laptop
 * (2026-11-26, a 12 ms tunnel) each read of a 48 MiB file that a teammate
 * had written in 48 pieces moved about 50 MB down and 50 MB up. The
 * teammate wrote again and read the file back about 2 s later; its juicefs,
 * near the server, compacted the chunk first, so the Mac's result was
 * wasted every time. On a slow link the compaction takes minutes, so that
 * window is wider, and even a compaction that commits costs the far link
 * the whole chunk both ways. Compaction is never needed for a read to be
 * correct: the slice list is already the truth.
 *
 * With read compaction off, a Read never starts one. Unchanged: the
 * writer's own compaction when a chunk passes 99 or 350 slices and the
 * synchronous one at maxSlices (base.go Write), and `juicefs gc --compact`
 * (force). Those keep the slice count bounded.
 *
 * Set by the mount flag --no-read-compaction and by the JMLinkPolicy
 * control message (pkg/vfs/jm_link_policy.go, flag bit 2), so the app can
 * turn it on and off as the link class changes.
 */

package meta

// JMReadCompactionGate turns compaction started by a Read on or off.
type JMReadCompactionGate interface {
	// JMSetReadCompaction sets the gate and returns the value before.
	JMSetReadCompaction(on bool) (was bool)
	JMReadCompaction() bool
}

func (m *baseMeta) JMSetReadCompaction(on bool) bool {
	return !m.jmNoReadComp.Swap(!on)
}

func (m *baseMeta) JMReadCompaction() bool { return m.jmReadCompaction() }

func (m *baseMeta) jmReadCompaction() bool { return !m.jmNoReadComp.Load() }
