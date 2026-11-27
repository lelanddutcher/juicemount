/*
 * JuiceMount: the JMLinkPolicy control message (patch 0027).
 *
 * The app sends it when the link class it measures changes (health/
 * jfs_link_follow.go), so a volume mounted on a fast link reads like a
 * volume mounted on a slow one once the link turns slow, and the reverse,
 * with no remount. pkg/chunk/jm_link_policy.go has the settings.
 *
 * Body: version(1) = 1 or 2, then either nothing (a query) or
 *
 *	flags(1)           bit 1: demand-first (patch 0020 and 0021 spans)
 *	get_timeout_ms(4)  --get-timeout
 *	put_timeout_ms(4)  --put-timeout
 *	io_retries(1)      --io-retries of a block upload
 *	readahead_kib(4)   version 2 only (patch 0035): the maximum read-ahead
 *	                   of one sequential reader (jm_readahead.go); 0 = off
 *
 * The vfs layer follows the set values too: the read retry budget before
 * EIO (reader.go) and the flush deadline and interrupt (writer.go) use the
 * live retries and PUT timeout once a message has set them, so a slow GET
 * timeout never multiplies with a lan mount's 10 retries. Not followed: the
 * metadata client's own Redis retries (meta redis.go), made once with the
 * client; each try is bounded by that client's own read timeout (30 s by
 * default), not by the object timeouts this message changes.
 *
 * Reply: errno(1), then on errno 0 the settings in force afterwards in the
 * layout of the request's version: 11 bytes for version 1, 15 for version 2
 * (read-ahead 0 when it is below one block). EINVAL: an unknown version, a
 * short body, or values outside 1 s..30 min, 0..50 retries and 0..1 GiB of
 * read-ahead (nothing changes). ENOTSUP: the store has no such settings. A
 * version 1 message leaves the read-ahead as it is.
 */

package vfs

import (
	"io"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	jmLinkPolicyVersion   = 1
	jmLinkPolicyVersion2  = 2 // patch 0035: adds readahead_kib
	jmLinkPolicyBodyLen   = 1 + 4 + 4 + 1
	jmLinkPolicyBodyLen2  = jmLinkPolicyBodyLen + 4
	jmLinkFlagDemandFirst = 1
)

// jmReadaheadSetter: a reader whose read-ahead a message may set
// (jm_readahead.go).
type jmReadaheadSetter interface {
	JMReadahead() uint64
	JMSetReadahead(n uint64) bool
}

// jmLinkOverrider: a store whose policy a message may have set.
type jmLinkOverrider interface {
	JMLinkOverride() (chunk.JMLinkPolicy, bool)
}

// jmLiveRetries is the vfs retry budget: the live --io-retries once a
// JMLinkPolicy message set it, else the mount value.
func jmLiveRetries(store chunk.ChunkStore, mount uint32) uint32 {
	if s, ok := store.(jmLinkOverrider); ok {
		if p, set := s.JMLinkOverride(); set {
			return uint32(p.MaxRetries)
		}
	}
	return mount
}

// jmLivePutTimeout is the live --put-timeout once a message set it, else the
// mount value.
func jmLivePutTimeout(store chunk.ChunkStore, mount time.Duration) time.Duration {
	if s, ok := store.(jmLinkOverrider); ok {
		if p, set := s.JMLinkOverride(); set {
			return p.PutTimeout
		}
	}
	return mount
}

type jmLinkStore interface {
	JMLinkPolicy() chunk.JMLinkPolicy
	JMSetLinkPolicy(p chunk.JMLinkPolicy) (chunk.JMLinkPolicy, bool)
}

func (v *VFS) jmLinkPolicyMsg(r *utils.Buffer, out io.Writer) {
	fail := func(eno syscall.Errno) { _, _ = out.Write([]byte{uint8(eno)}) }
	store, ok := v.Store.(jmLinkStore)
	if !ok {
		fail(syscall.ENOTSUP)
		return
	}
	if r.Left() < 1 {
		fail(syscall.EINVAL)
		return
	}
	version := r.Get8()
	bodyLen := jmLinkPolicyBodyLen
	var ra jmReadaheadSetter
	switch version {
	case jmLinkPolicyVersion:
	case jmLinkPolicyVersion2:
		if ra, ok = v.reader.(jmReadaheadSetter); !ok {
			fail(syscall.ENOTSUP)
			return
		}
		bodyLen = jmLinkPolicyBodyLen2
	default:
		fail(syscall.EINVAL)
		return
	}
	var now chunk.JMLinkPolicy
	switch r.Left() {
	case 0:
		now = store.JMLinkPolicy()
	case bodyLen:
		p := chunk.JMLinkPolicy{DemandFirst: r.Get8()&jmLinkFlagDemandFirst != 0}
		p.GetTimeout = time.Duration(r.Get32()) * time.Millisecond
		p.PutTimeout = time.Duration(r.Get32()) * time.Millisecond
		p.MaxRetries = int(r.Get8())
		var raBytes uint64
		if ra != nil {
			raBytes = uint64(r.Get32()) << 10
			if raBytes > jmReadaheadLimit || !p.Valid() {
				fail(syscall.EINVAL)
				return
			}
		}
		if now, ok = store.JMSetLinkPolicy(p); !ok {
			fail(syscall.EINVAL)
			return
		}
		if ra != nil {
			ra.JMSetReadahead(raBytes)
		}
	default:
		fail(syscall.EINVAL)
		return
	}
	w := utils.NewBuffer(uint32(1 + bodyLen))
	w.Put8(0)
	var flags uint8
	if now.DemandFirst {
		flags |= jmLinkFlagDemandFirst
	}
	w.Put8(flags)
	w.Put32(uint32(now.GetTimeout / time.Millisecond))
	w.Put32(uint32(now.PutTimeout / time.Millisecond))
	w.Put8(uint8(min(now.MaxRetries, 255)))
	if ra != nil {
		w.Put32(uint32(ra.JMReadahead() >> 10))
	}
	_, _ = out.Write(w.Bytes())
}
