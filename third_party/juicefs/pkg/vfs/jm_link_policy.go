/*
 * JuiceMount: the JMLinkPolicy control message (patch 0027).
 *
 * The app sends it when the link class it measures changes (health/
 * jfs_link_follow.go), so a volume mounted on a fast link reads like a
 * volume mounted on a slow one once the link turns slow, and the reverse,
 * with no remount. pkg/chunk/jm_link_policy.go has the settings.
 *
 * Body: version(1) = 1, then either nothing (a query) or
 *
 *	flags(1)           bit 1: demand-first (patch 0020 and 0021 spans)
 *	get_timeout_ms(4)  --get-timeout
 *	put_timeout_ms(4)  --put-timeout
 *	io_retries(1)      --io-retries of a block upload
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
 * same layout (flags, get, put, retries): 11 bytes. EINVAL: an unknown
 * version, a short body, or values outside 1 s..30 min and 0..50 retries
 * (nothing changes). ENOTSUP: the store has no such settings.
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
	jmLinkPolicyBodyLen   = 1 + 4 + 4 + 1
	jmLinkFlagDemandFirst = 1
)

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
	if r.Left() < 1 || r.Get8() != jmLinkPolicyVersion {
		fail(syscall.EINVAL)
		return
	}
	var now chunk.JMLinkPolicy
	switch r.Left() {
	case 0:
		now = store.JMLinkPolicy()
	case jmLinkPolicyBodyLen:
		p := chunk.JMLinkPolicy{DemandFirst: r.Get8()&jmLinkFlagDemandFirst != 0}
		p.GetTimeout = time.Duration(r.Get32()) * time.Millisecond
		p.PutTimeout = time.Duration(r.Get32()) * time.Millisecond
		p.MaxRetries = int(r.Get8())
		if now, ok = store.JMSetLinkPolicy(p); !ok {
			fail(syscall.EINVAL)
			return
		}
	default:
		fail(syscall.EINVAL)
		return
	}
	w := utils.NewBuffer(1 + jmLinkPolicyBodyLen)
	w.Put8(0)
	var flags uint8
	if now.DemandFirst {
		flags |= jmLinkFlagDemandFirst
	}
	w.Put8(flags)
	w.Put32(uint32(now.GetTimeout / time.Millisecond))
	w.Put32(uint32(now.PutTimeout / time.Millisecond))
	w.Put8(uint8(min(now.MaxRetries, 255)))
	_, _ = out.Write(w.Bytes())
}
