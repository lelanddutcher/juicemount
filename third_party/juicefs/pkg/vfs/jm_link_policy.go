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
