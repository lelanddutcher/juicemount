package vfs

// JuiceMount: the JMLinkPolicy control message (patch 0027).

import (
	"bytes"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func linkMsg(v *VFS, body []byte) []byte {
	var out bytes.Buffer
	v.handleInternalMsg(meta.NewContext(1, 0, []uint32{0}), meta.JMLinkPolicy, utils.ReadBuffer(body), &out)
	return out.Bytes()
}

func linkSet(flags byte, getMS, putMS uint32, retries byte) []byte {
	b := []byte{jmLinkPolicyVersion, flags}
	b = binary.BigEndian.AppendUint32(b, getMS)
	b = binary.BigEndian.AppendUint32(b, putMS)
	return append(b, retries)
}

// Query, set slow, set fast: each reply holds the settings in force.
func TestJMLinkPolicyMessageBothWays(t *testing.T) {
	v := pinVFS(t) // GetTimeout and PutTimeout 1 min, MaxRetries 1, no demand-first
	if got, want := linkMsg(v, []byte{jmLinkPolicyVersion}), append([]byte{0}, linkSet(0, 60000, 60000, 1)[1:]...); !bytes.Equal(got, want) {
		t.Fatalf("query: % x, want % x", got, want)
	}
	slow := linkSet(jmLinkFlagDemandFirst, 120000, 180000, 3)
	if got := linkMsg(v, slow); !bytes.Equal(got, append([]byte{0}, slow[1:]...)) {
		t.Fatalf("set slow: % x", got)
	}
	if got := linkMsg(v, []byte{jmLinkPolicyVersion}); !bytes.Equal(got, append([]byte{0}, slow[1:]...)) {
		t.Fatalf("query after slow: % x", got)
	}
	fast := linkSet(0, 60000, 60000, 10)
	if got := linkMsg(v, fast); !bytes.Equal(got, append([]byte{0}, fast[1:]...)) {
		t.Fatalf("set fast: % x", got)
	}
}

func TestJMLinkPolicyMessageRefusals(t *testing.T) {
	v := pinVFS(t)
	before := linkMsg(v, []byte{jmLinkPolicyVersion})
	for name, body := range map[string][]byte{
		"empty":      {},
		"version 2":  {2},
		"short":      linkSet(1, 60000, 60000, 3)[:6],
		"long":       append(linkSet(1, 60000, 60000, 3), 0),
		"get 0":      linkSet(1, 0, 60000, 3),
		"put 31 min": linkSet(1, 60000, 31*60000, 3),
		"retries 51": linkSet(1, 60000, 60000, 51),
	} {
		if got := linkMsg(v, body); !bytes.Equal(got, []byte{uint8(syscall.EINVAL)}) {
			t.Fatalf("%s: reply % x, want EINVAL", name, got)
		}
	}
	if after := linkMsg(v, []byte{jmLinkPolicyVersion}); !bytes.Equal(after, before) {
		t.Fatalf("a refused message changed the policy: % x -> % x", before, after)
	}
}

// The vfs read retry budget and flush interrupt follow a set policy, and
// keep the mount values until one is set.
func TestJMLinkPolicyVFSRetriesFollow(t *testing.T) {
	v := pinVFS(t)
	if got := jmLiveRetries(v.Store, 10); got != 10 {
		t.Fatalf("before any message: retries %d, want the mount's 10", got)
	}
	if got := jmLivePutTimeout(v.Store, 7*time.Second); got != 7*time.Second {
		t.Fatalf("before any message: put timeout %s, want the mount's", got)
	}
	linkMsg(v, linkSet(jmLinkFlagDemandFirst, 120000, 180000, 3))
	if got := jmLiveRetries(v.Store, 10); got != 3 {
		t.Fatalf("after slow: retries %d, want 3", got)
	}
	if got := jmLivePutTimeout(v.Store, 7*time.Second); got != 3*time.Minute {
		t.Fatalf("after slow: put timeout %s, want 3m", got)
	}
	linkMsg(v, linkSet(0, 60000, 60000, 10))
	if got := jmLiveRetries(v.Store, 3); got != 10 {
		t.Fatalf("after fast: retries %d, want 10", got)
	}
	// A refused message sets nothing new.
	linkMsg(v, linkSet(0, 0, 60000, 3))
	if got := jmLiveRetries(v.Store, 3); got != 10 {
		t.Fatalf("after a refused message: retries %d", got)
	}
}
