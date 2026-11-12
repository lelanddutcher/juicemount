package nfs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// The JMPinCache control message of the bundled juicefs (patch 0025,
// third_party/juicefs/pkg/vfs/jm_pin.go): fill and pin whole blocks.
const (
	jfsMsgPinCache = 1104

	jfsPinOpStart  = 1
	jfsPinOpStatus = 2
	jfsPinOpCheck  = 3
	jfsPinOpSweep  = 4

	// jfsPinSweepBatch bounds one sweep message (8 bytes per inode).
	jfsPinSweepBatch = 4096
)

// jfsPinTimeout bounds one pin exchange: every op is local to juicefs except
// START's slice lookup, which runs in juicefs's background.
const jfsPinTimeout = 20 * time.Second

// JFSBlockPinner implements pin.BlockPinner over the mount's .control file.
type JFSBlockPinner struct{ c *jfsControl }

// NewJFSBlockPinner talks to the juicefs mounted at fuseRoot.
func NewJFSBlockPinner(fuseRoot string) *JFSBlockPinner {
	return &JFSBlockPinner{c: sharedJFSControl(fuseRoot)}
}

func pinErrno(b byte) error {
	switch e := syscall.Errno(b); e {
	case 0:
		return nil
	case syscall.ENOTSUP, syscall.ENOSYS, syscall.EINVAL:
		// EINVAL: a juicefs without patch 0025 answers an unknown message so.
		return pin.ErrBlockPinUnsupported
	case syscall.ENOSPC:
		return pin.ErrPinnedShareFull
	default:
		return e
	}
}

func (b *JFSBlockPinner) op(op byte, body []byte, replyLen int) ([]byte, error) {
	msg := controlHeader(jfsMsgPinCache, 1+len(body))
	msg = append(msg, op)
	msg = append(msg, body...)
	reply, err := b.c.exchangeReply(msg, replyLen, jfsPinTimeout, true)
	if err != nil {
		return nil, err
	}
	if len(reply) == 0 {
		return nil, errors.New("juicefs pin: empty reply")
	}
	if err := pinErrno(reply[0]); err != nil {
		return nil, err
	}
	if len(reply) < replyLen {
		return nil, errors.New("juicefs pin: short reply")
	}
	return reply, nil
}

func (b *JFSBlockPinner) Start(inode uint64) error {
	_, err := b.op(jfsPinOpStart, binary.BigEndian.AppendUint64(nil, inode), 1)
	return err
}

func (b *JFSBlockPinner) Status(inode uint64) (pin.BlockFillStatus, error) {
	r, err := b.op(jfsPinOpStatus, binary.BigEndian.AppendUint64(nil, inode), 27)
	if err != nil {
		return pin.BlockFillStatus{}, err
	}
	return pin.BlockFillStatus{
		State:   pin.BlockFillState(r[1]),
		Errno:   syscall.Errno(r[2]),
		Total:   int64(binary.BigEndian.Uint64(r[3:])),
		Done:    int64(binary.BigEndian.Uint64(r[11:])),
		Fetched: int64(binary.BigEndian.Uint64(r[19:])),
	}, nil
}

func (b *JFSBlockPinner) Check(inode uint64) (bool, int64, int64, error) {
	r, err := b.op(jfsPinOpCheck, binary.BigEndian.AppendUint64(nil, inode), 18)
	if err != nil {
		return false, 0, 0, err
	}
	return r[1] == 1, int64(binary.BigEndian.Uint64(r[2:])), int64(binary.BigEndian.Uint64(r[10:])), nil
}

func (b *JFSBlockPinner) Sweep(keep []uint64) (int, int64, int64, error) {
	var dropped int
	var pinned, limit int64
	for start := 0; start == 0 || start < len(keep); start += jfsPinSweepBatch {
		end := min(start+jfsPinSweepBatch, len(keep))
		var phase byte
		if start == 0 {
			phase |= 1
		}
		if end == len(keep) {
			phase |= 2
		}
		body := []byte{phase}
		body = binary.BigEndian.AppendUint32(body, uint32(end-start))
		for _, ino := range keep[start:end] {
			body = binary.BigEndian.AppendUint64(body, ino)
		}
		r, err := b.op(jfsPinOpSweep, body, 21)
		if err != nil {
			return 0, 0, 0, err
		}
		dropped += int(binary.BigEndian.Uint32(r[1:]))
		pinned, limit = int64(binary.BigEndian.Uint64(r[5:])), int64(binary.BigEndian.Uint64(r[13:]))
		if end == len(keep) {
			break
		}
	}
	return dropped, pinned, limit, nil
}

// ReadyInode returns the inode a Ready entry's witness recorded.
func (p *OwnedPinProvider) ReadyInode(e pin.Entry) (uint64, bool) {
	var w pinWitness
	if len(e.Authority) == 0 || json.Unmarshal(e.Authority, &w) != nil || w.Version != 1 || w.Inode == 0 {
		return 0, false
	}
	return w.Inode, true
}

var _ pin.BlockPinner = (*JFSBlockPinner)(nil)
