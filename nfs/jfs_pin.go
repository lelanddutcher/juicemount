package nfs

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// The JMPinCache control message of the bundled juicefs (patch 0026,
// third_party/juicefs/pkg/vfs/jm_pin.go): fill and pin whole blocks. The
// numbers are copies of meta.JMPinCache and meta.JMPinOp*;
// TestJFSPinConstantsMatchJuiceFS keeps them equal.
const (
	jfsMsgPinCache = 1105

	jfsPinOpStart  = 1
	jfsPinOpStatus = 2
	jfsPinOpCheck  = 3
	jfsPinOpSweep  = 4
	jfsPinOpCancel = 5
	jfsPinOpVerify = 6
	jfsPinOpSetCap = 7

	// jfsPinBatch bounds one VERIFY or SWEEP message (juicefs refuses more).
	jfsPinBatch = 4096
)

// jfsPinTimeout bounds one pin exchange. Every op answers without metadata
// except VERIFY with slices, which juicefs bounds to 5 s.
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
	case syscall.ENOTSUP, syscall.ENOSYS:
		return pin.ErrBlockPinUnsupported
	case syscall.ENOSPC:
		return pin.ErrPinnedShareFull
	default:
		return e
	}
}

func (b *JFSBlockPinner) op(op byte, body []byte, replyLen int, more func([]byte) int) ([]byte, error) {
	msg := controlHeader(jfsMsgPinCache, 1+len(body))
	msg = append(msg, op)
	msg = append(msg, body...)
	// The longest reply: a full VERIFY batch (5 + 17 per item).
	maxLen := replyLen
	if more != nil {
		maxLen = max(replyLen, 5+17*jfsPinBatch)
	}
	reply, err := b.c.exchangeUntil(msg, maxLen, pinReplyWant(replyLen, maxLen, more), jfsPinTimeout)
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

// pinReplyWant is the reply length exchangeUntil reads to. Every reply
// starts with an errno; a nonzero one ends it (also the 1-byte EINVAL of a
// juicefs without the message). more, when set, gives the whole length from
// the header once replyLen bytes are in; never past maxLen.
func pinReplyWant(replyLen, maxLen int, more func([]byte) int) func([]byte) int {
	return func(r []byte) int {
		if len(r) > 0 && r[0] != 0 {
			return 1
		}
		if more != nil && len(r) >= replyLen {
			return min(max(replyLen, more(r)), maxLen)
		}
		return replyLen
	}
}

func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func (b *JFSBlockPinner) Start(inode uint64, workers int) error {
	_, err := b.op(jfsPinOpStart, append(u64(inode), byte(min(max(workers, 1), 4))), 1, nil)
	return err
}

func (b *JFSBlockPinner) Status(inode uint64) (pin.BlockFillStatus, error) {
	r, err := b.op(jfsPinOpStatus, u64(inode), 27, nil)
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

func (b *JFSBlockPinner) Cancel(inode uint64) error {
	_, err := b.op(jfsPinOpCancel, u64(inode), 1, nil)
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	return err
}

// Check is one owner's CHECK: known, pinned bytes, missing bytes, and the
// length and mtime the fill saw. The guard uses Verify; Check is for tools.
func (b *JFSBlockPinner) Check(inode uint64) (known bool, total, missing int64, attr pin.ExpectedFile, err error) {
	r, err := b.op(jfsPinOpCheck, u64(inode), 34, nil)
	if err != nil {
		return false, 0, 0, attr, err
	}
	attr = pin.ExpectedFile{Inode: inode, Size: int64(binary.BigEndian.Uint64(r[18:])), MtimeNS: int64(binary.BigEndian.Uint64(r[26:]))}
	return r[1] == 1, int64(binary.BigEndian.Uint64(r[2:])), int64(binary.BigEndian.Uint64(r[10:])), attr, nil
}

// Verify sends flags bit 1 when slices is set: juicefs then also compares
// each file's slices in metadata with the ones its fill saw.
func (b *JFSBlockPinner) Verify(items []pin.PinVerifyItem, slices bool) ([]pin.PinVerifyResult, error) {
	var out []pin.PinVerifyResult
	for start := 0; start < len(items); start += jfsPinBatch {
		batch := items[start:min(start+jfsPinBatch, len(items))]
		body := binary.BigEndian.AppendUint32(nil, uint32(len(batch)))
		for _, it := range batch {
			body = binary.BigEndian.AppendUint64(body, it.Inode)
			body = binary.BigEndian.AppendUint64(body, uint64(it.Size))
			body = binary.BigEndian.AppendUint64(body, uint64(it.MtimeNS))
		}
		if slices {
			body = append(body, 1)
		}
		r, err := b.op(jfsPinOpVerify, body, 5, func(r []byte) int {
			return 5 + 17*int(binary.BigEndian.Uint32(r[1:5]))
		})
		if err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint32(r[1:5]))
		if len(r) < 5+17*n {
			return nil, errors.New("juicefs pin: short verify reply")
		}
		for i := 0; i < n; i++ {
			rec := r[5+17*i:]
			out = append(out, pin.PinVerifyResult{Inode: binary.BigEndian.Uint64(rec), Reason: rec[8], Missing: int64(binary.BigEndian.Uint64(rec[9:]))})
		}
	}
	return out, nil
}

func (b *JFSBlockPinner) Sweep(keep []uint64) (int, int64, int64, error) {
	var dropped int
	var pinned, limit int64
	for start := 0; start == 0 || start < len(keep); start += jfsPinBatch {
		end := min(start+jfsPinBatch, len(keep))
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
		r, err := b.op(jfsPinOpSweep, body, 21, nil)
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

func (b *JFSBlockPinner) SetCapacity(bytes int64) (int64, int64, int64, error) {
	r, err := b.op(jfsPinOpSetCap, u64(uint64(max(bytes, 0))), 25, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	return int64(binary.BigEndian.Uint64(r[1:])), int64(binary.BigEndian.Uint64(r[9:])), int64(binary.BigEndian.Uint64(r[17:])), nil
}

// PinIdentity returns the inode a Ready entry's witness recorded and the file
// as the local mirror shows it now. No server access.
func (p *OwnedPinProvider) PinIdentity(e pin.Entry) (uint64, pin.ExpectedFile, bool) {
	var w pinWitness
	if len(e.Authority) == 0 || json.Unmarshal(e.Authority, &w) != nil || w.Version != 1 || w.Inode == 0 {
		return 0, pin.ExpectedFile{}, false
	}
	name, ok := p.inMount(e.Path)
	if !ok {
		return 0, pin.ExpectedFile{}, false
	}
	id, found := p.h.store.LookupCachedFileIdentity(name)
	if !found {
		// Gone from the mirror: keep the witness so its pins are swept
		// only when the pin itself goes.
		return w.Inode, pin.ExpectedFile{Inode: w.Inode, Size: w.Size, MtimeNS: w.MtimeNS}, true
	}
	return w.Inode, pin.ExpectedFile{Inode: id.Inode, Size: id.Size, MtimeNS: id.MtimeNS}, true
}

var _ pin.BlockPinner = (*JFSBlockPinner)(nil)
