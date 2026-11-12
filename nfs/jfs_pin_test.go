package nfs

import (
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

func TestJFSBlockPinnerWire(t *testing.T) {
	var sent [][]byte
	reply := func(msg []byte, _ int) ([]byte, error) {
		sent = append(sent, append([]byte(nil), msg...))
		if binary.BigEndian.Uint32(msg) != jfsMsgPinCache || int(binary.BigEndian.Uint32(msg[4:])) != len(msg)-8 {
			t.Fatalf("header %x", msg[:8])
		}
		switch msg[8] {
		case jfsPinOpStart:
			return []byte{0}, nil
		case jfsPinOpStatus:
			r := []byte{0, 2, 0}
			for _, v := range []uint64{300, 300, 100} {
				r = binary.BigEndian.AppendUint64(r, v)
			}
			return r, nil
		case jfsPinOpCheck:
			r := []byte{0, 1}
			r = binary.BigEndian.AppendUint64(r, 300)
			return binary.BigEndian.AppendUint64(r, 7), nil
		case jfsPinOpSweep:
			r := binary.BigEndian.AppendUint32([]byte{0}, 1)
			r = binary.BigEndian.AppendUint64(r, 1000)
			return binary.BigEndian.AppendUint64(r, 9000), nil
		}
		return []byte{byte(syscall.EINVAL)}, nil
	}
	b := &JFSBlockPinner{c: &jfsControl{testExchange: reply}}
	if err := b.Start(42); err != nil || binary.BigEndian.Uint64(sent[0][9:]) != 42 {
		t.Fatalf("start: %v %x", err, sent[0])
	}
	st, err := b.Status(42)
	if err != nil || st.State != pin.BlockFillDone || st.Total != 300 || st.Done != 300 || st.Fetched != 100 {
		t.Fatalf("status %+v %v", st, err)
	}
	if known, total, missing, err := b.Check(42); err != nil || !known || total != 300 || missing != 7 {
		t.Fatalf("check %v %d %d %v", known, total, missing, err)
	}
	keep := make([]uint64, jfsPinSweepBatch+3)
	for i := range keep {
		keep[i] = uint64(i + 1)
	}
	sent = nil
	dropped, pinned, limit, err := b.Sweep(keep)
	if err != nil || dropped != 2 || pinned != 1000 || limit != 9000 || len(sent) != 2 {
		t.Fatalf("sweep %d %d %d %v in %d messages", dropped, pinned, limit, err, len(sent))
	}
	if sent[0][9] != 1 || sent[1][9] != 2 || binary.BigEndian.Uint32(sent[1][10:]) != 3 {
		t.Fatalf("sweep phases %d %d", sent[0][9], sent[1][9])
	}
	sent = nil
	if _, _, _, err := b.Sweep(nil); err != nil || len(sent) != 1 || sent[0][9] != 3 {
		t.Fatalf("empty sweep must still begin and end: %v %x", err, sent)
	}
}

func TestJFSBlockPinnerErrnos(t *testing.T) {
	for eno, want := range map[syscall.Errno]error{
		syscall.EINVAL:  pin.ErrBlockPinUnsupported, // a juicefs without patch 0025
		syscall.ENOTSUP: pin.ErrBlockPinUnsupported,
		syscall.ENOSPC:  pin.ErrPinnedShareFull,
		syscall.ENOENT:  syscall.ENOENT,
	} {
		b := &JFSBlockPinner{c: &jfsControl{testExchange: func([]byte, int) ([]byte, error) { return []byte{byte(eno)}, nil }}}
		if err := b.Start(1); !errors.Is(err, want) {
			t.Fatalf("errno %v: %v, want %v", eno, err, want)
		}
	}
}
