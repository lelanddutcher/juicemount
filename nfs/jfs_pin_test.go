package nfs

import (
	"encoding/binary"
	"errors"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// The app and the bundled juicefs must agree on the message and op numbers;
// a renumber on one side fails here.
func TestJFSPinConstantsMatchJuiceFS(t *testing.T) {
	for _, c := range []struct {
		name     string
		app, jfs int
	}{
		{"message", jfsMsgPinCache, meta.JMPinCache},
		{"start", jfsPinOpStart, meta.JMPinOpStart},
		{"status", jfsPinOpStatus, meta.JMPinOpStatus},
		{"check", jfsPinOpCheck, meta.JMPinOpCheck},
		{"sweep", jfsPinOpSweep, meta.JMPinOpSweep},
		{"cancel", jfsPinOpCancel, meta.JMPinOpCancel},
		{"verify", jfsPinOpVerify, meta.JMPinOpVerify},
		{"setcap", jfsPinOpSetCap, meta.JMPinOpSetCap},
		{"batch", jfsPinBatch, meta.JMPinVerifyMax},
	} {
		if c.app != c.jfs {
			t.Errorf("%s: app %d, juicefs %d", c.name, c.app, c.jfs)
		}
	}
}

func TestJFSBlockPinnerWire(t *testing.T) {
	var sent [][]byte
	reply := func(msg []byte, _ int) ([]byte, error) {
		sent = append(sent, append([]byte(nil), msg...))
		if binary.BigEndian.Uint32(msg) != jfsMsgPinCache || int(binary.BigEndian.Uint32(msg[4:])) != len(msg)-8 {
			t.Fatalf("header %x", msg[:8])
		}
		switch msg[8] {
		case jfsPinOpStart, jfsPinOpCancel:
			return []byte{0}, nil
		case jfsPinOpStatus:
			r := []byte{0, 2, 0}
			for _, v := range []uint64{300, 300, 100} {
				r = binary.BigEndian.AppendUint64(r, v)
			}
			return r, nil
		case jfsPinOpCheck:
			r := []byte{0, 1}
			for _, v := range []uint64{300, 7, 300, 55} {
				r = binary.BigEndian.AppendUint64(r, v)
			}
			return r, nil
		case jfsPinOpVerify:
			n := binary.BigEndian.Uint32(msg[9:])
			if extra := len(msg) - (13 + 24*int(n)); extra != 0 && (extra != 1 || msg[len(msg)-1] != 1) {
				t.Fatalf("verify body %d bytes for %d items", len(msg)-13, n)
			}
			r := binary.BigEndian.AppendUint32([]byte{0}, 1)
			r = binary.BigEndian.AppendUint64(r, binary.BigEndian.Uint64(msg[13:]))
			r = append(r, pin.PinChanged)
			return binary.BigEndian.AppendUint64(r, 9), nil
		case jfsPinOpSweep:
			r := binary.BigEndian.AppendUint32([]byte{0}, 1)
			r = binary.BigEndian.AppendUint64(r, 1000)
			return binary.BigEndian.AppendUint64(r, 9000), nil
		case jfsPinOpSetCap:
			r := []byte{0}
			for _, v := range []uint64{binary.BigEndian.Uint64(msg[9:]), 1000, 9000} {
				r = binary.BigEndian.AppendUint64(r, v)
			}
			return r, nil
		}
		return []byte{byte(syscall.EINVAL)}, nil
	}
	b := &JFSBlockPinner{c: &jfsControl{testExchange: reply}}
	if err := b.Start(42, 1); err != nil || binary.BigEndian.Uint64(sent[0][9:]) != 42 || sent[0][17] != 1 {
		t.Fatalf("start: %v %x", err, sent[0])
	}
	st, err := b.Status(42)
	if err != nil || st.State != pin.BlockFillDone || st.Total != 300 || st.Done != 300 || st.Fetched != 100 {
		t.Fatalf("status %+v %v", st, err)
	}
	if known, total, missing, attr, err := b.Check(42); err != nil || !known || total != 300 || missing != 7 || attr.Size != 300 || attr.MtimeNS != 55 {
		t.Fatalf("check %v %d %d %+v %v", known, total, missing, attr, err)
	}
	items := make([]pin.PinVerifyItem, jfsPinBatch+2)
	for i := range items {
		items[i] = pin.PinVerifyItem{Inode: uint64(i + 1), Size: 10, MtimeNS: 20}
	}
	sent = nil
	res, err := b.Verify(items, false)
	if err != nil || len(sent) != 2 || len(res) != 2 || res[0].Inode != 1 || res[0].Reason != pin.PinChanged || res[0].Missing != 9 || res[1].Inode != jfsPinBatch+1 {
		t.Fatalf("verify %+v %v in %d messages", res, err, len(sent))
	}
	// With slices: the flags byte follows the items.
	sent = nil
	if _, err := b.Verify(items[:2], true); err != nil || len(sent) != 1 || len(sent[0]) != 13+48+1 || sent[0][len(sent[0])-1] != 1 {
		t.Fatalf("verify with slices: %v %x", err, sent)
	}
	keep := make([]uint64, jfsPinBatch+3)
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
	if capacity, _, _, err := b.SetCapacity(5 << 30); err != nil || capacity != 5<<30 {
		t.Fatalf("setcap %d %v", capacity, err)
	}
	if err := b.Cancel(42); err != nil {
		t.Fatal(err)
	}
}

func TestJFSBlockPinnerErrnos(t *testing.T) {
	for eno, want := range map[syscall.Errno]error{
		syscall.EINVAL:  syscall.EINVAL, // a real error, not "unsupported"
		syscall.ENOTSUP: pin.ErrBlockPinUnsupported,
		syscall.ENOSPC:  pin.ErrPinnedShareFull,
		syscall.ENOENT:  syscall.ENOENT,
	} {
		b := &JFSBlockPinner{c: &jfsControl{testExchange: func([]byte, int) ([]byte, error) { return []byte{byte(eno)}, nil }}}
		if err := b.Start(1, 4); !errors.Is(err, want) {
			t.Fatalf("errno %v: %v, want %v", eno, err, want)
		}
	}
	// CANCEL of a fill juicefs no longer knows is not an error.
	b := &JFSBlockPinner{c: &jfsControl{testExchange: func([]byte, int) ([]byte, error) { return []byte{byte(syscall.ENOENT)}, nil }}}
	if err := b.Cancel(1); err != nil {
		t.Fatal(err)
	}
}
