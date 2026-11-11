package nfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// JuiceFS control-file messages added by the bundled juicefs (patch 0022,
// third_party/juicefs/pkg/meta/jm_reserved_inode.go). A juicefs without the
// patch answers neither, and the caller falls back to a plain FUSE mkdir.
const (
	jfsMsgReserveInodes = 1101
	jfsMsgMkdirInode    = 1102
)

// errControlUnsupported reports a juicefs that does not know the messages.
var errControlUnsupported = errors.New("juicefs control: folder inode messages not supported")

// jfsControl talks to the JuiceFS mount through its .control file. A message
// is written, and the reply follows it in the same open file. On macOS juicefs
// keeps one control handle per process, so one message runs at a time.
type jfsControl struct {
	root string
	mu   sync.Mutex
	// replyTimeout bounds one exchange; a metadata transaction on a slow
	// link may need several round trips.
	replyTimeout time.Duration
	// testExchange replaces the file exchange in tests.
	testExchange func(msg []byte, replyLen int) ([]byte, error)
}

func newJFSControl(root string) *jfsControl {
	return &jfsControl{root: root, replyTimeout: 60 * time.Second}
}

func (c *jfsControl) exchange(msg []byte, replyLen int) ([]byte, error) {
	if c.testExchange != nil {
		return c.testExchange(msg, replyLen)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// juicefs keys the macOS control handle by the caller's pid, and FUSE
	// requests carry the thread's identity on Linux. Keep the whole exchange
	// on one thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	f, err := os.OpenFile(filepath.Join(c.root, ".control"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Never send a message to a plain folder that stands in for a missing
	// mount (the V2.3 G0 identity rule): it is not juicefs.
	if err := pin.FUSEIdentityCheckFD(f.Fd()); err != nil {
		return nil, err
	}
	if _, err := f.Write(msg); err != nil {
		return nil, err
	}
	reply := make([]byte, 0, replyLen)
	buf := make([]byte, replyLen)
	deadline := time.Now().Add(c.replyTimeout)
	wait := time.Millisecond
	for len(reply) < replyLen {
		n, err := f.Read(buf[:replyLen-len(reply)])
		reply = append(reply, buf[:n]...)
		if n > 0 {
			continue
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("juicefs control: no reply after %s", c.replyTimeout)
		}
		// The reply is written when the transaction ends; poll briefly.
		time.Sleep(wait)
		if wait < 20*time.Millisecond {
			wait *= 2
		}
	}
	return reply, nil
}

func controlHeader(cmd uint32, body int) []byte {
	msg := make([]byte, 8, 8+body)
	binary.BigEndian.PutUint32(msg[0:4], cmd)
	binary.BigEndian.PutUint32(msg[4:8], uint32(body))
	return msg
}

func controlErrno(b byte) error {
	switch e := syscall.Errno(b); e {
	case 0:
		return nil
	case syscall.ENOTSUP, syscall.ENOSYS:
		return errControlUnsupported
	default:
		return e
	}
}

// reserveInodes reserves up to n folder inode numbers in the running juicefs.
func (c *jfsControl) reserveInodes(n uint32) (first uint64, count uint32, err error) {
	msg := controlHeader(jfsMsgReserveInodes, 4)
	msg = binary.BigEndian.AppendUint32(msg, n)
	reply, err := c.exchange(msg, 13)
	if err != nil {
		return 0, 0, err
	}
	if err := controlErrno(reply[0]); err != nil {
		return 0, 0, err
	}
	return binary.BigEndian.Uint64(reply[1:9]), binary.BigEndian.Uint32(reply[9:13]), nil
}

// mkdirInode creates name in the folder with inode parent, using a number
// reserveInodes returned. syscall.ESRCH means juicefs does not hold that
// number (it restarted): create the folder an ordinary way instead.
func (c *jfsControl) mkdirInode(parent uint64, name string, mode os.FileMode, ino uint64) error {
	if len(name) == 0 || len(name) > 255 {
		return syscall.ENAMETOOLONG
	}
	body := 8 + 8 + 2 + 2 + 1 + len(name)
	msg := controlHeader(jfsMsgMkdirInode, body)
	msg = binary.BigEndian.AppendUint64(msg, parent)
	msg = binary.BigEndian.AppendUint64(msg, ino)
	msg = binary.BigEndian.AppendUint16(msg, uint16(mode.Perm()))
	msg = binary.BigEndian.AppendUint16(msg, 0) // the NFS client already applied its umask
	msg = append(msg, byte(len(name)))
	msg = append(msg, name...)
	reply, err := c.exchange(msg, 1)
	if err != nil {
		return err
	}
	return controlErrno(reply[0])
}
