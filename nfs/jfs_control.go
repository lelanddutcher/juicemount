package nfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

// JuiceFS control-file messages added by the bundled juicefs (patch 0022,
// third_party/juicefs/pkg/meta/jm_reserved_inode.go). A juicefs without the
// patch answers neither, and the caller falls back to a plain FUSE mkdir.
const (
	jfsMsgReserveInodes = 1101
	jfsMsgMkdirInode    = 1102
	jfsMsgReleaseInodes = 1103
)

// errControlUnsupported reports a juicefs that does not know the messages.
var errControlUnsupported = errors.New("juicefs control: folder inode messages not supported")

// jfsControl talks to the JuiceFS mount through its .control file (or the
// control lane, see jfsControlLaneFile). A message is written, and the reply
// follows it in the same open file. On macOS juicefs keeps one control handle
// per process and control file, so one message per file runs at a time.
type jfsControl struct {
	root string
	// file is the control file in root: ".control", or the lane (below).
	file string
	mu   sync.Mutex
	// replyTimeout bounds one exchange; a metadata transaction on a slow
	// link may need several round trips.
	replyTimeout time.Duration
	// fallback is set on the lane: the shared channel, used once the lane
	// turned out to be missing (a juicefs without patch 0033).
	fallback    *jfsControl
	laneMissing atomic.Bool
	// testExchange replaces the file exchange in tests; testLocked replaces
	// it while the channel's lock is held.
	testExchange func(msg []byte, replyLen int) ([]byte, error)
	testLocked   func(msg []byte, replyLen int) ([]byte, error)
}

func newJFSControl(root string) *jfsControl {
	return &jfsControl{root: root, file: ".control", replyTimeout: 60 * time.Second}
}

// jfsControlLaneFile is the second control file of the bundled juicefs
// (patch 0033, third_party/juicefs/pkg/vfs/jm_control_lane.go).
//
// Bug F2 (laptop run 2026-11-14). On macOS juicefs keeps one control handle
// per process, so every control message of the app shares one channel. The
// drain's create-files messages hold it until their uploads end. Abroad, a
// content refresh waited 1.5-6.7 s for it, 67-83 s per session, and each
// READ of a clip a teammate was saving waited for a refresh: 35-49 s player
// freezes while the Mac's own saves drained. The lane is a separate control
// file with its own handle, so the refresh runs beside the drain. Only the
// content refresh uses it: the drain, pins and the link follower keep the
// shared channel and its order (the fences of #82 and #93 are unchanged).
const jfsControlLaneFile = ".jmcontrol"

func (c *jfsControl) exchange(msg []byte, replyLen int) ([]byte, error) {
	return c.exchangeWithin(msg, replyLen, c.replyTimeout)
}

func (c *jfsControl) exchangeWithin(msg []byte, replyLen int, timeout time.Duration) ([]byte, error) {
	return c.exchangeUntil(msg, replyLen, func([]byte) int { return replyLen }, timeout)
}

// exchangeUntil reads until the reply holds want(reply so far) bytes, so a
// reply whose first byte says it is short (an older juicefs refusing an
// unknown message with one byte) ends at once. maxLen is the longest reply.
func (c *jfsControl) exchangeUntil(msg []byte, maxLen int, want func([]byte) int, timeout time.Duration) ([]byte, error) {
	reply, _, _, err := c.exchangeUntilTimed(msg, maxLen, want, timeout)
	return reply, err
}

// exchangeUntilTimed is exchangeUntil that also reports how long the call
// waited for the control mutex (another control message in flight) and how
// long it held it. A wait of controlWaitLog or more is logged with the
// message number, so a slow message's effect on the drainer, pins and the
// link follower shows (review M1 of #97).
func (c *jfsControl) exchangeUntilTimed(msg []byte, maxLen int, want func([]byte) int, timeout time.Duration) ([]byte, time.Duration, time.Duration, error) {
	if c.fallback != nil && c.laneMissing.Load() {
		return c.fallback.exchangeUntilTimed(msg, maxLen, want, timeout)
	}
	reply, wait, hold, err := c.exchangeOwn(msg, maxLen, want, timeout)
	var notSent *errControlNotSent
	if c.fallback != nil && errors.As(err, &notSent) && errors.Is(err, fs.ErrNotExist) {
		// No lane: nothing was sent. Use the shared channel from now on.
		if !c.laneMissing.Swap(true) {
			jmlog.Warn("juicefs control: the bundled juicefs has no control lane (patch 0033); content refreshes share the drain's control channel",
				"root", c.root)
		}
		r, w, h, err := c.fallback.exchangeUntilTimed(msg, maxLen, want, timeout)
		return r, wait + w, hold + h, err
	}
	return reply, wait, hold, err
}

func (c *jfsControl) exchangeOwn(msg []byte, maxLen int, want func([]byte) int, timeout time.Duration) ([]byte, time.Duration, time.Duration, error) {
	if c.testExchange != nil {
		reply, err := c.testExchange(msg, maxLen)
		return reply, 0, 0, err
	}
	lockStart := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	held := time.Now()
	lockWait := held.Sub(lockStart)
	if lockWait >= controlWaitLog && len(msg) >= 4 {
		jmlog.Info("juicefs control: a message waited for the control channel",
			"cmd", binary.BigEndian.Uint32(msg[0:4]), "file", c.controlFile(), "wait_ms", lockWait.Milliseconds())
	}
	var reply []byte
	var err error
	if c.testLocked != nil {
		reply, err = c.testLocked(msg, maxLen)
	} else {
		reply, err = c.exchangeLocked(msg, maxLen, want, timeout)
	}
	return reply, lockWait, time.Since(held), err
}

func (c *jfsControl) controlFile() string {
	if c.file == "" {
		return ".control"
	}
	return c.file
}

// controlWaitLog: a control message that waited this long for the channel
// is logged.
const controlWaitLog = 200 * time.Millisecond

func (c *jfsControl) exchangeLocked(msg []byte, maxLen int, want func([]byte) int, timeout time.Duration) ([]byte, error) {
	// juicefs keys the macOS control handle by the caller's pid, and FUSE
	// requests carry the thread's identity on Linux. Keep the whole exchange
	// on one thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	f, err := os.OpenFile(filepath.Join(c.root, c.controlFile()), os.O_RDWR, 0)
	if err != nil {
		return nil, &errControlNotSent{err}
	}
	defer f.Close()
	// Never send a message to a plain folder that stands in for a missing
	// mount (the V2.3 G0 identity rule): it is not juicefs. The device check
	// answers from fstat; fstatfs would cost juicefs a metadata round trip.
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		return nil, &errControlNotSent{err}
	}
	if err := pin.FUSEIdentityCheckDev(uint64(st.Dev)); err != nil { //nolint:unconvert // int32 on Darwin
		return nil, &errControlNotSent{err}
	}
	if _, err := f.Write(msg); err != nil {
		return nil, err
	}
	reply := make([]byte, 0, maxLen)
	buf := make([]byte, maxLen)
	deadline := time.Now().Add(timeout)
	wait := time.Millisecond
	for len(reply) < want(reply) {
		n, err := f.Read(buf[:want(reply)-len(reply)])
		reply = append(reply, buf[:n]...)
		if n > 0 {
			continue
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("juicefs control: no reply after %s", timeout)
		}
		// The reply is written when the transaction ends; poll briefly.
		time.Sleep(wait)
		if wait < 20*time.Millisecond {
			wait *= 2
		}
	}
	return reply, nil
}

// errControlNotSent: the exchange failed before any byte of the message
// reached juicefs.
type errControlNotSent struct{ err error }

func (e *errControlNotSent) Error() string { return e.err.Error() }
func (e *errControlNotSent) Unwrap() error { return e.err }

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

// releaseInodes gives back reserved numbers first..first+n-1 that were not
// used. It returns how many juicefs still held: 0 means it does not know them
// (it restarted since it reserved them).
func (c *jfsControl) releaseInodes(first uint64, n uint32, timeout time.Duration) (uint32, error) {
	msg := controlHeader(jfsMsgReleaseInodes, 12)
	msg = binary.BigEndian.AppendUint64(msg, first)
	msg = binary.BigEndian.AppendUint32(msg, n)
	reply, err := c.exchangeWithin(msg, 5, timeout)
	if err != nil {
		return 0, err
	}
	if err := controlErrno(reply[0]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(reply[1:5]), nil
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

// jfsMsgCreateFiles creates small files, each complete in one metadata
// transaction (patch 0025, third_party/juicefs/pkg/vfs/jm_create_files.go).
const jfsMsgCreateFiles = 1104

// jfsNewFile is one file of a createFiles message.
type jfsNewFile struct {
	parent uint64
	// preIno is a number this client reserved (reserveInodes) for the file,
	// recorded before sending so a retry recognises its own file; 0: juicefs
	// picks one.
	preIno uint64
	name   string
	mode   os.FileMode
	mtime  time.Time
	data   []byte
}

// jfsCreated is juicefs's answer for one file: its inode, or the errno of a
// refusal (EEXIST: the name exists and nothing was written).
type jfsCreated struct {
	ino uint64
	err error
}

const (
	jfsCreateFilesReplyMarker = 0xA5
	jfsCreateFilesMax         = 64
	jfsCreateFileMaxBytes     = 1 << 20
	// The reply waits for the uploads: a base for the transaction plus
	// the bytes at a thin link's rate (16 KiB/s).
	jfsCreateFilesTimeoutBase = 2 * time.Minute
	jfsCreateFilesTimeoutRate = 16 << 10
	jfsCreateFilesBodyBase    = 2
)

// jfsCreateFilesTimeout bounds one exchange carrying total bytes. The
// control lock is held for its whole length.
func jfsCreateFilesTimeout(total int) time.Duration {
	return jfsCreateFilesTimeoutBase + time.Duration(total/jfsCreateFilesTimeoutRate)*time.Second
}

// sharedJFSControl returns the one control client of the mount at root. The
// drainer's inode pool and its direct publisher share it, so their exchanges
// queue on one lock instead of racing for juicefs's per-process control
// handle (on macOS one handle per pid: two clients would read each other's
// replies).
func sharedJFSControl(root string) *jfsControl {
	jfsControlsMu.Lock()
	defer jfsControlsMu.Unlock()
	c := jfsControls[root]
	if c == nil {
		c = newJFSControl(root)
		jfsControls[root] = c
	}
	return c
}

// refreshJFSControl returns the control client the content refresh of the
// mount at root uses: the lane, which falls back to the shared channel.
func refreshJFSControl(root string) *jfsControl {
	jfsControlsMu.Lock()
	defer jfsControlsMu.Unlock()
	c := jfsLanes[root]
	if c == nil {
		shared := jfsControls[root]
		if shared == nil {
			shared = newJFSControl(root)
			jfsControls[root] = shared
		}
		c = newJFSControl(root)
		c.file, c.fallback = jfsControlLaneFile, shared
		jfsLanes[root] = c
	}
	return c
}

var (
	jfsControlsMu sync.Mutex
	jfsControls   = map[string]*jfsControl{}
	jfsLanes      = map[string]*jfsControl{}
)

// errCreateFilesRefused wraps the refusal of a whole createFiles message
// (malformed, read-only, unsupported): juicefs touched no file.
type errCreateFilesRefused struct{ err error }

func (e *errCreateFilesRefused) Error() string {
	return "juicefs refused create-files: " + e.err.Error()
}
func (e *errCreateFilesRefused) Unwrap() error { return e.err }

// createFiles sends one JMCreateFiles message. A nil error with per-file
// results means juicefs answered. *errCreateFilesRefused means it refused the
// whole message and wrote nothing (errControlUnsupported inside: a juicefs or
// metadata engine without the message). Any other error leaves the outcome of
// every file unknown: any of them may have been created.
func (c *jfsControl) createFiles(files []jfsNewFile) ([]jfsCreated, error) {
	if len(files) == 0 || len(files) > jfsCreateFilesMax {
		return nil, &errCreateFilesRefused{syscall.EINVAL}
	}
	body := jfsCreateFilesBodyBase
	total := 0
	for _, f := range files {
		if len(f.name) == 0 || len(f.name) > 255 {
			return nil, &errCreateFilesRefused{syscall.ENAMETOOLONG}
		}
		if len(f.data) > jfsCreateFileMaxBytes {
			return nil, &errCreateFilesRefused{syscall.EFBIG}
		}
		body += 8 + 8 + 2 + 2 + 8 + 4 + 1 + len(f.name) + 4 + len(f.data)
		total += len(f.data)
	}
	msg := controlHeader(jfsMsgCreateFiles, body)
	msg = binary.BigEndian.AppendUint16(msg, uint16(len(files)))
	for _, f := range files {
		sec, nsec := f.mtime.Unix(), f.mtime.Nanosecond()
		if sec < 0 {
			sec, nsec = 0, 0
		}
		msg = binary.BigEndian.AppendUint64(msg, f.parent)
		msg = binary.BigEndian.AppendUint64(msg, f.preIno)
		msg = binary.BigEndian.AppendUint16(msg, uint16(f.mode.Perm()))
		msg = binary.BigEndian.AppendUint16(msg, 0) // the mode is final; no umask
		msg = binary.BigEndian.AppendUint64(msg, uint64(sec))
		msg = binary.BigEndian.AppendUint32(msg, uint32(nsec))
		msg = append(msg, byte(len(f.name)))
		msg = append(msg, f.name...)
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(f.data)))
		msg = append(msg, f.data...)
	}
	replyLen := 4 + 9*len(files)
	want := func(got []byte) int {
		if len(got) >= 1 && got[0] != jfsCreateFilesReplyMarker {
			return 1 // an older juicefs: one errno byte
		}
		return replyLen
	}
	reply, err := c.exchangeUntil(msg, replyLen, want, jfsCreateFilesTimeout(total))
	if err != nil {
		var ns *errControlNotSent
		if errors.As(err, &ns) {
			return nil, &errCreateFilesRefused{ns.err}
		}
		return nil, err
	}
	if len(reply) >= 1 && reply[0] != jfsCreateFilesReplyMarker {
		// A juicefs without the message ("unknown message type"): it wrote
		// nothing.
		return nil, &errCreateFilesRefused{errControlUnsupported}
	}
	if len(reply) != replyLen || int(binary.BigEndian.Uint16(reply[2:4])) != len(files) {
		return nil, fmt.Errorf("juicefs control: malformed create-files reply")
	}
	reply = reply[1:]
	if err := controlErrno(reply[0]); err != nil {
		// The whole message was refused before any file was touched.
		return nil, &errCreateFilesRefused{err}
	}
	out := make([]jfsCreated, len(files))
	for i := range out {
		out[i].err = controlErrno(reply[3+9*i])
		out[i].ino = binary.BigEndian.Uint64(reply[4+9*i:])
	}
	return out, nil
}
