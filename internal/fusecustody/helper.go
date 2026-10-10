package fusecustody

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const protocolVersion = 1
const maxFrame = 512

type Event struct {
	Version  int      `json:"version"`
	Session  string   `json:"session"`
	ID       uint64   `json:"id"`
	State    string   `json:"state"`
	UUID     string   `json:"uuid,omitempty"`
	ReadOnly *bool    `json:"read_only,omitempty"`
	FSID     [2]int32 `json:"fsid,omitempty"`
}
type request struct {
	Version int    `json:"version"`
	Session string `json:"session"`
	ID      uint64 `json:"id"`
	Op      string `json:"op"`
}
type rootCustody interface {
	UUID() string
	ReadOnly() bool
	FSID() [2]int32
	Detach() error
	Close() error
}

func holdLostControl() {
	for {
		time.Sleep(time.Hour)
	}
}
func validSession(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func writeEvent(w io.Writer, event Event) error {
	raw, err := json.Marshal(event)
	if err != nil || len(raw)+1 > maxFrame {
		return errors.New("invalid custody event")
	}
	raw = append(raw, '\n')
	n, err := w.Write(raw)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	return nil
}
func readFrame(r *bufio.Reader, value any) error {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if len(line) > maxFrame {
		return errors.New("custody frame oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("custody frame contains trailing JSON")
	}
	return nil
}
func CapabilityRun(args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 {
		fmt.Fprintln(stderr, "capability takes no arguments")
		return 64
	}
	state := "available"
	if !nativeCapability() {
		state = "unsupported_os"
	}
	if err := writeEvent(stdout, Event{Version: protocolVersion, State: state}); err != nil {
		return 74
	}
	if state != "available" {
		return 78
	}
	return 0
}

// Explicit custody mode ignores SIGPIPE before any output, preserving the FD
// when the parent closes stdout. Snapshot mode keeps its original behavior.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) != 5 || !validSession(args[4]) {
		fmt.Fprintln(stderr, "custodian identity invalid")
		return 64
	}
	child, childErr := strconv.Atoi(args[1])
	parent, parentErr := strconv.Atoi(args[2])
	if childErr != nil || parentErr != nil || child <= 1 || parent <= 1 {
		return 64
	}
	return runCustodianWithOpen(args[0], child, parent, args[3], args[4], stdin, stdout, openNativeRoot)
}
func runCustodianWithOpen(root string, child, parent int, source, session string, stdin io.Reader, stdout io.Writer, open func(string, int, int, string) (rootCustody, error)) int {
	sigpipe := make(chan os.Signal, 1)
	signal.Notify(sigpipe, syscall.SIGPIPE)
	defer signal.Stop(sigpipe)
	return runWithOpen(root, child, parent, source, session, stdin, stdout, open)
}
func runWithOpen(root string, child, parent int, source, session string, stdin io.Reader, stdout io.Writer, open func(string, int, int, string) (rootCustody, error)) int {
	emit := func(id uint64, state, uuid string, readonly *bool, fsid [2]int32) {
		if err := writeEvent(stdout, Event{Version: protocolVersion, Session: session, ID: id, State: state, UUID: uuid, ReadOnly: readonly, FSID: fsid}); err != nil {
			holdLostControl()
		}
	}
	custody, err := open(root, child, parent, source)
	if err != nil {
		state := "admission_refused"
		switch {
		case errors.Is(err, ErrUnsupported):
			state = "unsupported_os"
		case errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES):
			// A denied macOS Network Volumes approval fails the root open
			// with "operation not permitted". Report it as its own terminal
			// state so the owner can tell the user what to grant.
			state = "admission_permission_denied"
		}
		emit(0, state, "", nil, [2]int32{})
		return 78
	}
	readonly := custody.ReadOnly()
	emit(0, "root_admitted", custody.UUID(), &readonly, custody.FSID())
	reader := bufio.NewReaderSize(stdin, maxFrame+1)
	detached := false
	var lastID uint64
	for {
		var req request
		if err := readFrame(reader, &req); err != nil {
			holdLostControl()
		}
		if req.Version != protocolVersion || req.Session != session || req.ID != lastID+1 {
			holdLostControl()
		}
		lastID = req.ID
		switch req.Op {
		case "detach":
			if detached {
				emit(req.ID, "already_detached", "", nil, [2]int32{})
				continue
			}
			if err := custody.Detach(); err != nil {
				emit(req.ID, "retained_retry", "", nil, [2]int32{})
				continue
			}
			detached = true
			emit(req.ID, "detached_child_join_required", "", nil, [2]int32{})
		case "release":
			if !detached {
				emit(req.ID, "command_refused", "", nil, [2]int32{})
				continue
			}
			if err := custody.Close(); err != nil {
				emit(req.ID, "close_failed", "", nil, [2]int32{})
				continue
			}
			emit(req.ID, "released", "", nil, [2]int32{})
			return 0
		default:
			holdLostControl()
		}
	}
}

var ErrUnsupported = errors.New("descriptor FUSE detach unsupported")
