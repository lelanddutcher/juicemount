package nfscustody

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
)

const protocolVersion = 1
const maxFrame = 768
const maxArmFrame = 4096

type event struct {
	Version   int      `json:"version"`
	Session   string   `json:"session,omitempty"`
	ID        uint64   `json:"id"`
	State     string   `json:"state"`
	Boot      string   `json:"boot,omitempty"`
	Operation string   `json:"operation,omitempty"`
	Path      string   `json:"path,omitempty"`
	Source    string   `json:"source,omitempty"`
	FSID      [2]int32 `json:"fsid,omitempty"`
}
type request struct {
	Version int                 `json:"version"`
	Session string              `json:"session"`
	ID      uint64              `json:"id"`
	Op      string              `json:"op"`
	Record  *mountbroker.Record `json:"record,omitempty"`
}
type rootCustody interface {
	FSID() [2]int32
	Detach() error
	Close() error
}

func validNonce(s string) bool {
	if len(s) != 32 {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == s
}
func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil || len(b)+1 > maxFrame {
		return errors.New("invalid NFS custody frame")
	}
	b = append(b, '\n')
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func writeRequestFrame(w io.Writer, req request) error {
	if req.Op != "arm" {
		return writeFrame(w, req)
	}
	b, err := json.Marshal(req)
	if err != nil || len(b)+1 > maxArmFrame {
		return errors.New("invalid NFS custody arm frame")
	}
	b = append(b, '\n')
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func readFrame(r *bufio.Reader, v any) error {
	b, err := r.ReadSlice('\n')
	if err != nil {
		if err == io.EOF && len(b) != 0 {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if len(b) > maxFrame {
		return errors.New("NFS custody frame oversized")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("NFS custody frame has trailing data")
	}
	return nil
}
func readRequestFrame(r *bufio.Reader, v *request) error {
	b, err := r.ReadSlice('\n')
	if err != nil {
		if err == io.EOF && len(b) != 0 {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if len(b) > maxArmFrame {
		return errors.New("NFS custody request oversized")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("NFS custody request has trailing data")
	}
	if v.Op != "arm" && len(b) > maxFrame {
		return errors.New("NFS custody non-arm request oversized")
	}
	if v.Op != "arm" && v.Record != nil {
		return errors.New("NFS custody non-arm request carried record")
	}
	return nil
}
func holdLostControl() {
	for {
		time.Sleep(time.Hour)
	}
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
	if err := writeFrame(stdout, event{Version: protocolVersion, State: state}); err != nil {
		return 74
	}
	if state != "available" {
		return 78
	}
	return 0
}

// Run is only invoked in the standalone sibling helper. Loss of the command
// pipe or output never releases an admitted descriptor.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) != 5 || !validNonce(args[0]) || !validNonce(args[2]) || args[1] == "" || args[3] == "" || args[4] == "" {
		fmt.Fprintln(stderr, "NFS custodian identity invalid")
		return 64
	}
	sigpipe := make(chan os.Signal, 1)
	signal.Notify(sigpipe, syscall.SIGPIPE, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(sigpipe)
	return runWithOpen(args[0], args[1], args[2], args[3], args[4], stdin, stdout, openNativeRoot)
}
func runWithOpen(session, boot, operation, path, source string, stdin io.Reader, stdout io.Writer, open func(string, string) (rootCustody, error)) int {
	return runWithOpenAndCreator(session, boot, operation, path, source, stdin, stdout, open, mountbroker.LocalCreator)
}
func runWithOpenAndCreator(session, boot, operation, path, source string, stdin io.Reader, stdout io.Writer, open func(string, string) (rootCustody, error), makeCreator func() (mountbroker.Creator, error)) int {
	return runWithOpenAndCreatorHold(session, boot, operation, path, source,
		stdin, stdout, open, makeCreator, holdLostControl)
}
func runWithOpenAndCreatorHold(session, boot, operation, path, source string, stdin io.Reader, stdout io.Writer, open func(string, string) (rootCustody, error), makeCreator func() (mountbroker.Creator, error), hold func()) int {
	var armed *mountbroker.ArmedDescriptorRetirement
	emit := func(id uint64, state string, fsid [2]int32) {
		if err := writeFrame(stdout, event{Version: protocolVersion, Session: session, ID: id, State: state, Boot: boot, Operation: operation, Path: path, Source: source, FSID: fsid}); err != nil {
			if armed == nil {
				hold()
			}
		}
	}
	root, err := open(path, source)
	if err != nil {
		state := "admission_refused"
		if errors.Is(err, ErrUnsupported) {
			state = "unsupported_os"
		}
		emit(0, state, [2]int32{})
		return 78
	}
	emit(0, "root_admitted", root.FSID())
	r := bufio.NewReaderSize(stdin, maxArmFrame+1)
	detached := false
	normalDetachAttempted := false
	var last uint64
	for {
		var req request
		readErr := readRequestFrame(r, &req)
		if readErr == io.EOF && armed != nil && !detached && !normalDetachAttempted {
			if err := armed.Cleanup(context.Background()); err != nil {
				hold()
			}
			return 0
		}
		if readErr != nil || req.Version != protocolVersion || req.Session != session || req.ID != last+1 {
			hold()
		}
		last = req.ID
		switch req.Op {
		case "arm":
			if detached || armed != nil || req.Record == nil ||
				req.Record.Version != 1 || req.Record.State != mountbroker.Observed ||
				req.Record.BootID != boot || req.Record.OperationID != operation ||
				req.Record.Spec.MountPath != path || req.Record.Spec.Source != source ||
				req.Record.Mount == nil || req.Record.Mount.FSID != root.FSID() {
				hold()
			}
			creator, err := makeCreator()
			if err != nil {
				hold()
			}
			armed, err = creator.ArmDescriptorRetirement(context.Background(), *req.Record, root)
			if err != nil {
				hold()
			}
			// A failed acknowledgment does not imply controller death. Keep the
			// original descriptor and continue reading the original stdin.
			_ = writeFrame(stdout, event{Version: protocolVersion, Session: session,
				ID: req.ID, State: "armed", Boot: boot, Operation: operation,
				Path: path, Source: source, FSID: root.FSID()})
		case "detach":
			if detached {
				emit(req.ID, "already_detached", [2]int32{})
				continue
			}
			normalDetachAttempted = true
			if root.Detach() != nil {
				emit(req.ID, "retained_retry", [2]int32{})
				continue
			}
			detached = true
			emit(req.ID, "detached", [2]int32{})
		case "release":
			if !detached {
				emit(req.ID, "command_refused", [2]int32{})
				continue
			}
			if root.Close() != nil {
				emit(req.ID, "close_failed", [2]int32{})
				continue
			}
			emit(req.ID, "released", [2]int32{})
			return 0
		default:
			hold()
		}
	}
}

var ErrUnsupported = errors.New("descriptor NFS detach unsupported")
