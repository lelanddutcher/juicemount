// Package client is the unprivileged side of the JuiceMount unmount helper
// (juicemount-unmountd): the XPC wire protocol, the code-signing requirement
// strings, and a typed Go client for juicemount-service.
//
// The package is a leaf. It does not import internal/unmountd (the root
// daemon's core), so the service binary links none of the helper's
// privileged code. The daemon imports this package for the codec, so both
// ends share one definition of the wire.
//
// # Wire format
//
// Every message is an XPC dictionary with two keys:
//
//	"jm.v"    int64  ProtocolVersion
//	"jm.body" data   one JSON object (Request or Response), at most MaxBody bytes
//
// A request always expects a reply. Swift callers (the GUI's status reads)
// build the same dictionary with xpc_dictionary_set_int64/_set_data.
//
// # Two mach services, two roles
//
// The daemon listens on two names. Each listener sets its own peer
// code-signing requirement on every incoming connection, so the role is
// proven by which requirement the peer passed, never by a payload field:
//
//	MachService        com.juicemount.unmountd         service binary only  -> RoleService
//	StatusMachService  com.juicemount.unmountd.status  app or service       -> RoleGUI (status/ping only)
//
// # Leases
//
// A connection that sends "register" or "reclaim" successfully is bound to
// that op for its lifetime. When the last bound connection for an op closes
// (the owner exited or crashed), the daemon arms the core's orphan timer
// (Service.Disconnected). A reconnecting owner sends "reclaim" within the
// window to cancel it. Keep a Lease open for as long as the mount is yours.
package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

// ProtocolVersion is the "jm.v" value of every message.
const ProtocolVersion = 1

// XPC dictionary keys.
const (
	KeyVersion = "jm.v"
	KeyBody    = "jm.body"
)

// MaxBody bounds one JSON body in either direction.
const MaxBody = 64 << 10

// Mach service names (the LaunchDaemon plist's MachServices keys).
const (
	MachService       = "com.juicemount.unmountd"
	StatusMachService = "com.juicemount.unmountd.status"
)

// Op names.
const (
	OpPing     = "ping"
	OpRegister = "register"
	OpReclaim  = "reclaim"
	OpRetire   = "retire"
	OpStatus   = "status"
	OpCancel   = "cancel"
)

// Liveness strings (core Liveness).
const (
	LivenessUnknown = "unknown"
	LivenessAlive   = "alive"
	LivenessDead    = "dead"
	LivenessWedged  = "wedged"
)

// Response codes. CodeOK and CodePending are not errors.
const (
	CodeOK              = "ok"
	CodePending         = "pending" // retire: the job is still running; Outcome is progress
	CodeBadRequest      = "bad_request"
	CodeNotAuthorized   = "not_authorized"
	CodeReceiptLimit    = "receipt_limit"
	CodeReceiptExists   = "receipt_exists"
	CodeNoReceipt       = "no_receipt"
	CodeInvalidReceipt  = "invalid_receipt"
	CodeIdentityRefused = "identity_refused"
	CodeOpIDReused      = "op_id_reused"
	CodePostState       = "post_state"
	CodeAlreadyBound    = "already_bound"
	CodeBusy            = "busy"
	CodeInternal        = "internal"
	CodeNotCancellable  = "not_cancellable"
)

// MaxRetireWait is the longest a daemon holds one retire request open.
const MaxRetireWait = 60_000 // milliseconds

// opIDPattern mirrors the core's receipt op id rule.
var opIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// ValidOpID reports whether id is a well-formed op id.
func ValidOpID(id string) bool { return opIDPattern.MatchString(id) }

// Request is one call. Exactly the arguments for Op may be present.
type Request struct {
	Op         string        `json:"op"`
	OpID       string        `json:"op_id,omitempty"`
	Register   *RegisterArgs `json:"register,omitempty"`
	Retire     *RetireArgs   `json:"retire,omitempty"`
	WaitMillis int64         `json:"wait_ms,omitempty"` // retire only, 0..MaxRetireWait
}

// RegisterArgs is the owner's claim about a mount it just observed. The
// daemon proves it against a fresh mount-table snapshot before storing it.
type RegisterArgs struct {
	Type        string   `json:"type"` // "nfs" or "macfuse"
	FSID        [2]int32 `json:"fsid"`
	Token       string   `json:"token"` // 32 lowercase hex chars
	MntOnName   string   `json:"mntonname"`
	MntFromName string   `json:"mntfromname"`
	Generation  uint64   `json:"generation,omitempty"`
}

// RetireArgs is what the owner proved when asking for retirement.
type RetireArgs struct {
	Liveness string `json:"liveness"`
	EjectNow bool   `json:"eject_now,omitempty"`
}

// Response is one reply.
type Response struct {
	Code      string    `json:"code"`
	Message   string    `json:"message,omitempty"`
	Receipt   *Receipt  `json:"receipt,omitempty"`
	Outcome   *Outcome  `json:"outcome,omitempty"`
	Reclaimed bool      `json:"reclaimed,omitempty"`
	Ping      *PingInfo `json:"ping,omitempty"`
}

// Receipt is the stored record, without the token.
type Receipt struct {
	UID         uint32   `json:"uid"`
	Boot        string   `json:"boot"`
	OpID        string   `json:"op_id"`
	Type        string   `json:"type"`
	FSID        [2]int32 `json:"fsid"`
	MntOnName   string   `json:"mntonname"`
	MntFromName string   `json:"mntfromname"`
	Generation  uint64   `json:"generation,omitempty"`
}

// Outcome mirrors the core's retirement progress.
type Outcome struct {
	OpID          string   `json:"op_id"`
	State         string   `json:"state"`
	Reason        string   `json:"reason,omitempty"`
	Syscalls      int      `json:"syscalls,omitempty"`
	Forced        bool     `json:"forced,omitempty"`
	StuckObserved bool     `json:"stuck_observed,omitempty"`
	Holders       []Holder `json:"holders,omitempty"`
	HoldersStale  bool     `json:"holders_stale,omitempty"`
	Descendants   []Entry  `json:"descendants,omitempty"`
	Anomaly       []Entry  `json:"anomaly,omitempty"`
	Err           string   `json:"err,omitempty"`
}

// Terminal states (core State.terminal).
var terminalStates = map[string]bool{
	"RETIRED": true, "REFUSED_IDENTITY": true, "VOID": true, "FAILED": true, "ANOMALY": true,
}

// Terminal reports whether the outcome is final.
func (o Outcome) Terminal() bool { return terminalStates[o.State] }

// Holder names a process keeping the volume busy. Display only.
type Holder struct {
	PID   int32  `json:"pid"`
	UID   uint32 `json:"uid"`
	Name  string `json:"name"`
	Files int    `json:"files"`
}

// Entry is one mount-table row.
type Entry struct {
	FSID        [2]int32 `json:"fsid"`
	Owner       uint32   `json:"owner"`
	Type        string   `json:"type"`
	MntOnName   string   `json:"mntonname"`
	MntFromName string   `json:"mntfromname"`
}

// PingInfo describes the running daemon.
type PingInfo struct {
	Protocol int    `json:"protocol"`
	Channel  string `json:"channel"` // ChannelRelease or ChannelDev
	Role     string `json:"role"`    // the role this connection was admitted as
	UID      uint32 `json:"uid"`     // the euid the daemon sees for this connection
}

// ErrFraming is returned for any message that is not a well-formed v1 body.
var ErrFraming = errors.New("unmountd wire: malformed message")

func framing(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrFraming, fmt.Sprintf(format, args...))
}

// decodeStrict decodes exactly one JSON object with no unknown fields and
// nothing after it.
func decodeStrict(version int64, body []byte, v any) error {
	if version != ProtocolVersion {
		return framing("protocol version %d, want %d", version, ProtocolVersion)
	}
	if len(body) == 0 {
		return framing("empty body")
	}
	if len(body) > MaxBody {
		return framing("body %d bytes exceeds %d", len(body), MaxBody)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return framing("%v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return framing("trailing data after the body")
	}
	return nil
}

func encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxBody {
		return nil, framing("encoded body %d bytes exceeds %d", len(b), MaxBody)
	}
	return b, nil
}

// EncodeRequest validates and encodes a request body.
func EncodeRequest(r Request) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return encode(r)
}

// DecodeRequest decodes and validates a request body.
func DecodeRequest(version int64, body []byte) (Request, error) {
	var r Request
	if err := decodeStrict(version, body, &r); err != nil {
		return Request{}, err
	}
	if err := r.Validate(); err != nil {
		return Request{}, err
	}
	return r, nil
}

// EncodeResponse encodes a response body.
func EncodeResponse(r Response) ([]byte, error) {
	if r.Code == "" {
		return nil, framing("response without code")
	}
	return encode(r)
}

// DecodeResponse decodes a response body.
func DecodeResponse(version int64, body []byte) (Response, error) {
	var r Response
	if err := decodeStrict(version, body, &r); err != nil {
		return Response{}, err
	}
	if r.Code == "" {
		return Response{}, framing("response without code")
	}
	return r, nil
}

// Validate checks that the request is well formed: a known op, a valid op
// id where one is needed, and exactly that op's arguments.
func (r Request) Validate() error {
	needOpID := true
	switch r.Op {
	case OpPing:
		needOpID = false
		if r.Register != nil || r.Retire != nil || r.WaitMillis != 0 {
			return framing("ping takes no arguments")
		}
	case OpRegister:
		if r.Register == nil || r.Retire != nil || r.WaitMillis != 0 {
			return framing("register takes register arguments only")
		}
		if err := r.Register.validate(); err != nil {
			return err
		}
	case OpRetire:
		if r.Retire == nil || r.Register != nil {
			return framing("retire takes retire arguments only")
		}
		switch r.Retire.Liveness {
		case LivenessUnknown, LivenessAlive, LivenessDead, LivenessWedged:
		default:
			return framing("retire liveness %q", r.Retire.Liveness)
		}
		if r.WaitMillis < 0 || r.WaitMillis > MaxRetireWait {
			return framing("retire wait %d ms outside 0..%d", r.WaitMillis, MaxRetireWait)
		}
	case OpReclaim, OpStatus, OpCancel:
		if r.Register != nil || r.Retire != nil || r.WaitMillis != 0 {
			return framing("%s takes no arguments", r.Op)
		}
	default:
		return framing("unknown op %q", r.Op)
	}
	if needOpID && !ValidOpID(r.OpID) {
		return framing("op id %q", r.OpID)
	}
	if !needOpID && r.OpID != "" {
		return framing("%s takes no op id", r.Op)
	}
	return nil
}

// validate is a shape check only. The daemon's core re-validates every
// field against the live mount table; this just refuses obvious garbage
// before it crosses the privilege boundary.
func (a *RegisterArgs) validate() error {
	if a.Type != "nfs" && a.Type != "macfuse" {
		return framing("register type %q", a.Type)
	}
	if a.FSID == [2]int32{} {
		return framing("register fsid is zero")
	}
	if len(a.Token) != 32 {
		return framing("register token length %d", len(a.Token))
	}
	if a.MntOnName == "" || len(a.MntOnName) >= 1024 || a.MntFromName == "" || len(a.MntFromName) >= 1024 {
		return framing("register kernel strings out of range")
	}
	return nil
}
