// Package wire is the message format of the JuiceMount engine socket: the
// private Unix socket between the JuiceMount app and its `juicefs jm-engine`
// helper (JuiceMount patch 0037). It has no JuiceFS dependencies, so the app
// imports it without pulling in the engine.
//
// One connection carries one request at a time: the client writes a request,
// then reads exactly one reply, then may send the next request. There is no
// multiplexing and no request ID. The first request on every connection is
// OpHello with the launch token; the helper closes a connection whose first
// request is anything else or carries the wrong token.
//
// Request header (16 bytes, big endian):
//
//	op u16 | class u8 | flags u8 | deadline_ms u32 | body_len u32 | reserved u32
//
// Reply header (12 bytes, big endian):
//
//	status u32 (0 or an errno of this host) | flags u16 | reserved u16 | body_len u32
//
// Step 1 bodies are JSON. Later steps send file bytes raw after the header.
package wire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Protocol version. A different major is refused by both sides.
const (
	ProtoMajor = 1
	ProtoMinor = 0
)

const (
	RequestHeaderLen = 16
	ReplyHeaderLen   = 12
	// MaxRequestBody bounds a request body. Step 1 requests are tiny.
	MaxRequestBody = 64 << 10
	// MaxReplyBody bounds a reply body (stats carries the metrics text).
	MaxReplyBody = 8 << 20
	// MaxStartMessage bounds the start message on the private pipe.
	MaxStartMessage = 64 << 10
)

// Op is one engine operation.
type Op uint16

const (
	OpHello    Op = 1
	OpPing     Op = 2
	OpStats    Op = 3
	OpGetAttr  Op = 4
	OpShutdown Op = 5
)

// String names an operation for logs and the hello's list.
func (o Op) String() string {
	switch o {
	case OpHello:
		return "hello"
	case OpPing:
		return "ping"
	case OpStats:
		return "stats"
	case OpGetAttr:
		return "getattr"
	case OpShutdown:
		return "shutdown"
	}
	return fmt.Sprintf("op%d", uint16(o))
}

// Class is the connection lane a request belongs to. The app keeps a pool of
// connections per class so a request never waits behind another class.
type Class uint8

const (
	ClassCtl   Class = 0
	ClassRead  Class = 1
	ClassMeta  Class = 2
	ClassWrite Class = 3
	ClassBulk  Class = 4
)

// Request is one decoded request.
type Request struct {
	Op         Op
	Class      Class
	Flags      uint8
	DeadlineMs uint32 // 0 = the helper's default bound
	Body       []byte
}

// Reply is one decoded reply.
type Reply struct {
	Status uint32 // 0 = success; otherwise an errno of this host
	Flags  uint16
	Body   []byte
}

// ErrTooLarge is returned for a body over the bound.
var ErrTooLarge = errors.New("engine wire: body too large")

// WriteRequest writes one request in a single write.
func WriteRequest(w io.Writer, r Request) error {
	if len(r.Body) > MaxRequestBody {
		return ErrTooLarge
	}
	buf := make([]byte, RequestHeaderLen+len(r.Body))
	binary.BigEndian.PutUint16(buf[0:], uint16(r.Op))
	buf[2] = byte(r.Class)
	buf[3] = r.Flags
	binary.BigEndian.PutUint32(buf[4:], r.DeadlineMs)
	binary.BigEndian.PutUint32(buf[8:], uint32(len(r.Body)))
	copy(buf[RequestHeaderLen:], r.Body)
	_, err := w.Write(buf)
	return err
}

// ReadRequest reads one request.
func ReadRequest(rd io.Reader) (Request, error) {
	var h [RequestHeaderLen]byte
	if _, err := io.ReadFull(rd, h[:]); err != nil {
		return Request{}, err
	}
	n := binary.BigEndian.Uint32(h[8:])
	if n > MaxRequestBody {
		return Request{}, ErrTooLarge
	}
	r := Request{
		Op:         Op(binary.BigEndian.Uint16(h[0:])),
		Class:      Class(h[2]),
		Flags:      h[3],
		DeadlineMs: binary.BigEndian.Uint32(h[4:]),
	}
	if n > 0 {
		r.Body = make([]byte, n)
		if _, err := io.ReadFull(rd, r.Body); err != nil {
			return Request{}, err
		}
	}
	return r, nil
}

// WriteReply writes one reply in a single write.
func WriteReply(w io.Writer, r Reply) error {
	if len(r.Body) > MaxReplyBody {
		return ErrTooLarge
	}
	buf := make([]byte, ReplyHeaderLen+len(r.Body))
	binary.BigEndian.PutUint32(buf[0:], r.Status)
	binary.BigEndian.PutUint16(buf[4:], r.Flags)
	binary.BigEndian.PutUint32(buf[8:], uint32(len(r.Body)))
	copy(buf[ReplyHeaderLen:], r.Body)
	_, err := w.Write(buf)
	return err
}

// ReadReply reads one reply.
func ReadReply(rd io.Reader) (Reply, error) {
	var h [ReplyHeaderLen]byte
	if _, err := io.ReadFull(rd, h[:]); err != nil {
		return Reply{}, err
	}
	n := binary.BigEndian.Uint32(h[8:])
	if n > MaxReplyBody {
		return Reply{}, ErrTooLarge
	}
	r := Reply{Status: binary.BigEndian.Uint32(h[0:]), Flags: binary.BigEndian.Uint16(h[4:])}
	if n > 0 {
		r.Body = make([]byte, n)
		if _, err := io.ReadFull(rd, r.Body); err != nil {
			return Reply{}, err
		}
	}
	return r, nil
}

// StartMessage is the one message the app writes to the helper's private
// pipe at launch. Secrets travel here, never in argv or the environment.
type StartMessage struct {
	Token        string `json:"token"` // hex, 32 bytes
	MetaPassword string `json:"meta_password,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
}

// WriteStartMessage writes a length-prefixed JSON start message.
func WriteStartMessage(w io.Writer, m StartMessage) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(body) > MaxStartMessage {
		return ErrTooLarge
	}
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf, uint32(len(body)))
	copy(buf[4:], body)
	_, err = w.Write(buf)
	return err
}

// ReadStartMessage reads a length-prefixed JSON start message.
func ReadStartMessage(r io.Reader) (StartMessage, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return StartMessage{}, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n > MaxStartMessage {
		return StartMessage{}, ErrTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return StartMessage{}, err
	}
	var m StartMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return StartMessage{}, fmt.Errorf("engine wire: start message: %w", err)
	}
	return m, nil
}

// HelloRequest is the body of OpHello from the app.
type HelloRequest struct {
	ProtoMajor int    `json:"proto_major"`
	ProtoMinor int    `json:"proto_minor"`
	Token      string `json:"token"` // hex
}

// HelloReply is the body of the helper's answer to OpHello. The helper sends
// it also when it refuses a protocol major, so the app can log what it met.
type HelloReply struct {
	ProtoMajor     int      `json:"proto_major"`
	ProtoMinor     int      `json:"proto_minor"`
	JuiceFSVersion string   `json:"juicefs_version"`
	Ops            []string `json:"ops"`
	Control        []int    `json:"control"` // control message numbers served (none in step 1)
	VolumeID       string   `json:"volume_id"`
	VolumeName     string   `json:"volume_name"`
	SessionID      uint64   `json:"session_id"` // 0 for a read-only engine (no session record)
	Epoch          uint64   `json:"epoch"`
	PID            int      `json:"pid"`
	ReadOnly       bool     `json:"read_only"`
}

// PingReply is the body of the answer to OpPing. Ping never touches the NAS
// or an engine lock. Ages are -1 when no such request has succeeded yet.
type PingReply struct {
	Epoch         uint64 `json:"epoch"`
	UptimeMs      int64  `json:"uptime_ms"`
	MetaOKAgeMs   int64  `json:"meta_ok_age_ms"`
	ObjectOKAgeMs int64  `json:"object_ok_age_ms"`
	InFlight      int64  `json:"in_flight"`
}

// StatsReply is the body of the answer to OpStats.
type StatsReply struct {
	Epoch       uint64            `json:"epoch"`
	UptimeMs    int64             `json:"uptime_ms"`
	Requests    map[string]uint64 `json:"requests"`
	Refused     map[string]uint64 `json:"refused"`
	Connections int64             `json:"connections"`
	Goroutines  int               `json:"goroutines"`
	Metrics     string            `json:"metrics,omitempty"` // Prometheus text of the engine registry
}

// GetAttrRequest is the body of OpGetAttr.
type GetAttrRequest struct {
	Inode uint64 `json:"inode"`
}

// Attr is the body of the answer to OpGetAttr. Times are Unix nanoseconds.
type Attr struct {
	Inode uint64 `json:"inode"`
	Type  uint8  `json:"type"`
	Mode  uint16 `json:"mode"`
	Uid   uint32 `json:"uid"`
	Gid   uint32 `json:"gid"`
	Nlink uint32 `json:"nlink"`
	Size  uint64 `json:"size"`
	Atime int64  `json:"atime_ns"`
	Mtime int64  `json:"mtime_ns"`
	Ctime int64  `json:"ctime_ns"`
}
