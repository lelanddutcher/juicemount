// Package respproxy is the H-8 option B Redis-protocol (RESP) proxy that sits
// in front of the JuiceFS metadata Redis. It authenticates every client as a
// named principal, classifies every command and key, refuses admin-only and
// unknown write shapes, and hands project-scoped writes to a Policy (lane L2:
// per-project leases). See policy.go for the frozen Decide interface and the
// package tests for the protocol behavior.
//
// Ported from the measurement prototype (prototype/respproxy @ 828baa3).
package respproxy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// maxBulk bounds one reply bulk string (Redis's proto-max-bulk-len).
const maxBulk = 512 << 20

// maxReplyElems bounds one reply aggregate parsed by ReadValue.
const maxReplyElems = 1 << 20

// Limits bounds one client command. Memory is allocated only for bytes the
// client has actually sent (bulk arguments grow as they arrive), so a
// declared size alone costs nothing.
type Limits struct {
	MaxArgs  int   // arguments per command
	MaxBulk  int64 // bytes per argument
	MaxTotal int64 // bytes per command, all arguments together
}

var (
	// PreAuthLimits apply until a connection authenticates: AUTH user pass
	// is 3 arguments and HELLO 3 AUTH u p SETNAME n is 7; Redis itself caps
	// unauthenticated multibulk at 10 arguments and bulk at 16 KB.
	PreAuthLimits = Limits{MaxArgs: 10, MaxBulk: 16 << 10, MaxTotal: 16 << 10}
	// AuthLimits apply to authenticated principals. The largest values the
	// tree writes are xattrs (<= 64 KiB), farm job JSON and the 512 KiB link
	// benchmark payload (bridge/cbridge.go linkDataPlaneBenchmarkBytes);
	// 16 MiB per argument leaves 32x headroom. 64 MiB per command and 1M
	// arguments bound a juicefs pipeline of large DELs/MSETs.
	AuthLimits = Limits{MaxArgs: 1 << 20, MaxBulk: 16 << 20, MaxTotal: 64 << 20}
)

var errProtocol = errors.New("respproxy: protocol error")

// readLine returns one CRLF-terminated line without the CRLF. The slice is
// only valid until the next read on r.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// Rare: a line longer than the buffer (only inline commands).
		buf := append([]byte(nil), line...)
		for err == bufio.ErrBufferFull {
			if len(buf) > 64<<10 {
				return nil, errProtocol
			}
			line, err = r.ReadSlice('\n')
			buf = append(buf, line...)
		}
		line = buf
	}
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errProtocol
	}
	return line[:len(line)-2], nil
}

func parseInt(b []byte) (int64, error) {
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, errProtocol
	}
	return n, nil
}

// ReadCommand reads one client command under AuthLimits.
func ReadCommand(r *bufio.Reader) ([][]byte, error) { return ReadCommandLimits(r, AuthLimits) }

// ReadCommandLimits reads one client command: a RESP array of bulk strings,
// or an inline command. Arguments are freshly allocated. Any declared size
// over lim is a protocol error before anything is allocated for it.
func ReadCommandLimits(r *bufio.Reader, lim Limits) ([][]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil // empty inline line: ignore
	}
	if line[0] != '*' {
		if int64(len(line)) > lim.MaxTotal {
			return nil, errProtocol
		}
		args := splitInline(line)
		if len(args) > lim.MaxArgs {
			return nil, errProtocol
		}
		return args, nil
	}
	n, err := parseInt(line[1:])
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > int64(lim.MaxArgs) {
		return nil, errProtocol
	}
	args := make([][]byte, 0, min(n, 64))
	total := int64(0)
	for i := int64(0); i < n; i++ {
		hdr, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, errProtocol
		}
		l, err := parseInt(hdr[1:])
		if err != nil || l < 0 || l > lim.MaxBulk {
			return nil, errProtocol
		}
		total += l
		if total > lim.MaxTotal {
			return nil, errProtocol
		}
		b, err := readBulk(r, l)
		if err != nil {
			return nil, err
		}
		args = append(args, b)
	}
	return args, nil
}

// readBulk reads l bytes plus CRLF, growing the buffer as data arrives.
func readBulk(r *bufio.Reader, l int64) ([]byte, error) {
	const chunk = 64 << 10
	b := make([]byte, 0, min(l+2, chunk))
	for int64(len(b)) < l+2 {
		need := l + 2 - int64(len(b))
		if need > chunk {
			need = chunk
		}
		start := len(b)
		b = append(b, make([]byte, need)...)
		if _, err := io.ReadFull(r, b[start:]); err != nil {
			return nil, err
		}
	}
	if b[l] != '\r' || b[l+1] != '\n' {
		return nil, errProtocol
	}
	return b[:l:l], nil
}

func splitInline(line []byte) [][]byte {
	var args [][]byte
	start := -1
	for i, c := range line {
		if c == ' ' || c == '\t' {
			if start >= 0 {
				args = append(args, append([]byte(nil), line[start:i]...))
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		args = append(args, append([]byte(nil), line[start:]...))
	}
	return args
}

// WriteCommand writes args as a RESP array of bulk strings.
func WriteCommand(w *bufio.Writer, args [][]byte) error {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(args)))
	w.WriteString("\r\n")
	for _, a := range args {
		w.WriteByte('$')
		w.WriteString(strconv.Itoa(len(a)))
		w.WriteString("\r\n")
		w.Write(a)
		if _, err := w.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return nil
}

func strArgs(s ...string) [][]byte {
	out := make([][]byte, len(s))
	for i, a := range s {
		out[i] = []byte(a)
	}
	return out
}

// FrameInfo describes one copied reply frame.
type FrameInfo struct {
	Type byte // RESP type byte of the logical reply (after any attribute)
	Null bool // a RESP2 null bulk/array ($-1 / *-1) or RESP3 null (_)
}

// IsError reports a simple or blob error.
func (f FrameInfo) IsError() bool { return f.Type == '-' || f.Type == '!' }

// CopyFrame copies exactly one RESP2/RESP3 reply frame from r to w. An
// attribute frame ('|') is copied together with the frame it annotates, so
// one call always copies one logical reply. Parsing the reply stream (rather
// than a blind io.Copy) is what lets the proxy splice a refusal into the
// correct reply slot.
func CopyFrame(r *bufio.Reader, w *bufio.Writer) (FrameInfo, error) {
	line, err := readLine(r)
	if err != nil {
		return FrameInfo{}, err
	}
	if len(line) == 0 {
		return FrameInfo{}, errProtocol
	}
	t := line[0]
	info := FrameInfo{Type: t, Null: t == '_'}
	w.Write(line)
	if _, err := w.WriteString("\r\n"); err != nil {
		return info, err
	}
	switch t {
	case '+', '-', ':', ',', '_', '#', '(':
		return info, nil
	case '$', '!', '=':
		n, err := parseInt(line[1:])
		if err != nil {
			return info, err
		}
		if n < 0 {
			info.Null = true
			return info, nil
		}
		if n > maxBulk {
			return info, errProtocol
		}
		_, err = io.CopyN(w, r, n+2)
		return info, err
	case '*', '~', '>', '%', '|':
		n, err := parseInt(line[1:])
		if err != nil {
			return info, err
		}
		if n < 0 {
			info.Null = true
			return info, nil
		}
		if t == '%' || t == '|' {
			n *= 2
		}
		for i := int64(0); i < n; i++ {
			if _, err := CopyFrame(r, w); err != nil {
				return info, err
			}
		}
		if t == '|' { // attribute precedes the real reply
			return CopyFrame(r, w)
		}
		return info, nil
	default:
		return info, fmt.Errorf("%w: reply type %q", errProtocol, t)
	}
}

// Value is one parsed reply, used by the proxy's own side connections.
type Value struct {
	Type  byte
	Str   []byte // simple/bulk string, error text, verbatim, double, big number
	Int   int64
	Elems []Value // array, set, push, map (k,v,k,v...)
	Null  bool
}

// Err returns the error text as an error for '-' / '!' replies.
func (v Value) Err() error {
	if v.Type == '-' || v.Type == '!' {
		return errors.New(string(v.Str))
	}
	return nil
}

// ReadValue parses one reply frame.
func ReadValue(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, errProtocol
	}
	v := Value{Type: line[0]}
	switch v.Type {
	case '+', '-', ',', '(', '#':
		v.Str = append([]byte(nil), line[1:]...)
	case '_':
		v.Null = true
	case ':':
		v.Int, err = parseInt(line[1:])
	case '$', '!', '=':
		n, e := parseInt(line[1:])
		if e != nil {
			return v, e
		}
		if n < 0 {
			v.Null = true
			return v, nil
		}
		if n > maxBulk {
			return v, errProtocol
		}
		b := make([]byte, n+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return v, err
		}
		v.Str = b[:n]
	case '*', '~', '>', '%', '|':
		n, e := parseInt(line[1:])
		if e != nil {
			return v, e
		}
		if n < 0 {
			v.Null = true
			return v, nil
		}
		if v.Type == '%' || v.Type == '|' {
			n *= 2
		}
		if n > maxReplyElems {
			return v, errProtocol
		}
		v.Elems = make([]Value, 0, min(n, 1024))
		for i := int64(0); i < n; i++ {
			e, err := ReadValue(r)
			if err != nil {
				return v, err
			}
			v.Elems = append(v.Elems, e)
		}
		if v.Type == '|' {
			return ReadValue(r)
		}
	default:
		return v, fmt.Errorf("%w: reply type %q", errProtocol, v.Type)
	}
	return v, err
}

// EncodeValue writes v as one RESP frame (the inverse of ReadValue; an
// attribute frame ReadValue skipped is not reproduced).
func EncodeValue(w *bufio.Writer, v Value) error {
	if v.Null {
		switch v.Type {
		case '_':
			w.WriteString("_\r\n")
		case '*', '~', '>', '%':
			w.WriteByte(v.Type)
			w.WriteString("-1\r\n")
		default:
			w.WriteByte(v.Type)
			w.WriteString("-1\r\n")
		}
		return nil
	}
	switch v.Type {
	case '+', '-', ',', '(', '#':
		w.WriteByte(v.Type)
		w.Write(v.Str)
		w.WriteString("\r\n")
	case ':':
		w.WriteByte(':')
		w.WriteString(strconv.FormatInt(v.Int, 10))
		w.WriteString("\r\n")
	case '$', '!', '=':
		w.WriteByte(v.Type)
		w.WriteString(strconv.Itoa(len(v.Str)))
		w.WriteString("\r\n")
		w.Write(v.Str)
		w.WriteString("\r\n")
	case '*', '~', '>', '%':
		n := len(v.Elems)
		if v.Type == '%' {
			n /= 2
		}
		w.WriteByte(v.Type)
		w.WriteString(strconv.Itoa(n))
		w.WriteString("\r\n")
		for _, e := range v.Elems {
			if err := EncodeValue(w, e); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: encode type %q", errProtocol, v.Type)
	}
	return nil
}
