//go:build latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func latencyTestRequest(xid uint32) []byte {
	b := make([]byte, 40)
	for offset, value := range map[int]uint32{0: xid, 8: 2, 12: 100003, 16: 3, 20: 1} {
		binary.BigEndian.PutUint32(b[offset:], value)
	}
	return b
}

func latencyTestReply(xid uint32, status uint32) []byte {
	b := make([]byte, 28)
	binary.BigEndian.PutUint32(b, xid)
	binary.BigEndian.PutUint32(b[4:], 1)
	binary.BigEndian.PutUint32(b[24:], status)
	return b
}

func latencyTestFrame(body []byte) []byte {
	b := make([]byte, 4, len(body)+4)
	binary.BigEndian.PutUint32(b, uint32(len(body))|0x80000000)
	return append(b, body...)
}

type latencyBufferConn struct {
	net.Conn
	input  *bytes.Reader
	output bytes.Buffer
	short  bool
}

func (c *latencyBufferConn) Read(p []byte) (int, error) { return c.input.Read(p) }
func (c *latencyBufferConn) Write(p []byte) (int, error) {
	if c.short && len(p) > 1 {
		p = p[:1]
	}
	return c.output.Write(p)
}

func TestLatencyTransportSplitCoalescedAndOutOfOrder(t *testing.T) {
	requests := append(latencyTestFrame(latencyTestRequest(7)), latencyTestFrame(latencyTestRequest(8))...)
	replies := append(latencyTestFrame(latencyTestReply(8, 0)), latencyTestFrame(latencyTestReply(7, 10008))...)
	for _, chunk := range []int{1, 5, 4096} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			trace, _ := NewLatencyTransport(8)
			base := &latencyBufferConn{input: bytes.NewReader(requests)}
			conn := &latencyTransportConn{Conn: base, owner: trace, id: 1}
			buf := make([]byte, chunk)
			var read bytes.Buffer
			for {
				n, err := conn.Read(buf)
				read.Write(buf[:n])
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for offset := 0; offset < len(replies); {
				end := min(offset+chunk, len(replies))
				n, err := conn.Write(replies[offset:end])
				if err != nil || n != end-offset {
					t.Fatal("wire bytes changed")
				}
				offset = end
			}
			if !bytes.Equal(read.Bytes(), requests) || !bytes.Equal(base.output.Bytes(), replies) {
				t.Fatal("trace modified transport bytes")
			}
			status := trace.Status()
			if status.Error != "" || status.Requests != 2 || status.Pending != 0 {
				t.Fatalf("trace coverage: %+v", status)
			}
			if trace.rows[0].NFSOK || !trace.rows[1].NFSOK || trace.rows[0].WriteNS < trace.rows[0].ReadNS || trace.rows[1].WriteNS < trace.rows[1].ReadNS {
				t.Fatal("out-of-order/status attribution changed")
			}
		})
	}
}

func TestLatencyTransportRefusesIncompleteAttribution(t *testing.T) {
	for _, mode := range []string{"wrong-connection", "wrong-xid", "duplicate", "overflow", "fragment", "oversized", "short-write", "partial-eof"} {
		t.Run(mode, func(t *testing.T) {
			trace, _ := NewLatencyTransport(1)
			trace.request(1, latencyTestRequest(1), 1)
			switch mode {
			case "wrong-connection":
				trace.reply(2, latencyTestReply(1, 0), 2, true)
			case "wrong-xid":
				trace.reply(1, latencyTestReply(2, 0), 2, true)
			case "duplicate":
				trace.request(1, latencyTestRequest(1), 2)
			case "overflow":
				trace.request(1, latencyTestRequest(2), 2)
			case "fragment", "oversized":
				var stream latencyRecordStream
				var marker [4]byte
				if mode == "fragment" {
					binary.BigEndian.PutUint32(marker[:], 40)
				} else {
					binary.BigEndian.PutUint32(marker[:], 0x80000000|((2<<20)+1))
				}
				stream.feed(marker[:], func([]byte) { t.Fatal("invalid frame accepted") }, trace.fail)
			case "short-write":
				base := &latencyBufferConn{short: true}
				conn := &latencyTransportConn{Conn: base, owner: trace, id: 1}
				_, _ = conn.Write(latencyTestFrame(latencyTestReply(1, 0)))
			case "partial-eof":
				base := &latencyBufferConn{input: bytes.NewReader([]byte{0x80, 0})}
				conn := &latencyTransportConn{Conn: base, owner: trace, id: 1}
				_, _ = io.ReadAll(conn)
			}
			if trace.Status().Error == "" || trace.rows[0].WriteNS != 0 {
				t.Fatal("invalid trace became completed evidence")
			}
		})
	}
}

func TestLatencyTransportHandlerPreservesBodyAndSeparatesConnection(t *testing.T) {
	trace, _ := NewLatencyTransport(4)
	trace.request(1, latencyTestRequest(7), 1)
	trace.request(2, latencyTestRequest(7), 2)
	body := bytes.NewReader([]byte{0, 0, 0, 4, 1, 2, 3, 4})
	w := &response{conn: &conn{Conn: &latencyTransportConn{owner: trace, id: 2}}, req: &request{xid: 7, Body: body}}
	called := false
	err := trace.observeHandler(func(_ context.Context, response *response, _ Handler) error {
		called = true
		if body.Len() != 8 {
			t.Fatal("trace consumed request body")
		}
		_, _ = io.Copy(io.Discard, response.req.Body)
		return io.EOF
	})(context.Background(), w, nil)
	if err != io.EOF || !called || trace.rows[0].HandlerNS != 0 || trace.rows[1].Handle != "01020304" || trace.rows[1].HandledNS < trace.rows[1].HandlerNS {
		t.Fatal("handler trace changed semantics or connection identity")
	}
	var output bytes.Buffer
	if err := trace.WriteJSON(&output); err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		LatencyTransportStatus
		Records []LatencyTransportRecord `json:"records"`
	}
	if err := json.Unmarshal(output.Bytes(), &snapshot); err != nil || len(snapshot.Records) != 2 || snapshot.Pending != 2 {
		t.Fatal("snapshot discarded pending operations")
	}
}

type latencyNagleListener struct {
	net.Listener
	disabled bool
}

func (l *latencyNagleListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		err = c.(*net.TCPConn).SetNoDelay(false)
	}
	if err != nil {
		return c, err
	}
	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	var value int
	var optionErr error
	err = raw.Control(func(fd uintptr) { value, optionErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NODELAY) })
	if err != nil || optionErr != nil || value != 0 {
		_ = c.Close()
		return nil, fmt.Errorf("fixture did not disable TCP_NODELAY: %d %v %v", value, err, optionErr)
	}
	l.disabled = true
	return c, err
}

func TestLatencyTransportPreservesTCPNoDelay(t *testing.T) {
	trace, _ := NewLatencyTransport(1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
	client, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	nagle := &latencyNagleListener{Listener: listener}
	accepted, err := trace.WrapListener(nagle).Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	conn := accepted.(*latencyTransportConn)
	raw, err := conn.Conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var value int
	var optionErr error
	if err := raw.Control(func(fd uintptr) { value, optionErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NODELAY) }); err != nil || optionErr != nil || value == 0 || !nagle.disabled {
		t.Fatalf("TCP_NODELAY lost: %d %v %v", value, err, optionErr)
	}
}
