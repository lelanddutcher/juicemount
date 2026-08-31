package nfs

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type flushCountingConn struct {
	mu     sync.Mutex
	writes int
	data   bytes.Buffer
}

func (c *flushCountingConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *flushCountingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	return c.data.Write(p)
}

func (c *flushCountingConn) Close() error                     { return nil }
func (c *flushCountingConn) LocalAddr() net.Addr              { return serializerTestAddr("local") }
func (c *flushCountingConn) RemoteAddr() net.Addr             { return serializerTestAddr("remote") }
func (c *flushCountingConn) SetDeadline(time.Time) error      { return nil }
func (c *flushCountingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *flushCountingConn) SetWriteDeadline(time.Time) error { return nil }

type serializerTestAddr string

func (a serializerTestAddr) Network() string { return "test" }
func (a serializerTestAddr) String() string  { return string(a) }

func TestSerializeWritesFlushesContinuouslyRefilledQueueInBoundedBatches(t *testing.T) {
	const messages = maxSerializedResponseBatch*3 + 1
	fc := &flushCountingConn{}
	c := &conn{
		Conn:            fc,
		writeSerializer: make(chan []byte, messages),
	}
	for i := 0; i < messages; i++ {
		c.writeSerializer <- []byte{byte(i)}
	}
	close(c.writeSerializer)

	c.serializeWrites(context.Background())

	// All messages are far smaller than the 1 MiB bufio buffer, so each
	// underlying Write is an explicit Flush. With a 32-response cap, 97 queued
	// replies must be flushed as 32 + 32 + 32 + 1 rather than one unbounded
	// batch. This is the wire-progress guarantee navigation depends on.
	if got, want := fc.writes, 4; got != want {
		t.Fatalf("underlying flush writes=%d, want %d bounded batches", got, want)
	}
}
