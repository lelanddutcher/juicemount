//go:build darwin && cgo

package client

/*
#cgo CFLAGS: -fblocks -mmacosx-version-min=26.0
#cgo LDFLAGS: -mmacosx-version-min=26.0
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

uintptr_t jm_uc_open(const char *service, const char *requirement, uint64_t conn_id, int *err);
void jm_uc_send(uintptr_t conn, int64_t version, const void *body, size_t len, uint64_t token);
void jm_uc_close(uintptr_t conn);
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
	"unsafe"
)

// Reply/event kinds shared with xpc_darwin.c.
const (
	kindReply        = 0
	kindInterrupted  = 1
	kindInvalid      = 2
	kindPeerRejected = 3
	kindOtherError   = 4
)

type xpcReply struct {
	kind int
	v    int64
	body []byte
}

var (
	ucMu      sync.Mutex
	ucToken   uint64
	ucPending = map[uint64]chan xpcReply{}
	ucConnID  uint64
	ucConns   = map[uint64]*xpcConn{}
)

// xpcConn is one libxpc mach-service connection. The C side holds the
// connection; Go holds its handle and owns exactly one reference, released
// once by Close.
type xpcConn struct {
	id     uint64
	events chan Event

	mu     sync.Mutex
	h      C.uintptr_t
	closed bool
}

func dialXPC(service, requirement string) (Conn, error) {
	ucMu.Lock()
	ucConnID++
	c := &xpcConn{id: ucConnID, events: make(chan Event, 8)}
	ucConns[c.id] = c
	ucMu.Unlock()

	cs, cr := C.CString(service), C.CString(requirement)
	defer C.free(unsafe.Pointer(cs))
	defer C.free(unsafe.Pointer(cr))
	var cerr C.int
	c.mu.Lock()
	c.h = C.jm_uc_open(cs, cr, C.uint64_t(c.id), &cerr)
	h := c.h
	c.mu.Unlock()
	if h == 0 {
		ucMu.Lock()
		delete(ucConns, c.id)
		ucMu.Unlock()
		return nil, fmt.Errorf("%w: libxpc refused the daemon requirement (%d)", ErrUnavailable, int(cerr))
	}
	return c, nil
}

func (c *xpcConn) Events() <-chan Event { return c.events }

func (c *xpcConn) RoundTrip(ctx context.Context, version int64, body []byte) (int64, []byte, error) {
	ch := make(chan xpcReply, 1)
	ucMu.Lock()
	ucToken++
	tok := ucToken
	ucPending[tok] = ch
	ucMu.Unlock()
	defer func() {
		ucMu.Lock()
		delete(ucPending, tok)
		ucMu.Unlock()
	}()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, nil, ErrClosed
	}
	var p unsafe.Pointer
	if len(body) > 0 {
		p = C.CBytes(body)
	}
	// xpc_dictionary_set_data copies the bytes, so the buffer is freed here.
	C.jm_uc_send(c.h, C.int64_t(version), p, C.size_t(len(body)), C.uint64_t(tok))
	c.mu.Unlock()
	if p != nil {
		C.free(p)
	}

	select {
	case r := <-ch:
		switch r.kind {
		case kindReply:
			return r.v, r.body, nil
		case kindInterrupted:
			return 0, nil, ErrInterrupted
		case kindPeerRejected:
			return 0, nil, ErrPeerRejected
		default:
			return 0, nil, ErrUnavailable
		}
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (c *xpcConn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	C.jm_uc_close(c.h)
}

//export jmUnmountdClientReply
func jmUnmountdClientReply(token C.uint64_t, kind C.int, v C.int64_t, body unsafe.Pointer, n C.size_t) {
	ucMu.Lock()
	ch := ucPending[uint64(token)]
	ucMu.Unlock()
	if ch == nil {
		return // the caller gave up (timeout); drop the late reply
	}
	r := xpcReply{kind: int(kind), v: int64(v)}
	if body != nil && n > 0 {
		// Copy at most one byte past the limit so the decoder rejects an
		// oversized body without the client ever buffering it all.
		m := int(n)
		if m > MaxBody+1 {
			m = MaxBody + 1
		}
		r.body = C.GoBytes(body, C.int(m))
	}
	select {
	case ch <- r:
	default:
	}
}

//export jmUnmountdClientEvent
func jmUnmountdClientEvent(connID C.uint64_t, kind C.int) {
	ucMu.Lock()
	c := ucConns[uint64(connID)]
	if kind == kindInvalid {
		delete(ucConns, uint64(connID))
	}
	ucMu.Unlock()
	if c == nil {
		return
	}
	var ev Event
	switch int(kind) {
	case kindInterrupted:
		ev = EventInterrupted
	case kindPeerRejected:
		ev = EventPeerRejected
	default:
		ev = EventInvalid
	}
	select {
	case c.events <- ev:
	default:
	}
}
