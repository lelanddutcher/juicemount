//go:build latency_acceptance

package nfs

import (
	"context"
	"encoding/binary"
)

// ObserveLatencyReplies instruments encoded NFS3_OK replies in a dedicated
// acceptance process. Install before starting any server and restore only after
// all servers quiesce; the global handler registry is otherwise immutable.
// The callback must be nonblocking. This hook is absent from ordinary builds.
// These are encoded replies; the caller must also validate client completion.
func ObserveLatencyReplies(success func(proc uint32)) (restore func()) {
	originals := make(map[registeredHandlerID]HandleFunc)
	for _, proc := range []uint32{1, 3, 6, 16, 17} {
		id := registeredHandlerID{protocol: 100003, proc: proc}
		if handler := registeredHandlers[id]; handler != nil {
			originals[id] = handler
			registeredHandlers[id] = observeLatencyReply(proc, handler, success)
		}
	}
	return func() {
		for id, handler := range originals {
			registeredHandlers[id] = handler
		}
	}
}

func observeLatencyReply(proc uint32, handler HandleFunc, success func(uint32)) HandleFunc {
	return func(ctx context.Context, w *response, userHandler Handler) error {
		err := handler(ctx, w, userHandler)
		if err == nil && w.err == nil && w.responded && latencyReplyOK(w.writer.Bytes()) &&
			binary.BigEndian.Uint32(w.writer.Bytes()) == w.req.xid {
			success(proc)
		}
		return err
	}
}

func latencyReplyOK(reply []byte) bool {
	// Current response.writeHeader emits six 32-bit words before NFS status:
	// xid, REPLY, MSG_ACCEPTED, AUTH_NULL, zero verifier length, RPC_SUCCESS.
	// Fail closed if that encoding changes; do not treat an NFS error as a
	// successful operation merely because its transport completed normally.
	if len(reply) < 28 {
		return false
	}
	want := [...]uint32{1, 0, 0, 0, 0, uint32(NFSStatusOk)}
	for index, value := range want {
		if binary.BigEndian.Uint32(reply[(index+1)*4:]) != value {
			return false
		}
	}
	return true
}
