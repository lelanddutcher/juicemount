//go:build latency_acceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

func TestLatencyReplyObserverRejectsErrors(t *testing.T) {
	wantErr := errors.New("handler failed")
	for _, tc := range []struct {
		name   string
		status NFSStatus
		err    error
		mutate func(*response)
		want   int
	}{
		{name: "success", want: 1},
		{name: "jukebox", status: NFSStatusJukebox},
		{name: "unsupported", status: NFSStatusNotSupp},
		{name: "handler-error-after-write", err: wantErr},
		{name: "retained-response-error", mutate: func(w *response) { w.err = wantErr }},
		{name: "rpc-failure", mutate: func(w *response) { binary.BigEndian.PutUint32(w.writer.Bytes()[20:], 3) }},
		{name: "wrong-xid", mutate: func(w *response) { binary.BigEndian.PutUint32(w.writer.Bytes(), w.req.xid+1) }},
		{name: "denied", mutate: func(w *response) { binary.BigEndian.PutUint32(w.writer.Bytes()[8:], 1) }},
		{name: "different-verifier", mutate: func(w *response) { binary.BigEndian.PutUint32(w.writer.Bytes()[16:], 4) }},
		{name: "unanswered", mutate: func(w *response) { w.responded = false }},
		{name: "truncated", mutate: func(w *response) { w.writer.Truncate(27) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &response{writer: new(bytes.Buffer), req: &request{}}
			count := 0
			handler := observeLatencyReply(17, func(_ context.Context, w *response, _ Handler) error {
				var status [4]byte
				binary.BigEndian.PutUint32(status[:], uint32(tc.status))
				if err := w.Write(status[:]); err != nil {
					t.Fatal(err)
				}
				if tc.mutate != nil {
					tc.mutate(w)
				}
				return tc.err
			}, func(proc uint32) {
				if proc != 17 {
					t.Errorf("proc=%d, want17", proc)
				}
				count++
			})
			if err := handler(context.Background(), w, nil); !errors.Is(err, tc.err) {
				t.Fatalf("error changed: %v", err)
			}
			if count != tc.want {
				t.Fatalf("count=%d, want%d", count, tc.want)
			}
		})
	}
}
