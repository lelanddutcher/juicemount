package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"

	billy "github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

type mountDecisionHandler struct {
	Handler
	status      MountStatus
	handleCalls int
	requested   string
}

func (h *mountDecisionHandler) Mount(_ context.Context, _ net.Conn, req MountRequest) (MountStatus, billy.Filesystem, []AuthFlavor) {
	h.requested = string(req.Dirpath)
	return h.status, nil, []AuthFlavor{AuthFlavorNull}
}

func (h *mountDecisionHandler) ToHandle(billy.Filesystem, []string) []byte {
	h.handleCalls++
	return []byte("root-ref")
}

// A rejected export may return no filesystem at all. The RPC dispatcher must
// return the denial without entering the handler's handle/namespace machinery.
func TestMountDenialDoesNotAcquireRootHandle(t *testing.T) {
	for _, status := range []MountStatus{MountStatusErrAcces, MountStatusErrNoEnt, MountStatusOk} {
		t.Run(statusName(status), func(t *testing.T) {
			body := &bytes.Buffer{}
			if err := xdr.Write(body, []byte("/generation")); err != nil {
				t.Fatal(err)
			}
			h := &mountDecisionHandler{status: status}
			w := &response{conn: &conn{}, writer: &bytes.Buffer{}, req: &request{Body: body}}
			if err := onMount(context.Background(), w, h); err != nil {
				t.Fatal(err)
			}
			if h.requested != "/generation" {
				t.Fatalf("mount requested %q", h.requested)
			}
			wantCalls := 0
			if status == MountStatusOk {
				wantCalls = 1
			}
			if h.handleCalls != wantCalls {
				t.Fatalf("status=%d entered root-handle machinery %d times, want %d", status, h.handleCalls, wantCalls)
			}
			// Accepted RPC header is six uint32 words; mount status follows it.
			out := w.writer.Bytes()
			if len(out) < 28 || binary.BigEndian.Uint32(out[24:28]) != uint32(status) {
				t.Fatalf("incorrect mount response: %x", out)
			}
			if status != MountStatusOk && len(out) != 28 {
				t.Fatalf("denied mount leaked a handle or auth payload: %x", out)
			}
		})
	}
}

func statusName(s MountStatus) string {
	switch s {
	case MountStatusOk:
		return "accepted"
	case MountStatusErrAcces:
		return "access-denied"
	default:
		return "unknown-export"
	}
}
