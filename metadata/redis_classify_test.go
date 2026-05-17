package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

// TestClassifyConnErr verifies the network-vs-backend classification
// against the real error shapes observed in JuiceMount logs over
// 2026-05-13 to 2026-05-16. Each case is annotated with where it was
// seen.
func TestClassifyConnErr(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantKind connErrKind
	}{
		{
			name:     "nil",
			err:      nil,
			wantKind: errKindOther,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			wantKind: errKindOther,
		},
		{
			// Observed verbatim in juicemount.log overnight 2026-05-16.
			name: "no route to host (syscall errno)",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "connect",
					Err:     syscall.EHOSTUNREACH,
				},
			},
			wantKind: errKindNetworkPath,
		},
		{
			name: "network unreachable",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "connect",
					Err:     syscall.ENETUNREACH,
				},
			},
			wantKind: errKindNetworkPath,
		},
		{
			name: "dial timeout (network)",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "connect",
					Err:     syscall.ETIMEDOUT,
				},
			},
			wantKind: errKindNetworkPath,
		},
		{
			name: "read timeout (backend stuck after we got through)",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "read",
					Err:     syscall.ETIMEDOUT,
				},
			},
			wantKind: errKindBackend,
		},
		{
			name: "connection refused (backend down)",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "connect",
					Err:     syscall.ECONNREFUSED,
				},
			},
			wantKind: errKindBackend,
		},
		{
			name: "connection reset",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "read",
					Err:     syscall.ECONNRESET,
				},
			},
			wantKind: errKindBackend,
		},
		{
			name: "DNS not found",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &net.DNSError{
					Err:        "no such host",
					Name:       "backend.invalid",
					IsNotFound: true,
				},
			},
			wantKind: errKindNetworkPath,
		},
		{
			// String-fallback path: go-redis wraps some errors in
			// fmt.Errorf without preserving the *net.OpError chain.
			name:     "string fallback: no route to host",
			err:      errors.New("dial tcp 127.0.0.1:6379: connect: no route to host"),
			wantKind: errKindNetworkPath,
		},
		{
			name:     "string fallback: i/o timeout (ambiguous → network)",
			err:      errors.New("read tcp 127.0.0.1:6379: i/o timeout"),
			wantKind: errKindNetworkPath,
		},
		{
			name:     "string fallback: connection refused",
			err:      errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"),
			wantKind: errKindBackend,
		},
		{
			name:     "redis client closed (app state)",
			err:      errors.New("redis: client is closed"),
			wantKind: errKindOther,
		},
		{
			name:     "unclassified error",
			err:      errors.New("some random redis error"),
			wantKind: errKindOther,
		},
		// --- additions from code-reviewer feedback ---
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			wantKind: errKindOther,
		},
		{
			name:     "bare io.EOF (backend closed mid-command)",
			err:      io.EOF,
			wantKind: errKindBackend,
		},
		{
			name:     "connection pool exhausted",
			err:      errors.New("redis: connection pool exhausted"),
			wantKind: errKindOther,
		},
		{
			// The actual error shape produced by `fmt.Errorf("redis
			// reconnect ping: %w", err)` wrapping a real Mac dial
			// failure. Confirms errors.As unwraps through fmt.Errorf.
			name: "fmt.Errorf-wrapped no route to host",
			err: fmt.Errorf("redis reconnect ping: %w", &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "connect",
					Err:     syscall.EHOSTUNREACH,
				},
			}),
			wantKind: errKindNetworkPath,
		},
		{
			// Write timeout = connection was established, backend
			// stopped responding mid-write. Backend, not network.
			name: "write timeout (backend slow)",
			err: &net.OpError{
				Op:  "write",
				Net: "tcp",
				Err: &os.SyscallError{
					Syscall: "write",
					Err:     syscall.ETIMEDOUT,
				},
			},
			wantKind: errKindBackend,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, friendly := classifyConnErr(tc.err)
			if kind != tc.wantKind {
				t.Errorf("classifyConnErr() kind = %q, want %q (friendly=%q)", kind, tc.wantKind, friendly)
			}
			if friendly == "" {
				t.Errorf("classifyConnErr() returned empty friendly message for %v", tc.err)
			}
		})
	}
}
