package nfs

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForTailscaleDialPlanNeverAcceptsSystemFallback(t *testing.T) {
	want := netip.MustParseAddrPort("192.0.2.10:6379")
	var calls atomic.Int32
	planner := func(context.Context, string, string) (netip.AddrPort, bool, error) {
		return want, calls.Add(1) >= 3, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := waitForTailscaleDialPlan(ctx, "tcp", want.String(), planner)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || calls.Load() != 3 {
		t.Fatalf("plan = %s after %d calls, want %s after 3", got, calls.Load(), want)
	}
}

func TestWaitForTailscaleDialPlanTimesOutInsteadOfDialingLAN(t *testing.T) {
	want := netip.MustParseAddrPort("192.0.2.10:9000")
	planner := func(context.Context, string, string) (netip.AddrPort, bool, error) {
		return want, false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := waitForTailscaleDialPlan(ctx, "tcp", want.String(), planner); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}

func TestWaitForTailscaleDialPlanPropagatesPlannerError(t *testing.T) {
	wantErr := errors.New("resolver failed")
	planner := func(context.Context, string, string) (netip.AddrPort, bool, error) {
		return netip.AddrPort{}, false, wantErr
	}
	if _, err := waitForTailscaleDialPlan(context.Background(), "tcp", "nas.invalid:6379", planner); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

type staticAddr string

func (a staticAddr) Network() string { return "tcp" }
func (a staticAddr) String() string  { return string(a) }

type staticLocalConn struct {
	net.Conn
	local net.Addr
}

func (c staticLocalConn) LocalAddr() net.Addr { return c.local }

func TestLinkConnUsesTailnet(t *testing.T) {
	for _, tt := range []struct {
		name string
		addr string
		want bool
	}{
		{name: "tailnet-v4", addr: "100.64.0.11:49152", want: true},
		{name: "tailnet-v6", addr: "[fd7a:115c:a1e0::b]:49152", want: true},
		{name: "ordinary-lan", addr: "192.168.0.42:49152", want: false},
		{name: "loopback", addr: "127.0.0.1:49152", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := staticLocalConn{local: staticAddr(tt.addr)}
			if got := linkConnUsesTailnet(conn); got != tt.want {
				t.Fatalf("linkConnUsesTailnet(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}
