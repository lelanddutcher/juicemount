package health

import (
	"context"
	"net"
	"testing"
	"time"
)

// The link estimator gets one round trip (the PING exchange on the open
// connection), the adaptive dial timeout the whole probe. Counting the
// handshake too gave netprofile two round trips: a direct 12 ms tunnel read
// 25-31 ms and kept the high-latency flag on (real remote run, 2026-11-26).
func TestReachabilityRTTObserverGetsOneRoundTrip(t *testing.T) {
	const handshake, ping = 60 * time.Millisecond, 15 * time.Millisecond
	var observed time.Duration
	r := NewReachability("ignored:0",
		WithDialContext(func(ctx context.Context, _, _ string) (net.Conn, error) {
			time.Sleep(handshake)
			left, right := net.Pipe()
			go func() {
				defer right.Close()
				buf := make([]byte, 64)
				if _, err := right.Read(buf); err != nil {
					return
				}
				time.Sleep(ping)
				_, _ = right.Write([]byte("+PONG\r\n"))
			}()
			return left, nil
		}),
		WithDialTimeout(2*time.Second),
		WithRTTObserver(func(d time.Duration) { observed = d }))
	if !r.probe() {
		t.Fatal("probe failed against a live peer")
	}
	if observed < ping || observed >= handshake {
		t.Fatalf("observer got %v; want the PING round trip (>= %v, < the %v handshake)", observed, ping, handshake)
	}
	r.mu.Lock()
	srtt := r.srtt
	r.mu.Unlock()
	if srtt < handshake+ping {
		t.Fatalf("adaptive timeout RTT %v; want the whole probe (>= %v)", srtt, handshake+ping)
	}
}
