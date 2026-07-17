package health

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// mustCIDR parses a CIDR into the *net.IPNet form net.Interface.Addrs
// yields (IP = the interface address, Mask = the subnet mask).
func mustCIDR(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("bad cidr %q: %v", cidr, err)
	}
	return &net.IPNet{IP: ip, Mask: ipnet.Mask}
}

func opErr(err error) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: err}
}

type fakeTimeoutErr struct{}

func (fakeTimeoutErr) Error() string   { return "i/o timeout" }
func (fakeTimeoutErr) Timeout() bool   { return true }
func (fakeTimeoutErr) Temporary() bool { return true }

// TestDialErrorClassHealth pins the shared taxonomy (moved here from
// bridge/diagnose.go, which now aliases it), including the new
// EACCES/EPERM "denied" class.
func TestDialErrorClassHealth(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, DialClassOK},
		{"ehostunreach", opErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH)), DialClassNoRoute},
		{"enetunreach", opErr(os.NewSyscallError("connect", syscall.ENETUNREACH)), DialClassNoRoute},
		{"no route by string", errors.New("dial tcp 10.0.1.5:6379: connect: no route to host"), DialClassNoRoute},
		{"econnrefused", opErr(os.NewSyscallError("connect", syscall.ECONNREFUSED)), DialClassRefused},
		{"eperm", opErr(os.NewSyscallError("connect", syscall.EPERM)), DialClassDenied},
		{"eacces", opErr(os.NewSyscallError("connect", syscall.EACCES)), DialClassDenied},
		{"denied by string", errors.New("dial tcp: operation not permitted"), DialClassDenied},
		{"dns", &net.DNSError{Err: "no such host", Name: "nas.local", IsNotFound: true}, DialClassDNS},
		{"timeout", opErr(fakeTimeoutErr{}), DialClassTimeout},
		{"context deadline", context.DeadlineExceeded, DialClassTimeout},
		{"other", errors.New("something exotic"), DialClassOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DialErrorClass(tc.err); got != tc.want {
				t.Fatalf("DialErrorClass(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestIsLocalNetPermissionSignature is the #106 classifier truth table:
// only an EHOSTUNREACH/EACCES-class failure, to a private on-link address,
// while the machine's network is up, classifies as the macOS Local Network
// permission signature. Everything ambiguous stays false (conservative).
func TestIsLocalNetPermissionSignature(t *testing.T) {
	ehostunreach := opErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH))
	eperm := opErr(os.NewSyscallError("connect", syscall.EPERM))
	refused := opErr(os.NewSyscallError("connect", syscall.ECONNREFUSED))
	timeout := opErr(fakeTimeoutErr{})

	lanUp := func(t *testing.T) LocalNetSnapshot {
		return LocalNetSnapshot{Up: true, Networks: []*net.IPNet{
			mustCIDR(t, "10.0.1.7/24"),
			mustCIDR(t, "192.168.4.20/22"),
		}}
	}
	offline := LocalNetSnapshot{} // no interfaces: Wi-Fi off / no cable

	cases := []struct {
		name string
		err  error
		addr string
		snap func(*testing.T) LocalNetSnapshot
		want bool
	}{
		{
			name: "EHOSTUNREACH to attached private subnet while network up → permission",
			err:  ehostunreach, addr: "10.0.1.5:6379",
			snap: lanUp, want: true,
		},
		{
			name: "EPERM (EACCES-class) to attached private subnet → permission",
			err:  eperm, addr: "10.0.1.5:6379",
			snap: lanUp, want: true,
		},
		{
			name: "wide-mask attachment matches too (/22)",
			err:  ehostunreach, addr: "192.168.5.9:9000",
			snap: lanUp, want: true,
		},
		{
			name: "IPv4 link-local direct-attach NAS matches",
			err:  ehostunreach, addr: "169.254.10.2:6379",
			snap: func(t *testing.T) LocalNetSnapshot {
				return LocalNetSnapshot{Up: true, Networks: []*net.IPNet{
					mustCIDR(t, "169.254.10.1/16"),
				}}
			},
			want: true,
		},
		{
			name: "no route while machine genuinely offline → ordinary offline, NOT permission",
			err:  ehostunreach, addr: "10.0.1.5:6379",
			snap: func(*testing.T) LocalNetSnapshot { return offline }, want: false,
		},
		{
			name: "ECONNREFUSED → backend down, not permission",
			err:  refused, addr: "10.0.1.5:6379",
			snap: lanUp, want: false,
		},
		{
			name: "timeout → not permission",
			err:  timeout, addr: "10.0.1.5:6379",
			snap: lanUp, want: false,
		},
		{
			name: "no error → not permission",
			err:  nil, addr: "10.0.1.5:6379",
			snap: lanUp, want: false,
		},
		{
			name: "public target → not a Local Network matter",
			err:  ehostunreach, addr: "8.8.8.8:443",
			snap: lanUp, want: false,
		},
		{
			name: "loopback target is exempt from the permission",
			err:  ehostunreach, addr: "127.0.0.1:11049",
			snap: lanUp, want: false,
		},
		{
			name: "hostname target cannot be verified → conservative false",
			err:  ehostunreach, addr: "nas.local:6379",
			snap: lanUp, want: false,
		},
		{
			name: "private but NOT on an attached subnet (coffee-shop case) → false",
			err:  ehostunreach, addr: "10.99.0.5:6379",
			snap: lanUp, want: false,
		},
		{
			name: "bare host without port still classifies",
			err:  ehostunreach, addr: "10.0.1.5",
			snap: lanUp, want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsLocalNetPermissionSignature(tc.err, tc.addr, tc.snap(t)); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestClassifyBackendFailure verifies the monitor-level upgrade path: a
// matching failure reports the distinct MsgLocalNetworkPermission status
// (component stays unhealthy), a non-matching failure passes through
// verbatim, and the suspected latch clears on the matching→generic edge.
func TestClassifyBackendFailure(t *testing.T) {
	prevSnap := localNetSnapshotFn
	t.Cleanup(func() { localNetSnapshotFn = prevSnap })

	lan := LocalNetSnapshot{Up: true, Networks: []*net.IPNet{mustCIDR(t, "10.0.1.7/24")}}
	localNetSnapshotFn = func() LocalNetSnapshot { return lan }

	m := &HealthMonitor{}
	base := ComponentStatus{Healthy: false, LastCheck: time.Now(), Message: "ping failed: no route"}
	noRoute := opErr(os.NewSyscallError("connect", syscall.EHOSTUNREACH))

	got := m.classifyBackendFailure("redis", "10.0.1.5:6379", noRoute, base)
	if got.Message != MsgLocalNetworkPermission {
		t.Fatalf("Message = %q, want %q", got.Message, MsgLocalNetworkPermission)
	}
	if got.Healthy {
		t.Fatal("component must stay unhealthy under the permission verdict")
	}
	if !m.localNetSuspected["redis"] {
		t.Fatal("suspected latch not set")
	}

	// A refused error must pass through untouched and clear the latch.
	refused := opErr(os.NewSyscallError("connect", syscall.ECONNREFUSED))
	got = m.classifyBackendFailure("redis", "10.0.1.5:6379", refused, base)
	if got.Message != base.Message {
		t.Fatalf("generic failure rewritten: %q", got.Message)
	}
	if m.localNetSuspected["redis"] {
		t.Fatal("suspected latch not cleared on non-matching failure")
	}

	// Genuinely offline: even a no-route failure stays generic.
	localNetSnapshotFn = func() LocalNetSnapshot { return LocalNetSnapshot{} }
	got = m.classifyBackendFailure("redis", "10.0.1.5:6379", noRoute, base)
	if got.Message != base.Message {
		t.Fatalf("offline machine misclassified as permission: %q", got.Message)
	}

	// Empty target (unconfigured minio) never classifies.
	localNetSnapshotFn = func() LocalNetSnapshot { return lan }
	got = m.classifyBackendFailure("minio", "", noRoute, base)
	if got.Message != base.Message {
		t.Fatalf("empty target misclassified: %q", got.Message)
	}
}

// TestMinioHostPort covers the URL → host:port extraction used to feed the
// classifier a judgeable target.
func TestMinioHostPort(t *testing.T) {
	cases := []struct{ in, want string }{
		{"http://10.0.1.5:9000", "10.0.1.5:9000"},
		{"https://minio.nas:9000", "minio.nas:9000"},
		{"http://10.0.1.5", "10.0.1.5"},
		{"", ""},
		{"::bad::url::", ""},
	}
	for _, tc := range cases {
		if got := minioHostPort(tc.in); got != tc.want {
			t.Errorf("minioHostPort(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
