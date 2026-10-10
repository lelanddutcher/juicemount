package respproxy

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func singletonWait(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(time.Millisecond)
	}
}

// Close may already be running asynchronously. Its closed flag is admission
// state, not completion; the side pool closes only after release I/O finishes.
func singletonCloseJoined(t *testing.T, p *Proxy) {
	t.Helper()
	p.Close()
	singletonWait(t, "proxy Close did not finish singleton release", func() bool {
		p.side.mu.Lock()
		defer p.side.mu.Unlock()
		return p.side.closed
	})
}

// Exercise the production SidePool/RESP path with controlled replies and
// latency. Each owned pipe and server goroutine is retired by the fixture.
func singletonScripted(t *testing.T, ttl, renew time.Duration, reply func([][]byte) string) *Proxy {
	t.Helper()
	p, err := New(Config{Upstream: "unused", Singleton: true, SingletonTTL: ttl,
		SingletonRenew: renew, ModePoll: time.Hour, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var servers []net.Conn
	var wg sync.WaitGroup
	p.side.dial = func() (net.Conn, *bufio.Reader, error) {
		client, server := net.Pipe()
		mu.Lock()
		servers = append(servers, server)
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer server.Close()
			r := bufio.NewReader(server)
			for {
				args, err := ReadCommand(r)
				if err != nil {
					return
				}
				var frame string
				switch string(args[0]) {
				case "CLIENT", "SELECT":
					frame = "+OK\r\n"
				case "HGET", "GET":
					frame = "$-1\r\n"
				default:
					frame = reply(args)
				}
				if frame == "" {
					return // a deliberately lost transport reply
				}
				if _, err := server.Write([]byte(frame)); err != nil {
					return
				}
			}
		}()
		return client, bufio.NewReader(client), nil
	}
	t.Cleanup(func() {
		singletonCloseJoined(t, p)
		mu.Lock()
		for _, s := range servers {
			s.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return p
}

func TestSingletonAcquisitionRequiresTimelyOK(t *testing.T) {
	for _, tc := range []struct {
		name, frame string
		delay       time.Duration
	}{
		{"bulk_ok", "$2\r\nOK\r\n", 0},
		{"integer", ":1\r\n", 0},
		{"other_simple", "+QUEUED\r\n", 0},
		{"error", "-ERR refused\r\n", 0},
		{"occupied", "$-1\r\n", 0},
		{"late_ok", "+OK\r\n", 150 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := singletonScripted(t, 100*time.Millisecond, time.Hour, func(a [][]byte) string {
				if string(a[0]) == "SET" {
					time.Sleep(tc.delay)
					return tc.frame
				}
				return ":0\r\n"
			})
			if err := p.Start(); err == nil {
				t.Fatal("invalid or late singleton acquisition was accepted")
			}
			if !p.fenced() {
				t.Fatal("failed acquisition admitted writes")
			}
		})
	}
}

func TestSingletonRenewalCannotBridgeExpiredProof(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "expired_before_request"
		interval := 400 * time.Millisecond
		if late {
			name, interval = "expired_during_request", 10*time.Millisecond
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			p := singletonScripted(t, 300*time.Millisecond, interval, func(a [][]byte) string {
				if string(a[0]) == "SET" {
					return "+OK\r\n"
				}
				if string(a[1]) == renewLua {
					if calls.Add(1) == 1 && late {
						time.Sleep(300 * time.Millisecond)
					}
					return ":1\r\n"
				}
				return ":1\r\n"
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			singletonWait(t, "valid renewal resurrected expired ownership proof", p.closed.Load)
			if !p.fenced() {
				t.Fatal("expired proof no longer fenced")
			}
			want := int32(0)
			if late {
				want = 1
			}
			if calls.Load() != want {
				t.Fatalf("renewal calls = %d, want %d", calls.Load(), want)
			}
		})
	}
}

func TestSingletonRenewalRequiresIntegerOne(t *testing.T) {
	for _, frame := range []string{"$1\r\n1\r\n", "+OK\r\n", ":2\r\n", ":0\r\n", ":-1\r\n"} {
		t.Run(strings.TrimSpace(frame), func(t *testing.T) {
			var sets atomic.Int32
			p := singletonScripted(t, 5*time.Second, 10*time.Millisecond, func(a [][]byte) string {
				if string(a[0]) == "SET" {
					sets.Add(1)
					return "+OK\r\n"
				}
				if string(a[1]) == renewLua {
					return frame
				}
				return ":0\r\n"
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			singletonWait(t, "invalid renewal kept proxy serving", p.closed.Load)
			if !p.fenced() || sets.Load() != 1 {
				t.Fatalf("ownership reacquired: fenced=%v SETs=%d", p.fenced(), sets.Load())
			}
		})
	}
}

func TestSingletonTransientFailureKeepsHealthyOwnership(t *testing.T) {
	for _, fault := range []string{"", "-ERR transient\r\n"} {
		t.Run(map[bool]string{true: "transport", false: "redis_error"}[fault == ""], func(t *testing.T) {
			var renewals atomic.Int32
			p := singletonScripted(t, 5*time.Second, 10*time.Millisecond, func(a [][]byte) string {
				if string(a[0]) == "SET" {
					return "+OK\r\n"
				}
				if string(a[1]) == renewLua && renewals.Add(1) == 1 {
					return fault
				}
				return ":1\r\n"
			})
			if err := p.Start(); err != nil {
				t.Fatal(err)
			}
			singletonWait(t, "timely renewal did not recover from one transient failure", func() bool { return renewals.Load() >= 3 })
			if p.fenced() || p.closed.Load() {
				t.Fatal("healthy ownership fenced by transient failure")
			}
		})
	}
}

func TestSingletonOwnershipLossRetiresOldInstance(t *testing.T) {
	addr := redisAddr(t, true)
	for _, foreign := range []bool{false, true} {
		name := "missing"
		if foreign {
			name = "foreign"
		}
		t.Run(name, func(t *testing.T) {
			db := direct(t, addr)
			ctx := context.Background()
			tp := startProxy(t, addr, ModePassthrough, func(c *Config) {
				c.Singleton, c.SingletonTTL, c.SingletonRenew = true, 5*time.Second, 10*time.Millisecond
			})
			client := tp.raw(t)
			client.login("mac1")
			if v := client.do("SET", "i3", "original"); v.Err() != nil {
				t.Fatal(v.Err())
			}
			old := tp.Incarnation()
			if foreign {
				if err := db.Set(ctx, SingletonKey, "foreign-owner", 0).Err(); err != nil {
					t.Fatal(err)
				}
			} else if err := db.Del(ctx, SingletonKey).Err(); err != nil {
				t.Fatal(err)
			}
			singletonWait(t, "proxy kept serving after ownership loss", tp.closed.Load)
			singletonCloseJoined(t, tp.Proxy)
			sessionRevocationNoSuccess(t, client, "SET", "i3", "overwritten")
			if got := db.Get(ctx, "i3").Val(); got != "original" {
				t.Fatalf("backing data changed: %q", got)
			}
			if foreign {
				if got := db.Get(ctx, SingletonKey).Val(); got != "foreign-owner" {
					t.Fatalf("foreign singleton changed: %q", got)
				}
			} else {
				if db.Exists(ctx, SingletonKey).Val() != 0 {
					t.Fatal("old proxy reacquired missing singleton")
				}
				fresh := startProxy(t, addr, ModePassthrough, func(c *Config) { c.Singleton = true })
				if fresh.Incarnation() == old {
					t.Fatal("fresh proxy reused old identity")
				}
				if err := fresh.client(t, "mac1").Set(ctx, "i4", "fresh", 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type singletonLocalSpy struct {
	AllowProjectWrites
	calls atomic.Int32
}

func (h *singletonLocalSpy) HandleLocal(_ Principal, _ int, _ [][]byte) []byte {
	h.calls.Add(1)
	return []byte("+HANDLED\r\n")
}

func TestSingletonFencesLocalLeaseCommands(t *testing.T) {
	addr := redisAddr(t, true)
	for _, mode := range []Mode{ModeEnforce, ModeObserve, ModePassthrough} {
		t.Run(string(mode), func(t *testing.T) {
			direct(t, addr)
			spy := &singletonLocalSpy{}
			tp := startProxy(t, addr, mode, func(c *Config) {
				c.Singleton, c.SingletonTTL, c.SingletonRenew = true, 500*time.Millisecond, time.Hour
				c.Policy = spy
			})
			mac, admin := tp.raw(t), tp.raw(t)
			mac.login("mac1")
			admin.login("admin1")
			if got := mac.do("JM.LEASE", "STATUS"); string(got.Str) != "HANDLED" {
				t.Fatalf("healthy local dispatch: %+v", got)
			}
			singletonWait(t, "ownership did not expire", tp.fenced)
			for _, a := range [][]string{{"STATUS"}, {"HEARTBEAT"}, {"ACQUIRE", "ino:3", "auto"}, {"RELEASE", "ino:3", "1"}, {"BREAK", "ino:3"}} {
				for _, client := range []*rawConn{mac, admin} {
					v := client.do(append([]string{"JM.LEASE"}, a...)...)
					if v.Type != '-' || !strings.Contains(string(v.Str), "PROXY_FENCED") {
						t.Fatalf("expired local command reached handler: %v => %+v", a, v)
					}
				}
			}
			if spy.calls.Load() != 1 {
				t.Fatalf("handler invoked %d times, want healthy call only", spy.calls.Load())
			}
			if v := mac.do("PING"); string(v.Str) != "PONG" {
				t.Fatalf("read path during fence: %+v", v)
			}
		})
	}
}

func TestSingletonStartRequiresFreshInstance(t *testing.T) {
	p := singletonScripted(t, 5*time.Second, time.Hour, func(a [][]byte) string {
		if string(a[0]) == "SET" {
			return "+OK\r\n"
		}
		return ":1\r\n"
	})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	identity := p.Incarnation()
	if err := p.Start(); err == nil || p.Incarnation() != identity {
		t.Fatal("second Start changed running identity")
	}
	p.Close()
	if err := p.Start(); err == nil {
		t.Fatal("closed instance restarted")
	}
}
