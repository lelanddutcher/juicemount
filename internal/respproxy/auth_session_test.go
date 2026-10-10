package respproxy

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func authSessionConn(t *testing.T, a *Authenticator, p *Proxy) *conn {
	t.Helper()
	client, peer := net.Pipe()
	c := &conn{p: p, client: client, dead: make(chan struct{})}
	t.Cleanup(func() {
		a.unbind(c)
		c.shutdown()
		peer.Close()
	})
	return c
}

func authSessionBind(t *testing.T, a *Authenticator, c *conn, name string) {
	t.Helper()
	ticket, result := a.authenticateTicket("127.0.0.1:1234", name, passOf(name))
	if result != AuthOK || !a.bind(c, ticket) {
		t.Fatalf("bind %s: result=%v", name, result)
	}
}

func copySessionPrincipals(ps *Principals) *Principals {
	next := &Principals{byName: make(map[string]principalRec, len(ps.byName))}
	for name, rec := range ps.byName {
		next.byName[name] = rec
	}
	return next
}

func TestAuthSessionRecordContinuity(t *testing.T) {
	base := principalsForTest(t)
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *Principals)
		close  bool
	}{
		{"identical", func(*testing.T, *Principals) {}, false},
		{"removed-or-disabled", func(_ *testing.T, ps *Principals) { delete(ps.byName, "mac1") }, true},
		{"role", func(_ *testing.T, ps *Principals) {
			r := ps.byName["mac1"]
			r.p.Kind = KindFarm
			ps.byName["mac1"] = r
		}, true},
		{"host", func(_ *testing.T, ps *Principals) {
			r := ps.byName["mac1"]
			r.p.Host = "rotated-host"
			ps.byName["mac1"] = r
		}, true},
		{"credential", func(t *testing.T, ps *Principals) {
			h, err := HashPassword("new-secret", 1, 64, 1)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := parseArgon2id(h)
			if err != nil {
				t.Fatal(err)
			}
			r := ps.byName["mac1"]
			r.hash = parsed
			ps.byName["mac1"] = r
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAuthenticator(base)
			target := authSessionConn(t, a, &Proxy{})
			witness := authSessionConn(t, a, &Proxy{}) // two Proxies sharing one Auth
			authSessionBind(t, a, target, "mac1")
			authSessionBind(t, a, witness, "mac2")
			next := copySessionPrincipals(base)
			tc.change(t, next)
			a.Swap(next)
			if target.revoked.Load() != tc.close {
				t.Fatalf("target revoked=%v, want %v", target.revoked.Load(), tc.close)
			}
			if witness.revoked.Load() {
				t.Fatal("unaffected principal's socket was revoked")
			}
			if tc.close {
				select {
				case <-target.dead:
				default:
					t.Fatal("changed principal's blocked transport remains open")
				}
				ticket, result := a.authenticateTicket("127.0.0.1:1234", "mac2", passOf("mac2"))
				if result != AuthOK || a.bind(target, ticket) {
					t.Fatal("revoked socket re-AUTHed instead of requiring a fresh connection")
				}
			}
		})
	}
}

func TestAuthSessionSwapTableRace(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, restoreOriginal := range []bool{false, true} {
			name := "uncached"
			if cached {
				name = "verified-cache"
			}
			if restoreOriginal {
				name += "-same-pointer-ABA"
			}
			t.Run(name, func(t *testing.T) {
				base := principalsForTest(t)
				a := NewAuthenticator(base)
				if cached {
					if _, result := a.Authenticate("127.0.0.1:2345", "mac1", passOf("mac1")); result != AuthOK {
						t.Fatal("cache priming AUTH failed")
					}
				}
				entered, release := make(chan struct{}), make(chan struct{})
				a.beforeCommit = func() { close(entered); <-release }
				result := make(chan AuthResult, 1)
				go func() {
					_, got := a.Authenticate("127.0.0.1:2345", "mac1", passOf("mac1"))
					result <- got
				}()
				<-entered
				next := copySessionPrincipals(base)
				delete(next.byName, "mac1")
				a.Swap(next)
				if restoreOriginal {
					a.Swap(base)
				}
				close(release)
				if got := <-result; got != AuthBad {
					t.Fatalf("AUTH against old table committed after Swap: %v", got)
				}
				a.mu.Lock()
				cachedCount := len(a.verified)
				a.mu.Unlock()
				if cachedCount != 0 {
					t.Fatalf("old verified cache repopulated after Swap: %d", cachedCount)
				}
			})
		}
	}
}

func TestAuthSessionBindShutdownAndReauthOrder(t *testing.T) {
	base := principalsForTest(t)
	for _, tc := range []struct {
		name          string
		reauthBefore  bool
		shutdownFirst bool
	}{
		{"reauth-wins-before-swap", true, false},
		{"reauth-loses-after-swap", false, false},
		{"shutdown-refuses-bind", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAuthenticator(base)
			c := authSessionConn(t, a, &Proxy{})
			if !tc.shutdownFirst {
				authSessionBind(t, a, c, "mac1")
			}
			successor, result := a.authenticateTicket("127.0.0.1:1234", "mac2", passOf("mac2"))
			if result != AuthOK {
				t.Fatal("successor ticket failed")
			}
			if tc.reauthBefore && !a.bind(c, successor) {
				t.Fatal("valid successor re-AUTH was rejected before Swap")
			}
			if tc.shutdownFirst {
				c.shutdown()
			} else {
				next := copySessionPrincipals(base)
				delete(next.byName, "mac1")
				a.Swap(next)
			}
			if tc.reauthBefore {
				if c.revoked.Load() || c.principal.Name != "mac2" {
					t.Fatal("successor binding was incorrectly revoked")
				}
			} else if a.bind(c, successor) {
				t.Fatal("dead or revoked connection re-registered")
			}
		})
	}
}

func TestAuthSessionLateUpstreamDialCannotPublish(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	entered := make(chan error, 1)
	release := make(chan struct{})
	go func() {
		up, err := ln.Accept()
		if err != nil {
			entered <- err
			return
		}
		defer up.Close()
		_, err = ReadCommand(bufio.NewReader(up)) // hold the upstream AUTH reply
		entered <- err
		<-release
		_, _ = up.Write([]byte("+OK\r\n"))
	}()
	p := &Proxy{cfg: Config{Upstream: ln.Addr().String(), UpstreamUser: "probe", UpstreamPassword: "synthetic"}, conns: map[net.Conn]struct{}{}}
	a := NewAuthenticator(principalsForTest(t))
	c := authSessionConn(t, a, p)
	authSessionBind(t, a, c, "mac1")
	done := make(chan error, 1)
	go func() { done <- c.startUpstream() }()
	select {
	case err := <-entered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream AUTH did not enter held server")
	}
	next := copySessionPrincipals(a.table.Load())
	delete(next.byName, "mac1")
	a.Swap(next)
	close(release)
	select {
	case err := <-done:
		if err != errClose {
			t.Fatalf("late upstream dial returned %v, want closed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late upstream dial did not finish")
	}
	if c.up != nil || c.replyEnd != nil {
		t.Fatal("revoked connection published a late upstream or reply loop")
	}
	p.mu.Lock()
	tracked := len(p.conns)
	p.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("late upstream remained tracked: %d", tracked)
	}
}
