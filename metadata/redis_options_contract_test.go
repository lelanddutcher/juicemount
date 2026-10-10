package metadata

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// authRedisFixture is a private loopback RESP2 peer. It verifies the wire
// behavior of the production constructors without a Redis install or NAS.
type authRedisFixture struct {
	ln      net.Listener
	mu      sync.Mutex
	auths   int
	selects []string
	pings   int
}

func newAuthRedisFixture(t *testing.T) *authRedisFixture {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &authRedisFixture{ln: ln}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() { defer wg.Done(); f.serve(c) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })
	return f
}

func (f *authRedisFixture) URL() string {
	u := &url.URL{Scheme: "redis", Host: f.ln.Addr().String(), User: url.UserPassword("fixture-user", "synthetic-secret"), Path: "/2"}
	return u.String()
}

func (f *authRedisFixture) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)
	authed := false
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			return
		}
		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "HELLO":
			fmt.Fprint(w, "-ERR unknown command 'HELLO'\r\n")
		case "AUTH":
			if len(args) == 3 && args[1] == "fixture-user" && args[2] == "synthetic-secret" {
				authed = true
				f.mu.Lock()
				f.auths++
				f.mu.Unlock()
				fmt.Fprint(w, "+OK\r\n")
			} else {
				fmt.Fprint(w, "-WRONGPASS invalid credentials\r\n")
			}
		case "SELECT":
			if !authed {
				fmt.Fprint(w, "-NOAUTH Authentication required\r\n")
				break
			}
			f.mu.Lock()
			f.selects = append(f.selects, args[1])
			f.mu.Unlock()
			fmt.Fprint(w, "+OK\r\n")
		case "PING":
			if !authed {
				fmt.Fprint(w, "-NOAUTH Authentication required\r\n")
				break
			}
			f.mu.Lock()
			f.pings++
			f.mu.Unlock()
			fmt.Fprint(w, "+PONG\r\n")
		case "CLIENT":
			fmt.Fprint(w, "+OK\r\n")
		default:
			fmt.Fprint(w, "-ERR unsupported fixture command\r\n")
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array")
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("expected RESP bulk")
		}
		length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		b := make([]byte, length+2)
		if _, err = io.ReadFull(r, b); err != nil {
			return nil, err
		}
		args = append(args, string(b[:length]))
	}
	return args, nil
}

func TestSavedRedisURLActualConstructors(t *testing.T) {
	f := newAuthRedisFixture(t)
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	u := f.URL() + "?db=3"
	client, err := NewRedisClient(u, store)
	if err != nil {
		t.Fatalf("connected: %v", err)
	}
	defer client.Stop()
	deferred, err := NewRedisClientDeferred(u, store)
	if err != nil {
		t.Fatalf("deferred: %v", err)
	}
	defer deferred.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := deferred.RawDB().Ping(ctx).Err(); err != nil {
		t.Fatalf("deferred first use: %v", err)
	}
	if err := client.Reconnect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.auths < 3 || f.pings < 3 || len(f.selects) < 3 {
		t.Fatalf("wire auth=%d ping=%d select=%v, want each constructor", f.auths, f.pings, f.selects)
	}
	for _, db := range f.selects {
		if db != "3" {
			t.Fatalf("selected DB %q, want query override 3", db)
		}
	}
}

func TestSavedRedisURLParseContract(t *testing.T) {
	for _, tc := range []struct {
		raw, addr string
		db        int
	}{
		{"redis://[::1]:6380/2?db=3", "[::1]:6380", 3},
		{"rediss://[::1]/2", "[::1]:6379", 2},
		{"redis://host", "host:6379", 0},
		{"host:6380/2", "host:6380", 2},
	} {
		addr, db, err := ParseRedisURL(tc.raw)
		if err != nil || addr != tc.addr || db != tc.db {
			t.Errorf("parse %q: (%q,%d,%v), want (%q,%d)", tc.raw, addr, db, err, tc.addr, tc.db)
		}
	}
	for _, raw := range []string{"redis://host:0/1", "redis://host:70000/1", "redis://host:/1", "redis://host:6379/garbage", "redis://host/-1", "redis://host/1?db=-1", "redis://host/1?db=%zz", "redis://[not-ip]:6379/1", "http://host:6379/1"} {
		if _, _, err := ParseRedisURL(raw); err == nil {
			t.Errorf("accepted invalid URL %q", raw)
		}
	}
	if _, _, err := ParseRedisURL("redis://user:synthetic-secret@host:bad/1"); err == nil || strings.Contains(err.Error(), "synthetic-secret") {
		t.Errorf("malformed URL error was nil or exposed synthetic secret: %v", err)
	}
}
