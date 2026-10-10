package main

// Regression tests for the 2026-10-06 adversarial review of Mac enrollment:
// finding 1a (an approval before the proxy cutover must not switch the Mac's
// working login) and finding 3 (the enrollment client must not follow a
// redirect).

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
)

// The production Manager client never follows a redirect: a 307 replays
// the POST body, so the secret would reach the Location host.
func TestH8BEnrollHTTPClientRefusesRedirect(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer evil.Close()
	mgr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+"/api/principals/enroll", http.StatusTemporaryRedirect)
	}))
	defer mgr.Close()
	old := enrollHTTPClient
	enrollHTTPClient = newEnrollHTTPClient()
	t.Cleanup(func() { enrollHTTPClient = old })

	db := filepath.Join(t.TempDir(), "metadata.db")
	out := runEnrollStep(t, mgr.URL, db, enrollclient.State{})
	out = runEnrollStep(t, mgr.URL, db, out.State)
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("the redirect target received %d request(s): the enrollment secret was forwarded", hits)
	}
	if out.Phase != enrollclient.Failed || !strings.Contains(out.Message, "redirect") {
		t.Fatalf("redirect answer: phase %s %q, want Failed naming the redirect", out.Phase, out.Message)
	}
}

// fakeRESP is a minimal Redis: AUTH <user> <pass> succeeds only for the
// accepted pair; PING needs a successful AUTH. It records each AUTH user.
type fakeRESP struct {
	ln             net.Listener
	mu             sync.Mutex
	acceptUser     string
	acceptPassword string
	authUsers      []string
}

func newFakeRESP(t *testing.T, user, password string) *fakeRESP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRESP{ln: ln, acceptUser: user, acceptPassword: password}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeRESP) url() string { return "redis://legacyuser@" + f.ln.Addr().String() + "/1" }

func (f *fakeRESP) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	authed := false
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		switch strings.ToUpper(args[0]) {
		case "AUTH":
			user := "default"
			pass := args[len(args)-1]
			if len(args) == 3 {
				user = args[1]
			}
			f.mu.Lock()
			f.authUsers = append(f.authUsers, user)
			ok := user == f.acceptUser && pass == f.acceptPassword
			f.mu.Unlock()
			if ok {
				authed = true
				fmt.Fprint(c, "+OK\r\n")
			} else {
				fmt.Fprint(c, "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
			}
		case "SELECT", "CLIENT":
			fmt.Fprint(c, "+OK\r\n")
		case "PING":
			if !authed {
				fmt.Fprint(c, "-NOAUTH Authentication required.\r\n")
			} else {
				fmt.Fprint(c, "+PONG\r\n")
			}
		default:
			fmt.Fprint(c, "-ERR unknown command\r\n")
		}
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("bad request %q: %v", line, err)
	}
	n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if _, err := r.ReadString('\n'); err != nil { // $len
			return nil, err
		}
		v, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args = append(args, strings.TrimRight(v, "\r\n"))
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	return args, nil
}

func runEnrollStepWithRedis(t *testing.T, mgr, db, redisURL string, st enrollclient.State) enrollStepOutput {
	t.Helper()
	in, _ := json.Marshal(enrollStepInput{ManagerURL: mgr, DBPath: db, Host: "Edit-Bay-2", Label: "Edit Bay 2", State: st, RedisURL: redisURL})
	var out enrollStepOutput
	if err := json.Unmarshal([]byte(enrollStep(string(in))), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// An approval is reported live only when the configured Redis endpoint
// accepts AUTH <principal> <secret>. Before the proxy cutover the plain
// Redis refuses the principal, so the app must keep its working login.
func TestH8BEnrollStepApprovedChecksPrincipalLogin(t *testing.T) {
	stub := &stubManager{}
	srv := httptest.NewServer(stub)
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	db := filepath.Join(t.TempDir(), "metadata.db")

	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	out = runEnrollStep(t, srv.URL, db, out.State)
	if out.Phase != enrollclient.Waiting || out.CredentialsLive {
		t.Fatalf("pending: phase %s live=%v", out.Phase, out.CredentialsLive)
	}
	principal, secret := out.State.Principal, out.State.Secret
	stub.mu.Lock()
	stub.approved = true
	stub.mu.Unlock()

	// Pre-cutover Redis: accepts only the legacy login.
	pre := newFakeRESP(t, "legacyuser", "legacy-password")
	got := runEnrollStepWithRedis(t, srv.URL, db, pre.url(), out.State)
	if got.Phase != enrollclient.Approved {
		t.Fatalf("phase %s %s", got.Phase, got.Message)
	}
	if got.CredentialsLive {
		t.Fatal("approved step reported the login live although the Redis endpoint refused the principal")
	}
	if !strings.Contains(got.CredentialCheck, "does not accept this Mac's new login yet") {
		t.Fatalf("credential check = %q", got.CredentialCheck)
	}
	pre.mu.Lock()
	users := append([]string(nil), pre.authUsers...)
	pre.mu.Unlock()
	if len(users) == 0 || users[len(users)-1] != principal {
		t.Fatalf("login check did not AUTH as the principal: %v", users)
	}

	// Post-cutover: the endpoint accepts the principal with its secret.
	post := newFakeRESP(t, principal, secret)
	got = runEnrollStepWithRedis(t, srv.URL, db, post.url(), out.State)
	if !got.CredentialsLive || got.CredentialCheck != "" {
		t.Fatalf("live endpoint: live=%v check=%q", got.CredentialsLive, got.CredentialCheck)
	}

	// Unreachable endpoint and no endpoint: not live.
	dead := newFakeRESP(t, principal, secret)
	deadURL := dead.url()
	dead.ln.Close()
	got = runEnrollStepWithRedis(t, srv.URL, db, deadURL, out.State)
	if got.CredentialsLive || !strings.Contains(got.CredentialCheck, "could not be reached") {
		t.Fatalf("unreachable endpoint: live=%v check=%q", got.CredentialsLive, got.CredentialCheck)
	}
	got = runEnrollStepWithRedis(t, srv.URL, db, "", out.State)
	if got.CredentialsLive {
		t.Fatal("no Redis address, yet reported live")
	}
	for _, s := range []string{got.CredentialCheck, got.Message} {
		if strings.Contains(s, secret) {
			t.Fatal("a secret reached the credential check text")
		}
	}
}

// The same check against a real Redis (disposable server, JM_FARM_TEST_REDIS_URL)
// with an ACL user standing in for the proxy's principal.
func TestH8BPrincipalLoginCheckRealRedis(t *testing.T) {
	raw := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if raw == "" {
		t.Skip("JM_FARM_TEST_REDIS_URL not set")
	}
	opts, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	admin := redis.NewClient(opts)
	defer admin.Close()
	ctx := context.Background()
	const principal, secret = "mac-4f1c2a9e7b3d", "principal-secret-0123456789abcdef0123456789"
	if err := admin.Do(ctx, "ACL", "SETUSER", principal, "reset", "on", ">"+secret, "+ping", "+select", "+client", "allkeys").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", principal).Err() })
	endpoint := "redis://" + opts.Addr + "/" + strconv.Itoa(opts.DB)
	if ok, why := verifyPrincipalLogin(endpoint, principal, secret); !ok {
		t.Fatalf("real Redis refused the principal: %s", why)
	}
	if ok, why := verifyPrincipalLogin(endpoint, principal, secret+"x"); ok || !strings.Contains(why, "does not accept") {
		t.Fatalf("wrong secret: ok=%v %q", ok, why)
	}
	if ok, _ := verifyPrincipalLogin(endpoint, "mac-000000000000", secret); ok {
		t.Fatal("unknown principal accepted")
	}
}
