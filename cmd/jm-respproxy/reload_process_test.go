//go:build redisoptionsacceptance

package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

type reloadCLIConn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func reloadCLIDial(t *testing.T, addr string) *reloadCLIConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &reloadCLIConn{c, bufio.NewReader(c), bufio.NewWriter(c)}
}
func (c *reloadCLIConn) command(t *testing.T, args ...string) respproxy.Value {
	t.Helper()
	if err := c.c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	raw := make([][]byte, len(args))
	for i, s := range args {
		raw[i] = []byte(s)
	}
	if err := respproxy.WriteCommand(c.w, raw); err != nil {
		t.Fatal(err)
	}
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	v, err := respproxy.ReadValue(c.r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func (c *reloadCLIConn) expect(t *testing.T, want string, args ...string) {
	t.Helper()
	v := c.command(t, args...)
	if v.Type != '+' || string(v.Str) != want {
		t.Fatalf("%s response=%c %q", args[0], v.Type, v.Str)
	}
}
func reloadCLIWrite(t *testing.T, path string, b []byte) {
	t.Helper()
	tmp := path + ".next"
	if err := os.WriteFile(tmp, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}
func reloadCLIRequest(t *testing.T, dir string, body []byte, nonce int) string {
	t.Helper()
	line := fmt.Sprintf("%x %d %016x", sha256.Sum256(body), time.Now().UnixMilli(), nonce)
	reloadCLIWrite(t, filepath.Join(dir, "reload.request"), []byte(line+"\n"))
	return line
}
func reloadCLIAwait(t *testing.T, p *proxyCLIProcess, what string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-p.done:
			t.Fatalf("CLI exited while awaiting %s: %v", what, p.err)
		case <-deadline.C:
			t.Fatalf("timeout awaiting %s", what)
		case <-tick.C:
		}
	}
}
func reloadCLIApplied(t *testing.T, p *proxyCLIProcess, dir, line string) {
	t.Helper()
	reloadCLIAwait(t, p, "applied receipt", func() bool { b, _ := os.ReadFile(filepath.Join(dir, "reload.applied")); return string(b) == line+"\n" })
	info, err := os.Stat(filepath.Join(dir, "reload.applied"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("receipt permissions: %v %v", info, err)
	}
}

func TestProxyCLIAppliedReloadAndRevocation(t *testing.T) {
	f := newProxyCLIFixture(t)
	f.config.ReloadHandshake = true
	dir := filepath.Dir(f.config.Principals)
	raw, err := os.ReadFile(f.config.Principals)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Principals []respproxy.PrincipalEntry `json:"principals"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	peer := doc.Principals[0]
	peer.Name = "mac-witness"
	peer.Host = "witness"
	doc.Principals = append(doc.Principals, peer)
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	reloadCLIWrite(t, f.config.Principals, body)
	first := reloadCLIRequest(t, dir, body, 1)
	p := startProxyCLI(t, f.config, f.password)
	p.awaitReady(t)
	reloadCLIApplied(t, p, dir, first)
	target, witness := reloadCLIDial(t, p.addr), reloadCLIDial(t, p.addr)
	target.expect(t, "OK", "AUTH", "mac-cli", "cli-password")
	witness.expect(t, "OK", "AUTH", "mac-witness", "cli-password")
	target.expect(t, "PONG", "PING")
	witness.expect(t, "PONG", "PING")
	backing := proxyCLISnapshot(t, f)

	doc.Principals = []respproxy.PrincipalEntry{peer}
	valid, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	invalid := append(append([]byte(nil), valid...), []byte(" trailing")...)
	reloadCLIWrite(t, f.config.Principals, invalid)
	reloadCLIRequest(t, dir, invalid, 2)
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	reloadCLIAwait(t, p, "parse refusal", func() bool { return bytes.Contains(p.logBytes(t), []byte("trailing JSON or data")) })
	b, err := os.ReadFile(filepath.Join(dir, "reload.applied"))
	if err != nil || string(b) != first+"\n" {
		t.Fatal("refused parse advanced applied receipt")
	}
	target.expect(t, "PONG", "PING")
	witness.expect(t, "PONG", "PING")

	reloadCLIWrite(t, f.config.Principals, valid)
	if err := os.Chmod(f.config.Principals, 0644); err != nil {
		t.Fatal(err)
	}
	second := reloadCLIRequest(t, dir, valid, 3)
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	reloadCLIAwait(t, p, "permission refusal", func() bool { return bytes.Contains(p.logBytes(t), []byte("mode 0644")) })
	b, err = os.ReadFile(filepath.Join(dir, "reload.applied"))
	if err != nil || string(b) != first+"\n" {
		t.Fatal("refused permissions advanced receipt")
	}
	target.expect(t, "PONG", "PING")
	if err := os.Chmod(f.config.Principals, 0600); err != nil {
		t.Fatal(err)
	}
	// No new request or signal: ordinary polling must retry the same request.
	reloadCLIApplied(t, p, dir, second)
	if err := target.c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = respproxy.ReadValue(target.r)
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("applied receipt preceded target transport closure: %v", err)
	}
	witness.expect(t, "PONG", "PING")
	fresh := reloadCLIDial(t, p.addr)
	v := fresh.command(t, "AUTH", "mac-cli", "cli-password")
	if v.Type != '-' || !strings.Contains(string(v.Str), "WRONGPASS") {
		t.Fatalf("revoked fresh AUTH accepted: %c %q", v.Type, v.Str)
	}
	if !reflect.DeepEqual(backing, proxyCLISnapshot(t, f)) {
		t.Fatal("credential reload mutated backing data")
	}
	third := reloadCLIRequest(t, dir, valid, 4)
	reloadCLIApplied(t, p, dir, third)
	witness.expect(t, "PONG", "PING")
	p.stop(t)
	proxyCLINoSingleton(t, f)

	// Same private directory, new actual process: a stale applied receipt must
	// not combine with the new heartbeat when the current request mismatches.
	reloadCLIRequest(t, dir, invalid, 5)
	next := startProxyCLI(t, f.config, f.password)
	next.awaitReady(t)
	reloadCLIAwait(t, next, "restart mismatch", func() bool { return bytes.Contains(next.logBytes(t), []byte("does not match principals bytes")) })
	if _, err := os.Stat(filepath.Join(dir, "reload.applied")); !os.IsNotExist(err) {
		t.Fatalf("restart reused stale applied receipt: %v", err)
	}
	fourth := reloadCLIRequest(t, dir, valid, 6)
	reloadCLIApplied(t, next, dir, fourth)
	survivor := reloadCLIDial(t, next.addr)
	survivor.expect(t, "OK", "AUTH", "mac-witness", "cli-password")
	survivor.expect(t, "PONG", "PING")
	next.stop(t)
	proxyCLINoSingleton(t, f)
}
