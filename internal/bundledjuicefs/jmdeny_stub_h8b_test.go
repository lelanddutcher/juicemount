//go:build h8b_acceptance

package bundledjuicefs

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

// jmdenyReply is the H-8 option B proxy refusal (H8_OPTION_B_PLAN.md §1.5).
// Its first token is deliberately not one that juicefs's shouldRetry retries.
const jmdenyReply = "-JMDENY LEASE_HELD proj=4242 holder=edit-bay-2@edit2.local epoch=7\r\n"

// denyStub is a synchronous RESP relay in front of a disposable Redis. While
// armed it does what the real proxy does for a refused transaction: it sends
// DISCARD upstream (so nothing in the MULTI commits), swallows the +OK and
// puts JMDENY in the EXEC slot. It counts every refused EXEC, so a client
// that retried a refusal is visible as a count above one.
type denyStub struct {
	ln       net.Listener
	upstream string
	armed    atomic.Bool
	denyAll  atomic.Bool // every command gets AUTH_REQUIRED (unauthenticated client)
	// denySliceRefRead also refuses "HGET sliceRef ...", the read the Redis
	// engine's compaction double-check makes after a failed EXEC. A refused
	// read keeps that result EROFS instead of turning it into EINVAL.
	denySliceRefRead atomic.Bool
	refused  atomic.Int64
	denied   atomic.Int64
	execs    atomic.Int64
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
}

func startDenyStub(t *testing.T, upstream string) *denyStub {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &denyStub{ln: ln, upstream: upstream}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *denyStub) addr() string { return s.ln.Addr().String() }

func (s *denyStub) accept() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		u, err := net.Dial("tcp", s.upstream)
		if err != nil {
			_ = c.Close()
			continue
		}
		s.mu.Lock()
		s.conns = append(s.conns, c, u)
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer c.Close()
			defer u.Close()
			s.relay(c, u)
		}()
	}
}

func (s *denyStub) relay(c, u net.Conn) {
	cr, ur := bufio.NewReader(c), bufio.NewReader(u)
	for {
		raw, name, args, err := readCommand(cr)
		if err != nil {
			return
		}
		if name == "HGET" && len(args) > 1 && args[1] == "sliceRef" && s.denySliceRefRead.Load() {
			if _, err := c.Write([]byte(jmdenyReply)); err != nil {
				return
			}
			continue
		}
		if name == "EXEC" {
			s.execs.Add(1)
		}
		if s.denyAll.Load() {
			s.refused.Add(1)
			if _, err := c.Write([]byte("-JMDENY AUTH_REQUIRED proj=0 holder=-@- epoch=0\r\n")); err != nil {
				return
			}
			continue
		}
		if name == "EXEC" && s.armed.Load() {
			if _, err := u.Write([]byte("*1\r\n$7\r\nDISCARD\r\n")); err != nil {
				return
			}
			if _, err := readReply(ur); err != nil {
				return
			}
			s.denied.Add(1)
			if _, err := c.Write([]byte(jmdenyReply)); err != nil {
				return
			}
			continue
		}
		if _, err := u.Write(raw); err != nil {
			return
		}
		reply, err := readReply(ur)
		if err != nil {
			return
		}
		if _, err := c.Write(reply); err != nil {
			return
		}
	}
}

// readCommand reads one client command (always a RESP array of bulk strings
// from go-redis) and returns its raw bytes, upper-cased name and arguments.
func readCommand(r *bufio.Reader) ([]byte, string, []string, error) {
	var raw bytes.Buffer
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, "", nil, err
	}
	raw.Write(line)
	if len(line) < 3 || line[0] != '*' {
		return nil, "", nil, fmt.Errorf("unexpected client frame %q", line)
	}
	n, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
	if err != nil {
		return nil, "", nil, err
	}
	name := ""
	var args []string
	for i := 0; i < n; i++ {
		hdr, err := r.ReadBytes('\n')
		if err != nil {
			return nil, "", nil, err
		}
		raw.Write(hdr)
		size, err := strconv.Atoi(string(bytes.TrimSpace(hdr[1:])))
		if err != nil || hdr[0] != '$' {
			return nil, "", nil, fmt.Errorf("unexpected argument frame %q", hdr)
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, "", nil, err
		}
		raw.Write(body)
		args = append(args, string(body[:size]))
		if i == 0 {
			name = strings.ToUpper(string(body[:size]))
		}
	}
	return raw.Bytes(), name, args, nil
}

// readReply reads exactly one RESP2/RESP3 reply and returns its raw bytes.
func readReply(r *bufio.Reader) ([]byte, error) {
	var raw bytes.Buffer
	if err := readValue(r, &raw); err != nil {
		return nil, err
	}
	return raw.Bytes(), nil
}

func readValue(r *bufio.Reader, raw *bytes.Buffer) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}
	raw.Write(line)
	if len(line) < 3 {
		return errors.New("short RESP line")
	}
	body := string(bytes.TrimSpace(line[1:]))
	switch line[0] {
	case '+', '-', ':', ',', '#', '(', '_':
		return nil
	case '$', '=', '!':
		n, err := strconv.Atoi(body)
		if err != nil {
			return err
		}
		if n < 0 {
			return nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		raw.Write(buf)
		return nil
	case '*', '~', '>', '%', '|':
		n, err := strconv.Atoi(body)
		if err != nil {
			return err
		}
		if line[0] == '%' || line[0] == '|' {
			n *= 2
		}
		for i := 0; i < n; i++ {
			if err := readValue(r, raw); err != nil {
				return err
			}
		}
		if line[0] == '|' { // an attribute precedes the real reply
			return readValue(r, raw)
		}
		return nil
	}
	return fmt.Errorf("unknown RESP type %q", line[0])
}

// disposableRedis returns host:port and DB of the lane's disposable Redis and
// flushes that DB. It refuses to run without the explicit opt-in so it can
// never flush a Redis it was not handed.
func disposableRedis(t *testing.T) (string, int, *redis.Client) {
	t.Helper()
	raw := os.Getenv("JM_FARM_TEST_REDIS_URL")
	if raw == "" || os.Getenv("JM_FARM_TEST_REDIS_FLUSH") != "1" {
		t.Fatal("h8b_acceptance needs JM_FARM_TEST_REDIS_URL (disposable Redis) and JM_FARM_TEST_REDIS_FLUSH=1")
	}
	opt, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	host, _, _ := net.SplitHostPort(opt.Addr)
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Fatalf("refusing non-loopback Redis %s", host)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.FlushDB(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	return opt.Addr, opt.DB, rdb
}
