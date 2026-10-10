//go:build sdkacceptance

package mutationbackend

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// linkAckRelay consumes a complete real successful EXEC response before
// withholding it. Requests and all other replies are forwarded unchanged.
func linkAckRelay(t *testing.T, socket, upstream string, lose bool) (<-chan error, func(), <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0700); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	held, release, dropped := make(chan error, 1), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var fired atomic.Bool
	var mu sync.Mutex
	closing := false
	connections := map[net.Conn]bool{}
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(acceptDone)
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[client] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				captured := false
				defer func() {
					if captured {
						close(dropped)
					}
				}()
				defer func() { _ = client.Close(); mu.Lock(); delete(connections, client); mu.Unlock() }()
				server, err := net.DialTimeout("unix", upstream, time.Second)
				if err != nil {
					return
				}
				defer func() { _ = server.Close(); mu.Lock(); delete(connections, server); mu.Unlock() }()
				mu.Lock()
				if closing {
					mu.Unlock()
					return
				}
				connections[server] = true
				mu.Unlock()
				_ = client.SetDeadline(time.Now().Add(25 * time.Second))
				_ = server.SetDeadline(time.Now().Add(25 * time.Second))
				requests, replies := bufio.NewReader(client), bufio.NewReader(server)
				publicSet := false
				for {
					frame, args, err := readLinkCommand(requests)
					if err != nil {
						return
					}
					command := strings.ToLower(args[0])
					if command == "multi" {
						publicSet = false
					}
					if command == "hset" && len(args) >= 4 && strings.HasPrefix(args[1], "d") && args[2] == "published.mov" {
						publicSet = true
					}
					if _, err := server.Write(frame); err != nil {
						return
					}
					reply, redisError, err := readLinkReply(replies, 0)
					if command == "exec" && publicSet && lose && fired.CompareAndSwap(false, true) {
						captured = true
						if err != nil || redisError || len(reply) == 0 || reply[0] != '*' || bytes.HasPrefix(reply, []byte("*-1\r\n")) {
							held <- fmt.Errorf("target EXEC did not return a complete successful array")
							return
						}
						held <- nil
						<-release // only the reaped-writer barrier or cleanup releases it
						return    // discard the reply after the test reaps the writer
					}
					if err != nil {
						return
					}
					if command == "exec" || command == "discard" {
						publicSet = false
					}
					if _, err := client.Write(reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		<-acceptDone
		mu.Lock()
		closing = true
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		unblock()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("ack relay failed to join")
		}
	})
	return held, unblock, dropped
}

// The SDK negotiates RESP3 while its control reader uses RESP2. Retain exact
// frames from both, including nested EXEC errors, without logging credentials.
func readLinkReply(r *bufio.Reader, depth int) ([]byte, bool, error) {
	if depth > 16 {
		return nil, false, fmt.Errorf("reply nesting limit")
	}
	line, err := r.ReadSlice('\n') // bounded by the reader's fixed buffer
	if err != nil || len(line) < 3 || !bytes.HasSuffix(line, []byte("\r\n")) {
		return nil, false, fmt.Errorf("invalid reply line")
	}
	var out bytes.Buffer
	out.Write(line)
	kind := line[0]
	switch line[0] {
	case '+', ':', ',', '(':
		return out.Bytes(), false, nil
	case '_':
		if string(line) != "_\r\n" {
			return nil, false, fmt.Errorf("invalid null reply")
		}
		return out.Bytes(), false, nil
	case '#':
		if string(line) != "#t\r\n" && string(line) != "#f\r\n" {
			return nil, false, fmt.Errorf("invalid bool reply")
		}
		return out.Bytes(), false, nil
	case '-':
		return out.Bytes(), true, nil
	case '$', '!', '=', '*', '%', '~':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil || n < -1 || n > 1<<20 {
			return nil, false, fmt.Errorf("invalid reply size")
		}
		if n == -1 {
			if kind != '$' && kind != '*' {
				return nil, false, fmt.Errorf("invalid null aggregate")
			}
			return out.Bytes(), false, nil
		}
		if kind == '$' || kind == '!' || kind == '=' {
			body := make([]byte, n+2)
			if _, err := io.ReadFull(r, body); err != nil || !bytes.HasSuffix(body, []byte("\r\n")) {
				return nil, false, fmt.Errorf("invalid bulk reply")
			}
			out.Write(body)
			return out.Bytes(), kind == '!', nil
		}
		if n > 1024 {
			return nil, false, fmt.Errorf("reply array limit")
		}
		if kind == '%' {
			n *= 2
		}
		failed := false
		for i := 0; i < n; i++ {
			frame, childError, err := readLinkReply(r, depth+1)
			if err != nil {
				return nil, false, err
			}
			out.Write(frame)
			failed = failed || childError
			if out.Len() > 8<<20 {
				return nil, false, fmt.Errorf("reply byte limit")
			}
		}
		return out.Bytes(), failed, nil
	default:
		return nil, false, fmt.Errorf("unsupported reply type")
	}
}

func TestSDKLinkAckReplyFraming(t *testing.T) {
	for _, tc := range []struct {
		frame      string
		redisError bool
	}{
		{"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n", false},
		{"*3\r\n+OK\r\n:1\r\n:1\r\n", false},
		{"*2\r\n+OK\r\n-ERR failed\r\n", true},
		{"*2\r\n~2\r\n#t\r\n_\r\n%1\r\n+k\r\n$1\r\nv\r\n", false},
		{"!3\r\nERR\r\n", true},
	} {
		r := bufio.NewReader(strings.NewReader(tc.frame + "+NEXT\r\n"))
		frame, failed, err := readLinkReply(r, 0)
		if err != nil || string(frame) != tc.frame || failed != tc.redisError {
			t.Fatal("reply framing changed", err)
		}
		next, failed, err := readLinkReply(r, 0)
		if err != nil || failed || string(next) != "+NEXT\r\n" {
			t.Fatal("reply consumed successor", err)
		}
	}
	for _, frame := range []string{"%1\r\n+k\r\n", "$3\r\nx\r\n", "*1025\r\n", "%oops\r\n", "_-1\r\n"} {
		if _, _, err := readLinkReply(bufio.NewReader(strings.NewReader(frame)), 0); err == nil {
			t.Fatal("malformed reply accepted")
		}
	}
}
