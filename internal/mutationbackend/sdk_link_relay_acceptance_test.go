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

// linkExecRelay holds the first EXEC containing the fixture's public HSET.
// It forwards real Redis replies unchanged; it never simulates WATCH behavior.
func linkExecRelay(t *testing.T, socket, upstream string) (<-chan struct{}, chan<- struct{}) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0700); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	acceptDone := make(chan struct{})
	var fired atomic.Bool
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(acceptDone)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[client] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer client.Close()
				defer func() { mu.Lock(); delete(connections, client); mu.Unlock() }()
				server, err := net.DialTimeout("unix", upstream, time.Second)
				if err != nil {
					return
				}
				defer server.Close()
				// A hard deadline bounds malformed fixtures and cleanup failures.
				_ = client.SetDeadline(time.Now().Add(20 * time.Second))
				_ = server.SetDeadline(time.Now().Add(20 * time.Second))
				copied := make(chan struct{})
				go func() {
					_, _ = io.Copy(client, server)
					_ = client.Close()
					close(copied)
				}()
				defer func() { _ = server.Close(); _ = client.Close(); <-copied }()
				reader := bufio.NewReader(client)
				publicSet := false
				for {
					frame, args, err := readLinkCommand(reader)
					if err != nil {
						return
					}
					switch strings.ToLower(args[0]) {
					case "multi":
						publicSet = false
					case "hset":
						if len(args) >= 4 && strings.HasPrefix(args[1], "d") && args[2] == "published.mov" {
							publicSet = true
						}
					case "exec":
						if publicSet && fired.CompareAndSwap(false, true) {
							close(held)
							select {
							case <-release:
							case <-time.After(5 * time.Second):
								return
							}
						}
						publicSet = false
					}
					if _, err := server.Write(frame); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone // no accepted connection can enter the registry after this
		mu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("owned Redis relay did not join")
		}
	})
	return held, release
}

func readLinkCommand(reader *bufio.Reader) ([]byte, []string, error) {
	var frame bytes.Buffer
	line := func(prefix byte) (int, error) {
		b, err := reader.ReadString('\n')
		if err != nil {
			return 0, err
		}
		frame.WriteString(b)
		if len(b) < 4 || b[0] != prefix || !strings.HasSuffix(b, "\r\n") {
			return 0, fmt.Errorf("invalid owned Redis command framing")
		}
		n, err := strconv.Atoi(b[1 : len(b)-2])
		if err != nil || n < 0 || n > 1<<20 {
			return 0, fmt.Errorf("invalid owned Redis command length")
		}
		return n, nil
	}
	n, err := line('*')
	if err != nil || n == 0 || n > 1024 {
		return nil, nil, fmt.Errorf("invalid owned Redis command array")
	}
	args := make([]string, n)
	for i := range args {
		size, err := line('$')
		if err != nil {
			return nil, nil, err
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, nil, err
		}
		if !bytes.HasSuffix(body, []byte("\r\n")) {
			return nil, nil, fmt.Errorf("invalid owned Redis bulk terminator")
		}
		frame.Write(body)
		args[i] = string(body[:size])
	}
	return frame.Bytes(), args, nil
}
