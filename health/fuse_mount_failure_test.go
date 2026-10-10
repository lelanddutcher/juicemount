package health

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestShouldClassifyKextApprovalRequiresReadyBackends(t *testing.T) {
	cases := []struct {
		name        string
		kextLoaded  bool
		redisReady  bool
		objectReady bool
		want        bool
	}{
		{"all evidence present", false, true, true, true},
		{"kext already loaded", true, true, true, false},
		{"redis unavailable", false, false, true, false},
		{"object store unavailable", false, true, false, false},
		{"both backends unavailable", false, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldClassifyKextApproval(tc.kextLoaded, tc.redisReady, tc.objectReady)
			if got != tc.want {
				t.Fatalf("shouldClassifyKextApproval(%v, %v, %v) = %v, want %v",
					tc.kextLoaded, tc.redisReady, tc.objectReady, got, tc.want)
			}
		})
	}
}

func TestRedisMountBackendReadyRequiresProtocolPing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			command, err := readRESPCommand(reader)
			if err != nil {
				return
			}
			switch command {
			case "hello":
				_, _ = conn.Write([]byte("-ERR unknown command 'hello'\r\n"))
			case "ping":
				_, _ = conn.Write([]byte("+PONG\r\n"))
				return
			default:
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}()

	if !redisMountBackendReady(listener.Addr().String()) {
		t.Fatal("Redis protocol PING was not accepted as ready")
	}
	if redisMountBackendReady("redis://127.0.0.1:1/0") {
		t.Fatal("a non-listening Redis endpoint was accepted as ready")
	}
}

func readRESPCommand(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || count < 1 {
		return "", io.ErrUnexpectedEOF
	}
	var command string
	for i := 0; i < count; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
		if err != nil || length < 0 {
			return "", io.ErrUnexpectedEOF
		}
		value := make([]byte, length+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		if i == 0 {
			command = strings.ToLower(string(value[:length]))
		}
	}
	return command, nil
}

func TestObjectMountBackendReadyUsesMinIOHealthEndpoint(t *testing.T) {
	paths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	if !objectMountBackendReady(server.URL + "/zpool") {
		t.Fatal("MinIO live endpoint was not accepted as ready")
	}
	gotPath := <-paths
	if gotPath != "/minio/health/live" {
		t.Fatalf("object-store probe path = %q", gotPath)
	}
	if objectMountBackendReady("://malformed") {
		t.Fatal("malformed object-store endpoint was accepted as ready")
	}
}
