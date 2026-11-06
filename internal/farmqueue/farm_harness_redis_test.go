//go:build linux

package farmqueue

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// harnessRedis is a private redis-server the harness owns, so it can crash
// and restart it. Every write is in the append-only file before Redis replies
// (appendfsync always), which is the durability the farm queue assumes of the
// metadata Redis: a restart may lose a reply, never an acknowledged write. It
// listens only on a unix socket in a private directory, so it needs no port.
type harnessRedis struct {
	bin, dir, sock string
	cmd            *exec.Cmd
	exited         chan struct{}
}

// findHarnessRedisServer returns a Redis 7.4 server binary for the harness,
// or "" with the reason none is usable. It prefers JM_FARM_TEST_REDIS_SERVER,
// then the binary of the Redis that JM_FARM_TEST_REDIS_URL names (the CI
// stages start theirs from a fixed path), then redis-server on PATH.
func findHarnessRedisServer(ctx context.Context, provided *redis.Client) (bin, reason string, err error) {
	if explicit := os.Getenv("JM_FARM_TEST_REDIS_SERVER"); explicit != "" {
		if err := checkHarnessRedisVersion(explicit); err != nil {
			return "", "", fmt.Errorf("JM_FARM_TEST_REDIS_SERVER: %w", err)
		}
		return explicit, "", nil
	}
	var candidates []string
	if info, infoErr := provided.Info(ctx, "server").Result(); infoErr == nil {
		for _, line := range strings.Split(info, "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "executable:"); ok && value != "" {
				candidates = append(candidates, value)
			}
		}
	}
	if onPath, pathErr := exec.LookPath("redis-server"); pathErr == nil {
		candidates = append(candidates, onPath)
	}
	var reasons []string
	for _, candidate := range candidates {
		if err := checkHarnessRedisVersion(candidate); err != nil {
			reasons = append(reasons, err.Error())
			continue
		}
		return candidate, "", nil
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no redis-server binary found")
	}
	return "", strings.Join(reasons, "; "), nil
}

func checkHarnessRedisVersion(bin string) error {
	info, err := os.Stat(bin)
	if err != nil {
		return err
	}
	if info.IsDir() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("%s is not an executable file", bin)
	}
	out, err := exec.Command(bin, "--version").Output()
	if err != nil {
		return fmt.Errorf("%s --version: %w", bin, err)
	}
	if !strings.Contains(string(out), " v=7.4.") {
		return fmt.Errorf("%s is not Redis 7.4: %s", bin, strings.TrimSpace(string(out)))
	}
	return nil
}

func startHarnessRedis(bin string) (*harnessRedis, error) {
	// A unix socket path is limited to about 100 bytes, so use a short
	// private directory rather than the test's temporary directory.
	dir, err := os.MkdirTemp("", "jmfh")
	if err != nil {
		return nil, err
	}
	r := &harnessRedis{bin: bin, dir: dir, sock: filepath.Join(dir, "redis.sock")}
	if err := r.start(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return r, nil
}

func (r *harnessRedis) start() error {
	cmd := exec.Command(r.bin, "--port", "0", "--unixsocket", r.sock, "--unixsocketperm", "700",
		"--dir", r.dir, "--appendonly", "yes", "--appendfsync", "always", "--save", "",
		"--logfile", filepath.Join(r.dir, "redis.log"))
	if err := cmd.Start(); err != nil {
		return err
	}
	r.cmd = cmd
	r.exited = make(chan struct{})
	exited := r.exited
	go func() { _ = cmd.Wait(); close(exited) }()
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, err := net.DialTimeout("unix", r.sock, 200*time.Millisecond)
		if err == nil {
			_, _ = conn.Write([]byte("PING\r\n"))
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			buf := make([]byte, 64)
			n, _ := conn.Read(buf)
			_ = conn.Close()
			// A server still replaying its append-only file answers -LOADING.
			if strings.HasPrefix(string(buf[:n]), "+PONG") {
				return nil
			}
		}
		select {
		case <-exited:
			log, _ := os.ReadFile(filepath.Join(r.dir, "redis.log"))
			return fmt.Errorf("redis-server exited during start: %s", log)
		default:
		}
		if time.Now().After(deadline) {
			_ = r.crash()
			return fmt.Errorf("redis-server did not answer PING on %s", r.sock)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// crash SIGKILLs the server: no shutdown hook runs, so only what reached the
// append-only file survives.
func (r *harnessRedis) crash() error {
	if r.cmd == nil || r.cmd.Process == nil {
		return nil
	}
	_ = r.cmd.Process.Signal(syscall.SIGKILL)
	select {
	case <-r.exited:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("redis-server did not exit after SIGKILL")
	}
}

func (r *harnessRedis) dial() (net.Conn, error) {
	return net.DialTimeout("unix", r.sock, 500*time.Millisecond)
}

func (r *harnessRedis) options() *redis.Options {
	return &redis.Options{Network: "unix", Addr: r.sock}
}

func (r *harnessRedis) close() {
	_ = r.crash()
	_ = os.RemoveAll(r.dir)
}
