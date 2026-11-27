// enginelife measures the engine helper's life cycle (engine-direct step 1,
// checks E1–E6) in the remote lab's client container. It drives the app's
// own supervisor (internal/enginehost) against the bundled juicefs
// `jm-engine`, with the design's default timings.
//
//	enginelife life  [flags]   E1 start, E2 kill -9, E3 quit and app killed,
//	                           E4 ping, E6 SIGSTOP
//	enginelife auth  [flags]   E5: wrong token, other protocol major, wrong
//	                           juicefs version, same user other process,
//	                           other user
//	enginelife hold  [flags]   E6 link: a ready helper for -seconds while the
//	                           lab cuts the link; counts restarts
//
// Flags: -bin juicefs, -meta META-URL without the password, -pass-file (the
// Redis password, read from a file so it never reaches argv), -n trials,
// -pings, -volume (a FUSE mount that must stay mounted), -label.
// Each result is one JSON line on stdout.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/lelanddutcher/juicemount/internal/enginehost"
)

var (
	bin      = flag.String("bin", "/usr/local/bin/juicefs", "the juicefs with jm-engine")
	metaURL  = flag.String("meta", "", "META-URL without the password")
	passFile = flag.String("pass-file", "", "file holding the Redis password")
	trials   = flag.Int("n", 5, "trials per check")
	pings    = flag.Int("pings", 10000, "pings for E4")
	volume   = flag.String("volume", "/jfs", "FUSE mount that must stay mounted (empty = no check)")
	label    = flag.String("label", "", "label in every line")
	seconds  = flag.Int("seconds", 90, "hold: how long to hold")
	expect   = flag.String("expect", enginehost.ExpectedJuiceFSVersion, "juicefs version the supervisor expects")
	helperLg = flag.String("helper-log", "/var/jm/enginelife-helper.log", "the helper's log (appended)")
)

func emit(check string, v map[string]any) {
	v["check"] = check
	v["label"] = *label
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "enginelife: "+f+"\n", a...)
	os.Exit(2)
}

func password() string {
	if *passFile == "" {
		return ""
	}
	b, err := os.ReadFile(*passFile)
	if err != nil {
		die("pass-file: %v", err)
	}
	return strings.TrimSpace(string(b))
}

func baseEnv() []string {
	return []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"AWS_SHARED_CREDENTIALS_FILE=/dev/null", "AWS_CONFIG_FILE=/dev/null", "AWS_EC2_METADATA_DISABLED=true"}
}

// recorder keeps the supervisor's events with their wall time.
type recorder struct {
	mu     sync.Mutex
	events []stamped
	notify chan struct{}
}

type stamped struct {
	at time.Time
	e  enginehost.Event
}

func newRecorder() *recorder { return &recorder{notify: make(chan struct{}, 64)} }

func (r *recorder) add(e enginehost.Event) {
	r.mu.Lock()
	r.events = append(r.events, stamped{time.Now(), e})
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	if e.Kind != enginehost.EventBackoff && e.Kind != enginehost.EventLaunch {
		fmt.Fprintf(os.Stderr, "event %s pid=%d ms=%d err=%v\n", e.Kind, e.PID, e.Elapsed.Milliseconds(), e.Err)
	}
}

func (r *recorder) count(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, s := range r.events {
		if s.e.Kind == kind {
			n++
		}
	}
	return n
}

// waitKind waits for the n-th event of kind and returns it.
func (r *recorder) waitKind(kind string, n int, within time.Duration) (stamped, bool) {
	deadline := time.Now().Add(within)
	for {
		r.mu.Lock()
		seen := 0
		for _, s := range r.events {
			if s.e.Kind == kind {
				seen++
				if seen == n {
					r.mu.Unlock()
					return s, true
				}
			}
		}
		r.mu.Unlock()
		if time.Now().After(deadline) {
			return stamped{}, false
		}
		select {
		case <-r.notify:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func config(rec *recorder) enginehost.Config {
	args := []string{"jm-engine", *metaURL, "--no-usage-report", "--no-agent", "--no-bgjob", "--max-deletes", "0",
		"--read-only", "--cache-size", "0"}
	cfg := enginehost.Config{
		Bin: *bin, Args: args, Env: baseEnv(), MetaPassword: password(),
		LogPath: *helperLg, ExpectVersion: *expect,
	}
	if rec != nil {
		cfg.OnEvent = rec.add
	}
	return cfg
}

func gone(pid int, within time.Duration) (time.Duration, bool) {
	start := time.Now()
	for syscall.Kill(pid, 0) == nil {
		if time.Since(start) > within {
			return time.Since(start), false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return time.Since(start), true
}

func mounted() bool {
	if *volume == "" {
		return true
	}
	b, err := os.ReadFile("/proc/mounts")
	if err != nil || !strings.Contains(string(b), " "+*volume+" ") {
		return false
	}
	_, err = os.ReadDir(*volume)
	return err == nil
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func life() {
	// E1 and E3 (quit, idle): start to ready, then a clean stop.
	for i := 0; i < *trials; i++ {
		rec := newRecorder()
		s := enginehost.New(config(rec))
		t0 := time.Now()
		s.Start()
		st, err := s.WaitReady(context.Background())
		wall := time.Since(t0)
		if err != nil {
			emit("E1", map[string]any{"trial": i, "error": err.Error()})
			s.Stop()
			continue
		}
		emit("E1", map[string]any{"trial": i, "start_ms": st.LastStart.Milliseconds(), "wall_ms": wall.Milliseconds()})
		how, took := s.Stop()
		g, ok := gone(st.PID, 5*time.Second)
		emit("E3-quit", map[string]any{"trial": i, "how": how, "stop_ms": took.Milliseconds(),
			"helper_gone": ok, "gone_after_stop_ms": g.Milliseconds()})
	}

	// E2: kill -9 the ready helper; time to ready again; the mount stays.
	rec := newRecorder()
	s := enginehost.New(config(rec))
	s.Start()
	if _, err := s.WaitReady(context.Background()); err != nil {
		die("E2 start: %v", err)
	}
	for i := 0; i < *trials; i++ {
		// Wait out the back-off reset so each trial starts from the first step.
		if i > 0 {
			time.Sleep(61 * time.Second)
		}
		pid := s.Status().PID
		readyBefore := rec.count(enginehost.EventReady)
		t0 := time.Now()
		_ = syscall.Kill(pid, syscall.SIGKILL)
		ev, ok := rec.waitKind(enginehost.EventReady, readyBefore+1, 60*time.Second)
		row := map[string]any{"trial": i, "ready_again": ok, "volume_mounted": mounted()}
		if ok {
			row["ready_ms"] = ev.at.Sub(t0).Milliseconds()
			row["start_ms"] = ev.e.Elapsed.Milliseconds()
		}
		emit("E2", row)
	}

	// E4: ping round trips on one ready helper; getattr of the root for
	// reference (it reaches Redis).
	cl := s.Client()
	if cl == nil {
		die("E4: no client")
	}
	lat := make([]time.Duration, 0, *pings)
	fails := 0
	for i := 0; i < *pings; i++ {
		t := time.Now()
		if _, err := cl.Ping(2 * time.Second); err != nil {
			fails++
			continue
		}
		lat = append(lat, time.Since(t))
	}
	emit("E4-ping", pct(lat, fails))
	lat = lat[:0]
	fails = 0
	for i := 0; i < 200; i++ {
		t := time.Now()
		if _, err := cl.GetAttr(1, 10*time.Second); err != nil {
			fails++
			continue
		}
		lat = append(lat, time.Since(t))
	}
	emit("E4-getattr-root", pct(lat, fails))

	// E6: SIGSTOP the helper; time until a new helper is ready.
	for i := 0; i < *trials; i++ {
		time.Sleep(61 * time.Second)
		pid := s.Status().PID
		readyBefore := rec.count(enginehost.EventReady)
		unBefore := rec.count(enginehost.EventUnresponsive)
		// Stop at a random point of the ping interval.
		var b [1]byte
		_, _ = rand.Read(b[:])
		time.Sleep(time.Duration(b[0]) * 2 * time.Second / 256)
		t0 := time.Now()
		_ = syscall.Kill(pid, syscall.SIGSTOP)
		ev, ok := rec.waitKind(enginehost.EventReady, readyBefore+1, 60*time.Second)
		row := map[string]any{"trial": i, "replaced": ok,
			"unresponsive_events": rec.count(enginehost.EventUnresponsive) - unBefore, "volume_mounted": mounted()}
		if ok {
			row["ready_ms"] = ev.at.Sub(t0).Milliseconds()
		}
		_, oldGone := gone(pid, 5*time.Second)
		row["old_helper_gone"] = oldGone
		emit("E6-stop", row)
	}
	how, took := s.Stop()
	emit("E3-quit-after-use", map[string]any{"how": how, "stop_ms": took.Milliseconds()})

	// E3, app killed: a separate app process starts a helper and is killed.
	for i := 0; i < *trials; i++ {
		cmd := exec.Command(os.Args[0], append([]string{"app"}, os.Args[2:]...)...)
		cmd.Stderr = os.Stderr
		out, err := cmd.StdoutPipe()
		if err != nil {
			die("app: %v", err)
		}
		if err := cmd.Start(); err != nil {
			die("app: %v", err)
		}
		line, err := bufio.NewReader(out).ReadString('\n')
		pid, perr := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || perr != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			emit("E3-killed", map[string]any{"trial": i, "error": fmt.Sprintf("%v %v", err, perr)})
			continue
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		g, ok := gone(pid, 40*time.Second)
		emit("E3-killed", map[string]any{"trial": i, "helper_gone": ok, "gone_ms": g.Milliseconds()})
	}
}

func pct(lat []time.Duration, fails int) map[string]any {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	at := func(p float64) float64 {
		if len(lat) == 0 {
			return -1
		}
		return ms(lat[min(len(lat)-1, int(p*float64(len(lat))))])
	}
	return map[string]any{"n": len(lat), "fails": fails, "p50_ms": at(0.50), "p99_ms": at(0.99), "max_ms": at(1)}
}

// app is the E3 "app killed" child: start a helper, print its PID, wait.
func app() {
	s := enginehost.New(config(nil))
	s.Start()
	st, err := s.WaitReady(context.Background())
	if err != nil {
		die("app: %v", err)
	}
	fmt.Println(st.PID)
	select {}
}

// launch starts a helper directly (not through the supervisor), so the
// refusal cases can use the token. It returns the socket path, the token
// and a stop function.
func launch() (string, []byte, func()) {
	dir, err := os.MkdirTemp("", "jme-auth-")
	if err != nil {
		die("%v", err)
	}
	sock := filepath.Join(dir, "s")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		die("%v", err)
	}
	l.SetUnlinkOnClose(false)
	_ = os.Chmod(sock, 0o600)
	lf, _ := l.File()
	l.Close()
	pr, pw, _ := os.Pipe()
	token := make([]byte, 32)
	_, _ = rand.Read(token)
	cfg := config(nil)
	cmd := exec.Command(cfg.Bin, cfg.Args...)
	cmd.Env = cfg.Env
	cmd.ExtraFiles = []*os.File{pr, lf}
	if err := cmd.Start(); err != nil {
		die("%v", err)
	}
	pr.Close()
	lf.Close()
	_ = wire.WriteStartMessage(pw, wire.StartMessage{Token: hex.EncodeToString(token), MetaPassword: cfg.MetaPassword})
	return sock, token, func() {
		pw.Close()
		_ = cmd.Wait()
		os.RemoveAll(dir)
	}
}

// hello dials and sends a hello; it returns the reply status (0 = accepted)
// or the error text when there was no reply.
func hello(sock string, token []byte, major int) (uint32, string) {
	c, err := net.DialTimeout("unix", sock, 30*time.Second)
	if err != nil {
		return 0, err.Error()
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))
	body, _ := json.Marshal(wire.HelloRequest{ProtoMajor: major, Token: hex.EncodeToString(token)})
	_ = wire.WriteRequest(c, wire.Request{Op: wire.OpHello, Body: body})
	r, err := wire.ReadReply(c)
	if err != nil {
		return 0, err.Error()
	}
	return r.Status, ""
}

func refused(status uint32, errText string) bool {
	return status == uint32(syscall.EACCES) || status == uint32(syscall.EPROTONOSUPPORT) ||
		strings.Contains(errText, "permission denied")
}

func auth() {
	sock, token, stop := launch()
	defer stop()
	// The right hello first: proves the helper accepts its app.
	if st, e := hello(sock, token, wire.ProtoMajor); st != 0 || e != "" {
		die("auth: the right hello failed: %d %s", st, e)
	}
	n := *trials * 20
	tally := func(name string, f func() (uint32, string)) {
		ref, last := 0, ""
		for i := 0; i < n; i++ {
			st, e := f()
			if refused(st, e) {
				ref++
			}
			last = fmt.Sprintf("status=%d err=%s", st, e)
		}
		emit("E5", map[string]any{"case": name, "attempts": n, "refused": ref, "last": last})
	}
	bad := make([]byte, 32)
	tally("wrong-token", func() (uint32, string) { _, _ = rand.Read(bad); return hello(sock, bad, wire.ProtoMajor) })
	tally("no-token", func() (uint32, string) { return hello(sock, nil, wire.ProtoMajor) })
	tally("protocol-major", func() (uint32, string) { return hello(sock, token, wire.ProtoMajor+1) })
	// Same user, another process (not the helper's parent), right token.
	tally("same-user-other-process", func() (uint32, string) { return dialChild(sock, token, nil) })
	// Another user (needs root to switch; the lab client runs as root).
	if os.Getuid() == 0 {
		tally("other-user", func() (uint32, string) {
			return dialChild(sock, token, &syscall.Credential{Uid: 65534, Gid: 65534})
		})
	} else {
		emit("E5", map[string]any{"case": "other-user", "skipped": "not root"})
	}
	// Wrong version: the supervisor expects another juicefs.
	ok := 0
	for i := 0; i < *trials; i++ {
		cfg := config(nil)
		cfg.ExpectVersion = "1.3.1+0000-00-00.000000000000-jm0"
		s := enginehost.New(cfg)
		s.Start()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		_, err := s.WaitReady(ctx)
		cancel()
		if errors.Is(err, enginehost.ErrRefused) && s.Status().PID == 0 {
			ok++
		}
		s.Stop()
	}
	emit("E5", map[string]any{"case": "wrong-version", "attempts": *trials, "refused": ok})
}

func dialChild(sock string, token []byte, cred *syscall.Credential) (uint32, string) {
	cmd := exec.Command(os.Args[0], "dial", sock)
	cmd.Env = append(baseEnv(), "ENGINELIFE_TOKEN="+hex.EncodeToString(token))
	if cred != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	}
	out, _ := cmd.Output()
	var r struct {
		Status uint32 `json:"status"`
		Err    string `json:"err"`
	}
	if json.Unmarshal(out, &r) != nil {
		return 0, "child: " + string(out)
	}
	return r.Status, r.Err
}

func dial(sock string) {
	t, _ := hex.DecodeString(os.Getenv("ENGINELIFE_TOKEN"))
	st, e := hello(sock, t, wire.ProtoMajor)
	b, _ := json.Marshal(map[string]any{"status": st, "err": e})
	fmt.Println(string(b))
}

// hold keeps one helper ready for -seconds and reports what the supervisor
// did. A getattr every 5 s shows whether the engine could reach the NAS.
func hold() {
	rec := newRecorder()
	s := enginehost.New(config(rec))
	s.Start()
	if _, err := s.WaitReady(context.Background()); err != nil {
		die("hold: %v", err)
	}
	fmt.Fprintln(os.Stderr, "hold: ready")
	pid := s.Status().PID
	readyBefore := rec.count(enginehost.EventReady)
	end := time.Now().Add(time.Duration(*seconds) * time.Second)
	getattrFail, getattrOK := 0, 0
	maxMetaAge := int64(0)
	for time.Now().Before(end) {
		if cl := s.Client(); cl != nil {
			if p, err := cl.Ping(2 * time.Second); err == nil && p.MetaOKAgeMs > maxMetaAge {
				maxMetaAge = p.MetaOKAgeMs
			}
			if _, err := cl.GetAttr(1, 4*time.Second); err != nil {
				getattrFail++
			} else {
				getattrOK++
			}
		}
		time.Sleep(5 * time.Second)
	}
	emit("E6-link", map[string]any{
		"seconds": *seconds, "restarts": rec.count(enginehost.EventReady) - readyBefore,
		"ping_misses": rec.count(enginehost.EventPingMiss), "exits": rec.count(enginehost.EventExit),
		"same_pid": s.Status().PID == pid, "getattr_failed": getattrFail, "getattr_ok": getattrOK,
		"max_meta_ok_age_ms": maxMetaAge,
	})
	s.Stop()
}

func main() {
	if len(os.Args) < 2 {
		die("usage: enginelife life|auth|hold [flags]")
	}
	cmd := os.Args[1]
	if cmd == "dial" {
		dial(os.Args[2])
		return
	}
	if err := flag.CommandLine.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}
	if *metaURL == "" {
		die("-meta is required")
	}
	switch cmd {
	case "life":
		life()
	case "auth":
		auth()
	case "hold":
		hold()
	case "app":
		app()
	default:
		die("unknown command %s", cmd)
	}
}
