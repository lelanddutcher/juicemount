package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enginehost"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	jmnfs "github.com/lelanddutcher/juicemount/nfs"
)

// Engine-direct step 2 in jm5 (JM_ENGINE=reads). With its own FUSE manager
// the door rides on the manager's mount child, as in the app. With
// --no-fuse the remote lab can hand jm5 the argv of a foreground
// `juicefs mount` (--engine-mount-argv FILE or JM_ENGINE_MOUNT_ARGV, a JSON
// array): jm5 then starts that mount as its own child with the socket
// inherited, exactly as the app does, and stops it on exit. A mount the lab
// already made at --fuse-path is unmounted first, so the existing lab
// scripts run unchanged with only jm5's environment set. With the level
// below reads the same argv starts the same mount without the socket (the
// lab's "off" arm on an identical mount).

func doorReadSource(d *enginehost.Door) jmnfs.EngineReadSource {
	return func() (jmnfs.EngineReadClient, uint64, bool) {
		cl, epoch, ok := d.Current()
		if !ok {
			return nil, 0, false
		}
		return cl, epoch, true
	}
}

// labMount is a juicefs mount child jm5 started for the lab.
type labMount struct {
	cmd    *exec.Cmd
	exited chan struct{}
}

// startLabMount starts argv (from a JSON file) with the door's socket on
// descriptors 3 and 4, and waits until mountPoint is a FUSE mount.
func startLabMount(argvFile, mountPoint, logPath string, door *enginehost.Door) (*labMount, error) {
	raw, err := os.ReadFile(argvFile)
	if err != nil {
		return nil, err
	}
	var argv []string
	if err := json.Unmarshal(raw, &argv); err != nil || len(argv) < 2 {
		return nil, fmt.Errorf("engine mount argv %s: want a JSON array of at least 2 words", argvFile)
	}
	if isFUSEMount(mountPoint) {
		if err := takeOverMount(mountPoint); err != nil {
			return nil, err
		}
	}
	var dargs []string
	var files []*os.File
	if door != nil {
		if dargs, files, err = door.Prepare(3); err != nil {
			return nil, err
		}
	}
	cmd := exec.Command(argv[0], append(argv[1:], dargs...)...)
	cmd.ExtraFiles = files
	cmd.Env = os.Environ()
	if logPath != "" {
		if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
			cmd.Stdout, cmd.Stderr = f, f
			defer f.Close()
		}
	}
	err = cmd.Start()
	if door != nil {
		door.Started(err)
	}
	if err != nil {
		return nil, err
	}
	m := &labMount{cmd: cmd, exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(m.exited) }()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if isFUSEMount(mountPoint) {
			jmlog.Info("lab juicefs mount ready with the engine socket", "pid", cmd.Process.Pid, "mount", mountPoint)
			return m, nil
		}
		select {
		case <-m.exited:
			return nil, errors.New("the lab juicefs mount exited during start")
		case <-time.After(200 * time.Millisecond):
		}
	}
	m.stop()
	return nil, fmt.Errorf("%s did not become a FUSE mount within 90 s", mountPoint)
}

// takeOverMount unmounts the lab's own mount at mountPoint so jm5 can mount
// there as the app does.
func takeOverMount(mountPoint string) error {
	if out, err := exec.Command("umount", mountPoint).CombinedOutput(); err != nil {
		if out2, err2 := exec.Command("umount", "-l", mountPoint).CombinedOutput(); err2 != nil {
			return fmt.Errorf("unmount the lab's mount at %s: %v %s / %v %s", mountPoint, err, out, err2, out2)
		}
	}
	for i := 0; i < 100 && isFUSEMount(mountPoint); i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if isFUSEMount(mountPoint) {
		return fmt.Errorf("%s is still mounted", mountPoint)
	}
	time.Sleep(time.Second) // the old juicefs flushes and exits
	jmlog.Info("lab: took over the juicefs mount", "mount", mountPoint)
	return nil
}

func isFUSEMount(mountPoint string) bool {
	f, err := os.Open("/proc/self/mounts")
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 3 && fields[1] == mountPoint && strings.HasPrefix(fields[2], "fuse") {
			return true
		}
	}
	return false
}

// stop sends SIGTERM (juicefs flushes and unmounts), then SIGKILL after 30 s.
func (m *labMount) stop() {
	if m == nil {
		return
	}
	_ = m.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-m.exited:
	case <-time.After(30 * time.Second):
		_ = m.cmd.Process.Kill()
		<-m.exited
	}
}

// logEngineReads writes the step-2 counters every interval until stop.
func logEngineReads(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			logEngineReadCounters("engine reads (final)")
			return
		case <-t.C:
			logEngineReadCounters("engine reads")
		}
	}
}

func logEngineReadCounters(msg string) {
	c := jmnfs.EngineReadCounters()
	kv := make([]any, 0, 2*len(c))
	for _, k := range []string{"opens", "open_fails", "fallbacks", "reopens", "reads", "read_bytes", "read_errors", "releases", "prefetch_reads"} {
		kv = append(kv, k, c[k])
	}
	jmlog.Info(msg, kv...)
}
