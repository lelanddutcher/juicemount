package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enginehost"
)

var (
	doorMount = flag.String("door-mount", "/jfs-door", "door: where the juicefs mount with the socket goes")
	doorFile  = flag.String("door-file", "", "door: a file, relative to the mount, to read through both doors")
	doorReads = flag.Int("door-reads", 500, "door: warm 1 MiB socket reads timed for E4")
	doorFlags = flag.String("door-flags", "", "door: extra mount flags, space separated")
)

// door (engine-direct step 2): start `juicefs mount` with the engine socket
// exactly as the app does (enginehost.Door, descriptors 3 and 4), then
//
//	E4 read  time warm 1 MiB reads of SSD-cached data through the socket
//	both     the file read through the socket and through the kernel mount
//	         of the same process: same SHA-256 (one engine, two doors)
//	E5 read  a handle this client did not open is refused (EBADF)
func door() {
	d := enginehost.NewDoor(enginehost.DoorConfig{ExpectVersion: *expect})
	defer d.Close()
	dargs, files, err := d.Prepare(3)
	if err != nil {
		die("prepare: %v", err)
	}
	_ = os.MkdirAll(*doorMount, 0o755)
	args := []string{"mount", *metaURL, *doorMount, "--no-usage-report", "--no-bgjob", "--cache-dir", "/var/jfscache-door"}
	if *doorFlags != "" {
		args = append(args, strings.Fields(*doorFlags)...)
	}
	cmd := exec.Command(*bin, append(args, dargs...)...)
	cmd.ExtraFiles = files
	// JFS_SUPERVISOR as the app sets it: one juicefs process, this one its
	// parent (the socket's peer check).
	cmd.Env = append(baseEnv(), "META_PASSWORD="+password(), "JFS_SUPERVISOR="+strconv.Itoa(os.Getpid()))
	if lf, err := os.OpenFile(*helperLg, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
		cmd.Stdout, cmd.Stderr = lf, lf
	}
	start := time.Now()
	err = cmd.Start()
	d.Started(err)
	if err != nil {
		die("start mount: %v", err)
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h, err := d.WaitReady(ctx)
	if err != nil {
		die("door: %v", err)
	}
	emit("door-ready", map[string]any{"ms": ms(time.Since(start)), "epoch": h.Epoch, "ops": h.Ops, "version": h.JuiceFSVersion})
	path := *doorMount + "/" + *doorFile
	fi, err := os.Stat(path)
	if err != nil {
		die("stat through the mount: %v", err)
	}
	ino := fi.Sys().(*syscall.Stat_t).Ino
	cl, _, ok := d.Current()
	if !ok {
		die("no current client")
	}
	t := time.Now()
	o, err := cl.Open(ino, 30*time.Second)
	if err != nil {
		die("open: %v", err)
	}
	openMs := ms(time.Since(t))
	// Whole file through the socket (this also warms the SSD cache).
	hs := sha256.New()
	buf := make([]byte, 1<<20)
	var off int64
	for {
		n, err := cl.Read(ino, o.Handle, buf, off, 2*time.Minute)
		if err != nil {
			die("socket read at %d: %v", off, err)
		}
		hs.Write(buf[:n])
		off += int64(n)
		if n < len(buf) {
			break
		}
	}
	// The same file through the kernel door of the same process.
	f, err := os.Open(path)
	if err != nil {
		die("open through the mount: %v", err)
	}
	hk := sha256.New()
	kn, err := io.Copy(hk, f)
	f.Close()
	if err != nil {
		die("read through the mount: %v", err)
	}
	sSum, kSum := hex.EncodeToString(hs.Sum(nil)), hex.EncodeToString(hk.Sum(nil))
	emit("both-doors", map[string]any{"open_ms": openMs, "socket_bytes": off, "mount_bytes": kn,
		"socket_sha256": sSum, "mount_sha256": kSum, "same": sSum == kSum && off == kn})
	// E4 read: warm 1 MiB reads through the socket.
	var lat []time.Duration
	fails := 0
	blocks := off / (1 << 20)
	if blocks < 1 {
		blocks = 1
	}
	for i := 0; i < *doorReads; i++ {
		at := (int64(i) % blocks) << 20
		t := time.Now()
		if _, err := cl.Read(ino, o.Handle, buf, at, 10*time.Second); err != nil {
			fails++
			continue
		}
		lat = append(lat, time.Since(t))
	}
	emit("E4-read", pct(lat, fails))
	_, err = cl.Read(ino, o.Handle+12345, buf, 0, time.Second)
	var se *enginehost.StatusError
	refused := err != nil && asStatus(err, &se) && se.Status == syscall.EBADF
	emit("E5-read", map[string]any{"foreign_handle_refused": refused, "error": errText(err)})
	_ = cl.Release(ino, o.Handle, 5*time.Second)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
