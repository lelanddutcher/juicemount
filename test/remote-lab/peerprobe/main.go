// Command peerprobe measures how fast a peer's edit becomes visible through
// the JuiceMount NFS layer (jm5). It runs in the lab's client container, next
// to jm5. It reads through jm5's NFS server, as the Mac does. It writes as the
// peer through a second, separate juicefs mount of the same volume (another
// client with its own session and caches), as a teammate or the farm does.
//
//	peerprobe fixture -peer /jfs-peer -dir pe -n 4
//	peerprobe run -case held|fresh|append|append-held -file pe/f01.bin ...
//
// Each run prints one JSON line. A delay is measured from the moment the
// peer's close() returned, which is when juicefs has committed the edit to
// the metadata server. "never" means the probe gave up after -timeout.
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

const (
	mib       = 1 << 20
	fileSize  = 40 * mib // in-place cases: tags at 0 and 32 MiB, prime at 16 MiB
	smallSize = 8 * mib  // append cases: the peer appends 1 MiB at 8 MiB
	tagLen    = 16
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: peerprobe fixture|run [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "fixture":
		err = fixture(os.Args[2:])
	case "run":
		err = run(os.Args[2:])
	default:
		err = fmt.Errorf("unknown step %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "peerprobe:", err)
		os.Exit(1)
	}
}

func emit(v any) {
	b, _ := json.Marshal(v)
	fmt.Println(string(b))
}

func secs(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

// fixture writes, through the peer mount, n 40 MiB files f01.bin… and n
// 8 MiB files a01.bin… of random bytes. Existing files are rewritten, so
// every session starts from bytes no cache has seen.
func fixture(args []string) error {
	fs := flag.NewFlagSet("fixture", flag.ExitOnError)
	peer := fs.String("peer", "/jfs-peer", "the peer's juicefs mount")
	dir := fs.String("dir", "pe", "folder under both mounts")
	n := fs.Int("n", 4, "files of each kind")
	_ = fs.Parse(args)
	if err := os.MkdirAll(filepath.Join(*peer, *dir), 0o777); err != nil {
		return err
	}
	for i := 1; i <= *n; i++ {
		for _, f := range []struct {
			name string
			size int
		}{{fmt.Sprintf("f%02d.bin", i), fileSize}, {fmt.Sprintf("a%02d.bin", i), smallSize}} {
			buf := make([]byte, f.size)
			_, _ = rand.Read(buf)
			if err := os.WriteFile(filepath.Join(*peer, *dir, f.name), buf, 0o666); err != nil {
				return err
			}
		}
	}
	emit(map[string]any{"step": "fixture", "dir": *dir, "files": *n * 2})
	return nil
}

type probe struct {
	v       *nfsc.Target
	fh      []byte
	f       *nfsc.File
	name    string
	peer    string
	poll    time.Duration
	timeout time.Duration
	jukebox int // NFS3ERR_JUKEBOX replies retried
}

// read sends one READ. On NFS3ERR_JUKEBOX ("try later", which jm5 returns on
// purpose when a cold read misses its budget, and which this client reports
// as os.ErrInvalid) it waits and sends it again, as the macOS client does.
// The returned latency includes the retries.
func (p *probe) read(off int64, n int) ([]byte, time.Duration, error) {
	buf := make([]byte, n)
	t := time.Now()
	for try := 0; ; try++ {
		got, err := p.f.ReadAt(buf, off)
		if err != nil && (errors.Is(err, os.ErrInvalid) || strings.Contains(err.Error(), "invalid argument")) && try < 20 {
			p.jukebox++
			time.Sleep(time.Second)
			continue
		}
		d := time.Since(t)
		if err != nil && !errors.Is(err, io.EOF) && err.Error() != "EOF" {
			return nil, d, err
		}
		return buf[:got], d, nil
	}
}

func (p *probe) attr() (*nfsc.Fattr, error) { return p.v.GetAttr(p.fh) }

// peerWrite writes tag at each offset through the peer mount and closes the
// file. It returns when close() returned: the edit is committed.
func (p *probe) peerWrite(tag []byte, offs ...int64) (time.Time, float64, error) {
	t := time.Now()
	f, err := os.OpenFile(filepath.Join(p.peer, p.name), os.O_WRONLY, 0)
	if err != nil {
		return t, 0, err
	}
	for _, off := range offs {
		if _, err := f.WriteAt(tag, off); err != nil {
			f.Close()
			return t, 0, err
		}
	}
	if err := f.Close(); err != nil {
		return t, 0, err
	}
	done := time.Now()
	return done, secs(done.Sub(t)), nil
}

func (p *probe) peerRead(off int64, n int) []byte {
	f, err := os.Open(filepath.Join(p.peer, p.name))
	if err != nil {
		return nil
	}
	defer f.Close()
	buf := make([]byte, n)
	got, _ := f.ReadAt(buf, off)
	return buf[:got]
}

// waitFor polls cond every p.poll until it holds or p.timeout passes after
// t0. It returns the delay after t0 (or -1 for never), the number of polls
// and the latency of the poll that first saw it.
func (p *probe) waitFor(t0 time.Time, cond func() (bool, time.Duration, error)) (float64, int, float64, error) {
	polls := 0
	for {
		polls++
		ok, took, err := cond()
		if err != nil {
			return -1, polls, 0, err
		}
		if ok {
			return secs(time.Since(t0)), polls, secs(took), nil
		}
		if time.Since(t0) > p.timeout {
			return -1, polls, 0, nil
		}
		time.Sleep(p.poll)
	}
}

func run(args []string) (err error) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "jm5's NFS server")
	export := fs.String("export", "", "export path from the jm5 log")
	peer := fs.String("peer", "/jfs-peer", "the peer's juicefs mount")
	kase := fs.String("case", "held", "held | fresh | append | append-held | steady")
	name := fs.String("file", "pe/f01.bin", "file, relative to the export root and the peer mount")
	poll := fs.Duration("poll", 50*time.Millisecond, "poll interval")
	timeout := fs.Duration("timeout", 30*time.Second, "give up after this long")
	settle := fs.Duration("settle", time.Second, "pause between the prime read and the peer edit")
	reads := fs.Int("reads", 200, "steady: reads of the held file")
	_ = fs.Parse(args)

	c, err := rpc.DialTCP("tcp", *addr, false)
	if err != nil {
		return err
	}
	m := &nfsc.Mount{Client: c}
	v, err := m.Mount(*export, rpc.NewAuthUnix("peerprobe", 0, 0).Auth())
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	p := &probe{v: v, name: *name, peer: *peer, poll: *poll, timeout: *timeout}
	_, p.fh, err = v.Lookup(*name)
	if err != nil {
		return fmt.Errorf("lookup: %w", err)
	}
	p.f, err = v.Open(*name)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	row := map[string]any{"step": "run", "case": *kase, "file": *name, "start_unix_ns": time.Now().UnixNano()}
	defer func() {
		if err != nil {
			row["error"] = err.Error()
		}
		row["jukebox_retries"] = p.jukebox
		emit(row)
	}()

	a0, err := p.attr()
	if err != nil {
		return err
	}
	row["size0"] = a0.Filesize

	tagBytes := make([]byte, tagLen/2)
	_, _ = rand.Read(tagBytes)
	tag := []byte(strings.ToUpper(hex.EncodeToString(tagBytes)))
	row["tag"] = string(tag)

	switch *kase {
	case "steady":
		// The per-read cost with no edit: the held file is read at one
		// offset again and again (served from the open file).
		if _, _, err := p.read(16*mib, tagLen); err != nil {
			return err
		}
		t := time.Now()
		var worst time.Duration
		for i := 0; i < *reads; i++ {
			_, d, err := p.read(16*mib+int64(i%64)*4096, tagLen)
			if err != nil {
				return err
			}
			worst = max(worst, d)
		}
		row["reads"] = *reads
		row["mean_read_s"] = secs(time.Since(t)) / float64(*reads)
		row["worst_read_s"] = secs(worst)
		return nil

	case "held", "fresh":
		// held: jm5 has the file open in juicefs (the prime read pools its
		// descriptor) when the peer edits it in place: B1. fresh: jm5 never
		// read it; only its size/mtime in the mirror can be stale: B2.
		if *kase == "held" {
			if _, _, err := p.read(0, tagLen); err != nil {
				return err
			}
			if _, d, err := p.read(16*mib, tagLen); err != nil {
				return err
			} else {
				row["prime_read_s"] = secs(d)
			}
			if _, _, err := p.read(32*mib, tagLen); err != nil {
				return err
			}
		}
		time.Sleep(*settle)
		t0, wt, err := p.peerWrite(tag, 0, 32*mib)
		if err != nil {
			return err
		}
		row["peer_write_s"] = wt
		row["peer_sees"] = bytes.Equal(p.peerRead(0, tagLen), tag)
		var saw0, saw2 bool
		mtimeAt := -1.0 // the attr is polled in the same loop, timed on its own
		d, polls, took, err := p.waitFor(t0, func() (bool, time.Duration, error) {
			if mtimeAt < 0 {
				if a, err := p.attr(); err == nil && a.Mtime != a0.Mtime {
					mtimeAt = secs(time.Since(t0))
				}
			}
			b0, d0, err := p.read(0, tagLen)
			if err != nil {
				return false, 0, err
			}
			b2, d2, err := p.read(32*mib, tagLen)
			if err != nil {
				return false, 0, err
			}
			saw0, saw2 = bytes.Equal(b0, tag), bytes.Equal(b2, tag)
			return saw0 && saw2, d0 + d2, nil
		})
		if err != nil {
			return err
		}
		row["data_visible_s"], row["data_polls"], row["data_first_new_read_s"] = d, polls, took
		row["saw_new_at_0"], row["saw_new_at_32m"] = saw0, saw2
		if d < 0 {
			// A fresh offset nobody read before, and a new lookup + open.
			b, _, _ := p.read(24*mib, tagLen)
			row["fresh_offset_bytes"] = len(b)
			if f2, err := v.Open(*name); err == nil {
				b0 := make([]byte, tagLen)
				n, _ := f2.ReadAt(b0, 0)
				row["reopen_new"] = bytes.Equal(b0[:n], tag)
			}
		}
		md, mpolls, _, err := p.waitFor(t0, func() (bool, time.Duration, error) {
			if mtimeAt >= 0 {
				return true, 0, nil
			}
			a, err := p.attr()
			if err != nil {
				return false, 0, err
			}
			return a.Mtime != a0.Mtime, 0, nil
		})
		if mtimeAt >= 0 {
			md = mtimeAt
		}
		if err != nil {
			return err
		}
		row["mtime_visible_s"], row["mtime_polls"] = md, mpolls
		return nil

	case "append", "append-held":
		// The peer appends 1 MiB (tag at the start of it) to an 8 MiB file.
		size0 := int64(a0.Filesize)
		if *kase == "append-held" {
			if _, _, err := p.read(4*mib, tagLen); err != nil {
				return err
			}
		}
		time.Sleep(*settle)
		t := time.Now()
		f, err := os.OpenFile(filepath.Join(*peer, *name), os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		blob := make([]byte, mib)
		copy(blob, tag)
		if _, err := f.WriteAt(blob, size0); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		t0 := time.Now()
		row["peer_write_s"] = secs(t0.Sub(t))
		want := uint64(size0 + mib)
		sd, spolls, _, err := p.waitFor(t0, func() (bool, time.Duration, error) {
			a, err := p.attr()
			if err != nil {
				return false, 0, err
			}
			return a.Filesize == want, 0, nil
		})
		if err != nil {
			return err
		}
		row["size_visible_s"], row["size_polls"] = sd, spolls
		dd, dpolls, took, err := p.waitFor(t0, func() (bool, time.Duration, error) {
			b, d, err := p.read(size0, tagLen)
			if err != nil {
				return false, 0, err
			}
			return bytes.Equal(b, tag), d, nil
		})
		if err != nil {
			return err
		}
		row["data_visible_s"], row["data_polls"], row["data_first_new_read_s"] = dd, dpolls, took
		a1, err := p.attr()
		if err == nil {
			row["size1"] = a1.Filesize
			row["mtime_changed"] = a1.Mtime != a0.Mtime
		}
		return nil
	}
	return fmt.Errorf("unknown case %q", *kase)
}
