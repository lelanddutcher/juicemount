package main

// ownsave: the Mac's own saves through jm5 (laptop bug B1, 2026-11-14).
// Abroad, a save in one folder failed with EIO and then every GETATTR of the
// volume root answered EIO for the rest of the session.
//
//	peerprobe ownsave -export /jm-… -dir w/os -trials 3
//
// Setup writes, through NFS, b<i> (in-place targets), c<i> (chmod), r<i>
// (rename), 2 MiB each, then waits until each is on the volume (/jfs shows
// its full size: drained). Files already on /jfs at full size are kept. Each trial then runs, in this order:
//
//	atomic   CREATE a<i>.prproj.tmp, WRITE 2 MiB + 4 KiB, RENAME over
//	         a<i>.prproj (Premiere and Resolve save this way);
//	inplace  WRITE 64 KiB at 1 MiB into b<i>.prproj, twice (two saves);
//	chmod    SETATTR mode 0600 on c<i>.prproj;
//	rename   RENAME r<i>.prproj to r<i>-renamed.prproj;
//	truncate SETATTR size 1 MiB on t<i>.prproj, then WRITE 64 KiB at 1 MiB
//	         (a rewrite after a truncate; only with -cases including it).
//
// Every WRITE is FILE_SYNC, as the macOS client sends a save's last block.
// NFS3ERR_JUKEBOX is retried after the macOS waits (4, 8, 16, 30 s, at most
// 6 tries). After each operation the root and the folder are checked with
// GETATTR; a root GETATTR that fails is a volume-wide failure. -load writes a
// 256 KiB file into <dir>-load every second meanwhile (drain load). One JSON
// line per operation, then a summary line.

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

const (
	nfsJukebox = 10008
	saveSize   = 2 * mib
)

var macWaits = []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second}

type saver struct {
	v       *nfsc.Target
	auth    rpc.Auth
	root    []byte
	mu      sync.Mutex // one RPC at a time on the shared connection
	jukebox atomic.Int64
}

// statusError is a non-OK NFS status after the JUKEBOX retries.
type statusError struct {
	proc   string
	status uint32
}

func (e *statusError) Error() string { return fmt.Sprintf("%s status %d", e.proc, e.status) }

func statusOf(err error) uint32 {
	var se *statusError
	if errors.As(err, &se) {
		return se.status
	}
	return 0
}

func (s *saver) header(proc uint32) rpc.Header {
	return rpc.Header{Rpcvers: 2, Prog: nfsc.Nfs3Prog, Vers: nfsc.Nfs3Vers, Proc: proc, Cred: s.auth, Verf: rpc.AuthNull}
}

// call sends one RPC, retrying JUKEBOX as macOS does. It returns the reply
// body after the status word.
func (s *saver) call(name string, args func() any) (io.ReadSeeker, int, error) {
	for try := 0; ; try++ {
		s.mu.Lock()
		r, err := s.v.Call(args())
		var status uint32
		if err == nil {
			status, err = xdr.ReadUint32(r)
		}
		s.mu.Unlock()
		if err != nil {
			return nil, try, err
		}
		if status == nfsJukebox && try < 6 {
			s.jukebox.Add(1)
			time.Sleep(macWaits[min(try, len(macWaits)-1)])
			continue
		}
		if status != 0 {
			return nil, try, &statusError{name, status}
		}
		return r, try, nil
	}
}

func (s *saver) lookup(p string) ([]byte, error) {
	dir, name := path.Split(p)
	fh := s.root
	if d := path.Clean(dir); d != "." && d != "/" {
		var err error
		if fh, err = s.lookup(d); err != nil {
			return nil, err
		}
	}
	type args struct {
		rpc.Header
		What nfsc.Diropargs3
	}
	r, _, err := s.call("LOOKUP "+p, func() any { return &args{s.header(nfsc.NFSProc3Lookup), nfsc.Diropargs3{FH: fh, Filename: name}} })
	if err != nil {
		return nil, err
	}
	var out struct{ FH []byte }
	if err := xdr.Read(r, &out); err != nil {
		return nil, err
	}
	return out.FH, nil
}

func (s *saver) getattr(fh []byte) error {
	type args struct {
		rpc.Header
		FH []byte
	}
	_, _, err := s.call("GETATTR", func() any { return &args{s.header(nfsc.NFSProc3GetAttr), fh} })
	return err
}

func (s *saver) create(p string) ([]byte, error) {
	dfh, err := s.lookup(path.Dir(p))
	if err != nil {
		return nil, err
	}
	type how struct {
		Mode uint32
		Attr nfsc.Sattr3
	}
	type args struct {
		rpc.Header
		Where nfsc.Diropargs3
		HW    how
	}
	r, _, err := s.call("CREATE", func() any {
		return &args{s.header(nfsc.NFSProc3Create), nfsc.Diropargs3{FH: dfh, Filename: path.Base(p)},
			how{Attr: nfsc.Sattr3{Mode: nfsc.SetMode{SetIt: true, Mode: 0o644}}}}
	})
	if err != nil {
		return nil, err
	}
	var out struct{ FH nfsc.PostOpFH3 }
	if err := xdr.Read(r, &out); err != nil {
		return nil, err
	}
	if !out.FH.IsSet {
		return s.lookup(p)
	}
	return out.FH.FH, nil
}

// write sends data at off in 1 MiB FILE_SYNC WRITEs.
func (s *saver) write(fh []byte, off uint64, data []byte) error {
	type args struct {
		rpc.Header
		FH     []byte
		Offset uint64
		Count  uint32
		How    uint32
		Data   []byte
	}
	for len(data) > 0 {
		n := min(len(data), mib)
		chunk := data[:n]
		o := off
		if _, _, err := s.call("WRITE", func() any {
			return &args{s.header(nfsc.NFSProc3Write), fh, o, uint32(len(chunk)), 2, chunk}
		}); err != nil {
			return err
		}
		data, off = data[n:], off+uint64(n)
	}
	return nil
}

func (s *saver) rename(from, to string) error {
	ffh, err := s.lookup(path.Dir(from))
	if err != nil {
		return err
	}
	tfh, err := s.lookup(path.Dir(to))
	if err != nil {
		return err
	}
	type args struct {
		rpc.Header
		From nfsc.Diropargs3
		To   nfsc.Diropargs3
	}
	_, _, err = s.call("RENAME", func() any {
		return &args{s.header(nfsc.NFSProc3Rename), nfsc.Diropargs3{FH: ffh, Filename: path.Base(from)},
			nfsc.Diropargs3{FH: tfh, Filename: path.Base(to)}}
	})
	return err
}

func (s *saver) chmod(fh []byte, mode uint32) error {
	type args struct {
		rpc.Header
		FH    []byte
		Attr  nfsc.Sattr3
		Guard uint32
	}
	_, _, err := s.call("SETATTR", func() any {
		return &args{s.header(nfsc.NFSProc3SetAttr), fh, nfsc.Sattr3{Mode: nfsc.SetMode{SetIt: true, Mode: mode}}, 0}
	})
	return err
}

func (s *saver) truncate(fh []byte, size uint64) error {
	type args struct {
		rpc.Header
		FH    []byte
		Attr  nfsc.Sattr3
		Guard uint32
	}
	_, _, err := s.call("SETATTR", func() any {
		return &args{s.header(nfsc.NFSProc3SetAttr), fh, nfsc.Sattr3{Size: nfsc.SetSize{SetIt: true, Size: size}}, 0}
	})
	return err
}

func (s *saver) writeFile(p string, size int) error {
	fh, err := s.create(p)
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	_, _ = rand.Read(buf)
	return s.write(fh, 0, buf)
}

func ownsave(args []string) error {
	fs := flag.NewFlagSet("ownsave", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "jm5's NFS server")
	export := fs.String("export", "", "export path from the jm5 log")
	dir := fs.String("dir", "w/os", "folder for the saves, relative to the export and /jfs")
	jfs := fs.String("jfs", "/jfs", "jm5's juicefs mount (to see when setup files drained)")
	trials := fs.Int("trials", 3, "trials")
	load := fs.Bool("load", true, "write a 256 KiB file every second into <dir>-load meanwhile")
	watch := fs.Duration("watch", 20*time.Second, "keep checking the root after the last trial")
	drainCap := fs.Duration("drain-cap", 600*time.Second, "setup: wait at most this long for the drain")
	cases := fs.String("cases", "atomic,inplace,chmod,rename", "the operations to run, comma-separated")
	_ = fs.Parse(args)
	runCase := map[string]bool{}
	for _, c := range strings.Split(*cases, ",") {
		runCase[strings.TrimSpace(c)] = true
	}

	c, err := rpc.DialTCP("tcp", *addr, false)
	if err != nil {
		return err
	}
	m := &nfsc.Mount{Client: c}
	auth := rpc.NewAuthUnix("peerprobe", 501, 20).Auth()
	v, err := m.Mount(*export, auth)
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	_, root, err := v.Lookup(".")
	if err != nil {
		return fmt.Errorf("root: %w", err)
	}
	s := &saver{v: v, auth: auth, root: root}

	for _, d := range []string{*dir, *dir + "-load"} {
		if _, err := v.Mkdir(d, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	t0 := time.Now()
	var setup []string
	for i := 0; i < *trials; i++ {
		for _, k := range []string{"b", "c", "r"} {
			setup = append(setup, fmt.Sprintf("%s/%s%d.prproj", *dir, k, i))
		}
		if runCase["truncate"] {
			setup = append(setup, fmt.Sprintf("%s/t%d.prproj", *dir, i))
		}
	}
	for _, p := range setup {
		// The session script may seed them on lan (OS_SEED_LAN): keep those.
		if st, err := os.Stat(path.Join(*jfs, p)); err == nil && st.Size() == saveSize {
			continue
		}
		if err := s.writeFile(p, saveSize); err != nil {
			return fmt.Errorf("setup %s: %w", p, err)
		}
	}
	for _, p := range setup {
		for {
			if st, err := os.Stat(path.Join(*jfs, p)); err == nil && st.Size() == saveSize {
				break
			}
			if time.Since(t0) > *drainCap {
				return fmt.Errorf("setup: %s not drained after %s", p, *drainCap)
			}
			time.Sleep(time.Second)
		}
	}
	emit(map[string]any{"step": "ownsave-setup", "files": len(setup), "drained_s": secs(time.Since(t0))})
	time.Sleep(5 * time.Second) // let the setup's own push echoes settle

	stop := make(chan struct{})
	var loadWG sync.WaitGroup
	var loadErrs atomic.Int64
	var loadFirst atomic.Value // the first load error's text
	if *load {
		loadWG.Add(1)
		go func() {
			defer loadWG.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(time.Second):
				}
				if err := s.writeFile(fmt.Sprintf("%s-load/l%04d.bin", *dir, i), 256<<10); err != nil {
					if loadErrs.Add(1) == 1 {
						loadFirst.Store(err.Error())
					}
				}
			}
		}()
	}

	var ops, opFail, opIO int
	var volumeFirst float64 = -1
	start := time.Now()
	check := func(row map[string]any) {
		dfh, derr := s.lookup(*dir)
		if derr == nil {
			derr = s.getattr(dfh)
		}
		rerr := s.getattr(root)
		row["root_status"] = statusOf(rerr)
		row["root_ok"] = rerr == nil
		row["dir_ok"] = derr == nil
		if rerr != nil && volumeFirst < 0 {
			volumeFirst = secs(time.Since(start))
		}
	}
	do := func(kind string, trial int, p string, fn func() error) {
		if !runCase[kind] {
			return
		}
		row := map[string]any{"step": "ownsave", "kind": kind, "trial": trial, "path": p, "at_s": secs(time.Since(start))}
		jb := s.jukebox.Load()
		t := time.Now()
		err := fn()
		row["took_s"] = secs(time.Since(t))
		row["jukebox"] = s.jukebox.Load() - jb
		ops++
		if err != nil {
			opFail++
			row["error"] = err.Error()
			row["status"] = statusOf(err)
			if statusOf(err) == 5 {
				opIO++
			}
		}
		check(row)
		emit(row)
	}
	for i := 0; i < *trials; i++ {
		a := fmt.Sprintf("%s/a%d.prproj", *dir, i)
		do("atomic", i, a, func() error {
			if err := s.writeFile(a+".tmp", saveSize+4096); err != nil {
				return err
			}
			return s.rename(a+".tmp", a)
		})
		b := fmt.Sprintf("%s/b%d.prproj", *dir, i)
		for k := 0; k < 2; k++ {
			do("inplace", i, b, func() error {
				fh, err := s.lookup(b)
				if err != nil {
					return err
				}
				blk := bytes.Repeat([]byte{byte('A' + k)}, 64<<10)
				return s.write(fh, mib, blk)
			})
		}
		cpath := fmt.Sprintf("%s/c%d.prproj", *dir, i)
		do("chmod", i, cpath, func() error {
			fh, err := s.lookup(cpath)
			if err != nil {
				return err
			}
			return s.chmod(fh, 0o600)
		})
		r := fmt.Sprintf("%s/r%d.prproj", *dir, i)
		do("rename", i, r, func() error { return s.rename(r, fmt.Sprintf("%s/r%d-renamed.prproj", *dir, i)) })
		tp := fmt.Sprintf("%s/t%d.prproj", *dir, i)
		do("truncate", i, tp, func() error {
			fh, err := s.lookup(tp)
			if err != nil {
				return err
			}
			if err := s.truncate(fh, mib); err != nil {
				return err
			}
			return s.write(fh, mib, bytes.Repeat([]byte{'T'}, 64<<10))
		})
	}
	// Watch the root for a while: a sticky failure stays.
	end := time.Now().Add(*watch)
	rootFails, rootChecks := 0, 0
	for time.Now().Before(end) {
		rootChecks++
		if s.getattr(root) != nil {
			rootFails++
		}
		time.Sleep(time.Second)
	}
	finalErr := s.getattr(root)
	close(stop)
	loadWG.Wait()
	emit(map[string]any{
		"step": "ownsave-summary", "trials": *trials, "ops": ops, "op_failed": opFail, "op_eio": opIO,
		"volume_eio_first_s": volumeFirst, "volume_eio": volumeFirst >= 0,
		"watch_root_fail": rootFails, "watch_root_checks": rootChecks, "root_ok_at_end": finalErr == nil,
		"jukebox": s.jukebox.Load(), "load_errors": loadErrs.Load(), "load_error_first": loadFirst.Load(), "total_s": secs(time.Since(start)),
	})
	return nil
}
