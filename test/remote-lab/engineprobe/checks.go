package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const marker = ".engineprobe-marker"

// The fixture: clips c00.. (T1/T2), pair files p00.. (T3: two per trial),
// edit files q00.. (T4). T5 makes its own folders under the fixture.
type layout struct {
	dir                    string
	clips, pairs, edits    int
	clipMiB, pairMiB, eMiB int
}

func (l *layout) flags(fs *flag.FlagSet) {
	fs.StringVar(&l.dir, "dir", "engineprobe-fx", "fixture folder in the volume (name must start with engineprobe-)")
	fs.IntVar(&l.clips, "clips", 12, "clips for T1/T2")
	fs.IntVar(&l.pairs, "pairs", 8, "trials for T3 (two files each)")
	fs.IntVar(&l.edits, "edits", 6, "files for T4")
	fs.IntVar(&l.clipMiB, "clip-mib", 64, "clip size, MiB")
	fs.IntVar(&l.pairMiB, "pair-mib", 8, "T3 file size, MiB")
	fs.IntVar(&l.eMiB, "edit-mib", 8, "T4 file size, MiB")
}

func (l *layout) path(kind string, i int) string {
	return filepath.Join(l.dir, fmt.Sprintf("%s%02d.bin", kind, i))
}

func checkDir(dir string) error {
	if !strings.HasPrefix(filepath.Base(dir), "engineprobe-") || strings.Contains(dir, "..") {
		return fmt.Errorf("refusing folder %q: the name must start with engineprobe-", dir)
	}
	return nil
}

// cmdFixture writes the fixture through a FUSE mount (on the lan profile).
func cmdFixture(args []string) error {
	fs := flag.NewFlagSet("fixture", flag.ExitOnError)
	var l layout
	l.flags(fs)
	mnt := fs.String("mnt", "", "a FUSE mount of the volume")
	allow := fs.Bool("allow-write", false, "required: this command writes")
	_ = fs.Parse(args)
	if !*allow || *mnt == "" {
		return errors.New("fixture needs -mnt and -allow-write")
	}
	if err := checkDir(l.dir); err != nil {
		return err
	}
	root := filepath.Join(*mnt, l.dir)
	if _, err := os.Stat(root); err == nil {
		if _, err := os.Stat(filepath.Join(root, marker)); err != nil {
			return fmt.Errorf("%s exists and is not a probe fixture", root)
		}
	} else if err := os.Mkdir(root, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, marker), []byte("engineprobe fixture\n"), 0o644); err != nil {
		return err
	}
	var seed [32]byte
	_, _ = rand.Read(seed[:])
	src := mrand.NewChaCha8(seed)
	write := func(p string, size int) error {
		f, err := os.Create(filepath.Join(*mnt, p))
		if err != nil {
			return err
		}
		buf := make([]byte, mib)
		for off := 0; off < size; off += mib {
			_, _ = src.Read(buf)
			if _, err := f.Write(buf); err != nil {
				f.Close()
				return err
			}
		}
		return f.Close()
	}
	start := time.Now()
	n := 0
	for i := 0; i < l.clips; i++ {
		if err := write(l.path("c", i), l.clipMiB*mib); err != nil {
			return err
		}
		n++
	}
	for i := 0; i < 2*l.pairs; i++ {
		if err := write(l.path("p", i), l.pairMiB*mib); err != nil {
			return err
		}
		n++
	}
	for i := 0; i < l.edits; i++ {
		if err := write(l.path("q", i), l.eMiB*mib); err != nil {
			return err
		}
		n++
	}
	emit(map[string]any{"test": "fixture", "files": n, "secs": time.Since(start).Seconds()})
	return nil
}

type runOpts struct {
	l          layout
	label      string
	resolve    string // direct arm: walk (name lookups timed) or ino (inode known, as from the mirror)
	tests      map[string]bool
	relay      *tripCounter
	allowWrite bool
	peer       string
	t4Modes    []string
	t4Play     float64
	t4Timeout  time.Duration
	t5N        int
	t5Size     int
	t5Workers  []int
	t5Rename   bool
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var o runOpts
	o.l.flags(fs)
	armName := fs.String("arm", "", "direct (engine, no mount) or fuse (a mount path)")
	mnt := fs.String("mnt", "", "fuse arm: the FUSE mount")
	openat := fs.Bool("openat", false, "fuse arm: open files with openat() on a held folder descriptor (tuning arm a1)")
	metaURL := fs.String("meta", "", "direct arm: metadata URL without password (META_PASSWORD holds it)")
	var ef engineFlags
	fs.StringVar(&ef.cacheDir, "cache-dir", "", "direct arm: block cache folder (its own, never a mount's)")
	fs.Int64Var(&ef.cacheSizeMiB, "cache-size", 20480, "direct arm: cache size, MiB")
	fs.Int64Var(&ef.bufferMiB, "buffer-size", 512, "direct arm: read/write buffer, MiB")
	readahead := fs.Int("max-readahead-mib", 1, "direct arm: --max-readahead, MiB")
	fs.IntVar(&ef.prefetch, "prefetch", 1, "direct arm: --prefetch")
	fs.IntVar(&ef.maxUploads, "max-uploads", 4, "direct arm: --max-uploads")
	fs.BoolVar(&ef.demandFirst, "demand-first", true, "direct arm: --demand-first")
	fs.StringVar(&ef.bucket, "bucket", "", "direct arm: bucket URL override (the app's --bucket)")
	fs.StringVar(&o.label, "label", "", "label for the result lines")
	fs.StringVar(&o.resolve, "resolve", "ino", "direct arm: ino (inode known before the timer, as from the app's mirror) or walk")
	tests := fs.String("tests", "t1t2,t3", "checks: t1t2,t3,t4,t5 (t4 and t5 write; need -allow-write)")
	relayURL := fs.String("relay", "", "relay counter URL (http://127.0.0.1:16380); empty: no round-trip counts")
	fs.BoolVar(&o.allowWrite, "allow-write", false, "allow T4's peer edit and T5's creates, inside the fixture only")
	fs.StringVar(&o.peer, "peer", "", "T4: a second, separate mount of the volume (the teammate)")
	t4Modes := fs.String("t4-modes", "refresh,none", "T4: refresh (the app's metadata event sends JMRefreshInode), none (no event)")
	fs.Float64Var(&o.t4Play, "t4-play", 0, "T4: play the file on the held handle at this many MiB/s while the peer edits (0: off)")
	fs.DurationVar(&o.t4Timeout, "t4-timeout", 20*time.Second, "T4: give up after")
	fs.IntVar(&o.t5N, "t5-n", 200, "T5: files")
	fs.IntVar(&o.t5Size, "t5-size", 64<<10, "T5: bytes per file")
	t5Workers := fs.String("t5-workers", "4,8", "T5: worker counts")
	fs.BoolVar(&o.t5Rename, "t5-rename", true, "T5: write a temporary name and rename it, as the app's drain does")
	_ = fs.Parse(args)
	if err := checkDir(o.l.dir); err != nil {
		return err
	}
	o.tests = map[string]bool{}
	for _, t := range strings.Split(*tests, ",") {
		o.tests[strings.TrimSpace(t)] = true
	}
	o.t4Modes = strings.Split(*t4Modes, ",")
	for _, w := range strings.Split(*t5Workers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil || n < 1 {
			return fmt.Errorf("bad -t5-workers %q", *t5Workers)
		}
		o.t5Workers = append(o.t5Workers, n)
	}
	if *relayURL != "" {
		o.relay = &tripCounter{url: *relayURL}
	}
	if (o.tests["t4"] || o.tests["t5"]) && !o.allowWrite {
		return errors.New("t4 and t5 write: pass -allow-write")
	}
	ef.readahead = int64(*readahead) << 20
	ef.readOnly = !o.allowWrite

	var a arm
	setupStart := time.Now()
	switch *armName {
	case "fuse":
		if *mnt == "" {
			return errors.New("fuse arm needs -mnt")
		}
		a = &fuseArm{mnt: *mnt, openat: *openat}
	case "direct":
		if *metaURL == "" || ef.cacheDir == "" {
			return errors.New("direct arm needs -meta and -cache-dir")
		}
		if strings.Contains(*metaURL, ":@") || strings.Count(strings.SplitN(*metaURL, "@", 2)[0], ":") > 1 {
			return errors.New("put the password in META_PASSWORD, not in -meta")
		}
		d, err := openDirect(*metaURL, ef)
		if err != nil {
			return err
		}
		a = d
	default:
		return errors.New("-arm direct|fuse")
	}
	defer a.Close()
	emit(map[string]any{"test": "setup", "arm": a.Name(), "label": o.label, "setup_ms": ms(time.Since(setupStart)), "resolve": o.resolve, "openat": *openat})

	// The fixture marker must be there before anything runs.
	if h, err := a.Open(filepath.Join(o.l.dir, marker), 0); err != nil {
		return fmt.Errorf("fixture marker: %w (run fixture first)", err)
	} else {
		h.Close()
	}
	if o.tests["t1t2"] {
		if err := checkT1T2(a, &o); err != nil {
			return err
		}
	}
	if o.tests["t3"] {
		if err := checkT3(a, &o); err != nil {
			return err
		}
	}
	if o.tests["t4"] {
		if err := checkT4(a, &o); err != nil {
			return err
		}
	}
	if o.tests["t5"] {
		if err := checkT5(a, &o); err != nil {
			return err
		}
	}
	// Object bytes this arm moved (the mount arms: since their mount), for
	// the run's data ledger.
	end := o.base(a, "bytes")
	m := a.Metrics()
	end["get_bytes"], end["put_bytes"] = m["bytes.GET"], m["bytes.PUT"]
	emit(end)
	return nil
}

func (o *runOpts) base(a arm, test string) map[string]any {
	return map[string]any{"test": test, "arm": a.Name(), "label": o.label}
}

// checkT1T2: per cold clip, open and read the first MiB. T1 is the serial
// metadata round trips before the first byte (every relay trip in the window
// is metadata: object reads do not pass the relay). T2 is open → first MiB.
func checkT1T2(a arm, o *runOpts) error {
	buf := make([]byte, mib)
	var later []float64
	for i := 0; i < o.l.clips; i++ {
		p := o.l.path("c", i)
		var ino uint64
		r := o.base(a, "t1t2")
		r["clip"], r["i"] = p, i
		if a.Name() == "direct" && o.resolve == "ino" {
			// The inode is known before the timer, as the app's mirror knows
			// it. The lookups are measured on their own.
			t := time.Now()
			var err error
			if ino, err = a.Resolve(p); err != nil {
				return err
			}
			r["pre_resolve_ms"] = ms(time.Since(t))
			r["pre_resolve_rt"] = o.relay.between(t, time.Now())
		}
		m0 := a.Metrics()
		t0 := time.Now()
		if a.Name() == "direct" && o.resolve == "walk" {
			var err error
			if ino, err = a.Resolve(p); err != nil {
				return err
			}
			r["resolve_ms"] = ms(time.Since(t0))
		}
		h, err := a.Open(p, ino)
		if err != nil {
			return err
		}
		t1 := time.Now()
		n, err := h.ReadAt(buf, 0)
		t2 := time.Now()
		h.Close()
		if err != nil || n != mib {
			return fmt.Errorf("read %s: n=%d err=%v", p, n, err)
		}
		r["open_ms"], r["read_ms"], r["first_mib_ms"] = ms(t1.Sub(t0)), ms(t2.Sub(t1)), ms(t2.Sub(t0))
		r["rt_open"] = o.relay.between(t0, t1)
		r["rt_before_data"] = o.relay.between(t0, t2)
		r["ops"] = diffMetrics(m0, a.Metrics())
		emit(r)
		if i > 0 {
			later = append(later, ms(t2.Sub(t0)))
		}
	}
	s := o.base(a, "t2-summary")
	s["later_median_ms"], s["later_n"] = median(later), len(later)
	emit(s)
	return nil
}

// checkT3: one 1 MiB read versus two at once (offsets 0 and 1 MiB) on cold
// files that are already open, so only the reads are timed.
func checkT3(a arm, o *runOpts) error {
	var one, two []float64
	for i := 0; i < o.l.pairs; i++ {
		h1, err := a.Open(o.l.path("p", 2*i), 0)
		if err != nil {
			return err
		}
		h2, err := a.Open(o.l.path("p", 2*i+1), 0)
		if err != nil {
			return err
		}
		b0, b1 := make([]byte, mib), make([]byte, mib)
		t := time.Now()
		if _, err := h1.ReadAt(b0, 0); err != nil {
			return err
		}
		d1 := time.Since(t)
		var wg sync.WaitGroup
		var e0, e1 error
		var d0a, d1a time.Duration
		t = time.Now()
		wg.Add(2)
		go func() { defer wg.Done(); _, e0 = h2.ReadAt(b0, 0); d0a = time.Since(t) }()
		go func() { defer wg.Done(); _, e1 = h2.ReadAt(b1, mib); d1a = time.Since(t) }()
		wg.Wait()
		d2 := time.Since(t)
		h1.Close()
		h2.Close()
		if e0 != nil || e1 != nil {
			return fmt.Errorf("t3 reads: %v %v", e0, e1)
		}
		r := o.base(a, "t3")
		r["i"], r["one_ms"], r["two_ms"], r["two_at0_ms"], r["two_at1_ms"] = i, ms(d1), ms(d2), ms(d0a), ms(d1a)
		emit(r)
		one, two = append(one, ms(d1)), append(two, ms(d2))
	}
	s := o.base(a, "t3-summary")
	s["one_median_ms"], s["two_median_ms"] = median(one), median(two)
	if m := median(one); m > 0 {
		s["ratio"] = median(two) / m
	}
	emit(s)
	return nil
}

// checkT4: the reader holds the file open (and plays it, with -t4-play); the
// peer writes a 16-byte tag at 0 through its own mount and closes. In mode
// refresh the probe then does what the app does on the metadata event (send
// JMRefreshInode) and polls the held handle; in mode none it only polls.
// A fresh open is tried at the end of each trial as well.
func checkT4(a arm, o *runOpts) error {
	if o.peer == "" {
		return errors.New("t4 needs -peer (a second mount of the volume)")
	}
	if _, err := os.Stat(filepath.Join(o.peer, o.l.dir, marker)); err != nil {
		return fmt.Errorf("t4: no fixture marker under the peer mount: %w", err)
	}
	trial := 0
	for _, mode := range o.t4Modes {
		var vis []float64
		per := o.l.edits / len(o.t4Modes)
		if per < 1 {
			per = 1
		}
		for k := 0; k < per && trial < o.l.edits; k, trial = k+1, trial+1 {
			p := o.l.path("q", trial)
			r, err := t4Trial(a, o, p, mode)
			if err != nil {
				return err
			}
			emit(r)
			if v, ok := r["visible_ms"].(float64); ok {
				vis = append(vis, v)
			}
		}
		s := o.base(a, "t4-summary")
		s["mode"], s["visible_median_ms"], s["visible_n"] = mode, median(vis), len(vis)
		emit(s)
	}
	return nil
}

func t4Trial(a arm, o *runOpts, p, mode string) (map[string]any, error) {
	r := o.base(a, "t4")
	r["file"], r["mode"] = p, mode
	var ino uint64
	if a.Name() == "direct" {
		var err error
		if ino, err = a.Resolve(p); err != nil {
			return nil, err
		}
	}
	h, err := a.Open(p, ino)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	head := make([]byte, 16)
	if _, err := h.ReadAt(head, 0); err != nil {
		return nil, err
	}
	// Playback on the same handle, from 1 MiB on, looping.
	stop := make(chan struct{})
	var playMax atomic.Int64
	var playWG sync.WaitGroup
	if o.t4Play > 0 {
		playWG.Add(1)
		go func() {
			defer playWG.Done()
			b := make([]byte, mib)
			size := int64(o.l.eMiB) * mib
			off := int64(mib)
			tick := time.Duration(float64(time.Second) / o.t4Play)
			for {
				select {
				case <-stop:
					return
				default:
				}
				t := time.Now()
				_, _ = h.ReadAt(b, off)
				d := time.Since(t)
				if int64(d) > playMax.Load() {
					playMax.Store(int64(d))
				}
				off += mib
				if off >= size {
					off = mib
				}
				if d < tick {
					time.Sleep(tick - d)
				}
			}
		}()
		time.Sleep(2 * time.Second)
	}
	tag := make([]byte, 16)
	_, _ = rand.Read(tag)
	pf, err := os.OpenFile(filepath.Join(o.peer, p), os.O_WRONLY, 0)
	if err != nil {
		close(stop)
		return nil, err
	}
	if _, err := pf.WriteAt(tag, 0); err != nil {
		pf.Close()
		close(stop)
		return nil, err
	}
	if err := pf.Close(); err != nil {
		close(stop)
		return nil, err
	}
	commit := time.Now()
	if mode == "refresh" {
		flags, err := a.Refresh(p, ino)
		r["refresh_ms"], r["refresh_flags"] = ms(time.Since(commit)), flags
		if err != nil {
			r["refresh_err"] = err.Error()
		}
	}
	got := make([]byte, 16)
	polls := 0
	for time.Since(commit) < o.t4Timeout {
		polls++
		if _, err := h.ReadAt(got, 0); err == nil && bytes.Equal(got, tag) {
			r["visible_ms"] = ms(time.Since(commit))
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := r["visible_ms"]; !ok {
		r["visible_ms"] = "never"
	}
	r["polls"] = polls
	close(stop)
	playWG.Wait()
	if o.t4Play > 0 {
		r["play_read_max_ms"] = ms(time.Duration(playMax.Load()))
	}
	// A fresh open after the edit (the reopen path; on macFUSE it shares the
	// held handle, which is bug M2).
	t := time.Now()
	if h2, err := a.Open(p, ino); err == nil {
		_, _ = h2.ReadAt(got, 0)
		h2.Close()
		r["reopen_ms"], r["reopen_new"] = ms(time.Since(t)), bytes.Equal(got, tag)
	}
	return r, nil
}

// checkT5: N small files into one new folder, W workers, through each arm's
// create path. The direct arm knows the folder's inode (as the app's drain
// would); the fuse arm goes through the kernel.
func checkT5(a arm, o *runOpts) error {
	data := make([]byte, o.t5Size)
	_, _ = rand.Read(data)
	for _, w := range o.t5Workers {
		dir := filepath.Join(o.l.dir, fmt.Sprintf("t5-%s-%s-w%d-%d", a.Name(), sanitize(o.label), w, time.Now().UnixNano()))
		if err := a.Mkdir(dir); err != nil {
			return fmt.Errorf("t5 mkdir: %w", err)
		}
		var write func(name string) error
		if d, ok := a.(*directArm); ok {
			pi, err := d.Resolve(dir)
			if err != nil {
				return err
			}
			write = func(name string) error { return d.writeIn(metaIno(pi), name, data, o.t5Rename) }
		} else {
			write = func(name string) error { return a.WriteFile(filepath.Join(dir, name), data, o.t5Rename) }
		}
		var next atomic.Int64
		lat := make([]float64, o.t5N)
		errs := make(chan error, w)
		m0 := a.Metrics()
		t0 := time.Now()
		var wg sync.WaitGroup
		for k := 0; k < w; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= o.t5N {
						return
					}
					t := time.Now()
					if err := write(fmt.Sprintf("f%04d.bin", i)); err != nil {
						errs <- err
						return
					}
					lat[i] = ms(time.Since(t))
				}
			}()
		}
		wg.Wait()
		d := time.Since(t0)
		select {
		case err := <-errs:
			return fmt.Errorf("t5: %w", err)
		default:
		}
		r := o.base(a, "t5")
		r["workers"], r["files"], r["bytes"], r["rename"] = w, o.t5N, o.t5Size, o.t5Rename
		r["secs"], r["files_per_s"] = d.Seconds(), float64(o.t5N)/d.Seconds()
		r["file_p50_ms"], r["file_p95_ms"] = pct(lat, 50), pct(lat, 95)
		if st := o.relay.between(t0, time.Now()); st != nil {
			r["rt"], r["rt_per_file"] = st.Trips, float64(st.Trips)/float64(o.t5N)
		}
		r["ops"] = diffMetrics(m0, a.Metrics())
		emit(r)
	}
	return nil
}

func sanitize(s string) string {
	if s == "" {
		return "x"
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '_'
	}, s)
}

func median(xs []float64) float64 { return pct(xs, 50) }

func pct(xs []float64, p int) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := (len(s) - 1) * p / 100
	return s[i]
}
