package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	nfsc "github.com/willscott/go-nfs-client/nfs"
)

// Engine-direct step 2 checks through the NFS front (the Mac's view):
//
//	seqread  read whole files in rsize READs with N in flight (the macOS
//	         client keeps 2), as a player plays a clip: first byte, Mbit/s,
//	         and the SHA-256 of the bytes (check C1). One JSON line per file.
//	pair     check T3: the time of two 1 MiB READs of one file at once over
//	         the time of one, each at a block nobody read yet.

// readRetry sends one READ and repeats it, after the macOS client's first
// JUKEBOX pause (4 s), while the server answers "try later". This client
// reports JUKEBOX as os.ErrInvalid.
func readRetry(f *nfsc.File, p []byte, off int64) (int, int, error) {
	retries := 0
	for {
		n, err := f.ReadAt(p, off)
		if err != nil && errors.Is(err, os.ErrInvalid) && retries < 20 {
			retries++
			time.Sleep(4 * time.Second)
			continue
		}
		if err != nil && err.Error() == "EOF" {
			err = nil
		}
		return n, retries, err
	}
}

type chunkResult struct {
	took    time.Duration
	idx     int64
	b       []byte
	err     error
	retries int
}

func seqread(args []string) error {
	fs := flag.NewFlagSet("seqread", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "NFS server address")
	export := fs.String("export", "/Volumes/zpool", "export path")
	files := fs.String("files", "", "comma-separated files, relative to the export root")
	inflight := fs.Int("inflight", 2, "READs in flight (one NFS connection each)")
	rsize := fs.Int("rsize", 1<<20, "READ size")
	budget := fs.Duration("budget", 0, "stop each file after this long (0 = whole file; no checksum when cut)")
	gap := fs.Duration("gap", time.Second, "pause between files")
	_ = fs.Parse(args)
	conns := make([]*nfsc.Target, *inflight)
	for i := range conns {
		v, err := dial(*addr, *export)
		if err != nil {
			return fmt.Errorf("mount: %w", err)
		}
		conns[i] = v
	}
	for fi, name := range strings.Split(*files, ",") {
		if fi > 0 {
			time.Sleep(*gap)
		}
		row := map[string]any{"step": "seqread", "file": name, "index": fi, "inflight": *inflight,
			"start_unix_ns": time.Now().UnixNano()}
		t0 := time.Now()
		info, _, err := conns[0].Lookup(name)
		if err != nil {
			row["error"] = "lookup: " + err.Error()
			emit(row)
			continue
		}
		size := info.Size()
		handles := make([]*nfsc.File, len(conns))
		for i, v := range conns {
			if handles[i], err = v.Open(name); err != nil {
				break
			}
		}
		if err != nil {
			row["error"] = "open: " + err.Error()
			emit(row)
			continue
		}
		nchunks := (size + int64(*rsize) - 1) / int64(*rsize)
		next := int64(0)
		var mu sync.Mutex
		stop := false
		results := make(chan chunkResult, 2*len(conns))
		var wg sync.WaitGroup
		for w := range handles {
			wg.Add(1)
			go func(f *nfsc.File) {
				defer wg.Done()
				for {
					mu.Lock()
					if stop || next >= nchunks || (*budget > 0 && time.Since(t0) > *budget) {
						mu.Unlock()
						return
					}
					idx := next
					next++
					mu.Unlock()
					want := int64(*rsize)
					if rem := size - idx*int64(*rsize); rem < want {
						want = rem
					}
					buf := make([]byte, want)
					cs := time.Now()
					got := 0
					var rerr error
					tries := 0
					for int64(got) < want {
						n, r, err := readRetry(f, buf[got:], idx*int64(*rsize)+int64(got))
						tries += r
						if err != nil {
							rerr = err
							break
						}
						if n == 0 {
							rerr = fmt.Errorf("short file: 0 bytes at %d of %d", idx*int64(*rsize)+int64(got), size)
							break
						}
						got += n
					}
					results <- chunkResult{idx: idx, b: buf[:got], err: rerr, retries: tries, took: time.Since(cs)}
					if rerr != nil {
						mu.Lock()
						stop = true
						mu.Unlock()
						return
					}
				}
			}(handles[w])
		}
		go func() { wg.Wait(); close(results) }()
		h := sha256.New()
		pending := map[int64][]byte{}
		var want, total int64
		var firstByte, maxRead time.Duration
		var lats []float64
		errs, retries := 0, 0
		var firstErr string
		for r := range results {
			retries += r.retries
			lats = append(lats, r.took.Seconds())
			if r.took > maxRead {
				maxRead = r.took
			}
			if r.err != nil {
				errs++
				if firstErr == "" {
					firstErr = r.err.Error()
				}
				continue
			}
			if r.idx == 0 && firstByte == 0 {
				firstByte = time.Since(t0)
			}
			total += int64(len(r.b))
			pending[r.idx] = r.b
			for b, ok := pending[want]; ok; b, ok = pending[want] {
				h.Write(b)
				delete(pending, want)
				want++
			}
		}
		el := time.Since(t0)
		row["bytes"] = total
		row["size"] = size
		row["seconds"] = secs(el)
		row["first_byte_s"] = secs(firstByte)
		row["mbit_s"] = float64(total) * 8 / 1e6 / el.Seconds()
		row["errors"] = errs
		row["max_read_s"] = secs(maxRead)
		if len(lats) > 0 {
			sort.Float64s(lats)
			row["p50_read_s"] = lats[len(lats)/2]
			row["p99_read_s"] = lats[len(lats)*99/100]
		}
		row["jukebox_retries"] = retries
		if firstErr != "" {
			row["error"] = firstErr
		}
		if want == nchunks && errs == 0 {
			row["sha256"] = hex.EncodeToString(h.Sum(nil))
		}
		emit(row)
	}
	return nil
}

func pair(args []string) error {
	fs := flag.NewFlagSet("pair", flag.ExitOnError)
	addr := fs.String("nfs", "127.0.0.1:21049", "NFS server address")
	export := fs.String("export", "/Volumes/zpool", "export path")
	file := fs.String("file", "", "a large file nobody read yet, relative to the export root")
	reps := fs.Int("reps", 8, "repetitions; each uses 3 fresh blocks")
	stride := fs.Int64("stride", 4<<20, "distance between the blocks read (one object block)")
	_ = fs.Parse(args)
	a, err := dial(*addr, *export)
	if err != nil {
		return err
	}
	b, err := dial(*addr, *export)
	if err != nil {
		return err
	}
	fa, err := a.Open(*file)
	if err != nil {
		return err
	}
	fb, err := b.Open(*file)
	if err != nil {
		return err
	}
	info, _, err := a.Lookup(*file)
	if err != nil {
		return err
	}
	if need := int64(3**reps+1) * *stride; info.Size() < need {
		return fmt.Errorf("%s is %d bytes; %d reps need %d", *file, info.Size(), *reps, need)
	}
	// Warm the handles (the open is not what T3 measures).
	buf := make([]byte, 4096)
	if _, _, err := readRetry(fa, buf, 0); err != nil {
		return err
	}
	if _, _, err := readRetry(fb, buf, 0); err != nil {
		return err
	}
	var ratios []float64
	for r := 0; r < *reps; r++ {
		base := int64(1+3*r) * *stride
		one := make([]byte, 1<<20)
		t := time.Now()
		if _, _, err := readRetry(fa, one, base); err != nil {
			return err
		}
		single := time.Since(t)
		time.Sleep(200 * time.Millisecond)
		var wg sync.WaitGroup
		var ea, eb error
		pa, pb := make([]byte, 1<<20), make([]byte, 1<<20)
		t = time.Now()
		wg.Add(2)
		go func() { defer wg.Done(); _, _, ea = readRetry(fa, pa, base+*stride) }()
		go func() { defer wg.Done(); _, _, eb = readRetry(fb, pb, base+2**stride) }()
		wg.Wait()
		both := time.Since(t)
		if ea != nil || eb != nil {
			return errors.Join(ea, eb)
		}
		ratio := both.Seconds() / single.Seconds()
		ratios = append(ratios, ratio)
		emit(map[string]any{"step": "pair", "rep": r, "one_s": secs(single), "two_s": secs(both), "ratio": ratio})
		time.Sleep(200 * time.Millisecond)
	}
	sort.Float64s(ratios)
	emit(map[string]any{"step": "pair-summary", "reps": len(ratios), "median_ratio": ratios[len(ratios)/2],
		"min": ratios[0], "max": ratios[len(ratios)-1]})
	return nil
}
