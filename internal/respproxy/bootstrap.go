package respproxy

// bootstrap.go fills the directory-parent map at proxy start (H8 plan §1.4
// "Cache and bootstrap"):
//
//  1. Synchronously, d1 and its directory children down to the project
//     depth: every project root and its display path is known before the
//     proxy takes its first client.
//  2. In the background, SCAN d* and HGETALL every directory hash, keeping
//     only directory entries. Until it finishes, a miss costs O(depth) side
//     GETs (ancestry.go).
//
// The scan never overwrites an edge that is already known: an edge learned
// from a committed unit (a rename) is newer than anything the scan read.

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// BootstrapStats reports what the bootstrap did (measurements).
type BootstrapStats struct {
	SyncDur   time.Duration // project roots (synchronous)
	SyncDirs  int           // directories read synchronously
	ScanDur   time.Duration // background SCAN d* + HGETALL
	ScanKeys  int           // d* hashes read
	ScanEdges int           // directory edges added by the scan
	ScanDone  bool
	ScanErr   string
}

type bootstrapState struct {
	mu    sync.Mutex
	st    BootstrapStats
	done  atomic.Bool
	doneC chan struct{}
}

func (a *ancestry) hgetall(key string) ([]Value, error) {
	a.sideReads.Add(1)
	v, err := a.side.Do(a.db, []byte("HGETALL"), []byte(key))
	if err != nil {
		return nil, err
	}
	if err := v.Err(); err != nil {
		return nil, err
	}
	return v.Elems, nil
}

// dirEntries returns the directory children of d (name -> inode).
func (a *ancestry) dirEntries(d uint64) (map[string]uint64, error) {
	el, err := a.hgetall("d" + strconv.FormatUint(d, 10))
	if err != nil {
		return nil, err
	}
	out := map[string]uint64{}
	for i := 0; i+1 < len(el); i += 2 {
		v := el[i+1].Str
		if t, ok := entryType(v); !ok || t != typeDir {
			continue
		}
		if n, ok := entryInode(v); ok {
			out[string(el[i].Str)] = n
		}
	}
	return out, nil
}

// bootstrapRoots reads the tree down to the project depth.
func (a *ancestry) bootstrapRoots() (int, error) {
	level := []uint64{rootIno}
	read := 0
	for depth := 0; depth < a.depth && len(level) > 0; depth++ {
		var next []uint64
		for _, d := range level {
			kids, err := a.dirEntries(d)
			if err != nil {
				return read, err
			}
			read++
			edges := make([]unitEdge, 0, len(kids))
			for name, c := range kids {
				if isTrashIno(c) {
					continue
				}
				edges = append(edges, unitEdge{parent: d, child: c, isDir: true, name: name})
				next = append(next, c)
			}
			a.learnBootstrap(edges)
		}
		level = next
	}
	return read, nil
}

// scanAll walks every d<ino> hash and records directory edges not yet known.
func (a *ancestry) scanAll(workers int, stop <-chan struct{}) (keys, added int, err error) {
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan uint64, 1024)
	var nKeys, nAdded atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				if firstErr.Load() != nil {
					continue
				}
				start := a.tomb.now()
				kids, err := a.dirEntries(d)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					continue
				}
				nKeys.Add(1)
				if len(kids) == 0 {
					continue
				}
				n := 0
				a.mu.Lock()
				for _, c := range kids {
					if isTrashIno(c) {
						continue
					}
					if _, ok := a.dirParent[c]; !ok && a.tomb.fresh(c, start) {
						a.dirParent[c] = d
						n++
					}
				}
				a.mu.Unlock()
				nAdded.Add(int64(n))
			}
		}()
	}
	cursor := "0"
	stopped := false
	for {
		select {
		case <-stop:
			stopped = true
		default:
		}
		if stopped || firstErr.Load() != nil {
			break
		}
		a.sideReads.Add(1)
		v, err := a.side.Do(a.db, strArgs("SCAN", cursor, "MATCH", "d*", "COUNT", "2000")...)
		if err == nil {
			err = v.Err()
		}
		if err != nil {
			firstErr.CompareAndSwap(nil, err)
			break
		}
		if len(v.Elems) != 2 {
			firstErr.CompareAndSwap(nil, errProtocol)
			break
		}
		cursor = string(v.Elems[0].Str)
		for _, k := range v.Elems[1].Elems {
			ref := ParseKey(string(k.Str))
			if ref.Shape == ShapeDentry && !ref.Trash {
				jobs <- ref.Ino
			}
		}
		if cursor == "0" {
			break
		}
	}
	close(jobs)
	wg.Wait()
	if e, ok := firstErr.Load().(error); ok {
		err = e
	}
	if stopped && err == nil {
		err = errClose
	}
	return int(nKeys.Load()), int(nAdded.Load()), err
}
