package manager

// Read-only estimate of what a Compact sweep (juicefs gc --compact) would
// repack. juicefs visits every chunk whose slice list holds 2 or more slices
// (pkg/meta/redis.go scanAllChunks: cnt > 1), then skips leading slices that
// are already big (pkg/meta/slice.go skipSome: at least 1 MiB and at least a
// fifth of what follows, and not overwritten later) and repacks the rest
// when 2 or more remain. The estimate applies the same rule to the raw slice
// list; it ignores the finer overlap checks of skipSome, so it can differ
// slightly for files edited in place. It reads the metadata only (SCAN
// c*_*, LLEN, LRANGE, as principal bg, which the sweep itself uses) and
// writes nothing.
//
// The bytes are the repacked ranges: what a Mac away from the NAS would
// download again, once, after the sweep, if it had those parts cached or
// pinned.

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

// compactEstimate is the API shape of GET /api/maintenance/compact-meta/estimate.
type compactEstimate struct {
	ChunksScanned    int64 `json:"chunks_scanned"`
	ChunksMultiSlice int64 `json:"chunks_multi_slice"` // slice count >= 2: the sweep looks at these
	ChunksRepack     int64 `json:"chunks_repack"`      // ... and repacks these
	SlicesRepack     int64 `json:"slices_repack"`      // slices it merges
	BytesRepack      int64 `json:"bytes_repack"`       // repacked ranges: the most a remote Mac re-downloads
	FilesRepack      int64 `json:"files_repack"`       // distinct files with one or more repacked chunks
	// Buckets of repacked chunks by slice count: "2-4", "5-19", "20-98", "99+".
	Buckets   map[string]int64 `json:"buckets"`
	ElapsedMS int64            `json:"elapsed_ms"`
}

const sliceRecordBytes = 24 // pos u32, id u64, size u32, off u32, len u32 (pkg/meta/slice.go)

func sliceBucket(n int64) string {
	switch {
	case n >= 99:
		return "99+"
	case n >= 20:
		return "20-98"
	case n >= 5:
		return "5-19"
	default:
		return "2-4"
	}
}

type sliceSpan struct{ pos, len int64 }

func decodeSlices(vals []string) []sliceSpan {
	out := make([]sliceSpan, 0, len(vals))
	for _, v := range vals {
		if len(v) != sliceRecordBytes {
			continue
		}
		b := []byte(v)
		out = append(out, sliceSpan{
			pos: int64(binary.BigEndian.Uint32(b[0:4])),
			len: int64(binary.BigEndian.Uint32(b[20:24])),
		})
	}
	return out
}

// extent is the range from the first byte to the last byte the slices cover.
func extent(ss []sliceSpan) (start, size int64) {
	if len(ss) == 0 {
		return 0, 0
	}
	start, end := ss[0].pos, ss[0].pos+ss[0].len
	for _, s := range ss[1:] {
		start = min(start, s.pos)
		end = max(end, s.pos+s.len)
	}
	return start, end - start
}

// repackPlan applies juicefs's skipSome rule (see the file comment) and
// returns how many slices the sweep merges and the size of the merged range;
// (0, 0) when it leaves the chunk alone.
func repackPlan(ss []sliceSpan) (slices int, size int64) {
	skipped := 0
	for skipped < len(ss) {
		rest := ss[skipped:]
		first := rest[0]
		_, sz := extent(rest)
		if first.len < 1<<20 || first.len*5 < sz || sz == 0 {
			break
		}
		overwritten := false
		for _, s := range rest[1:] {
			if s.pos < first.pos+first.len && first.pos < s.pos+s.len {
				overwritten = true
				break
			}
		}
		if overwritten {
			break
		}
		skipped++
	}
	rest := ss[skipped:]
	if len(rest) < 2 {
		return 0, 0
	}
	_, size = extent(rest)
	return len(rest), size
}

// estimateCompaction scans the volume's chunk keys. Read-only.
func estimateCompaction(ctx context.Context, rdb redis.UniversalClient) (compactEstimate, error) {
	start := time.Now()
	est := compactEstimate{Buckets: map[string]int64{"2-4": 0, "5-19": 0, "20-98": 0, "99+": 0}}
	files := make(map[uint64]struct{})
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "c*_*", 1000).Result()
		if err != nil {
			return est, fmt.Errorf("scan chunks: %w", err)
		}
		var inodes []uint64
		var chunkKeys []string
		for _, k := range keys {
			var ino uint64
			var indx uint32
			if n, err := fmt.Sscanf(k, "c%d_%d", &ino, &indx); err == nil && n == 2 {
				chunkKeys = append(chunkKeys, k)
				inodes = append(inodes, ino)
			}
		}
		if len(chunkKeys) > 0 {
			p := rdb.Pipeline()
			lens := make([]*redis.IntCmd, len(chunkKeys))
			for i, k := range chunkKeys {
				lens[i] = p.LLen(ctx, k)
			}
			if _, err := p.Exec(ctx); err != nil {
				return est, fmt.Errorf("count slices: %w", err)
			}
			p = rdb.Pipeline()
			var ranges []*redis.StringSliceCmd
			var rangeIno []uint64
			for i, c := range lens {
				est.ChunksScanned++
				if c.Val() < 2 {
					continue
				}
				est.ChunksMultiSlice++
				ranges = append(ranges, p.LRange(ctx, chunkKeys[i], 0, -1))
				rangeIno = append(rangeIno, inodes[i])
			}
			if len(ranges) > 0 {
				if _, err := p.Exec(ctx); err != nil {
					return est, fmt.Errorf("read slices: %w", err)
				}
				for i, r := range ranges {
					n, size := repackPlan(decodeSlices(r.Val()))
					if n == 0 {
						continue
					}
					est.ChunksRepack++
					est.SlicesRepack += int64(n)
					est.BytesRepack += size
					est.Buckets[sliceBucket(int64(n))]++
					files[rangeIno[i]] = struct{}{}
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return est, err
		}
	}
	est.FilesRepack = int64(len(files))
	est.ElapsedMS = time.Since(start).Milliseconds()
	return est, nil
}

// bgRedisOptions: the sweep's own connection (principal bg, password from
// the env the juicefs subprocesses get).
func (mm *MaintenanceManager) bgRedisOptions() (*redis.Options, error) {
	opts, err := redisurl.Parse(mm.metaURL)
	if err != nil {
		return nil, err
	}
	for _, e := range mm.metaEnv {
		if v, ok := strings.CutPrefix(e, "META_PASSWORD="); ok {
			opts.Password = v
		}
	}
	opts.Protocol = 2
	opts.DisableIdentity = true
	opts.MaxRetries = 0
	opts.PoolSize = 2
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 30 * time.Second
	opts.WriteTimeout = 30 * time.Second
	return opts, nil
}

// handleCompactEstimate is GET /api/maintenance/compact-meta/estimate: how
// many chunks, files and bytes the next Compact sweep would repack. Scans
// the whole chunk keyspace; on a big volume it takes a while (30 min cap).
func (a *API) handleCompactEstimate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET", http.StatusMethodNotAllowed)
		return
	}
	if a.maintenance == nil || a.maintenance.metaURL == "" {
		http.Error(w, "estimate requires metaURL", http.StatusNotImplemented)
		return
	}
	opts, err := a.maintenance.bgRedisOptions()
	if err != nil {
		http.Error(w, "metadata URL: "+err.Error(), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	est, err := estimateCompaction(ctx, rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, est)
}
