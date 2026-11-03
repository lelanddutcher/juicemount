package manager

// quotascope.go: the quota scope rule of per-person grants. While grants
// are in force a quota exists only on project folders and below: a quota
// above them (on the volume root or a client folder) sums the writes of
// several projects, so a person limited to one of them could not flush
// its usage without being able to move it for all of them. The proxy
// refuses a limited person's quota flush that names such a quota (all of
// it: juicefs flushes every quota in one transaction, so their per-project
// quota usage stops updating too) and refuses creating one while grants
// are in force. The Manager does not put grants in force while one exists,
// and reports one that exists anyway (created before an upgrade, or
// directly in Redis).

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

// quotaScopeFunc lists the folders above the project depth that have a
// quota, by path.
type quotaScopeFunc func(ctx context.Context) ([]string, error)

// redisQuotaScope reads the volume's quotas through metaURL.
func redisQuotaScope(metaURL string) quotaScopeFunc {
	return func(ctx context.Context) ([]string, error) {
		opts, err := redisurl.Parse(metaURL)
		if err != nil {
			return nil, err
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		return quotasAboveProjects(ctx, rdb)
	}
}

// noQuotaScope is the check of a Manager with no metadata URL: it cannot
// read the volume.
func noQuotaScope(context.Context) ([]string, error) {
	return nil, errors.New("the Manager has no metadata URL (--meta, or --overview-meta / JM_OVERVIEW_META with --fuse-mount)")
}

// quotasAboveProjects lists the quotas (dirQuota fields) on folders above
// the project depth: the depth the proxy uses, read from the same field
// with the same rule (respproxy.ProjectDepth; jmlease:v1:cfg "depth"). The
// proxy fixes its depth when it starts, so a depth changed since then
// applies to both only after the proxy restarts. A quota row whose folder
// no longer exists is skipped.
func quotasAboveProjects(ctx context.Context, rdb redis.Cmdable) ([]string, error) {
	dv, err := rdb.HGet(ctx, respproxy.ConfigKey, "depth").Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	depth := respproxy.ProjectDepth(dv)
	fields, err := rdb.HKeys(ctx, "dirQuota").Result()
	if err != nil {
		return nil, err
	}
	names := map[uint64]map[uint64]string{} // directory -> child inode -> name
	nameIn := func(dir, child uint64) (string, error) {
		m, ok := names[dir]
		if !ok {
			ents, err := rdb.HGetAll(ctx, "d"+strconv.FormatUint(dir, 10)).Result()
			if err != nil {
				return "", err
			}
			m = map[uint64]string{}
			for n, v := range ents {
				if len(v) >= 9 {
					m[binary.BigEndian.Uint64([]byte(v[1:9]))] = n
				}
			}
			names[dir] = m
		}
		if n, ok := m[child]; ok {
			return n, nil
		}
		return "inode " + strconv.FormatUint(child, 10), nil
	}
	var out []string
	for _, f := range fields {
		ino, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			continue
		}
		// The chain up to the root, if it is shorter than depth.
		chain := []uint64{}
		cur, gone := ino, false
		for cur != 1 && len(chain) < depth {
			attr, err := rdb.Get(ctx, "i"+strconv.FormatUint(cur, 10)).Bytes()
			if err == redis.Nil {
				gone = true
				break
			}
			if err != nil {
				return nil, err
			}
			// pkg/meta Attr.Marshal: Parent is the big-endian uint64 at 63.
			if len(attr) < 71 {
				return nil, fmt.Errorf("inode %d: attribute of %d bytes", cur, len(attr))
			}
			chain = append(chain, cur)
			cur = binary.BigEndian.Uint64(attr[63:71])
		}
		if gone || cur != 1 || len(chain) >= depth {
			continue // at or below the project depth, or no longer there
		}
		path := ""
		parent := uint64(1)
		for i := len(chain) - 1; i >= 0; i-- {
			n, err := nameIn(parent, chain[i])
			if err != nil {
				return nil, err
			}
			path += "/" + n
			parent = chain[i]
		}
		if path == "" {
			path = "/"
		}
		out = append(out, path)
	}
	sort.Strings(out)
	return out, nil
}

// quotaScopeMessage explains quotas above the project folders and how to
// remove them.
func quotaScopeMessage(paths []string) string {
	cmds := make([]string, 0, len(paths))
	for _, p := range paths {
		cmds = append(cmds, "juicefs quota delete <META-URL> --path "+strconv.Quote(p))
	}
	return fmt.Sprintf("quotas above the project folders are not supported while per-person grants are in force: %s has a quota "+
		"(remove it with %s; quotas on project folders and below keep working)",
		strings.Join(paths, ", "), strings.Join(cmds, "; "))
}

// quotaScopeTimeout bounds one check.
var quotaScopeTimeout = 5 * time.Second

// SetQuotaScope installs the check the store runs before it puts grants in
// force (nil: none).
func (s *PrincipalStore) SetQuotaScope(fn quotaScopeFunc) {
	s.mu.Lock()
	s.quotaScope = fn
	s.mu.Unlock()
}

// checkQuotaScopeLocked refuses to put grants in force while a quota
// exists above the project folders, or while that cannot be checked.
func (s *PrincipalStore) checkQuotaScopeLocked() error {
	if s.quotaScope == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), quotaScopeTimeout)
	defer cancel()
	paths, err := s.quotaScope(ctx)
	switch {
	case err != nil:
		return fmt.Errorf("%w: grants were not put in force: the Manager cannot check the volume for quotas above the project folders (%v)",
			ErrPersonInvalid, err)
	case len(paths) > 0:
		return fmt.Errorf("%w: grants were not put in force: %s", ErrPersonInvalid, quotaScopeMessage(paths))
	}
	return nil
}

// quotaScopeCache keeps the last check for the People page (at most one
// read of the volume per quotaScopeEvery).
type quotaScopeCache struct {
	fn quotaScopeFunc

	mu    sync.Mutex
	at    time.Time
	paths []string
	err   error
}

var quotaScopeEvery = 30 * time.Second

func (c *quotaScopeCache) get() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at.IsZero() || time.Since(c.at) > quotaScopeEvery {
		ctx, cancel := context.WithTimeout(context.Background(), quotaScopeTimeout)
		c.paths, c.err = c.fn(ctx)
		cancel()
		c.at = time.Now()
	}
	return c.paths, c.err
}
