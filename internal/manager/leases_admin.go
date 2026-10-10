package manager

// leases_admin.go: H-8 option B project leases from the Manager's side
// (H8_OPTION_B_PLAN.md §1.3).
//
//   - LeaseClient is the proxy's JM.LEASE surface (lane L2). The Manager only
//     depends on this interface; RedisLeaseClient speaks it over RESP, and
//     tests use a fake. Until L2 is deployed the proxy answers "unknown
//     command", which maps to ErrLeasesUnsupported.
//   - ProjectLeaser takes a checkout lease on every project a Manager sync or
//     trash restore writes into, BEFORE the first write, all or none. A
//     project another principal holds refuses the operation
//     (*ProjectLeasedError) instead of letting the proxy refuse it half way.
//   - leasesAdmin serves the leases admin view and break (admin principal).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrLeasesUnsupported: the endpoint has no lease service (direct Redis,
	// or a proxy without the L2 lease engine). Writes are not fenced there.
	ErrLeasesUnsupported = errors.New("lease service not available on this endpoint")
	// ErrProjectAbsent: the project directory does not exist yet. Creating it
	// grants the creator an implicit lease, so no checkout is needed.
	ErrProjectAbsent = errors.New("project does not exist yet")
)

// projectDepth is jmlease:v1:cfg depth: a project is the directory two
// levels below the volume root (/data/<Project>).
const projectDepth = 2

// LeaseGrant is a JM.LEASE ACQUIRE reply.
type LeaseGrant struct {
	Status       string // GRANTED | RENEWED | HELD
	Epoch        int64
	Holder       string // "<principal>@<host>"
	ExpiresMs    int64
	ForeignSince bool
}

// LeaseRecord is one JM.LEASE STATUS record.
type LeaseRecord struct {
	Proj            int64  `json:"proj"`
	Epoch           int64  `json:"epoch"`
	State           string `json:"state"`
	Holder          string `json:"holder"`
	Host            string `json:"host"`
	Kind            string `json:"kind"`
	Mode            string `json:"mode"`
	GrantedMs       int64  `json:"granted_ms"`
	LastWriteMs     int64  `json:"last_write_ms"`
	LastHbMs        int64  `json:"last_hb_ms"`
	CheckoutUntilMs int64  `json:"checkout_until_ms"`
	ExpiresMs       int64  `json:"expires_ms"`
	Pending         bool   `json:"pending"`
	Label           string `json:"label"`
}

// BreakResult is a JM.LEASE BREAK reply.
type BreakResult struct {
	Status         string `json:"status"` // BROKEN | FREE
	Epoch          int64  `json:"epoch"`
	PreviousHolder string `json:"previous_holder"`
}

// LeaseClient is the proxy's JM.LEASE command surface.
type LeaseClient interface {
	Acquire(ctx context.Context, proj, mode string, ttl time.Duration) (LeaseGrant, error)
	Release(ctx context.Context, proj string, epoch int64) (string, error)
	Status(ctx context.Context, proj string) ([]LeaseRecord, error)
	Break(ctx context.Context, proj string) (BreakResult, error)
}

// ProjectLeasedError refuses a Manager write into a project another
// principal holds.
type ProjectLeasedError struct {
	Project string // path:/<a>/<b>
	Holder  string // principal@host
}

func (e *ProjectLeasedError) Error() string {
	return fmt.Sprintf("project %s is being edited by %s; the write was not started (ask an admin to break the lease, or wait for release)",
		strings.TrimPrefix(e.Project, "path:"), e.Holder)
}

// ---- RESP client ----------------------------------------------------------

// RedisLeaseClient speaks JM.LEASE to the proxy, plus HGET/HSET on
// jmlease:v1:cfg for the mode switch (admin principal only).
type RedisLeaseClient struct {
	rdb *redis.Client
}

// NewRedisLeaseClient connects as the principal in metaURL.
func NewRedisLeaseClient(metaURL, _ string) (*RedisLeaseClient, error) {
	opts, err := redisurl.Parse(metaURL)
	if err != nil {
		return nil, err
	}
	return newRedisLeaseClient(opts), nil
}

// NewRedisLeaseClientAs connects to metaURL's endpoint as user/pass.
func NewRedisLeaseClientAs(metaURL, user, pass string) (*RedisLeaseClient, error) {
	opts, err := redisurl.Parse(metaURL)
	if err != nil {
		return nil, err
	}
	opts.Username, opts.Password = user, pass
	return newRedisLeaseClient(opts), nil
}

func newRedisLeaseClient(opts *redis.Options) *RedisLeaseClient {
	opts.Protocol = 2 // plain RESP2 arrays; the proxy speaks both
	opts.MaxRetries = 0
	opts.PoolSize = 4
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 10 * time.Second
	opts.WriteTimeout = 10 * time.Second
	opts.DisableIdentity = true
	return &RedisLeaseClient{rdb: redis.NewClient(opts)}
}

func classifyLeaseErr(err error) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return err
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unknown command"):
		return ErrLeasesUnsupported
	case strings.Contains(msg, "no such directory"):
		return ErrProjectAbsent
	}
	return err
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func (c *RedisLeaseClient) Acquire(ctx context.Context, proj, mode string, ttl time.Duration) (LeaseGrant, error) {
	args := []any{"JM.LEASE", "ACQUIRE", proj, mode}
	if ttl > 0 {
		args = append(args, strconv.FormatInt(int64(ttl/time.Second), 10))
	}
	v, err := c.rdb.Do(ctx, args...).Slice()
	if err != nil {
		return LeaseGrant{}, classifyLeaseErr(err)
	}
	if len(v) != 5 {
		return LeaseGrant{}, fmt.Errorf("JM.LEASE ACQUIRE: unexpected reply of %d elements", len(v))
	}
	return LeaseGrant{Status: toString(v[0]), Epoch: toInt64(v[1]), Holder: toString(v[2]),
		ExpiresMs: toInt64(v[3]), ForeignSince: toInt64(v[4]) == 1}, nil
}

func (c *RedisLeaseClient) Release(ctx context.Context, proj string, epoch int64) (string, error) {
	v, err := c.rdb.Do(ctx, "JM.LEASE", "RELEASE", proj, strconv.FormatInt(epoch, 10)).Slice()
	if err != nil {
		return "", classifyLeaseErr(err)
	}
	if len(v) < 1 {
		return "", errors.New("JM.LEASE RELEASE: empty reply")
	}
	return toString(v[0]), nil
}

func parseLeaseRecord(v any) (LeaseRecord, error) {
	arr, ok := v.([]any)
	if !ok || len(arr)%2 != 0 {
		return LeaseRecord{}, errors.New("JM.LEASE STATUS: record is not a field/value array")
	}
	var r LeaseRecord
	for i := 0; i < len(arr); i += 2 {
		val := arr[i+1]
		switch toString(arr[i]) {
		case "proj":
			r.Proj = toInt64(val)
		case "epoch":
			r.Epoch = toInt64(val)
		case "state":
			r.State = toString(val)
		case "holder":
			r.Holder = toString(val)
		case "host":
			r.Host = toString(val)
		case "kind":
			r.Kind = toString(val)
		case "mode":
			r.Mode = toString(val)
		case "granted_ms":
			r.GrantedMs = toInt64(val)
		case "last_write_ms":
			r.LastWriteMs = toInt64(val)
		case "last_hb_ms":
			r.LastHbMs = toInt64(val)
		case "checkout_until_ms":
			r.CheckoutUntilMs = toInt64(val)
		case "expires_ms":
			r.ExpiresMs = toInt64(val)
		case "pending":
			r.Pending = toInt64(val) == 1
		case "label":
			r.Label = toString(val)
		}
	}
	return r, nil
}

// Status returns one record for proj, or every lease when proj is "".
func (c *RedisLeaseClient) Status(ctx context.Context, proj string) ([]LeaseRecord, error) {
	if proj == "" {
		proj = "*"
	}
	v, err := c.rdb.Do(ctx, "JM.LEASE", "STATUS", proj).Slice()
	if err != nil {
		return nil, classifyLeaseErr(err)
	}
	if proj != "*" {
		r, err := parseLeaseRecord(v)
		if err != nil {
			return nil, err
		}
		return []LeaseRecord{r}, nil
	}
	out := make([]LeaseRecord, 0, len(v))
	for _, e := range v {
		r, err := parseLeaseRecord(e)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func (c *RedisLeaseClient) Break(ctx context.Context, proj string) (BreakResult, error) {
	v, err := c.rdb.Do(ctx, "JM.LEASE", "BREAK", proj).Slice()
	if err != nil {
		return BreakResult{}, classifyLeaseErr(err)
	}
	if len(v) != 3 {
		return BreakResult{}, fmt.Errorf("JM.LEASE BREAK: unexpected reply of %d elements", len(v))
	}
	return BreakResult{Status: toString(v[0]), Epoch: toInt64(v[1]), PreviousHolder: toString(v[2])}, nil
}

// leaseConfigKey is the proxy's mode switch (respproxy.ConfigKey), in the
// volume DB the metaURL selects.
const leaseConfigKey = "jmlease:v1:cfg"

func (c *RedisLeaseClient) GetMode(ctx context.Context) (string, error) {
	v, err := c.rdb.HGet(ctx, leaseConfigKey, "mode").Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func (c *RedisLeaseClient) SetMode(ctx context.Context, mode string) error {
	return c.rdb.HSet(ctx, leaseConfigKey, "mode", mode).Err()
}

// ---- project checkout -----------------------------------------------------

// ProjectLeaser checks out projects for a Manager write.
type ProjectLeaser interface {
	// Checkout acquires a checkout lease on every project, all or none.
	// release gives them back; lost is closed if a lease is lost while
	// held (the caller must stop writing).
	Checkout(ctx context.Context, projects []string) (release func(), lost <-chan struct{}, err error)
}

type checkoutLeaser struct {
	client LeaseClient
	ttl    time.Duration // checkout length; renewed every ttl/3
}

// NewProjectLeaser returns the production leaser: one-hour checkouts,
// renewed while the operation runs, so a crashed Manager blocks a project
// for at most an hour.
func NewProjectLeaser(c LeaseClient) ProjectLeaser {
	if c == nil {
		return nil
	}
	return &checkoutLeaser{client: c, ttl: time.Hour}
}

type heldLease struct {
	proj  string
	epoch int64
}

func (l *checkoutLeaser) Checkout(ctx context.Context, projects []string) (func(), <-chan struct{}, error) {
	projects = append([]string(nil), projects...)
	sort.Strings(projects) // fixed order: two Managers never deadlock-livelock
	var held []heldLease
	releaseAll := func() {
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, h := range held {
			if st, err := l.client.Release(rctx, h.proj, h.epoch); err != nil || st != "RELEASED" {
				log.Printf("manager: lease release %s epoch %d: %s %v", h.proj, h.epoch, st, err)
			}
		}
	}
	for _, p := range projects {
		g, err := l.client.Acquire(ctx, p, "checkout", l.ttl)
		switch {
		case errors.Is(err, ErrLeasesUnsupported):
			// No lease service on this endpoint (pre-L2 proxy or direct
			// Redis): nothing to fence with; the write proceeds as before.
			releaseAll()
			log.Printf("manager: no lease service; %d project(s) written without a checkout", len(projects))
			return func() {}, nil, nil
		case errors.Is(err, ErrProjectAbsent):
			continue // the write creates it and gets the implicit grant
		case err != nil:
			releaseAll()
			return nil, nil, fmt.Errorf("lease checkout %s: %w", p, err)
		}
		if g.Status != "GRANTED" && g.Status != "RENEWED" {
			releaseAll()
			return nil, nil, &ProjectLeasedError{Project: p, Holder: g.Holder}
		}
		held = append(held, heldLease{proj: p, epoch: g.Epoch})
	}
	lost := make(chan struct{})
	stop := make(chan struct{})
	var once sync.Once
	if len(held) > 0 {
		go func() {
			t := time.NewTicker(l.ttl / 3)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					for _, h := range held {
						rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						g, err := l.client.Acquire(rctx, h.proj, "checkout", l.ttl)
						cancel()
						if err == nil && g.Status != "GRANTED" && g.Status != "RENEWED" {
							log.Printf("manager: checkout of %s lost to %s; stopping the write", h.proj, g.Holder)
							once.Do(func() { close(lost) })
							return
						}
					}
				}
			}
		}()
	}
	var relOnce sync.Once
	release := func() {
		relOnce.Do(func() {
			close(stop)
			releaseAll()
		})
	}
	return release, lost, nil
}

// SetProjectLeaser makes sync jobs and trash restores check out their
// target projects first (nil disables it).
func (m *JobManager) SetProjectLeaser(l ProjectLeaser) {
	m.mu.Lock()
	m.spec.Leases = l
	m.mu.Unlock()
}

func (m *JobManager) projectLeaser() ProjectLeaser {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.spec.Leases
}

// projectOfVolumePath maps a volume path (/data/P/x) to path:/data/P, or ""
// when the path is above project depth.
func projectOfVolumePath(p string) string {
	parts := strings.FieldsFunc(filepath.Clean("/"+p), func(r rune) bool { return r == '/' })
	if len(parts) < projectDepth {
		return ""
	}
	return "path:/" + strings.Join(parts[:projectDepth], "/")
}

// projectsForSync lists the projects a sync of source into destination
// (user-facing /jfs/... path) writes. A destination inside a project is that
// project. A destination above project depth spreads the source's top-level
// entries (or the source itself without preserve-structure) into projects
// one level down. A destination outside /jfs writes no project.
//
// An unreadable source above project depth is an error, not "no projects":
// the sync would still write, so it must not run without its checkouts.
func projectsForSync(source, destination string, preserveStructure bool) ([]string, error) {
	dest := strings.TrimSpace(destination)
	if dest != "/jfs" && !strings.HasPrefix(dest, "/jfs/") {
		return nil, nil
	}
	vol := filepath.Clean("/" + strings.TrimPrefix(dest, "/jfs"))
	if p := projectOfVolumePath(vol); p != "" {
		return []string{p}, nil
	}
	parts := strings.FieldsFunc(vol, func(r rune) bool { return r == '/' })
	if len(parts) != projectDepth-1 {
		// The volume root itself: top-level names are not projects
		// (ns:root); the proxy governs those writes on its own.
		return nil, nil
	}
	var names []string
	src := strings.TrimSpace(source)
	if preserveStructure {
		ents, err := os.ReadDir(src)
		if err != nil {
			return nil, fmt.Errorf("listing the sync source to find its target projects: %w", err)
		}
		for _, e := range ents {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
	} else {
		names = []string{filepath.Base(filepath.Clean(src))}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, "path:/"+parts[0]+"/"+n)
	}
	sort.Strings(out)
	return out, nil
}

// checkoutForSync is RunSync's gate: it returns a context that is cancelled
// if a checkout is lost, and the release function.
func checkoutForSync(ctx context.Context, spec RunSyncSpec, source, destination string, opts SyncOptions) (context.Context, func(), error) {
	if spec.Leases == nil {
		return ctx, func() {}, nil
	}
	projects, err := projectsForSync(source, destination, opts.PreserveStructure)
	if err != nil {
		return ctx, nil, err
	}
	if len(projects) == 0 {
		return ctx, func() {}, nil
	}
	release, lost, err := spec.Leases.Checkout(ctx, projects)
	if err != nil {
		return ctx, nil, err
	}
	if lost == nil {
		return ctx, release, nil
	}
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-lost:
			cancel()
		case <-cctx.Done():
		}
	}()
	return cctx, func() { cancel(); release() }, nil
}

// ---- admin view -----------------------------------------------------------

type leasesAdmin struct {
	client LeaseClient
}

func (a *leasesAdmin) ready(w http.ResponseWriter) bool {
	if a == nil || a.client == nil {
		http.Error(w, "lease admin is not configured (set JM_META_ADMIN_PASSWORD to the proxy admin principal's secret)", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// handleLeases is GET /api/leases: every lease the proxy tracks.
func (a *leasesAdmin) handleLeases(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if !a.ready(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	recs, err := a.client.Status(ctx, "")
	if errors.Is(err, ErrLeasesUnsupported) {
		writeJSON(w, http.StatusOK, map[string]any{"available": false, "leases": []LeaseRecord{},
			"error": "the proxy has no lease service yet (passthrough-only build)"})
		return
	}
	if err != nil {
		http.Error(w, "lease status failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{"available": true, "leases": recs})
}

func validLeaseProject(s string) bool {
	if strings.HasPrefix(s, "ino:") {
		_, err := strconv.ParseUint(s[4:], 10, 64)
		return err == nil
	}
	if strings.HasPrefix(s, "path:/") {
		_, err := url.PathUnescape(s) // reject nothing valid; just bound it
		return err == nil && len(s) < 4096
	}
	return false
}

// handleLeaseBreak is POST /api/leases/break {project, confirm}. confirm
// must repeat the project: breaking a lease lets another principal write
// over the holder's unsynced work (it becomes a conflict copy on the Mac).
func (a *leasesAdmin) handleLeaseBreak(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !a.ready(w) {
		return
	}
	var body struct {
		Project string `json:"project"`
		Confirm string `json:"confirm"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8192)).Decode(&body); err != nil || !validLeaseProject(body.Project) {
		http.Error(w, "project must be ino:<n> or path:/<a>/<b>", http.StatusBadRequest)
		return
	}
	if body.Confirm != body.Project {
		http.Error(w, "confirm must repeat the project to break its lease", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	res, err := a.client.Break(ctx, body.Project)
	if err != nil {
		http.Error(w, "lease break failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	log.Printf("manager: lease on %s broken (%s, previous holder %q, epoch %d)", body.Project, res.Status, res.PreviousHolder, res.Epoch)
	writeJSON(w, http.StatusOK, res)
}
