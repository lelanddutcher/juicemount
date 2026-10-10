// Package mutationbackend fences a dedicated local Redis backend before an
// in-process filesystem acquires its runtime credentials. It never issues media
// publication receipts and cannot replace deployment credential isolation.
package mutationbackend

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/privatefile"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

var ErrUnavailable = errors.New("backend authority unavailable; process restart and reconciliation required")

const ControlUser = "jmgw_control"

// RedisConfig refers only to dedicated same-host resources. The control secret
// and static ACL baseline are private files. No Redis URL or ambient credentials
// are accepted. The Redis daemon and these resources must remain exclusively
// controlled by the deployment; another holder of the control secret is unsafe.
type RedisConfig struct {
	Socket     string `json:"socket"`
	ACLFile    string `json:"acl_file"`
	SecretFile string `json:"control_secret_file"`
}

// Credentials are supplied only to the trusted, in-process SDK constructor.
// The administrative credential is never included or inherited via environment.
type Credentials struct {
	socket, username, password string
}

func (c Credentials) MetadataURL() string {
	u := url.URL{Scheme: "unix", Path: c.socket, User: url.UserPassword(c.username, c.password)}
	q := u.Query()
	q.Set("read_timeout", "1s")
	q.Set("write_timeout", "1s")
	u.RawQuery = q.Encode()
	return u.String()
}

// Fence keeps one non-reconnecting control connection. Any failed check
// permanently freezes admission. Closing it never releases the journal pin.
type Fence struct {
	mu          sync.Mutex
	cfg         RedisConfig
	client      *redis.Client
	conn        *redis.Conn
	controlHash string
	baseline    string
	runID       string
	clientID    string
	username    string
	workerHash  string
	process     mutationauthority.BackendProcess
	failed      bool
}

// StaticACL is the only accepted startup ACL file. Runtime worker identities
// must never be persisted into it. Provisioning it belongs to deployment setup,
// not to startup recovery; startup refuses changed content instead of repairing.
func StaticACL(secret string) (string, error) {
	if !lowerHex(secret, 64) {
		return "", mutationauthority.ErrInvalid
	}
	h := sha256.Sum256([]byte(secret))
	return fmt.Sprintf("user default reset off -@all\nuser %s reset on #%x ~* &* +@all\n", ControlUser, h), nil
}

// Open first irreversibly pins the SAME ownership journal, then fences old
// control connections, verifies the static baseline again, and revokes every
// recorded old worker. New credentials are journaled before ACL provisioning.
// A failed attempt cannot be retried in this process or release its journal.
func Open(ctx context.Context, j *mutationauthority.Journal, volume string, cfg RedisConfig) (_ *Fence, _ Credentials, err error) {
	if j == nil || volume == "" || !filepath.IsAbs(cfg.Socket) || !filepath.IsAbs(cfg.ACLFile) || !filepath.IsAbs(cfg.SecretFile) || cfg.ACLFile == cfg.SecretFile {
		return nil, Credentials{}, mutationauthority.ErrInvalid
	}
	if err := j.BeginBackendProcess(); err != nil {
		return nil, Credentials{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	secretBytes, err := privateRead(cfg.SecretFile, 65)
	if err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	secret := strings.TrimSuffix(string(secretBytes), "\n")
	baseline, err := StaticACL(secret)
	if err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	h := sha256.Sum256([]byte(secret))
	f := &Fence{cfg: cfg, controlHash: hex.EncodeToString(h[:]), baseline: baseline}
	defer func() {
		if err != nil {
			f.Freeze()
		}
	}()
	if err := f.checkFile(baseline); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := privateSocket(cfg.Socket); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	var dialed atomic.Bool
	f.client = redis.NewClient(&redis.Options{Network: "unix", Addr: cfg.Socket, Username: ControlUser, Password: secret,
		Protocol: 2, DisableIdentity: true, MaxRetries: -1, PoolSize: 1, MaxActiveConns: 1,
		ContextTimeoutEnabled: true, ReadTimeout: time.Second, WriteTimeout: time.Second,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			if !dialed.CompareAndSwap(false, true) {
				return nil, ErrUnavailable
			}
			return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", cfg.Socket)
		}})
	f.conn = f.client.Conn()
	value, err := f.command(ctx, "CLIENT", "ID")
	if err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	id, ok := value.(int64)
	if !ok || id <= 0 {
		return nil, Credentials{}, ErrUnavailable
	}
	f.clientID = fmt.Sprint(id)
	if err := f.server(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.acl(ctx, "default", false, "", "-@all"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.acl(ctx, ControlUser, true, f.controlHash, "+@all"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.effectiveACLFile(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	previous, err := j.BackendProcesses(ctx, volume)
	if err != nil {
		return nil, Credentials{}, err
	}
	allowed := map[string]bool{"default": true, ControlUser: true}
	for _, p := range previous {
		if p.RedisRunID != f.runID {
			return nil, Credentials{}, ErrUnavailable
		}
		allowed[p.Username] = true
	}
	if err := f.users(ctx, allowed); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	// The journal pin proves the prior cooperating process cannot still run.
	// This removes its surviving privileged sockets before any new provision.
	if _, err := f.command(ctx, "CLIENT", "KILL", "USER", ControlUser, "SKIPME", "yes"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.waitOnlyControl(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	// Old queued ACL SAVE/CONFIG effects could have completed before the kill.
	// Re-read after their sockets disappear; never silently repair this state.
	if err := f.checkFile(baseline); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.server(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.effectiveACLFile(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.acl(ctx, "default", false, "", "-@all"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.acl(ctx, ControlUser, true, f.controlHash, "+@all"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.users(ctx, allowed); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	rows, err := f.clients(ctx)
	if err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	oldIDs := map[string]bool{}
	for _, row := range rows {
		if allowed[row["user"]] && row["user"] != "default" && row["user"] != ControlUser {
			oldIDs[row["id"]] = true
		}
	}
	for _, p := range previous {
		if _, err := f.command(ctx, "ACL", "DELUSER", p.Username); err != nil {
			return nil, Credentials{}, ErrUnavailable
		}
	}
	if err := f.waitClientsGone(ctx, oldIDs); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	// A worker may have connected after the ID snapshot but before DELUSER.
	// No SDK connection exists yet, so the sole permitted survivor is control.
	if err := f.waitSoleClient(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.users(ctx, map[string]bool{"default": true, ControlUser: true}); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.server(ctx); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	if err := f.acl(ctx, "default", false, "", "-@all"); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	var random [64]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	username := "jmgw_" + hex.EncodeToString(random[:32])
	password := hex.EncodeToString(random[32:])
	if f.process, err = j.RecordBackendProcess(ctx, volume, username, f.runID); err != nil {
		return nil, Credentials{}, err
	}
	args := []any{"ACL", "SETUSER", username, "reset", "on", ">" + password, "~*", "&*"}
	for _, rule := range workerRules {
		args = append(args, rule)
	}
	if _, err := f.command(ctx, args...); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	f.username = username
	workerHash := sha256.Sum256([]byte(password))
	f.workerHash = hex.EncodeToString(workerHash[:])
	if err := f.check(ctx, baseline); err != nil {
		return nil, Credentials{}, ErrUnavailable
	}
	return f, Credentials{cfg.Socket, username, password}, nil
}

// Explicit command names avoid the future-command grant implicit in +@all.
// SDK metadata operations may use these keys/transactions/scripts only.
var workerRules = []string{
	"+auth", "+hello", "+ping", "+select", "+client|id", "+client|setinfo", "+info",
	"+get", "+getrange", "+set", "+setnx", "+del", "+exists", "+expire", "+pexpire", "+ttl", "+pttl",
	"+incr", "+incrby", "+decrby", "+keys", "+scan", "+type", "+mget", "+mset", "+unlink",
	"+hget", "+hset", "+hsetnx", "+hgetall", "+hdel", "+hexists", "+hlen", "+hkeys", "+hvals", "+hincrby", "+hmget", "+hmset", "+hscan",
	"+sadd", "+srem", "+smembers", "+sismember", "+scard", "+spop", "+sscan",
	"+zadd", "+zrem", "+zrange", "+zrangebyscore", "+zrevrangebyscore", "+zremrangebyscore", "+zcard", "+zcount", "+zscore", "+zincrby", "+zscan",
	"+lpush", "+rpush", "+lpop", "+rpop", "+lrange", "+llen", "+ltrim", "+lset",
	"+watch", "+unwatch", "+multi", "+exec", "+discard", "+eval", "+evalsha", "+script|load", "+script|exists", "+time",
}

// Check is required before admitting any backend operation. A control loss,
// restart, ACL change or check failure is permanent for this process.
func (f *Fence) Check(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	secret, err := privateRead(f.cfg.SecretFile, 65)
	if err != nil {
		f.failed = true
		return ErrUnavailable
	}
	baseline, err := StaticACL(strings.TrimSuffix(string(secret), "\n"))
	if err == nil && baseline != f.baseline {
		err = ErrUnavailable
	}
	if err == nil {
		err = f.check(ctx, baseline)
	}
	if err != nil {
		f.failed = true
		return ErrUnavailable
	}
	return nil
}

func (f *Fence) check(ctx context.Context, baseline string) error {
	if err := f.server(ctx); err != nil {
		return err
	}
	if err := f.onlyControl(ctx); err != nil {
		return err
	}
	if err := f.checkFile(baseline); err != nil {
		return err
	}
	if err := f.effectiveACLFile(ctx); err != nil {
		return err
	}
	if err := f.users(ctx, map[string]bool{"default": true, ControlUser: true, f.username: true}); err != nil {
		return err
	}
	if err := f.acl(ctx, "default", false, "", "-@all"); err != nil {
		return err
	}
	if err := f.acl(ctx, ControlUser, true, f.controlHash, "+@all"); err != nil {
		return err
	}
	// Redis normalizes command order, so compare the effective grant against
	// a sorted explicit allowlist rather than allowing a broad ACL category.
	return f.acl(ctx, f.username, true, f.workerHash, "-@all "+strings.Join(workerRules, " "))
}

func (f *Fence) Freeze() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = true
	if f.conn != nil {
		_ = f.conn.Close()
	}
	if f.client != nil {
		_ = f.client.Close()
	}
}

func (f *Fence) command(ctx context.Context, args ...any) (any, error) {
	if f.failed || f.conn == nil {
		return nil, ErrUnavailable
	}
	cmd := redis.NewCmd(ctx, args...)
	if err := f.conn.Process(ctx, cmd); err != nil {
		return nil, ErrUnavailable // Never expose a Redis reply containing credentials.
	}
	return cmd.Result()
}

func (f *Fence) server(ctx context.Context) error {
	v, err := f.command(ctx, "INFO", "server", "replication", "cluster")
	info, ok := v.(string)
	if err != nil || !ok {
		return ErrUnavailable
	}
	fields := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok {
			fields[k] = v
		}
	}
	if fields["redis_version"] != "7.4.11" || fields["redis_mode"] != "standalone" || fields["role"] != "master" || fields["connected_slaves"] != "0" || fields["cluster_enabled"] != "0" || !lowerHex(fields["run_id"], 40) {
		return ErrUnavailable
	}
	if f.runID != "" && f.runID != fields["run_id"] {
		return ErrUnavailable
	}
	f.runID = fields["run_id"]
	v, err = f.command(ctx, "MODULE", "LIST")
	modules, ok := v.([]any)
	if err != nil || !ok || len(modules) != 0 {
		return ErrUnavailable
	}
	return nil
}

func (f *Fence) clients(ctx context.Context) ([]map[string]string, error) {
	v, err := f.command(ctx, "CLIENT", "LIST")
	text, ok := v.(string)
	if err != nil || !ok || len(text) > 1<<20 {
		return nil, ErrUnavailable
	}
	var rows []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		row := map[string]string{}
		for _, field := range strings.Fields(line) {
			k, v, ok := strings.Cut(field, "=")
			if ok {
				row[k] = v
			}
		}
		if row["id"] == "" || row["user"] == "" {
			return nil, ErrUnavailable
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (f *Fence) onlyControl(ctx context.Context) error {
	self, others, err := f.controlClients(ctx)
	if err != nil || !self || others {
		return ErrUnavailable
	}
	return nil
}

func (f *Fence) controlClients(ctx context.Context) (self, others bool, err error) {
	rows, err := f.clients(ctx)
	if err != nil {
		return false, false, err
	}
	for _, r := range rows {
		if r["user"] == ControlUser {
			if r["id"] != f.clientID {
				others = true
			} else {
				self = true
			}
		}
	}
	return self, others, nil
}

func (f *Fence) waitOnlyControl(ctx context.Context) error {
	for {
		self, others, err := f.controlClients(ctx)
		if err != nil || !self {
			return ErrUnavailable
		}
		if !others {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrUnavailable
		case <-time.After(time.Millisecond):
		}
	}
}

func (f *Fence) waitClientsGone(ctx context.Context, ids map[string]bool) error {
	for {
		rows, err := f.clients(ctx)
		if err != nil {
			return err
		}
		remaining := false
		for _, row := range rows {
			remaining = remaining || ids[row["id"]]
		}
		if !remaining {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrUnavailable
		case <-time.After(time.Millisecond):
		}
	}
}

func (f *Fence) waitSoleClient(ctx context.Context) error {
	for {
		rows, err := f.clients(ctx)
		if err != nil {
			return err
		}
		if len(rows) == 1 && rows[0]["id"] == f.clientID && rows[0]["user"] == ControlUser {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrUnavailable
		case <-time.After(time.Millisecond):
		}
	}
}

func (f *Fence) users(ctx context.Context, allowed map[string]bool) error {
	v, err := f.command(ctx, "ACL", "USERS")
	users, ok := v.([]any)
	if err != nil || !ok {
		return ErrUnavailable
	}
	for _, v := range users {
		u, ok := v.(string)
		if !ok || !allowed[u] {
			return ErrUnavailable
		}
	}
	return nil
}

func (f *Fence) acl(ctx context.Context, user string, enabled bool, passwordHash, commands string) error {
	v, err := f.command(ctx, "ACL", "GETUSER", user)
	row, ok := v.([]any)
	if err != nil || !ok || len(row)%2 != 0 {
		return ErrUnavailable
	}
	m := map[string]any{}
	for i := 0; i < len(row); i += 2 {
		k, ok := row[i].(string)
		if !ok {
			return ErrUnavailable
		}
		m[k] = row[i+1]
	}
	flags, ok := m["flags"].([]any)
	if !ok {
		return ErrUnavailable
	}
	gotState := false
	for _, flag := range flags {
		if flag == "nopass" || flag == "on" && !enabled || flag == "off" && enabled {
			return ErrUnavailable
		}
		gotState = gotState || flag == "on" && enabled || flag == "off" && !enabled
	}
	passwords, ok := m["passwords"].([]any)
	if !ok || !gotState || (passwordHash == "" && len(passwords) != 0) || (passwordHash != "" && (len(passwords) != 1 || passwords[0] != passwordHash)) {
		return ErrUnavailable
	}
	selectors, ok := m["selectors"].([]any)
	if !ok || len(selectors) != 0 {
		return ErrUnavailable
	}
	actual, ok := m["commands"].(string)
	if !ok || normalized(actual) != normalized(commands) {
		return ErrUnavailable
	}
	if enabled && (m["keys"] != "~*" || m["channels"] != "&*") {
		return ErrUnavailable
	}
	return nil
}

func normalized(s string) string {
	parts := strings.Fields(s)
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func (f *Fence) effectiveACLFile(ctx context.Context) error {
	v, err := f.command(ctx, "CONFIG", "GET", "aclfile")
	parts, ok := v.([]any)
	if err != nil || !ok || len(parts) != 2 || parts[0] != "aclfile" || parts[1] != f.cfg.ACLFile {
		return ErrUnavailable
	}
	return nil
}

func (f *Fence) checkFile(want string) error {
	got, err := privateRead(f.cfg.ACLFile, 4096)
	if err != nil || string(got) != want {
		return ErrUnavailable
	}
	return nil
}

func lowerHex(s string, size int) bool {
	if len(s) != size || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func privateRead(path string, limit int64) ([]byte, error) {
	if err := privateParent(path); err != nil {
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	var parent unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &parent); err != nil || parent.Uid != uint32(os.Getuid()) || parent.Mode&0077 != 0 {
		return nil, ErrUnavailable
	}
	file, err := privatefile.OpenAt(dir, filepath.Base(path), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var st unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Uid != uint32(os.Getuid()) || st.Nlink != 1 {
		return nil, ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, ErrUnavailable
	}
	return data, nil
}

func privateSocket(path string) error {
	if err := privateParent(path); err != nil {
		return err
	}
	fi, err := os.Lstat(path)
	if err != nil || fi.Mode()&os.ModeSocket == 0 || fi.Mode().Perm()&0077 != 0 {
		return ErrUnavailable
	}
	st, ok := fi.Sys().(*unix.Stat_t)
	if !ok {
		// os.FileInfo.Sys uses syscall.Stat_t on some platforms; inspect via
		// Lstat directly so the ownership check has one portable representation.
		var raw unix.Stat_t
		if err := unix.Lstat(path, &raw); err != nil {
			return ErrUnavailable
		}
		st = &raw
	}
	if st.Uid != uint32(os.Getuid()) {
		return ErrUnavailable
	}
	return nil
}

// The same-user deployment owns the directories for the entire process
// lifetime. Refuse symlinked or other-user-writable ancestors before any secret
// is sent; leaf-only checks would permit redirecting the authenticated socket.
func privateParent(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrUnavailable
	}
	parent := filepath.Dir(path)
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || canonical != parent {
		return ErrUnavailable
	}
	for p := parent; ; p = filepath.Dir(p) {
		var st unix.Stat_t
		if err := unix.Lstat(p, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Mode&0022 != 0 || st.Uid != 0 && st.Uid != uint32(os.Getuid()) {
			return ErrUnavailable
		}
		if p == parent && (st.Uid != uint32(os.Getuid()) || st.Mode&0077 != 0) {
			return ErrUnavailable
		}
		if filepath.Dir(p) == p {
			return nil
		}
	}
}
