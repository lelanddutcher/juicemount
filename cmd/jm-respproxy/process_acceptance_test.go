//go:build redisoptionsacceptance

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
	"github.com/redis/go-redis/v9"
)

const proxyCLIVolumeUUID = "12345678-1234-1234-1234-123456789abc"

func proxyCLIContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func proxyCLIFreeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type proxyCLIFixture struct {
	db           *redis.Client
	config       fileConfig
	password     string
	top, project meta.Ino
}

// Bootstrap uses the actual SDK against this test's isolated, owned Redis.
// No production endpoint, home directory, mount or disk cache is involved.
func newProxyCLIFixture(t *testing.T) *proxyCLIFixture {
	return newProxyCLIFixtureDB(t, 3)
}

func newProxyCLIFixtureDB(t *testing.T, volumeDB int) *proxyCLIFixture {
	t.Helper()
	raw := redistestfixture.Start(t)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = fmt.Sprintf("/%d", volumeDB)
	query := u.Query()
	query.Set("db", strconv.Itoa(volumeDB))
	u.RawQuery = query.Encode()
	raw = u.String()
	options, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	options.ContextTimeoutEnabled, options.MaxRetries = true, -1
	db := redis.NewClient(options)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	conf := meta.DefaultConf()
	conf.Retries, conf.MaxDeletes, conf.NoBGJob = 0, 0, true
	bootstrap := meta.NewClient(raw, conf)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = bootstrap.Shutdown()
		}
	})
	format := &meta.Format{Name: "cli-fixture", UUID: proxyCLIVolumeUUID, Storage: "mem", BlockSize: 4096, Compression: "none"}
	if err := bootstrap.Init(format, false); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Load(false); err != nil {
		t.Fatal(err)
	}
	ctx := meta.WrapContext(proxyCLIContext(t))
	var top, project meta.Ino
	var attr meta.Attr
	if st := bootstrap.Mkdir(ctx, meta.RootInode, "Projects", 0777, 0, 0, &top, &attr); st != 0 {
		t.Fatalf("bootstrap top: %v", st)
	}
	if st := bootstrap.Mkdir(ctx, top, "A", 0777, 0, 0, &project, &attr); st != 0 {
		t.Fatalf("bootstrap project: %v", st)
	}
	if err := bootstrap.Shutdown(); err != nil {
		t.Fatal(err)
	}
	stopped = true
	hash, err := respproxy.HashPassword("cli-password", 1, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	principals, err := json.Marshal(map[string]any{"principals": []map[string]string{{"name": "mac-cli", "kind": "mac", "host": "cli-fixture", "argon2id": hash}}})
	if err != nil {
		t.Fatal(err)
	}
	principalsPath := filepath.Join(t.TempDir(), "principals.json")
	if err := os.WriteFile(principalsPath, principals, 0600); err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	volume, singleton := options.DB, true
	return &proxyCLIFixture{db: db, password: password, top: top, project: project, config: fileConfig{
		Listen: proxyCLIFreeAddress(t), Upstream: u.Host, UpstreamUser: u.User.Username(), UpstreamPasswordEnv: "JM_RESPPROXY_CLI_UPSTREAM_SECRET",
		Stats: "", Principals: principalsPath, Mode: "enforce", VolumeDB: &volume, AllowedDBs: []int{volume}, Singleton: &singleton, StageBirth: true,
	}}
}

func proxyCLIClient(t *testing.T, addr string) *redis.Client {
	return proxyCLIClientDB(t, addr, 3)
}

func proxyCLIClientDB(t *testing.T, addr string, volumeDB int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Username: "mac-cli", Password: "cli-password", DB: volumeDB,
		Protocol: 2, ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(proxyCLIContext(t)).Err(); err != nil {
		t.Fatalf("CLI authenticated PING: %v", err)
	}
	return c
}

func proxyCLIIncarnation(t *testing.T, f *proxyCLIFixture, p *proxyCLIProcess) string {
	t.Helper()
	incarnation, err := f.db.Get(proxyCLIContext(t), respproxy.SingletonKey).Result()
	if err != nil || !strings.Contains(incarnation, fmt.Sprintf(":%d:", p.cmd.Process.Pid)) {
		t.Fatalf("CLI original PID singleton: %q %v", incarnation, err)
	}
	if !bytes.Contains(p.logBytes(t), []byte("incarnation="+incarnation)) {
		t.Fatal("CLI banner does not match owned singleton")
	}
	return incarnation
}

func proxyCLIGrant(t *testing.T, c *redis.Client, project meta.Ino, wanted string, epoch int64) int64 {
	t.Helper()
	values, err := c.Do(proxyCLIContext(t), "JM.LEASE", "ACQUIRE", "ino:"+project.String(), "auto").Slice()
	if err != nil || len(values) != 5 {
		t.Fatalf("CLI lease response: %+v %v", values, err)
	}
	gotEpoch, ok := values[1].(int64)
	expiry, expiryOK := values[3].(int64)
	if values[0] != wanted || !ok || gotEpoch <= 0 || (epoch != 0 && gotEpoch != epoch) || values[2] != "mac-cli@cli-fixture" || !expiryOK || expiry <= time.Now().UnixMilli() || values[4] != int64(0) {
		t.Fatalf("CLI lease grant differs: %+v wanted=%s epoch=%d", values, wanted, epoch)
	}
	return gotEpoch
}

func proxyCLICreateBirth(t *testing.T, f *proxyCLIFixture, p *proxyCLIProcess, operation, incarnation string, epoch int64) (meta.Ino, []byte) {
	t.Helper()
	if f.config.VolumeDB == nil {
		t.Fatal("CLI fixture volume DB is unset")
	}
	conf := meta.DefaultConf()
	conf.Retries, conf.MaxDeletes, conf.NoBGJob = 0, 0, true
	endpoint := (&url.URL{Scheme: "redis", Host: p.addr, User: url.UserPassword("mac-cli", "cli-password"), Path: fmt.Sprintf("/%d", *f.config.VolumeDB)}).String()
	m := meta.NewClient(endpoint, conf)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = m.Shutdown()
		}
	})
	if format, err := m.Load(false); err != nil || format == nil || format.UUID != proxyCLIVolumeUUID {
		t.Fatalf("CLI SDK format: %+v %v", format, err)
	}
	ctx := meta.WrapContext(proxyCLIContext(t))
	var ino meta.Ino
	var attr meta.Attr
	name := replaceguard.BirthStageName(operation)
	if st := m.Create(ctx, f.project, name, 0600, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("CLI SDK owned create: %v", st)
	}
	raw, err := f.db.Get(ctx, replaceguard.BirthOperationKey(operation)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	birth, err := replaceguard.DecodeBirth(raw)
	expectedParents := []replaceguard.Parent{{Inode: 1}, {Inode: uint64(f.top), Name: "Projects"}, {Inode: uint64(f.project), Name: "A"}}
	if err != nil || birth.OperationID != operation || birth.Owner != "mac-cli" || birth.Proxy != incarnation || birth.Database != int64(*f.config.VolumeDB) || birth.Project != uint64(f.project) || birth.Epoch != uint64(epoch) || birth.BackendUUID != proxyCLIVolumeUUID || birth.Stage.Inode != uint64(ino) || birth.Stage.Parent != uint64(f.project) || birth.Stage.Name != name || birth.Stage.Length != 0 || birth.Mode != 0600 || birth.Type != meta.TypeFile || birth.Nlink != 1 || !reflect.DeepEqual(birth.Parents, expectedParents) {
		t.Fatalf("CLI reciprocal birth identity: %+v %v", birth, err)
	}
	reciprocal, err := f.db.Get(ctx, replaceguard.BirthInodeKey(uint64(ino))).Bytes()
	if err != nil || !bytes.Equal(reciprocal, raw) {
		t.Fatalf("CLI inode birth differs: %v", err)
	}
	storedAttr, err := f.db.Get(ctx, "i"+ino.String()).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(storedAttr)
	if birth.AttrSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("CLI birth did not bind actual stored attrs")
	}
	entry, err := f.db.HGet(ctx, "d"+f.project.String(), name).Bytes()
	if err != nil || len(entry) != 9 || entry[0] != meta.TypeFile || binary.BigEndian.Uint64(entry[1:]) != uint64(ino) {
		t.Fatalf("CLI created namespace differs: %v", err)
	}
	if st := m.Close(ctx, ino); st != 0 {
		t.Fatal(st)
	}
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	stopped = true
	return ino, raw
}

func proxyCLINoSingleton(t *testing.T, f *proxyCLIFixture) {
	t.Helper()
	if n, err := f.db.Exists(proxyCLIContext(t), respproxy.SingletonKey).Result(); err != nil || n != 0 {
		t.Fatalf("CLI retained singleton: %d %v", n, err)
	}
}

// Dump only this exclusively owned, small fixture. This detects unexpected
// mutations during invalid startup, including keys outside the seeded tree.
func proxyCLISnapshot(t *testing.T, f *proxyCLIFixture) map[string]string {
	t.Helper()
	ctx := proxyCLIContext(t)
	keys, err := f.db.Keys(ctx, "*").Result()
	if err != nil || len(keys) > 64 {
		t.Fatalf("owned fixture inventory: %d %v", len(keys), err)
	}
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		value, err := f.db.Dump(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		result[key] = value
	}
	return result
}

func TestProxyCLIStageBirthLifecycle(t *testing.T) {
	f := newProxyCLIFixture(t)
	first := startProxyCLI(t, f.config, f.password)
	first.awaitReady(t)
	client := proxyCLIClient(t, first.addr)
	firstIncarnation := proxyCLIIncarnation(t, f, first)
	epoch := proxyCLIGrant(t, client, f.project, "GRANTED", 1)
	ino, birth := proxyCLICreateBirth(t, f, first, strings.Repeat("a", 32), firstIncarnation, epoch)
	collisionConfig := f.config
	collisionConfig.Listen = proxyCLIFreeAddress(t)
	collision := startProxyCLI(t, collisionConfig, f.password)
	collision.reject(t, "another proxy holds")
	if proxyCLIIncarnation(t, f, first) != firstIncarnation {
		t.Fatal("refused duplicate disrupted current owner")
	}
	if err := client.Ping(proxyCLIContext(t)).Err(); err != nil {
		t.Fatal(err)
	}
	heartbeatAt := time.Now().UnixMilli()
	heartbeat, err := client.Do(proxyCLIContext(t), "JM.LEASE", "HEARTBEAT", "ino:"+f.project.String()).Slice()
	if err != nil || len(heartbeat) != 1 {
		t.Fatalf("CLI heartbeat: %+v %v", heartbeat, err)
	}
	held, ok := heartbeat[0].([]interface{})
	if !ok || len(held) != 4 || held[0] != int64(f.project) || held[1] != epoch || held[2] != "held" || held[3] != "auto" {
		t.Fatalf("CLI pending heartbeat tuple: %+v", heartbeat)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	first.stop(t)
	proxyCLINoSingleton(t, f)
	persisted, err := f.db.HGetAll(proxyCLIContext(t), respproxy.LeaseKeyPrefix+f.project.String()).Result()
	if err != nil || persisted["holder"] != "mac-cli" || persisted["epoch"] != strconv.FormatInt(epoch, 10) {
		t.Fatalf("CLI shutdown lease persistence: %+v %v", persisted, err)
	}
	lastHeartbeat, err := strconv.ParseInt(persisted["last_hb_ms"], 10, 64)
	if err != nil || lastHeartbeat < heartbeatAt || persisted["pending"] != "1" {
		t.Fatalf("CLI shutdown lost pending heartbeat: %+v %v", persisted, err)
	}
	// Relaunch the same real config against the same Redis, without reseeding.
	second := startProxyCLI(t, f.config, f.password)
	second.awaitReady(t)
	next := proxyCLIClient(t, second.addr)
	secondIncarnation := proxyCLIIncarnation(t, f, second)
	if secondIncarnation == firstIncarnation {
		t.Fatal("CLI restart reused incarnation")
	}
	proxyCLIGrant(t, next, f.project, "RENEWED", epoch)
	later, _ := proxyCLICreateBirth(t, f, second, strings.Repeat("b", 32), secondIncarnation, epoch)
	if later == ino {
		t.Fatal("CLI restart reused owned stage inode")
	}
	prior, err := f.db.Get(proxyCLIContext(t), replaceguard.BirthInodeKey(uint64(ino))).Bytes()
	if err != nil || !bytes.Equal(prior, birth) {
		t.Fatalf("CLI restart rewrote historical birth: %v", err)
	}
	priorOperation, err := f.db.Get(proxyCLIContext(t), replaceguard.BirthOperationKey(strings.Repeat("a", 32))).Bytes()
	if err != nil || !bytes.Equal(priorOperation, birth) {
		t.Fatalf("CLI restart rewrote operation birth: %v", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	second.stop(t)
	proxyCLINoSingleton(t, f)
	t.Logf("CLI lifecycle original PIDs first=%d duplicate=%d second=%d joined; lease_epoch=%d stage_inodes=%d,%d; same Redis; historical birth preserved; singleton released twice; pending heartbeat persisted", first.cmd.Process.Pid, collision.cmd.Process.Pid, second.cmd.Process.Pid, epoch, ino, later)
}

func TestProxyCLIStageBirthRejectsConfiguration(t *testing.T) {
	for _, name := range []string{"observe-mode", "without-singleton", "effective-observe-mode"} {
		t.Run(name, func(t *testing.T) {
			f := newProxyCLIFixture(t)
			expected := "stage_birth requires enforce mode and singleton ownership"
			switch name {
			case "observe-mode":
				f.config.Mode = "observe"
			case "without-singleton":
				disabled := false
				f.config.Singleton = &disabled
			case "effective-observe-mode":
				if err := f.db.HSet(proxyCLIContext(t), respproxy.ConfigKey, "mode", "observe").Err(); err != nil {
					t.Fatal(err)
				}
				expected = "stage_birth startup requires current enforcing singleton ownership"
			}
			before := proxyCLISnapshot(t, f)
			child := startProxyCLI(t, f.config, f.password)
			child.reject(t, expected)
			proxyCLINoSingleton(t, f)
			if after := proxyCLISnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid CLI startup changed fixture Redis")
			}
		})
	}
}
