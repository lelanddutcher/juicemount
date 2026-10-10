//go:build ownerservingacceptance && !serviceclient

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// The production selector execs this test binary with --volume-owner. Only
// this tagged binary intercepts that entry; no owner dispatch is simulated.
func TestMain(m *testing.M) {
	if volumeOwnerMode(os.Args[1:]) {
		if err := runVolumeOwnerProcess(); err != nil {
			fmt.Fprintln(os.Stderr, "selected volume owner:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// A bound, non-listening TCP socket reserves an endpoint that always refuses
// Redis connections, including if another local process races this fixture.
func ownerAcceptanceOfflinePort(t *testing.T) (string, func()) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		_ = syscall.Close(fd)
		t.Fatal(err)
	}
	addr, err := syscall.Getsockname(fd)
	if err != nil {
		_ = syscall.Close(fd)
		t.Fatal(err)
	}
	return "127.0.0.1:" + strconv.Itoa(addr.(*syscall.SockaddrInet4).Port), func() { _ = syscall.Close(fd) }
}

func ownerAcceptanceFreePort(t *testing.T, used map[string]bool) string {
	t.Helper()
	for i := 0; i < 32; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		if !used[addr] {
			used[addr] = true
			return addr
		}
	}
	t.Fatal("no distinct private loopback port")
	return ""
}

// This is a real NFSv3 NULL over the listener created by NFSServerStart.
func ownerAcceptanceNFSNull(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("NFS listener %s: %v", addr, err)
	}
	defer conn.Close()
	var payload bytes.Buffer
	for _, word := range []uint32{0x76543210, 0} {
		if err := binary.Write(&payload, binary.BigEndian, word); err != nil {
			t.Fatal(err)
		}
	}
	if err := xdr.Write(&payload, rpc.Header{Rpcvers: 2, Prog: 100003, Vers: 3, Proc: 0, Cred: rpc.AuthNull, Verf: rpc.AuthNull}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	frame := make([]byte, 4+payload.Len())
	binary.BigEndian.PutUint32(frame, uint32(payload.Len())|0x80000000)
	copy(frame[4:], payload.Bytes())
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		t.Fatal(err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size>>31 != 1 || size&0x7fffffff > 1<<20 {
		t.Fatalf("invalid NFS reply frame %#x", size)
	}
	body := make([]byte, size&0x7fffffff)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatal(err)
	}
	if len(body) < 24 || binary.BigEndian.Uint32(body[0:4]) != 0x76543210 ||
		binary.BigEndian.Uint32(body[4:8]) != 1 || binary.BigEndian.Uint32(body[8:12]) != 0 ||
		binary.BigEndian.Uint32(body[12:16]) != 0 || binary.BigEndian.Uint32(body[16:20]) != 0 ||
		binary.BigEndian.Uint32(body[20:24]) != 0 {
		t.Fatalf("NFS NULL was not accepted: %x", body)
	}
}

// Covers the actual dispatch -> owner selection/claim/attach/deferred activate
// -> local SQLite/pin/handler/NFS listener -> authenticated hard retirement.
// It deliberately has no Redis, Link, kernel NFS mount, or FUSE mount.
func TestOwnerServingStartShutdownStartAcceptance(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("selected owner refuses root; run as the logged-in user")
	}
	bin := os.Getenv("JM_OWNER_ACCEPTANCE_JUICEFS_BIN")
	if bin == "" || !filepath.IsAbs(bin) {
		t.Fatal("JM_OWNER_ACCEPTANCE_JUICEFS_BIN must name the real absolute JuiceFS executable")
	}
	if st, err := os.Stat(bin); err != nil || st.IsDir() || st.Mode()&0111 == 0 {
		t.Fatalf("JuiceFS executable unavailable: %q %v", bin, err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(filepath.Dir(exe), "juicemount-mount-probe")
	if st, err := os.Stat(probe); err != nil || st.IsDir() || st.Mode()&0111 == 0 {
		t.Fatalf("real sibling mount probe required: %s: %v", probe, err)
	}
	base := os.TempDir()
	if base == "/tmp" || strings.HasPrefix(base, "/tmp/") || base == "/private/tmp" || strings.HasPrefix(base, "/private/tmp/") || base == "/var/tmp" || strings.HasPrefix(base, "/var/tmp/") {
		t.Fatalf("private approved TMPDIR required, got %q", base)
	}
	root, err := os.MkdirTemp(base, "owner-serving-")
	if err != nil {
		t.Fatal(err)
	}
	privateTmp := filepath.Join(root, "tmp")
	if err := os.Mkdir(privateTmp, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// The test owns this root, but an uncertain Stop must retain every byte and
	// the original owner process for inspection. No t.TempDir cleanup is armed.
	fullyRetired := false
	active := false
	var currentIntent volumeOwnerIntent
	var currentWaiter *ownerChildResult
	defer func() {
		if fullyRetired {
			if err := os.RemoveAll(root); err != nil {
				t.Errorf("retired private root cleanup: %v", err)
			}
		} else {
			t.Errorf("owner acceptance incomplete; private root retained: %s", root)
		}
	}()
	oldEnv := map[string]*string{}
	setenv := func(key, value string) {
		if old, ok := os.LookupEnv(key); ok {
			copy := old
			oldEnv[key] = &copy
		} else {
			oldEnv[key] = nil
		}
		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}
	setenv("TMPDIR", privateTmp)
	setenv("JM_APP_ISOLATION", "1")
	setenv("JM_APP_ISOLATION_ROOT", root)
	setenv("JM_DEBUG_META_ADDR", "")
	setenv("JM_BOOT_PARALLEL", "0")
	setenv("JM_SPOOL_ENABLE", "0")
	setenv("JM_QL_PREWARM", "0")
	setenv("JM_PIN_WARM_METADATA", "0")
	setenv("JM_THUMB_WARM", "0")
	defer func() {
		if !fullyRetired {
			return // callbacks may still use this fixture and environment
		}
		for key, value := range oldEnv {
			if value == nil {
				_ = os.Unsetenv(key)
			} else {
				_ = os.Setenv(key, *value)
			}
		}
	}()
	if _, isolated, err := appisolation.Current(); err != nil || !isolated {
		t.Fatalf("private isolation preflight: %v", err)
	}
	redisAddr, releaseRedis := ownerAcceptanceOfflinePort(t)
	defer func() {
		if !active {
			releaseRedis()
		}
	}()
	used := map[string]bool{redisAddr: true}
	listenAddr := ownerAcceptanceFreePort(t, used)
	metricsAddr := ownerAcceptanceFreePort(t, used)
	fuseMetricsAddr := ownerAcceptanceFreePort(t, used)
	setenv("JM_APP_ISOLATION_FUSE_METRICS_ADDR", fuseMetricsAddr)
	cfg := ServerConfig{RedisURL: "redis://" + redisAddr + "/0", FUSEPath: filepath.Join(root, "fuse-internal"),
		MountPoint: filepath.Join(root, "volume"), ListenAddr: listenAddr, MetricsAddr: metricsAddr,
		FUSEMetricsAddr: fuseMetricsAddr, DBPath: filepath.Join(root, "metadata.db"),
		LogFile: filepath.Join(root, "test.log"), LogLevel: "error", SpoolDir: filepath.Join(root, "spool"),
		JuiceFSBin: bin, CacheSize: "1024"}
	if err := validateIsolatedServerConfig(cfg); err != nil {
		t.Fatalf("isolated config preflight: %v", err)
	}
	globalMu.Lock()
	occupied := globalServer != nil || globalStore != nil || globalOwner != nil || globalFUSE != nil || globalNFSController != nil || globalStopPending
	globalMu.Unlock()
	if occupied {
		t.Fatal("acceptance requires an empty serving runtime")
	}
	oldMode, oldCreator := servingProcessMode.Load(), newNFSMountCreator
	const mountDenied = "owner serving acceptance denies kernel mount"
	mountDenials := make(chan struct{}, 1)
	newNFSMountCreator = func() (mountbroker.Creator, error) {
		select {
		case mountDenials <- struct{}{}:
		default:
		}
		return mountbroker.Creator{}, errors.New(mountDenied)
	}
	servingProcessMode.Store(true)
	defer func() {
		if fullyRetired {
			newNFSMountCreator = oldCreator
			servingProcessMode.Store(oldMode)
		}
	}()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method string, values ...string) servicecontrol.Response {
		t.Helper()
		return dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: method, Strings: values})
	}
	// A failed assertion after entering Start still attempts the supported
	// shutdown. An uncertain result leaves the original child and root intact.
	defer func() {
		if !active {
			return
		}
		servingOwnerSelection.Lock()
		currentIntent, currentWaiter = servingOwnerSelection.intent, servingOwnerSelection.childResult
		servingOwnerSelection.Unlock()
		stop := dispatchServingRequest(servicecontrol.Request{Version: servicecontrol.ProtocolVersion,
			Build: version.Commit, Method: "shutdown"})
		if stop.Error != "" || stop.Int != 1 {
			t.Errorf("failure cleanup: authenticated shutdown incomplete: %+v; root %s", stop, root)
			return
		}
		if err := confirmOwnerRetired(currentIntent); err != nil {
			t.Errorf("failure cleanup: exact retirement unconfirmed: %v; root %s", err, root)
			return
		}
		if currentWaiter == nil {
			t.Errorf("failure cleanup: original Wait handle missing; root %s", root)
			return
		}
		select {
		case <-currentWaiter.done:
			if currentWaiter.err != nil {
				t.Errorf("failure cleanup: original Wait failed: %v; root %s", currentWaiter.err, root)
				return
			}
		case <-time.After(3 * time.Second):
			t.Errorf("failure cleanup: original Wait unjoined; root %s", root)
			return
		}
		active = false
	}()
	var first volumeOwnerIntent
	var firstPhysical string
	for generation := 0; generation < 2; generation++ {
		select {
		case <-mountDenials:
		default:
		}
		active = true
		start := request("start", string(raw))
		if start.Error != "" || strings.HasPrefix(start.Text, "error:") || start.Text == "" {
			t.Fatalf("generation %d actual Start failed: %+v; private root %s", generation, start, root)
		}
		intent, err := readOwnerIntent(appisolation.Scope{Root: root})
		if err != nil {
			t.Fatal(err)
		}
		active, currentIntent = true, intent
		servingOwnerSelection.Lock()
		child := servingOwnerSelection.child
		waiter := servingOwnerSelection.childResult
		servingOwnerSelection.Unlock()
		currentWaiter = waiter
		globalMu.Lock()
		owner, server, store, pins := globalOwner, globalServer, globalStore, globalPinStore
		physical, fuse, controller := globalFUSEPath, globalFUSE, globalNFSController
		mountedPath, effectiveMetrics := globalMountPath, globalFUSEMetricsAddr
		globalMu.Unlock()
		if owner == nil || server == nil || store == nil || pins == nil || child == nil || waiter == nil || fuse != nil || controller != nil || mountedPath != "" {
			t.Fatalf("generation %d did not publish real owner, store, pin and handler without local FUSE/kernel mount", generation)
		}
		if owner.intent != intent || owner.claimed.JuiceFSBin != bin || owner.claimed.MountPath != physical ||
			owner.claimed.RedisURL != cfg.RedisURL || owner.claimed.MetricsAddr != effectiveMetrics || effectiveMetrics != fuseMetricsAddr || physical == cfg.FUSEPath || !strings.HasPrefix(physical, root+string(os.PathSeparator)) {
			t.Fatalf("generation %d effective owner configuration not consumed: %+v physical=%s", generation, owner.claimed, physical)
		}
		if got := reflect.ValueOf(server.Handler()).Elem().FieldByName("fusePath").String(); got != physical {
			t.Fatalf("generation %d handler uses %q, owner physical path %q", generation, got, physical)
		}
		if err := store.DB().Ping(); err != nil {
			t.Fatalf("generation %d SQLite store unavailable: %v", generation, err)
		}
		if got := pin.CacheBudgetBytes(); got != 1024<<20 {
			t.Fatalf("generation %d effective cache budget %d", generation, got)
		}
		if server.Addr() != listenAddr || !server.IsRunning() {
			t.Fatalf("generation %d NFS listener absent: %q", generation, server.Addr())
		}
		ownerAcceptanceNFSNull(t, listenAddr)
		receipt, err := owner.inspect()
		if err != nil || receipt.Instance != intent.Instance || receipt.MountPath != physical || receipt.Attempted {
			t.Fatalf("generation %d original owner did not retain deferred generation: %+v %v", generation, receipt, err)
		}
		if generation == 1 && (intent.Instance == first.Instance || physical == firstPhysical) {
			t.Fatal("successor reused original owner identity")
		}
		select {
		case <-mountDenials:
		case <-time.After(3 * time.Second):
			t.Fatal("NFS mount did not reach the explicit pre-effect denial")
		}
		stop := request("shutdown")
		if stop.Error != "" || stop.Int != 1 {
			t.Fatalf("generation %d authenticated hard shutdown incomplete: %+v; root %s", generation, stop, root)
		}
		if err := confirmOwnerRetired(intent); err != nil {
			t.Fatalf("generation %d original retirement not durable: %v", generation, err)
		}
		select {
		case <-waiter.done:
			if waiter.err != nil {
				t.Fatalf("generation %d original child Wait: %v", generation, waiter.err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("generation %d original child Wait unjoined", generation)
		}
		if conn, err := net.DialTimeout("tcp", listenAddr, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			t.Fatalf("generation %d NFS listener remained open", generation)
		}
		globalMu.Lock()
		retained := globalOwner != nil || globalServer != nil || globalStore != nil || globalPinStore != nil || globalFUSE != nil || globalNFSController != nil
		globalMu.Unlock()
		if retained {
			t.Fatalf("generation %d engine retained resources after confirmed retirement", generation)
		}
		t.Logf("generation=%d owner=%s physical=%s metrics=%s cache_bytes=%d RPC_NULL=PASS retirement=PASS original_wait=0", generation, intent.Instance, physical, effectiveMetrics, pin.CacheBudgetBytes())
		active = false
		if generation == 0 {
			first = intent
			firstPhysical = physical
		}
	}
	fullyRetired = true
}
