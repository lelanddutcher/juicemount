package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
	"github.com/lelanddutcher/juicemount/internal/version"
)

// Only the desktop archive uses this transport. Its serving counterpart runs
// the same engine entry points in a separate process. No failed call is replayed
// or replaced by an in-process operation: a lost reply may follow real effects.
var callServingProcess = servicecontrol.Call
var servingProcessMode atomic.Bool
var servingSocketPath = servicecontrol.SocketPath

// resolveServingJuiceFS names the app's bundled, pinned JuiceFS only (beside
// the running executable); it never searches PATH or Homebrew. Tests replace
// it; no environment variable or flag reaches it.
var resolveServingJuiceFS = health.ResolveJuiceFSBinary

var pendingNetworkCost struct {
	sync.Mutex
	valid                  bool
	expensive, constrained int
}

func serviceDeadline(method string) time.Duration {
	switch method {
	case "is-running", "is-offline", "ready", "auto-start-eligible", "network-cost":
		return 250 * time.Millisecond
	case "log":
		return 100 * time.Millisecond
	case "start", "start-automatic", "link-test", "stop", "stop-mount", "shutdown", "shutdown-forced", "sync":
		return 240 * time.Second
	case "enroll-verify":
		// The serving process's login check is bounded by principalAuthTimeout
		// (5 s, a slow or cold Link); the 2 s default dropped its answer
		// (review 3 finding 1).
		return enrollVerifyDeadline
	case enrollHTTPMethod:
		// The serving process's Manager request is bounded at
		// enrollHTTPTimeout (a cold Link).
		return enrollHTTPDeadline
	default:
		return 2 * time.Second
	}
}

func forwardServiceCall(method string, strings []string, ints []int) servicecontrol.Response {
	result, _ := forwardServiceCallReached(method, strings, ints)
	return result
}

// forwardServiceCallReached is forwardServiceCall that also reports whether
// the request may have reached the serving process. reached is false only
// when the control socket could not be located or dialled, so nothing was
// sent. Any later failure (timeout, lost reply) leaves reached true: the
// serving process may have acted, and the caller must not replace the call
// with an in-process one.
func forwardServiceCallReached(method string, strings []string, ints []int) (result servicecontrol.Response, reached bool) {
	failure := func(err error) servicecontrol.Response {
		return servicecontrol.Response{Version: 1, Error: "serving service unavailable or operation outcome unknown: " + err.Error()}
	}
	path, err := servingSocketPath()
	if err != nil {
		return failure(err), false
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceDeadline(method))
	defer cancel()
	result, err = callServingProcess(ctx, path, servicecontrol.Request{Version: 1, Build: version.Commit, Method: method, Strings: strings, Ints: ints})
	if err != nil {
		var op *net.OpError
		notSent := errors.Is(err, servicecontrol.ErrNotSent) || (errors.As(err, &op) && op.Op == "dial")
		return failure(err), !notSent
	}
	return result, true
}

func forwardServiceText(method string, values []string, ints []int) *C.char {
	if method == "start" || method == "start-automatic" {
		if len(values) != 1 {
			return C.CString("error: missing startup configuration")
		}
		prepared, err := servingConfiguration(values[0])
		if err != nil {
			return C.CString("error: " + err.Error())
		}
		values = []string{prepared}
		// Network.framework can report cost before launchd has started the
		// service. Reapply the most recent signal before immutable mount flags
		// are chosen, serializing with live signals so an older value cannot win.
		pendingNetworkCost.Lock()
		if pendingNetworkCost.valid {
			result := forwardServiceCall("network-cost", nil, []int{pendingNetworkCost.expensive, pendingNetworkCost.constrained})
			if result.Error != "" {
				pendingNetworkCost.Unlock()
				return C.CString("error: " + result.Error)
			}
		}
		pendingNetworkCost.Unlock()
	}
	result := forwardServiceCall(method, values, ints)
	if result.Error != "" {
		return C.CString("error: " + result.Error)
	}
	return C.CString(result.Text)
}

// servingGUIEnvAllowed lists the only JM_ variables the GUI process may carry
// when it starts the launchd serving service. Anything else would silently
// differ between the GUI and the service, so Start refuses it. The app bundle's
// own Info.plist LSEnvironment is checked against this list by a test.
var servingGUIEnvAllowed = map[string]bool{
	"JM_SPOOL_DIR": true, "JM_SPOOL_SIZE_GB": true, "JM_SPOOL_ENABLE": true,
	"JM_APP_ISOLATION": true, "JM_APP_ISOLATION_ROOT": true, "JM_APP_ISOLATION_FUSE_METRICS_ADDR": true,
}

func servingConfiguration(raw string) (string, error) {
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return raw, nil
	} // engine returns its ordinary parse error
	// A launchd job does not inherit GUI overrides. Translate supported custody
	// settings explicitly; never silently switch a custom spool to another dir.
	for _, item := range os.Environ() {
		key, value, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "JM_") && value != "" && !servingGUIEnvAllowed[key] {
			return "", fmt.Errorf("GUI environment override %s is not supported by the serving service; use explicit service configuration", key)
		}
	}
	if cfg.JuiceFSBin == "" {
		cfg.JuiceFSBin = resolveServingJuiceFS()
	} else if bin, err := juicefsbin.Validate(cfg.JuiceFSBin, "serving configuration"); err != nil {
		return "", err // only the bundled juicefs may be forwarded
	} else {
		cfg.JuiceFSBin = bin
	}
	if cfg.SpoolDir == "" {
		cfg.SpoolDir = os.Getenv("JM_SPOOL_DIR")
	}
	for _, path := range []*string{&cfg.JuiceFSBin, &cfg.SpoolDir} {
		if *path == "" {
			continue
		}
		absolute, err := filepath.Abs(*path)
		if err != nil {
			return "", err
		}
		*path = absolute
	}
	if os.Getenv("JM_SPOOL_ENABLE") == "1" {
		cfg.SpoolEnable = true
	}
	if value := os.Getenv("JM_SPOOL_SIZE_GB"); cfg.SpoolSizeGB < 1 && value != "" {
		size, err := strconv.ParseInt(value, 10, 32)
		if err != nil || size < 1 {
			return "", fmt.Errorf("JM_SPOOL_SIZE_GB must be a positive number")
		}
		cfg.SpoolSizeGB = int(size)
	}
	if err := validateIsolatedServerConfig(cfg); err != nil {
		return "", err
	}
	data, err := json.Marshal(cfg)
	return string(data), err
}

func forwardServiceInt(method string, strings []string, ints []int) C.int {
	result := forwardServiceCall(method, strings, ints)
	if result.Error != "" {
		// Unknown availability cannot report successful teardown or online
		// permission. The existing Boolean C ABI is conservative on failure.
		if method == "is-offline" {
			return -1
		}
		return 0
	}
	return C.int(result.Int)
}

func forwardNetworkCost(expensive, constrained int) {
	pendingNetworkCost.Lock()
	defer pendingNetworkCost.Unlock()
	pendingNetworkCost.valid = true
	pendingNetworkCost.expensive, pendingNetworkCost.constrained = expensive, constrained
	_ = forwardServiceCall("network-cost", nil, []int{expensive, constrained})
}

//export NFSServerUsesServingService
func NFSServerUsesServingService() C.int {
	if serviceClientBuild {
		return 1
	}
	return 0
}

//export NFSServerServiceReady
func NFSServerServiceReady() C.int {
	if serviceClientBuild {
		return forwardServiceInt("ready", nil, nil)
	}
	return 1
}

//export NFSServerAutoStartEligible
func NFSServerAutoStartEligible() C.int {
	if serviceClientBuild {
		return forwardServiceInt("auto-start-eligible", nil, nil)
	}
	servingDesiredMu.Lock()
	defer servingDesiredMu.Unlock()
	_, err := readServingDesired()
	if os.IsNotExist(err) {
		return 1
	}
	return 0 // existing or uncertain intent belongs to the service or user
}

// The errors dispatchServingRequest returns before it runs anything. Their
// text is the same in every build with a control socket, so a newer app can
// tell that an older serving process ran nothing (servingRequestNotRun).
const (
	servingErrBuildDiffers = "serving service build/protocol differs; existing runtime retained"
	servingErrBadOperation = "invalid serving service operation or arguments"
)

// dispatchServingRequest intentionally has no context from the client. Closing
// a GUI socket cannot revoke a mount already admitted to the serving runtime.
func dispatchServingRequest(req servicecontrol.Request) servicecontrol.Response {
	result := servicecontrol.Response{Version: 1}
	if req.Version != 1 || req.Build != version.Commit {
		result.Error = servingErrBuildDiffers
		return result
	}
	shapes := map[string][2]int{
		"ready": {}, "auto-start-eligible": {}, "recovery-status": {}, "start": {1, 0}, "start-automatic": {1, 0}, "link-test": {1, 0}, "stop": {}, "stop-mount": {},
		"shutdown": {}, "shutdown-forced": {}, "is-running": {}, "stats": {}, "metrics": {},
		"sync": {}, "search": {2, 1}, "pin": {1, 0}, "unpin": {1, 0}, "cache-status": {},
		"set-offline": {0, 1}, "is-offline": {}, "network-cost": {0, 2}, "log": {1, 0},
		"shutdown-outcome": {}, "enroll-verify": {3, 0}, enrollHTTPMethod: {3, 0},
	}
	shape, ok := shapes[req.Method]
	if !ok || len(req.Strings) != shape[0] || len(req.Ints) != shape[1] {
		result.Error = servingErrBadOperation
		return result
	}
	// Preserve the C ABI's integer range. JSON integers are host-width ints.
	for _, v := range req.Ints {
		if int64(v) < -2147483648 || int64(v) > 2147483647 {
			result.Error = "serving service integer outside C ABI range"
			return result
		}
	}
	args := make([]*C.char, len(req.Strings))
	for i, value := range req.Strings {
		// C strings cannot carry embedded NUL; refuse instead of silently
		// truncating a configuration or path at a different authority boundary.
		for _, b := range []byte(value) {
			if b == 0 {
				result.Error = "NUL in serving service argument"
				return result
			}
		}
		args[i] = C.CString(value)
		defer C.free(unsafe.Pointer(args[i]))
	}
	text := func(value *C.char) {
		if value == nil {
			result.Error = "empty engine response"
			return
		}
		result.Text = C.GoString(value)
		NFSServerFreeString(value)
	}
	switch req.Method {
	case "ready":
		result.Int = int(NFSServerServiceReady())
	case "auto-start-eligible":
		result.Int = int(NFSServerAutoStartEligible())
	case "recovery-status":
		result.Text = recoveryStatus()
	case "start":
		if err := validateIsolatedRaw(req.Strings[0]); err != nil {
			result.Error = err.Error()
			return result
		}
		text(NFSServerStart(args[0]))
	case "start-automatic":
		if err := validateIsolatedRaw(req.Strings[0]); err != nil {
			result.Error = err.Error()
			return result
		}
		text(NFSServerStartAutomatic(args[0]))
	case "link-test":
		if err := validateIsolatedRaw(req.Strings[0]); err != nil {
			result.Error = err.Error()
			return result
		}
		text(NFSServerLinkTest(args[0]))
	case "stop":
		result.Int = int(NFSServerStop())
	case "stop-mount":
		result.Int = int(NFSServerStopMount())
	case "shutdown":
		result.Int = int(NFSServerShutdown())
	case "shutdown-forced":
		result.Int = int(NFSServerShutdownForced())
	case "is-running":
		result.Int = int(NFSServerIsRunning())
	case "stats":
		text(NFSServerStats())
	case "metrics":
		text(NFSServerMetrics())
	case "sync":
		text(NFSServerSyncNow())
	case "search":
		text(NFSServerSearch(args[0], C.int(req.Ints[0]), args[1]))
	case "pin":
		text(NFSServerPin(args[0]))
	case "unpin":
		text(NFSServerUnpin(args[0]))
	case "cache-status":
		text(NFSServerCacheStatus())
	case "set-offline":
		text(NFSServerSetOffline(C.int(req.Ints[0])))
	case "is-offline":
		result.Int = int(NFSServerIsOffline())
	case "network-cost":
		NFSServerSetNetworkCost(C.int(req.Ints[0]), C.int(req.Ints[1]))
	case "log":
		NFSServerLog(args[0])
	case "shutdown-outcome":
		text(NFSServerLastShutdownOutcome())
	case "enroll-verify":
		// The approved-step login check over this process's mount route
		// (bridge/enrollment.go). The strings carry the enrollment secret,
		// like "start" carries the Redis password; neither is logged.
		result.Text = enrollVerifyInServingProcess(req.Strings[0], req.Strings[1], req.Strings[2])
	case enrollHTTPMethod:
		// One enrollment request to the Manager of this process's Redis
		// endpoint, over its route (bridge/enrollment_manager_route.go). A
		// POST body carries the enrollment secret; it is not logged.
		result.Text = enrollHTTPInServingProcess(req.Strings[0], req.Strings[1], req.Strings[2])
	}
	return result
}

// NFSServerLastShutdownOutcome returns the typed reason behind the most
// recent NFSServerShutdown/NFSServerShutdownForced result as JSON
// {"complete":bool,"reason":string,"detail":string}. The Boolean C result of
// those calls stays authoritative for success; this only explains a 0. The
// desktop archive forwards to the serving process, which is the process that
// ran the shutdown. A failed forward returns "error: ..." and the GUI falls
// back to its generic incomplete wording.
//
//export NFSServerLastShutdownOutcome
func NFSServerLastShutdownOutcome() *C.char {
	if serviceClientBuild {
		return forwardServiceText("shutdown-outcome", nil, nil)
	}
	data, err := json.Marshal(lastShutdownOutcome())
	if err != nil {
		return C.CString("error: " + err.Error())
	}
	return C.CString(string(data))
}

func runServingProcess(args []string) error {
	if serviceClientBuild {
		return fmt.Errorf("desktop control archive cannot own serving runtime")
	}
	if len(args) != 1 || args[0] != "--serve" {
		return fmt.Errorf("usage: juicemount-service --serve")
	}
	if os.Geteuid() == 0 {
		return fmt.Errorf("serving service must run as the logged-in user, never root")
	}
	if _, _, err := appisolation.Current(); err != nil {
		return err
	}
	path, err := servicecontrol.SocketPath()
	if err != nil {
		return err
	}
	server, err := servicecontrol.Listen(path)
	if err != nil {
		return err
	}
	servingProcessMode.Store(true)
	recoveryCtx, cancelRecovery := context.WithCancel(context.Background())
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		resumeConfirmedServingOnce(recoveryCtx)
	}()
	// A recovered Start holds lifecycle custody through completion. Join this
	// worker before Close releases the listener's process-lifetime fence.
	serveErr := server.Serve(dispatchServingRequest)
	cancelRecovery()
	<-recoveryDone
	closeErr := server.Close()
	waitErr := server.WaitClosed(context.Background())
	servingProcessMode.Store(false)
	return errors.Join(serveErr, closeErr, waitErr)
}

// c-archive builds do not run main. Only the separately bundled launchd job
// owns the Go engine. Service-process death/reboot still requires the broker's
// durable reconciliation; this boundary protects against GUI death only.
func main() {
	if isRetainedGenerationRecoveryCommand(os.Args[1:]) {
		os.Exit(runRetainedGenerationRecovery(os.Args[1:], os.Stdout, os.Geteuid() == 0, serviceClientBuild))
	}
	if isUnmountOperationStatusCommand(os.Args[1:]) {
		os.Exit(runUnmountOperationStatusCommand(os.Args[1:], os.Stdout, os.Stderr))
	}
	if volumeOwnerMode(os.Args[1:]) {
		if err := runVolumeOwnerProcess(); err != nil {
			fmt.Fprintln(os.Stderr, "juicemount-volume-owner:", err)
			os.Exit(1)
		}
		return
	}
	if err := runServingProcess(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "juicemount-service:", err)
		os.Exit(1)
	}
}

// validateIsolatedServerConfig runs in both the GUI bridge and the serving
// process. The service cannot rely on a well-behaved GUI: same-user callers
// can reach its socket directly. A private service never accepts production
// state roots, default control ports, or an implicit spool directory.
func validateIsolatedServerConfig(cfg ServerConfig) error {
	scope, isolated, err := appisolation.Current()
	if err != nil || !isolated {
		return err
	}
	if _, _, err := appisolation.ResolvePaths(); err != nil {
		return err
	}
	for _, path := range []string{cfg.FUSEPath, cfg.MountPoint, cfg.DBPath, cfg.LogFile, cfg.SpoolDir} {
		if err := scope.RequireChild(path); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		path string
		dir  bool
	}{
		{cfg.DBPath, false}, {cfg.DBPath + "-wal", false}, {cfg.DBPath + "-shm", false},
		{pinStorePath(cfg.DBPath), false}, {pinStorePath(cfg.DBPath) + "-wal", false}, {pinStorePath(cfg.DBPath) + "-shm", false},
		{derivStorePath(cfg.DBPath), false}, {derivStorePath(cfg.DBPath) + "-wal", false}, {derivStorePath(cfg.DBPath) + "-shm", false},
		{linkStateDir(cfg), true},
		{filepath.Join(filepath.Dir(cfg.DBPath), "thumbs"), true},
		{cfg.LogFile, false}, {cfg.SpoolDir, true},
	} {
		if err := scope.RequirePrivateChild(item.path, item.dir); err != nil {
			return err
		}
	}
	fuseMetrics := os.Getenv("JM_APP_ISOLATION_FUSE_METRICS_ADDR")
	if !privateLoopbackAddr(fuseMetrics) || fuseMetrics == health.DefaultFUSEMetricsAddr ||
		cfg.FUSEMetricsAddr != fuseMetrics || !privateLoopbackAddr(cfg.ListenAddr) ||
		!privateLoopbackAddr(cfg.MetricsAddr) || cfg.ListenAddr == "127.0.0.1:11049" ||
		cfg.MetricsAddr == "127.0.0.1:11050" || cfg.ListenAddr == cfg.MetricsAddr ||
		cfg.FUSEMetricsAddr == cfg.ListenAddr || cfg.FUSEMetricsAddr == cfg.MetricsAddr {
		return fmt.Errorf("isolated serving requires distinct explicit loopback ports")
	}
	if cfg.RedisURL == "" || cfg.RedisURL == "redis://127.0.0.1:6379/1" {
		return fmt.Errorf("isolated serving requires an explicit private backend")
	}
	return nil
}

func privateLoopbackAddr(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535
}

// Normal serving requests retain the engine's existing parse path.
func validateIsolatedRaw(raw string) error {
	_, isolated, err := appisolation.Current()
	if err != nil || !isolated {
		return err
	}
	var cfg ServerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return err
	}
	return validateIsolatedServerConfig(cfg)
}
