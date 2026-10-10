// Package health — JuiceFS FUSE mount lifecycle management.
//
// FUSEManager mounts JuiceFS, monitors the mount health, and automatically
// remounts if the FUSE process dies or the mount becomes stale.
package health

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/fusecustody"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/juicefsbin"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
)

// linkAwareJuiceFSPolicy picks the mount-time juicefs prefetch flags
// (--buffer-size / --prefetch) for the current link. JuiceFS's own sequential
// readahead is the dominant prefetcher (validated 2026-06-15: a cold 4 KB read
// pulls the whole file even with our server readahead capped) and these flags
// are mount-time only, so we classify the link HERE, at mount, from a single
// cheap TCP dial to the backend — the same passive philosophy as the
// reachability probe (no synthetic load on a link we may already suspect).
//
// On any parse/dial failure we feed no RTT sample and fall through to the
// profile's default (medium == the historical mount flags), so a transient
// backend hiccup never degrades the mount config.
func (fm *FUSEManager) linkAwareJuiceFSPolicy() netprofile.JuiceFSPolicy {
	if u, err := url.Parse(fm.cfg.RedisURL); err == nil && u.Host != "" && !urlHostIsLoopback(u) {
		start := time.Now()
		conn, derr := net.DialTimeout("tcp", u.Host, 2*time.Second)
		if derr == nil {
			rtt := time.Since(start)
			_ = conn.Close()
			netprofile.Default().ObserveRTT(rtt)
		}
	}
	pol := netprofile.Default().JuiceFS()
	snap := netprofile.Default().Snapshot()

	// Log the DECISION, not just the probe. These flags are mount-time only and
	// never re-track the class afterwards, so this line is the sole record of
	// what the mount actually got and how much the estimator knew when it chose.
	//
	// class_measured matters more than class here. At mount the profile has just
	// been restarted, so it usually has an RTT bootstrap and NO bandwidth
	// sample, and class is BW-driven once samples exist. Measured 2026-08-16
	// across six runs on one 10GbE link: three recorded bandwidth=0 with
	// class_measured=false, and the observed class swung medium/fast (and the
	// bandwidth estimate 0 -> 26 -> 1098 -> 1650 MB/s) while the mount flags
	// stayed frozen at whatever this moment decided. A mount that came up
	// `medium` and a runtime that reports `fast` is not a bug by itself — but it
	// is indistinguishable from one without this line.
	jmlog.Info("link-aware mount policy chosen",
		"class", snap.Class.String(),
		"class_measured", snap.HaveRTT && snap.HaveBW,
		"have_rtt", snap.HaveRTT, "have_bandwidth", snap.HaveBW,
		"rtt_ms", snap.RTT.Milliseconds(),
		"bandwidth_mbps", snap.BytesPerSec/(1024*1024),
		"bootstrapped_from_rtt", snap.BootstrappedRTT,
		"buffer_size_mb", pol.BufferSizeMB,
		"prefetch", pol.Prefetch,
		"nfs_readahead", netprofile.Default().NFSReadahead())
	return pol
}

func urlHostIsLoopback(u *url.URL) bool {
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// redactURLCreds keeps only a URL's scheme and authority for diagnostics.
// Userinfo, paths and arbitrary query values may contain credentials.
func redactURLCreds(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "<invalid URL>"
	}
	return u.Scheme + "://" + u.Host
}

func mountLogArgs(args []string) []string {
	logArgs := append([]string(nil), args...)
	for i, a := range logArgs {
		if a == "mount" && i+1 < len(logArgs) {
			logArgs[i+1] = redactURLCreds(logArgs[i+1])
		}
		if a == "--bucket" && i+1 < len(logArgs) {
			logArgs[i+1] = redactURLCreds(logArgs[i+1])
		}
	}
	return logArgs
}

const desktopJuiceFSHeartbeat = "60s"

func desktopJuiceFSSessionArgs() []string {
	return []string{
		"--heartbeat", desktopJuiceFSHeartbeat,
		"--max-uploads", "4",
	}
}

// desktopJuiceFSObjectIOArgs keeps one fixed 4 MiB JuiceFS block viable on a
// measured cellular path without allowing the upstream default of ten full
// retries to hold a dirty inode indefinitely. At 1 Mbps a block needs at least
// 33.6 seconds before protocol overhead, and a contended phone uplink can take
// materially longer than the upstream 60-second PUT deadline. JuiceMount's
// durable spool owns retry across attempts, so the filesystem client needs
// only a small in-session retry budget.
//
// Fast and medium links retain upstream defaults. JM_JFS_OBJECT_TIMEOUTS=0 is
// the field rollback switch for slow/metered overrides.
func desktopJuiceFSObjectIOArgs(class netprofile.LinkClass) []string {
	if os.Getenv("JM_JFS_OBJECT_TIMEOUTS") == "0" {
		return nil
	}
	switch class {
	case netprofile.ClassMetered:
		return []string{"--get-timeout", "3m", "--put-timeout", "5m", "--io-retries", "2"}
	case netprofile.ClassSlow:
		return []string{"--get-timeout", "2m", "--put-timeout", "3m", "--io-retries", "3"}
	default:
		return nil
	}
}

// desktopJuiceFSReadAheadArgs fixes JuiceFS's immutable per-mount session
// readahead at the conservative floor.  JuiceMount's NFS readahead controller
// is the adaptive layer: it changes depth and concurrency live as measured RTT
// and throughput move between fast, slow, and metered classes.
//
// Leaving JuiceFS's own 32 MiB default enabled on a mount that happened to
// start on Wi-Fi defeats that controller after a Wi-Fi -> cellular handoff:
// JuiceMount can disable its own speculative reads, but the already-running
// JuiceFS session continues pulling ahead until the next remount.  A fixed 1
// MiB ceiling disables that hidden second prefetcher for every startup class;
// fast links still get parallel 4 MiB block reads from JuiceMount's measured,
// live controller.  JM_JFS_MAX_READAHEAD=0 is the field rollback switch.
func desktopJuiceFSReadAheadArgs() []string {
	if os.Getenv("JM_JFS_MAX_READAHEAD") == "0" {
		return nil
	}
	return []string{"--max-readahead", "1M"}
}

// juiceFSChildEnvironment deliberately gives the external JuiceFS daemon a
// small, non-secret environment. The upstream macOS daemon launcher preserves
// its environment in the daemon process title, which makes inherited API keys
// and tokens visible through the process table even though JuiceMount never
// logs them. The desktop mount is fully configured by flags plus the volume
// metadata in Redis, so it does not need arbitrary parent-process variables.
//
// Keep only values required to find the user's cache/home, execute helper
// binaries, localize diagnostics, and validate TLS certificates. Missing
// values are omitted rather than synthesized.
func juiceFSChildEnvironment() []string {
	allowed := [...]string{
		"HOME", "USER", "LOGNAME", "PATH", "TMPDIR",
		"LANG", "LC_ALL", "LC_CTYPE", "TZ",
		"SSL_CERT_FILE", "SSL_CERT_DIR",
	}
	env := make([]string, 0, len(allowed))
	for _, key := range allowed {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return env
}

// juiceFSDirectEnvironment selects JuiceFS's service stage directly instead
// of its built-in launchMount supervisor. The upstream supervisor probes the
// mountpoint every five seconds and SIGKILLs the service after roughly one
// minute without a response. A cellular object request is legitimately allowed
// to run for 60 seconds and retry, so that watchdog can destroy a slow-but-live
// macFUSE session in the middle of useful work. JuiceMount already owns mount
// health, link-aware grace periods, restart backoff, and exact-process cleanup;
// running the service stage directly leaves one lifecycle authority.
//
// The value is intentionally the JuiceMount process ID. JuiceFS currently only
// tests that JFS_SUPERVISOR is non-empty, while retaining the parent identity is
// useful in diagnostics and matches the value set by its own launcher.
//
// metaPassword is the Redis password split out of the metadata URL by
// desktopJuiceFSMountArgs. It is the one credential the child receives, as
// META_PASSWORD (pkg/meta/redis.go reads it when the URL has no password), so
// it never appears in argv, which any local user can read with ps. The
// allowlist above already drops an inherited META_PASSWORD or REDIS_PASSWORD
// (REDIS_PASSWORD would take precedence in juicefs), so no other value can win.
//
// objectKey is this Mac's object-store key from enrollment (H-8B §2.1: the
// volume's `setting` holds no key once the server switches, and stock juicefs
// then reads MINIO_ACCESS_KEY / MINIO_SECRET_KEY, pkg/object/minio.go). It is
// passed only when both halves are present; an inherited MINIO_* is already
// dropped by the allowlist. While `setting` still holds a key, juicefs
// ignores these, so passing them before the switch changes nothing.
//
// The AWS SDK's default credential chain is closed (review 2026-10-06,
// finding 2). With no key in `setting` and none in MINIO_*, juicefs's minio
// backend calls config.LoadDefaultConfig, which reads ~/.aws/credentials and
// ~/.aws/config (HOME is passed through), a credential_process or SSO
// profile named there, and the EC2 instance metadata service. A Mac with an
// AWS default profile for the same MinIO would then mount with that key,
// possibly a delete-capable one. The AWS_* variables the chain reads from
// the environment (AWS_ACCESS_KEY_ID, AWS_PROFILE, AWS_WEB_IDENTITY_TOKEN_FILE,
// AWS_CONTAINER_*...) are already dropped by the allowlist; the three below
// point the file sources at /dev/null and turn off the metadata service.
func juiceFSDirectEnvironment(supervisorPID int, metaPassword string, objectKey ObjectKey) []string {
	env := juiceFSChildEnvironment()
	env = append(env, juiceFSNoAWSCredentialChain...)
	env = append(env, "JFS_SUPERVISOR="+strconv.Itoa(supervisorPID))
	if metaPassword != "" {
		env = append(env, "META_PASSWORD="+metaPassword)
	}
	if objectKey.Complete() {
		env = append(env, "MINIO_ACCESS_KEY="+objectKey.AccessKey, "MINIO_SECRET_KEY="+objectKey.SecretKey)
	}
	return env
}

// juiceFSNoAWSCredentialChain closes every file and network source of the
// AWS SDK default credential chain for a juicefs child (see
// juiceFSDirectEnvironment).
var juiceFSNoAWSCredentialChain = []string{
	"AWS_SHARED_CREDENTIALS_FILE=/dev/null",
	"AWS_CONFIG_FILE=/dev/null",
	"AWS_EC2_METADATA_DISABLED=true",
}

// ObjectKey is an object-store access key pair. It is never logged,
// serialized or hashed (FUSEConfig carries it with json:"-").
type ObjectKey struct {
	AccessKey string
	SecretKey string
}

// Complete reports whether both halves are present.
func (k ObjectKey) Complete() bool { return k.AccessKey != "" && k.SecretKey != "" }

// accessKeyIfComplete is the access-key ID juicefsDirectEnvironment passes,
// "" when it passes none.
func (k ObjectKey) accessKeyIfComplete() string {
	if !k.Complete() {
		return ""
	}
	return k.AccessKey
}

// ObjectKeyStatus is what this process knows about the object key its juicefs
// child uses (H-8 per-Mac keys, design §5 "Mac side"). It holds no secret:
// InUse is the access-key ID only, which MinIO prints in `mc admin` output and
// the Manager already knows.
type ObjectKeyStatus struct {
	// InUse is the access-key ID the last started juicefs child received
	// ("" = none: no child yet, or no key was configured).
	InUse string `json:"object_key_in_use,omitempty"`
	// Refusals counts juicefs log lines since that child started in which
	// the object store refused the key (ObjectKeyRefusalCode). 0 = none.
	Refusals uint64 `json:"refusals,omitempty"`
	// RefusedCode is the S3 error code of the last refusal.
	RefusedCode string `json:"refused_code,omitempty"`
	// RefusedAtMS is the time of the last refusal (Unix ms).
	RefusedAtMS int64 `json:"refused_at_ms,omitempty"`
}

var objectKeyState struct {
	sync.Mutex
	st ObjectKeyStatus
}

// CurrentObjectKeyStatus returns the object-key status of this process's
// juicefs child.
func CurrentObjectKeyStatus() ObjectKeyStatus {
	objectKeyState.Lock()
	defer objectKeyState.Unlock()
	return objectKeyState.st
}

// noteObjectKeyInUse records the key a juicefs child was just started with.
// A new child starts a new record: a refusal belonged to the previous child.
func noteObjectKeyInUse(accessKey string) {
	objectKeyState.Lock()
	objectKeyState.st = ObjectKeyStatus{InUse: accessKey}
	objectKeyState.Unlock()
}

// noteObjectKeyLogLine records a refusal when line is one. It reports
// whether it was.
func noteObjectKeyLogLine(line string, now time.Time) bool {
	code := ObjectKeyRefusalCode(line)
	if code == "" {
		return false
	}
	objectKeyState.Lock()
	objectKeyState.st.Refusals++
	objectKeyState.st.RefusedCode = code
	objectKeyState.st.RefusedAtMS = now.UnixMilli()
	objectKeyState.Unlock()
	return true
}

var (
	// The S3 error codes that mean "this key is not accepted": the key ID
	// is unknown (deleted service account), the secret is wrong, or the
	// policy denies the request. Only the full aws-sdk-go-v2 error form
	// counts (juicefs's s3/minio backend, third_party/juicefs/pkg/object/
	// s3.go; smithy-go OperationError + s3shared.ResponseError + GenericAPIError):
	//
	//	operation error S3: PutObject, [retry text, ]https response error
	//	StatusCode: 403, RequestID: <id>, HostID: <id>, api error <Code>: <msg>
	//
	// A code anywhere else in a line is not a refusal: juicefs also logs
	// file names other users chose (pkg/meta/redis.go "no attribute for
	// inode %d (%d, %s)"), and a file named "InvalidAccessKeyId.mov" must
	// not turn this Mac's status red (review keys-l3 F4).
	objectKeyRefusalRe = regexp.MustCompile(`operation error S3: (\w+), .*?https response error StatusCode: 403, RequestID: [^,]*, HostID: [^,]*, api error (InvalidAccessKeyId|SignatureDoesNotMatch|AccessDenied): `)
	// AccessDenied also answers an operation the Mac's policy never allowed
	// (jm-client has no Delete or multipart abort). Such a line says nothing
	// about the key, so AccessDenied counts only on a data read or write.
	objectKeyDataOps = map[string]bool{
		"PutObject": true, "GetObject": true, "UploadPart": true,
		"CreateMultipartUpload": true, "CompleteMultipartUpload": true,
	}
)

// ObjectKeyRefusalCode returns the S3 error code when a juicefs log line
// reports that the object store refused this Mac's key, else "". Only
// WARNING and ERROR records count (an upload retry is a WARNING,
// cached_store.go "Upload %s: %s (try %d)").
func ObjectKeyRefusalCode(line string) string {
	if !strings.Contains(line, "<WARNING>:") && !strings.Contains(line, "<ERROR>:") {
		return ""
	}
	m := objectKeyRefusalRe.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	if m[2] == "AccessDenied" && !objectKeyDataOps[m[1]] {
		return ""
	}
	return m[2]
}

// desktopJuiceFSMountArgs returns the fixed prefix for the directly supervised
// desktop mount. In particular it must never contain -d/--background: direct
// service mode stays attached to the exact child process JuiceMount starts and
// reaps.
//
// H-8 option B: under the per-project Redis proxy the Mac is an ordinary
// writer. The server's bg principal owns trash, deleted-file and slice cleanup
// and metadata backup, which the proxy refuses from a Mac, so the Mac runs no
// background jobs (--no-bgjob gates them in pkg/meta/base.go) and deletes no
// objects itself (--max-deletes 0; the upstream default is 2). Objects a Mac
// delete leaves behind are reclaimed by the server's gc.
//
// The metadata URL keeps its username (the Mac's principal) but loses its
// password, which is returned for the child environment instead (see
// juiceFSDirectEnvironment). A URL that does not parse is passed unchanged
// with no password; juicefs then refuses it with a redacted error (patch 0006).
func desktopJuiceFSMountArgs(redisURL, mountPoint string, bufferSizeMB, prefetch int) (args []string, metaPassword string) {
	metaURL, metaPassword := SplitMetaPassword(redisURL)
	return []string{
		"mount", metaURL, mountPoint,
		"--no-usage-report",
		"--no-bgjob",
		"--max-deletes", "0",
		"--buffer-size", strconv.Itoa(bufferSizeMB),
		"--prefetch", strconv.Itoa(prefetch),
	}, metaPassword
}

// SplitMetaPassword removes the password from a metadata URL's userinfo and
// returns it decoded. The username, host, database and query are unchanged.
// The bridge uses it too, so nothing it persists carries the password.
func SplitMetaPassword(raw string) (string, string) {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw, ""
	}
	password, ok := u.User.Password()
	if !ok {
		return raw, ""
	}
	if name := u.User.Username(); name != "" {
		u.User = url.User(name)
	} else {
		u.User = nil
	}
	return u.String(), password
}

// readFileTailSince returns at most max bytes written to path since offset.
// Directing the long-lived JuiceFS service to a real file descriptor avoids
// os/exec's hidden pipe-copy goroutines; on a LaunchServices app stderr can be
// undrained, and a forever-lived MultiWriter pipe eventually backpressures the
// filesystem daemon itself. Reading the bounded startup tail only on failure
// preserves actionable errors without putting a pipe in the hot lifecycle.
func readFileTailSince(path string, offset int64, max int) string {
	if path == "" || max < 1 {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() <= offset {
		return ""
	}
	start := st.Size() - int64(max)
	if start < offset {
		start = offset
	}
	buf := make([]byte, st.Size()-start)
	n, _ := f.ReadAt(buf, start)
	return string(buf[:n])
}

// FUSEConfig holds JuiceFS mount configuration.
type FUSEConfig struct {
	RedisURL   string // e.g. "redis://127.0.0.1:6379/1"
	MountPoint string // e.g. ~/.juicemount/fuse-internal
	CacheDir   string // e.g. ~/.juicefs/cache (empty = JuiceFS default)
	CacheSize  string // e.g. "100000" in MiB (empty = JuiceFS default ~100 GiB)
	// PinnedBytes is the total size of all pinned files, in bytes. The
	// cache-size policy (see Mount) grows the cache to at least this so pinned
	// content never LRU-evicts. 0 = unknown / no pins (cache-size stays at the
	// user's configured value).
	PinnedBytes int64
	JuiceFSBin  string // e.g. /opt/homebrew/bin/juicefs (auto-detected if empty)

	// FreeSpaceRatio is the fraction of cache-volume free space JuiceFS keeps
	// reserved (won't cache when below). Default in JuiceFS is 0.1 (10%) which
	// is hostile to video editors who fill their disks with media — once they
	// drop under 90 GB free on a 1 TB disk, the cache silently disables and
	// every read goes straight to S3 with no warning.
	//
	// Empty = pass nothing (JuiceFS default applies). "0.01" = keep 1% free.
	FreeSpaceRatio string

	// OpenCacheTTL is the --open-cache duration, e.g. "5s". Empty = omit the
	// flag (juicefs default 0s = every open re-validates against Redis).
	//
	// THIS TRAVELS IN THE CONFIG, NOT THE ENVIRONMENT, and that is the whole
	// point. Go snapshots os.Environ at c-archive init, so a host-side setenv()
	// from Swift is invisible to os.Getenv (see ServerController.start). Every
	// JM_* knob in this file is therefore UNREACHABLE from a shipped app — only
	// its default ships. That is harmless for the knobs whose default is the
	// wanted behaviour, and fatal for this one, whose default is OFF: the
	// measured 66x open win (2026-07-29 cellular: first open 1226-5660ms,
	// second 24-36ms) could not be switched on by the person running the test.
	// JM_WAN_MODE died exactly this way — read in six paths, set by nothing.
	OpenCacheTTL string

	// BucketOverride is the S3 endpoint URL the Mac juicefs daemon should
	// use, overriding whatever bucket URL was stored in Redis at format
	// time. Empty = no override; juicefs reads the URL from Redis.
	//
	// Why this exists: JuiceFS stores the bucket URL in Redis when the
	// volume is formatted. On a docker-bridge-networked server-side
	// install the format URL is the docker-internal DNS name (e.g.
	// http://minio:9000/zpool) which the Mac cannot resolve. Setting
	// BucketOverride to http://<truenas-lan-ip>:30151/zpool lets the
	// Mac client reach the server's MinIO directly without /etc/hosts
	// hacks or macvlan networking on the server.
	//
	// Passed as the `--bucket` flag on `juicefs mount`, which wins over
	// the Redis-stored URL.
	BucketOverride string

	// FUSEMetricsAddr is the host:port the JuiceFS mount daemon binds its
	// Prometheus /metrics endpoint to, passed explicitly as `--metrics`. The
	// bridge scrapes `juicefs_blockcache_bytes` from this endpoint to report
	// the TRUE on-disk block-cache size as `cache_used_bytes` in /cache-status
	// (replacing the pinned-only aggregate + the racy cache-dir du).
	//
	// Why explicit, and why NOT :9567: JuiceFS defaults the mount metrics to
	// 127.0.0.1:9567, but the `juicefs sync`/manager path (internal/manager/
	// sync.go) ALSO pins :9567. Relying on the mount's default would collide
	// with a concurrent sync's metrics listener (the documented loopback-port
	// collision). We bind the mount to a distinct default of 127.0.0.1:9568.
	//
	// Empty = pass nothing; JuiceFS falls back to its own :9567 default. The
	// bridge only scrapes the addr it knows about, so an empty value just
	// means cache_used_bytes uses the dir-walk fallback.
	FUSEMetricsAddr string

	// ObjectKey is this Mac's object-store key (enrollment), given to the
	// juicefs child as MINIO_ACCESS_KEY / MINIO_SECRET_KEY, never argv.
	// Excluded from JSON so generationConfigDigest never hashes it.
	ObjectKey ObjectKey `json:"-"`
}

// DefaultFUSEMetricsAddr is the loopback host:port the FUSE mount daemon binds
// its Prometheus metrics to when FUSEConfig.FUSEMetricsAddr is empty. It is
// deliberately :9568 — distinct from the `juicefs sync` path's :9567 — to avoid
// the documented loopback-port collision between a running mount and a
// concurrent sync.
const DefaultFUSEMetricsAddr = "127.0.0.1:9568"

// FUSEManager manages the JuiceFS FUSE mount lifecycle.
type FUSEManager struct {
	cfg                 FUSEConfig
	mu                  sync.Mutex
	cacheVolumePath     string // configured cache root, guarded by mu
	cacheVolumeDev      uint64 // device of that location's existing ancestor
	cacheFloorBytes     int64  // exact selected --free-space-ratio reserve
	cachePolicyKnown    bool   // false when volume or selected reserve was unmeasurable
	cachePolicyMeasured bool   // selected candidate, published only after custody admission
	stopCh              chan struct{}
	done                chan struct{}
	stopping            atomic.Bool
	stopOnce            sync.Once
	monitorState        atomic.Int32           // 0 unstarted, 1 running, 2 stopped before start
	activeReceipt       *MountReceipt          // guarded by mu, including incomplete launches
	generation          *GenerationReservation // durable physical-path intent; held until proven retirement
	callbackMu          sync.Mutex
	onRemount           func()
	callbacksClosed     bool
	callbacksActive     int
	callbacksIdle       chan struct{}
	helperMu            sync.Mutex
	helperClosed        bool
	helperActive        int
	helperIdle          chan struct{}
	// observeMount is injected only by health package tests. Production uses
	// the sibling standalone kernel observer, never an in-process mount syscall.
	observeMount     func(context.Context) (mounttable.Snapshot, error)
	dataPlaneProbe   func(string, time.Duration) bool        // test-only; nil uses the bounded child probes
	spotlightWrite   func(string, []byte, os.FileMode) error // test-only; nil uses os.WriteFile
	capabilityProbe  *fusecustody.CapabilityProbe
	custodyPreflight func() error // test-only capability seam
	// unmountd is the privileged unmount helper (H-3 L5); nil keeps the
	// descriptor-only behaviour. Set before Mount.
	unmountd UnmountHelper
	// helperView is the newest child's helper receipt, readable without
	// fm.mu for /health.
	helperView atomic.Pointer[fuseHelperReceipt]
}

// MountReceipt owns only the foreground child started by one MountWithReceipt
// call. An adopted mount has no receipt. It is deliberately independent of
// the mountpoint, whose current occupant may change before cleanup runs.
type MountReceipt struct {
	owner      *FUSEManager
	generation *GenerationReservation
	process    *os.Process
	waitDone   <-chan struct{}
	// A complete kernel observation is bound to this exact child before root
	// probes. It is process-local readiness evidence, not detach authority.
	readinessBoot   string
	readinessSource string
	readinessFSID   [2]int32
	tailStop        chan struct{}
	tailDone        <-chan struct{}
	observerDone    <-chan struct{}
	custody         fuseCustodian
	custodyBoot     string
	custodySource   string
	custodyFSID     [2]int32
	custodyUUID     string
	custodyRecorded bool
	exitErr         error
	tailStopOnce    sync.Once
	// juiceFSLogStart is the size of ~/.juicefs/juicefs.log just before
	// this child started (-1 = unknown). The log tail reads from there, so
	// the lines the child writes during mount verification (up to a
	// minute) are not skipped (review keys-l3 F5).
	juiceFSLogStart int64
	// custodyStartedAt is when the current custodian began its root open;
	// a pending admission hands off to the helper after
	// FUSEPendingHelperHandoff measured from here (design Q5).
	custodyStartedAt time.Time
	// helper is this child's unmount-helper receipt (H-3 L5); nil until a
	// registration was attempted. Its fields have their own lock.
	helper *fuseHelperReceipt
}

// Rollback enters the owner's typed teardown. A failed/uncertain teardown
// retains the receipt and dependencies for retry; it never kills by PID/path.
func (r *MountReceipt) Rollback() error {
	if r == nil {
		return nil
	}
	if r.owner == nil {
		if r.process == nil && r.waitDone == nil && r.tailDone == nil {
			return nil
		}
		return errors.New("FUSE child receipt has no lifecycle owner")
	}
	return r.owner.rollbackReceipt(r)
}

// SetOnRemount registers a callback invoked after every successful
// watchdog-driven remount. A caller-serialized new soft-stop session may
// rearm only after the previous callback has joined. Hard Stop never rearms.
func (fm *FUSEManager) SetOnRemount(fn func()) {
	fm.callbackMu.Lock()
	defer fm.callbackMu.Unlock()
	if fm.stopping.Load() {
		return
	}
	if fm.callbacksClosed {
		if fn == nil || fm.callbacksActive != 0 {
			return
		}
		fm.callbacksClosed = false
	}
	fm.onRemount = fn
}

// fireOnRemount invokes the registered callback (if any) without holding mu.
func (fm *FUSEManager) fireOnRemount() {
	fm.callbackMu.Lock()
	if fm.callbacksClosed || fm.stopping.Load() || fm.onRemount == nil {
		fm.callbackMu.Unlock()
		return
	}
	fn := fm.onRemount
	if fm.callbacksActive == 0 {
		fm.callbacksIdle = make(chan struct{})
	}
	fm.callbacksActive++
	fm.callbackMu.Unlock()
	defer func() {
		fm.callbackMu.Lock()
		fm.callbacksActive--
		if fm.callbacksActive == 0 {
			close(fm.callbacksIdle)
		}
		fm.callbackMu.Unlock()
	}()
	fn()
}

// EffectiveCacheSize returns the cache-size string actually passed to the
// JuiceFS daemon (post auto-expansion). Used by callers that want to log
// the effective value rather than the user's original input.
func (fm *FUSEManager) EffectiveCacheSize() string {
	return fm.cfg.CacheSize
}

// EffectiveMetricsAddr returns the host:port the FUSE daemon's Prometheus
// metrics endpoint is bound to (the value passed as `--metrics`), filling in
// DefaultFUSEMetricsAddr when the config left it empty. The bridge uses this to
// tell the block-cache scraper where to find juicefs_blockcache_bytes.
func (fm *FUSEManager) EffectiveMetricsAddr() string {
	if fm.cfg.FUSEMetricsAddr != "" {
		return fm.cfg.FUSEMetricsAddr
	}
	return DefaultFUSEMetricsAddr
}

// lastNonEmptyLine returns the last non-blank line of s, trimmed. juicefs
// prints progress/info lines before the fatal one, so the final non-empty
// line is the actionable reason. Returns "" if s has no non-blank content.
// juiceFSStartupFailureReason turns the failed child's log tail into the
// reason reported for the mount failure.
//
// H-8: the per-project Redis proxy refuses an unauthenticated client, or a
// wrong password, with "-JMDENY AUTH_REQUIRED ..." on every command, and the
// bundled juicefs exits at once with that line as its FATAL. Retrying cannot
// fix it, so say what can: enrolment approval or the saved password.
func juiceFSStartupFailureReason(tail string) string {
	last := lastNonEmptyLine(tail)
	i := strings.Index(last, "JMDENY ")
	if i < 0 {
		return last
	}
	fields := strings.Fields(last[i+len("JMDENY "):])
	if len(fields) == 0 {
		return last
	}
	if fields[0] == "AUTH_REQUIRED" {
		return "the metadata server refused this Mac's sign-in (JMDENY AUTH_REQUIRED): " +
			"approve this Mac in the JuiceMount Manager, or enter its Redis password in Settings > Connection"
	}
	return "the metadata server refused this Mac (JMDENY " + fields[0] + ")"
}

func lastNonEmptyLine(s string) string {
	lines := strings.Split(s, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(lines[i]); t != "" {
			return t
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// macFUSE kext-approval classification (V2.3 G0d, 2026-07-01).
//
// A macOS update silently revokes the macFUSE system-extension approval
// (kernelmanagerd: "Extension io.macfuse.filesystems.macfuse.N not approved
// to load"). From then on EVERY juicefs mount hangs at "Mounting volume…"
// and dies with "mount point is not ready in 10 seconds" — an error
// indistinguishable from a slow backend unless the kext state is checked.
// No amount of retrying can succeed; only the user can fix it (System
// Settings approval). Classify once per failure so the log carries the exact
// remediation and /health can surface it instead of a generic "no FUSE".

// kextBlockedFlag latches "mount failed AND the macFUSE kext is not loaded".
// Cleared on any successful mount. Read by the health monitor's FUSE check.
var kextBlockedFlag atomic.Bool

// KextApprovalBlocked reports whether the last mount failure was classified
// as the macFUSE kext being unloaded (approval loss). Cheap; any goroutine.
func KextApprovalBlocked() bool { return kextBlockedFlag.Load() }

// noteMountFailure classifies a mount verification failure. A missing kext is
// actionable only after Redis and object storage both answer their application
// protocols. A Link proxy can accept a loopback TCP connection while its
// encrypted route is still unavailable, so TCP-only reachability would falsely
// blame macFUSE for a backend outage.
func (fm *FUSEManager) noteMountFailure() {
	if kextLoaded() {
		kextBlockedFlag.Store(false)
		return
	}
	redisReady, objectReady := fm.mountBackendsReady()
	if !shouldClassifyKextApproval(false, redisReady, objectReady) {
		kextBlockedFlag.Store(false)
		jmlog.Warn("mount failed before backend readiness; macFUSE approval not inferred",
			"redis_ready", redisReady, "object_store_ready", objectReady)
		return
	}
	if kextBlockedFlag.CompareAndSwap(false, true) {
		jmlog.Error("macFUSE kext is NOT loaded — juicefs mount cannot succeed until it is approved. " +
			"Likely cause: a macOS update reset the system-extension approval. " +
			"USER ACTION: System Settings → Privacy & Security → Allow \"Benjamin Fleischer\" (macFUSE), then reboot if prompted. " +
			"Until then the FUSE identity gate parks drains and prunes; writes stay safely in the spool.")
	}
}

// shouldClassifyKextApproval centralizes the high-confidence gate so regression
// tests can prove a backend failure is never presented as an approval loss.
func shouldClassifyKextApproval(kextIsLoaded, redisReady, objectReady bool) bool {
	return !kextIsLoaded && redisReady && objectReady
}

func (fm *FUSEManager) mountBackendsReady() (redisReady, objectReady bool) {
	if fm == nil {
		return false, false
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		redisReady = redisMountBackendReady(fm.cfg.RedisURL)
	}()
	go func() {
		defer wg.Done()
		objectReady = objectMountBackendReady(fm.cfg.BucketOverride)
	}()
	wg.Wait()
	return redisReady, objectReady
}

func redisMountBackendReady(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	opts, err := redisurl.Parse(raw)
	if err != nil || opts.Addr == "" {
		return false
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	// This is a single compatibility PING, not a long-lived feature client.
	// RESP2 avoids a HELLO negotiation and identity command on a backend that
	// may be an older Redis-compatible JuiceFS metadata service.
	opts.Protocol = 2
	opts.DisableIdentity = true
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.Ping(ctx).Err() == nil
}

func objectMountBackendReady(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	u.Path = "/minio/health/live"
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// kextLoaded reports whether any macFUSE kext is currently loaded, bounded.
// Returns true (= "can't claim blocked") when kmutil fails or times out.
func kextLoaded() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kmutil", "showloaded").Output()
	if err != nil {
		return true
	}
	return strings.Contains(strings.ToLower(string(out)), "macfuse")
}

// NewFUSEManager creates a FUSE mount manager.
func NewFUSEManager(cfg FUSEConfig) *FUSEManager {
	if cfg.JuiceFSBin == "" {
		cfg.JuiceFSBin = findJuiceFSBin()
	}
	return &FUSEManager{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// NewGenerationFUSEManager reserves one durable physical path before a caller
// publishes it to FUSE-dependent consumers. A failed reservation starts no child.
// A failed birth consumes the path; automatic replacement remains closed until
// a generation-aware frontend barrier and exact custody join are implemented.
func NewGenerationFUSEManager(cfg FUSEConfig) (*FUSEManager, error) {
	paths, isolated, scopeErr := appisolation.ResolvePaths()
	if scopeErr != nil {
		return nil, scopeErr
	}
	if isolated && cfg.CacheDir != paths.CacheBase {
		return nil, errors.New("isolated FUSE cache must use the private cache base")
	}
	if isolated {
		if err := os.MkdirAll(paths.CacheBase, 0700); err != nil {
			return nil, err
		}
		if _, _, err := appisolation.ResolvePaths(); err != nil {
			return nil, err
		}
	}
	fm := NewFUSEManager(cfg)
	reservation, err := reserveFUSEGeneration(cfg.MountPoint, generationConfigDigest(cfg), fm.observeFUSE)
	if err != nil {
		return nil, err
	}
	fm.generation = reservation
	fm.cfg.MountPoint = reservation.Path
	fm.unmountd = defaultUnmountHelper()
	return fm, nil
}

// MountPoint is the reserved physical child path. The requested path remains
// in the caller's logical configuration and in the reservation record.
func (fm *FUSEManager) MountPoint() string { return fm.cfg.MountPoint }

// GenerationAttempted reports whether the reserved path has been consumed by
// a child launch attempt. The current watchdog cannot safely replace it.
func (fm *FUSEManager) GenerationAttempted() bool {
	if fm == nil {
		return false
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.generation != nil && fm.generation.spent
}

// Mount starts the JuiceFS FUSE mount. Blocks until the mount is verified live.
func (fm *FUSEManager) Mount() error {
	_, err := fm.MountWithReceipt()
	return err
}

// startFUSEChildWaiter owns the sole cmd.Wait from the moment Start succeeds.
// Closing waitDone publishes exitErr to Stop and failed-birth callers.
func startFUSEChildWaiter(cmd *exec.Cmd, logFile *os.File, receipt *MountReceipt) <-chan error {
	processExit := make(chan error, 1)
	waitDone := make(chan struct{})
	receipt.waitDone = waitDone
	go func() {
		err := cmd.Wait()
		if logFile != nil {
			_ = logFile.Close()
		}
		receipt.exitErr = err
		processExit <- err
		close(waitDone)
	}()
	return processExit
}

// MountWithReceipt returns a rollback receipt only when this call started a
// child, including when subsequent readiness verification fails.
func (fm *FUSEManager) MountWithReceipt() (*MountReceipt, error) {
	if fm.stopping.Load() {
		return nil, errors.New("FUSE manager is stopping")
	}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopping.Load() {
		return nil, errors.New("FUSE manager is stopping")
	}

	if fm.cfg.JuiceFSBin == "" {
		return nil, fmt.Errorf("juicefs binary not found")
	}
	if fm.generation != nil && fm.generation.spent {
		return nil, errors.New("generation path already attempted; replacement requires a joined generation handoff")
	}

	// A complete observation is mandatory before every creation, including
	// watchdog recovery. A present/unknown occupant is never replaced by path.
	if err := fm.prepareMountLocked(); err != nil {
		return nil, err
	}
	if fm.custodyPreflight != nil {
		if err := fm.custodyPreflight(); err != nil {
			return nil, fmt.Errorf("FUSE custody capability: %w", err)
		}
	} else {
		executable, err := os.Executable()
		if err != nil {
			return nil, err
		}
		probe, err := fusecustody.StartCapability(filepath.Join(filepath.Dir(executable), "juicemount-mount-probe"))
		if err != nil {
			return nil, fmt.Errorf("FUSE custody capability start: %w", err)
		}
		fm.capabilityProbe = probe
		if err := probe.Wait(3 * time.Second); err != nil {
			return nil, fmt.Errorf("FUSE custody capability: %w", err)
		}
		fm.capabilityProbe = nil
	}
	// Only a proven clear path may be traversed or created. An unavailable
	// observer cannot even trigger a mountpoint MkdirAll.
	if err := os.MkdirAll(fm.cfg.MountPoint, 0755); err != nil {
		return nil, fmt.Errorf("create mount point: %w", err)
	}

	// Never reclaim storage on startup. ReclaimPurgeableSpace thins Time
	// Machine local snapshots and can take up to 90 seconds; doing that before
	// mount made an ordinary app launch both destructive and Finder-blocking.
	// The cache-size/free-space-ratio policy below already preserves the hard
	// disk floor. Low space is surfaced in health/UI, and the user may choose
	// the explicit Reclaim action if they want to trade snapshots for cache.

	// Cache-size policy (2026-06-08). Replaces the prior "max(configured, 85%
	// of disk)" auto-expansion, which IGNORED the user's configured size — it
	// treated the app setting as a FLOOR and grew the cache to fill the disk
	// (observed: 90 GB configured → 787 GB), with --free-space-ratio as the
	// only real limit. New policy:
	//   1. RESPECT the user's configured cache-size.
	//   2. Grow it ONLY as far as needed to keep the pinned set fully cached
	//      (fm.cfg.PinnedBytes) — so a 158 GB pinned project doesn't LRU-evict
	//      the blocks the user paid to download (the original concern), without
	//      blowing the cache past what's actually pinned.
	//   3. NEVER squeeze the cache/boot disk below a hard 10 GiB free floor — a
	//      near-full boot disk causes system-wide instability. The nominal
	//      cache-size is clamped to (total − 10 GiB), and --free-space-ratio is
	//      raised so JuiceFS dynamically keeps ≥10 GiB free at write time (the
	//      REAL guarantee: it also covers OTHER files filling the disk after
	//      mount, which a static cap cannot).
	const cacheFreeFloorBytes = cacheFreeFloorBytesConst
	fm.cachePolicyKnown, fm.cachePolicyMeasured, fm.cacheFloorBytes = false, false, 0
	cacheProbePath := ""
	fm.cacheVolumePath, cacheProbePath, fm.cacheVolumeDev = cacheVolumeLocation(fm.cfg.CacheDir)
	pin.SetCachePolicy(0, 0, fm.cacheVolumePath, fm.cacheVolumeDev, false)
	if total, err := volumeTotalBytes(cacheProbePath); err == nil && total > 0 && fm.cacheVolumeDev != 0 {
		free, freeErr := volumeFreeBytes(cacheProbePath)
		if freeErr != nil {
			free = 0
		}
		var configuredMiB int64
		fmt.Sscanf(fm.cfg.CacheSize, "%d", &configuredMiB)

		effectiveMiB, seededBytes := resolveCacheSizeMiB(configuredMiB, fm.cfg.PinnedBytes, total)
		if effectiveMiB > 0 && effectiveMiB != configuredMiB {
			fm.cfg.CacheSize = fmt.Sprintf("%d", effectiveMiB)
			jmlog.Info("cache-size resolved",
				"configured_gb", seededBytes>>30,
				"pinned_gb", fm.cfg.PinnedBytes>>30,
				"effective_gb", (effectiveMiB<<20)>>30,
				"disk_total_gb", total>>30,
				"reason", "max(configured, pinned) clamped to keep >=10GiB free")
		}

		// R-1 startup diagnostic: if the pinned set is larger than the disk can
		// physically keep resident (free space + the floor it must preserve),
		// it will never fully cache no matter how high cache-size goes —
		// JuiceFS evicts to honor --free-space-ratio. The live verdict (pin.
		// CapacityLoop) surfaces this to the user; logging it here makes the
		// condition obvious at mount time. Compares against FREE disk, not
		// total — the prior policy's blind spot.
		if free > 0 {
			if sustainable := free - cacheFreeFloorBytes; sustainable > 0 && fm.cfg.PinnedBytes > sustainable {
				jmlog.Warn("pinned set exceeds free disk — cannot stay fully cached offline",
					"pinned_gb", fm.cfg.PinnedBytes>>30,
					"free_gb", free>>30,
					"sustainable_gb", sustainable>>30,
					"shortfall_gb", (fm.cfg.PinnedBytes-sustainable)>>30,
					"action", "free disk space or unpin a folder")
			}
		}

		// Raise --free-space-ratio so JuiceFS's EVICTION floor sits ABOVE the
		// write spool's ADMISSION floor (B-2 — see resolveFreeSpaceRatio). The
		// old value was cacheFreeFloorBytes/total, i.e. JuiceFS only started
		// evicting below 10 GiB free while the spool already stopped admitting
		// at 20 GiB — so the spool blocked itself first and the cache never gave
		// the space back. resolveFreeSpaceRatio still guarantees the >= 10 GiB
		// free floor this line originally existed for. max() so we never weaken
		// an already-stricter configured ratio.
		derivedRatio := resolveFreeSpaceRatio(total, free)
		if ratioArg, replace := resolveFreeSpaceRatioArg(fm.cfg.FreeSpaceRatio, total, free); replace {
			fm.cfg.FreeSpaceRatio = ratioArg
		}
		// PUBLISH the floor we are actually mounting with, including a stricter
		// configured value. The UI and pin-capacity guard must agree with the
		// exact JuiceFS argument rather than a historical constant or the raw
		// derived value before wire rounding.
		var effectiveRatio float64
		ratioCount, ratioErr := fmt.Sscanf(fm.cfg.FreeSpaceRatio, "%f", &effectiveRatio)
		ratioKnown := ratioCount == 1 && ratioErr == nil && effectiveRatio > 0 && effectiveRatio < 1
		if effectiveRatio <= 0 {
			effectiveRatio = derivedRatio
		}
		effectiveFloor := int64(effectiveRatio * float64(total))
		if effectiveFloor > 0 {
			budgetMiB, budgetErr := strconv.ParseInt(fm.cfg.CacheSize, 10, 64)
			fm.cachePolicyMeasured = freeErr == nil && ratioKnown && budgetErr == nil && budgetMiB > 0 && budgetMiB <= (1<<63-1)>>20
			if fm.cachePolicyMeasured {
				fm.cacheFloorBytes = effectiveFloor
			}
			preferredFloor := spoolFreeFloorBytesConst + cacheYieldHeadroomBytes
			jmlog.Info("free-space-ratio resolved to keep the JuiceFS eviction floor above the spool floor",
				"ratio", fm.cfg.FreeSpaceRatio,
				"keeps_free_gb", effectiveFloor>>30,
				"current_free_gb", free>>30,
				"cache_window_mb", max(int64(0), free-effectiveFloor)>>20,
				"spool_floor_gb", spoolFreeFloorBytesConst>>30,
				"adaptive", free > 0 && effectiveFloor < preferredFloor,
				"disk_total_gb", total>>30)
		}
	}

	// Build juicefs mount command
	// The FUSE mount is an internal implementation detail — users interact
	// only with the NFS volume at /Volumes/zpool. We hide the FUSE mount:
	//   - Mount point is ~/.juicemount/fuse-internal (hidden dotdir, not /Volumes)
	//   - nobrowse: tells macOS not to show the volume in Finder sidebar
	args := []string{}
	// QA-34 Slice 2 (2026-05-25): --verbose adds debug-level logging
	// to ~/.juicefs/juicefs.log. Useful for investigating write-path
	// wedges. Gated behind the JM_FUSE_VERBOSE env var so production
	// users don't pay the cost (per-FUSE-op log overhead + tens of MB
	// of log data per write burst). Set JM_FUSE_VERBOSE=1 for debug
	// sessions.
	if os.Getenv("JM_FUSE_VERBOSE") != "" {
		args = append(args, "--verbose")
	}
	// Link-aware prefetch (#16 phase 2): juicefs's own sequential readahead is
	// the dominant prefetcher; pick --buffer-size/--prefetch from the measured
	// link so a metered/cellular mount doesn't pull whole files for a 4 KB touch,
	// while a 10GbE mount keeps blocks in flight. Medium == historical defaults.
	jfp := fm.linkAwareJuiceFSPolicy()
	// --buffer-size caps JuiceFS's in-memory DIRTY-data ceiling, which is
	// exactly what a shutdown/remount FlushAll must push to MinIO before the
	// mount worker exits (2026-07-10: a 1 GB slow-class buffer = a 4m17s flush
	// over a cellular tunnel = a ~3-min mount outage on any restart). The class
	// policy already shrinks it on slow/metered links; JM_JFS_BUFFER_MB is the
	// absolute field-override (32-8192; set it to the old per-class value to
	// revert). Writes land on the spool first, so a smaller buffer never risks
	// durability — it only backpressures the drainer to MinIO throughput.
	bufMB := jfp.BufferSizeMB
	if v := os.Getenv("JM_JFS_BUFFER_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 32 && n <= 8192 {
			bufMB = n
		}
	}
	mountArgs, metaPassword := desktopJuiceFSMountArgs(
		fm.cfg.RedisURL, fm.cfg.MountPoint, bufMB, jfp.Prefetch,
	)
	args = append(args, mountArgs...)
	// The benchmark that selected jfp also determines whether fixed-size object
	// blocks need cellular-safe deadlines. Append these before starting JuiceFS
	// so the very first spool drain cannot inherit a guaranteed 60-second PUT
	// timeout/retry loop.
	args = append(args, desktopJuiceFSObjectIOArgs(netprofile.Default().Class())...)
	// A JuiceFS session expires at five heartbeat intervals. The upstream
	// 12s default therefore lets another always-on client reap this Mac
	// after only 60s. A cold or cellular S3 PUT can legitimately consume
	// that entire window; the live RC test reproduced the GPU worker
	// deleting the Mac session during the first 10 MiB write. Five minutes
	// protects slow-but-progressing writes without enabling writeback or
	// weakening durability. Metadata liveness is still monitored by
	// JuiceMount's independent, seconds-scale reachability checks.
	// Twenty simultaneous uploads can monopolize a cellular uplink and inflate
	// latency for Redis/Finder control traffic. Four 4 MiB flows still provide
	// ample bandwidth-delay-product on LAN and WAN while bounding queueing
	// pressure across a Wi-Fi -> cellular handoff.
	args = append(args, desktopJuiceFSSessionArgs()...)
	args = append(args, juiceFSMountPlatformOptions(runtime.GOOS)...)
	// Keep JuiceFS's mount-time-only prefetcher at its conservative floor on
	// every startup class.  The live, measured controller in nfs/readahead.go
	// supplies parallel depth on fast links and can actually react to a later
	// Wi-Fi <-> cellular handoff.  See desktopJuiceFSReadAheadArgs.
	args = append(args, desktopJuiceFSReadAheadArgs()...)
	// Bind the Prometheus metrics endpoint EXPLICITLY so the bridge can scrape
	// `juicefs_blockcache_bytes` (the true on-disk block-cache size) for the
	// cache_used_bytes field. We do NOT rely on JuiceFS's :9567 default — that
	// collides with the `juicefs sync`/manager metrics addr (the documented
	// loopback-port collision); DefaultFUSEMetricsAddr is :9568. See FUSEConfig.
	metricsAddr := fm.cfg.FUSEMetricsAddr
	if metricsAddr == "" {
		metricsAddr = DefaultFUSEMetricsAddr
	}
	args = append(args, "--metrics", metricsAddr)
	// DATA-LOSS FIX (2026-06-13): --writeback is DISABLED by default.
	//
	// With --writeback, a close/fsync on a FUSE file returns once the bytes are
	// in JuiceFS's LOCAL writeback cache; the MinIO upload happens async and may
	// lag by seconds. The drainer fsyncs the FUSE dest and then deletes the
	// spool safety-copy + frees capacity (nfs/spool.go MarkDrainComplete),
	// trusting that fsync = durable. It is NOT: at that instant the only durable
	// copy is a dirty writeback-cache block. A crash / power-loss / cache
	// eviction / juicefs SIGKILL in that window destroys an already-"drained"
	// photo with no recovery source — the spool copy is gone. A kill-9 crash
	// test CONFIRMED this: 5 of 50 drained Canon CR3s came back corrupt in MinIO.
	//
	// The write SPOOL already provides the local write buffer + burst absorption
	// that --writeback was added for (writes land on the spool SSD at local
	// speed; the drainer uploads in the background). WITHOUT --writeback the
	// drainer's close waits for the real MinIO PUT, so the data is confirmed
	// durable before the spool copy is deleted. Drain is then honestly
	// upload-bound, which is correct — you cannot acknowledge a photo as stored
	// faster than you can durably store it; the spool + capacity backpressure
	// pace ingest to that rate. Re-enable only if you accept the crash window:
	if os.Getenv("JM_FUSE_WRITEBACK") == "1" {
		args = append(args, "--writeback")
	}

	// Slice H — WAN mode. JuiceFS default --max-uploads is 20; on a
	// high-RTT path (Tailscale, cellular, distant MinIO) 20 concurrent
	// PUTs is the throughput cap and the upload pipe stays
	// underutilized. JM_WAN_MODE=1 bumps it to 64 — bandwidth-delay-
	// product math: at 100 ms RTT, 20×4MB chunks = 80 MB in flight
	// (~6 Gbps if uplink could deliver); 64×4MB = 256 MB in flight,
	// enough headroom for typical home/cellular uplinks where the
	// pipe is the real cap, not concurrency.
	//
	// Off by default. On a LAN, 20 is already over-saturating, more
	// makes no difference and may worsen tail latency.
	if os.Getenv("JM_WAN_MODE") == "1" {
		args = append(args, "--max-uploads", "64")
	}
	// Metadata caching. JuiceFS defaults the attr/entry/dir-entry caches to 1s,
	// so any cold or 1s-expired FUSE metadata access re-validates against Redis
	// — a full RTT, and the dominant navigation cost (≈82% of FUSE op time was
	// `lookup` in profiling). A modest cache (default 5s on LAN, 60s on WAN)
	// cuts that round-trip chatter — which ALSO shrinks the window in which a
	// transient backend/link blip can wedge a FUSE op (the cause of the Finder
	// "not responding" reports). `--negative-entry-cache` caches NEGATIVE
	// lookups so the storm of `._*` AppleDouble probes (~18k ENOENT observed)
	// stops re-hitting Redis on every miss.
	//
	// SAFETY (audited 2026-06-08): JuiceMount serves Stat/ReadDir from its
	// SQLite mirror first — FUSE is fallback-only (ARCHITECTURE:152) — and the
	// kernel NFS client already serves attrs up to actimeo=3600s stale, so a
	// few-seconds FUSE cache changes nothing user-visible. The only consumer of
	// FUSE metadata freshness is the phantom-purge / reconcile-prune Lstat
	// (handler.go phantom-purge, redis.go prune): a longer cache only makes
	// those LAZIER, the strictly-safe direction — those were OVER-aggressive
	// bugs (QA-19/30/32/35), never under-aggressive. JuiceMount's own writes
	// update SQLite directly and don't depend on FUSE-cache freshness. 60s is
	// reserved for WAN to keep the worst-case phantom-linger ≤5s on LAN.
	// Tunable via JM_META_CACHE_SECS.
	// 2026-07-28 — THE NAVIGATION FIX, and why it is unconditional.
	//
	// The 60s branch below used to be gated on JM_WAN_MODE=1. That knob is read
	// in six production paths and SET IN NOTHING BUT TESTS — no Swift setenv, no
	// LSEnvironment entry, no shell wrapper. So every shipping build has run
	// with a 5s TTL on ALL FOUR caches, and the WAN fix written here has never
	// once been active for a user. On a LAN that is invisible (5s revalidation
	// costs ~0.4ms). On cellular each revalidation is a ~250ms round trip, which
	// is exactly the reported symptom: navigating a directory is instant while
	// OFFLINE (never touches the network) and terrible while ONLINE (re-validates
	// against Redis every 5 seconds per entry).
	//
	// It is unconditional rather than link-conditional because these flags are
	// fixed AT MOUNT TIME: juicefs cannot be re-tuned when the link changes, so
	// a class-gated value would just bake in whatever the link looked like at
	// startup and be wrong after the first WiFi<->cellular switch. Picking one
	// value that is correct everywhere beats picking the wrong one adaptively.
	//
	// SAFETY — unchanged from the 2026-06-08 audit above, which already cleared
	// the 60s value: JuiceMount serves Stat/ReadDir from its own mirror first and
	// FUSE is fallback-only, the kernel NFS client already serves attrs up to
	// actimeo=3600s stale, and the only consumer of FUSE metadata freshness is
	// the phantom-purge / reconcile-prune Lstat, which a longer cache makes
	// LAZIER — the strictly-safe direction, and those were over-aggressive bugs
	// (QA-19/30/32/35), never under-aggressive.
	//
	// SPLIT, rather than one scalar for all four (audit P4):
	//   entry / dir-entry 60s — the navigation win; these are what a directory
	//     listing revalidates, and staleness here only delays a NEWLY ADDED file
	//     becoming visible. Leland explicitly accepted that trade for throttled
	//     links ("compromise of delay before new files show up ... is fine").
	//   attr 10s — deliberately NOT 60s. attr-cache backs LiveSize(), the
	//     anti-staleness authority for #85 (OpenLoupe verified-offload reads a
	//     transient short size after a big write, hashes it, and discards a good
	//     file). A 60s attr cache widens that window by 6x; 10s still removes
	//     most revalidation chatter.
	//   negative 5s — unchanged. A stale NEGATIVE is the one users experience as
	//     "I just created this file and it isn't there", which is far more
	//     confusing than a stale positive, and the ._* ENOENT storm this was
	//     added for is already absorbed at 5s.
	//
	// JM_META_CACHE_SECS still overrides ALL FOUR together (documented escape
	// hatch); the per-cache vars below allow finer tuning without it.
	// --fast-statfs: answer statfs from juicefs's LOCAL counters instead of
	// querying the metadata service. Default in juicefs is false, and we have
	// never passed it, so EVERY free-space query has been a Redis round trip.
	//
	// That is not a rare call. Profiling one real cellular session measured
	// StatFS 1,727 times at 230ms average — 397 seconds, 6.6 minutes, spent
	// reporting how much space is free. Finder polls it while a window is open,
	// `df` hits it, and the app's own health surface reads it. On a LAN each
	// call is sub-millisecond and invisible; on cellular it is pure latency
	// tax on an operation nobody is waiting for the exact answer to.
	//
	// SAFETY: the cost is a slightly stale free-space figure. Nothing in
	// JuiceMount makes a correctness decision from statfs — the pin-store
	// capacity guard (R-1) measures the LOCAL cache volume with syscall.Statfs
	// on the Mac's own disk, not the JuiceFS mount, so it is unaffected. A
	// stale byte count in a Finder window is exactly the class of staleness
	// already accepted for throttled links. Kill switch JM_FAST_STATFS=0.
	if os.Getenv("JM_FAST_STATFS") != "0" {
		args = append(args, "--fast-statfs")
	}

	// --open-cache: reuse an open file WITHOUT re-checking for updates.
	//
	// THE PRIZE. Measured on a real cellular link 2026-07-29: the FIRST open of
	// a file costs 1,226-5,660ms while the SECOND costs 24-36ms — a ~66x gap,
	// and the cost is per-FILE, not per-byte (5.7 KB costs the same as 4.4 MB;
	// a directory is 26ms). juicefs defaults this to 0s and we have never
	// passed it, so every open re-validates against Redis. That re-validation
	// IS the gap, and it is redundant for us: we already run a coherence engine
	// (the SQLite/RAM mirror + the Redis keyspace push feed).
	//
	// DEFAULT OFF, DELIBERATELY. Two facts make this opt-in rather than on:
	//
	//  1. juicefs exposes NO external cache invalidation — checked the whole
	//     subcommand surface (format/config/quota/destroy/gc/fsck/restore/dump/
	//     load/status/stats/profile/info/debug/summary/mount/umount/gateway/
	//     webdav/bench/objbench/warmup/rmr/sync). Our push feed can drop OUR
	//     pooled fd (R1) but cannot make juicefs re-check inside its window. The
	//     TTL is therefore a HARD staleness bound with no escape hatch.
	//  2. The exposure is a stale SLICE MAP — wrong bytes from an existing file,
	//     not a stale listing. That is the family this codebase has been burned
	//     by three times (#18 torn reads, #104 zero-tailed black frames, the
	//     membuf stale-partial image). It is NOT covered by the "delay before
	//     new files appear" trade that was accepted for throttled links.
	//
	// So: build the lever, measure the win on the real link, THEN decide the
	// default with data — rather than defaulting it on from reasoning, which is
	// how the metadata-TTL change earned a measurement that showed it did
	// nothing.
	//
	// Sizing when enabled: SHORT. The measured repeat-opens were seconds apart
	// (a browse-and-preview burst), so 5-10s captures nearly all of the 66x
	// while keeping the window too short for a farm/ClipLogger write to land
	// inside it. Do not reach for 30s+ without a multi-writer test.
	//
	// JM_OPEN_CACHE="" (unset) = OFF, today's behavior byte-identically.
	// JM_OPEN_CACHE="5s"       = pass --open-cache 5s.
	if v := fm.openCacheTTL(); v != "" {
		args = append(args, "--open-cache", v)
	}

	attrTTL, entryTTL, dirEntryTTL, negTTL := metaCacheTTLs()
	args = append(args,
		"--attr-cache", attrTTL,
		"--entry-cache", entryTTL,
		"--dir-entry-cache", dirEntryTTL,
		"--negative-entry-cache", negTTL,
	)
	if v := os.Getenv("JM_MAX_UPLOADS"); v != "" {
		// Direct override wins over WAN mode for operators that want
		// to tune themselves.
		args = append(args, "--max-uploads", v)
	}
	if fm.cfg.CacheDir != "" {
		args = append(args, "--cache-dir", fm.cfg.CacheDir)
	}
	if fm.cfg.CacheSize != "" {
		args = append(args, "--cache-size", fm.cfg.CacheSize)
	}
	if fm.cfg.FreeSpaceRatio != "" {
		args = append(args, "--free-space-ratio", fm.cfg.FreeSpaceRatio)
	}
	if fm.cfg.BucketOverride != "" {
		// Override the Redis-stored bucket URL for this Mac client only.
		// See FUSEConfig.BucketOverride comment for rationale.
		//
		// Arg-ordering note (2026-05-26): `--bucket` lands after the
		// positional <meta-url> <mountpoint> args. JuiceFS 1.3.1 accepts
		// flags in this position (verified empirically via direct CLI
		// invocation — `juicefs mount redis://... /mnt/test --bucket
		// http://<server-ip>:30151/zpool` mounts cleanly). If a future
		// JuiceFS version tightens its parser, this is the line to move
		// — relocate the entire conditional-flag block to BEFORE the
		// `mount, fm.cfg.RedisURL, fm.cfg.MountPoint` append above.
		args = append(args, "--bucket", fm.cfg.BucketOverride)
	}

	// Build a sanitized copy for both log sinks; exec receives real args.
	logArgs := mountLogArgs(args)
	log.Printf("[fuse] mounting JuiceFS: %s %s", fm.cfg.JuiceFSBin, strings.Join(logArgs, " "))
	jmlog.Info("mounting juicefs", "bin", fm.cfg.JuiceFSBin, "args", strings.Join(logArgs, " "))

	// Run JuiceFS's service stage as one foreground child owned and reaped by
	// JuiceMount. Do NOT use `-d`: upstream's launchMount supervisor has a
	// fixed activity watchdog that SIGKILLs a healthy mount when one cellular
	// object request exceeds roughly a minute. JuiceMount's monitor below has
	// measured-link grace periods and is the sole restart authority.
	cmd := exec.Command(fm.cfg.JuiceFSBin, args...)
	cmd.Env = juiceFSDirectEnvironment(os.Getpid(), metaPassword, fm.cfg.ObjectKey)
	scope, isolated, isolationErr := appisolation.Current()
	if isolationErr != nil {
		return nil, isolationErr
	}
	privatePaths, privateMode, privateErr := appisolation.ResolvePaths()
	if privateErr != nil {
		return nil, privateErr
	}
	if privateMode && (!isolated || fm.cfg.CacheDir != privatePaths.CacheBase) {
		return nil, errors.New("isolated FUSE child cache differs from the private cache base")
	}
	if isolated {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, err
		}
		statePath := filepath.Join(scope.Root, "fuse-recovery-"+hex.EncodeToString(nonce[:])+".json")
		if _, err := os.Lstat(statePath); !os.IsNotExist(err) {
			return nil, fmt.Errorf("private FUSE recovery state pathname is occupied or unknown: %v", err)
		}
		cmd.Env = append(cmd.Env, "_FUSE_STATE_PATH="+statePath)
	}

	// Foreground JuiceFS writes to stdout/stderr rather than its background log.
	// Give it a REAL file descriptor, not an io.MultiWriter: os/exec implements
	// non-file writers with parent-side pipes, which can backpressure a daemon
	// launched by Finder/LaunchServices when the app's stderr is not drained.
	// tailJuiceFSLog still promotes warnings; failures read a bounded suffix.
	logWriter := os.Stderr
	var juiceFSLog *os.File
	var juiceFSLogPath string
	var juiceFSLogOffset int64
	juiceFSLogStart := int64(-1) // for the tail: -1 = unknown
	logDir := ""
	if privateMode {
		logDir = filepath.Dir(privatePaths.FUSELog)
	} else if home, err := os.UserHomeDir(); err == nil {
		logDir = filepath.Join(home, ".juicefs")
	}
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0700); err == nil {
			logPath := filepath.Join(logDir, "juicefs.log")
			if privateMode {
				logPath = privatePaths.FUSELog
			}
			if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600); err == nil {
				juiceFSLog = f
				juiceFSLogPath = logPath
				if st, err := f.Stat(); err == nil {
					juiceFSLogOffset = st.Size()
					juiceFSLogStart = st.Size()
				}
				logWriter = f
			} else {
				if privateMode {
					return nil, fmt.Errorf("open private FUSE log: %w", err)
				}
				jmlog.Warn("juicefs log open failed; continuing with stderr", "path", logPath, "error", err.Error())
			}
		} else if privateMode {
			return nil, fmt.Errorf("create private FUSE log directory: %w", err)
		}
	}
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	if fm.stopping.Load() {
		if juiceFSLog != nil {
			_ = juiceFSLog.Close()
		}
		return nil, errors.New("FUSE manager stopped before child start")
	}
	if fm.generation != nil {
		if err := fm.generation.markLaunching(); err != nil {
			if juiceFSLog != nil {
				_ = juiceFSLog.Close()
			}
			return nil, fmt.Errorf("persist generation launch intent: %w", err)
		}
	}
	if err := cmd.Start(); err != nil {
		if juiceFSLog != nil {
			_ = juiceFSLog.Close()
		}
		// The process never started, so no kext diagnosis is possible.
		kextBlockedFlag.Store(false)
		return nil, fmt.Errorf("juicefs mount start: %w", err)
	}

	// The child runs with this key from now on (access-key ID only).
	noteObjectKeyInUse(fm.cfg.ObjectKey.accessKeyIfComplete())
	receipt := &MountReceipt{owner: fm, generation: fm.generation, process: cmd.Process, tailStop: make(chan struct{}),
		juiceFSLogStart: juiceFSLogStart}
	processExit := startFUSEChildWaiter(cmd, juiceFSLog, receipt)
	fm.activeReceipt = receipt // exact waiter exists before any post-spawn journal work
	if fm.generation != nil {
		if err := fm.generation.markChild(cmd.Process.Pid); err != nil {
			return receipt, fmt.Errorf("persist exact generation child: %w", err)
		}
	}
	if fm.stopping.Load() {
		return receipt, errors.New("FUSE manager stopped during child creation; exact child retained")
	}

	// The direct service stays attached, so verification also watches for a
	// fast child exit and returns its actual final stderr line immediately.
	if err := fm.waitForMountProcess(fm.mountVerifyTimeout(), processExit); err != nil {
		fm.noteMountFailure()
		if reason := juiceFSStartupFailureReason(readFileTailSince(juiceFSLogPath, juiceFSLogOffset, 64<<10)); reason != "" {
			return receipt, fmt.Errorf("mount verification: %w: %s", err, reason)
		}
		return receipt, fmt.Errorf("mount verification: %w", err)
	}
	// A published kernel mount needs its durable FSID and retained retirement
	// handles before any root-inode probe can park or time out. The custodian
	// opens the root in its own process; Start does not wait for that open.
	if err := fm.startOriginalCustody(receipt); err != nil {
		return receipt, err
	}
	// Mount-table publication plus a live JuiceFS process is necessary but not
	// sufficient on macOS. A fresh macFUSE session can be visible while its
	// first open/readdir calls still park in the kernel until daemon_timeout.
	// Prove the actual root data plane from short-lived helper processes before
	// any in-process goroutine, NFS listener, Spotlight marker, or spool drainer
	// is allowed to touch it. A timed-out helper can be killed without poisoning
	// an application thread; two consecutive successes reject a single lucky
	// response during session warm-up.
	probe := fm.dataPlaneProbe
	if probe == nil {
		probe = WaitForFUSEDataPlaneStable
	}
	if !probe(fm.cfg.MountPoint, fm.mountVerifyTimeout()) {
		fm.noteMountFailure()
		return receipt, fmt.Errorf("mount verification: FUSE data plane did not become stable within %v", fm.mountVerifyTimeout())
	}
	if fm.stopping.Load() {
		return receipt, errors.New("FUSE manager stopped during mount verification; exact child retained")
	}
	if !fm.isMountedSteadyLocked() {
		return receipt, errors.New("FUSE mount identity changed during readiness verification")
	}
	if err := fm.admitMountBudget(receipt, 3*time.Second); err != nil {
		return receipt, err
	}
	kextBlockedFlag.Store(false)
	observerDone := make(chan struct{})
	receipt.observerDone = observerDone
	go func() { defer close(observerDone); fm.observeJuiceFSExit(cmd.Process.Pid, receipt) }()

	log.Printf("[fuse] JuiceFS mounted at %s", fm.cfg.MountPoint)

	// Suppress Spotlight indexing on the hidden FUSE mount. This marker is a
	// cosmetic optimization, not a readiness dependency. A live cold launch
	// demonstrated that the FUSE CREATE/TRUNCATE syscall can remain blocked even
	// after mount-table/process verification succeeds; performing it inline
	// held NFSServerStart forever while the control plane truthfully stayed in
	// "starting". Best-effort it in the background so navigation readiness can
	// never depend on Spotlight suppression.
	fm.scheduleSpotlightIndexing()

	// Up-front disk-space sanity check. If the cache volume is so full that
	// JuiceFS will refuse to cache, we want the user to KNOW now — before
	// they try to play media and wonder why everything is slow.
	if msg := checkCacheVolumeHealth(fm.cfg); msg != "" {
		jmlog.Warn("juicefs cache health concern", "detail", msg)
	}

	// Log the purgeable-vs-free breakdown so the user can see at a glance
	// how much APFS is hiding from raw statfs(). On a 1 TB disk that says
	// 38 GB free, macOS often has 280+ GB available for "important use" if
	// the requesting app is willing to take it. JuiceFS uses raw statfs and
	// sees only the conservative number — so we report both.
	logVolumeCapacityBreakdown("/")

	// Log final cache config so any future "why is the cache so small"
	// question is answered by the same log that shows the volume size.
	jmlog.Info("juicefs cache config",
		"cache_size_mb", fm.cfg.CacheSize,
		"free_space_ratio", fm.cfg.FreeSpaceRatio,
		"cache_dir", fm.cfg.CacheDir)

	// Tail JuiceFS's service log into our structured logger so warnings like
	// "space not enough on device, upload it directly" are visible to the
	// user instead of buried in ~/.juicefs/juicefs.log. Bound to fm.stopCh
	// so the goroutine exits cleanly when the FUSEManager is stopped
	// (previously leaked on every Stop, holding the open file handle).
	tailDone := make(chan struct{})
	receipt.tailDone = tailDone
	go func() {
		defer close(tailDone)
		fm.tailJuiceFSLog(receipt.tailStop, receipt.juiceFSLogStart)
	}()

	return receipt, nil
}

// startOriginalCustody retains retirement authority as soon as the original
// child publishes one exact kernel mount. The custodian's root open is async:
// data-plane probes can still fail without discarding its process handle.
// Caller holds fm.mu; neither a path nor a process title alone is authority.
func (fm *FUSEManager) startOriginalCustody(receipt *MountReceipt) error {
	if receipt == nil || receipt != fm.activeReceipt || receipt.owner != fm ||
		receipt.generation != fm.generation || receipt.process == nil || receipt.process.Pid <= 1 ||
		receipt.waitDone == nil || waitFUSEChannel(receipt.waitDone, 0) || fm.stopping.Load() {
		return errors.New("original FUSE child unavailable before custody")
	}
	snapshot, present, err := fm.snapshotFUSE()
	if err != nil || !present || (fm.generation != nil && snapshot.BootID != fm.generation.bootID) {
		return errors.New("FUSE custody admission has no same-boot mount observation")
	}
	var source, onName string
	var fsid [2]int32
	for _, entry := range snapshot.Entries {
		if strings.EqualFold(entry.Path, fm.cfg.MountPoint) {
			if source != "" || entry.Path != fm.cfg.MountPoint || entry.Type != "macfuse" || entry.FSID == [2]int32{} {
				return errors.New("FUSE custody root observation is ambiguous")
			}
			source, fsid, onName = entry.Source, entry.FSID, entry.Path
		} else if strings.HasPrefix(strings.ToLower(entry.Path), strings.ToLower(fm.cfg.MountPoint)+string(os.PathSeparator)) {
			return errors.New("FUSE custody root has mounted descendant")
		}
	}
	if source == "" || snapshot.BootID != receipt.readinessBoot ||
		source != receipt.readinessSource || fsid != receipt.readinessFSID ||
		waitFUSEChannel(receipt.waitDone, 0) || fm.stopping.Load() {
		return errors.New("FUSE mount identity changed before custody retention")
	}
	receipt.custodyBoot, receipt.custodySource, receipt.custodyFSID = snapshot.BootID, source, fsid
	// Preserve the normal durable-observation-before-custodian order. Capture
	// a journal failure instead of returning yet: this same live owner can
	// still retain a descriptor custodian for a safe Stop.
	var observedErr error
	if fm.generation != nil {
		observedErr = fm.generation.markObserved(fsid)
	}
	if observedErr == nil {
		fm.registerWithHelperLocked(receipt, onName, source, fsid)
	}
	// The helper root open is async. Attempt it even when the journal failed,
	// but never admit dependent services after such a failure. If it too fails,
	// retain the original child and report both errors without false retirement.
	custody, custodyErr := startFUSECustodian(fm.cfg.MountPoint, source, receipt.process.Pid, os.Getpid())
	receipt.custody = custody
	if custody != nil {
		receipt.custodyStartedAt = fuseNow()
	}
	if observedErr != nil {
		journalErr := fmt.Errorf("persist observed FUSE identity: %w", observedErr)
		if custodyErr != nil {
			return errors.Join(journalErr, fmt.Errorf("FUSE custody helper start incomplete: %w", custodyErr))
		}
		return journalErr
	}
	if custodyErr != nil {
		return fmt.Errorf("FUSE custody helper start incomplete: %w", custodyErr)
	}
	if custody == nil {
		return errors.New("FUSE custody helper start returned no session")
	}
	return nil
}

// admitMountBudget is shared by inline startup and watchdog mounts. A failed
// birth cannot publish a resolved budget for a daemon it never admitted.
func (fm *FUSEManager) admitMountBudget(receipt *MountReceipt, timeout time.Duration) error {
	if err := fm.finishCustodyAdmission(receipt, timeout); err != nil {
		return err
	}
	var budget int64 // Unknown must not inherit a previous mount's budget.
	if mb, err := strconv.ParseInt(fm.cfg.CacheSize, 10, 64); err == nil && mb > 0 && mb <= (1<<63-1)>>20 {
		budget = mb << 20
	}
	fm.cachePolicyKnown = fm.cachePolicyMeasured && budget > 0
	pin.SetCachePolicy(budget, fm.cacheFloorBytes, fm.cacheVolumePath, fm.cacheVolumeDev, fm.cachePolicyKnown)
	return nil
}

func suppressSpotlightIndexing(mountPoint string, writeFile func(string, []byte, os.FileMode) error) <-chan struct{} {
	done := make(chan struct{})
	if mountPoint == "" || writeFile == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		if err := writeFile(filepath.Join(mountPoint, ".metadata_never_index"), nil, 0644); err != nil {
			jmlog.Warn("could not create Spotlight suppression marker",
				"mount_point", mountPoint, "error", err.Error())
		}
	}()
	return done
}

const desktopMacFUSEDaemonTimeoutSeconds = 900

// juiceFSMountPlatformOptions returns only options supported by the host FUSE
// implementation. nobrowse maps to macOS's MNT_DONTBROWSE flag; Linux
// fusermount3 rejects it and prevents JuiceFS from mounting at all. The
// internal macOS mount also needs native xattrs: otherwise macOS can create
// its own `._` AppleDouble companion when a directory or file is materialized,
// racing the separate AppleDouble body accepted through the NFS spool.
//
// macFUSE's default daemon timeout is 60 seconds. That is shorter than one
// legitimate JuiceFS object request (--get-timeout/--put-timeout both default
// to 60s), and far shorter than JuiceFS's bounded retry envelope. On a weak
// cellular path, or when a foreground read shares the tunnel with a durable
// spool upload, a single request can therefore cross exactly 60s while the
// daemon is still alive and making progress. macFUSE then tears the whole
// device down with ENXIO ("device not configured"). Finder loses every open
// handle and the app spends tens of seconds unmounting/remounting a filesystem
// that never actually crashed.
//
// Keep the kernel/daemon contract alive for the full bounded JuiceFS retry
// window. NFS still applies its own request/retry policy, JuiceMount still
// reports backend degradation promptly, and the FUSE watchdog still recovers a
// genuinely dead process. This only prevents a slow data operation from being
// mistaken for a dead filesystem session.
func juiceFSMountPlatformOptions(goos string) []string {
	if goos == "darwin" {
		return []string{"--enable-xattr", "-o", fmt.Sprintf("nobrowse,daemon_timeout=%d", desktopMacFUSEDaemonTimeoutSeconds)}
	}
	return nil
}

// checkCacheVolumeHealth returns a non-empty warning string if the volume
// hosting the cache directory is in a state where JuiceFS will silently
// stop caching (free space below ratio threshold, or directory unwritable).
func checkCacheVolumeHealth(cfg FUSEConfig) string {
	cacheDir := cfg.CacheDir
	if cacheDir == "" {
		// JuiceFS default: ~/.juicefs/cache
		home, _ := os.UserHomeDir()
		cacheDir = filepath.Join(home, ".juicefs", "cache")
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cacheDir, &stat); err != nil {
		// Try the parent if cacheDir doesn't exist yet
		if err := syscall.Statfs(filepath.Dir(cacheDir), &stat); err != nil {
			return fmt.Sprintf("cannot stat cache dir %s: %v", cacheDir, err)
		}
	}
	totalBytes := uint64(stat.Bsize) * stat.Blocks
	freeBytes := uint64(stat.Bsize) * stat.Bavail
	if totalBytes == 0 {
		return ""
	}
	freeRatio := float64(freeBytes) / float64(totalBytes)
	threshold := 0.01
	if cfg.FreeSpaceRatio != "" {
		// Parse our configured ratio as a number — we want to know if the
		// disk is below it. Crude parsing on purpose: invalid → use default.
		var r float64
		_, err := fmt.Sscanf(cfg.FreeSpaceRatio, "%f", &r)
		if err == nil && r > 0 && r < 1 {
			threshold = r
		}
	}
	if freeRatio < threshold {
		return fmt.Sprintf(
			"cache volume %s is %.1f%% free (%.1f GiB), below the %.1f%% threshold "+
				"— JuiceFS will skip caching and read every block from S3. "+
				"Free up disk space or pass a smaller --free-space-ratio.",
			cacheDir, freeRatio*100, float64(freeBytes)/(1<<30),
			threshold*100)
	}
	return ""
}

// tailJuiceFSLog watches the JuiceFS service log file for new lines and
// promotes WARNING / ERROR records into jmlog so the user can see them in
// the same JSON stream as everything else. Aggregates the chatty
// "space not enough" message — emit once per minute with a count instead
// of flooding the log with thousands of identical entries.
//
// Exits cleanly when fm.stopCh closes. Previously a package-level
// function with no stop signal — leaked the goroutine + open file handle
// on every FUSEManager.Stop(), accumulating across Stop/Start cycles.
//
// startAt is the log offset to read from: the size of the log just before
// this child started, so its startup lines are seen. startAt < 0 reads only
// lines written after the tail opens the file.
func (fm *FUSEManager) tailJuiceFSLog(receiptStop <-chan struct{}, startAt int64) {
	paths, isolated, scopeErr := appisolation.ResolvePaths()
	if scopeErr != nil {
		return
	}
	logPath := paths.FUSELog
	if !isolated {
		home, err := os.UserHomeDir()
		if err != nil {
			return
		}
		logPath = filepath.Join(home, ".juicefs", "juicefs.log")
	}
	// Wait for the file to appear (juicefs daemon may not have created it
	// yet). Interruptible by fm.stopCh so Stop() doesn't have to wait the
	// full 30 s to reap this goroutine.
	for i := 0; i < 30; i++ {
		if _, err := os.Stat(logPath); err == nil {
			break
		}
		select {
		case <-fm.stopCh:
			return
		case <-receiptStop:
			return
		case <-time.After(1 * time.Second):
		}
	}
	f, err := os.Open(logPath)
	if err != nil {
		jmlog.Debug("juicefs log tail: open failed", "path", logPath, "error", err.Error())
		return
	}
	defer f.Close()
	// Start where this child's lines start; never replay older history.
	// An offset past the end (the log was truncated meanwhile) is handled
	// by the reader's truncation check, which reads again from 0.
	if startAt >= 0 {
		_, err = f.Seek(startAt, io.SeekStart)
	} else {
		_, err = f.Seek(0, io.SeekEnd)
	}
	if err != nil {
		return
	}
	// A bufio.Reader, not a bufio.Scanner: a Scanner stops for good at its
	// first EOF, so the old loop promoted nothing written after the tail
	// caught up (in practice, nothing at all). The Reader reads the file
	// again on every pass and keeps a partial last line until its newline
	// arrives.
	lines := newLogLineReader(f, 1<<20)

	type aggKey struct{ pattern string }
	type aggState struct {
		count     int
		firstAt   time.Time
		lastFlush time.Time
	}
	agg := map[aggKey]*aggState{}
	const flushEvery = 60 * time.Second
	// At most juiceFSPromoteBudget lines a minute reach jmlog one by one; the
	// rest are counted under one aggregated warning. A revoked key makes
	// juicefs log every upload retry, and a slow link logs every slow
	// request: unbounded promotion would flood the JuiceMount log (the
	// slow-RPC log line alone once measured 45.6% of mutex contention).
	var windowStart time.Time
	promoted := 0
	flushAgg := func() {
		now := time.Now()
		for k, st := range agg {
			if st.count > 0 && now.Sub(st.lastFlush) >= flushEvery {
				jmlog.Warn("juicefs warning (aggregated)",
					"pattern", k.pattern,
					"count_in_window_sec", int(flushEvery.Seconds()),
					"count", st.count,
					"since", st.firstAt.Format(time.RFC3339))
				st.count = 0
				st.lastFlush = now
			}
		}
	}

	for {
		// Check stop before each scan pass so we don't block on a
		// scanner.Scan that's waiting on a quiet log file.
		select {
		case <-fm.stopCh:
			return
		case <-receiptStop:
			return
		default:
		}
		for {
			line, ok, err := lines.next()
			if err != nil {
				jmlog.Debug("juicefs log tail: read error", "error", err.Error())
				return
			}
			if !ok {
				break
			}
			// Two interesting tokens: <WARNING> and <ERROR>
			isWarn := strings.Contains(line, "<WARNING>:")
			isErr := strings.Contains(line, "<ERROR>:")
			if !isWarn && !isErr {
				continue
			}
			// The object store refused this Mac's key (revoked, or
			// replaced): the app polls the Manager now instead of at the
			// next 30-minute approved poll (bridge objectKeyStatus).
			noteObjectKeyLogLine(line, time.Now())
			// Aggregate the disk-full pattern and anything over the
			// budget; surface others immediately.
			now := time.Now()
			if now.Sub(windowStart) >= flushEvery {
				windowStart, promoted = now, 0
			}
			pattern := ""
			switch {
			case strings.Contains(line, "space not enough on device"):
				pattern = "space not enough on device, upload directly"
			case promoted >= juiceFSPromoteBudget:
				pattern = "juicefs warnings over the per-minute budget (see ~/.juicefs/juicefs.log)"
			}
			if pattern != "" {
				k := aggKey{pattern}
				st := agg[k]
				if st == nil {
					st = &aggState{firstAt: time.Now(), lastFlush: time.Now()}
					agg[k] = st
				}
				st.count++
				continue
			}
			promoted++
			level := jmlog.LevelWarn
			if isErr {
				level = jmlog.LevelError
			}
			if level == jmlog.LevelError {
				jmlog.Error("juicefs", "raw", line)
			} else {
				jmlog.Warn("juicefs", "raw", line)
			}
		}
		flushAgg()
		// At EOF: sleep, then read again.
		// Interruptible sleep so Stop() reaps us within 2 s instead of
		// waiting for the next read attempt.
		select {
		case <-fm.stopCh:
			return
		case <-receiptStop:
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// juiceFSPromoteBudget is how many juicefs WARNING/ERROR lines a minute
// tailJuiceFSLog copies to jmlog one by one.
const juiceFSPromoteBudget = 30

// logLineReader returns whole lines from a file that is still being written.
// At EOF it keeps the partial last line and reads the file again on the next
// call. A line longer than max is dropped up to its newline.
//
// When the source is a file that was truncated in place (the user cleared
// ~/.juicefs/juicefs.log, or a log cleaner did; juicefs keeps appending
// through its O_APPEND descriptor), the reader notices at EOF that the file
// is now shorter than what it has read, and reads again from offset 0
// (review keys-l3 F5). A truncation followed by more new bytes than were
// read before the next EOF check is not noticed.
type logLineReader struct {
	src     io.Reader
	r       *bufio.Reader
	partial []byte
	max     int
	skip    bool // dropping an over-long line until its newline
}

func newLogLineReader(f io.Reader, max int) *logLineReader {
	return &logLineReader{src: f, r: bufio.NewReaderSize(f, 64<<10), max: max}
}

// rewindIfTruncated restarts at offset 0 when the source file is now
// shorter than the offset read so far. It reports whether it did.
func (l *logLineReader) rewindIfTruncated() bool {
	f, ok := l.src.(interface {
		io.Seeker
		Stat() (os.FileInfo, error)
	})
	if !ok {
		return false
	}
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return false
	}
	st, err := f.Stat()
	if err != nil || st.Size() >= pos {
		return false
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false
	}
	l.r.Reset(l.src)
	l.partial, l.skip = l.partial[:0], false
	return true
}

// next returns the next complete line without its newline; ok is false at
// EOF (call again later). err is a read error other than EOF.
func (l *logLineReader) next() (line string, ok bool, err error) {
	for {
		chunk, err := l.r.ReadSlice('\n')
		switch {
		case err == nil:
			if l.skip {
				l.skip = false
				continue
			}
			l.partial = append(l.partial, chunk[:len(chunk)-1]...)
			if len(l.partial) > l.max {
				l.partial = l.partial[:0]
				continue
			}
			line = strings.TrimSuffix(string(l.partial), "\r")
			l.partial = l.partial[:0]
			return line, true, nil
		case errors.Is(err, bufio.ErrBufferFull) || errors.Is(err, io.EOF):
			if !l.skip {
				l.partial = append(l.partial, chunk...)
				if len(l.partial) > l.max {
					l.partial, l.skip = l.partial[:0], true
				}
			}
			if errors.Is(err, io.EOF) {
				if l.rewindIfTruncated() {
					continue
				}
				return "", false, nil
			}
		default:
			return "", false, err
		}
	}
}

// StartMonitor begins a background goroutine that checks mount health
// and remounts if the FUSE mount dies.
func (fm *FUSEManager) StartMonitor() {
	if fm.stopping.Load() || !fm.monitorState.CompareAndSwap(0, 1) {
		return
	}
	go fm.monitorLoop()
}

// Stop refuses to detach a present or uncertain kernel mount. The process
// receipt and dependencies remain owned until a repeatable typed result proves
// exact path-tree absence and child settlement.
func (fm *FUSEManager) Stop() FUSEStopResult {
	return fm.stopWithBounds(10*time.Second, 2*time.Second)
}

// tryLockWithin acquires fm.mu or returns when wait elapses. sync.Mutex.TryLock
// keeps the caller itself out of the mutex wait queue, which matters when the
// owner is parked in an uninterruptible filesystem syscall.
func (fm *FUSEManager) tryLockWithin(wait time.Duration) bool {
	if fm.mu.TryLock() {
		return true
	}
	if wait <= 0 {
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if fm.mu.TryLock() {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

// IsMounted returns true if the FUSE mount is live and responsive.
func (fm *FUSEManager) IsMounted() bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.isMountedLocked()
}

// isMountedSteadyLocked is the backend-free steady-state health probe. A ps
// command title is not child identity: macOS truncates long mount paths in ps,
// so a live directly supervised child can be missed indefinitely. Instead use
// its retained Wait handle and a complete, bounded kernel snapshot. Neither
// this probe nor periodic health opens the FUSE root or .config.
// Must be called with fm.mu held.
func (fm *FUSEManager) isMountedSteadyLocked() bool {
	r := fm.activeReceipt
	if r == nil || r.owner != fm || r.process == nil || r.process.Pid <= 1 || r.waitDone == nil ||
		waitFUSEChannel(r.waitDone, 0) || r.generation != fm.generation {
		return false
	}
	snapshot, present, err := fm.snapshotFUSE()
	if err != nil || !present || (fm.generation != nil &&
		(fm.generation.Path != fm.cfg.MountPoint || snapshot.BootID != fm.generation.bootID)) {
		return false
	}
	var root *mounttable.MountEntry
	folded := strings.ToLower(fm.cfg.MountPoint)
	for i := range snapshot.Entries {
		entry := &snapshot.Entries[i]
		if strings.EqualFold(entry.Path, fm.cfg.MountPoint) {
			if root != nil || entry.Path != fm.cfg.MountPoint || entry.Type != "macfuse" || entry.FSID == [2]int32{} {
				return false
			}
			root = entry
		} else if strings.HasPrefix(strings.ToLower(entry.Path), folded+string(os.PathSeparator)) {
			return false
		}
	}
	if root == nil || (r.readinessBoot != "" && (snapshot.BootID != r.readinessBoot ||
		root.Source != r.readinessSource || root.FSID != r.readinessFSID)) ||
		(r.custodyBoot != "" && (snapshot.BootID != r.custodyBoot ||
			root.Source != r.custodySource || root.FSID != r.custodyFSID)) ||
		waitFUSEChannel(r.waitDone, 0) {
		return false
	}
	if r.readinessBoot == "" {
		r.readinessBoot, r.readinessSource, r.readinessFSID = snapshot.BootID, root.Source, root.FSID
	}
	return true
}

// isMountedLocked checks if the FUSE mount is live. Must be called with fm.mu held.
//
// CRITICAL: `mount` (which calls `getfsstat()`) hangs in the kernel if the
// mount table contains a wedged entry (server gone, kernel mount retained).
// Without the context timeout, this function can wedge the FUSEManager lock
// forever during startup or an explicit IsMounted call, parking every other
// caller (Stop, IsMounted, Mount) behind it. That was the "click menu → app
// freezes" pattern. Periodic monitoring uses isMountedSteadyLocked instead.
func (fm *FUSEManager) isMountedLocked() bool {
	// Check 1: appears in macOS mount table as a FUSE mount.
	// Hard-bounded at 5 s. On timeout, treat as "unknown" (return false) so
	// the monitor loop doesn't pin fm.mu forever. The cost of a false
	// negative is a respawn attempt the user will see in the log; the cost
	// of hanging is the entire UI.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := mounttable.Output(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			jmlog.Warn("mount table query timed out — likely a wedged mount entry blocking getfsstat",
				"mount_point", fm.cfg.MountPoint,
				"hint", "treating as not-mounted; a wedged mount is retired by the JuiceMount unmount helper, never by path")
		}
		return false
	}
	if !strings.Contains(string(out), fm.cfg.MountPoint) {
		return false
	}

	// Check 2: actually responsive — try listing the directory. A stale FUSE
	// mount (dead daemon) hangs on any fs op. Kept as a root readdir (NOT the
	// .config probe) because this same check gates LAUNCH readiness via
	// waitForMount: right after the JuiceFS service starts the dir is listable
	// before the .config control file is served, so a .config probe here would
	// false-fail launch verification. This stronger data-path check must not be
	// used by a periodic loop; isMountedSteadyLocked owns that path. Run it in
	// a disposable helper process: a timed-out in-process ReadDir used to leave
	// a Go thread parked for the full 15-minute macFUSE daemon timeout.
	if !FUSEDataPlaneResponsive(fm.cfg.MountPoint, 5*time.Second) {
		log.Printf("[fuse] mount at %s is unresponsive (stale); reporting unhealthy. Remount decision deferred to monitorLoop.", fm.cfg.MountPoint)
		return false
	}
	return true
}

// fuseConfirmProbeTimeout is the GENEROUS window used to CONFIRM a mount is
// truly wedged before the destructive last-resort remount. isMountedLocked's
// 5s readdir flips to "stale" whenever the mount is merely SLOW — e.g. while a
// metadata reconcile or a backend blip contends the JuiceFS→Redis path — and
// under sustained slowness that accumulates to FUSEStaleEscalateTicks and
// SIGKILLs a perfectly-alive juicefs, which is exactly what drove the macFUSE
// remount thrash (2026-06-14, surfaced re-deploying mid-reconcile). Confirming
// with a long timeout distinguishes "slow" from "dead": a slow-but-alive mount
// answers within 25s; a genuinely wedged one never does.
const fuseConfirmProbeTimeout = 25 * time.Second

// mountResponsiveWithin returns true (responsive → DEFER the destructive remount)
// if juicefs answers a bounded .config read inside timeout. Safe to call without
// fm.mu (touches no fm state).
//
// DELIBERATELY a .config (LIVENESS) probe, NOT a data-path readdir — empirically
// grounded (2026-06-29). Three iterations were tried as the escalation-confirm:
//
//  1. root readdir (original) — false-SIGKILLs a slow-but-alive mount whenever a
//     readdir exceeds the confirm window under transient load.
//  2. root readdir gated by the GENEROUS 25s/45s/90s class window (the "obvious"
//     fix an adversarial review recommended) — STILL false-escalated: during the
//     cold-start metadata-sync burst the root readdir momentarily exceeded even
//     25s, so the watchdog SIGKILLed a PERFECTLY HEALTHY juicefs (verified
//     seconds later with escalation suppressed: readdir 130ms, reads 13ms, full
//     bytes) and the follow-up remount then failed ("mount not ready after 15s")
//     → a stuck no-mount loop. Destroying healthy mounts is the exact regression
//     tasks #13/#72 exist to PREVENT, so this is strictly worse than (3).
//  3. .config (this) — juicefs serves it from memory with NO backend round-trip,
//     so it answers whenever the process + FUSE session are alive and hangs only
//     on a TOTAL session death. It never false-trips on data-path slowness, so it
//     never SIGKILLs a healthy mount; in the common case .config-healthy tracks
//     data-path-healthy (verified: readdir 130ms / reads 13ms while .config OK).
//
// KNOWN LIMITATION (tracked as a #72 follow-up): a .config probe cannot see the
// RARE "juicefs alive but backend CONNECTION wedged" shape (.config answers while
// readdir hangs — the 2026-06-01 event). There this defers and the mount does not
// auto-recover; manual app restart is the recovery. Accepted for now because
// (a) it is rare, (b) the escalation's remount is itself currently broken ("mount
// not ready after 15s"), so detecting the wedge would not recover it anyway, and
// (c) the alternative (readdir-confirm) destroys HEALTHY mounts in the COMMON
// case. The proper fix is a SEPARATE sustained-data-path-stall counter (escalate
// only after readdir hangs for MINUTES while .config stays live) PLUS a fix for
// the broken remount — both deferred to the follow-up, not rushed into a release.
func (fm *FUSEManager) mountResponsiveWithin(timeout time.Duration) bool {
	// This is the LAST gate before a destructive SIGKILL+remount, so it must
	// return true on ANY sign of life. Race two liveness signals:
	//   1. reading .config (the in-memory control file) — content OR a prompt
	//      error both mean juicefs ANSWERED (alive);
	//   2. a root readdir succeeding — the DATA path serving.
	// Either returning ⇒ alive ⇒ do NOT remount. Only a genuine wedge hangs
	// BOTH past the timeout. 2026-07-15: .config alone false-negatived during
	// cold-warm churn — juicefs serves the data-path readdir before it serves
	// the .config control file, so a .config-only probe could hang and greenlight
	// a SIGKILL of a mount that was listing fine (the remount-thrash trigger on a
	// large-local-cache box). A serving readdir is definitive proof of life.
	done := make(chan bool, 2)
	go func() {
		_, _ = os.ReadFile(fm.cfg.MountPoint + "/.config")
		done <- true
	}()
	go func() {
		if _, err := os.ReadDir(fm.cfg.MountPoint); err == nil {
			done <- true // a successful readdir is unambiguous liveness
		}
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}

// runBoundedCommand fires a shell command with a hard time limit and reaps
// the process. Logs on timeout. Used for fire-and-forget cleanup work that
// previously called `.Start()` without ever calling `.Wait()` — leaking
// zombies and silently failing.
func runBoundedCommand(timeout time.Duration, name string, args ...string) {
	// TRULY bounded (2026-07-10): the old CommandContext(...).Run() killed
	// the child at the deadline but then BLOCKED waiting to reap it — and a
	// child parked in an uninterruptible kernel call (diskutil against a
	// wedged diskarbitrationd, umount of a haunted mountpoint) ignores
	// SIGKILL until its syscall returns, so the "bounded" call itself hung
	// (observed live: 30s-bounded diskutils surviving 20+ minutes while the
	// watchdog's remount crawled at ~65s/attempt). Now: kill at the deadline
	// and RETURN; a reaper goroutine collects the child whenever the kernel
	// finally releases it.
	cmd := exec.Command(name, args...)
	if err := cmd.Start(); err != nil {
		jmlog.Debug("bounded command failed to start",
			"cmd", name, "args", strings.Join(args, " "), "error", err.Error())
		return
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			jmlog.Debug("bounded command failed",
				"cmd", name, "args", strings.Join(args, " "), "error", err.Error())
		}
	case <-timer.C:
		_ = cmd.Process.Kill()
		jmlog.Warn("bounded command timed out — killed, not waiting for reap",
			"cmd", name, "args", strings.Join(args, " "),
			"timeout_sec", int(timeout.Seconds()))
		// The Wait goroutine reaps the child when (if) the kernel releases
		// it; an unkillable D-state child must never pin the caller.
	}
}

const (
	fuseDataPlaneProbeTimeout  = 5 * time.Second
	fuseDataPlaneInitialSettle = 750 * time.Millisecond
	fuseDataPlaneRetryPause    = 250 * time.Millisecond
	fuseDataPlaneStablePasses  = 2
)

// FUSEDataPlaneResponsive performs one root-inode data-plane probe in a
// disposable process. macFUSE can park an early filesystem syscall in the
// kernel even though the mount table and JuiceFS process both look healthy.
// Keeping that syscall out of this process prevents a failed readiness check
// from consuming an application goroutine/thread until daemon_timeout.
//
// This must NOT enumerate the root. JuiceMount serves directory listings from
// its local metadata mirror; making launch readiness run `ls -A` over the
// JuiceFS root needlessly fetched every top-level entry from remote Redis. A
// measured cellular start paid 20.9 seconds for one lookup inside that probe
// and required two complete enumerations before publishing an otherwise-ready
// mount. `test -d` still crosses the mount boundary and asks the live FUSE
// daemon for the root inode, which proves the kernel/FUSE request path without
// turning readiness into a namespace warm-up job.
func FUSEDataPlaneResponsive(path string, timeout time.Duration) bool {
	if path == "" || timeout <= 0 {
		return false
	}
	cmd := fuseDataPlaneProbeCommand(path)
	if err := cmd.Start(); err != nil {
		return false
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err == nil
	case <-timer.C:
		_ = cmd.Process.Kill()
		// The waiter reaps the helper if and when macFUSE releases its syscall.
		// Crucially, the application itself remains unpoisoned and can retry.
		return false
	}
}

func fuseDataPlaneProbeCommand(path string) *exec.Cmd {
	return exec.Command("/bin/test", "-d", path)
}

// WaitForFUSEDataPlaneStable admits dependent services only after two
// consecutive root-inode probes succeed. The small initial settle window
// avoids issuing the very first request at the exact mount-publication edge;
// failures reset the streak and remain bounded by the caller's total budget.
func WaitForFUSEDataPlaneStable(path string, timeout time.Duration) bool {
	// A fixed 5-second child budget made the wider slow/metered mount budget
	// ineffective: every root READDIR was killed at five seconds, so a
	// legitimate high-RTT mount could never produce even one successful probe
	// during its 90/120-second verification window. Give an individual probe a
	// proportional share of the caller's class-aware budget while retaining the
	// 5-second LAN floor. Dividing by passes+1 leaves room for one cold/failed
	// attempt followed by the required consecutive successes.
	probeBudget := fuseDataPlaneProbeBudget(timeout, fuseDataPlaneStablePasses)
	return waitForFUSEDataPlaneStable(
		path,
		timeout,
		probeBudget,
		fuseDataPlaneInitialSettle,
		fuseDataPlaneRetryPause,
		fuseDataPlaneStablePasses,
		FUSEDataPlaneResponsive,
	)
}

func fuseDataPlaneProbeBudget(totalBudget time.Duration, requiredPasses int) time.Duration {
	if totalBudget <= 0 || requiredPasses < 1 {
		return fuseDataPlaneProbeTimeout
	}
	budget := totalBudget / time.Duration(requiredPasses+1)
	if budget < fuseDataPlaneProbeTimeout {
		return fuseDataPlaneProbeTimeout
	}
	return budget
}

func waitForFUSEDataPlaneStable(
	path string,
	totalBudget time.Duration,
	probeBudget time.Duration,
	initialSettle time.Duration,
	retryPause time.Duration,
	requiredPasses int,
	probe func(string, time.Duration) bool,
) bool {
	if path == "" || totalBudget <= 0 || probeBudget <= 0 || requiredPasses < 1 || probe == nil {
		return false
	}
	deadline := time.Now().Add(totalBudget)
	if !pauseBeforeDeadline(initialSettle, deadline) {
		return false
	}
	consecutive := 0
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		attemptBudget := probeBudget
		if remaining < attemptBudget {
			attemptBudget = remaining
		}
		if probe(path, attemptBudget) {
			consecutive++
			if consecutive >= requiredPasses {
				return true
			}
		} else {
			consecutive = 0
		}
		if !pauseBeforeDeadline(retryPause, deadline) {
			return false
		}
	}
}

func pauseBeforeDeadline(delay time.Duration, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	if delay <= 0 {
		return true
	}
	if delay > remaining {
		delay = remaining
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C
	return time.Now().Before(deadline)
}

// waitForMountProcess polls until the kernel mount and its exact directly
// supervised JuiceFS process both exist, the process exits, the manager stops,
// or the verification budget expires. Launch readiness deliberately uses the
// backend-free steady probe: a root ReadDir can legitimately block behind a
// high-latency object read, and using it here caused a healthy service to be
// killed at the verification deadline during a LAN-to-cellular handoff.
// A nil processExit preserves the old polling behavior for focused tests.
func (fm *FUSEManager) waitForMountProcess(timeout time.Duration, processExit <-chan error) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-processExit:
			if err == nil {
				return fmt.Errorf("juicefs service exited before mount became ready")
			}
			return fmt.Errorf("juicefs service exited before mount became ready: %w", err)
		default:
		}
		if fm.isMountedSteadyLocked() {
			return nil
		}
		// U5 review fix: with class-widened budgets (45s/90s) a failing
		// verify would otherwise pin fm.mu through Mount() and park the
		// user's Stop/quit for the whole window — abort promptly on Stop.
		select {
		case err := <-processExit:
			if err == nil {
				return fmt.Errorf("juicefs service exited before mount became ready")
			}
			return fmt.Errorf("juicefs service exited before mount became ready: %w", err)
		case <-fm.stopCh:
			return fmt.Errorf("mount verification aborted: manager stopping")
		case <-time.After(500 * time.Millisecond):
		}
	}
	select {
	case err := <-processExit:
		if err == nil {
			return fmt.Errorf("juicefs service exited before mount became ready")
		}
		return fmt.Errorf("juicefs service exited before mount became ready: %w", err)
	default:
	}
	return fmt.Errorf("mount not ready after %v", timeout)
}

// waitForMount keeps the original focused-test seam while production Mount
// also observes the foreground service process.
func (fm *FUSEManager) waitForMount(timeout time.Duration) error {
	return fm.waitForMountProcess(timeout, nil)
}

// observeJuiceFSExit reports a direct service exit. The monitor loop remains
// the single remount authority; this observer only makes the cause visible.
func (fm *FUSEManager) observeJuiceFSExit(pid int, receipt *MountReceipt) {
	select {
	case <-receipt.waitDone:
		select {
		case <-fm.stopCh:
			return
		default:
		}
		if receipt.exitErr != nil {
			jmlog.Warn("juicefs service exited; mount monitor will recover it",
				"pid", pid, "error", receipt.exitErr.Error())
		} else {
			jmlog.Warn("juicefs service exited cleanly outside manager shutdown; mount monitor will recover it",
				"pid", pid)
		}
	case <-fm.stopCh:
	}
}

// FUSEStaleEscalateTicks is the number of CONTINUOUS "stale but juicefs alive"
// monitor ticks (10s each) the watchdog tolerates before escalating to a
// last-resort remount. 9 ticks ≈ 90s: long enough that brief flapping never
// reaches it (the counter resets on any recovery), short enough that a
// genuinely stuck backend connection doesn't leave the mount wedged for many
// minutes. Var, not const, so tests can shorten it.
var FUSEStaleEscalateTicks = 9

// fuseWatchdogLinkAware gates the link-aware scaling of the escalation thresholds
// below. Default ON. Set JM_FUSE_WATCHDOG_LINKAWARE=0 (or the master
// JM_NET_ADAPTIVE=0, which pins class=medium) to restore the fixed historical
// thresholds. See docs/TUNING/REVERT_LOG.md (R1).
var fuseWatchdogLinkAware = os.Getenv("JM_FUSE_WATCHDOG_LINKAWARE") != "0"

// fuseOfflineNoEscalate (default ON): when offline, NEVER escalate a stale-but-
// alive mount to an app-side remount. Offline, "stale" is the juicefs daemon
// saturated by in-flight spool drains (busy, not wedged); a remount SIGTERMs a
// healthy daemon and drops the NFS mount (Finder "connection interrupted").
// Set JM_FUSE_OFFLINE_ESCALATE=1 to restore the pre-fix escalate-when-offline
// behavior for debugging (cellular-revert-safety doctrine).
var fuseOfflineNoEscalate = os.Getenv("JM_FUSE_OFFLINE_ESCALATE") != "1"

// fuseSkipEscalateWhileOffline reports whether a stale-but-juicefs-alive FUSE
// tick must be deferred (the escalation counter frozen, NEVER accumulating
// toward the escalate-to-remount threshold) because the user is OFFLINE.
// Offline, a "stale" probe is the juicefs daemon saturated by in-flight spool
// drains (busy, not wedged); escalating would diskutil-unmount-force a HEALTHY
// daemon and drop the NFS mount → Finder "connection interrupted". Gated by the
// JM_FUSE_OFFLINE_ESCALATE=1 kill-switch. Extracted from monitorLoop so the
// release-critical guard is unit-testable (see TestFUSEOfflineGuard*).
func fuseSkipEscalateWhileOffline() bool {
	return fuseOfflineNoEscalate && pin.IsOffline()
}

// V2.3 U4 (task #74/K3, field report "clicking offline doesn't help"): while
// the USER has explicitly engaged offline mode, the gone-branch's app-side
// remount of a dead juicefs is pure churn — a fresh juicefs mount needs the
// backend anyway, and the user has asked us to stand down. Navigation keeps
// serving from the SQLite mirror the whole time. AUTO-offline deliberately
// does NOT gate this (blips must self-heal without user action); the
// backend-reachability check next to the call site covers that shape.
// Set JM_FUSE_OFFLINE_REMOUNT=1 to restore the pre-fix always-retry behavior
// (cellular-revert-safety doctrine).
var fuseOfflineNoRemount = os.Getenv("JM_FUSE_OFFLINE_REMOUNT") != "1"

func fuseSkipRemountWhileUserOffline() bool {
	return fuseOfflineNoRemount && pin.IsUserOffline()
}

// fuseColdStartGrace is how long after the watchdog starts (≈ app start / first
// mount) the escalation to a destructive SIGKILL+remount is SUPPRESSED. During
// cold start the JuiceFS mount is legitimately busy warming caches and serving
// the initial ~200k-entry reconcile, so its FUSE-root readdir can be unresponsive
// to even the generous probe for minutes — NOT wedged, just saturated. Escalating
// then SIGKILLs a healthy-but-busy juicefs and, because the app's NFS server is
// holding the FUSE mount open, the follow-up unmount fails and leaves a ZOMBIE
// mount (kernel entry, no daemon) that breaks all data reads (observed live
// 2026-06-25, post-redeploy cold start). Within the grace window the watchdog
// defers and lets the mount recover on its own — it does, once the reconcile
// settles. A mount that wedges LATER (steady state) still escalates normally.
// Env override JM_FUSE_COLDSTART_GRACE_SEC (0 disables the grace).
var fuseColdStartGrace = coldStartGraceFromEnv()

func coldStartGraceFromEnv() time.Duration {
	if v := os.Getenv("JM_FUSE_COLDSTART_GRACE_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 6 * time.Minute
}

// staleEscalateTicks returns how many CONTINUOUS stale-but-alive ticks to tolerate
// before escalating to the destructive SIGKILL+remount, scaled UP on slow/metered
// links. ROOT CAUSE (2026-06-16, root-caused from juicemount.log): on cellular the
// 5s readdir stale-probe times out tick after tick because the JuiceFS->Redis
// readdir is legitimately SLOW (not wedged); at the fixed 9 ticks the watchdog
// then SIGKILL'd a perfectly-alive juicefs ~469 times in a day, with ~6% remount
// success, leaving FUSE dead for 30-45s per cycle = the "loading bar that never
// cures." medium/fast keep the validated 9 (~90s) BYTE-FOR-BYTE; metered/slow get
// a wide margin so a slow-but-alive mount is never mistaken for a wedge. A truly
// wedged mount still escalates, just later. Tests (class=medium, no signal) are
// unaffected.
func (fm *FUSEManager) staleEscalateTicks() int {
	if !fuseWatchdogLinkAware {
		return FUSEStaleEscalateTicks
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return FUSEStaleEscalateTicks * 3 // ~270s
	case netprofile.ClassSlow:
		return FUSEStaleEscalateTicks * 2 // ~180s
	default:
		return FUSEStaleEscalateTicks // ~90s — unchanged on LAN/medium/fast
	}
}

// confirmProbeTimeout returns the GENEROUS "is it actually wedged or just slow"
// probe budget before the destructive remount, scaled UP on slow/metered. The
// historical 25s is too short for a cellular FUSE-root readdir under Redis
// contention (so the probe false-fails and the kill proceeds — the bug above).
// medium/fast keep 25s unchanged.
func (fm *FUSEManager) confirmProbeTimeout() time.Duration {
	if !fuseWatchdogLinkAware {
		return fuseConfirmProbeTimeout
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 90 * time.Second
	case netprofile.ClassSlow:
		return 45 * time.Second
	default:
		return fuseConfirmProbeTimeout // 25s — unchanged on LAN/medium/fast
	}
}

// mountVerifyTimeout is the launch-time mount-establish budget (V2.3 U5/K1,
// field "mount not ready after 15s" churn). Wider only where a juicefs
// cold-start legitimately needs longer to answer its first readdir.
//
// 2026-07-15 field regression (0.4.0 RC, Leland's box): the old 15s LAN/fast
// base assumed warm-up latency tracks LINK speed. It does NOT — a large LOCAL
// block cache (here ~100GB, --cache-size 102400) makes juicefs scan/index the
// cache dir on mount before its FUSE root readdir answers, ~30s on a FAST LAN.
// The 15s base false-failed ("mount not ready after 15s"); the watchdog then
// SIGKILL'd a HEALTHY-but-warming juicefs, and — because the NFS server holds
// the mount open — the follow-up unmount left a ZOMBIE macFUSE entry that made
// getfsstat (isMountedLocked check 1) hang → perpetual false-"stale" → remount
// thrash. So the base must cover local cache-warm, not just slow links. A real
// mount failure still fails FAST regardless (juicefs's own 10s "mount point is
// not ready" fatal fires from cmd.Run before we ever reach waitForMount — see
// noteMountFailure), so a generous verify budget only helps the slow-warm case
// and never delays genuine-failure detection.
//
// Env override JM_FUSE_VERIFY_SEC (seconds) for field-tuning without a rebuild.
func (fm *FUSEManager) mountVerifyTimeout() time.Duration {
	if v := os.Getenv("JM_FUSE_VERIFY_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	// 60s base covers a large-local-cache cold warm-up on any link class; the
	// slow/metered link classes still widen further (metadata warm-up over the
	// backend stacks on top of local warm-up).
	const base = 60 * time.Second
	if !fuseWatchdogLinkAware {
		return base
	}
	switch netprofile.Default().Class() {
	case netprofile.ClassMetered:
		return 120 * time.Second
	case netprofile.ClassSlow:
		return 90 * time.Second
	default:
		return base
	}
}

// backendReachable does a cheap TCP dial to the metadata (Redis) host to
// decide whether a remount could even succeed. Mirrors the reachability
// monitor's probe so the watchdog does NOT kill+remount juicefs during a real
// backend outage (where a fresh mount would just fail and churn). Best-effort:
// returns true (optimistic — don't block escalation) if the host can't be
// parsed.
func (fm *FUSEManager) backendReachable() bool {
	hostPort := redisHostPort(fm.cfg.RedisURL)
	if hostPort == "" {
		return true
	}
	conn, err := net.DialTimeout("tcp", hostPort, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// redisHostPort extracts "host:port" from a redis URL (redis://host:port/db).
// Returns "" if it can't be parsed.
func redisHostPort(redisURL string) string {
	u, err := url.Parse(redisURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// monitorLoop checks mount health periodically and remounts ONLY when the
// JuiceFS process tree is genuinely gone.
//
// Design (2026-08-26 cellular RC). There is one lifecycle authority:
//
//  1. Mount starts one directly supervised JuiceFS service process. The
//     upstream launchMount supervisor is deliberately bypassed because its
//     fixed activity watchdog kills legitimate >60s cellular operations.
//  2. This loop observes service and mount health, applies measured-link grace
//     periods, and performs the only restart when the service is truly gone.
//
// The pre-2026-05-29 code remounted after 3 consecutive unhealthy checks
// EVEN WHILE juicefs was still alive. On a flapping backend link that was
// catastrophic: it `kill -9`'d a live-but-slow daemon — and because the kill
// pattern matched `juicefs mount.*<mountpoint>`, it killed the whole process
// tree — then thrashed mount/unmount until macFUSE wedged with
// "init: 19=operation not supported by device", a kernel state only a full
// app restart clears. Observed live: juicefs SIGKILL'd mid-slow-PUT, never
// recovered, mount dead with no log trace.
//
// New policy: while the JuiceFS service is alive, do not remount for ordinary
// staleness — report it and let backend recovery clear it. Only after the
// measured-link grace and an independent liveness confirmation may a sustained
// wedge be replaced. A gone service is restarted here. All decisions log via
// jmlog (the rotating juicemount.log), not log.Printf → the app's /dev/null
// stdout — the old channel made this safety-critical loop invisible.
func (fm *FUSEManager) monitorLoop() {
	defer close(fm.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	watchdogStart := time.Now() // for the cold-start escalation grace window
	consecutiveFailures := 0    // ticks where the mount is stale AND juicefs is gone
	staleWhileAliveTicks := 0   // ticks where the mount is stale but juicefs is alive
	remountDeferTicks := 0      // gone-branch ticks deferred (user-offline / backend down) — V2.3 U4
	offlineDeferTicks := 0      // ticks deferred because offline (busy draining, not wedged)
	for {
		select {
		case <-fm.stopCh:
			return
		case <-ticker.C:
			fm.mu.Lock()
			healthy := fm.isMountedSteadyLocked()
			fm.mu.Unlock()

			if healthy {
				if consecutiveFailures > 0 || staleWhileAliveTicks > 0 {
					jmlog.Info("fuse mount recovered",
						"after_dead_remount_attempts", consecutiveFailures,
						"stale_while_alive_ticks", staleWhileAliveTicks)
					consecutiveFailures = 0
					staleWhileAliveTicks = 0
				}
				continue
			}

			// The mount is unhealthy. Decide WHY before acting — killing a
			// live-but-slow juicefs is what drove the macFUSE remount thrash.
			if fm.juiceFSAlive() {
				// OFFLINE GUARD (release fix — Finder "connection interrupted"):
				// when the user is OFFLINE, a "stale" probe is overwhelmingly the
				// juicefs daemon being SATURATED by in-flight spool drains (writes
				// with nowhere to go offline) — BUSY, not wedged. Escalating to a
				// remount unmounts (diskutil unmount force → SIGTERM) a HEALTHY
				// daemon and rips the FUSE mountpoint out from under the kernel NFS
				// export → Finder "connection interrupted" mid-offline-workflow.
				// juicefs is alive and the user chose offline, so a remount is
				// both unnecessary and harmful. Freeze the escalation counter and
				// defer; the mount recovers when the drain settles / on reconnect.
				// Kill-switch JM_FUSE_OFFLINE_ESCALATE=1 restores the pre-fix
				// behavior (cellular-revert-safety doctrine).
				if fuseSkipEscalateWhileOffline() {
					staleWhileAliveTicks = 0
					consecutiveFailures = 0
					offlineDeferTicks++
					if offlineDeferTicks == 1 || offlineDeferTicks%6 == 0 {
						jmlog.Warn("fuse stale but OFFLINE + juicefs alive — NOT escalating (busy draining, not wedged); an app-side remount here would drop the NFS mount → Finder 'connection interrupted'",
							"offline_defer_ticks", offlineDeferTicks)
					}
					continue
				}
				offlineDeferTicks = 0
				// juicefs is alive — the mount is
				// stale, usually a flapping/blipping backend link. Default to
				// deferring (no kill, no macFUSE thrash). A live process does not
				// necessarily mean its backend connection is making progress: a
				// link blip can leave juicefs's
				// connection wedged with FUSE stat hanging indefinitely
				// (observed 2026-06-01 — a 7-minute continuous wedge with zero
				// self-recovery). So after a SUSTAINED continuous wedge,
				// escalate to a remount as a last resort — but ONLY when the
				// backend is actually reachable (a fresh mount can't succeed
				// during a real outage, where deferring is correct). The
				// counter resets the instant the mount recovers, so brief
				// flapping never accumulates to the threshold.
				staleWhileAliveTicks++
				consecutiveFailures = 0
				if staleWhileAliveTicks >= fm.staleEscalateTicks() {
					// COLD-START GRACE: within the warm-up window a busy-but-alive
					// mount must never be SIGKILL'd — the app's NFS server holds the
					// FUSE mount, so the follow-up unmount fails and leaves a zombie
					// (2026-06-25). Defer; the mount recovers on its own once the
					// initial reconcile/cache-warm settles.
					if fuseColdStartGrace > 0 && time.Since(watchdogStart) < fuseColdStartGrace {
						jmlog.Warn("fuse stale at escalation threshold but within cold-start grace — deferring (busy warming, not wedged)",
							"stale_ticks", staleWhileAliveTicks,
							"grace_remaining_sec", int((fuseColdStartGrace - time.Since(watchdogStart)).Seconds()))
						staleWhileAliveTicks = 0
						continue
					}
					// CONFIRM truly-wedged before the destructive SIGKILL+remount.
					// The 5s isMountedLocked probe flips to "stale" under mere
					// SLOWNESS (reconcile/backend contention); accumulating that
					// to the threshold and killing a live juicefs is what drove
					// the macFUSE remount thrash (2026-06-14). A slow-but-alive
					// mount answers a generous probe — if it does, it is NOT
					// wedged; reset and keep deferring instead of remounting.
					confirmTimeout := fm.confirmProbeTimeout()
					if fm.mountResponsiveWithin(confirmTimeout) {
						jmlog.Warn("fuse stale at escalation threshold but a generous probe succeeded — NOT remounting (slow, not wedged)",
							"stale_ticks", staleWhileAliveTicks,
							"confirm_timeout_sec", int(confirmTimeout.Seconds()))
						staleWhileAliveTicks = 0
						continue
					}
					if fm.backendReachable() {
						jmlog.Warn("fuse wedged but juicefs alive for a sustained window + backend reachable — escalating to remount (last resort)",
							"stale_ticks", staleWhileAliveTicks)
						if err := fm.Mount(); err != nil {
							jmlog.Error("fuse escalation remount failed", "error", err.Error())
						} else {
							jmlog.Info("fuse escalation remount succeeded", "after_stale_ticks", staleWhileAliveTicks)
						}
						staleWhileAliveTicks = 0
					} else {
						jmlog.Warn("fuse wedged + juicefs alive, but backend unreachable — deferring (a remount cannot succeed during a real outage)",
							"stale_ticks", staleWhileAliveTicks)
					}
					continue
				}
				// Rate-limit: first stale tick, then ~once a minute.
				if staleWhileAliveTicks == 1 || staleWhileAliveTicks%6 == 0 {
					jmlog.Warn("fuse mount stale but juicefs alive — deferring, not remounting",
						"stale_ticks", staleWhileAliveTicks,
						"hint", "transient backend/link stall; replacing a live service here would thrash macFUSE")
				}
				continue
			}

			// juicefs is genuinely GONE. App-side recovery is now the only option.
			staleWhileAliveTicks = 0

			// V2.3 U4: suppress the retry loop when it cannot or should not
			// succeed. (a) The user engaged offline — stand down until they
			// come back online (the mirror keeps serving nav). (b) The
			// backend is unreachable — a fresh juicefs mount needs Redis, so
			// each attempt just burns a 30s launch timeout and churns macFUSE
			// state. The next 10s tick re-evaluates both, so recovery is
			// automatic the moment conditions clear. Rate-limited logging.
			if fuseSkipRemountWhileUserOffline() {
				remountDeferTicks++
				if remountDeferTicks == 1 || remountDeferTicks%30 == 0 {
					jmlog.Info("juicefs gone but user is OFFLINE — not remounting (mirror serves nav; remount resumes when back online)",
						"defer_ticks", remountDeferTicks)
				}
				continue
			}
			if !fm.backendReachable() {
				remountDeferTicks++
				if remountDeferTicks == 1 || remountDeferTicks%30 == 0 {
					jmlog.Info("juicefs gone but backend unreachable — not remounting (a mount cannot succeed; retrying on recovery)",
						"defer_ticks", remountDeferTicks)
				}
				continue
			}
			remountDeferTicks = 0
			consecutiveFailures++
			jmlog.Warn("juicefs process tree gone — app-side remount",
				"attempt", consecutiveFailures)
			// H-3 L5: the exact child's Wait returned, so its mount is proven
			// stale. Only the unmount helper may detach it (by fsid, with
			// Liveness Dead it forces); a new birth over it is refused.
			if err := fm.RetireDeadChildMount(20 * time.Second); err != nil {
				jmlog.Warn("dead juicefs mount not retired by the unmount helper", "error", err.Error())
			}

			if err := fm.Mount(); err != nil {
				jmlog.Error("fuse remount failed",
					"attempt", consecutiveFailures, "error", err.Error())
				// A persistent failure here is usually the macFUSE
				// "operation not supported by device" kernel wedge, which
				// only a full app restart clears — back off hard rather than
				// thrash the kernel further.
				if strings.Contains(err.Error(), "operation not supported") {
					jmlog.Error("macFUSE refused the mount — kernel FUSE state is wedged; a full app restart is required to clear it",
						"detail", "repeated mount/unmount churn exhausted macFUSE")
				}
				if consecutiveFailures >= 3 {
					backoff := time.Duration(consecutiveFailures) * 10 * time.Second
					if backoff > 2*time.Minute {
						backoff = 2 * time.Minute
					}
					jmlog.Warn("fuse remount backing off", "backoff_sec", int(backoff.Seconds()))
					select {
					case <-fm.stopCh:
						return
					case <-time.After(backoff):
					}
				}
			} else {
				jmlog.Info("fuse remount succeeded", "after_attempts", consecutiveFailures)
				consecutiveFailures = 0
				// #12: every pooled fd predating this remount references the
				// DEAD mount — notify the bridge so the FDPool flushes them.
				fm.fireOnRemount()
			}
		}
	}
}

// juiceFSMountCommandTargets reports whether one process-table command is a
// JuiceFS mount for the exact mount point. Scoping is release-critical: a test
// mount or a second JuiceMount volume must never convince this manager that its
// own dead daemon is still alive and suppress recovery forever.
func parseJuiceFSMountCommand(command string) (redisURL, mountPoint string, ok bool) {
	fields := strings.Fields(command)
	if len(fields) < 4 || filepath.Base(fields[0]) != "juicefs" {
		return "", "", false
	}
	// --verbose is a global JuiceFS option and may precede the subcommand.
	// Keep the scan deliberately tight so an unrelated command containing the
	// word "mount" cannot be mistaken for the daemon.
	mountIndex := -1
	for i := 1; i < len(fields) && i <= 3; i++ {
		if fields[i] == "mount" {
			mountIndex = i
			break
		}
	}
	if mountIndex < 0 || mountIndex+2 >= len(fields) {
		return "", "", false
	}
	return fields[mountIndex+1], fields[mountIndex+2], true
}

func juiceFSMountCommandTargets(command, mountPoint string) bool {
	mountPoint = filepath.Clean(strings.TrimSpace(mountPoint))
	if mountPoint == "." || mountPoint == "" {
		return false
	}
	_, commandMount, ok := parseJuiceFSMountCommand(command)
	if !ok {
		return false
	}
	return filepath.Clean(commandMount) == mountPoint
}

type juiceFSMountProcess struct {
	pid     string
	command string
}

func juiceFSMountEndpointMatchesProcesses(processes []juiceFSMountProcess, redisURL string) (known, matches bool) {
	for _, process := range processes {
		processRedis, _, ok := parseJuiceFSMountCommand(process.command)
		if !ok {
			continue
		}
		known = true
		if processRedis == redisURL {
			return true, true
		}
	}
	return known, false
}

// juiceFSMountProcesses returns the processes for JuiceFS daemons targeting
// one exact mount point. The ps call is bounded so a sick host cannot pin the
// watchdog, startup, or shutdown path.
func juiceFSMountProcesses(mountPoint string) []juiceFSMountProcess {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,command=").Output()
	if err != nil {
		return nil
	}
	var processes []juiceFSMountProcess
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		cut := strings.IndexAny(line, " \t")
		if cut <= 0 {
			continue
		}
		pid := line[:cut]
		command := strings.TrimSpace(line[cut:])
		if juiceFSMountCommandTargets(command, mountPoint) {
			processes = append(processes, juiceFSMountProcess{pid: pid, command: command})
		}
	}
	return processes
}

// juiceFSMountProcessIDs returns the process IDs for JuiceFS daemons targeting
// one exact mount point.
func juiceFSMountProcessIDs(mountPoint string) []string {
	processes := juiceFSMountProcesses(mountPoint)
	ids := make([]string, 0, len(processes))
	for _, process := range processes {
		ids = append(ids, process.pid)
	}
	return ids
}

// isJuiceFSProcessAlive reports whether the daemon for mountPoint is alive.
func isJuiceFSProcessAlive(mountPoint string) bool {
	return len(juiceFSMountProcessIDs(mountPoint)) > 0
}

// BundledJuiceFSName is the pinned, patched JuiceFS executable that
// scripts/build-app.sh places beside the app's executables in Contents/MacOS.
// Its signal handlers never unmount by path (third_party/juicefs patch 0005).
const BundledJuiceFSName = juicefsbin.Name

// ResolveJuiceFSBinary captures the GUI's executable search before transferring
// an explicit path to the serving process, whose launchd PATH may differ.
func ResolveJuiceFSBinary() string { return findJuiceFSBin() }

// findJuiceFSBin returns the bundled JuiceFS beside the running executable, or
// "" when it is absent. It never searches PATH, Homebrew or /usr: any other
// juicefs answers SIGTERM with `umount -f <path>`, which can remove a mount
// this process does not own. Tests substitute it with juicefsbin.SetForTest.
func findJuiceFSBin() string { return juicefsbin.Resolve() }

// DetectJuiceFSCacheDir finds the default JuiceFS cache directory for the volume.
func DetectJuiceFSCacheDir() string {
	paths, isolated, scopeErr := appisolation.ResolvePaths()
	if scopeErr != nil {
		return ""
	}
	cacheBase := paths.CacheBase
	if !isolated {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		cacheBase = filepath.Join(home, ".juicefs", "cache")
	}
	entries, err := os.ReadDir(cacheBase)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			selected := filepath.Join(cacheBase, e.Name())
			if err := appisolation.ValidateCachePath(filepath.Join(selected, "raw", "chunks"), true); err != nil {
				return ""
			}
			return selected
		}
	}
	return ""
}

// logVolumeCapacityBreakdown queries macOS's URLResourceKey-equivalent
// volume capacity numbers and logs them. Free (what statfs shows), Important
// (what the system would free for an app declaring important usage —
// includes Time Machine local snapshots, iCloud purgeable, system caches),
// and Opportunistic (lower-priority purgeable). On a typical 1 TB Mac these
// can differ by hundreds of GB; surfacing the gap explains why the JuiceFS
// "space not enough" warning fires when the user thinks they have headroom.
//
// Implementation note: we shell out to `df -k` for free, and parse
// `diskutil info -plist` for the canonical "important" number. Doing this
// in pure Go avoids pulling in CoreFoundation; the cost is two short execs
// at startup, which is fine.
func logVolumeCapacityBreakdown(volume string) {
	type capStats struct {
		totalBytes         int64
		freeBytes          int64
		importantBytes     int64
		opportunisticBytes int64
	}
	var c capStats

	// Free + total via statfs
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err == nil {
		c.totalBytes = int64(st.Bsize) * int64(st.Blocks)
		c.freeBytes = int64(st.Bsize) * int64(st.Bavail)
	}

	// We don't try to compute the "important capacity" (purgeable-aware)
	// number from Go — the only reliable source is Foundation's
	// URLResourceKey, and shelling out to swift is fragile in a signed
	// app bundle. The Swift popover queries it directly and displays it
	// to the user. The Go side just logs the conservative statfs view
	// and triggers reclamation when free is tight.

	gb := func(b int64) float64 { return float64(b) / (1 << 30) }
	jmlog.Info("cache volume capacity",
		"volume", volume,
		"total_gb", fmt.Sprintf("%.1f", gb(c.totalBytes)),
		"free_now_gb", fmt.Sprintf("%.1f", gb(c.freeBytes)),
		"hint", "popover shows reclaimable space (Foundation URLResourceKey for important usage); use the Reclaim button or POST /reclaim to free Time Machine local snapshots")
}

// ReclaimAuthorization makes the destructive nature of purgeable-space
// reclamation explicit at every call site. There is deliberately no startup or
// automatic authorization value.
type ReclaimAuthorization uint8

const ReclaimExplicitUserAction ReclaimAuthorization = 1

// ReclaimPurgeableSpace asks macOS to free purgeable disk space — primarily
// Time Machine local snapshots, which can hoard tens of GB on a typical
// laptop. Returns the bytes freed, count of snapshots thinned, and a
// human-readable source description (e.g. "Time Machine local snapshots").
//
// Mechanism: `tmutil thinlocalsnapshots <vol> <purgeAmountBytes> <urgency>`.
// Urgency 4 is "as much as possible." It's a non-interactive, no-sudo
// operation supported on macOS 10.13+.
//
// We measure the actual freed bytes by sampling the volume's free space
// before and after; the tmutil command's own output is unreliable for this
// (depends on macOS version and which snapshots existed).
func ReclaimPurgeableSpace(auth ReclaimAuthorization, volume string, targetBytes int64) (freedBytes int64, snapshotsThinned int, source string, err error) {
	if auth != ReclaimExplicitUserAction {
		return 0, 0, "Time Machine local snapshots", fmt.Errorf("purgeable-space reclaim requires explicit user authorization")
	}
	beforeFree, _ := volumeFreeBytes(volume)

	// Urgency 4 = thin as much as possible. tmutil rejects "0" as an invalid
	// amount, so when the caller passes 0 we substitute a high number that
	// effectively means "give me as much as you can." 1 TiB is more than
	// any laptop SSD; tmutil will only free what's actually thinnable.
	var amount string
	if targetBytes > 0 {
		amount = fmt.Sprintf("%d", targetBytes)
	} else {
		amount = fmt.Sprintf("%d", int64(1)<<40) // 1 TiB
	}
	// tmutil can occasionally take a while to walk + delete snapshots; cap
	// at 90 s. If it doesn't return by then, we return what we measured —
	// the user can retry via the Reclaim button.
	tmCtx, tmCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer tmCancel()
	cmd := exec.CommandContext(tmCtx, "tmutil", "thinlocalsnapshots", volume, amount, "4")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, 0, "Time Machine local snapshots", fmt.Errorf("tmutil thinlocalsnapshots: %w (output: %s)",
			err, strings.TrimSpace(string(out)))
	}

	afterFree, _ := volumeFreeBytes(volume)
	freedBytes = afterFree - beforeFree
	if freedBytes < 0 {
		freedBytes = 0 // negative means another process took space concurrently
	}

	// Parse tmutil's output to count thinned snapshots. The format is
	// macOS-version-dependent but the common case has lines like:
	//   "Thinned local snapshots:" followed by one snapshot ID per line
	//   ("2024-01-01-120000" etc.). On macOS without anything to thin
	//   the output is usually empty. We count snapshot-ID lines (the
	//   ones that look like a tmutil snapshot timestamp).
	snapshotsThinned = countTmutilSnapshotLines(string(out))
	source = "Time Machine local snapshots"

	jmlog.Info("reclaimed purgeable space",
		"volume", volume,
		"freed_gb", fmt.Sprintf("%.1f", float64(freedBytes)/(1<<30)),
		"before_free_gb", fmt.Sprintf("%.1f", float64(beforeFree)/(1<<30)),
		"after_free_gb", fmt.Sprintf("%.1f", float64(afterFree)/(1<<30)),
		"snapshots_thinned", snapshotsThinned,
		"tmutil_output", strings.TrimSpace(string(out)))
	return freedBytes, snapshotsThinned, source, nil
}

// countTmutilSnapshotLines extracts the count of thinned snapshots from
// `tmutil thinlocalsnapshots` output. Matches lines that look like a
// tmutil snapshot ID — typically "YYYY-MM-DD-HHMMSS" optionally prefixed
// by some path metadata. Conservative: only counts lines that start with
// 4 digits (a year), to avoid double-counting header lines.
func countTmutilSnapshotLines(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) < 10 {
			continue
		}
		// Crude but effective: snapshot lines start with a 4-digit year
		// followed by '-' (e.g. "2026-05-18-..." or full paths ending in
		// such a timestamp). Headers like "Thinned local snapshots:" do
		// not match. False positives are extremely unlikely for tmutil.
		if trimmed[0] >= '0' && trimmed[0] <= '9' &&
			trimmed[1] >= '0' && trimmed[1] <= '9' &&
			trimmed[2] >= '0' && trimmed[2] <= '9' &&
			trimmed[3] >= '0' && trimmed[3] <= '9' &&
			trimmed[4] == '-' {
			n++
		}
	}
	return n
}

func volumeFreeBytes(volume string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err != nil {
		return 0, err
	}
	return int64(st.Bsize) * int64(st.Bavail), nil
}

func volumeTotalBytes(volume string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(volume, &st); err != nil {
		return 0, err
	}
	return int64(st.Bsize) * int64(st.Blocks), nil
}

// cacheVolumeLocation resolves the volume JuiceFS will use for its block cache.
// The nearest existing ancestor handles a cache directory JuiceFS has not yet
// created; its device is checked again by engine capacity sampling.
func cacheVolumeLocation(configured string) (root, probe string, device uint64) {
	path := configured
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", "", 0
		}
		path = filepath.Join(home, ".juicefs", "cache")
	}
	// A multi-directory JuiceFS cache needs per-volume accounting. Refuse to
	// publish a single-volume policy for one.
	if path == "memory" || strings.Contains(path, ":") {
		return "", "", 0
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", "", 0
	}
	root = path
	for {
		var st syscall.Stat_t
		if err := syscall.Stat(path, &st); err == nil {
			return root, path, uint64(st.Dev)
		} else if !errors.Is(err, syscall.ENOENT) {
			return root, "", 0
		}
		parent := filepath.Dir(path)
		if parent == path {
			return root, "", 0
		}
		path = parent
	}
}

// envOr returns the value of environment variable key, or def when unset/empty.
// Used for the per-cache FUSE metadata TTL overrides so each knob has a visible
// default at its call site instead of a bare literal.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// metaCacheTTLs returns the four juicefs FUSE metadata cache TTLs, split by
// role. Extracted from Mount() purely so the values are unit-testable: the
// previous incarnation of this policy sat behind JM_WAN_MODE, which nothing in
// production ever set, so the intended WAN values shipped dead for months with
// no test to notice. See the commentary at the call site for the full rationale.
//
// Returns (attr, entry, dirEntry, negative).
func metaCacheTTLs() (attr, entry, dirEntry, negative string) {
	entry = envOr("JM_META_ENTRY_CACHE", "60s")
	dirEntry = envOr("JM_META_DIR_ENTRY_CACHE", "60s")
	attr = envOr("JM_META_ATTR_CACHE", "10s")
	negative = envOr("JM_META_NEGATIVE_CACHE", "5s")
	// Documented escape hatch: one value for all four.
	if v := os.Getenv("JM_META_CACHE_SECS"); v != "" {
		return v, v, v, v
	}
	return attr, entry, dirEntry, negative
}

// MetaURLForMount returns the metadata URL to use, applying the
// JM_DEBUG_META_ADDR development override when set.
//
// WHY THIS EXISTS. The reported bug — navigation instant offline, awful on
// cellular — is only reproducible at high RTT. Every measurement taken on a LAN
// shows online nav already at parity with offline (3ms/dir vs 6ms/dir at 4.8ms
// RTT), so a fix cannot be validated where it is being written. Iterating on a
// cellular problem while measuring on WiFi is how you convince yourself of a fix
// that does nothing.
//
// The latency lives in juicefs's own Redis round trips, not in this process's
// metadata client, so an in-Go delay would inject latency into the wrong path
// entirely. Pointing juicefs at a latency-injecting TCP proxy (cmd/jmlatency)
// reproduces the real thing: the same syscalls, the same FUSE layer, the same
// gates, with only the metadata round trip slowed.
//
// SAFETY. Unset — the overwhelmingly normal case — this is a pure passthrough
// and the mount is byte-identical to before. It never rewrites the SCHEME, DB
// index, or credentials, only host:port, and only when the override parses as a
// URL; anything malformed leaves the original untouched rather than mounting
// against a half-built address. It is deliberately NOT a persisted setting: an
// env var dies with the process, so a forgotten debug knob cannot survive a
// relaunch and silently degrade a user's mount.
func MetaURLForMount(redisURL string) string {
	addr := os.Getenv("JM_DEBUG_META_ADDR")
	if addr == "" {
		return redisURL
	}
	u, err := url.Parse(redisURL)
	if err != nil || u.Host == "" {
		return redisURL
	}
	u.Host = addr
	return u.String()
}

// openCacheTTL returns the --open-cache duration to pass, or "" to omit the
// flag entirely (juicefs default 0s = disabled = today's behavior). See the
// call site for why this is opt-in rather than on by default.
// Config wins over the environment: the config is the only channel that can
// reach a shipped app (see FUSEConfig.OpenCacheTTL). The env var is retained so
// a CLI/test run can still set it without building a config.
func (fm *FUSEManager) openCacheTTL() string {
	if v := normalizeOpenCacheTTL(fm.cfg.OpenCacheTTL); v != "" {
		return v
	}
	return normalizeOpenCacheTTL(os.Getenv("JM_OPEN_CACHE"))
}

// normalizeOpenCacheTTL treats empty/0/0s as "omit the flag" so an explicitly
// zeroed config reads as OFF rather than as unset.
func normalizeOpenCacheTTL(v string) string {
	if v == "" || v == "0" || v == "0s" {
		return ""
	}
	return v
}

// juiceFSAlive is the watchdog's liveness signal. With an exact child
// receipt it is that child's own Wait (design §5: no ps-title or path
// matching); without one it falls back to the process-table probe.
func (fm *FUSEManager) juiceFSAlive() bool {
	if fm.mu.TryLock() {
		r := fm.activeReceipt
		fm.mu.Unlock()
		if r != nil && r.waitDone != nil {
			return !waitFUSEChannel(r.waitDone, 0)
		}
	}
	return isJuiceFSProcessAliveFn(fm.cfg.MountPoint)
}
