import Foundation
import JuiceMountCore

/// Idiomatic Swift wrapper around the Go c-archive (libnfsd.a).
/// All calls are blocking — wrap in a background queue if calling from the UI thread.
public enum NFSBridge {

    /// Per-call ephemeral session for loopback metrics calls. URLSession.shared
    /// caches connections aggressively; after a wifi/interface switch the
    /// cached HTTP/1.1 connection to 127.0.0.1 can be stuck in a "reachable
    /// but unusable" state for many minutes — the call times out silently
    /// even though `curl` to the same URL succeeds. An ephemeral session has
    /// no on-disk cache and short connection reuse windows, so each call
    /// opens a fresh TCP socket. The wall-clock cost on loopback is single-
    /// digit milliseconds; well worth the resilience.
    fileprivate static func loopbackSession() -> URLSession {
        let cfg = URLSessionConfiguration.ephemeral
        cfg.timeoutIntervalForRequest = 2
        cfg.timeoutIntervalForResource = 5
        cfg.waitsForConnectivity = false
        return URLSession(configuration: cfg)
    }

    public struct ServerConfig: Codable {
        public var redisURL: String
        public var fusePath: String
        public var mountPoint: String
        public var listenAddr: String
        public var dbPath: String
        public var cacheSize: String
        public var metricsAddr: String
        public var logFile: String
        public var logLevel: String
        public var bucketOverride: String
        public var spoolEnable: Bool
        public var spoolSizeGB: Int
        /// LB-4 (Phase 3b) tuning knobs. 0 means "Go-side default"
        /// (2048 MB membuf budget / 128 MB file limit / 30 s reconcile)
        /// so config JSON without these fields keeps prior behavior.
        public var memoryBufferMB: Int
        public var memBufFileLimitMB: Int
        public var reconcileSeconds: Int

        enum CodingKeys: String, CodingKey {
            case redisURL = "redis_url"
            case fusePath = "fuse_path"
            case mountPoint = "mount_point"
            case listenAddr = "listen_addr"
            case dbPath = "db_path"
            case cacheSize = "cache_size"
            case metricsAddr = "metrics_addr"
            case logFile = "log_file"
            case logLevel = "log_level"
            case bucketOverride = "bucket_override"
            case spoolEnable = "spool_enable"
            case spoolSizeGB = "spool_size_gb"
            case memoryBufferMB = "memory_buffer_mb"
            case memBufFileLimitMB = "membuf_file_limit_mb"
            case reconcileSeconds = "reconcile_seconds"
        }

        public init(
            redisURL: String,
            fusePath: String,
            mountPoint: String,
            listenAddr: String = "127.0.0.1:11049",
            dbPath: String,
            cacheSize: String = "100000",
            metricsAddr: String = "127.0.0.1:11050",
            logFile: String = "",
            logLevel: String = "info",
            bucketOverride: String = "",
            spoolEnable: Bool = false,
            spoolSizeGB: Int = 0, // 0 = Auto (Go core sizes to free disk minus floor)
            memoryBufferMB: Int = 0,
            memBufFileLimitMB: Int = 0,
            reconcileSeconds: Int = 0
        ) {
            self.redisURL = redisURL
            self.fusePath = fusePath
            self.mountPoint = mountPoint
            self.listenAddr = listenAddr
            self.dbPath = dbPath
            self.cacheSize = cacheSize
            self.metricsAddr = metricsAddr
            self.logFile = logFile
            self.logLevel = logLevel
            self.bucketOverride = bucketOverride
            self.spoolEnable = spoolEnable
            self.spoolSizeGB = spoolSizeGB
            self.memoryBufferMB = memoryBufferMB
            self.memBufFileLimitMB = memBufFileLimitMB
            self.reconcileSeconds = reconcileSeconds
        }
    }

    public struct Stats: Codable, Equatable {
        public var running: Bool
        public var entryCount: Int
        public var lastSyncMs: Int64
        public var lastSyncTime: String
        public var serverAddr: String
        public var healthRedis: Bool
        public var healthMinIO: Bool
        public var healthFUSE: Bool

        enum CodingKeys: String, CodingKey {
            case running
            case entryCount = "entry_count"
            case lastSyncMs = "last_sync_ms"
            case lastSyncTime = "last_sync_time"
            case serverAddr = "server_addr"
            case healthRedis = "health_redis"
            case healthMinIO = "health_minio"
            case healthFUSE = "health_fuse"
        }

        public static let zero = Stats(
            running: false, entryCount: 0, lastSyncMs: 0,
            lastSyncTime: "", serverAddr: "",
            healthRedis: false, healthMinIO: false, healthFUSE: false
        )
    }

    // MARK: - Pin / cache status

    public struct AggregateStats: Codable, Equatable {
        public var TotalFiles: Int = 0
        public var ReadyFiles: Int = 0
        public var PendingFiles: Int = 0
        public var FailedFiles: Int = 0
        public var TotalBytes: Int64 = 0
        public var CachedBytes: Int64 = 0
    }

    public struct RootSummary: Codable, Identifiable, Equatable {
        public var Root: String
        public var TotalFiles: Int = 0
        public var ReadyFiles: Int = 0
        public var PendingFiles: Int = 0
        public var FailedFiles: Int = 0
        public var TotalBytes: Int64 = 0
        public var CachedBytes: Int64 = 0
        public var id: String { Root }
    }

    public struct LiveCacheStats: Codable, Equatable {
        public var BytesPrefetched: Int64 = 0
        public var FilesPrefetched: Int64 = 0
        public var CurrentFile: String = ""
        public var Workers: Int = 0
    }

    /// Pinned-set-vs-disk-capacity verdict (R-1). `over_capacity` true means the
    /// pinned set is larger than the cache disk can keep fully resident, so some
    /// pinned files will never be available offline until the user frees disk or
    /// unpins a folder. `shortfall_bytes` is how much to free / unpin.
    public struct CapacityVerdict: Codable, Equatable {
        public var over_capacity: Bool = false
        public var pinned_bytes: Int64 = 0
        public var cache_capacity_bytes: Int64 = 0
        public var shortfall_bytes: Int64 = 0
        public var disk_free_bytes: Int64 = 0
        public var cache_usage_bytes: Int64 = 0

        public init() {}

        private enum CodingKeys: String, CodingKey {
            case over_capacity, pinned_bytes, cache_capacity_bytes
            case shortfall_bytes, disk_free_bytes, cache_usage_bytes
        }

        // Absence-tolerant: an older Go core (or the no-pinstore branch) omits
        // `capacity`, and JSON null on any field must not abort the parent
        // CacheStatus decode (the roots:null lesson — see CacheStatus below).
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.over_capacity = try c.decodeIfPresent(Bool.self, forKey: .over_capacity) ?? false
            self.pinned_bytes = try c.decodeIfPresent(Int64.self, forKey: .pinned_bytes) ?? 0
            self.cache_capacity_bytes = try c.decodeIfPresent(Int64.self, forKey: .cache_capacity_bytes) ?? 0
            self.shortfall_bytes = try c.decodeIfPresent(Int64.self, forKey: .shortfall_bytes) ?? 0
            self.disk_free_bytes = try c.decodeIfPresent(Int64.self, forKey: .disk_free_bytes) ?? 0
            self.cache_usage_bytes = try c.decodeIfPresent(Int64.self, forKey: .cache_usage_bytes) ?? 0
        }
    }

    /// One pin root whose subtree is still being enumerated (R-2). Rendered as
    /// a "Scanning <folder>…" row so a click on Pin shows feedback immediately,
    /// before any pinned_files rows exist.
    public struct ScanningRoot: Codable, Identifiable, Equatable {
        public var root: String = ""
        public var files_found: Int = 0
        public var bytes_found: Int64 = 0
        public var since_sec: Int64 = 0
        public var id: String { root }

        public init() {}

        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.root = try c.decodeIfPresent(String.self, forKey: .root) ?? ""
            self.files_found = try c.decodeIfPresent(Int.self, forKey: .files_found) ?? 0
            self.bytes_found = try c.decodeIfPresent(Int64.self, forKey: .bytes_found) ?? 0
            self.since_sec = try c.decodeIfPresent(Int64.self, forKey: .since_sec) ?? 0
        }

        private enum CodingKeys: String, CodingKey {
            case root, files_found, bytes_found, since_sec
        }
    }

    public struct CacheStatus: Codable, Equatable {
        public var aggregate: AggregateStats = AggregateStats()
        public var roots: [RootSummary] = []
        public var live: LiveCacheStats = LiveCacheStats()
        public var offline_mode: Bool = false
        public var capacity: CapacityVerdict = CapacityVerdict()
        public var scanning: [ScanningRoot] = []

        public init() {}

        private enum CodingKeys: String, CodingKey {
            case aggregate, roots, live, offline_mode, capacity, scanning
        }

        /// Null/absence-tolerant decode. ROOT CAUSE of the long-standing
        /// "offline toggle is write-only / can't turn off" bug (2026-06-03):
        /// the Go cgo emits `"roots": null` whenever nothing is pinned (a nil
        /// Go slice marshals to JSON null). Swift's *synthesized* Codable uses
        /// `decode` (not `decodeIfPresent`) for non-optional properties and
        /// ignores their default values — so a null `roots` threw
        /// `valueNotFound`, aborting the ENTIRE decode. `NFSBridge.cacheStatus()`
        /// then silently returned a default `CacheStatus()` whose `offline_mode`
        /// is false. The `"offline_mode": true` sat right there in the JSON but
        /// was never read. Result: with no pins, the Swift-side offline_mode was
        /// hard-pinned false, so the toggle always showed "online" and every tap
        /// sent setOffline(true) — offline could never be cleared from the UI.
        /// (Verified: /tmp/cachestatus_proof.swift — OLD throws→false, NEW→true.)
        /// decodeIfPresent + per-field defaults make every field tolerant of a
        /// null or missing value so `offline_mode` is always read correctly.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.aggregate = try c.decodeIfPresent(AggregateStats.self, forKey: .aggregate) ?? AggregateStats()
            self.roots = try c.decodeIfPresent([RootSummary].self, forKey: .roots) ?? []
            self.live = try c.decodeIfPresent(LiveCacheStats.self, forKey: .live) ?? LiveCacheStats()
            self.offline_mode = try c.decodeIfPresent(Bool.self, forKey: .offline_mode) ?? false
            self.capacity = try c.decodeIfPresent(CapacityVerdict.self, forKey: .capacity) ?? CapacityVerdict()
            self.scanning = try c.decodeIfPresent([ScanningRoot].self, forKey: .scanning) ?? []
        }
    }

    public struct PinResult: Codable {
        public var ok: Bool = false
        public var files_pinned: Int = 0
        public var bytes_total: Int64 = 0
        public var error: String?
        /// R-2: true when the subtree walk was kicked off asynchronously —
        /// files_pinned/bytes_total are 0 and the real counts arrive via
        /// /cache-status as the walk lands. Defaults false for older cores.
        public var scanning: Bool = false
    }

    @discardableResult
    public static func pin(_ path: String) throws -> PinResult {
        let c = strdup(path); defer { free(c) }
        guard let cstr = NFSServerPin(c) else {
            throw BridgeError.notRunning
        }
        defer { NFSServerFreeString(cstr) }
        let json = String(cString: cstr)
        return try JSONDecoder().decode(PinResult.self, from: Data(json.utf8))
    }

    @discardableResult
    public static func unpin(_ path: String) throws -> PinResult {
        let c = strdup(path); defer { free(c) }
        guard let cstr = NFSServerUnpin(c) else {
            throw BridgeError.notRunning
        }
        defer { NFSServerFreeString(cstr) }
        let json = String(cString: cstr)
        return try JSONDecoder().decode(PinResult.self, from: Data(json.utf8))
    }

    public static func cacheStatus() -> CacheStatus {
        guard let cstr = NFSServerCacheStatus() else { return CacheStatus() }
        defer { NFSServerFreeString(cstr) }
        let json = String(cString: cstr)
        do {
            return try JSONDecoder().decode(CacheStatus.self, from: Data(json.utf8))
        } catch {
            // Surface decode failures LOUDLY rather than silently returning a
            // default CacheStatus (offline_mode=false). A silent default here is
            // exactly how the roots:null decode bug stayed invisible across many
            // sessions while the offline toggle appeared stuck. With the
            // null-tolerant CacheStatus.init(from:) this should never fire.
            appLog("cacheStatus decode failed — returning default, offline state may be wrong: \(error)")
            return CacheStatus()
        }
    }

    public static func setOffline(_ on: Bool) {
        let cstr = NFSServerSetOffline(on ? 1 : 0)
        if let cstr { NFSServerFreeString(cstr) }
    }

    public static var isOffline: Bool {
        NFSServerIsOffline() != 0
    }

    // MARK: - Offline state (iter 5 — tier-1.7/1.8/1.9)
    //
    // Mirrors `pin.OfflineState` on the Go side. Two independent
    // sources can engage offline mode: a manual user-intent toggle
    // (`user_offline`) and the reachability monitor's auto-engage
    // (`auto_offline`). The effective state is the OR. The `reason`
    // and `since` fields are populated only for auto-engage and let
    // the UI surface "disconnected for M:SS" without timestamp math.

    public struct OfflineState: Codable, Equatable {
        public var offline: Bool = false
        public var user_offline: Bool = false
        public var auto_offline: Bool = false
        public var reason: String = ""
        public var since: String = ""    // RFC3339 timestamp
        public var since_sec: Int64 = 0

        /// True when offline mode is engaged by automatic reachability
        /// detection (vs. user toggle). The UI uses this to pick a
        /// distinct color (blue) and message — distinguishes "offline
        /// because the network dropped" from "offline because you
        /// flipped the switch."
        public var isAutoEngaged: Bool {
            auto_offline && !user_offline
        }
    }

    /// Independent /health probe. Defensive backstop: when the cgo Stats
    /// path is for any reason reporting unhealthy (or being missed by the
    /// state machine) we still want to know whether the Go-side monitor
    /// considers itself healthy. Returns nil on any failure (timeout,
    /// decode error, non-200 status). Successful nil-`reason` + `healthy`
    /// means "Go thinks every backend is up right now."
    public struct HealthProbe: Codable, Equatable {
        public var healthy: Bool = false
        public var components: [String: String] = [:]
        public var reason: String?

        private enum CodingKeys: String, CodingKey {
            case healthy, components, reason
        }

        /// Null/absence-tolerant decode. Same root cause as the long-standing
        /// CacheStatus.roots:null bug (see `CacheStatus.init(from:)`): the Go
        /// `/health` handler builds `components` from a Go map that is nil
        /// before the health provider is wired up or after the monitor stops,
        /// and a nil Go map marshals to JSON `null` (or, under `,omitempty`,
        /// the key is dropped entirely). Swift's *synthesized* Codable uses
        /// `decode` (not `decodeIfPresent`) for the non-optional `components`,
        /// so a null OR a missing key throws (valueNotFound / keyNotFound) and
        /// aborts the ENTIRE decode — silently discarding the `healthy` flag the
        /// caller actually reads. `decodeIfPresent` + defaults make every field
        /// tolerant of a null/missing value. (Go side also normalizes to `{}`.)
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.healthy = try c.decodeIfPresent(Bool.self, forKey: .healthy) ?? false
            self.components = try c.decodeIfPresent([String: String].self, forKey: .components) ?? [:]
            self.reason = try c.decodeIfPresent(String.self, forKey: .reason)
        }

        /// Core backend reachability — FUSE/Redis/MinIO all "ok", IGNORING NFS.
        /// R-8: the menu-bar `.disconnected` state is ENTERED on a FUSE-only
        /// criterion, but the loopback NFS mount recovers on a slower timeline
        /// than the backend (deferred remount up to ~180s). `healthy` (= /health
        /// Overall) ANDs in NFS, so gating recovery on it leaves the red
        /// "Disconnected" latched while the backend — and actual reads — are
        /// already fine. Recovery backstops gate on THIS instead, matching the
        /// `fullyHealthy = FUSE&&Redis&&MinIO` criterion the primary state path
        /// uses. NFS-mount health is surfaced separately via `volumeMounted`.
        public var coreHealthy: Bool {
            components["fuse"] == "ok" && components["redis"] == "ok" && components["minio"] == "ok"
        }
    }

    public static func healthProbe(metricsAddr: String = "127.0.0.1:11050") -> HealthProbe? {
        guard let url = URL(string: "http://\(metricsAddr)/health") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 2
        let sem = DispatchSemaphore(value: 0)
        var result: HealthProbe?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            // Log decode failures rather than swallowing them with `try?` —
            // a silent failure here is exactly how the roots:null decode bug
            // hid for so long (see `cacheStatus()`). nil is still returned
            // (= "/health unreachable or unparseable"), now with a log line.
            do {
                result = try JSONDecoder().decode(HealthProbe.self, from: data)
            } catch {
                appLog("healthProbe decode failed — returning nil: \(error)")
            }
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    /// Fetch the offline state from the local metrics server. Blocking
    /// — call from a background queue. Endpoint: `/offline` (GET, no
    /// `?on=` param returns state JSON; `?on=true/false` is the toggle).
    public static func offlineState(metricsAddr: String = "127.0.0.1:11050") -> OfflineState? {
        guard let url = URL(string: "http://\(metricsAddr)/offline") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 2
        let sem = DispatchSemaphore(value: 0)
        var result: OfflineState?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            result = try? JSONDecoder().decode(OfflineState.self, from: data)
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    // MARK: - Write spool (Option 2)
    //
    // Mirrors `nfs.SpoolStatusResponse` on the Go side, served at `/spool`
    // on the metrics port. The endpoint returns 503 with `enabled:false`
    // when the spool is off (JM_SPOOL_ENABLE != 1) — the body still decodes,
    // so a non-nil `SpoolStatus{enabled:false}` means "reachable but off",
    // whereas nil means the metrics server itself was unreachable.

    public struct SpoolEntryView: Codable, Equatable, Identifiable {
        public var path: String = ""
        public var size: Int64 = 0
        public var drainState: String = ""
        public var drainAttempts: Int = 0
        public var lastError: String?
        public var updatedAtUnix: Int64 = 0
        /// Seconds since the row last made progress (now - updated_at).
        public var ageSec: Int64 = 0
        /// True when the entry is making no progress and won't without
        /// intervention (LB-5): a leaked-handle `writing` row quiescent
        /// beyond the escalation window, or a row whose drain retries are
        /// exhausted. The popover offers "Recover stalled" for these.
        public var stalled: Bool = false

        // Identity for ForEach: path + last-update so a re-queued path
        // (same name, new generation) gets a fresh row rather than animating
        // a stale one.
        public var id: String { "\(path)|\(updatedAtUnix)" }

        enum CodingKeys: String, CodingKey {
            case path, size
            case drainState = "drain_state"
            case drainAttempts = "drain_attempts"
            case lastError = "last_error"
            case updatedAtUnix = "updated_at_unix"
            case ageSec = "age_sec"
            case stalled
        }

        public init() {}

        /// Null/absence-tolerant decode — same discipline as SpoolStatus
        /// and CacheStatus below. Synthesized Codable would `decode` the
        /// non-optionals and abort the WHOLE /spool decode on a missing
        /// key (e.g. polling an older Go core that doesn't emit age_sec/
        /// stalled yet), silently blanking the spool UI.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.path = try c.decodeIfPresent(String.self, forKey: .path) ?? ""
            self.size = try c.decodeIfPresent(Int64.self, forKey: .size) ?? 0
            self.drainState = try c.decodeIfPresent(String.self, forKey: .drainState) ?? ""
            self.drainAttempts = try c.decodeIfPresent(Int.self, forKey: .drainAttempts) ?? 0
            self.lastError = try c.decodeIfPresent(String.self, forKey: .lastError)
            self.updatedAtUnix = try c.decodeIfPresent(Int64.self, forKey: .updatedAtUnix) ?? 0
            self.ageSec = try c.decodeIfPresent(Int64.self, forKey: .ageSec) ?? 0
            self.stalled = try c.decodeIfPresent(Bool.self, forKey: .stalled) ?? false
        }
    }

    public struct SpoolStatus: Codable, Equatable {
        public var enabled: Bool = false
        public var error: String?
        public var pendingFiles: Int = 0
        public var pendingBytes: Int64 = 0
        public var inProgress: Int64 = 0
        public var succeeded: Int64 = 0
        public var failed: Int64 = 0
        public var quarantined: Int64 = 0
        public var capacityUsed: Int64 = 0
        public var capacityTotal: Int64 = 0
        /// LB-5 stuck-spool signals (Phase 2): counts of listed stalled /
        /// failed entries and the age of the oldest pending row.
        public var stalledFiles: Int = 0
        public var failedFiles: Int = 0
        public var oldestPendingAgeSec: Int64 = 0
        public var entries: [SpoolEntryView] = []

        /// True when there is active or queued upload work to surface.
        public var hasActivity: Bool { pendingFiles > 0 || inProgress > 0 }

        /// True when something needs the user's attention (stalled or
        /// failed entries) even if nothing is actively uploading.
        public var needsAttention: Bool { stalledFiles > 0 || failedFiles > 0 }

        enum CodingKeys: String, CodingKey {
            case enabled, error
            case pendingFiles = "pending_files"
            case pendingBytes = "pending_bytes"
            case inProgress = "in_progress"
            case succeeded, failed, quarantined
            case capacityUsed = "capacity_used"
            case capacityTotal = "capacity_total"
            case stalledFiles = "stalled_files"
            case failedFiles = "failed_files"
            case oldestPendingAgeSec = "oldest_pending_age_sec"
            case entries
        }

        /// Null/absence-tolerant decode. Same root cause as the long-standing
        /// CacheStatus.roots:null bug (see `CacheStatus.init(from:)`): the Go
        /// `/spool` handler returns `"entries": null` whenever the spool is
        /// disabled — the COMMON case, since it is opt-in via JM_SPOOL_ENABLE=1
        /// — or an early error path is taken, because a nil Go slice marshals
        /// to JSON `null`. Swift's *synthesized* Codable uses `decode` (not
        /// `decodeIfPresent`) for the non-optional `entries`, so that null
        /// throws valueNotFound and aborts the ENTIRE decode — silently
        /// discarding the `enabled` flag the UI reads to show spool state.
        /// `decodeIfPresent` + defaults make every field tolerant of
        /// null/missing. (Go side also emits `[]` instead of null.)
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.enabled = try c.decodeIfPresent(Bool.self, forKey: .enabled) ?? false
            self.error = try c.decodeIfPresent(String.self, forKey: .error)
            self.pendingFiles = try c.decodeIfPresent(Int.self, forKey: .pendingFiles) ?? 0
            self.pendingBytes = try c.decodeIfPresent(Int64.self, forKey: .pendingBytes) ?? 0
            self.inProgress = try c.decodeIfPresent(Int64.self, forKey: .inProgress) ?? 0
            self.succeeded = try c.decodeIfPresent(Int64.self, forKey: .succeeded) ?? 0
            self.failed = try c.decodeIfPresent(Int64.self, forKey: .failed) ?? 0
            self.quarantined = try c.decodeIfPresent(Int64.self, forKey: .quarantined) ?? 0
            self.capacityUsed = try c.decodeIfPresent(Int64.self, forKey: .capacityUsed) ?? 0
            self.capacityTotal = try c.decodeIfPresent(Int64.self, forKey: .capacityTotal) ?? 0
            self.stalledFiles = try c.decodeIfPresent(Int.self, forKey: .stalledFiles) ?? 0
            self.failedFiles = try c.decodeIfPresent(Int.self, forKey: .failedFiles) ?? 0
            self.oldestPendingAgeSec = try c.decodeIfPresent(Int64.self, forKey: .oldestPendingAgeSec) ?? 0
            self.entries = try c.decodeIfPresent([SpoolEntryView].self, forKey: .entries) ?? []
        }
    }

    /// Fetch the write-spool status from the local metrics server. Blocking
    /// — call from a background queue. Endpoint: `/spool` (GET). Returns nil
    /// only when the metrics server is unreachable; an enabled:false body
    /// (HTTP 503) still decodes to a non-nil value.
    public static func spoolStatus(metricsAddr: String = "127.0.0.1:11050") -> SpoolStatus? {
        guard let url = URL(string: "http://\(metricsAddr)/spool") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 2
        let sem = DispatchSemaphore(value: 0)
        var result: SpoolStatus?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            // Log decode failures rather than swallowing them with `try?` (see
            // `cacheStatus()` and the roots:null root cause). nil is still
            // returned (= "/spool unreachable or unparseable").
            do {
                result = try JSONDecoder().decode(SpoolStatus.self, from: data)
            } catch {
                appLog("spoolStatus decode failed — returning nil: \(error)")
            }
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    /// Result of a `/spool-recover` action (LB-5).
    public struct SpoolRecoverResult: Codable, Equatable {
        public var ok: Bool = false
        public var action: String = ""
        public var recovered: Int = 0
        public var error: String?

        /// Tolerant decode — same discipline as SpoolStatus above.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.ok = try c.decodeIfPresent(Bool.self, forKey: .ok) ?? false
            self.action = try c.decodeIfPresent(String.self, forKey: .action) ?? ""
            self.recovered = try c.decodeIfPresent(Int.self, forKey: .recovered) ?? 0
            self.error = try c.decodeIfPresent(String.self, forKey: .error)
        }
    }

    /// Trigger a spool recovery action on the Go side (LB-5). Blocking —
    /// call from a background queue. Endpoint: `/spool-recover` (GET with
    /// query params, the same loopback mutation convention as
    /// `/offline?on=…`).
    ///
    ///   - `retry-failed`:  requeue failed rows whose spool file survives
    ///                      (fresh attempt budget) and wake the drainer.
    ///   - `clear-stalled`: force-finalize leaked-handle writing entries
    ///                      so their bytes drain (never deletes data).
    ///
    /// Returns nil when the metrics server is unreachable or the body is
    /// unparseable (logged, not swallowed).
    public static func spoolRecover(action: String, metricsAddr: String = "127.0.0.1:11050") -> SpoolRecoverResult? {
        var comps = URLComponents()
        comps.scheme = "http"
        // host:port split — metricsAddr is "127.0.0.1:11050".
        let parts = metricsAddr.split(separator: ":", maxSplits: 1)
        comps.host = parts.first.map(String.init) ?? "127.0.0.1"
        comps.port = parts.count > 1 ? Int(parts[1]) : nil
        comps.path = "/spool-recover"
        comps.queryItems = [URLQueryItem(name: "action", value: action)]
        guard let url = comps.url else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        // clear-stalled fsyncs + finalizes entries; allow headroom.
        req.timeoutInterval = 15
        let sem = DispatchSemaphore(value: 0)
        var result: SpoolRecoverResult?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            do {
                result = try JSONDecoder().decode(SpoolRecoverResult.self, from: data)
            } catch {
                appLog("spoolRecover(\(action)) decode failed — returning nil: \(error)")
            }
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    // MARK: - Clear failed files (roadmap 4.8)

    /// Result of `/spool-recover?action=clear-failed`. Two-phase: a PREVIEW
    /// (confirm=false) reports `wouldClear` / `wouldFreeBytes` and mutates
    /// nothing; a CONFIRM (confirm=true) reports `cleared` / `freedBytes`.
    /// Mirrors the clear-failed branch in handleSpoolRecoverHTTP.
    public struct ClearFailedResult: Codable, Equatable {
        public var ok: Bool = false
        public var preview: Bool = false
        public var wouldClear: Int = 0
        public var wouldFreeBytes: Int64 = 0
        public var cleared: Int = 0
        public var freedBytes: Int64 = 0
        public var error: String?

        enum CodingKeys: String, CodingKey {
            case ok, preview, cleared, error
            case wouldClear = "would_clear"
            case wouldFreeBytes = "would_free_bytes"
            case freedBytes = "freed_bytes"
        }

        /// Tolerant decode — same JSON-null discipline as SpoolStatus.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.ok = try c.decodeIfPresent(Bool.self, forKey: .ok) ?? false
            self.preview = try c.decodeIfPresent(Bool.self, forKey: .preview) ?? false
            self.wouldClear = try c.decodeIfPresent(Int.self, forKey: .wouldClear) ?? 0
            self.wouldFreeBytes = try c.decodeIfPresent(Int64.self, forKey: .wouldFreeBytes) ?? 0
            self.cleared = try c.decodeIfPresent(Int.self, forKey: .cleared) ?? 0
            self.freedBytes = try c.decodeIfPresent(Int64.self, forKey: .freedBytes) ?? 0
            self.error = try c.decodeIfPresent(String.self, forKey: .error)
        }
    }

    /// Clear (or preview clearing) permanently-failed spool entries. Blocking —
    /// call from a background queue. `confirm=false` is a safe PREVIEW (no
    /// mutation); `confirm=true` permanently discards the un-drained bytes. The
    /// UI MUST preview first, show the count/bytes, and only confirm on the
    /// user's explicit OK. Endpoint: `/spool-recover?action=clear-failed`.
    public static func spoolClearFailed(confirm: Bool, metricsAddr: String = "127.0.0.1:11050") -> ClearFailedResult? {
        var comps = URLComponents()
        comps.scheme = "http"
        let parts = metricsAddr.split(separator: ":", maxSplits: 1)
        comps.host = parts.first.map(String.init) ?? "127.0.0.1"
        comps.port = parts.count > 1 ? Int(parts[1]) : nil
        comps.path = "/spool-recover"
        comps.queryItems = [URLQueryItem(name: "action", value: "clear-failed")]
        if confirm { comps.queryItems?.append(URLQueryItem(name: "confirm", value: "true")) }
        guard let url = comps.url else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 15
        let sem = DispatchSemaphore(value: 0)
        var result: ClearFailedResult?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            do {
                result = try JSONDecoder().decode(ClearFailedResult.self, from: data)
            } catch {
                appLog("spoolClearFailed(confirm:\(confirm)) decode failed: \(error)")
            }
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    // MARK: - Background activity (roadmap 4.10)

    /// One background operation surfaced by `/activity`. Mirrors
    /// activityOperation in cbridge.go.
    public struct ActivityOperation: Codable, Equatable, Identifiable {
        public var kind: String = ""
        public var active: Bool = false
        public var detail: String = ""
        public var id: String { kind }

        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.kind = try c.decodeIfPresent(String.self, forKey: .kind) ?? ""
            self.active = try c.decodeIfPresent(Bool.self, forKey: .active) ?? false
            self.detail = try c.decodeIfPresent(String.self, forKey: .detail) ?? ""
        }
        enum CodingKeys: String, CodingKey { case kind, active, detail }
    }

    /// The `/activity` snapshot — what background work (reconcile / drain /
    /// prefetch) is running, with a plain-language `summary`.
    public struct Activity: Codable, Equatable {
        public var busy: Bool = false
        public var summary: String = ""
        public var operations: [ActivityOperation] = []

        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.busy = try c.decodeIfPresent(Bool.self, forKey: .busy) ?? false
            self.summary = try c.decodeIfPresent(String.self, forKey: .summary) ?? ""
            self.operations = try c.decodeIfPresent([ActivityOperation].self, forKey: .operations) ?? []
        }
        enum CodingKeys: String, CodingKey { case busy, summary, operations }
    }

    /// Fetch the background-activity snapshot. Blocking — call from a
    /// background queue. Endpoint: `/activity` (GET). Returns nil only when the
    /// metrics server is unreachable.
    public static func activity(metricsAddr: String = "127.0.0.1:11050") -> Activity? {
        guard let url = URL(string: "http://\(metricsAddr)/activity") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 5
        let sem = DispatchSemaphore(value: 0)
        var result: Activity?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            result = try? JSONDecoder().decode(Activity.self, from: data)
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    // MARK: - Mount Now (LB-2)

    /// Result of `/mount-now` — the control-plane action that re-runs the
    /// user-visible NFS mount. Mirrors handleMountNowHTTP in cbridge.go.
    public struct MountNowResult: Codable, Equatable {
        public var ok: Bool = false
        public var mountPoint: String = ""
        public var alreadyMounted: Bool = false
        public var error: String?

        enum CodingKeys: String, CodingKey {
            case ok
            case mountPoint = "mount_point"
            case alreadyMounted = "already_mounted"
            case error
        }

        /// Tolerant decode — same JSON-null discipline as SpoolStatus et al.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.ok = try c.decodeIfPresent(Bool.self, forKey: .ok) ?? false
            self.mountPoint = try c.decodeIfPresent(String.self, forKey: .mountPoint) ?? ""
            self.alreadyMounted = try c.decodeIfPresent(Bool.self, forKey: .alreadyMounted) ?? false
            self.error = try c.decodeIfPresent(String.self, forKey: .error)
        }
    }

    /// Trigger the LB-2 "Mount Now" action. Blocking — call from a
    /// background queue, NEVER the serial workQueue: the Go side may sit
    /// inside the macOS admin-password prompt for as long as the user
    /// stares at it, and parking workQueue would freeze stats polling.
    ///
    /// Idempotent server-side (already-mounted → ok). Returns nil when the
    /// metrics server is unreachable or the body is unparseable (logged).
    public static func mountNow(metricsAddr: String = "127.0.0.1:11050") -> MountNowResult? {
        guard let url = URL(string: "http://\(metricsAddr)/mount-now") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        // Generous: the AppleScript admin prompt waits on the human.
        req.timeoutInterval = 180
        // Deliberately NOT loopbackSession(): its 5 s resource timeout
        // would kill the request while the password prompt is up.
        let cfg = URLSessionConfiguration.ephemeral
        cfg.timeoutIntervalForRequest = 180
        cfg.timeoutIntervalForResource = 180
        cfg.waitsForConnectivity = false
        let session = URLSession(configuration: cfg)
        let sem = DispatchSemaphore(value: 0)
        var result: MountNowResult?
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            do {
                result = try JSONDecoder().decode(MountNowResult.self, from: data)
            } catch {
                appLog("mountNow decode failed — returning nil: \(error)")
            }
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    // MARK: - Self-test (A2)
    //
    // Phase A production-hardening: a 10 MB read measured against the live
    // mount, classified into green (>=200 MB/s), yellow (50–200 MB/s), or
    // red (<50 MB/s). Served by Go over HTTP on the metrics port so we don't
    // need to add a new C symbol for it.

    public struct SelfTestResult: Codable, Equatable {
        public var elapsed_ms: Int64 = 0
        public var bytes_read: Int64 = 0
        public var mb_per_sec: Double = 0
        public var status: String = ""     // "green" | "yellow" | "red" | "error"
        public var hint: String = ""
        public var ran_at: String = ""     // RFC3339
        public var target: String = ""

        /// Write probe results — mirrors the Go-side fields added to expose
        /// whether the user-visible write path is healthy. If `write_ok`
        /// is false, the status above has been downgraded accordingly.
        public var write_ok: Bool = true
        public var write_ms: Int64 = 0
        public var write_hint: String = ""

        /// B.6 (2026-05-17): first-byte read latency in milliseconds.
        /// Round-trip signal distinct from mb_per_sec — high
        /// first-byte-ms + high MB/s = cold cache hit after a slow
        /// metadata hop; low both = uniformly slow backend. Popover
        /// surfaces both alongside each other.
        public var first_byte_ms: Int64 = 0

        /// True when the result represents something the user might care to
        /// see surfaced on the icon (anything not green, including errors).
        /// "pending" (no run yet — placeholder served while Go's background
        /// goroutine is still running its first probe) is NOT attention-worthy;
        /// it's a transient state, not a real problem.
        public var isAttentionWorthy: Bool {
            !(status == "green" || status.isEmpty || status == "pending")
        }
    }

    /// Fetch the self-test result from the local metrics server. Pass
    /// `force: true` to rerun the probe; otherwise the cached result is
    /// returned. Blocking — call from a background queue.
    ///
    /// The metrics server listens on 127.0.0.1:11050 by default; we read the
    /// configured address to avoid hard-coding (the Swift ServerConfig allows
    /// overriding `metrics_addr`).
    public static func selfTest(force: Bool = false, metricsAddr: String = "127.0.0.1:11050") -> SelfTestResult? {
        guard let url = URL(string: "http://\(metricsAddr)/self-test") else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = force ? "POST" : "GET"
        // The probe itself blocks up to ~200 ms on a healthy mount; allow
        // headroom for slow paths (a red mount measuring at 5 MB/s still
        // needs ~2 s).
        req.timeoutInterval = 30

        let sem = DispatchSemaphore(value: 0)
        var result: SelfTestResult?
        let session = loopbackSession()
        session.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            result = try? JSONDecoder().decode(SelfTestResult.self, from: data)
        }.resume()
        sem.wait()
        session.finishTasksAndInvalidate()
        return result
    }

    public struct SearchResult: Codable, Identifiable, Hashable {
        public var path: String
        public var name: String
        public var isDir: Bool
        public var size: Int64
        public var mtime: String
        public var rank: Double

        public var id: String { path }

        enum CodingKeys: String, CodingKey {
            case path, name, size, mtime, rank
            case isDir = "is_dir"
        }
    }

    public enum BridgeError: Error, LocalizedError {
        case notRunning
        case startFailed(String)
        case decodingFailed(String)
        case searchFailed(String)

        public var errorDescription: String? {
            switch self {
            case .notRunning:                return "JuiceMount server is not running"
            case .startFailed(let msg):      return "Failed to start: \(msg)"
            case .decodingFailed(let msg):   return "Failed to decode response: \(msg)"
            case .searchFailed(let msg):     return "Search failed: \(msg)"
            }
        }
    }

    /// Bridge a one-line message into the Go-side rotating file log
    /// (~/Library/Logs/JuiceMount/juicemount.log). os.Logger info/warning lines
    /// are NOT persisted to `log show`, which kept the Swift state machine
    /// invisible across rounds of the stuck-offline bug. This routes key
    /// UI-state events into the same log everything else uses (prefixed
    /// "[swift]"). Cheap — a buffered file write on the Go side.
    public static func appLog(_ message: String) {
        message.withMutableCString { NFSServerLog($0) }
    }

    // MARK: - Lifecycle

    /// Start the NFS server with the given configuration. Returns the bound listen address.
    @discardableResult
    public static func start(config: ServerConfig) throws -> String {
        let json = try JSONEncoder().encode(config)
        let cString = String(data: json, encoding: .utf8) ?? ""
        return try cString.withMutableCString { ptr in
            guard let result = NFSServerStart(ptr) else {
                throw BridgeError.startFailed("null response")
            }
            defer { NFSServerFreeString(result) }
            let str = String(cString: result)
            if str.hasPrefix("error:") {
                throw BridgeError.startFailed(String(str.dropFirst(6).trimmingCharacters(in: .whitespaces)))
            }
            return str
        }
    }

    /// User-visible Stop: tears everything down AND unmounts FUSE + NFS.
    /// This matches user expectations — "Stop" means the mount is gone.
    ///
    /// NFS unmount goes through `diskutil unmount` first (no admin prompt
    /// in the common case), falling back to a privileged umount only if
    /// the mount is genuinely busy.
    public static func stop() {
        NFSServerShutdown()
    }

    /// Soft stop for the internal Restart path. Leaves FUSE + NFS mounted
    /// so the subsequent Start avoids re-mounting. Never call this from a
    /// user-initiated Stop — the user expects the mount to disappear.
    public static func softStop() {
        NFSServerStop()
    }

    /// Hard stop (alias for `stop()`). Kept for callers that want to be
    /// explicit about intent (e.g. applicationWillTerminate).
    public static func shutdown() {
        NFSServerShutdown()
    }

    /// Middle-ground stop (QA-7, 2026-05-17): unmounts NFS so the
    /// /Volumes/<name> path disappears from the user's view, then tears
    /// down the NFS server + metadata + caches + metrics. Leaves FUSE
    /// and the JuiceFS daemon alive so the subsequent Start avoids the
    /// admin password re-prompt for re-mount. Wired to the "Stop mount
    /// and finish sync" menu item.
    public static func stopMount() {
        NFSServerStopMount()
    }

    public static var isRunning: Bool {
        NFSServerIsRunning() != 0
    }

    // MARK: - Stats

    public static func stats() throws -> Stats {
        guard let cstr = NFSServerStats() else {
            return .zero
        }
        defer { NFSServerFreeString(cstr) }
        let json = String(cString: cstr)
        guard let data = json.data(using: .utf8) else {
            throw BridgeError.decodingFailed("invalid utf8")
        }
        do {
            return try JSONDecoder().decode(Stats.self, from: data)
        } catch {
            throw BridgeError.decodingFailed(error.localizedDescription)
        }
    }

    @discardableResult
    public static func syncNow() throws -> String {
        guard let cstr = NFSServerSyncNow() else {
            throw BridgeError.notRunning
        }
        defer { NFSServerFreeString(cstr) }
        let result = String(cString: cstr)
        if result.hasPrefix("error:") {
            throw BridgeError.startFailed(String(result.dropFirst(6).trimmingCharacters(in: .whitespaces)))
        }
        return result
    }

    // MARK: - Search

    /// Full-text search on filenames using the FTS5 trigram index.
    /// - Parameters:
    ///   - query: search string (partial matches supported, e.g. "explosion")
    ///   - limit: max results (default 50)
    ///   - parentPath: optional subtree scope (empty = whole filesystem)
    public static func search(_ query: String, limit: Int = 50, parentPath: String = "") throws -> [SearchResult] {
        if query.isEmpty { return [] }
        let cQuery = strdup(query)
        let cParent = strdup(parentPath)
        defer {
            free(cQuery)
            free(cParent)
        }
        guard let cstr = NFSServerSearch(cQuery, Int32(limit), cParent) else {
            throw BridgeError.searchFailed("null response")
        }
        defer { NFSServerFreeString(cstr) }
        let json = String(cString: cstr)
        if json.hasPrefix("error:") {
            throw BridgeError.searchFailed(String(json.dropFirst(6).trimmingCharacters(in: .whitespaces)))
        }
        guard let data = json.data(using: .utf8) else {
            throw BridgeError.decodingFailed("invalid utf8 in search response")
        }
        do {
            // Tolerate "null" response from Go (empty result)
            if json.trimmingCharacters(in: .whitespaces) == "null" { return [] }
            return try JSONDecoder().decode([SearchResult].self, from: data)
        } catch {
            throw BridgeError.decodingFailed(error.localizedDescription)
        }
    }
}

// MARK: - Helpers

private extension String {
    /// Run a closure with a mutable C string — the Go bridge expects `char*` not `const char*`.
    /// Even an empty string yields a one-element null-terminated array, so baseAddress
    /// is always non-nil in practice. We still guard explicitly to make intent clear.
    func withMutableCString<R>(_ body: (UnsafeMutablePointer<CChar>) throws -> R) rethrows -> R {
        var bytes = Array(self.utf8CString)
        return try bytes.withUnsafeMutableBufferPointer { buf in
            guard let ptr = buf.baseAddress else {
                // Empty buffer — fall back to a temporary null byte. utf8CString
                // always includes the trailing null so we shouldn't reach here.
                var nullByte: CChar = 0
                return try withUnsafeMutablePointer(to: &nullByte) { try body($0) }
            }
            return try body(ptr)
        }
    }
}
