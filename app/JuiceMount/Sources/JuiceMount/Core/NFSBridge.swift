import Foundation
import JuiceMountCore

/// Idiomatic Swift wrapper around the Go c-archive (libnfsd.a).
/// All calls are blocking — wrap in a background queue if calling from the UI thread.
public enum NFSBridge {

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
            logLevel: String = "info"
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

    public struct CacheStatus: Codable, Equatable {
        public var aggregate: AggregateStats = AggregateStats()
        public var roots: [RootSummary] = []
        public var live: LiveCacheStats = LiveCacheStats()
        public var offline_mode: Bool = false
    }

    public struct PinResult: Codable {
        public var ok: Bool = false
        public var files_pinned: Int = 0
        public var bytes_total: Int64 = 0
        public var error: String?
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
        return (try? JSONDecoder().decode(CacheStatus.self, from: Data(json.utf8))) ?? CacheStatus()
    }

    public static func setOffline(_ on: Bool) {
        let cstr = NFSServerSetOffline(on ? 1 : 0)
        if let cstr { NFSServerFreeString(cstr) }
    }

    public static var isOffline: Bool {
        NFSServerIsOffline() != 0
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
        URLSession.shared.dataTask(with: req) { data, _, _ in
            defer { sem.signal() }
            guard let data else { return }
            result = try? JSONDecoder().decode(SelfTestResult.self, from: data)
        }.resume()
        sem.wait()
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
