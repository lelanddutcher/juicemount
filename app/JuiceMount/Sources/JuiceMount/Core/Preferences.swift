import Foundation
import Observation

/// User-configurable preferences. Persists to UserDefaults under the suite
/// "com.juicemount.app". Default values match the JM5 CLI defaults.
@Observable
public final class Preferences {

    public var volumeName: String {
        didSet { save() }
    }
    public var mountPoint: String {
        didSet { save() }
    }
    public var redisURL: String {
        didSet { save() }
    }
    public var nfsListenAddr: String {
        didSet { save() }
    }
    public var metricsAddr: String {
        didSet { save() }
    }
    public var dbPath: String {
        didSet { save() }
    }
    public var ssdCacheGB: Int {
        didSet { save() }
    }
    public var memoryBufferMB: Int {
        didSet { save() }
    }
    public var memBufFileLimitMB: Int {
        didSet { save() }
    }
    public var reconcileSeconds: Int {
        didSet { save() }
    }
    public var startAtLogin: Bool {
        didSet { save() }
    }
    public var showSearchHotkey: Bool {
        didSet { save() }
    }
    /// Show macOS notifications when offline mode auto-engages or
    /// recovers. Default off — `VISION.md` non-negotiable says no
    /// notifications without opt-in, and user-attention budget is
    /// precious. Users who care about offline events can flip this
    /// in Preferences.
    public var offlineNotificationsEnabled: Bool {
        didSet { save() }
    }

    public init(
        volumeName: String = "zpool",
        mountPoint: String = "/Volumes/zpool",
        redisURL: String = "redis://127.0.0.1:6379/1",
        nfsListenAddr: String = "127.0.0.1:11049",
        metricsAddr: String = "127.0.0.1:11050",
        dbPath: String = "",
        ssdCacheGB: Int = 100,
        memoryBufferMB: Int = 2048,
        memBufFileLimitMB: Int = 128,
        reconcileSeconds: Int = 30,
        startAtLogin: Bool = false,
        showSearchHotkey: Bool = true,
        offlineNotificationsEnabled: Bool = false
    ) {
        self.volumeName = volumeName
        self.mountPoint = mountPoint
        self.redisURL = redisURL
        self.nfsListenAddr = nfsListenAddr
        self.metricsAddr = metricsAddr
        self.dbPath = dbPath.isEmpty ? Self.defaultDBPath() : dbPath
        self.ssdCacheGB = ssdCacheGB
        self.memoryBufferMB = memoryBufferMB
        self.memBufFileLimitMB = memBufFileLimitMB
        self.reconcileSeconds = reconcileSeconds
        self.startAtLogin = startAtLogin
        self.showSearchHotkey = showSearchHotkey
        self.offlineNotificationsEnabled = offlineNotificationsEnabled
    }

    public static func defaultDBPath() -> String {
        let support = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)
            .first ?? URL(fileURLWithPath: NSHomeDirectory())
        let dir = support.appendingPathComponent("JuiceMount", isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("metadata.db").path
    }

    public static func fuseMountPath() -> String {
        // JM5 mounts JuiceFS internally at this path
        return "\(NSHomeDirectory())/.juicemount/fuse-internal"
    }

    /// Default log file: ~/Library/Logs/JuiceMount/juicemount.log
    /// (rotated by the Go side; see jmlog.openWithRotation).
    public static func defaultLogPath() -> String {
        let logsDir: URL = {
            if let lib = FileManager.default.urls(
                for: .libraryDirectory, in: .userDomainMask).first {
                return lib.appendingPathComponent("Logs/JuiceMount", isDirectory: true)
            }
            return URL(fileURLWithPath: "\(NSHomeDirectory())/Library/Logs/JuiceMount")
        }()
        try? FileManager.default.createDirectory(
            at: logsDir, withIntermediateDirectories: true)
        return logsDir.appendingPathComponent("juicemount.log").path
    }

    public func toServerConfig() -> NFSBridge.ServerConfig {
        NFSBridge.ServerConfig(
            redisURL: redisURL,
            fusePath: Self.fuseMountPath(),
            mountPoint: mountPoint,
            listenAddr: nfsListenAddr,
            dbPath: dbPath,
            cacheSize: String(ssdCacheGB * 1024), // GB → MB
            metricsAddr: metricsAddr,
            logFile: Self.defaultLogPath(),
            logLevel: "info"
        )
    }

    // MARK: - Persistence

    // Use the standard suite — UserDefaults rejects suiteName == bundle identifier.
    private static let defaults = UserDefaults.standard

    private enum Key: String {
        case volumeName, mountPoint, redisURL, nfsListenAddr, metricsAddr, dbPath
        case ssdCacheGB, memoryBufferMB, memBufFileLimitMB, reconcileSeconds
        case startAtLogin, showSearchHotkey
        case offlineNotificationsEnabled
    }

    public static func load() -> Preferences {
        let d = defaults
        return Preferences(
            volumeName:        d.string(forKey: Key.volumeName.rawValue) ?? "zpool",
            mountPoint:        d.string(forKey: Key.mountPoint.rawValue) ?? "/Volumes/zpool",
            redisURL:          d.string(forKey: Key.redisURL.rawValue) ?? "redis://127.0.0.1:6379/1",
            nfsListenAddr:     d.string(forKey: Key.nfsListenAddr.rawValue) ?? "127.0.0.1:11049",
            metricsAddr:       d.string(forKey: Key.metricsAddr.rawValue) ?? "127.0.0.1:11050",
            dbPath:            d.string(forKey: Key.dbPath.rawValue) ?? defaultDBPath(),
            ssdCacheGB:        d.object(forKey: Key.ssdCacheGB.rawValue) as? Int ?? 100,
            memoryBufferMB:    d.object(forKey: Key.memoryBufferMB.rawValue) as? Int ?? 2048,
            memBufFileLimitMB: d.object(forKey: Key.memBufFileLimitMB.rawValue) as? Int ?? 128,
            reconcileSeconds:  d.object(forKey: Key.reconcileSeconds.rawValue) as? Int ?? 30,
            startAtLogin:      d.bool(forKey: Key.startAtLogin.rawValue),
            showSearchHotkey:  d.object(forKey: Key.showSearchHotkey.rawValue) as? Bool ?? true,
            offlineNotificationsEnabled: d.bool(forKey: Key.offlineNotificationsEnabled.rawValue)
        )
    }

    public func save() {
        let d = Self.defaults
        d.set(volumeName, forKey: Key.volumeName.rawValue)
        d.set(mountPoint, forKey: Key.mountPoint.rawValue)
        d.set(redisURL, forKey: Key.redisURL.rawValue)
        d.set(nfsListenAddr, forKey: Key.nfsListenAddr.rawValue)
        d.set(metricsAddr, forKey: Key.metricsAddr.rawValue)
        d.set(dbPath, forKey: Key.dbPath.rawValue)
        d.set(ssdCacheGB, forKey: Key.ssdCacheGB.rawValue)
        d.set(memoryBufferMB, forKey: Key.memoryBufferMB.rawValue)
        d.set(memBufFileLimitMB, forKey: Key.memBufFileLimitMB.rawValue)
        d.set(reconcileSeconds, forKey: Key.reconcileSeconds.rawValue)
        d.set(startAtLogin, forKey: Key.startAtLogin.rawValue)
        d.set(showSearchHotkey, forKey: Key.showSearchHotkey.rawValue)
        d.set(offlineNotificationsEnabled, forKey: Key.offlineNotificationsEnabled.rawValue)
    }
}
