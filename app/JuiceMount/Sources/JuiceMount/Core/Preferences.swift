import Foundation
import CoreFoundation
import JuiceMountCore
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
    /// The Redis metadata URL as saved: scheme, username, host, port and
    /// database, never the password. A password typed into this URL is moved
    /// to `credentialStore` on save and re-attached only in memory for the
    /// engine (`toServerConfig`). See RedisCredentialStore.swift.
    public var redisURL: String {
        didSet { save() }
    }

    /// The Redis password, held in memory after one store read at load() and
    /// updated in memory whenever it changes, so building a config (which
    /// Settings does on every render) never touches the Keychain. It applies
    /// only to `redisPasswordAccount`: after the URL's host, port or user
    /// changes, Settings shows that no password is saved for the new server
    /// until one is entered (`redisPasswordStatus`).
    public private(set) var redisPassword: String = ""
    public private(set) var redisPasswordAccount: String?

    /// In-memory Link pairing key (see `linkAuthKey`).
    private var linkAuthKeyValue: String = ""

    /// H-8B object-store key from Manager enrollment, held in memory after one
    /// store read at load() (like the Redis password). It applies only to
    /// `objectKeyAccount`, the Redis account it was saved for.
    @ObservationIgnored private var objectKey: (accessKey: String, secretKey: String)?
    @ObservationIgnored private var objectKeyAccount: String?

    /// The Manager's web address (enrollment, H-8B). Empty = the Redis host on
    /// the Manager's default port 30190 (`effectiveManagerURL`).
    public var managerURL: String {
        didSet { save() }
    }

    /// What Settings shows for this Mac's Manager enrollment. Main thread.
    public private(set) var enrollment = EnrollmentStatus()

    /// A pre-enrollment login is saved (`restorePreviousLogin` can undo the
    /// switch to the principal). Read once at load, then kept current.
    public private(set) var canRestorePreviousLogin = false

    /// Request access was clicked: the next approved step may switch the
    /// Redis login even though the user changed it by hand (in memory only).
    @ObservationIgnored var explicitSwitchRequested = false

    /// Storage-key status (H-8 per-Mac keys), main thread, in memory only:
    /// the last step said the key is still being created; the last status
    /// said the storage server refused the key in use (its S3 code); the
    /// periodic check runs.
    @ObservationIgnored var storageKeyPending = false
    @ObservationIgnored var storageKeyRefusedCode: String?
    @ObservationIgnored var objectKeyWatchStarted = false
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

    /// Opt into bounded native video preview preparation on a qualified,
    /// healthy connection. Saved changes apply after Stop everything → Start.
    public var nativePreviewWarm: Bool {
        didSet { save() }
    }

    /// Optional override for the S3 bucket URL the local JuiceFS daemon
    /// uses to fetch chunks. Empty = use the URL stored in Redis by the
    /// server-side `juicefs format`. Set this when the Redis-stored URL
    /// is unreachable from this Mac — typically because the server was
    /// formatted with a docker-internal hostname (e.g. `http://minio:9000/...`)
    /// that only resolves inside the docker network.
    ///
    /// Example value: `http://10.0.0.5:30151/zpool`
    ///
    /// Passed to `juicefs mount` as `--bucket <value>`, which takes
    /// precedence over the URL stored in Redis at format time.
    public var s3EndpointOverride: String {
        didSet { save() }
    }

    /// Enable the write spool (Option 2): writes land on local SSD and ack
    /// immediately, then drain into JuiceFS in the background — making large
    /// copies feel local even over a slow WAN. Sets the `JM_SPOOL_ENABLE`
    /// env var that the Go core reads at server start, so it takes effect on
    /// the next (re)start.
    ///
    /// ON BY DEFAULT since 2026-08-04. It shipped off "during rollout", and that
    /// left the default install on the IN-PLACE FUSE write path, which has no
    /// contiguous-prefix guard: a read of a not-yet-written region returns ZEROS
    /// WITH err=nil, indistinguishable from data (proven in
    /// nfs/inplace_hole_probe_test.go). The spool read path is the one that
    /// carries that guard — it holds such a read (JUKEBOX) instead of serving
    /// zeros — so the protection built for exactly this hazard was inert for
    /// everyone who never opened Preferences.
    ///
    /// An explicit user choice still wins: the loader reads the stored value and
    /// only falls back to true when the key is ABSENT, so anyone who deliberately
    /// turned the spool off stays off.
    public var spoolEnabled: Bool {
        didSet { save() }
    }
    /// Automatically resume accepted replacement saves when the provider can
    /// prove stage birth. A saved change takes effect after Stop everything → Start.
    /// The old replacementRecovery key was autosaved even without a user choice,
    /// so its false value cannot distinguish a legacy default from an opt-out.
    /// Only the new override records an explicit choice; ordinary saves leave
    /// both its absence and the old raw value intact.
    public var replacementRecovery: Bool {
        didSet {
            Self.defaults.set(replacementRecovery, forKey: Key.replacementRecoveryOverride.rawValue)
            save()
        }
    }
    /// Local-SSD spool capacity in GB (`JM_SPOOL_SIZE_GB`). 0 = Auto: the Go
    /// core normally sizes the buffer to free disk minus a 20 GB preferred
    /// safety floor. If the machine is already below that floor, a bounded
    /// working window keeps small writes functional while preserving a hard
    /// 10 GB OS floor. Once full, writes pause until the drain frees space or
    /// the user reconnects. Default Auto.
    public var spoolCapacityGB: Int {
        didSet { save() }
    }

    /// LB-1 first-run gate. False until the user finishes (or explicitly
    /// continues past) the setup-assistant welcome flow. While false, the
    /// app shows the onboarding window at launch instead of auto-starting
    /// into a dead-end "Disconnected". The window can be reopened any time
    /// via the popover's "Setup Assistant…" item.
    public var hasCompletedOnboarding: Bool {
        didSet { save() }
    }

    /// Use the Mac's existing private-network route (for example Tailscale,
    /// NetBird, Firezone, ZeroTier, or a conventional VPN) instead of starting
    /// JuiceMount's embedded tsnet node. The saved Link pairing is retained so
    /// switching modes never destroys credentials or identity.
    ///
    /// This is also the correct escape hatch when two userspace overlays would
    /// otherwise stack: embedded tsnet may bind its control socket to the
    /// physical interface and bypass a system Packet Tunnel's subnet route.
    /// In this mode the configured Redis and object-store addresses are dialed
    /// normally and macOS chooses the secure system route.
    public var useExistingPrivateNetwork: Bool {
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
        offlineNotificationsEnabled: Bool = false,
        nativePreviewWarm: Bool = false,
        s3EndpointOverride: String = "",
        spoolEnabled: Bool = true,
        replacementRecovery: Bool = true,
        spoolCapacityGB: Int = 0, // 0 = Auto (size to free disk minus a safety floor)
        hasCompletedOnboarding: Bool = false,
        useExistingPrivateNetwork: Bool = true,
        managerURL: String = ""
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
        self.nativePreviewWarm = nativePreviewWarm
        self.s3EndpointOverride = s3EndpointOverride
        self.spoolEnabled = spoolEnabled
        self.replacementRecovery = replacementRecovery
        self.spoolCapacityGB = spoolCapacityGB
        self.hasCompletedOnboarding = hasCompletedOnboarding
        self.useExistingPrivateNetwork = useExistingPrivateNetwork
        self.managerURL = managerURL
    }

    public static func defaultDBPath() -> String {
        if let scope = AppIsolation.active { return scope.child("data/metadata.db") }
        let support = FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)
            .first ?? URL(fileURLWithPath: NSHomeDirectory())
        let dir = support.appendingPathComponent("JuiceMount", isDirectory: true)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("metadata.db").path
    }

    public static func fuseMountPath() -> String {
        // JM5 mounts JuiceFS internally at this path
        if let scope = AppIsolation.active { return scope.child("fuse-internal") }
        return "\(NSHomeDirectory())/.juicemount/fuse-internal"
    }

    /// Default log file: ~/Library/Logs/JuiceMount/juicemount.log
    /// (rotated by the Go side; see jmlog.openWithRotation).
    public static func defaultLogPath() -> String {
        if let scope = AppIsolation.active { return scope.child("logs/juicemount.log") }
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

    /// juicefs `--open-cache` TTL, read straight from UserDefaults at
    /// server-start time. Read-only and NOT persisted by `save()` on purpose:
    /// a stored property would be loaded once at launch and then written back
    /// by any unrelated preference change, silently clobbering a value the
    /// tester had just set from the command line.
    ///
    /// This is the cellular-test lever. It has no Settings control — see the
    /// stale-slice-map warning on `NFSBridge.ServerConfig.openCacheTTL`. To arm
    /// it for a measured run:
    ///
    ///     defaults write com.juicemount.app openCacheTTL -string 5s
    ///
    /// then restart the server (the flag is only read when the FUSE mount is
    /// built). To disarm: `defaults delete com.juicemount.app openCacheTTL`.
    ///
    /// It rides the CONFIG rather than the environment because the environment
    /// cannot reach a shipped app at all: Go snapshots os.Environ at c-archive
    /// init, so a Swift setenv() afterwards is invisible to os.Getenv. That is
    /// exactly how JM_WAN_MODE spent every shipped build doing nothing.
    public var openCacheTTL: String {
        let raw = (Self.defaults.string(forKey: Key.openCacheTTL.rawValue) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return (raw == "0" || raw == "0s") ? "" : raw
    }

    // JuiceMount Link (Tier-2): remote access pairing.
    // Stored in UserDefaults directly (not save()) — these are set once
    // during pairing and rarely change.
    public var linkServerURL: String {
        get { Self.defaults.string(forKey: "link_server_url") ?? "" }
        set {
            Self.defaults.set(newValue, forKey: "link_server_url")
            // The stored key is bound to the control URL. It is visible in the
            // same form, so it follows an edited URL rather than vanishing.
            if !linkAuthKeyValue.isEmpty { storeLinkAuthKey() }
        }
    }
    /// The Link pairing key. Kept in the credential store (never the
    /// preferences plist or the service's saved config) so the service can
    /// re-pair on its own after launchd restarts it; held in memory after load.
    public var linkAuthKey: String {
        get { linkAuthKeyValue }
        set {
            guard newValue != linkAuthKeyValue else { return }
            linkAuthKeyValue = newValue
            storeLinkAuthKey()
        }
    }

    private func storeLinkAuthKey() {
        let key = linkAuthKeyValue, account = RedisURLCredential.linkAccount(for: linkServerURL)
        let store = Self.linkCredentialStore
        Self.credentialWrites.async {
            if !store.setPassword(key, for: account) {
                NSLog("JuiceMount: could not save the Link pairing key to the Keychain")
            }
        }
    }
    public var linkHostname: String {
        get { Self.defaults.string(forKey: "link_hostname") ?? "" }
        set { Self.defaults.set(newValue, forKey: "link_hostname") }
    }

    public func toServerConfig() -> NFSBridge.ServerConfig {
        var cfg = NFSBridge.ServerConfig(
            // In memory only: the engine's Redis clients authenticate from the
            // URL, and the FUSE child gets the password via META_PASSWORD,
            // never argv (health/fuse.go desktopJuiceFSMountArgs).
            redisURL: engineRedisURL(),
            fusePath: Self.fuseMountPath(),
            mountPoint: mountPoint,
            listenAddr: nfsListenAddr,
            dbPath: dbPath,
            cacheSize: String(ssdCacheGB * 1024), // GB → MB
            metricsAddr: metricsAddr,
            logFile: Self.defaultLogPath(),
            spoolDir: AppIsolation.active?.child("spool") ?? "",
            fuseMetricsAddr: AppIsolation.active?.fuseMetricsAddr ?? "",
            logLevel: "info",
            bucketOverride: s3EndpointOverride,
            spoolEnable: spoolEnabled,
            spoolSizeGB: spoolCapacityGB,
            replacementRecovery: spoolEnabled && replacementRecovery,
            // LB-4: previously saved-but-ignored; now consumed by the Go
            // side (membuf construction + reconcile loop) at server start.
            memoryBufferMB: memoryBufferMB,
            memBufFileLimitMB: memBufFileLimitMB,
            reconcileSeconds: reconcileSeconds,
            openCacheTTL: openCacheTTL,
            nativePreviewWarm: nativePreviewWarm
        )
        // H-8B object key, in memory only, and only for the account it was
        // saved for (the service gives it to juicefs via the environment).
        if let key = objectKey, let account = RedisURLCredential.account(for: redisURL),
           account == objectKeyAccount {
            cfg.objectAccessKey = key.accessKey
            cfg.objectSecretKey = key.secretKey
        }
        // JuiceMount Link fields — assigned post-init (ServerConfig vars).
        // Existing-private-network mode intentionally leaves them blank: the
        // original Redis/object-store endpoints then follow macOS routing
        // without deleting the user's saved Link enrollment.
        if !useExistingPrivateNetwork {
            cfg.netControlURL = linkServerURL
            cfg.netAuthKey = linkAuthKey
            cfg.netHostname = linkHostname
        }
        return cfg
    }

    // MARK: - Persistence

    // Use the standard suite — UserDefaults rejects suiteName == bundle identifier.
    private static let defaults = AppIsolation.preferences

    /// Credential stores (the Keychain in the app; see RedisCredentialStore).
    /// A probe may substitute in-memory stores before load().
    static var credentialStore: any RedisCredentialStoring = RedisCredentialStore.standard(.redisPassword)
    static var linkCredentialStore: any RedisCredentialStoring = RedisCredentialStore.standard(.linkAuthKey)
    static var objectKeyStore: any RedisCredentialStoring = RedisCredentialStore.standard(.objectKey)
    static var enrollmentStore: any RedisCredentialStoring = RedisCredentialStore.standard(.enrollment)
    static var previousLoginStore: any RedisCredentialStoring = RedisCredentialStore.standard(.previousLogin)

    /// Keychain writes from Settings run here, off the main thread.
    static let credentialWrites = DispatchQueue(label: "com.juicemount.credential-writes")

    /// Moves a password embedded in `url` to the store. Returns the URL to save
    /// and what was moved. If the store refuses, the URL is returned unchanged
    /// (moved == nil) so the only copy of the password is not lost; the next
    /// load retries. A URL without a password is returned as is.
    static func extractEmbeddedPassword(_ url: String) -> (saved: String, moved: (password: String, account: String)?) {
        let (stripped, password) = RedisURLCredential.split(url)
        guard stripped != url else { return (url, nil) }
        guard let password else { return (stripped, nil) }
        guard let account = RedisURLCredential.account(for: stripped),
              credentialStore.setPassword(password, for: account) else { return (url, nil) }
        return (stripped, (password, account))
    }

    /// The URL handed to the engine: the saved URL plus the in-memory password
    /// when it belongs to this endpoint. Never reads the store.
    func engineRedisURL() -> String {
        // A password typed into the URL is re-spelled canonically so the
        // service and recovery see one string (credstore.CanonicalRedisURL).
        let (stripped, typed) = RedisURLCredential.split(redisURL)
        if let typed { return RedisURLCredential.withPassword(stripped, password: typed) }
        guard !redisPassword.isEmpty,
              let account = RedisURLCredential.account(for: redisURL),
              account == redisPasswordAccount
        else { return redisURL }
        return RedisURLCredential.withPassword(redisURL, password: redisPassword)
    }

    /// Non-nil when this Mac's identity was copied from another Mac
    /// (Migration Assistant, Time Machine, cloned disk): a credential stamped
    /// for another Mac was discarded this launch, or the service rotated the
    /// instance ID and left `instance-clone.json` beside the metadata DB
    /// (internal/cplane) until the Manager approves the new principal. The
    /// metadata cache is kept. Spool rows copied from the other Mac are held
    /// (never drained automatically) until the user uploads or discards them.
    public var deviceMigrationNotice: String? {
        let marker = ((dbPath as NSString).expandingTildeInPath as NSString)
            .deletingLastPathComponent + "/instance-clone.json"
        return DeviceBinding.cloneDetected || FileManager.default.fileExists(atPath: marker)
            ? DeviceBinding.cloneMessage : nil
    }

    public enum RedisPasswordStatus: Equatable {
        case invalidURL
        case missing(account: String)
        case saved(account: String)
    }

    /// What Settings shows next to the password field. Memory only.
    public var redisPasswordStatus: RedisPasswordStatus {
        guard let account = RedisURLCredential.account(for: redisURL) else { return .invalidURL }
        return account == redisPasswordAccount && !redisPassword.isEmpty
            ? .saved(account: account) : .missing(account: account)
    }

    /// Saves `password` for the current Redis endpoint. Memory is updated at
    /// once; the Keychain write runs on `credentialWrites` and `completion`
    /// reports it there (callers hop to the main thread themselves).
    public func setRedisPassword(_ password: String, completion: ((Bool) -> Void)? = nil) {
        guard let account = RedisURLCredential.account(for: RedisURLCredential.split(redisURL).url) else {
            completion?(false)
            return
        }
        redisPassword = password
        redisPasswordAccount = account
        let store = Self.credentialStore
        Self.credentialWrites.async {
            // Not `completion?(store.setPassword(...))`: optional chaining
            // skips evaluating the argument when completion is nil, so a call
            // without a completion never wrote the Keychain.
            let ok = store.setPassword(password, for: account)
            completion?(ok)
        }
    }

    private enum Key: String {
        case volumeName, mountPoint, redisURL, nfsListenAddr, metricsAddr, dbPath
        case ssdCacheGB, memoryBufferMB, memBufFileLimitMB, reconcileSeconds
        case startAtLogin, showSearchHotkey
        case offlineNotificationsEnabled
        case nativePreviewWarm
        case s3EndpointOverride
        case spoolEnabled, spoolCapacityGB
        case replacementRecovery
        case replacementRecoveryOverride
        case hasCompletedOnboarding
        case useExistingPrivateNetwork
        case openCacheTTL
        case managerURL
        /// Set when the user went back to the pre-enrollment login; the
        /// enrollment loop then does not switch again until Request access.
        case enrollmentAdoptionPaused
        /// The principal this Mac last switched its Redis login to. Set by
        /// the first switch; later approved polls never copy the current
        /// login to the previous-login slot, and never switch a login the
        /// user changed by hand afterwards (only Request access does).
        case enrollmentSwitchedPrincipal
    }

    public static func load() -> Preferences {
        let d = defaults
        // Upgrade migration (review P2-B): hasCompletedOnboarding didn't
        // exist before the onboarding build, and `d.bool` defaults to false —
        // which would turn EVERY existing, configured install into
        // "first-run" after upgrade (no auto-start; a window demanding a
        // click, brutal for headless/remote Macs). Evidence this machine ran
        // JuiceMount before = its metadata DB exists on disk. Run once: the
        // marker persists immediately.
        if d.object(forKey: Key.hasCompletedOnboarding.rawValue) == nil {
            let dbPath = ((d.string(forKey: Key.dbPath.rawValue) ?? defaultDBPath()) as NSString).expandingTildeInPath
            if FileManager.default.fileExists(atPath: dbPath) {
                d.set(true, forKey: Key.hasCompletedOnboarding.rawValue)
            }
        }
        // H-8: older builds saved the Redis password inside redisURL. Move it
        // to the Keychain and save the stripped URL (no-op once migrated).
        var storedRedisURL = d.string(forKey: Key.redisURL.rawValue)
        if let raw = storedRedisURL {
            let migrated = extractEmbeddedPassword(raw).saved
            if migrated != raw {
                d.set(migrated, forKey: Key.redisURL.rawValue)
                storedRedisURL = migrated
            }
        }
        let prefs = Preferences(
            volumeName:        d.string(forKey: Key.volumeName.rawValue) ?? "zpool",
            mountPoint:        d.string(forKey: Key.mountPoint.rawValue) ?? (AppIsolation.active?.child("volume") ?? "/Volumes/zpool"),
            redisURL:          storedRedisURL ?? (AppIsolation.active == nil ? "redis://127.0.0.1:6379/1" : ""),
            nfsListenAddr:     d.string(forKey: Key.nfsListenAddr.rawValue) ?? (AppIsolation.active == nil ? "127.0.0.1:11049" : "127.0.0.1:0"),
            metricsAddr:       d.string(forKey: Key.metricsAddr.rawValue) ?? (AppIsolation.active == nil ? "127.0.0.1:11050" : "127.0.0.1:0"),
            dbPath:            d.string(forKey: Key.dbPath.rawValue) ?? defaultDBPath(),
            ssdCacheGB:        d.object(forKey: Key.ssdCacheGB.rawValue) as? Int ?? 100,
            memoryBufferMB:    d.object(forKey: Key.memoryBufferMB.rawValue) as? Int ?? 2048,
            memBufFileLimitMB: d.object(forKey: Key.memBufFileLimitMB.rawValue) as? Int ?? 128,
            reconcileSeconds:  d.object(forKey: Key.reconcileSeconds.rawValue) as? Int ?? 30,
            startAtLogin:      d.bool(forKey: Key.startAtLogin.rawValue),
            showSearchHotkey:  d.object(forKey: Key.showSearchHotkey.rawValue) as? Bool ?? true,
            offlineNotificationsEnabled: d.bool(forKey: Key.offlineNotificationsEnabled.rawValue),
            nativePreviewWarm: d.bool(forKey: Key.nativePreviewWarm.rawValue),
            s3EndpointOverride: d.string(forKey: Key.s3EndpointOverride.rawValue) ?? "",
            spoolEnabled:       d.object(forKey: Key.spoolEnabled.rawValue) as? Bool ?? true,
            replacementRecovery: Self.savedReplacementRecovery(in: d),
            spoolCapacityGB:    d.object(forKey: Key.spoolCapacityGB.rawValue) as? Int ?? 0, // 0 = Auto
            hasCompletedOnboarding: d.bool(forKey: Key.hasCompletedOnboarding.rawValue),
            useExistingPrivateNetwork: d.object(forKey: Key.useExistingPrivateNetwork.rawValue) as? Bool ?? true,
            managerURL:         d.string(forKey: Key.managerURL.rawValue) ?? ""
        )
        prefs.loadCredentials(defaults: d)
        // Only the real app enrolls on its own (probes and tools load
        // preferences too, and must never contact a Manager).
        if Bundle.main.bundleIdentifier == "com.juicemount.app" {
            prefs.resumeEnrollmentIfNeeded()
        }
        return prefs
    }

    private static func savedReplacementRecovery(in d: any PreferenceValues) -> Bool {
        guard let value = d.object(forKey: Key.replacementRecoveryOverride.rawValue) else { return true }
        guard CFGetTypeID(value as CFTypeRef) == CFBooleanGetTypeID() else { return false }
        return (value as! NSNumber).boolValue
    }

    /// The one store read per launch (main thread at startup, never a render
    /// path). Also migrates a Link pairing key older builds kept in the plist.
    private func loadCredentials(defaults d: any PreferenceValues) {
        let (url, embedded) = RedisURLCredential.split(redisURL)
        if let embedded, let account = RedisURLCredential.account(for: url) {
            // The store refused at migration; keep using the URL's copy.
            (redisPassword, redisPasswordAccount) = (embedded, account)
        } else if let account = RedisURLCredential.account(for: url),
                  let stored = Self.credentialStore.password(for: account), !stored.isEmpty {
            (redisPassword, redisPasswordAccount) = (stored, account)
        }
        canRestorePreviousLogin = previousLogin != nil
        if let account = RedisURLCredential.account(for: url),
           let stored = Self.objectKeyStore.password(for: account),
           let key = ObjectKeyCredential.decode(stored) {
            (objectKey, objectKeyAccount) = (key, account)
        }
        let linkAccount = RedisURLCredential.linkAccount(for: linkServerURL)
        if let legacy = d.string(forKey: "link_auth_key"), !legacy.isEmpty {
            linkAuthKeyValue = legacy
            if Self.linkCredentialStore.setPassword(legacy, for: linkAccount) {
                d.set(nil, forKey: "link_auth_key")
            }
        } else {
            linkAuthKeyValue = Self.linkCredentialStore.password(for: linkAccount) ?? ""
        }
    }

    public func save() {
        let d = Self.defaults
        d.set(volumeName, forKey: Key.volumeName.rawValue)
        d.set(mountPoint, forKey: Key.mountPoint.rawValue)
        // A typed URL with a password stays as typed in memory (rewriting the
        // field mid-edit would fight the user); only its stripped form is
        // saved. Re-store only when the password or endpoint actually changed.
        let (stripped, typed) = RedisURLCredential.split(redisURL)
        if let typed, typed == redisPassword,
           RedisURLCredential.account(for: stripped) == redisPasswordAccount {
            d.set(stripped, forKey: Key.redisURL.rawValue)
        } else {
            let (savedURL, moved) = Self.extractEmbeddedPassword(redisURL)
            if let moved {
                redisPassword = moved.password
                redisPasswordAccount = moved.account
            }
            d.set(savedURL, forKey: Key.redisURL.rawValue)
        }
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
        d.set(nativePreviewWarm, forKey: Key.nativePreviewWarm.rawValue)
        d.set(s3EndpointOverride, forKey: Key.s3EndpointOverride.rawValue)
        d.set(spoolEnabled, forKey: Key.spoolEnabled.rawValue)
        d.set(spoolCapacityGB, forKey: Key.spoolCapacityGB.rawValue)
        d.set(hasCompletedOnboarding, forKey: Key.hasCompletedOnboarding.rawValue)
        d.set(useExistingPrivateNetwork, forKey: Key.useExistingPrivateNetwork.rawValue)
        d.set(managerURL, forKey: Key.managerURL.rawValue)
    }
}

// MARK: - Manager enrollment (H-8B)

/// What Settings shows about this Mac's access to the server.
public struct EnrollmentStatus: Equatable {
    public enum Phase: String, Equatable {
        case notEnrolled = "not_enrolled"
        case requesting
        case waiting
        /// Approved by the Manager, but the server does not accept the new
        /// login yet (no proxy cutover). The Mac keeps its working login.
        case approvedPendingCutover = "approved_pending_cutover"
        /// Approved and in use, but the server refused (or could not be
        /// asked about) this Mac's login on the last check.
        case approvedLoginRefused = "approved_login_refused"
        /// Approved, but this Mac does not use the principal's login now:
        /// the user went back to the previous login, or changed the login
        /// in Settings by hand. Request access switches to it.
        case approvedNotInUse = "approved_not_in_use"
        case approved
        case revoked
        case conflict
        case failed
    }

    public var phase: Phase = .notEnrolled
    /// This Mac's principal, `mac-<12 hex>` (once the Manager answered).
    public var principal: String = ""
    /// What this Mac knows about its object-store ("storage") key (H-8
    /// per-Mac keys). Shown under the access line when not `.normal`.
    public var storageKey: StorageKeyState = .normal
    /// The access-key ID the running volume uses ("" = not running, or not
    /// known). Never a secret.
    public var storageKeyInUse: String = ""
    /// Whether new writes go to JuiceMount's spool on this Mac (Settings);
    /// decides what a refusal means for new writes.
    public var spoolEnabled: Bool = true
    /// The code the admin matches on the Manager's Principals page.
    public var fingerprint: String = ""
    /// Last error or advice, never a secret.
    public var message: String = ""
    /// When the next automatic check runs (nil = none scheduled).
    public var nextCheck: Date?

    public enum StorageKeyState: Equatable {
        case normal
        /// Approved, but the Manager is still creating this Mac's storage key.
        case pending
        /// A new storage key is saved; the running volume still uses the old one.
        case restartNeeded
        /// The storage server refused the key the running volume uses.
        /// `replaced`: a newer key is already saved (a restart uses it).
        case refused(code: String, replaced: Bool)
    }

    /// What a refusal means for writes. Traced (review keys-l3 F2):
    /// - Only a file created on this Mac goes to the spool (nfs/handler.go:
    ///   a path with no spool entry stays on the direct juicefs path). A
    ///   failed upload keeps the spooled file (nfs/drainer.go
    ///   failTransient/failPermanent); after 5 failed attempts it is marked
    ///   failed but kept, and boot recovery queues it again when the volume
    ///   restarts (nfs/spool.go RecoverOnBoot).
    /// - A write into a file that already exists goes straight to juicefs,
    ///   which retries the refused upload and then fails it: that change is
    ///   not kept on this Mac.
    /// - A new file saved over an existing name is drained by opening the
    ///   existing file with O_TRUNC (nfs/drainer_publish.go), so other Macs
    ///   can see it empty or incomplete until the upload succeeds.
    private var refusedWritesText: String {
        spoolEnabled
            ? "new files stay on this Mac. They upload after access is restored and the volume restarts (Stop everything → Start). Changes to files that already exist fail while access is refused. A new file saved over an existing one can look empty or incomplete on other Macs until it uploads."
            : "writes to the volume fail until access is restored (the spool is off, so nothing is kept on this Mac)."
    }

    /// The storage-key line under the access line (nil = nothing to say).
    public var storageKeyNote: String? {
        switch storageKey {
        case .normal:
            return nil
        case .pending:
            return "The Manager is still creating this Mac's storage key. This Mac keeps the key it has and checks again every 15 seconds."
        case .restartNeeded:
            return "A new storage key was saved. The volume still uses the previous one; the new storage key will be used after restart (Stop everything → Start)."
        case let .refused(code, replaced):
            let why = code.isEmpty ? "" : " (\(code))"
            if replaced {
                return "This Mac's storage access was refused\(why) — the volume still uses its previous storage key, and a new one is saved. Restart the volume (Stop everything → Start) to use it. Until then \(refusedWritesText)"
            }
            if phase == .revoked {
                return "This Mac's storage access was refused\(why) — \(refusedWritesText)"
            }
            let who = principal.isEmpty ? "this Mac" : principal
            return "This Mac's storage access was refused\(why) — \(refusedWritesText) JuiceMount asked the Manager again. If this continues, ask an admin to check \(who) on the Manager's Principals page."
        }
    }

    /// One-line status for Settings.
    public var summary: String {
        switch phase {
        case .notEnrolled:
            return "Not enrolled. This Mac has no access of its own to this server yet."
        case .requesting:
            return "Requesting access…"
        case .waiting:
            let code = fingerprint.isEmpty ? "" : " Code shown there: \(fingerprint)."
            return "Waiting for approval. Ask an admin to approve \(principal) on the Manager's Principals page.\(code)"
        case .approvedPendingCutover:
            let why = message.isEmpty ? "" : " (\(message))"
            return "Approved — waiting for the server cutover. This Mac keeps its current login until the server accepts \(principal)\(why). It checks again automatically."
        case .approvedLoginRefused:
            let why = message.isEmpty ? "" : " (\(message))"
            return "Approved as \(principal), but the server did not accept this Mac's login on the last check\(why). JuiceMount checks again automatically. If this continues, ask an admin to check \(principal) on the Manager's Principals page."
        case .approvedNotInUse:
            let why = message.isEmpty ? "" : ": \(message)"
            return "Approved as \(principal), but this Mac does not use that login now\(why). Click Request access to use \(principal)."
        case .approved:
            return "Approved as \(principal). The password and object key are saved in the Keychain."
        case .revoked:
            return "Revoked. An admin removed this Mac's access (\(principal)). To ask again, the admin must forget it in the Manager first."
        case .conflict:
            return "The Manager has a different request on record for this Mac (\(principal)). An admin must revoke and forget it in the Manager, then click Request access."
        case .failed:
            return message.isEmpty ? "The Manager could not be reached. Retrying." : "\(message.prefix(1).uppercased())\(message.dropFirst())."
        }
    }
}

/// Output of NFSServerEnrollStep (bridge/enrollment.go). Holds secrets.
struct EnrollStepOutput: Decodable {
    var state: EnrollmentRecord
    var persist: Bool
    var phase: String
    var objectAccessKey: String?
    var objectSecretKey: String?
    var retryAfterMS: Int64
    var message: String?
    var cloneNotice: String?
    /// The configured Redis endpoint accepted AUTH <principal> <secret> on
    /// this approved step (bridge verifyPrincipalLogin). Only then may the
    /// app switch this Mac's login.
    var credentialsLive: Bool?
    /// Why the login check failed (fixed text, never a secret).
    var credentialCheck: String?
    /// Approved, but the Manager is still creating this Mac's storage key:
    /// no key is delivered, and the next step comes in 15 seconds.
    var objectKeyPending: Bool?
    /// The access-key ID the running volume uses (nil = not known).
    var objectKeyInUse: String?
    /// The storage server refused that key (juicefs log, bridge).
    var objectKeyRefused: Bool?
    var refusedCode: String?

    enum CodingKeys: String, CodingKey {
        case state, persist, phase, message
        case objectAccessKey = "object_access_key", objectSecretKey = "object_secret_key"
        case retryAfterMS = "retry_after_ms", cloneNotice = "clone_notice"
        case credentialsLive = "credentials_live", credentialCheck = "credential_check"
        case objectKeyPending = "object_key_pending", objectKeyInUse = "object_key_in_use"
        case objectKeyRefused = "object_key_refused", refusedCode = "refused_code"
    }
}

/// Output of NFSServerEnrollStep with op "object-key-status" (bridge
/// objectKeyStatusOutput). Holds no secret.
struct ObjectKeyStatusOutput: Decodable {
    /// A serving process answered (false: every other field is empty).
    var known: Bool
    var objectKeyInUse: String?
    var objectKeyRefused: Bool?
    var refusedCode: String?
    /// Run an enrollment step now (a new refusal; the bridge rate-limits).
    var pollNow: Bool?

    enum CodingKeys: String, CodingKey {
        case known
        case objectKeyInUse = "object_key_in_use", objectKeyRefused = "object_key_refused"
        case refusedCode = "refused_code", pollNow = "poll_now"
    }
}

extension Preferences {
    /// The Manager's default published port (server/docker-compose.yml).
    static let defaultManagerPort = 30190

    /// Where enrollment requests go: the configured Manager URL, else the
    /// Redis host on port 30190. nil when neither gives a usable address.
    public var effectiveManagerURL: String? {
        let typed = managerURL.trimmingCharacters(in: .whitespacesAndNewlines)
        if !typed.isEmpty { return typed }
        guard let c = URLComponents(string: RedisURLCredential.split(redisURL).url),
              var host = c.host, !host.isEmpty else { return nil }
        if host.contains(":"), !host.hasPrefix("[") { host = "[\(host)]" }
        return "http://\(host):\(Self.defaultManagerPort)"
    }

    /// True when the effective Manager address was derived from a loopback
    /// Redis URL (a developer setup): nothing is sent there automatically.
    private var managerIsDerivedLoopback: Bool {
        guard managerURL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let host = URLComponents(string: RedisURLCredential.split(redisURL).url)?.host?.lowercased()
        else { return false }
        return host == "localhost" || host == "::1" || host.hasPrefix("127.")
    }

    private static let enrollmentQueue = DispatchQueue(label: "com.juicemount.enrollment")
    /// Object-key status checks run here, never on `enrollmentQueue`: a slow
    /// serving process must not delay an enrollment step.
    private static let objectKeyQueue = DispatchQueue(label: "com.juicemount.object-key-status")

    /// How often the app asks whether the storage server refused this Mac's
    /// key while it is approved or revoked. The check reads the serving
    /// process's state only; it contacts no Manager.
    static var objectKeyWatchInterval: TimeInterval = 15

    /// The bridge's NFSServerEnrollStep (input and output JSON). A probe
    /// substitutes it; the real one is linked only into the app.
    static var enrollStepCall: (String) -> String? = { json in
        var input = Array(json.utf8CString)
        return input.withUnsafeMutableBufferPointer { buf in
            guard let ptr = buf.baseAddress, let result = NFSServerEnrollStep(ptr) else { return nil }
            defer { NFSServerFreeString(result) }
            return String(cString: result)
        }
    }
    private static let generationLock = NSLock()
    private static var generationValue = 0

    /// Bumped by every (re)start; a step from an older generation stops.
    private static var enrollmentGeneration: Int {
        generationLock.lock(); defer { generationLock.unlock() }
        return generationValue
    }

    private static func nextEnrollmentGeneration() -> Int {
        generationLock.lock(); defer { generationLock.unlock() }
        generationValue += 1
        return generationValue
    }

    /// At launch: continue an enrollment already under way, or start one
    /// when this Mac has no Redis password for the configured server (the
    /// 0.5 server refuses connections without one).
    func resumeEnrollmentIfNeeded() {
        guard effectiveManagerURL != nil else { return }
        startObjectKeyWatch()
        let record = EnrollmentRecord.load(Self.enrollmentStore.password(for: EnrollmentRecord.account))
        if let record {
            enrollment.principal = record.principal
            enrollment.fingerprint = record.fingerprint
            switch record.status {
            case "pending": enrollment.phase = .waiting
            case "approved":
                enrollment.phase = hasAdopted(principal: record.principal, secret: record.secret) ? .approved : .approvedPendingCutover
            default: enrollment.phase = EnrollmentStatus.Phase(rawValue: record.status) ?? .notEnrolled
            }
            startEnrollment()
            return
        }
        if case .missing = redisPasswordStatus, !managerIsDerivedLoopback {
            startEnrollment()
        }
    }

    /// Settings' "Request access": (re)start enrollment now. It also ends a
    /// pause set by `restorePreviousLogin`, so an approved Mac switches to its
    /// principal again once the server accepts it.
    public func requestAccess() {
        guard effectiveManagerURL != nil else {
            enrollment = EnrollmentStatus(phase: .failed, message: "enter the Manager address first")
            return
        }
        if enrollmentAdoptionPaused {
            enrollmentAdoptionPaused = false
            enrollment.message = ""
        }
        explicitSwitchRequested = true
        startObjectKeyWatch()
        startEnrollment()
    }

    /// The user went back to the pre-enrollment login; persisted.
    var enrollmentAdoptionPaused: Bool {
        get { Self.defaults.bool(forKey: Key.enrollmentAdoptionPaused.rawValue) }
        set { Self.defaults.set(newValue, forKey: Key.enrollmentAdoptionPaused.rawValue) }
    }

    /// The principal this Mac last switched its login to ("" = it never
    /// switched); persisted.
    var enrollmentSwitchedPrincipal: String {
        get { Self.defaults.string(forKey: Key.enrollmentSwitchedPrincipal.rawValue) ?? "" }
        set { Self.defaults.set(newValue, forKey: Key.enrollmentSwitchedPrincipal.rawValue) }
    }

    /// The Redis URL's user when the user changed the login by hand after
    /// this Mac switched to a principal (nil otherwise). A login that is one
    /// of this Mac's principals is not a hand edit: a clone rotation moves
    /// the Mac from its old principal to the new one. A URL with no user name
    /// logs in as Redis's "default" user, so an edit back to it is a hand
    /// edit too (review 3 finding 5): switching it back would overwrite the
    /// password typed for it.
    private func handEditedLoginUser(principal: String) -> String? {
        let switched = enrollmentSwitchedPrincipal
        guard !switched.isEmpty,
              let components = URLComponents(string: RedisURLCredential.split(redisURL).url) else { return nil }
        let user = (components.user ?? "").isEmpty ? "default" : components.user!
        guard user != principal, user != switched else { return nil }
        return user
    }

    /// Starts (or restarts) the enrollment loop on its own queue. Each step
    /// makes at most one request; the Go client paces the next one.
    private func startEnrollment() {
        guard let manager = effectiveManagerURL else { return }
        let generation = Self.nextEnrollmentGeneration()
        if enrollment.phase == .notEnrolled || enrollment.phase == .failed {
            enrollment.phase = .requesting
            enrollment.message = ""
        }
        let dbPath = (self.dbPath as NSString).expandingTildeInPath
        let redis = RedisURLCredential.split(redisURL).url
        Self.enrollmentQueue.async { [weak self] in
            let record = EnrollmentRecord.load(Self.enrollmentStore.password(for: EnrollmentRecord.account)) ?? EnrollmentRecord()
            self?.enrollmentStep(generation: generation, manager: manager, dbPath: dbPath, redisURL: redis, record: record)
        }
    }

    /// One step, on `enrollmentQueue`. `redisURL` is the configured endpoint
    /// without its password: on an approved step the bridge logs in there as
    /// the principal before the app may switch to it.
    private func enrollmentStep(generation: Int, manager: String, dbPath: String, redisURL: String, record: EnrollmentRecord) {
        let label = Host.current().localizedName ?? ""
        let host = ProcessInfo.processInfo.hostName
        struct Input: Encodable {
            let manager_url: String, db_path: String, host: String, label: String
            let state: EnrollmentRecord
            let redis_url: String
        }
        guard let data = try? JSONEncoder().encode(Input(manager_url: manager, db_path: dbPath, host: host,
                                                         label: label, state: record, redis_url: redisURL)),
              let json = String(data: data, encoding: .utf8) else { return }
        let raw = Self.enrollStepCall(json)
        guard let raw, let out = try? JSONDecoder().decode(EnrollStepOutput.self, from: Data(raw.utf8)) else {
            DispatchQueue.main.async { [weak self] in
                guard let self, generation == Self.enrollmentGeneration else { return }
                self.enrollment.phase = .failed
                self.enrollment.message = "the enrollment step returned no result"
            }
            return
        }
        // The secret must be saved before it is ever sent: a Manager that
        // holds a secret this Mac lost leaves the request in conflict until an
        // admin forgets it. So a step that must persist and cannot is the end.
        var next = record
        if out.persist {
            guard let stored = out.state.stored, Self.enrollmentStore.setPassword(stored, for: EnrollmentRecord.account) else {
                DispatchQueue.main.async { [weak self] in
                    guard let self, generation == Self.enrollmentGeneration else { return }
                    self.enrollment.phase = .failed
                    self.enrollment.message = "the Keychain did not accept the enrollment request; nothing was sent"
                }
                return
            }
            next = out.state
        } else if out.state.secret == record.secret {
            next = out.state // pacing only (failure count)
        }
        let delay = max(Double(out.retryAfterMS) / 1000, 0)
        DispatchQueue.main.async { [weak self] in
            guard let self, generation == Self.enrollmentGeneration else { return }
            self.applyEnrollmentStep(out, record: next)
            self.enrollment.nextCheck = Date().addingTimeInterval(delay)
            let redis = RedisURLCredential.split(self.redisURL).url
            Self.enrollmentQueue.asyncAfter(deadline: .now() + delay) { [weak self] in
                guard let self, Self.enrollmentGeneration == generation else { return }
                self.enrollmentStep(generation: generation, manager: manager, dbPath: dbPath, redisURL: redis, record: next)
            }
        }
    }

    /// Main thread: show the outcome and store what an approval delivered.
    func applyEnrollmentStep(_ out: EnrollStepOutput, record: EnrollmentRecord) {
        enrollment.principal = record.principal
        enrollment.fingerprint = record.fingerprint
        enrollment.message = out.message ?? ""
        defer {
            // After the switch below: a revoke forgets the stored key, an
            // approval may have replaced it.
            storageKeyPending = out.phase == "approved" && out.objectKeyPending == true
            if let inUse = out.objectKeyInUse {
                updateStorageKey(inUse: inUse, refused: out.objectKeyRefused == true, code: out.refusedCode ?? "")
            } else {
                updateStorageKey(inUse: enrollment.storageKeyInUse, refused: storageKeyRefusedCode != nil,
                                 code: storageKeyRefusedCode ?? "")
            }
        }
        switch out.phase {
        case "approved":
            let adopted = hasAdopted(principal: record.principal, secret: record.secret)
            let live = out.credentialsLive == true
            if adopted {
                explicitSwitchRequested = false
                // Already this Mac's login: refresh the object key, and say
                // so when the server refuses the login (review 2 finding F5a).
                adoptApprovedCredentials(principal: record.principal, secret: record.secret,
                                         accessKey: out.objectAccessKey ?? "", secretKey: out.objectSecretKey ?? "")
                if live {
                    enrollment.phase = .approved
                } else {
                    enrollment.phase = .approvedLoginRefused
                    enrollment.message = out.credentialCheck ?? "the login was not checked"
                }
            } else if enrollmentAdoptionPaused {
                // The user went back to the previous login on purpose.
                enrollment.phase = .approvedNotInUse
                enrollment.message = "you switched back to the previous login"
            } else if !live {
                // Only a login the server accepted right now replaces the
                // working one (review 2026-10-06 finding 1a).
                enrollment.phase = .approvedPendingCutover
                enrollment.message = out.credentialCheck ?? ""
            } else if let user = handEditedLoginUser(principal: record.principal), !explicitSwitchRequested {
                // Changed by hand after the switch: never switched back
                // silently (review 2 finding F5b).
                enrollment.phase = .approvedNotInUse
                enrollment.message = "the Redis login in Settings was changed to \"\(user)\""
            } else {
                explicitSwitchRequested = false
                enrollment.phase = .approved
                adoptApprovedCredentials(principal: record.principal, secret: record.secret,
                                         accessKey: out.objectAccessKey ?? "", secretKey: out.objectSecretKey ?? "")
            }
        case "waiting": enrollment.phase = .waiting
        case "revoked":
            enrollment.phase = .revoked
            forgetObjectKey()
            forgetPreviousLogin()
            // The switch belonged to the revoked approval. A later approval
            // is a first switch again: it keeps the working login and offers
            // "Use previous login" (review 3 finding 8).
            enrollmentSwitchedPrincipal = ""
        case "conflict": enrollment.phase = .conflict
        case "not_enrolled": enrollment.phase = .requesting
        default: enrollment.phase = .failed
        }
    }

    /// True when this Mac already logs in as `principal` with `secret`.
    func hasAdopted(principal: String, secret: String) -> Bool {
        guard !principal.isEmpty, !secret.isEmpty,
              let c = URLComponents(string: RedisURLCredential.split(redisURL).url), c.user == principal,
              let account = RedisURLCredential.account(for: redisURL) else { return false }
        return redisPasswordAccount == account && redisPassword == secret
    }

    /// The login saved before an approval switched this Mac (nil = none).
    var previousLogin: PreviousLogin? {
        PreviousLogin.load(Self.previousLoginStore.password(for: PreviousLogin.account))
    }

    /// Approval, once the server accepted the principal's login: this Mac
    /// logs in as `principal` with its enrollment secret, and gives juicefs
    /// the object key. The URL keeps no password; both secrets go to the
    /// Keychain, bound to the principal's Redis account. Applies at the next
    /// start (Stop everything → Start for a running volume).
    ///
    /// The metadata slot holds ONE item, so overwriting it would destroy the
    /// pre-cutover password. The current login is first copied to the
    /// `.previousLogin` slot (`restorePreviousLogin` undoes the switch); if
    /// that copy cannot be saved, nothing is switched.
    func adoptApprovedCredentials(principal: String, secret: String, accessKey: String, secretKey: String) {
        guard !principal.isEmpty, !secret.isEmpty,
              var c = URLComponents(string: RedisURLCredential.split(redisURL).url) else { return }
        if c.user != principal {
            let oldURL = RedisURLCredential.split(redisURL).url
            let oldAccount = RedisURLCredential.account(for: oldURL)
            // Only the FIRST switch keeps the working login, and never a
            // login that is one of this Mac's principals: a later switch
            // (Request access after a hand edit) would otherwise overwrite the
            // pre-enrollment login with, say, the admin password (review 2
            // finding F5b).
            let firstSwitch = enrollmentSwitchedPrincipal.isEmpty
            let isThisMacsPrincipal = (c.user ?? "").hasPrefix("mac-") &&
                (c.user == principal || c.user == enrollmentSwitchedPrincipal)
            if firstSwitch, !isThisMacsPrincipal, let oldAccount, oldAccount == redisPasswordAccount, !redisPassword.isEmpty {
                // Keep the working login before anything is replaced. This
                // one write is synchronous (on the main thread, once per
                // switch): it must be stored before the switch happens.
                let keep = PreviousLogin(redisURL: oldURL, password: redisPassword)
                guard let stored = keep.stored,
                      Self.previousLoginStore.setPassword(stored, for: PreviousLogin.account) else {
                    enrollment.phase = .failed
                    enrollment.message = "the Keychain did not accept a copy of the current login, so this Mac keeps it and did not switch to \(principal)"
                    return
                }
                canRestorePreviousLogin = true
            }
            c.percentEncodedUser = RedisURLCredential.escape(principal)
            c.percentEncodedPassword = nil
            if let url = c.string { redisURL = url }
            enrollmentSwitchedPrincipal = principal
        }
        guard let account = RedisURLCredential.account(for: redisURL) else { return }
        if !(redisPasswordAccount == account && redisPassword == secret) {
            setRedisPassword(secret) { ok in
                if !ok { NSLog("JuiceMount: could not save the approved Redis password to the Keychain") }
            }
        }
        guard let stored = ObjectKeyCredential.encode(accessKey: accessKey, secretKey: secretKey) else { return }
        if objectKeyAccount == account, objectKey?.accessKey == accessKey, objectKey?.secretKey == secretKey { return }
        objectKey = (accessKey, secretKey)
        objectKeyAccount = account
        let store = Self.objectKeyStore
        Self.credentialWrites.async {
            if !store.setPassword(stored, for: account) {
                NSLog("JuiceMount: could not save the object-store key to the Keychain")
            }
        }
    }

    /// Undo an approval's switch: log in again with the saved pre-enrollment
    /// URL and password, and stop switching automatically until Request
    /// access. The principal's password stays in the enrollment record, so
    /// Request access can switch back without a new approval.
    @discardableResult
    public func restorePreviousLogin() -> Bool {
        guard let previous = previousLogin else { return false }
        enrollmentAdoptionPaused = true
        redisURL = previous.redisURL
        // The previous login is the current one now: nothing is left to go
        // back to, and the slot must not keep a second copy of the password
        // (review 2 finding F5c). It is cleared once the restored password is
        // saved; if that save fails the slot stays and the button returns.
        canRestorePreviousLogin = false
        let previousStore = Self.previousLoginStore
        setRedisPassword(previous.password) { [weak self] ok in
            if ok {
                _ = previousStore.setPassword("", for: PreviousLogin.account)
            } else {
                NSLog("JuiceMount: could not save the restored Redis password to the Keychain")
                DispatchQueue.main.async { self?.canRestorePreviousLogin = true }
            }
        }
        if enrollment.phase == .approved || enrollment.phase == .approvedLoginRefused {
            enrollment.phase = .approvedNotInUse
            enrollment.message = "you switched back to the previous login"
        }
        return true
    }

    /// Revoked: the saved pre-enrollment login is dropped too (review 2
    /// finding F5c). Its password must not outlive this Mac's access.
    private func forgetPreviousLogin() {
        canRestorePreviousLogin = false
        let store = Self.previousLoginStore
        Self.credentialWrites.async { _ = store.setPassword("", for: PreviousLogin.account) }
    }

    /// Revoked: stop handing the object key to juicefs.
    private func forgetObjectKey() {
        guard let account = objectKeyAccount else { return }
        objectKey = nil
        objectKeyAccount = nil
        let store = Self.objectKeyStore
        Self.credentialWrites.async { _ = store.setPassword("", for: account) }
    }

    /// True when an object key is held for the current Redis endpoint.
    public var hasObjectKey: Bool {
        objectKey != nil && objectKeyAccount == RedisURLCredential.account(for: redisURL)
    }

    /// The access-key ID of the key the next start hands juicefs ("" = none).
    var storedObjectAccessKey: String {
        hasObjectKey ? (objectKey?.accessKey ?? "") : ""
    }

    // MARK: Storage key status (H-8 per-Mac keys, design §5 "Mac side")

    /// Main thread: recompute `enrollment.storageKey` from what the running
    /// volume uses, whether the storage server refused it, and the key now
    /// stored for the next start.
    func updateStorageKey(inUse: String, refused: Bool, code: String) {
        enrollment.storageKeyInUse = inUse
        enrollment.spoolEnabled = spoolEnabled
        storageKeyRefusedCode = refused ? code : nil
        let stored = storedObjectAccessKey
        let replaced = !stored.isEmpty && !inUse.isEmpty && stored != inUse
        if refused {
            enrollment.storageKey = .refused(code: code, replaced: replaced)
        } else if storageKeyPending {
            enrollment.storageKey = .pending
        } else if replaced {
            enrollment.storageKey = .restartNeeded
        } else {
            enrollment.storageKey = .normal
        }
    }

    /// Phases in which a refused storage key means something.
    private var storageKeyMatters: Bool {
        switch enrollment.phase {
        case .approved, .approvedLoginRefused, .approvedPendingCutover, .approvedNotInUse, .revoked: return true
        default: return false
        }
    }

    /// Main thread: apply one object-key status answer. A new refusal
    /// (`pollNow`, rate-limited by the bridge) runs an enrollment step now
    /// instead of at the 30-minute approved poll, so a revoke or a replaced
    /// key shows within seconds.
    func applyObjectKeyStatus(_ out: ObjectKeyStatusOutput) {
        guard out.known else { return }
        updateStorageKey(inUse: out.objectKeyInUse ?? "", refused: out.objectKeyRefused == true, code: out.refusedCode ?? "")
        if out.pollNow == true, storageKeyMatters, effectiveManagerURL != nil {
            startEnrollment()
        }
    }

    /// Asks the bridge for the object-key status once (off the main thread)
    /// and applies it on the main thread.
    func checkObjectKeyStatus(then done: (() -> Void)? = nil) {
        Self.objectKeyQueue.async { [weak self] in
            let raw = Self.enrollStepCall(#"{"op":"object-key-status"}"#)
            let out = raw.flatMap { try? JSONDecoder().decode(ObjectKeyStatusOutput.self, from: Data($0.utf8)) }
            DispatchQueue.main.async {
                if let self, let out { self.applyObjectKeyStatus(out) }
                done?()
            }
        }
    }

    /// Starts the periodic object-key check (once per app run). Each check
    /// runs only while a refusal would mean something (approved or revoked).
    func startObjectKeyWatch() {
        guard !objectKeyWatchStarted else { return }
        objectKeyWatchStarted = true
        scheduleObjectKeyCheck()
    }

    private func scheduleObjectKeyCheck() {
        DispatchQueue.main.asyncAfter(deadline: .now() + Self.objectKeyWatchInterval) { [weak self] in
            guard let self else { return }
            guard self.storageKeyMatters else { self.scheduleObjectKeyCheck(); return }
            self.checkObjectKeyStatus { [weak self] in self?.scheduleObjectKeyCheck() }
        }
    }
}
