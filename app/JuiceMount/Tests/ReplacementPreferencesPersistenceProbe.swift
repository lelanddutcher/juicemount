import Foundation
import Darwin

// Build as a standalone executable in an owned Probe.app bundle. The runner
// gives each invocation the same unique bundle identifier and a sandbox that
// permits writes only inside root. This probe never starts the app or service.
enum RecoveryPreferencesProbeError: Error {
    case failed(String)
}

final class RecoveryMemoryCredentials: RedisCredentialStoring {
    var writes = 0
    func password(for account: String) -> String? { nil }
    func setPassword(_ password: String, for account: String) -> Bool {
        writes += 1
        return false
    }
}

// Core/PermissionProbes.swift needs this UI type when the Core sources are
// compiled without the app target. It is never called by this probe.
enum OnboardingPreflight {
    enum ParseResult {
        case success((String, UInt16))
        case failure(String)
    }
    static func parseRedisHostPort(_ raw: String) -> ParseResult { .failure("probe stub") }
}

@main
struct ReplacementPreferencesPersistenceProbe {
    static let markerKey = "jm_recovery_probe_owner"
    static let recoveryKey = "replacementRecoveryOverride"
    static let legacyKey = "replacementRecovery"

    static func require(_ condition: @autoclosure () -> Bool, _ message: String) throws {
        if !condition() { throw RecoveryPreferencesProbeError.failed(message) }
    }

    static func permissionDenied(_ error: Error) -> Bool {
        let e = error as NSError
        if e.domain == NSCocoaErrorDomain && e.code == NSFileWriteNoPermissionError { return true }
        if e.domain == NSPOSIXErrorDomain && (e.code == Int(EACCES) || e.code == Int(EPERM)) { return true }
        if let underlying = e.userInfo[NSUnderlyingErrorKey] as? Error {
            return permissionDenied(underlying)
        }
        return false
    }

    // Keep guard/receipt writes within the declared root. Foundation atomic
    // replacement can require additional filesystem operations outside it.
    static func exclusiveWrite(_ data: Data, path: String) throws {
        let fd = Darwin.open(path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600)
        if fd < 0 { throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno)) }
        var closed = false
        defer { if !closed { _ = Darwin.close(fd) } }
        try data.withUnsafeBytes { bytes in
            var offset = 0
            while offset < bytes.count {
                let count = Darwin.write(fd, bytes.baseAddress!.advanced(by: offset), bytes.count - offset)
                if count < 0 && errno == EINTR { continue }
                if count <= 0 {
                    throw NSError(domain: NSPOSIXErrorDomain, code: Int(count < 0 ? errno : EIO))
                }
                offset += count
            }
        }
        if fsync(fd) != 0 { throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno)) }
        let result = Darwin.close(fd)
        closed = true
        if result != 0 { throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno)) }
    }

    static func privateJSON(_ value: NFSBridge.ServerConfig, root: String, name: String) throws {
        let path = root + "/" + name
        try require(!FileManager.default.fileExists(atPath: path), "config receipt already exists")
        let data = try JSONEncoder().encode(value)
        try exclusiveWrite(data, path: path)
        let observed = try Data(contentsOf: URL(fileURLWithPath: path))
        try require(observed == data, "config receipt changed during write")
    }

    static func config(_ preferences: Preferences, root: String, name: String, expected: Bool) throws {
        let value = preferences.toServerConfig()
        try require(value.replacementRecovery == expected, "actual ServerConfig recovery flag changed")
        let json = try JSONEncoder().encode(value)
        let object = try JSONSerialization.jsonObject(with: json) as? [String: Any]
        try require(object?["replacement_recovery"] as? Bool == expected,
                    "actual ServerConfig JSON omitted the explicit Boolean")
        try require(value.dbPath == root + "/state/metadata.db" &&
                    value.mountPoint == root + "/volume" && value.redisURL.isEmpty &&
                    value.listenAddr == "127.0.0.1:0" && value.metricsAddr == "127.0.0.1:0",
                    "actual ServerConfig lost fixture-safe settings")
        let decoded = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: json)
        try require(decoded == value && decoded.replacementRecovery == expected,
                    "actual ServerConfig JSON round trip changed")
        try privateJSON(value, root: root, name: name)
    }

    static func domain(_ defaults: UserDefaults, name: String, owner: String) throws -> [String: Any] {
        let values = defaults.persistentDomain(forName: name) ?? [:]
        try require(values[markerKey] as? String == owner, "persistent domain owner marker missing")
        return values
    }

    static func installMemoryCredentials() -> [RecoveryMemoryCredentials] {
        let stores = (0..<5).map { _ in RecoveryMemoryCredentials() }
        Preferences.credentialStore = stores[0]
        Preferences.linkCredentialStore = stores[1]
        Preferences.objectKeyStore = stores[2]
        Preferences.enrollmentStore = stores[3]
        Preferences.previousLoginStore = stores[4]
        DeviceFingerprint.platformUUID = { "AAAAAAAA-0000-4000-8000-00000000000A" }
        return stores
    }

    static func run() throws {
        let args = Array(CommandLine.arguments.dropFirst())
        try require(args.count == 4, "expected root domain phase denied-sentinel-path")
        let root = args[0], domainName = args[1], phase = args[2], denied = args[3]
        let allowed = ["namespace-seed", "namespace-read", "default-enable", "reload-disable", "reload-off", "cleanup"]
        try require(allowed.contains(phase), "unknown phase")
        let prefix = "com.juicemount.probe.recovery."
        let suffix = String(domainName.dropFirst(prefix.count))
        try require(domainName.hasPrefix(prefix) && suffix.count == 32 &&
                    suffix.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) },
                    "bundle identifier is not an owned unique recovery domain")
        try require(Bundle.main.bundleIdentifier == domainName,
                    "binary did not launch inside the requested bundle domain")
        try require(root.hasPrefix("/") && root != "/" &&
                    URL(fileURLWithPath: root).standardizedFileURL.path == root &&
                    URL(fileURLWithPath: root).resolvingSymlinksInPath().path == root,
                    "root is not a canonical private path")
        try require(Bundle.main.bundleURL.standardizedFileURL.path == root + "/Probe.app",
                    "binary is not in the owned Probe.app")
        let attributes = try FileManager.default.attributesOfItem(atPath: root)
        try require(attributes[.type] as? FileAttributeType == .typeDirectory &&
                    (attributes[.ownerAccountID] as? NSNumber)?.intValue == Int(geteuid()) &&
                    (attributes[.posixPermissions] as? NSNumber)?.intValue == 0o700,
                    "root ownership or permissions changed")
        let env = ProcessInfo.processInfo.environment
        try require(env["JM_APP_ISOLATION"] == nil && env["JM_APP_ISOLATION_ROOT"] == nil,
                    "AppIsolation would replace the standard bundle domain")

        // This must happen before even UserDefaults.standard is requested.
        // The outer sandbox, not an in-process path check, denies the write.
        try require(denied.hasPrefix("/") && !denied.hasPrefix(root + "/") &&
                    !FileManager.default.fileExists(atPath: denied),
                    "outside-root denial sentinel was not fresh")
        do {
            try exclusiveWrite(Data("forbidden".utf8), path: denied)
            throw RecoveryPreferencesProbeError.failed("outside-root write unexpectedly succeeded")
        } catch let error as RecoveryPreferencesProbeError {
            throw error
        } catch {
            try require(permissionDenied(error), "outside-root write failed for a reason other than permission")
        }
        try require(!FileManager.default.fileExists(atPath: denied), "outside-root sentinel appeared")
        let inside = root + "/guard-" + phase
        try require(!FileManager.default.fileExists(atPath: inside), "inside guard already exists")
        let guardBytes = Data((domainName + ":" + phase).utf8)
        try exclusiveWrite(guardBytes, path: inside)
        let observedGuard = try Data(contentsOf: URL(fileURLWithPath: inside))
        try require(observedGuard == guardBytes, "inside-root guard write failed")

        let defaults = UserDefaults.standard
        var before: Bool? = nil
        var after: Bool? = nil
        var credentialWrites = 0
        switch phase {
        case "namespace-seed":
            let prior = defaults.persistentDomain(forName: domainName) ?? [:]
            try require(prior.isEmpty && defaults.object(forKey: recoveryKey) == nil,
                        "new domain already has preferences")
            defaults.set(domainName, forKey: markerKey)
            try require(defaults.synchronize(), "owner marker did not synchronize")
            let saved = try domain(defaults, name: domainName, owner: domainName)
            try require(saved.count == 1, "namespace seed persisted keys besides owner marker")
        case "namespace-read":
            // This phase deliberately never touches Preferences or credentials.
            let saved = try domain(defaults, name: domainName, owner: domainName)
            try require(saved.count == 1 && defaults.object(forKey: recoveryKey) == nil,
                        "fresh process did not read the sole owner marker")
        case "default-enable", "reload-disable", "reload-off":
            let saved = try domain(defaults, name: domainName, owner: domainName)
            let stores = installMemoryCredentials() // before Preferences.load()
            if phase == "default-enable" {
                try require(saved.count == 1 && defaults.object(forKey: recoveryKey) == nil,
                            "default-on was inherited instead of absent")
                defaults.set(root + "/state/metadata.db", forKey: "dbPath")
                defaults.set(root + "/volume", forKey: "mountPoint")
                defaults.set("", forKey: "redisURL")
                defaults.set("127.0.0.1:0", forKey: "nfsListenAddr")
                defaults.set("127.0.0.1:0", forKey: "metricsAddr")
                defaults.set(false, forKey: "startAtLogin")
                defaults.set("", forKey: "managerURL")
                defaults.set(true, forKey: "useExistingPrivateNetwork")
                try require(defaults.synchronize(), "fixture-safe preferences did not synchronize")
            } else {
                let expected = phase == "reload-disable"
                try require(saved[recoveryKey] as? Bool == expected,
                            "fresh process did not see expected persisted recovery value")
            }
            var preferences = Preferences.load()
            before = preferences.replacementRecovery
            try require(before == (phase != "reload-off"), "Preferences.load recovery value changed")
            try require(preferences.dbPath == root + "/state/metadata.db" &&
                        preferences.mountPoint == root + "/volume" && preferences.redisURL.isEmpty &&
                        preferences.nfsListenAddr == "127.0.0.1:0" &&
                        preferences.metricsAddr == "127.0.0.1:0" && !preferences.startAtLogin &&
                        preferences.managerURL.isEmpty && preferences.useExistingPrivateNetwork,
                        "fixture-safe settings were not loaded")
            try config(preferences, root: root, name: phase + "-before-config.json", expected: before!)
            if phase == "default-enable" {
                preferences.reconcileSeconds += 1 // ordinary save cannot materialize an override
                try require(defaults.object(forKey: recoveryKey) == nil && defaults.object(forKey: legacyKey) == nil,
                            "ordinary save wrote a recovery preference")
                for legacy in [false, true] {
                    defaults.set(legacy, forKey: legacyKey)
                    preferences = Preferences.load()
                    try require(preferences.replacementRecovery, "legacy recovery key overrode new default")
                    preferences.reconcileSeconds += 1
                    try require(defaults.object(forKey: recoveryKey) == nil && defaults.object(forKey: legacyKey) as? Bool == legacy,
                                "ordinary save rewrote legacy recovery key")
                }
                defaults.set("malformed", forKey: recoveryKey)
                preferences = Preferences.load()
                try require(!preferences.replacementRecovery, "malformed override enabled recovery")
                preferences.reconcileSeconds += 1
                try require(defaults.object(forKey: recoveryKey) as? String == "malformed",
                            "ordinary save repaired malformed override without user action")
                defaults.set(NSNumber(value: 1), forKey: recoveryKey)
                preferences = Preferences.load()
                try require(!preferences.replacementRecovery, "numeric override was coerced to true")
                preferences.reconcileSeconds += 1
                try require((defaults.object(forKey: recoveryKey) as? NSNumber)?.intValue == 1,
                            "ordinary save repaired numeric override without user action")
                defaults.removeObject(forKey: recoveryKey)
                preferences = Preferences.load()
                try require(preferences.replacementRecovery, "absent override did not return to on")
            }
            if phase != "reload-off" {
                preferences.replacementRecovery = phase == "default-enable"
                try require(defaults.synchronize(), "toggle did not synchronize")
                after = preferences.replacementRecovery
                try require(defaults.persistentDomain(forName: domainName)?[recoveryKey] as? Bool == after,
                            "didSet/save did not persist exact Boolean")
                try config(preferences, root: root, name: phase + "-after-config.json", expected: after!)
            } else {
                after = before
            }
            credentialWrites = stores.reduce(0) { $0 + $1.writes }
            try require(credentialWrites == 0, "a preference phase wrote a credential")
        case "cleanup":
            let saved = defaults.persistentDomain(forName: domainName) ?? [:]
            if !saved.isEmpty {
                try require(saved[markerKey] as? String == domainName,
                            "refusing to remove an unowned persistent domain")
                defaults.removePersistentDomain(forName: domainName)
                try require(defaults.synchronize(), "domain cleanup did not synchronize")
            }
            try require((defaults.persistentDomain(forName: domainName) ?? [:]).isEmpty,
                        "owned persistent domain remains after cleanup")
        default:
            throw RecoveryPreferencesProbeError.failed("unreachable phase")
        }
        let receipt: [String: Any] = [
            "verdict": "PASS", "phase": phase, "pid": Int(getpid()), "domain": domainName,
            "outside_denied": true, "inside_written": true,
            "before": before.map { $0 as Any } ?? NSNull(),
            "after": after.map { $0 as Any } ?? NSNull(), "credential_writes": credentialWrites
        ]
        let raw = try JSONSerialization.data(withJSONObject: receipt, options: [.sortedKeys])
        guard let line = String(data: raw, encoding: .utf8) else {
            throw RecoveryPreferencesProbeError.failed("receipt is not UTF-8")
        }
        print("RECOVERY_PREFERENCES " + line)
    }

    static func main() {
        do { try run() }
        catch {
            FileHandle.standardError.write(Data(("RECOVERY_PREFERENCES FAIL: \(error)\n").utf8))
            exit(1)
        }
    }
}
