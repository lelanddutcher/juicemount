import CryptoKit
import Foundation
import IOKit
import Security

/// This Mac's device digest, the same value internal/deviceid computes:
/// first 16 bytes of SHA-256("juicemount-device-v1\n" + upper(trim(IOPlatformUUID))),
/// lower-case hex. Both sides are checked against
/// internal/credstore/testdata/device_fingerprint_vectors.json. Only the digest
/// is ever stored, never the hardware UUID.
enum DeviceFingerprint {
    /// The raw hardware UUID source. Probes substitute it; nil = unavailable.
    static var platformUUID: () -> String? = readPlatformUUID

    static var current: String? { platformUUID().flatMap(digest) }

    static func digest(_ uuid: String) -> String? {
        let norm = uuid.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        guard !norm.isEmpty else { return nil }
        let sum = SHA256.hash(data: Data(("juicemount-device-v1\n" + norm).utf8))
        return sum.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private static func readPlatformUUID() -> String? {
        let dev = IOServiceGetMatchingService(kIOMainPortDefault, IOServiceMatching("IOPlatformExpertDevice"))
        guard dev != IO_OBJECT_NULL else { return nil }
        defer { IOObjectRelease(dev) }
        let value = IORegistryEntryCreateCFProperty(dev, kIOPlatformUUIDKey as CFString, kCFAllocatorDefault, 0)
        return value?.takeRetainedValue() as? String
    }
}

/// Device binding of stored secrets (H-8 option B: one Manager principal per
/// Mac). Each secret is stamped with the digest of the Mac that saved it. The
/// login keychain and the app's files move with Migration Assistant (and,
/// before macOS 26.4, with a Time Machine restore or a cloned disk), and this
/// legacy keychain item cannot carry a ThisDeviceOnly class, so the stamp is
/// the check. A secret stamped for another Mac is discarded on first read and
/// `cloneDetected` is set; an unstamped secret (saved before the binding) is
/// stamped for this Mac once and kept, so an upgrade never forces re-enrollment.
enum DeviceBinding {
    /// What the app (and the Manager, for the rotated principal) shows.
    static let cloneMessage = "This Mac was migrated or cloned; it must be approved in the Manager as a new Mac."
    /// Prefix of the Keychain comment stamp. Must match credstore.DeviceStampPrefix.
    static let stampPrefix = "jm-device:"

    private static let lock = NSLock()
    private static var discarded = false

    /// True once this process discarded a secret saved on another Mac.
    static var cloneDetected: Bool {
        lock.lock(); defer { lock.unlock() }
        return discarded
    }

    static func recordDiscard() {
        lock.lock(); discarded = true; lock.unlock()
        NSLog("JuiceMount: discarded a credential saved on another Mac (migrated or cloned)")
    }

    /// Probes only.
    static func resetForProbe() { lock.lock(); discarded = false; lock.unlock() }

    enum Verdict { case keep, adopt, discard }

    /// stored: the digest on the secret ("" or nil = unstamped).
    static func verdict(stored: String?, device: String?) -> Verdict {
        guard let device else { return .keep } // nothing to compare against
        guard let stored, !stored.isEmpty else { return .adopt }
        return stored == device ? .keep : .discard
    }
}

/// Where the app keeps the secrets it never writes to a preferences plist or
/// to the service's saved config: the Redis metadata password (once the server
/// enforces per-Mac principals, H-8, this Mac's write identity) and the Link
/// pairing key.
///
/// Each slot holds ONE secret, bound to the endpoint it was saved for
/// (`RedisURLCredential.account` for Redis, the control URL for Link). A lookup
/// for any other endpoint returns nil, so a secret is never sent to a server it
/// was not saved for. One slot overwritten in place also means a URL typed key
/// by key cannot leave a trail of items for half-typed host names.
///
/// The service reads the same items itself (internal/credstore), so launchd can
/// restart it and recover the volume with no app running. Slot names, the
/// Keychain service and the isolated file format must match that package.
protocol RedisCredentialStoring {
    func password(for account: String) -> String?
    /// Returns false when the secret could not be stored. Callers must then
    /// keep their copy rather than drop the only one.
    func setPassword(_ password: String, for account: String) -> Bool
}

/// The four Security calls the store makes, so a probe can substitute a fake
/// and never touch the real Keychain.
protocol KeychainOperations {
    func copyMatching(_ query: [String: Any]) -> (OSStatus, CFTypeRef?)
    func update(_ query: [String: Any], _ values: [String: Any]) -> OSStatus
    func add(_ attributes: [String: Any]) -> OSStatus
    func delete(_ query: [String: Any]) -> OSStatus
}

struct SystemKeychain: KeychainOperations {
    func copyMatching(_ query: [String: Any]) -> (OSStatus, CFTypeRef?) {
        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        return (status, result)
    }
    func update(_ query: [String: Any], _ values: [String: Any]) -> OSStatus {
        SecItemUpdate(query as CFDictionary, values as CFDictionary)
    }
    func add(_ attributes: [String: Any]) -> OSStatus { SecItemAdd(attributes as CFDictionary, nil) }
    func delete(_ query: [String: Any]) -> OSStatus { SecItemDelete(query as CFDictionary) }
}

enum CredentialSlot: String {
    case redisPassword = "metadata"
    case linkAuthKey = "link-authkey"
    /// H-8B: the volume's no-delete object-store key from Manager
    /// enrollment, bound to the same Redis account as the password and stored
    /// as `ObjectKeyCredential.encode`. The service reads it too
    /// (credstore.ObjectKey) to recover with no app running.
    case objectKey = "object-key"
    /// The pending Manager enrollment (`EnrollmentRecord` JSON: the secret,
    /// poll token and last known status), bound to the fixed account
    /// `EnrollmentRecord.account`. Which Manager it belongs to is decided by
    /// the Go client: the Manager's ID once one answered with it, else the
    /// address (enrollclient.ManagerKey). Only the app reads it.
    case enrollment = "enrollment"
    /// The Redis login this Mac used before an approval switched it to its
    /// principal (`PreviousLogin` JSON: the password-free URL and its
    /// password), so the switch can be undone. Written BEFORE the metadata
    /// slot is overwritten; an approval never destroys the only copy of the
    /// pre-cutover password. Only the app reads it.
    case previousLogin = "metadata-previous"
}

/// The pre-enrollment login kept in the `.previousLogin` slot.
struct PreviousLogin: Codable, Equatable {
    /// The Redis URL without a password.
    var redisURL: String
    var password: String
    /// The slot's fixed account (one item per slot).
    static let account = "previous-login"

    enum CodingKeys: String, CodingKey { case redisURL = "redis_url", password }

    var stored: String? {
        (try? JSONEncoder().encode(self)).flatMap { String(data: $0, encoding: .utf8) }
    }

    static func load(_ stored: String?) -> PreviousLogin? {
        guard let data = stored?.data(using: .utf8), !data.isEmpty,
              let login = try? JSONDecoder().decode(PreviousLogin.self, from: data),
              !login.redisURL.isEmpty, !login.password.isEmpty else { return nil }
        return login
    }
}

/// Stored form of an object key: access key, newline, secret key. Must match
/// credstore.EncodeObjectKey / DecodeObjectKey in Go.
enum ObjectKeyCredential {
    private static func hasLineBreak(_ s: Substring) -> Bool {
        s.unicodeScalars.contains { $0 == "\n" || $0 == "\r" }
    }

    static func encode(accessKey: String, secretKey: String) -> String? {
        guard !accessKey.isEmpty, !secretKey.isEmpty,
              !hasLineBreak(Substring(accessKey)), !hasLineBreak(Substring(secretKey)) else { return nil }
        return accessKey + "\n" + secretKey
    }

    static func decode(_ stored: String) -> (accessKey: String, secretKey: String)? {
        // Split on the first U+000A scalar (a "\r\n" pair is one Character).
        guard let i = stored.unicodeScalars.firstIndex(of: "\n") else { return nil }
        let access = Substring(stored.unicodeScalars[..<i])
        let secret = Substring(stored.unicodeScalars[stored.unicodeScalars.index(after: i)...])
        guard !access.isEmpty, !secret.isEmpty, !hasLineBreak(access), !hasLineBreak(secret) else { return nil }
        return (String(access), String(secret))
    }
}

/// The enrollment state the Go client (internal/enrollclient State) works on.
/// Kept whole in the `.enrollment` slot: it holds the enrollment secret,
/// which becomes this Mac's Redis password once an admin approves it.
struct EnrollmentRecord: Codable, Equatable {
    /// The `.enrollment` slot's fixed account. Not the Manager URL: a
    /// respelled address (localhost for 127.0.0.1, a trailing slash) would
    /// miss the record, and the one-item slot would then be overwritten with
    /// a fresh secret, leaving the Manager's request in Conflict.
    static let account = "manager-enrollment"

    var managerURL: String = ""
    /// The Manager's ID the record is bound to (Go State.ManagerID; "" =
    /// unbound). Must round-trip: dropping it would unbind the record on
    /// every step.
    var managerID: String = ""
    var instanceID: String = ""
    var secret: String = ""
    var pollToken: String = ""
    var principal: String = ""
    var fingerprint: String = ""
    var status: String = ""
    var failures: Int = 0

    enum CodingKeys: String, CodingKey {
        case managerURL = "manager_url", managerID = "manager_id", instanceID = "instance_id", secret
        case pollToken = "poll_token", principal, fingerprint, status, failures
    }

    init() {}

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        managerURL = try c.decodeIfPresent(String.self, forKey: .managerURL) ?? ""
        managerID = try c.decodeIfPresent(String.self, forKey: .managerID) ?? ""
        instanceID = try c.decodeIfPresent(String.self, forKey: .instanceID) ?? ""
        secret = try c.decodeIfPresent(String.self, forKey: .secret) ?? ""
        pollToken = try c.decodeIfPresent(String.self, forKey: .pollToken) ?? ""
        principal = try c.decodeIfPresent(String.self, forKey: .principal) ?? ""
        fingerprint = try c.decodeIfPresent(String.self, forKey: .fingerprint) ?? ""
        status = try c.decodeIfPresent(String.self, forKey: .status) ?? ""
        failures = try c.decodeIfPresent(Int.self, forKey: .failures) ?? 0
    }

    /// The JSON stored in the slot (never logged).
    var stored: String? {
        (try? JSONEncoder().encode(self)).flatMap { String(data: $0, encoding: .utf8) }
    }

    static func load(_ stored: String?) -> EnrollmentRecord? {
        guard let data = stored?.data(using: .utf8), !data.isEmpty else { return nil }
        return try? JSONDecoder().decode(EnrollmentRecord.self, from: data)
    }
}

/// One generic-password item per slot in the user's login Keychain. The
/// endpoint is kept in the item's generic attribute.
///
/// Access list: the app and the bundled service binary
/// (Contents/MacOS/juicemount-service) are trusted applications, so the
/// LaunchAgent reads the item without a prompt. This is a legacy (file-based)
/// login-keychain item: the data-protection keychain would need a
/// keychain-access-groups entitlement, which needs a provisioning profile and
/// is unavailable to ad-hoc builds. Measured 2026-10-06 with a separate reader
/// binary on the access list, reading with UI disabled: when both binaries are
/// Developer ID signed (team PKUE74YS72) the read succeeds; when the reader is
/// not listed it is refused (errSecAuthFailed) with no dialog; when both are
/// ad-hoc signed the listed reader is ALSO refused, because the item's
/// partition list is per-cdhash for ad-hoc code. So a JM_ADHOC dev build
/// cannot recover with no app running: its service mounts without the
/// password and the proxy fails the mount with AUTH_REQUIRED. The service
/// never prompts; it reads with user interaction disabled.
struct RedisCredentialStore: RedisCredentialStoring {
    let service: String
    let slot: CredentialSlot
    /// Executables besides this process that may read the item.
    var trustedPaths: [String] = []
    var keychain: any KeychainOperations = SystemKeychain()
    /// This Mac's digest (DeviceFingerprint); probes substitute it.
    var device: () -> String? = { DeviceFingerprint.current }

    /// The app's store for a slot. An isolated acceptance root (AppIsolation)
    /// keeps HOME unchanged and never consults the real install's Keychain.
    /// Its secrets stay in owned 0600 JSON files inside the private root.
    static func standard(_ slot: CredentialSlot) -> any RedisCredentialStoring {
        if let scope = AppIsolation.active {
            return IsolatedFileCredentials(path: scope.child("credential-\(slot.rawValue).json"))
        }
        let servicePath = Bundle.main.bundleURL
            .appendingPathComponent("Contents/MacOS/juicemount-service").path
        return RedisCredentialStore(
            service: "com.juicemount.redis-metadata", slot: slot,
            trustedPaths: FileManager.default.isExecutableFile(atPath: servicePath) ? [servicePath] : [])
    }

    private var item: [String: Any] {
        [kSecClass as String: kSecClassGenericPassword,
         kSecAttrService as String: service,
         kSecAttrAccount as String: slot.rawValue]
    }

    func password(for account: String) -> String? {
        var q = item
        q[kSecReturnData as String] = true
        q[kSecReturnAttributes as String] = true
        q[kSecMatchLimit as String] = kSecMatchLimitOne
        let (status, result) = keychain.copyMatching(q)
        guard status == errSecSuccess, let found = result as? [String: Any] else { return nil }
        // Device binding first, whatever the endpoint: a secret saved on
        // another Mac is that Mac's principal and is never used here.
        let comment = found[kSecAttrComment as String] as? String
        let stamp = comment.flatMap { $0.hasPrefix(DeviceBinding.stampPrefix)
            ? String($0.dropFirst(DeviceBinding.stampPrefix.count)) : nil }
        let here = device()
        let verdict = DeviceBinding.verdict(stored: stamp, device: here)
        if verdict == .discard {
            remove()
            DeviceBinding.recordDiscard()
            return nil
        }
        guard let bound = found[kSecAttrGeneric as String] as? Data,
              String(data: bound, encoding: .utf8) == account,
              let data = found[kSecValueData as String] as? Data else { return nil }
        if verdict == .adopt, let here {
            // Upgrade: bind the legacy item to this Mac, once. A failed stamp
            // leaves it unstamped and is retried on the next read.
            _ = keychain.update(item, [kSecAttrComment as String: DeviceBinding.stampPrefix + here])
        }
        return String(data: data, encoding: .utf8)
    }

    /// Replaces the secret in place (SecItemUpdate keeps the item, and its
    /// access list, until the new value is stored); adds the item only when
    /// none exists. It never deletes: a failed write leaves the previous
    /// secret where it was, so a Settings change that cannot be saved is not
    /// also a lost password on the next launch. The access list is set when the
    /// item is first added; a Developer ID update keeps the same designated
    /// requirement, so it stays valid across app updates.
    func setPassword(_ password: String, for account: String) -> Bool {
        var values: [String: Any] = [kSecValueData as String: Data(password.utf8),
                                     kSecAttrGeneric as String: Data(account.utf8)]
        if let here = device() { values[kSecAttrComment as String] = DeviceBinding.stampPrefix + here }
        let status = keychain.update(item, values)
        if status == errSecSuccess { return true }
        guard status == errSecItemNotFound, let access = makeAccess() else { return false }
        var add = item.merging(values) { $1 }
        add[kSecAttrLabel as String] = "JuiceMount metadata server"
        add[kSecAttrAccess as String] = access
        return keychain.add(add) == errSecSuccess
    }

    @discardableResult
    func remove() -> Bool {
        let status = keychain.delete(item)
        return status == errSecSuccess || status == errSecItemNotFound
    }

    /// Legacy-keychain access list: this process plus `trustedPaths`.
    private func makeAccess() -> SecAccess? {
        var apps: [SecTrustedApplication] = []
        var me: SecTrustedApplication?
        guard SecTrustedApplicationCreateFromPath(nil, &me) == errSecSuccess, let me else { return nil }
        apps.append(me)
        for path in trustedPaths {
            var app: SecTrustedApplication?
            guard SecTrustedApplicationCreateFromPath(path, &app) == errSecSuccess, let app else { return nil }
            apps.append(app)
        }
        var access: SecAccess?
        guard SecAccessCreate("JuiceMount metadata server" as CFString, apps as CFArray, &access) == errSecSuccess
        else { return nil }
        return access
    }
}

/// Isolation-only store: a private 0600 JSON file inside the 0700 isolation
/// root, `{"account": ..., "secret": ..., "device": ...}`, read by
/// internal/credstore too. Device binding as in RedisCredentialStore.
struct IsolatedFileCredentials: RedisCredentialStoring {
    let path: String
    var device: () -> String? = { DeviceFingerprint.current }

    private struct Entry: Codable { let account: String; let secret: String; var device: String? }

    func password(for account: String) -> String? {
        guard let scope = AppIsolation.active,
              let data = try? scope.readPrivateFile(path),
              let entry = try? JSONDecoder().decode(Entry.self, from: data) else { return nil }
        let here = device()
        let verdict = DeviceBinding.verdict(stored: entry.device, device: here)
        if verdict == .discard {
            guard (try? scope.removePrivateFile(path)) != nil else { return nil }
            DeviceBinding.recordDiscard()
            return nil
        }
        guard entry.account == account, !entry.secret.isEmpty else { return nil }
        if verdict == .adopt { _ = setPassword(entry.secret, for: entry.account) }
        return entry.secret
    }

    func setPassword(_ password: String, for account: String) -> Bool {
        guard let scope = AppIsolation.active,
              let data = try? JSONEncoder().encode(Entry(account: account, secret: password, device: device())) else { return false }
        return (try? scope.writePrivateFile(data, to: path)) != nil
    }
}

/// Pure URL helpers shared by preference migration and engine config.
enum RedisURLCredential {
    /// The URL without its password, and the decoded password (nil when the
    /// URL carries none). A URL that does not parse is returned unchanged.
    static func split(_ raw: String) -> (url: String, password: String?) {
        guard var c = URLComponents(string: raw), c.percentEncodedPassword != nil else {
            return (raw, nil)
        }
        let password = c.password
        c.percentEncodedPassword = nil
        if c.percentEncodedUser?.isEmpty ?? false { c.percentEncodedUser = nil }
        guard let stripped = c.string else { return (raw, nil) }
        return (stripped, (password?.isEmpty ?? true) ? nil : password)
    }

    /// The account a Redis password is bound to, in the one canonical form the
    /// service (credstore.RedisAccount) also produces; both are checked against
    /// internal/credstore/testdata/endpoint_vectors.json:
    /// `<scheme lower>://[<user>@]<host lower>:<port>`, user decoded then
    /// re-encoded with `escape`, an IPv6 literal in brackets, port 6379 by
    /// default. Never the password, database or query.
    static func account(for raw: String) -> String? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let c = URLComponents(string: split(trimmed).url),
              let scheme = c.scheme?.lowercased(), var host = c.host?.lowercased(),
              !host.isEmpty else { return nil }
        if host.hasPrefix("["), host.hasSuffix("]") { host = String(host.dropFirst().dropLast()) }
        if host.contains(":") { host = "[\(host)]" }
        let name = c.user ?? ""
        let user = name.isEmpty ? "" : escape(name) + "@"
        return "\(scheme)://\(user)\(host):\(c.port ?? 6379)"
    }

    /// Percent-encodes everything but A-Z a-z 0-9 - . _ ~ (upper-case hex),
    /// the single userinfo encoding shared with credstore.EscapeUnreserved.
    static func escape(_ s: String) -> String {
        let unreserved = CharacterSet(charactersIn:
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
        return s.addingPercentEncoding(withAllowedCharacters: unreserved) ?? s
    }

    /// The account a Link pairing key is bound to. Must match
    /// credstore.LinkAccount in Go.
    static func linkAccount(for controlURL: String) -> String {
        var s = controlURL.trimmingCharacters(in: .whitespacesAndNewlines)
        while s.hasSuffix("/") { s.removeLast() }
        return s
    }

    /// Re-attaches a password for the in-memory engine config only, with the
    /// username and password in canonical form (credstore.WithPassword).
    static func withPassword(_ raw: String, password: String) -> String {
        guard var c = URLComponents(string: raw), !password.isEmpty else { return raw }
        let name = c.user ?? ""
        c.percentEncodedUser = escape(name)
        c.percentEncodedPassword = escape(password)
        return c.string ?? raw
    }
}
