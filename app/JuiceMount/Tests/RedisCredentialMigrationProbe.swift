import Foundation

// Standalone source probe for H-8 option B lane L3. It compiles against the
// app's Core sources only, with a module map for JuiceMountCore; it never calls
// into libnfsd, so those symbols are left to dynamic lookup:
//
//   swiftc -parse-as-library -I <dir with a JuiceMountCore module.modulemap> \
//     -Xlinker -undefined -Xlinker dynamic_lookup -o <scratch>/redis-cred-probe \
//     Tests/RedisCredentialMigrationProbe.swift Sources/JuiceMount/Core/*.swift (minus ServerController.swift)
//
// Run it only in a fresh AppIsolation root (it writes real isolated
// preferences and a private credential file under the root):
//
//   JM_APP_ISOLATION=1 JM_APP_ISOLATION_ROOT=<root> \
//     TMPDIR=<root>/tmp JM_APP_ISOLATION_FUSE_METRICS_ADDR=127.0.0.1:<port> \
//     JM_ENDPOINT_VECTORS=<repo>/internal/credstore/testdata/endpoint_vectors.json \
//     <scratch>/redis-cred-probe
//
// Checks (default mode, never the real Keychain): a password embedded in a
// saved redisURL and a Link key kept in the plist move to the credential
// stores; a store that refuses keeps them rather than losing them; a second
// load changes nothing; building the engine config (Settings does it on every
// render) never reads a store; a password is attached only to the endpoint it
// was saved for; the Settings password field saves for the current endpoint;
// a typed URL password is stored, not saved; and the isolated file store is
// private, endpoint-bound and in the JSON shape the service reads.
// Device binding (JM_DEVICE_VECTORS=<repo>/internal/credstore/testdata/
// device_fingerprint_vectors.json): the digest matches the service's; a
// secret stamped for another Mac is discarded and the clone notice is raised;
// a matching stamp keeps it; an unstamped (pre-binding) secret is kept and
// stamped once. The hardware UUID is injected; the real one is never used.
// `redis-cred-probe keychain`, run by hand with the real HOME and no isolation,
// round-trips one throwaway login-keychain item (unique service name).

// Core/PermissionProbes.swift references this UI type; the probe never calls it.
enum OnboardingPreflight {
    enum ParseResult {
        case success((String, UInt16))
        case failure(String)
    }
    static func parseRedisHostPort(_ raw: String) -> ParseResult { .failure("probe stub") }
}

final class MemoryCredentials: RedisCredentialStoring {
    var account: String?
    var secret: String?
    var reads = 0
    var writes = 0
    var refuse = false
    func password(for account: String) -> String? {
        reads += 1
        return account == self.account ? secret : nil
    }
    func setPassword(_ password: String, for account: String) -> Bool {
        if refuse { return false }
        writes += 1
        (self.account, secret) = (account, password)
        return true
    }
}

/// A Keychain stand-in holding at most one item, with switchable failures.
final class FakeKeychain: KeychainOperations {
    var item: [String: Any]?
    var failUpdate: OSStatus?
    var failAdd: OSStatus?
    var deletes = 0
    func copyMatching(_ query: [String: Any]) -> (OSStatus, CFTypeRef?) {
        guard let item else { return (errSecItemNotFound, nil) }
        return (errSecSuccess, item as CFDictionary)
    }
    func update(_ query: [String: Any], _ values: [String: Any]) -> OSStatus {
        if let failUpdate { return failUpdate }
        guard item != nil else { return errSecItemNotFound }
        item!.merge(values) { $1 }
        return errSecSuccess
    }
    func add(_ attributes: [String: Any]) -> OSStatus {
        if let failAdd { return failAdd }
        item = attributes
        return errSecSuccess
    }
    func delete(_ query: [String: Any]) -> OSStatus {
        deletes += 1
        item = nil
        return errSecSuccess
    }
}

@main
struct RedisCredentialMigrationProbe {
    /// exit() skips defers, so a failing check runs this first.
    static var cleanup: () -> Void = {}

    static func check(_ ok: Bool, _ message: String) {
        if !ok {
            cleanup()
            FileHandle.standardError.write(Data(("FAIL: " + message + "\n").utf8))
            exit(1)
        }
    }

    /// `redis-cred-probe keychain`: one throwaway item under a unique service.
    static func keychainRoundTrip() throws {
        check(AppIsolation.active == nil, "keychain mode must run without isolation")
        let store = RedisCredentialStore(service: "com.juicemount.redis-metadata.probe-\(UUID().uuidString)",
                                         slot: .redisPassword)
        let account = "redis://probe@127.0.0.1:1"
        cleanup = { store.remove() }
        defer { store.remove() }
        check(store.setPassword("first", for: account), "Keychain add failed")
        check(store.setPassword("s3cr:et@/pa%ss", for: account), "Keychain replace failed")
        check(store.password(for: account) == "s3cr:et@/pa%ss", "Keychain read-back mismatch")
        check(store.password(for: "redis://probe@127.0.0.2:1") == nil, "Keychain password leaked to another endpoint")
        check(store.remove() && store.password(for: account) == nil, "Keychain delete")
        print("RedisCredentialMigrationProbe keychain PASS")
    }

    static func main() throws {
        if CommandLine.arguments.dropFirst().first == "keychain" {
            try keychainRoundTrip()
            return
        }
        // By hand only: `keychain-acl create <service> <trusted exe or ->` makes
        // the item the app would make (secret "acl-probe" for account
        // "redis://probe@127.0.0.1:1"), `keychain-acl remove <service>` deletes
        // it. Used to check that a separate binary (the service) can read it
        // when, and only when, it is on the access list.
        let args = Array(CommandLine.arguments.dropFirst())
        if args.first == "keychain-acl", args.count >= 3 {
            check(AppIsolation.active == nil, "keychain mode must run without isolation")
            let trusted = args.count > 3 && args[3] != "-" ? [args[3]] : []
            let store = RedisCredentialStore(service: args[2], slot: .redisPassword, trustedPaths: trusted)
            if args[1] == "create" {
                check(store.setPassword("acl-probe", for: "redis://probe@127.0.0.1:1"), "Keychain add with access list failed")
            } else {
                check(store.remove(), "Keychain delete")
            }
            print("keychain-acl \(args[1]) PASS")
            return
        }
        guard AppIsolation.active != nil else {
            fatalError("RedisCredentialMigrationProbe requires a fresh isolated app root")
        }
        let macA = "AAAAAAAA-0000-4000-8000-00000000000A", macB = "BBBBBBBB-0000-4000-8000-00000000000B"
        DeviceFingerprint.platformUUID = { macA }
        let secret = "s3cr:et@/pa%ss"
        let legacy = "redis://mac-edit2:s3cr%3Aet%40%2Fpa%25ss@nas.local:30179/1"
        let stripped = "redis://mac-edit2@nas.local:30179/1"
        let account = "redis://mac-edit2@nas.local:30179"
        let linkKey = "tskey-auth-legacy"

        // Pure helpers.
        check(RedisURLCredential.split(legacy).url == stripped, "split URL = \(RedisURLCredential.split(legacy).url)")
        check(RedisURLCredential.split(legacy).password == secret, "split password not decoded")
        check(RedisURLCredential.split("redis://:pw@h:6379/1").url == "redis://h:6379/1", "empty-user URL kept '@'")
        check(RedisURLCredential.split(stripped).password == nil, "password-less URL reported a password")
        check(RedisURLCredential.account(for: legacy) == account, "account = \(String(describing: RedisURLCredential.account(for: legacy)))")
        check(RedisURLCredential.withPassword(stripped, password: secret) == legacy, "re-attached URL = \(RedisURLCredential.withPassword(stripped, password: secret))")
        check(RedisURLCredential.split(RedisURLCredential.withPassword("redis://h/1", password: "pw")).password == "pw",
              "user-less password did not round trip")
        check(RedisURLCredential.linkAccount(for: " https://hs.example/ ") == "https://hs.example", "link account")

        // Shared endpoint-key and URL vectors (also checked by internal/credstore).
        guard let vectorPath = ProcessInfo.processInfo.environment["JM_ENDPOINT_VECTORS"],
              let vectorData = FileManager.default.contents(atPath: vectorPath),
              let vectors = try JSONSerialization.jsonObject(with: vectorData) as? [String: Any] else {
            check(false, "set JM_ENDPOINT_VECTORS to internal/credstore/testdata/endpoint_vectors.json"); return
        }
        func cases(_ key: String) -> [[String: String]] { vectors[key] as? [[String: String]] ?? [] }
        check(!cases("redis_accounts").isEmpty && !cases("password_urls").isEmpty, "vector file has an empty section")
        for c in cases("redis_accounts") {
            let got = RedisURLCredential.account(for: c["url"]!)
            check(got == c["account"], "account(\(c["url"]!)) = \(got ?? "nil"), want \(c["account"]!)")
        }
        for c in cases("link_accounts") {
            check(RedisURLCredential.linkAccount(for: c["url"]!) == c["account"], "linkAccount(\(c["url"]!))")
        }
        for c in cases("password_urls") {
            let got = RedisURLCredential.withPassword(c["url"]!, password: c["password"]!)
            check(got == c["canonical"], "withPassword(\(c["url"]!)) = \(got), want \(c["canonical"]!)")
            check(RedisURLCredential.split(got).password == c["password"], "canonical URL does not decode to its password")
        }

        // The Keychain store never deletes the old secret to write a new one.
        let fake = FakeKeychain()
        let kc = RedisCredentialStore(service: "probe", slot: .redisPassword, keychain: fake)
        check(kc.setPassword("old", for: account), "fake Keychain add")
        check(kc.setPassword("new", for: account) && kc.password(for: account) == "new", "fake Keychain update")
        check(kc.password(for: "redis://mac-edit2@other.local:30179") == nil, "Keychain store returned a secret for another endpoint")
        fake.failUpdate = errSecInteractionNotAllowed
        check(!kc.setPassword("newer", for: account), "a refused update reported success")
        check(kc.password(for: account) == "new" && fake.deletes == 0, "a refused update lost the stored secret")
        fake.failUpdate = nil
        fake.item = nil
        fake.failAdd = errSecDuplicateItem
        check(!kc.setPassword("first", for: account) && fake.deletes == 0, "a refused add deleted something")

        // Older builds kept the password in redisURL and the Link key in the
        // plist. Seed that before Preferences first touches its cached store.
        func saved(_ key: String = "redisURL") -> String? { AppIsolation.preferences.string(forKey: key) }
        AppIsolation.preferences.set(legacy, forKey: "redisURL")
        AppIsolation.preferences.set("https://hs.example/", forKey: "link_server_url")
        AppIsolation.preferences.set(linkKey, forKey: "link_auth_key")

        // Stores that cannot hold the secrets must not lose them.
        let refusing = MemoryCredentials(), refusingLink = MemoryCredentials()
        (refusing.refuse, refusingLink.refuse) = (true, true)
        (Preferences.credentialStore, Preferences.linkCredentialStore) = (refusing, refusingLink)
        let kept = Preferences.load()
        check(saved() == legacy && saved("link_auth_key") == linkKey, "refused store: the only copy of a secret was dropped")
        check(kept.toServerConfig().redisURL == legacy && kept.linkAuthKey == linkKey, "refused store: engine lost a secret")

        // Migration: both secrets move to the stores, the plist is cleaned.
        let memory = MemoryCredentials(), link = MemoryCredentials()
        (Preferences.credentialStore, Preferences.linkCredentialStore) = (memory, link)
        let first = Preferences.load()
        check(memory.account == account && memory.secret == secret, "password was not moved to the credential store")
        check(link.account == "https://hs.example" && link.secret == linkKey, "Link key was not moved to the credential store")
        check(first.redisURL == stripped && saved() == stripped, "saved redisURL = \(saved() ?? "nil"), want stripped")
        check(saved("link_auth_key") == nil, "Link key left in the preferences plist")

        // Building the config never reads a store (Settings renders call it).
        first.useExistingPrivateNetwork = false
        let reads = memory.reads + link.reads
        for _ in 0..<3 { _ = first.toServerConfig() }
        _ = first.redisPasswordStatus
        let cfg = first.toServerConfig()
        check(memory.reads + link.reads == reads, "toServerConfig read the credential store")
        check(cfg.redisURL == legacy, "engine URL = \(cfg.redisURL), want the password re-attached")
        check(cfg.netAuthKey == linkKey, "engine Link key = \(cfg.netAuthKey)")
        check(saved() == stripped, "toServerConfig persisted the password")

        let writes = memory.writes + link.writes
        let second = Preferences.load()
        check(second.redisURL == stripped && memory.writes + link.writes == writes && memory.secret == secret,
              "second load was not idempotent")

        // Another endpoint never gets the password; Settings says so.
        let other = "redis://mac-edit2@other.local:30179/1"
        first.redisURL = other
        check(first.toServerConfig().redisURL == other, "password attached to an endpoint it was not saved for")
        check(first.redisPasswordStatus == .missing(account: "redis://mac-edit2@other.local:30179"),
              "status for an unsaved endpoint = \(first.redisPasswordStatus)")

        // The Settings password field saves for the current endpoint.
        let done = DispatchSemaphore(value: 0)
        var stored = false
        first.setRedisPassword("new:pw") { ok in stored = ok; done.signal() }
        done.wait()
        check(stored && memory.account == "redis://mac-edit2@other.local:30179" && memory.secret == "new:pw",
              "password field did not save for the current endpoint")
        check(first.toServerConfig().redisURL == "redis://mac-edit2:new%3Apw@other.local:30179/1",
              "engine URL after saving = \(first.toServerConfig().redisURL)")
        check(first.redisPasswordStatus == .saved(account: "redis://mac-edit2@other.local:30179"), "status after saving")

        // A password typed into the URL is stored, not saved to the plist.
        let typed = MemoryCredentials()
        Preferences.credentialStore = typed
        second.redisURL = "redis://mac-edit2:typed-pw@nas.local:30179/1"
        check(saved() == stripped, "save() persisted a typed password in the preferences plist")
        check(typed.account == account && typed.secret == "typed-pw", "save() did not store the typed password")
        let typedWrites = typed.writes
        second.ssdCacheGB += 1 // any later save must not rewrite the unchanged password
        check(typed.writes == typedWrites, "an unrelated save rewrote the stored password")

        // The Link key follows an edited control URL (it is shown next to it).
        second.linkServerURL = "https://hs2.example"
        Preferences.credentialWrites.sync {}
        check(link.account == "https://hs2.example" && link.secret == linkKey, "Link key did not follow its URL")

        // Isolation keeps secrets in private files the service reads, never the
        // Keychain, bound to their endpoint.
        guard let isolated = RedisCredentialStore.standard(.redisPassword) as? IsolatedFileCredentials else {
            check(false, "isolated root uses the Keychain"); return
        }
        check(isolated.setPassword(secret, for: account) && isolated.password(for: account) == secret,
              "isolated credential file round trip")
        check(isolated.password(for: "redis://mac-edit2@other.local:30179") == nil,
              "isolated credential returned for another endpoint")
        let mode = (try FileManager.default.attributesOfItem(atPath: isolated.path)[.posixPermissions] as? NSNumber)?.intValue
        check(mode == 0o600, "isolated credential file mode \(String(describing: mode))")
        let json = try JSONSerialization.jsonObject(with: Data(contentsOf: URL(fileURLWithPath: isolated.path))) as? [String: String]
        check(json == ["account": account, "secret": secret, "device": DeviceFingerprint.digest(macA)!],
              "isolated file is not the service's JSON shape: \(String(describing: json))")
        check(isolated.path.hasSuffix("/credential-metadata.json"), "isolated file name \(isolated.path)")

        try deviceBindingChecks(macA: macA, macB: macB, account: account, isolatedPath: isolated.path)
        print("RedisCredentialMigrationProbe PASS")
    }

    static func deviceBindingChecks(macA: String, macB: String, account: String, isolatedPath: String) throws {
        // Shared digest vectors (also checked by internal/deviceid).
        guard let path = ProcessInfo.processInfo.environment["JM_DEVICE_VECTORS"],
              let data = FileManager.default.contents(atPath: path),
              let root = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let vectors = root["vectors"] as? [[String: String]], !vectors.isEmpty else {
            check(false, "set JM_DEVICE_VECTORS to internal/credstore/testdata/device_fingerprint_vectors.json"); return
        }
        for v in vectors {
            let got = DeviceFingerprint.digest(v["platform_uuid"]!)
            check(got == v["digest"], "digest(\(v["platform_uuid"]!)) = \(got ?? "nil"), want \(v["digest"]!)")
        }
        let a = DeviceFingerprint.digest(macA)!, b = DeviceFingerprint.digest(macB)!
        func comment(_ f: FakeKeychain) -> String? { f.item?[kSecAttrComment as String] as? String }

        // Matching fingerprint: the secret is kept and nothing is deleted.
        DeviceBinding.resetForProbe()
        let fake = FakeKeychain()
        var onA = RedisCredentialStore(service: "probe", slot: .redisPassword, keychain: fake)
        onA.device = { a }
        check(onA.setPassword("mine", for: account), "stamped add")
        check(comment(fake) == DeviceBinding.stampPrefix + a, "saved secret not stamped for this Mac: \(comment(fake) ?? "nil")")
        check(onA.password(for: account) == "mine" && fake.deletes == 0 && !DeviceBinding.cloneDetected,
              "matching fingerprint did not keep the secret")

        // Mismatch: the same item read on another Mac is discarded, not used.
        var onB = onA
        onB.device = { b }
        check(onB.password(for: account) == nil, "a secret saved on another Mac was returned")
        check(fake.item == nil && fake.deletes == 1, "the copied secret was not discarded")
        check(DeviceBinding.cloneDetected, "clone not surfaced after discarding a copied secret")
        let prefs = Preferences.load()
        check(prefs.deviceMigrationNotice == DeviceBinding.cloneMessage,
              "Preferences notice = \(prefs.deviceMigrationNotice ?? "nil")")

        // Upgrade: an unstamped item is kept and stamped for this Mac once.
        DeviceBinding.resetForProbe()
        fake.item = [kSecAttrGeneric as String: Data(account.utf8), kSecValueData as String: Data("legacy".utf8)]
        check(onA.password(for: account) == "legacy" && fake.deletes == 1, "upgrade dropped an unstamped secret")
        check(comment(fake) == DeviceBinding.stampPrefix + a, "upgrade did not stamp the legacy secret")
        check(!DeviceBinding.cloneDetected, "upgrade raised the clone notice")
        check(onB.password(for: account) == nil, "an adopted secret copied to another Mac was returned")

        // No fingerprint (IOKit refused): nothing to compare, so a secret
        // stamped for another Mac is KEPT and nothing is deleted.
        DeviceBinding.resetForProbe()
        check(onA.setPassword("kept", for: account), "stamped add before unavailable read")
        let deletesBefore = fake.deletes
        var unknown = onA
        unknown.device = { nil }
        check(unknown.password(for: account) == "kept", "no fingerprint: the secret was not kept")
        check(fake.item != nil && fake.deletes == deletesBefore, "no fingerprint: the Keychain item was deleted")
        check(!DeviceBinding.cloneDetected, "no fingerprint: clone notice raised")
        check(comment(fake) == DeviceBinding.stampPrefix + a, "no fingerprint: the stamp was rewritten")

        // Isolated file store (the service's mode in acceptance roots).
        DeviceBinding.resetForProbe()
        var fileA = IsolatedFileCredentials(path: isolatedPath)
        fileA.device = { a }
        var fileB = fileA
        fileB.device = { b }
        check(fileA.setPassword("iso", for: account) && fileA.password(for: account) == "iso", "isolated stamped round trip")
        check(fileB.password(for: account) == nil && !FileManager.default.fileExists(atPath: isolatedPath),
              "isolated secret from another Mac was not discarded")
        check(DeviceBinding.cloneDetected, "isolated discard not surfaced")
        let legacy = try JSONSerialization.data(withJSONObject: ["account": account, "secret": "old"])
        check(FileManager.default.createFile(atPath: isolatedPath, contents: legacy, attributes: [.posixPermissions: 0o600]),
              "seed legacy isolated file")
        check(fileA.password(for: account) == "old", "upgrade dropped an unstamped isolated secret")
        let stamped = try JSONSerialization.jsonObject(with: Data(contentsOf: URL(fileURLWithPath: isolatedPath))) as? [String: String]
        check(stamped?["device"] == a, "upgrade did not stamp the isolated secret")
        var fileUnknown = fileA
        fileUnknown.device = { nil }
        var fileOther = fileA
        fileOther.device = { b }
        check(fileOther.setPassword("other", for: account), "seed isolated secret stamped for another Mac")
        check(fileUnknown.password(for: account) == "other" && FileManager.default.fileExists(atPath: isolatedPath),
              "no fingerprint: isolated secret not kept")
        DeviceBinding.resetForProbe()
    }
}
