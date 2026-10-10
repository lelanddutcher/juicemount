import Foundation
import Darwin

// Compile against actual Core sources, excluding ServerController.swift, like
// RedisCredentialMigrationProbe. Each mode runs in a fresh process. Never set HOME.
enum OnboardingPreflight {
    enum ParseResult { case success((String, UInt16)); case failure(String) }
    static func parseRedisHostPort(_ raw: String) -> ParseResult { .failure("probe stub") }
}

@main
struct AppIsolationStorageProbe {
    static var checks = 0
    static func check(_ value: Bool, _ message: String) {
        guard value else { fatalError("FAIL: " + message) }
        checks += 1
    }

    static func main() throws {
        let mode = CommandLine.arguments.dropFirst().first ?? "read"
        if mode == "reject" {
            do { try AppIsolation.requireValid() }
            catch { print("PASS reject 1"); return }
            fatalError("invalid scope was accepted")
        }
        try AppIsolation.requireValid()
        guard let pw = getpwuid(geteuid()) else { fatalError("account unavailable") }
        check(ProcessInfo.processInfo.environment["HOME"] == String(cString: pw.pointee.pw_dir), "HOME changed")
        if mode == "normal" {
            check(AppIsolation.active == nil, "ordinary mode became isolated")
            print("PASS normal \(checks)"); return
        }
        guard let scope = AppIsolation.active else { fatalError("private scope required") }
        let slots: [CredentialSlot] = [.redisPassword, .linkAuthKey, .objectKey, .enrollment, .previousLogin]
        let account = "scope-account"
        if mode == "write" || mode == "read" {
            let preferences = AppIsolation.preferences
            if mode == "write" {
                preferences.set("private-persisted", forKey: "scope-value")
                preferences.set(true, forKey: "scope-flag")
            }
            check(preferences.string(forKey: "scope-value") == "private-persisted", "private preference restart")
            check(preferences.bool(forKey: "scope-flag"), "private bool restart")
            for slot in slots {
                guard let store = RedisCredentialStore.standard(slot) as? IsolatedFileCredentials else {
                    fatalError("Keychain selected in isolated mode")
                }
                if mode == "write" { check(store.setPassword("scope-" + slot.rawValue, for: account), "private slot write") }
                check(store.password(for: account) == "scope-" + slot.rawValue, "private slot restart")
                check(store.password(for: "other-account") == nil, "endpoint binding lost")
                try scope.checkPrivateFile(store.path)
            }
            print("PASS \(mode) \(checks)"); return
        }
        if mode == "refuse-preferences" {
            do { _ = try AppIsolation.isolatedPreferences(for: scope) }
            catch { print("PASS refuse-preferences \(checks + 1)"); return }
            fatalError("malformed preferences accepted")
        }
        if mode == "helper-marker" {
            guard let outside = ProcessInfo.processInfo.environment["JM_SCOPE_SENTINEL"] else {
                fatalError("external sentinel required")
            }
            let sentinel = try Data(contentsOf: URL(fileURLWithPath: outside))
            let marker = scope.child(".juicemount-existing-helper-v1")
            let url = URL(fileURLWithPath: marker)
            let exact = Data("JuiceMount existing helper acceptance v1\n".utf8)
            func writeMarker(_ data: Data) throws {
                try data.write(to: url)
                check(chmod(marker, 0o600) == 0, "private marker mode")
            }
            check(!scope.permitsExistingHelperForBusyQuit(), "absent marker enabled helper")
            try writeMarker(exact)
            check(scope.permitsExistingHelperForBusyQuit(), "exact marker refused")
            check(unlink(marker) == 0, "remove exact marker")
            try writeMarker(Data("JuiceMount existing helper acceptance v1\nextra".utf8))
            check(!scope.permitsExistingHelperForBusyQuit(), "malformed marker enabled helper")
            check(unlink(marker) == 0, "remove malformed marker")
            try writeMarker(exact)
            check(chmod(marker, 0o644) == 0, "set unsafe marker mode")
            check(!scope.permitsExistingHelperForBusyQuit(), "unsafe mode enabled helper")
            check(chmod(marker, 0o000) == 0, "set unreadable marker mode")
            check(!scope.permitsExistingHelperForBusyQuit(), "unreadable marker enabled helper")
            check(unlink(marker) == 0, "remove unsafe marker")
            check(symlink(outside, marker) == 0, "plant marker symlink")
            check(!scope.permitsExistingHelperForBusyQuit(), "marker symlink enabled helper")
            check(unlink(marker) == 0, "remove marker symlink")
            check(link(outside, marker) == 0, "plant marker hardlink")
            check(!scope.permitsExistingHelperForBusyQuit(), "marker hardlink enabled helper")
            check(unlink(marker) == 0, "remove marker hardlink")
            check(mkdir(marker, 0o700) == 0, "plant marker directory")
            check(!scope.permitsExistingHelperForBusyQuit(), "marker directory enabled helper")
            check(rmdir(marker) == 0, "remove marker directory")
            check(mkfifo(marker, 0o600) == 0, "plant marker FIFO")
            check(!scope.permitsExistingHelperForBusyQuit(), "marker FIFO enabled helper")
            check(unlink(marker) == 0, "remove marker FIFO")
            check(try Data(contentsOf: URL(fileURLWithPath: outside)) == sentinel, "external sentinel changed")
            print("PASS helper-marker \(checks)"); return
        }
        guard mode == "files", let outside = ProcessInfo.processInfo.environment["JM_SCOPE_SENTINEL"] else {
            fatalError("unknown fixture mode")
        }
        let sentinel = try Data(contentsOf: URL(fileURLWithPath: outside))
        for slot in slots {
            let path = scope.child("credential-" + slot.rawValue + ".json")
            let store = IsolatedFileCredentials(path: path)
            try scope.removePrivateFile(path)
            check(symlink(outside, path) == 0, "plant leaf link")
            check(store.password(for: account) == nil, "linked credential read")
            check(!store.setPassword("replacement", for: account), "linked credential write")
            check((try? scope.removePrivateFile(path)) == nil, "linked credential deletion accepted")
            check(unlink(path) == 0, "retire owned link")
            check(try Data(contentsOf: URL(fileURLWithPath: outside)) == sentinel, "external sentinel altered")

            try Data("owned-invalid-mode".utf8).write(to: URL(fileURLWithPath: path))
            check(chmod(path, 0o644) == 0, "set fixture mode")
            check(store.password(for: account) == nil && !store.setPassword("replacement", for: account), "unprivate credential accepted")
            check(try Data(contentsOf: URL(fileURLWithPath: path)) == Data("owned-invalid-mode".utf8), "refused file changed")
            check(unlink(path) == 0, "retire owned mode fixture")

            check(link(outside, path) == 0, "plant hard link")
            check(store.password(for: account) == nil && !store.setPassword("replacement", for: account), "multiply linked credential accepted")
            check(unlink(path) == 0, "retire owned hard link")
            check(mkdir(path, 0o700) == 0, "plant directory")
            check(store.password(for: account) == nil && !store.setPassword("replacement", for: account), "credential directory accepted")
            check(rmdir(path) == 0, "retire owned directory")
            check(mkfifo(path, 0o600) == 0, "plant FIFO")
            check(store.password(for: account) == nil && !store.setPassword("replacement", for: account), "credential FIFO accepted")
            check(unlink(path) == 0, "retire owned FIFO")
        }
        let externalStore = IsolatedFileCredentials(path: outside)
        check(externalStore.password(for: account) == nil && !externalStore.setPassword("replacement", for: account), "outside store accepted")
        let prefs = scope.child("preferences.plist")
        try scope.removePrivateFile(prefs)
        check(symlink(outside, prefs) == 0, "plant preference link")
        check((try? scope.checkPrivateFile(prefs)) == nil, "linked preferences path accepted")
        do { _ = try AppIsolation.isolatedPreferences(for: scope); fatalError("linked preferences accepted") }
        catch { checks += 1 }
        check(unlink(prefs) == 0, "retire owned preference link")
        check(try Data(contentsOf: URL(fileURLWithPath: outside)) == sentinel, "external sentinel changed")
        print("PASS files \(checks)")
    }
}
