import Foundation
import Darwin

/// Opt-in acceptance namespace. Both packaged processes must start with the
/// same environment; the service is launched explicitly for this mode.
enum AppIsolation {
    struct Scope {
        let root: URL
        let fuseMetricsAddr: String
        private static let existingHelperMarker = Data("JuiceMount existing helper acceptance v1\n".utf8)

        func child(_ name: String) -> String {
            root.appendingPathComponent(name).path
        }

        func requireChild(_ path: String) throws {
            let prefix = root.path + "/"
            guard path.hasPrefix(prefix),
                  URL(fileURLWithPath: path).standardizedFileURL.path == path,
                  URL(fileURLWithPath: path).resolvingSymlinksInPath().path == path else {
                throw Failure.invalid("a serving path escapes the private root")
            }
        }

        /// A private acceptance fixture may consult the helper already approved
        /// for this account during busy Quit. This grants no registration or
        /// eject authority; the real readiness and service checks still apply.
        func permitsExistingHelperForBusyQuit() -> Bool {
            guard let data = try? readPrivateFile(child(".juicemount-existing-helper-v1")) else {
                return false
            }
            return data == Self.existingHelperMarker
        }
    }

    enum Failure: LocalizedError {
        case invalid(String)
        var errorDescription: String? {
            if case .invalid(let message) = self { return message }
            return nil
        }
    }

    private static let checked: Result<Scope?, Error> = Result { try inspect() }

    static func requireValid() throws { _ = try checked.get() }

    static var active: Scope? {
        switch checked {
        case .success(let scope): return scope
        case .failure(let error): fatalError("Invalid JuiceMount isolation scope: \(error)")
        }
    }

    private static func inspect() throws -> Scope? {
        let env = ProcessInfo.processInfo.environment
        let flag = env["JM_APP_ISOLATION"]
        let path = env["JM_APP_ISOLATION_ROOT"]
        if flag == nil && path == nil { return nil }
        guard flag == "1", let path, !path.isEmpty,
              path.hasPrefix("/"), path != "/",
              URL(fileURLWithPath: path).standardizedFileURL.path == path,
              URL(fileURLWithPath: path).resolvingSymlinksInPath().path == path,
              env["TMPDIR"] == path + "/tmp",
              (path + "/s.sock").utf8.count <= 103 else {
            throw Failure.invalid("isolation requires a canonical private root and private TMPDIR before launch")
        }
        guard let account = getpwuid(geteuid()) else {
            throw Failure.invalid("cannot resolve the account home for isolation")
        }
        let realHome = String(cString: account.pointee.pw_dir)
        guard env["HOME"] == realHome else {
            throw Failure.invalid("isolation must preserve the account HOME")
        }
        let protected = [realHome + "/Library", realHome + "/.juicemount", realHome + "/.juicefs",
                         "/tmp", "/private/tmp", "/var/tmp", "/Volumes"]
        guard path != realHome,
              !protected.contains(where: { path == $0 || path.hasPrefix($0 + "/") }) else {
            throw Failure.invalid("isolation root overlaps an ordinary state or mount directory")
        }
        try requireOwned(path, directory: true, permissions: 0o700)
        try requireOwned(path + "/tmp", directory: true, permissions: 0o700)
        let marker = path + "/.juicemount-isolated-v1"
        try requireOwned(marker, directory: false, permissions: 0o600)
        guard try String(contentsOfFile: marker, encoding: .utf8) == "JuiceMount isolated state v1\n" else {
            throw Failure.invalid("isolation marker is invalid")
        }
        guard let addr = env["JM_APP_ISOLATION_FUSE_METRICS_ADDR"],
              validLoopback(addr), addr != "127.0.0.1:9568" else {
            throw Failure.invalid("isolation requires a separate FUSE metrics address")
        }
        let scope = Scope(root: URL(fileURLWithPath: path, isDirectory: true), fuseMetricsAddr: addr)
        for child in ["s.sock", "mount-transactions", "preferences.plist", "data", "logs", "spool",
                      "fuse-internal", "volume", ".juicefs", ".juicefs/cache", ".juicefs/juicefs.log",
                      ".juicemount", ".juicemount/offline-intent", "Library", "Library/Logs",
                      "Library/Logs/JuiceMount", "Library/Logs/JuiceMount/stall-dumps",
                      "NativeLibrary", "NativeLibrary/index.json", "NativeLibrary/index.lock",
                      "NativeLibrary/journals"] {
            try scope.requireChild(scope.child(child))
        }
        for name in ["preferences.plist", "credential-metadata.json", "credential-link-authkey.json",
                     "credential-object-key.json", "credential-enrollment.json", "credential-metadata-previous.json",
                     ".native-library-ack-v1"] {
            try scope.checkPrivateFile(scope.child(name))
        }
        // Native delivery credentials are isolation-only, one owned direct
        // leaf per exact profile digest. None may resolve outside this root.
        for name in try FileManager.default.contentsOfDirectory(atPath: path)
            where name.hasPrefix("credential-native-library-") {
            let digest = name.dropFirst("credential-native-library-".count).dropLast(".json".count)
            guard name.hasSuffix(".json"), digest.count == 64,
                  digest.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }) else {
                throw Failure.invalid("an isolated native credential has an invalid name")
            }
            try scope.checkPrivateFile(scope.child(name))
        }
        return scope
    }

    private static func requireOwned(_ path: String, directory: Bool, permissions: Int) throws {
        let attributes = try FileManager.default.attributesOfItem(atPath: path)
        let type = attributes[.type] as? FileAttributeType
        let owner = (attributes[.ownerAccountID] as? NSNumber)?.intValue
        let mode = (attributes[.posixPermissions] as? NSNumber)?.intValue
        guard owner == Int(geteuid()), mode == permissions,
              type == (directory ? .typeDirectory : .typeRegular),
              URL(fileURLWithPath: path).resolvingSymlinksInPath().path == path else {
            throw Failure.invalid("isolation root or marker has unsafe ownership, mode, or type")
        }
    }

    private static func validLoopback(_ value: String) -> Bool {
        let parts = value.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 2, parts[0] == "127.0.0.1",
              let port = Int(parts[1]), (1...65535).contains(port) else { return false }
        return true
    }

    static func validate(_ cfg: NFSBridge.ServerConfig) throws {
        guard let scope = active else { return }
        for path in [cfg.fusePath, cfg.mountPoint, cfg.dbPath, cfg.logFile, cfg.spoolDir] {
            try scope.requireChild(path)
        }
        guard !cfg.redisURL.isEmpty, cfg.redisURL != "redis://127.0.0.1:6379/1",
              validLoopback(cfg.listenAddr), validLoopback(cfg.metricsAddr),
              cfg.listenAddr != "127.0.0.1:11049", cfg.metricsAddr != "127.0.0.1:11050",
              cfg.listenAddr != cfg.metricsAddr,
              cfg.fuseMetricsAddr == scope.fuseMetricsAddr,
              cfg.fuseMetricsAddr != cfg.metricsAddr,
              cfg.fuseMetricsAddr != cfg.listenAddr else {
            throw Failure.invalid("isolated Start requires explicit private backend and distinct loopback ports")
        }
    }
}


// Descriptor-relative operations prevent a replaced state leaf from redirecting
// reads or writes. These files are direct children of the owned private root.
extension AppIsolation.Scope {
    private func withPrivateFile<T>(_ path: String, _ body: (Int32, String) throws -> T) throws -> T {
        try requireChild(path)
        guard URL(fileURLWithPath: path).deletingLastPathComponent().path == root.path else {
            throw AppIsolation.Failure.invalid("private state must be directly under the isolation root")
        }
        let dir = open(root.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard dir >= 0 else { throw AppIsolation.Failure.invalid("private root is unavailable") }
        defer { close(dir) }
        var info = stat()
        guard fstat(dir, &info) == 0, info.st_uid == geteuid(),
              info.st_mode & S_IFMT == S_IFDIR, info.st_mode & 0o777 == 0o700 else {
            throw AppIsolation.Failure.invalid("private root ownership or mode changed")
        }
        return try body(dir, URL(fileURLWithPath: path).lastPathComponent)
    }

    private func validateState(_ info: stat) throws {
        guard info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFREG,
              info.st_mode & 0o777 == 0o600, info.st_nlink == 1 else {
            throw AppIsolation.Failure.invalid("private state must be an owned 0600 regular file")
        }
    }

    private func checkLeaf(_ dir: Int32, _ name: String) throws {
        var info = stat()
        if fstatat(dir, name, &info, AT_SYMLINK_NOFOLLOW) != 0 {
            if errno == ENOENT { return }
            throw AppIsolation.Failure.invalid("private state is unavailable")
        }
        try validateState(info)
    }

    func checkPrivateFile(_ path: String) throws {
        try withPrivateFile(path) { try checkLeaf($0, $1) }
    }

    func readPrivateFile(_ path: String) throws -> Data? {
        try withPrivateFile(path) { dir, name in
            let fd = openat(dir, name, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
            if fd < 0 {
                if errno == ENOENT { return nil }
                throw AppIsolation.Failure.invalid("private state cannot be opened")
            }
            defer { close(fd) }
            var info = stat()
            guard fstat(fd, &info) == 0 else { throw AppIsolation.Failure.invalid("private state cannot be inspected") }
            try validateState(info)
            return try FileHandle(fileDescriptor: fd, closeOnDealloc: false).readToEnd() ?? Data()
        }
    }

    func writePrivateFile(_ data: Data, to path: String) throws {
        try withPrivateFile(path) { dir, name in
            try checkLeaf(dir, name)
            let temporary = ".state-" + UUID().uuidString
            let fd = openat(dir, temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
            guard fd >= 0 else { throw AppIsolation.Failure.invalid("private state cannot be created") }
            defer { close(fd); unlinkat(dir, temporary, 0) }
            let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: false)
            try handle.write(contentsOf: data)
            try handle.synchronize()
            try checkLeaf(dir, name)
            guard renameat(dir, temporary, dir, name) == 0 else {
                throw AppIsolation.Failure.invalid("private state cannot be replaced")
            }
        }
    }

    func removePrivateFile(_ path: String) throws {
        try withPrivateFile(path) { dir, name in
            try checkLeaf(dir, name)
            guard unlinkat(dir, name, 0) == 0 || errno == ENOENT else {
                throw AppIsolation.Failure.invalid("private state cannot be removed")
            }
        }
    }
}

/// The isolated store persists only under the private root. Normal installs
/// keep using UserDefaults.standard and their existing migration behavior.
protocol PreferenceValues {
    func object(forKey key: String) -> Any?
    func string(forKey key: String) -> String?
    func bool(forKey key: String) -> Bool
    func set(_ value: Any?, forKey key: String)
}

extension UserDefaults: PreferenceValues {}

private final class IsolatedPreferences: PreferenceValues {
    private let scope: AppIsolation.Scope
    private let path: String
    private let lock = NSLock()
    private var values: [String: Any]

    init(scope: AppIsolation.Scope) throws {
        self.scope = scope
        path = scope.child("preferences.plist")
        if let data = try scope.readPrivateFile(path) {
            guard let saved = try PropertyListSerialization.propertyList(from: data, format: nil) as? [String: Any] else {
                throw AppIsolation.Failure.invalid("private preferences are malformed")
            }
            values = saved
        } else {
            values = [:]
        }
    }

    func object(forKey key: String) -> Any? {
        lock.lock(); defer { lock.unlock() }
        return values[key]
    }

    func string(forKey key: String) -> String? { object(forKey: key) as? String }
    func bool(forKey key: String) -> Bool { object(forKey: key) as? Bool ?? false }

    func set(_ value: Any?, forKey key: String) {
        lock.lock(); defer { lock.unlock() }
        values[key] = value
        do {
            let data = try PropertyListSerialization.data(fromPropertyList: values,
                                                          format: .binary, options: 0)
            try scope.writePrivateFile(data, to: path)
        } catch {
            fatalError("Unable to persist isolated preferences: \(error)")
        }
    }
}

extension AppIsolation {
    static func isolatedPreferences(for scope: Scope) throws -> any PreferenceValues {
        try IsolatedPreferences(scope: scope)
    }

    static var preferences: any PreferenceValues {
        if let scope = active {
            do { return try isolatedPreferences(for: scope) }
            catch { fatalError("Invalid isolated preferences: \(error)") }
        }
        return UserDefaults.standard
    }
}
