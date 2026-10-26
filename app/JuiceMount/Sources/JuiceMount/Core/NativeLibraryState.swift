import CryptoKit
import Darwin
import Foundation
import Security

/// The app's small intent index is separate from the Go-owned journal. It
/// gives the UI stable copy/operation IDs; it never grants byte custody or a
/// raw path to a caller. All writes finish file and parent-directory barriers
/// before a native helper may be launched.
final class NativeLibraryState {
    enum Failure: Error {
        case unavailable, unsafeState, missingIndex, corruptIndex, conflict, limit, uncertain
    }

    struct Copy: Codable, Equatable, Identifiable {
        let copyID: String
        let operationID: String
        let assetID: String
        let revisionID: String
        let basename: String
        let expiresUnix: Int64
        let maxBytes: Int64
        let selectBytes: Data
        var selectReceipt: Data?
        var id: String { copyID }
    }
    struct ProfileEntry: Codable, Equatable, Identifiable {
        let profileID: String
        let endpoint: String
        let libraryID: String
        let allowLoopbackHTTP: Bool
        let rootName: String
        var created: Bool
        var copies: [Copy]
        var id: String { profileID }
    }
    private struct Index: Codable {
        let version: Int
        var profiles: [ProfileEntry]
    }

    private static let processLock = NSLock()
    private static let maxIndexBytes = 16 << 20
    private static let maxCopies = 4096
    private static let markerName = ".native-library-ack-v1"
    private static let markerBytes = Data("JuiceMount Native Library acknowledged v1\n".utf8)
    #if NATIVE_LIBRARY_PROBE
    static var testStatePath: String?
    static var testBarrier: ((String) -> Bool)?
    #endif
    private static func sync(_ fd: Int32, _ label: String) -> Bool {
        #if NATIVE_LIBRARY_PROBE
        if testBarrier?(label) == false { return false }
        #endif
        return fsync(fd) == 0
    }
    private let rootPath: String
    private let originalDevice: dev_t
    private let originalInode: ino_t

    private init(rootPath: String, info: stat) {
        self.rootPath = rootPath
        originalDevice = info.st_dev
        originalInode = info.st_ino
    }

    static func statePath() throws -> String {
        #if NATIVE_LIBRARY_PROBE
        if let testStatePath { return testStatePath }
        #endif
        if let isolation = AppIsolation.active {
            return isolation.child("NativeLibrary")
        }
        guard let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
            throw Failure.unavailable
        }
        return support.appendingPathComponent("JuiceMount/NativeLibrary").path
    }

    /// Opening an acknowledged root never creates or repairs it. An absent
    /// root is distinct from a corrupt/missing index inside an existing root.
    static func openExisting() throws -> NativeLibraryState? {
        let path = try statePath()
        let acknowledged = try markerExists(path)
        var info = stat()
        if lstat(path, &info) != 0 {
            if errno == ENOENT && !acknowledged { return nil }
            if errno == ENOENT { throw Failure.missingIndex }
            throw Failure.unavailable
        }
        guard acknowledged else { throw Failure.missingIndex }
        try ownedDirectory(info)
        let state = NativeLibraryState(rootPath: path, info: info)
        _ = try state.load()
        return state
    }

    /// This is an explicit first-profile action only. Existing malformed
    /// roots are refused, never replaced with a fresh quota namespace.
    static func createIfAbsent() throws -> NativeLibraryState {
        if let existing = try openExisting() { return existing }
        let path = try statePath()
        let parent = URL(fileURLWithPath: path).deletingLastPathComponent().path
        if AppIsolation.active == nil {
            let support = URL(fileURLWithPath: parent).deletingLastPathComponent().path
            let supportFD = open(support, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
            guard supportFD >= 0 else { throw Failure.unavailable }
            let name = URL(fileURLWithPath: parent).lastPathComponent
            if mkdirat(supportFD, name, 0o700) != 0 && errno != EEXIST {
                _ = Darwin.close(supportFD); throw Failure.unavailable
            }
            guard fsync(supportFD) == 0 else { _ = Darwin.close(supportFD); throw Failure.uncertain }
            guard Darwin.close(supportFD) == 0 else { throw Failure.uncertain }
        }
        let parentFD = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard parentFD >= 0 else { throw Failure.unavailable }
        var closeParent = true
        defer { if closeParent { _ = Darwin.close(parentFD) } }
        var parentInfo = stat()
        guard fstat(parentFD, &parentInfo) == 0 else { throw Failure.unsafeState }
        try ownedSafeParent(parentInfo)
        let leaf = URL(fileURLWithPath: path).lastPathComponent
        guard mkdirat(parentFD, leaf, 0o700) == 0 else {
            if errno == EEXIST {
                guard let existing = try openExisting() else { throw Failure.unsafeState }
                return existing
            }
            throw Failure.unavailable
        }
        guard fsync(parentFD) == 0 else { throw Failure.uncertain }
        guard Darwin.close(parentFD) == 0 else { closeParent = false; throw Failure.uncertain }
        closeParent = false
        let root = open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard root >= 0 else { throw Failure.uncertain }
        var closeRoot = true
        defer { if closeRoot { _ = Darwin.close(root) } }
        var info = stat()
        guard fstat(root, &info) == 0 else { throw Failure.uncertain }
        try ownedDirectory(info)
        guard mkdirat(root, "journals", 0o700) == 0 else { throw Failure.uncertain }
        let lock = openat(root, "index.lock", O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard lock >= 0 else { throw Failure.uncertain }
        guard fsync(lock) == 0 else { _ = Darwin.close(lock); throw Failure.uncertain }
        guard Darwin.close(lock) == 0 else { throw Failure.uncertain }
        let state = NativeLibraryState(rootPath: path, info: info)
        try state.writeIndex(Index(version: 1, profiles: []), root: root, replacing: false)
        guard fsync(root) == 0 else { throw Failure.uncertain }
        guard Darwin.close(root) == 0 else { closeRoot = false; throw Failure.uncertain }
        closeRoot = false
        try writeMarker(path, expectedRoot: info)
        _ = try state.load()
        return state
    }

    private static func markerExists(_ rootPath: String) throws -> Bool {
        let parent = URL(fileURLWithPath: rootPath).deletingLastPathComponent().path
        let dir = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        if dir < 0 {
            if errno == ENOENT { return false }
            throw Failure.unsafeState
        }
        defer { _ = Darwin.close(dir) }
        let fd = openat(dir, markerName, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
        if fd < 0 {
            if errno == ENOENT { return false }
            throw Failure.unsafeState
        }
        defer { _ = Darwin.close(fd) }
        var info = stat()
        guard fstat(fd, &info) == 0 else { throw Failure.unsafeState }
        try ownedFile(info)
        guard info.st_size == Int64(markerBytes.count) else { throw Failure.unsafeState }
        var bytes = [UInt8](repeating: 0, count: markerBytes.count)
        guard Darwin.read(fd, &bytes, bytes.count) == bytes.count,
              Data(bytes) == markerBytes else { throw Failure.unsafeState }
        return true
    }
    private static func writeMarker(_ rootPath: String, expectedRoot: stat) throws {
        let parent = URL(fileURLWithPath: rootPath).deletingLastPathComponent().path
        let rootName = URL(fileURLWithPath: rootPath).lastPathComponent
        let dir = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard dir >= 0 else { throw Failure.uncertain }
        var dirOpen = true
        defer { if dirOpen { _ = Darwin.close(dir) } }
        var parentHeld = stat(), parentLive = stat(), rootLive = stat()
        guard fstat(dir, &parentHeld) == 0, lstat(parent, &parentLive) == 0 else { throw Failure.unsafeState }
        try ownedSafeParent(parentHeld)
        guard parentHeld.st_dev == parentLive.st_dev, parentHeld.st_ino == parentLive.st_ino,
              fstatat(dir, rootName, &rootLive, AT_SYMLINK_NOFOLLOW) == 0,
              rootLive.st_dev == expectedRoot.st_dev, rootLive.st_ino == expectedRoot.st_ino else { throw Failure.unsafeState }
        let fd = openat(dir, markerName, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard fd >= 0 else { throw Failure.uncertain }
        let wrote = markerBytes.withUnsafeBytes { ptr in
            Darwin.write(fd, ptr.baseAddress, ptr.count) == ptr.count
        }
        guard wrote, sync(fd, "marker_written") else { _ = Darwin.close(fd); throw Failure.uncertain }
        guard Darwin.close(fd) == 0, sync(dir, "marker_parent") else { throw Failure.uncertain }
        guard lstat(parent, &parentLive) == 0,
              parentHeld.st_dev == parentLive.st_dev, parentHeld.st_ino == parentLive.st_ino,
              fstatat(dir, rootName, &rootLive, AT_SYMLINK_NOFOLLOW) == 0,
              rootLive.st_dev == expectedRoot.st_dev, rootLive.st_ino == expectedRoot.st_ino else { throw Failure.unsafeState }
        guard Darwin.close(dir) == 0 else { dirOpen = false; throw Failure.uncertain }
        dirOpen = false
    }
    /// Readability after a failed file/parent sync is not a durable
    /// acknowledgement. Repeat both barriers under the current index flock
    /// before any recovered profile or copy can launch a helper.
    private func durableMarker() throws {
        let parent = URL(fileURLWithPath: rootPath).deletingLastPathComponent().path
        let rootName = URL(fileURLWithPath: rootPath).lastPathComponent
        let dir = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard dir >= 0 else { throw Failure.unsafeState }
        var dirOpen = true
        defer { if dirOpen { _ = Darwin.close(dir) } }
        var parentHeld = stat(), parentLive = stat(), rootLive = stat()
        guard fstat(dir, &parentHeld) == 0, lstat(parent, &parentLive) == 0 else { throw Failure.unsafeState }
        try Self.ownedSafeParent(parentHeld)
        guard parentHeld.st_dev == parentLive.st_dev, parentHeld.st_ino == parentLive.st_ino,
              fstatat(dir, rootName, &rootLive, AT_SYMLINK_NOFOLLOW) == 0,
              rootLive.st_dev == originalDevice, rootLive.st_ino == originalInode,
              rootLive.st_mode & S_IFMT == S_IFDIR else { throw Failure.unsafeState }
        let fd = openat(dir, Self.markerName, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw Failure.missingIndex }
        var fileOpen = true
        defer { if fileOpen { _ = Darwin.close(fd) } }
        var held = stat(), live = stat()
        guard fstat(fd, &held) == 0,
              fstatat(dir, Self.markerName, &live, AT_SYMLINK_NOFOLLOW) == 0,
              held.st_dev == live.st_dev, held.st_ino == live.st_ino,
              held.st_size == Int64(Self.markerBytes.count) else { throw Failure.unsafeState }
        try Self.ownedFile(held)
        var bytes = [UInt8](repeating: 0, count: Self.markerBytes.count)
        guard Darwin.read(fd, &bytes, bytes.count) == bytes.count,
              Data(bytes) == Self.markerBytes, Self.sync(fd, "marker_recovery_file") else { throw Failure.uncertain }
        guard Darwin.close(fd) == 0 else { fileOpen = false; throw Failure.uncertain }
        fileOpen = false
        guard Self.sync(dir, "marker_recovery_parent") else { throw Failure.uncertain }
        guard lstat(parent, &parentLive) == 0,
              parentHeld.st_dev == parentLive.st_dev, parentHeld.st_ino == parentLive.st_ino,
              fstatat(dir, rootName, &rootLive, AT_SYMLINK_NOFOLLOW) == 0,
              rootLive.st_dev == originalDevice, rootLive.st_ino == originalInode,
              fstatat(dir, Self.markerName, &live, AT_SYMLINK_NOFOLLOW) == 0,
              held.st_dev == live.st_dev, held.st_ino == live.st_ino else { throw Failure.unsafeState }
        guard Darwin.close(dir) == 0 else { dirOpen = false; throw Failure.uncertain }
        dirOpen = false
    }

    private static func ownedDirectory(_ info: stat) throws {
        guard info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFDIR,
              info.st_mode & 0o777 == 0o700 else { throw Failure.unsafeState }
    }
    /// Application Support/JuiceMount is shared with existing preferences and
    /// may already be 0755. The NativeLibrary child remains private 0700.
    private static func ownedSafeParent(_ info: stat) throws {
        guard info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFDIR,
              info.st_mode & 0o022 == 0 else { throw Failure.unsafeState }
    }
    private static func ownedFile(_ info: stat) throws {
        guard info.st_uid == geteuid(), info.st_mode & S_IFMT == S_IFREG,
              info.st_mode & 0o777 == 0o600, info.st_nlink == 1 else { throw Failure.unsafeState }
    }
    private func sameRoot(_ root: Int32) throws {
        var held = stat(), live = stat()
        guard fstat(root, &held) == 0, lstat(rootPath, &live) == 0 else { throw Failure.unsafeState }
        try Self.ownedDirectory(held)
        guard held.st_dev == originalDevice, held.st_ino == originalInode,
              live.st_dev == held.st_dev, live.st_ino == held.st_ino,
              live.st_mode & S_IFMT == S_IFDIR else { throw Failure.unsafeState }
    }
    private func checkLeaf(_ root: Int32, _ name: String, expected: stat? = nil) throws -> stat {
        var value = stat()
        guard fstatat(root, name, &value, AT_SYMLINK_NOFOLLOW) == 0 else {
            if errno == ENOENT { throw Failure.missingIndex }
            throw Failure.unsafeState
        }
        try Self.ownedFile(value)
        if let expected, (value.st_dev != expected.st_dev || value.st_ino != expected.st_ino) {
            throw Failure.unsafeState
        }
        return value
    }
    private func withRoot<T>(_ action: (Int32) throws -> T) throws -> T {
        Self.processLock.lock()
        defer { Self.processLock.unlock() }
        let root = open(rootPath, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard root >= 0 else { throw Failure.unsafeState }
        var rootOpen = true
        defer { if rootOpen { _ = Darwin.close(root) } }
        do {
            guard try Self.markerExists(rootPath) else { throw Failure.missingIndex }
            try sameRoot(root)
            _ = try checkLeaf(root, "index.lock")
            let lockFD = openat(root, "index.lock", O_RDWR | O_NOFOLLOW | O_CLOEXEC)
            guard lockFD >= 0 else { throw Failure.unsafeState }
            var lockOpen = true
            defer { if lockOpen { _ = Darwin.close(lockFD) } }
            do {
                var lockInfo = stat()
                guard fstat(lockFD, &lockInfo) == 0 else { throw Failure.unsafeState }
                _ = try checkLeaf(root, "index.lock", expected: lockInfo)
                guard flock(lockFD, LOCK_EX | LOCK_NB) == 0 else { throw Failure.conflict }
                try sameRoot(root)
                try durableMarker()
                let result = try action(root)
                try sameRoot(root)
                _ = try checkLeaf(root, "index.lock", expected: lockInfo)
                guard flock(lockFD, LOCK_UN) == 0 else { throw Failure.uncertain }
                guard Darwin.close(lockFD) == 0 else { lockOpen = false; throw Failure.uncertain }
                lockOpen = false
                guard Darwin.close(root) == 0 else { rootOpen = false; throw Failure.uncertain }
                rootOpen = false
                return result
            } catch {
                if lockOpen { _ = flock(lockFD, LOCK_UN) }
                throw error
            }
        } catch { throw error }
    }
    private func readIndex(_ root: Int32) throws -> Index {
        let before = try checkLeaf(root, "index.json")
        guard before.st_size > 0, before.st_size <= Int64(Self.maxIndexBytes) else { throw Failure.corruptIndex }
        let fd = openat(root, "index.json", O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw Failure.unsafeState }
        var fdOpen = true
        do {
            var held = stat()
            guard fstat(fd, &held) == 0 else { throw Failure.unsafeState }
            try Self.ownedFile(held)
            guard held.st_dev == before.st_dev, held.st_ino == before.st_ino,
                  held.st_size == before.st_size else { throw Failure.unsafeState }
            var bytes = Data(count: Int(held.st_size))
            let count = bytes.withUnsafeMutableBytes { ptr -> Int in
                guard let base = ptr.baseAddress else { return -1 }
                var position = 0
                while position < ptr.count {
                    let n = Darwin.read(fd, base.advanced(by: position), ptr.count - position)
                    if n < 0 && errno == EINTR { continue }
                    if n <= 0 { return -1 }
                    position += n
                }
                return position
            }
            guard count == bytes.count else { throw Failure.unsafeState }
            _ = try checkLeaf(root, "index.json", expected: held)
            guard Self.sync(fd, "index_recovery_file") else { throw Failure.uncertain }
            guard Darwin.close(fd) == 0 else { fdOpen = false; throw Failure.uncertain }
            fdOpen = false
            guard Self.sync(root, "index_recovery_parent") else { throw Failure.uncertain }
            try sameRoot(root)
            _ = try checkLeaf(root, "index.json", expected: held)
            return try decodedIndex(bytes)
        } catch {
            if fdOpen { _ = Darwin.close(fd) }
            throw error
        }
    }
    private func decodedIndex(_ bytes: Data) throws -> Index {
        guard let decoded = try? JSONDecoder().decode(Index.self, from: bytes) else { throw Failure.corruptIndex }
        try validate(decoded, bytes: bytes)
        return decoded
    }
    private func encode(_ index: Index) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        let data = try encoder.encode(index)
        guard data.count <= Self.maxIndexBytes else { throw Failure.limit }
        return data
    }
    private func validate(_ index: Index, bytes: Data) throws {
        guard index.version == 1, try encode(index) == bytes else { throw Failure.corruptIndex }
        var IDs = Set<String>()
        var copies = Set<String>()
        for entry in index.profiles {
            let rootName = "journals/" + entry.profileID
            guard IDs.insert(entry.profileID).inserted, entry.rootName == rootName,
                  entry.copies.count <= Self.maxCopies else { throw Failure.corruptIndex }
            let profile = try NativeLibraryWire.Profile(endpoint: entry.endpoint, libraryID: entry.libraryID,
                                                        nativeRoot: rootPath + "/" + rootName,
                                                        allowLoopbackHTTP: entry.allowLoopbackHTTP)
            guard profile.id == entry.profileID else { throw Failure.corruptIndex }
            for copy in entry.copies {
                guard copies.insert(copy.copyID).inserted,
                      NativeLibraryWire.lowerHexID(copy.copyID, prefix: "copy_", digits: 32),
                      NativeLibraryWire.validBasename(copy.basename) else { throw Failure.corruptIndex }
                let request = NativeLibraryWire.SelectRequest(operationID: copy.operationID,
                    assetID: copy.assetID, mode: "exact", revisionID: copy.revisionID,
                    expiresUnix: copy.expiresUnix, maxBytes: copy.maxBytes)
                guard try NativeLibraryWire.select(request, profile: profile) == copy.selectBytes else {
                    throw Failure.corruptIndex
                }
                if let receipt = copy.selectReceipt {
                    _ = try NativeLibraryWire.selectReceipt(receipt, profile: profile,
                                                             copyID: copy.copyID, request: request)
                }
            }
        }
        guard copies.count <= Self.maxCopies else { throw Failure.corruptIndex }
    }
    private func writeIndex(_ index: Index, root: Int32, replacing: Bool) throws {
        let bytes = try encode(index)
        try validate(index, bytes: bytes)
        if replacing { _ = try checkLeaf(root, "index.json") }
        let temporary = ".index-" + UUID().uuidString
        let fd = openat(root, temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard fd >= 0 else { throw Failure.uncertain }
        var closed = false
        var renamed = false
        defer {
            if !closed { _ = Darwin.close(fd) }
            if !renamed { _ = unlinkat(root, temporary, 0) }
        }
        let wrote = bytes.withUnsafeBytes { ptr -> Bool in
            guard let base = ptr.baseAddress else { return false }
            var position = 0
            while position < ptr.count {
                let n = Darwin.write(fd, base.advanced(by: position), ptr.count - position)
                if n < 0 && errno == EINTR { continue }
                if n <= 0 { return false }
                position += n
            }
            return true
        }
        guard wrote, Self.sync(fd, "index_written") else { throw Failure.uncertain }
        guard Darwin.close(fd) == 0 else { closed = true; throw Failure.uncertain }
        closed = true
        if replacing { _ = try checkLeaf(root, "index.json") }
        try sameRoot(root)
        guard renameat(root, temporary, root, "index.json") == 0 else { throw Failure.uncertain }
        renamed = true
        guard Self.sync(root, "index_parent") else { throw Failure.uncertain }
        try sameRoot(root)
    }

    func load() throws -> [ProfileEntry] {
        try withRoot { root in
            let index = try readIndex(root)
            return index.profiles
        }
    }
    private func update<T>(_ action: (inout Index, Int32) throws -> T) throws -> T {
        try withRoot { root in
            var index = try readIndex(root)
            let result = try action(&index, root)
            try writeIndex(index, root: root, replacing: true)
            return result
        }
    }
    func profile(_ id: String) throws -> (NativeLibraryWire.Profile, ProfileEntry) {
        let (profile, entry) = try retainedProfile(id)
        guard entry.created else { throw Failure.conflict }
        return (profile, entry)
    }
    func retainedProfile(_ id: String) throws -> (NativeLibraryWire.Profile, ProfileEntry) {
        let entries = try load()
        guard let entry = entries.first(where: { $0.profileID == id }) else { throw Failure.conflict }
        return (try NativeLibraryWire.Profile(endpoint: entry.endpoint, libraryID: entry.libraryID,
                    nativeRoot: rootPath + "/" + entry.rootName,
                    allowLoopbackHTTP: entry.allowLoopbackHTTP), entry)
    }
    enum PendingRoot: Equatable { case absent, present }
    func pendingRoot(_ id: String) throws -> PendingRoot {
        try withRoot { root in
            let index = try readIndex(root)
            guard let entry = index.profiles.first(where: { $0.profileID == id }), !entry.created,
                  entry.rootName == "journals/" + id else { throw Failure.conflict }
            let journals = openat(root, "journals", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
            guard journals >= 0 else { throw Failure.unsafeState }
            var journalsOpen = true
            defer { if journalsOpen { _ = Darwin.close(journals) } }
            var info = stat()
            guard fstat(journals, &info) == 0 else { throw Failure.unsafeState }
            try Self.ownedDirectory(info)
            var leaf = stat()
            if fstatat(journals, id, &leaf, AT_SYMLINK_NOFOLLOW) != 0 {
                let absent = errno == ENOENT
                guard Darwin.close(journals) == 0 else { journalsOpen = false; throw Failure.uncertain }
                journalsOpen = false
                if absent { return .absent }
                throw Failure.unsafeState
            }
            try Self.ownedDirectory(leaf)
            guard Darwin.close(journals) == 0 else { journalsOpen = false; throw Failure.uncertain }
            journalsOpen = false
            return .present
        }
    }
    func createProfile(endpoint: String, libraryID: String, allowLoopbackHTTP: Bool) throws -> NativeLibraryWire.Profile {
        // The root name is a pure digest of the exact canonical tuple.
        let provisional = try NativeLibraryWire.Profile(endpoint: endpoint, libraryID: libraryID,
                             nativeRoot: rootPath + "/journals/provisional", allowLoopbackHTTP: allowLoopbackHTTP)
        let profile = try NativeLibraryWire.Profile(endpoint: endpoint, libraryID: libraryID,
                             nativeRoot: rootPath + "/journals/" + provisional.id, allowLoopbackHTTP: allowLoopbackHTTP)
        return try update { index, _ in
            guard !index.profiles.contains(where: { $0.profileID == profile.id }) else { throw Failure.conflict }
            // Go CreateNativeRoot owns this leaf and refuses adoption. The
            // app only persists its stable name before that explicit launch.
            index.profiles.append(ProfileEntry(profileID: profile.id, endpoint: endpoint,
                    libraryID: libraryID, allowLoopbackHTTP: allowLoopbackHTTP,
                    rootName: "journals/" + profile.id, created: false, copies: []))
            return profile
        }
    }
    func markProfileCreated(_ id: String) throws {
        try update { index, _ in
            guard let slot = index.profiles.firstIndex(where: { $0.profileID == id }) else { throw Failure.conflict }
            index.profiles[slot].created = true
        }
    }
    func beginCopy(profileID: String, assetID: String, revisionID: String,
                   basename: String, expiresUnix: Int64, maxBytes: Int64) throws -> Copy {
        return try update { index, _ in
            guard let slot = index.profiles.firstIndex(where: { $0.profileID == profileID }),
                  index.profiles[slot].created, NativeLibraryWire.validBasename(basename),
                  index.profiles.reduce(0, { $0 + $1.copies.count }) < Self.maxCopies else { throw Failure.limit }
            let entry = index.profiles[slot]
            let profile = try NativeLibraryWire.Profile(endpoint: entry.endpoint, libraryID: entry.libraryID,
                        nativeRoot: rootPath + "/" + entry.rootName,
                        allowLoopbackHTTP: entry.allowLoopbackHTTP)
            let copyID = "copy_" + (try Self.randomHex(16))
            let opID = "native_" + (try Self.randomHex(16))
            let request = NativeLibraryWire.SelectRequest(operationID: opID, assetID: assetID,
                            mode: "exact", revisionID: revisionID, expiresUnix: expiresUnix, maxBytes: maxBytes)
            let bytes = try NativeLibraryWire.select(request, profile: profile)
            let copy = Copy(copyID: copyID, operationID: opID, assetID: assetID,
                            revisionID: revisionID, basename: basename, expiresUnix: expiresUnix,
                            maxBytes: maxBytes, selectBytes: bytes, selectReceipt: nil)
            index.profiles[slot].copies.append(copy)
            return copy
        }
    }
    func pinSelection(profileID: String, copyID: String, receipt: Data) throws -> Copy {
        try update { index, _ in
            guard let p = index.profiles.firstIndex(where: { $0.profileID == profileID }),
                  let c = index.profiles[p].copies.firstIndex(where: { $0.copyID == copyID }) else { throw Failure.conflict }
            let entry = index.profiles[p]
            let copy = entry.copies[c]
            let profile = try NativeLibraryWire.Profile(endpoint: entry.endpoint, libraryID: entry.libraryID,
                        nativeRoot: rootPath + "/" + entry.rootName,
                        allowLoopbackHTTP: entry.allowLoopbackHTTP)
            let request = NativeLibraryWire.SelectRequest(operationID: copy.operationID, assetID: copy.assetID,
                            mode: "exact", revisionID: copy.revisionID, expiresUnix: copy.expiresUnix,
                            maxBytes: copy.maxBytes)
            _ = try NativeLibraryWire.selectReceipt(receipt, profile: profile, copyID: copyID, request: request)
            if let existing = copy.selectReceipt, existing != receipt { throw Failure.conflict }
            index.profiles[p].copies[c].selectReceipt = receipt
            return index.profiles[p].copies[c]
        }
    }
    func selectedCopy(profileID: String, copyID: String) throws -> (NativeLibraryWire.Profile, Copy, NativeLibraryWire.SelectReceipt) {
        let (profile, entry) = try self.profile(profileID)
        guard let copy = entry.copies.first(where: { $0.copyID == copyID }), let raw = copy.selectReceipt else {
            throw Failure.conflict
        }
        let request = NativeLibraryWire.SelectRequest(operationID: copy.operationID,
            assetID: copy.assetID, mode: "exact", revisionID: copy.revisionID,
            expiresUnix: copy.expiresUnix, maxBytes: copy.maxBytes)
        return (profile, copy, try NativeLibraryWire.selectReceipt(raw, profile: profile,
                                                                    copyID: copyID, request: request))
    }
    func verifiedURL(profileID: String, copyID: String, statusData: Data) throws -> URL {
        let (profile, copy, pin) = try selectedCopy(profileID: profileID, copyID: copyID)
        let status = try NativeLibraryWire.localStatus(statusData, copyID: copyID, basename: copy.basename)
        let expected = profile.nativeRoot + "/copies/" + copyID + "/" + copy.basename
        guard status.size == pin.selection.content.size,
              status.sha256 == pin.selection.content.sha256, status.path == expected else { throw Failure.unsafeState }
        // Go's offline copy-status has re-opened/hashed the native inode; no
        // server-origin path or stale UI projection becomes Finder authority.
        return URL(fileURLWithPath: expected)
    }
    private static func randomHex(_ count: Int) throws -> String {
        var bytes = [UInt8](repeating: 0, count: count)
        let status = SecRandomCopyBytes(kSecRandomDefault, count, &bytes)
        guard status == errSecSuccess else { throw Failure.unavailable }
        return bytes.map { String(format: "%02x", $0) }.joined()
    }
}
