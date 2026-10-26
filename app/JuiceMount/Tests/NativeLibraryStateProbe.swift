import Foundation
import Darwin

#if NATIVE_LIBRARY_PROBE
// Standalone state probe links NativeLibraryState and NativeLibraryWire only.
// Real app isolation and Keychain access are deliberately absent.
enum AppIsolation {
    struct Scope { func child(_ name: String) -> String { name } }
    static let active: Scope? = nil
}

@main
enum NativeLibraryStateProbe {
    static func main() throws {
        let base = NSTemporaryDirectory() + "native-library-state-" + UUID().uuidString
        try FileManager.default.createDirectory(atPath: base, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(atPath: base) }
        let support = base + "/Support"
        try FileManager.default.createDirectory(atPath: support, withIntermediateDirectories: false)
        let parent = support + "/JuiceMount"
        try check(mkdir(parent, 0o755) == 0 && chmod(parent, 0o755) == 0,
                  "preexisting shared 0755 preferences parent")
        NativeLibraryState.testStatePath = support + "/JuiceMount/NativeLibrary"
        defer { NativeLibraryState.testBarrier = nil; NativeLibraryState.testStatePath = nil }
        try check(try NativeLibraryState.openExisting() == nil, "fresh state is absent")
        let state = try NativeLibraryState.createIfAbsent()
        let endpoint = "http://127.0.0.1:18421"
        let lib = "lib_" + String(repeating: "a", count: 32)
        let profile = try state.createProfile(endpoint: endpoint, libraryID: lib, allowLoopbackHTTP: true)
        try check(!FileManager.default.fileExists(atPath: profile.nativeRoot), "app does not pre-create Go's native root")
        try state.markProfileCreated(profile.id) // stands in for a validated native profile-create receipt
        let asset = "asset_" + String(repeating: "b", count: 32)
        let revision = "rev1_" + String(repeating: "c", count: 64)
        let now = Int64(Date().timeIntervalSince1970)
        let expiration = now + 600
        try check(expiration > now && expiration <= now + 900, "new Select stays within catalog expiry bound")
        NativeLibraryState.testBarrier = { $0 != "index_parent" }
        do { _ = try state.beginCopy(profileID: profile.id, assetID: asset, revisionID: revision,
                                     basename: "chosen.png", expiresUnix: expiration, maxBytes: 1 << 30)
             throw NSError(domain: "NativeLibraryStateProbe", code: 5) }
        catch NativeLibraryState.Failure.uncertain { }
        NativeLibraryState.testBarrier = { $0 != "index_recovery_parent" }
        do { _ = try NativeLibraryState.openExisting(); throw NSError(domain: "NativeLibraryStateProbe", code: 6) }
        catch NativeLibraryState.Failure.uncertain { }
        NativeLibraryState.testBarrier = nil
        let opened = try NativeLibraryState.openExisting()!
        try check(chmod(parent, 0o750) == 0, "set safe owned 0750 parent")
        _ = try opened.load()
        try check(chmod(parent, 0o777) == 0, "set unsafe writable parent")
        do { _ = try opened.load(); throw NSError(domain: "NativeLibraryStateProbe", code: 10) }
        catch NativeLibraryState.Failure.unsafeState { }
        try check(chmod(parent, 0o755) == 0, "restore original shared 0755 parent")
        _ = try opened.load()
        let pending = try opened.profile(profile.id).1.copies
        try check(pending.count == 1 && pending[0].selectReceipt == nil,
                  "uncertain post-rename barrier retains exactly one pending ID")
        let copy = pending[0]
        let retried = try opened.profile(profile.id).1.copies[0]
        try check(retried.copyID == copy.copyID && retried.selectBytes == copy.selectBytes &&
                  retried.expiresUnix == expiration, "reopen retries exact ID, bytes and original expiry")
        let selection = "{\"version\":1,\"library_id\":\"\(lib)\",\"selection_id\":\"dsel_\(String(repeating: "d", count: 32))\",\"operation_id\":\"\(copy.operationID)\",\"asset_id\":\"\(asset)\",\"revision_id\":\"\(revision)\",\"source_id\":\"dsrc_\(String(repeating: "e", count: 32))\",\"source_version\":1,\"content\":{\"SHA256\":\"\(String(repeating: "f", count: 64))\",\"Size\":3},\"expires_unix\":\(expiration),\"canceled\":false}"
        let receipt = Data("{\"version\":1,\"copy_id\":\"\(copy.copyID)\",\"selection\":\(selection)}\n".utf8)
        _ = try opened.pinSelection(profileID: profile.id, copyID: copy.copyID, receipt: receipt)
        let derived = profile.nativeRoot + "/copies/" + copy.copyID + "/chosen.png"
        let status = Data("{\"copy_id\":\"\(copy.copyID)\",\"basename\":\"chosen.png\",\"size\":3,\"sha256\":\"\(String(repeating: "f", count: 64))\",\"path\":\(NativeLibraryWire.quote(derived)),\"previously_downloaded\":true}\n".utf8)
        try check(try opened.verifiedURL(profileID: profile.id, copyID: copy.copyID, statusData: status).path == derived,
                  "only exact pinned local receipt derives a path")
        let substituted = Data(String(decoding: status, as: UTF8.self).replacingOccurrences(of: "chosen.png", with: "else.png").utf8)
        do { _ = try opened.verifiedURL(profileID: profile.id, copyID: copy.copyID, statusData: substituted)
             throw NSError(domain: "NativeLibraryStateProbe", code: 2) }
        catch NativeLibraryWire.Failure.invalidReceipt { }
        let displaced = support + "/JuiceMount-displaced"
        var replacedDuringBarrier = false
        NativeLibraryState.testBarrier = { label in
            guard label == "marker_recovery_parent" else { return true }
            guard rename(parent, displaced) == 0, mkdir(parent, 0o700) == 0,
                  rename(displaced + "/NativeLibrary", parent + "/NativeLibrary") == 0 else { return false }
            replacedDuringBarrier = true
            return true
        }
        do { _ = try opened.load(); throw NSError(domain: "NativeLibraryStateProbe", code: 9) }
        catch NativeLibraryState.Failure.unsafeState { }
        NativeLibraryState.testBarrier = nil
        try check(replacedDuringBarrier, "parent substitution reached recovery barrier")
        try check(rename(parent + "/NativeLibrary", displaced + "/NativeLibrary") == 0 &&
                  rmdir(parent) == 0 && rename(displaced, parent) == 0,
                  "restore original acknowledged parent and root after refusal")
        _ = try opened.load()
        try FileManager.default.removeItem(atPath: NativeLibraryState.testStatePath!)
        do { _ = try NativeLibraryState.openExisting(); throw NSError(domain: "NativeLibraryStateProbe", code: 3) }
        catch NativeLibraryState.Failure.missingIndex { }
        do { _ = try NativeLibraryState.createIfAbsent(); throw NSError(domain: "NativeLibraryStateProbe", code: 4) }
        catch NativeLibraryState.Failure.missingIndex { }
        let support2 = base + "/Support2"
        try FileManager.default.createDirectory(atPath: support2, withIntermediateDirectories: false)
        NativeLibraryState.testStatePath = support2 + "/JuiceMount/NativeLibrary"
        NativeLibraryState.testBarrier = { $0 != "marker_parent" }
        do { _ = try NativeLibraryState.createIfAbsent(); throw NSError(domain: "NativeLibraryStateProbe", code: 7) }
        catch NativeLibraryState.Failure.uncertain { }
        NativeLibraryState.testBarrier = { $0 != "marker_recovery_parent" }
        do { _ = try NativeLibraryState.openExisting(); throw NSError(domain: "NativeLibraryStateProbe", code: 8) }
        catch NativeLibraryState.Failure.uncertain { }
        NativeLibraryState.testBarrier = nil
        _ = try NativeLibraryState.openExisting()!
        let support3 = base + "/Support3"
        try FileManager.default.createDirectory(atPath: support3, withIntermediateDirectories: false)
        let unsafeParent = support3 + "/JuiceMount"
        try check(mkdir(unsafeParent, 0o700) == 0 && chmod(unsafeParent, 0o777) == 0,
                  "unsafe parent fixture")
        NativeLibraryState.testStatePath = unsafeParent + "/NativeLibrary"
        do { _ = try NativeLibraryState.createIfAbsent(); throw NSError(domain: "NativeLibraryStateProbe", code: 11) }
        catch NativeLibraryState.Failure.unsafeState { }
        try check(!FileManager.default.fileExists(atPath: unsafeParent + "/NativeLibrary"),
                  "unsafe shared parent refuses first initialization")
        print("native library state probe passed")
    }
    private static func check(_ value: @autoclosure () throws -> Bool, _ label: String) throws {
        if try !value() { throw NSError(domain: "NativeLibraryStateProbe", code: 1,
                                       userInfo: [NSLocalizedDescriptionKey: label]) }
    }
}
#endif
