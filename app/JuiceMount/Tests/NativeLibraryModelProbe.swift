import AppKit
import Darwin
import Foundation

#if NATIVE_LIBRARY_PROBE
// Compiled separately from the state/core probes with the real model, state,
// wire and helper sources. Only the child reply and Keychain are replaced.
enum AppIsolation {
    struct Scope { func child(_ name: String) -> String { name } }
    static let active: Scope? = nil
}
struct NativeLibraryCredentials {
    enum Failure: Error { case inaccessible }
    enum Lookup { case absent, present(Data) }
    func read(profileID: String) throws -> Lookup { .absent }
    func save(_ bearer: Data, profileID: String) throws {}
    func remove(profileID: String) throws {}
}

@main
@MainActor
enum NativeLibraryModelProbe {
    static func main() async throws {
        let base = NSTemporaryDirectory() + "native-library-model-" + UUID().uuidString
        try FileManager.default.createDirectory(atPath: base, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(atPath: base) }
        let support = base + "/Support"
        try FileManager.default.createDirectory(atPath: support, withIntermediateDirectories: false)
        NativeLibraryState.testStatePath = support + "/JuiceMount/NativeLibrary"
        defer {
            NativeLibraryModel.testRun = nil
            NativeLibraryModel.testBearer = nil
            NativeLibraryState.testStatePath = nil
        }
        guard let version = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String,
              let commit = Bundle.main.object(forInfoDictionaryKey: "JMBuildCommit") as? String else {
            throw failure("probe bundle identity is absent")
        }
        let state = try NativeLibraryState.createIfAbsent()
        let libraryID = "lib_" + String(repeating: "a", count: 32)
        let first = try state.createProfile(endpoint: "http://127.0.0.1:18421", libraryID: libraryID,
                                            allowLoopbackHTTP: true)
        let second = try state.createProfile(endpoint: "http://127.0.0.1:18422", libraryID: libraryID,
                                             allowLoopbackHTTP: true)
        try state.markProfileCreated(first.id)
        try state.markProfileCreated(second.id)
        let asset = "asset_" + String(repeating: "b", count: 32)
        let revA = "rev1_" + String(repeating: "c", count: 64)
        let revB = "rev1_" + String(repeating: "d", count: 64)
        var calls: [String] = []
        var selectInputs: [Data] = []
        var observedExpiry: Int64 = 0
        var searchOrdinal = 0
        var allowOpen = true
        var mismatchVersion = false
        NativeLibraryModel.testBearer = Data(("jmc1_" + String(repeating: "e", count: 32) + "." +
                                              String(repeating: "f", count: 64)).utf8)
        NativeLibraryModel.testRun = { command, profile, input in
            switch command {
            case .version:
                calls.append("version")
                let sent = mismatchVersion ? "wrong" : commit
                return Data("{\"version\":1,\"product_version\":\(NativeLibraryWire.quote(version)),\"commit\":\(NativeLibraryWire.quote(sent))}\n".utf8)
            case .profileOpen:
                calls.append("open:" + profile.id)
                return Data("{\"version\":1,\"opened\":\(allowOpen ? "true" : "false")}\n".utf8)
            case .profileCreate:
                calls.append("create:" + profile.id)
                return Data("{\"version\":1,\"created\":true}\n".utf8)
            case .search:
                calls.append("search:" + profile.id)
                searchOrdinal += 1
                let rev = searchOrdinal == 1 ? revA : revB
                return Data("{\"version\":2,\"kind\":\"search\",\"items\":[{\"asset_id\":\"\(asset)\",\"label\":\"Photo\",\"head_revision_id\":\"\(rev)\",\"reasons\":[\"stable_id\"]}],\"matched_count\":1,\"count_scope\":\"frozen_window\",\"window_complete\":true,\"next_page_token\":null,\"sort_version\":\"discovery2\"}\n".utf8)
            case .select:
                calls.append("select:" + profile.id)
                selectInputs.append(input)
                let envelope = try JSONSerialization.jsonObject(with: input) as? [String: Any]
                let request = envelope?["request"] as? [String: Any]
                guard let operationID = request?["operation_id"] as? String,
                      let selectedAsset = request?["asset_id"] as? String,
                      let selectedRevision = request?["revision_id"] as? String,
                      let expiry = (request?["expires_unix"] as? NSNumber)?.int64Value,
                      case .select(let copyID, _) = command else { throw failure("bad fixed Select envelope") }
                observedExpiry = expiry
                if selectInputs.count == 1 {
                    throw NativeLibraryHelper.Failure.childFailure(code: "unavailable", httpStatus: nil, uncertain: false)
                }
                let selection = "{\"version\":1,\"library_id\":\"\(profile.libraryID)\",\"selection_id\":\"dsel_\(String(repeating: "1", count: 32))\",\"operation_id\":\"\(operationID)\",\"asset_id\":\"\(selectedAsset)\",\"revision_id\":\"\(selectedRevision)\",\"source_id\":\"dsrc_\(String(repeating: "2", count: 32))\",\"source_version\":1,\"content\":{\"SHA256\":\"\(String(repeating: "3", count: 64))\",\"Size\":3},\"expires_unix\":\(expiry),\"canceled\":false}"
                return Data("{\"version\":1,\"copy_id\":\"\(copyID)\",\"selection\":\(selection)}\n".utf8)
            default: throw failure("unexpected helper operation")
            }
        }
        let model = NativeLibraryModel()
        await model.selectProfile(first.id)
        try check(model.activeProfileID == first.id, "saved created profile opened before activation")
        await model.search("photo")
        guard let firstAsset = model.page?.items.first, let choiceA = model.choice(for: firstAsset) else {
            throw failure("first selected revision absent")
        }
        await model.search("other")
        try check(model.page?.items.first?.headRevisionID == revB, "second search changed visible head")
        let before = Int64(Date().timeIntervalSince1970)
        await model.pin(choice: choiceA, basename: "A.png")
        let pending = try state.profile(first.id).1.copies
        try check(pending.count == 1 && pending[0].revisionID == revA && pending[0].selectReceipt == nil,
                  "same-profile A choice retained after B and persisted before failed Select")
        try check(observedExpiry >= before + 599 && observedExpiry <= before + 601 &&
                  observedExpiry <= before + 900, "generated expiry fits catalog upper bound")
        await model.retrySelect(copyID: pending[0].copyID)
        let selectedAfterRetry = try state.profile(first.id).1.copies[0]
        try check(selectInputs.count == 2 && selectInputs[0] == selectInputs[1] &&
                  selectedAfterRetry.selectReceipt != nil,
                  "exact retry reused durable ID, request and expiry")
        await model.selectProfile(second.id)
        try check(model.activeProfileID == second.id, "second saved profile opened")
        let selectCount = selectInputs.count
        await model.pin(choice: choiceA, basename: "stale.png")
        let secondCopies = try state.profile(second.id).1.copies
        try check(selectInputs.count == selectCount && secondCopies.isEmpty,
                  "stale profile choice cannot allocate a copy or launch Select")

        await model.selectProfile(first.id)
        await model.search("before recovery")
        guard let priorRecoveryRow = model.page?.items.first,
              let priorRecoveryChoice = model.choice(for: priorRecoveryRow) else {
            throw failure("pre-recovery profile row absent")
        }
        let absent = try state.createProfile(endpoint: "http://127.0.0.1:18423", libraryID: libraryID,
                                             allowLoopbackHTTP: true)
        await model.recoverProfile(absent.id, bearer: "")
        let absentEntry = try state.profile(absent.id).1
        try check(calls.contains("create:" + absent.id) && absentEntry.created,
                  "saved pending absent root uses strict create and durable acknowledgement")
        try check(model.page == nil && model.choice(for: priorRecoveryRow) == nil,
                  "profile recovery clears old discovery projection before new choice")
        let beforeStaleRecoveryPin = selectInputs.count
        await model.pin(choice: priorRecoveryChoice, basename: "stale-recovery.png")
        let recoveredCopies = try state.profile(absent.id).1.copies
        try check(selectInputs.count == beforeStaleRecoveryPin && recoveredCopies.isEmpty,
                  "recovered profile refuses old choice without allocating a copy")
        let present = try state.createProfile(endpoint: "http://127.0.0.1:18424", libraryID: libraryID,
                                              allowLoopbackHTTP: true)
        guard mkdir(present.nativeRoot, 0o700) == 0 else { throw failure("owned pending Go leaf setup") }
        allowOpen = false
        await model.recoverProfile(present.id, bearer: "")
        let refusedEntry = try state.retainedProfile(present.id).1
        try check(!refusedEntry.created &&
                  FileManager.default.fileExists(atPath: present.nativeRoot),
                  "failed strict open leaves exact pending root and ID unacknowledged")
        allowOpen = true
        await model.recoverProfile(present.id, bearer: "")
        let recoveredEntry = try state.profile(present.id).1
        try check(calls.filter({ $0 == "open:" + present.id }).count == 2 &&
                  recoveredEntry.created,
                  "same saved existing leaf retries strict open without fresh create")

        await model.selectProfile(first.id)
        await model.search("before create")
        guard let priorCreateRow = model.page?.items.first else { throw failure("pre-create row absent") }
        await model.createProfile(endpoint: "http://127.0.0.1:18425", libraryID: libraryID,
                                  allowLoopbackHTTP: true,
                                  bearer: String(decoding: NativeLibraryModel.testBearer!, as: UTF8.self))
        try check(model.activeProfileID != first.id && model.activeProfileID != nil &&
                  model.page == nil && model.choice(for: priorCreateRow) == nil,
                  "new profile creation clears old discovery projection")

        mismatchVersion = true
        let untrusted = NativeLibraryModel()
        let priorOpens = calls.filter { $0.hasPrefix("open:") }.count
        await untrusted.selectProfile(first.id)
        try check(untrusted.activeProfileID == nil && calls.filter({ $0.hasPrefix("open:") }).count == priorOpens,
                  "mismatched bundled identity blocks profile open before credential or copy operation")
        mismatchVersion = false
        allowOpen = false
        let unavailableRoot = NativeLibraryModel()
        let priorStrictOpens = calls.filter({ $0 == "open:" + first.id }).count
        let priorCreates = calls.filter({ $0 == "create:" + first.id }).count
        await unavailableRoot.selectProfile(first.id)
        try check(unavailableRoot.activeProfileID == nil &&
                  calls.filter({ $0 == "open:" + first.id }).count == priorStrictOpens + 1 &&
                  calls.filter({ $0 == "create:" + first.id }).count == priorCreates,
                  "created profile refuses one failed strict Go open without recreating its root")
        await model.stopForQuit()
        let priorCalls = calls.count
        await model.retrySelect(copyID: pending[0].copyID)
        try check(calls.count == priorCalls, "Quit fence blocks subsequent helper launches")
        print("native library model probe passed")
    }

    private static func check(_ value: @autoclosure () throws -> Bool, _ message: String) throws {
        if try !value() { throw failure(message) }
    }
    private static func failure(_ message: String) -> NSError {
        NSError(domain: "NativeLibraryModelProbe", code: 1,
                userInfo: [NSLocalizedDescriptionKey: message])
    }
}
#endif
