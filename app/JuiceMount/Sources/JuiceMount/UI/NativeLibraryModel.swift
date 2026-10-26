import AppKit
import Combine
import Foundation

/// Retained by MenuBarController, not by the window. Closing the window does
/// not cancel, forget, or orphan a child or a durable copy intent.
@MainActor
final class NativeLibraryModel: ObservableObject {
    struct Choice: Equatable {
        let asset: NativeLibraryWire.DiscoveryAsset
        let profileID: String
        let generation: UInt64
    }
    @Published private(set) var profiles: [NativeLibraryState.ProfileEntry] = []
    @Published private(set) var activeProfileID: String?
    @Published private(set) var page: NativeLibraryWire.DiscoveryPage?
    @Published private(set) var busy = false
    @Published private(set) var message = "Set up a Mac Library profile to begin."
    @Published private(set) var verifiedCopies: Set<String> = []
    private let helper = NativeLibraryHelper()
    private let credentials = NativeLibraryCredentials()
    private var state: NativeLibraryState?
    private var generation: UInt64 = 0
    private var queryToken: UInt64 = 0
    private var nextToken: String?
    private var closing = false
    private var transitioning = false
    private var operationWaiters: [CheckedContinuation<Void, Never>] = []
    private var busyWaiters: [CheckedContinuation<Void, Never>] = []
    private var helperIdentityValidated = false
    #if NATIVE_LIBRARY_PROBE
    static var testRun: ((NativeLibraryHelper.Command, NativeLibraryWire.Profile, Data) async throws -> Data)?
    static var testBearer: Data?
    #endif

    private func runCommand(_ command: NativeLibraryHelper.Command,
                            profile: NativeLibraryWire.Profile, stdin: Data) async throws -> Data {
        #if NATIVE_LIBRARY_PROBE
        if let testRun = Self.testRun { return try await testRun(command, profile, stdin) }
        #endif
        return try await helper.run(command, profile: profile, stdin: stdin)
    }

    init() {
        do {
            state = try NativeLibraryState.openExisting()
            profiles = try state?.load() ?? []
        } catch {
            message = "The Mac Library index is unavailable. Existing copies have not been changed."
        }
    }

    var activeEntry: NativeLibraryState.ProfileEntry? {
        profiles.first { $0.profileID == activeProfileID }
    }
    var copies: [NativeLibraryState.Copy] { activeEntry?.copies ?? [] }
    func choice(for asset: NativeLibraryWire.DiscoveryAsset) -> Choice? {
        guard let id = activeProfileID, activeEntry?.created == true,
              page?.items.contains(asset) == true else { return nil }
        return Choice(asset: asset, profileID: id, generation: generation)
    }

    private func refresh() throws { profiles = try state?.load() ?? [] }
    private func clearProfileProjection() {
        queryToken &+= 1
        page = nil
        nextToken = nil
        verifiedCopies = []
    }
    private func profile() throws -> NativeLibraryWire.Profile {
        guard helperIdentityValidated, let state, let id = activeProfileID else {
            throw NativeLibraryState.Failure.conflict
        }
        return try state.profile(id).0
    }
    private func bearer() throws -> Data {
        #if NATIVE_LIBRARY_PROBE
        if let testBearer = Self.testBearer { return testBearer }
        #endif
        guard let id = activeProfileID else { throw NativeLibraryState.Failure.conflict }
        switch try credentials.read(profileID: id) {
        case .absent: throw NativeLibraryCredentials.Failure.inaccessible
        case .present(let value): return value
        }
    }
    private func begin() -> Bool {
        guard !busy && !closing && !transitioning else { return false }
        busy = true
        return true
    }
    private func finishOperation() {
        busy = false
        let waitingForBusy = busyWaiters
        busyWaiters = []
        for waiter in waitingForBusy { waiter.resume() }
        wakeWaitersIfIdle()
    }
    private func wakeWaitersIfIdle() {
        guard !busy && !transitioning else { return }
        let waiters = operationWaiters
        operationWaiters = []
        for waiter in waiters { waiter.resume() }
    }
    private func waitOperation() async {
        guard busy || transitioning else { return }
        await withCheckedContinuation { operationWaiters.append($0) }
    }
    private func report(_ error: Error, while action: String) {
        if let failure = error as? NativeLibraryHelper.Failure {
            switch failure {
            case .childFailure(let code, _, let uncertain):
                message = uncertain ? "\(action) outcome uncertain. Keep this exact copy ID; retry or check its local status."
                                    : "\(action) failed (\(code)). The saved copy ID is retained."
            default:
                message = failure.outcomeUncertain ? "\(action) outcome uncertain. The original operation is retained."
                                                   : "\(action) could not finish."
            }
        } else {
            message = "\(action) could not be verified. Existing records and files were preserved."
        }
    }
    private func verifyBundledHelper(_ profile: NativeLibraryWire.Profile) async throws {
        if helperIdentityValidated { return }
        struct Version: Decodable { let version: Int; let productVersion: String; let commit: String
            enum CodingKeys: String, CodingKey {
                case version, productVersion = "product_version", commit
            }
        }
        let raw = try await runCommand(.version, profile: profile, stdin: Data())
        guard raw.count <= 512, raw.last == 10,
              let value = try? JSONDecoder().decode(Version.self, from: Data(raw.dropLast())),
              value.version == 1,
              value.productVersion == Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String,
              value.commit == Bundle.main.object(forInfoDictionaryKey: "JMBuildCommit") as? String,
              raw == Data("{\"version\":1,\"product_version\":\(NativeLibraryWire.quote(value.productVersion)),\"commit\":\(NativeLibraryWire.quote(value.commit))}\n".utf8) else {
            throw NativeLibraryWire.Failure.invalidReceipt
        }
        helperIdentityValidated = true
    }

    func createProfile(endpoint: String, libraryID: String, allowLoopbackHTTP: Bool, bearer text: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            let secret = Data(text.utf8)
            guard NativeLibraryWire.validBearer(secret) else { throw NativeLibraryWire.Failure.invalidRequest }
            let probe = try NativeLibraryWire.Profile(endpoint: endpoint, libraryID: libraryID,
                              nativeRoot: "/native-version-only", allowLoopbackHTTP: allowLoopbackHTTP)
            try await verifyBundledHelper(probe)
            guard current == generation && !closing else { return }
            if state == nil { state = try NativeLibraryState.createIfAbsent() }
            guard let state else { throw NativeLibraryState.Failure.unavailable }
            let profile = try state.createProfile(endpoint: endpoint, libraryID: libraryID,
                                                  allowLoopbackHTTP: allowLoopbackHTTP)
            guard current == generation && !closing else { return }
            let receipt = try await runCommand(.profileCreate, profile: profile, stdin: Data())
            try NativeLibraryWire.profileCreated(receipt)
            guard current == generation && !closing else { return }
            try state.markProfileCreated(profile.id)
            guard current == generation && !closing else { try refresh(); return }
            try credentials.save(secret, profileID: profile.id)
            try refresh()
            clearProfileProjection()
            activeProfileID = profile.id
            generation &+= 1
            message = "Mac Library profile ready. Search the remote library to choose an exact file."
        } catch {
            try? refresh()
            if current == generation && !closing { report(error, while: "Profile setup") }
        }
    }

    /// Only a retained pending tuple can be recovered. The app checks the
    /// owned leaf's presence under its index lock; the Go command then either
    /// strictly creates the absent leaf or strictly validates the present one.
    func recoverProfile(_ id: String, bearer text: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard let state else { throw NativeLibraryState.Failure.unavailable }
            let (profile, entry) = try state.retainedProfile(id)
            guard !entry.created else { throw NativeLibraryState.Failure.conflict }
            try await verifyBundledHelper(profile)
            guard current == generation && !closing else { return }
            let presence = try state.pendingRoot(id)
            let command: NativeLibraryHelper.Command = presence == .absent ? .profileCreate : .profileOpen
            let receipt = try await runCommand(command, profile: profile, stdin: Data())
            if presence == .absent { try NativeLibraryWire.profileCreated(receipt) }
            else { try NativeLibraryWire.profileOpened(receipt) }
            guard current == generation && !closing else { return }
            try state.markProfileCreated(id)
            try refresh()
            guard current == generation && !closing else { return }
            if !text.isEmpty { try credentials.save(Data(text.utf8), profileID: id) }
            clearProfileProjection()
            activeProfileID = id
            generation &+= 1
            message = "The original saved profile is ready. Its root and pending IDs were retained."
        } catch { if current == generation && !closing { report(error, while: "Profile recovery") } }
    }

    func selectProfile(_ id: String) async {
        guard !transitioning && !closing else { return }
        transitioning = true
        defer { transitioning = false; wakeWaitersIfIdle() }
        generation &+= 1
        queryToken &+= 1
        page = nil; nextToken = nil; verifiedCopies = []
        await helper.stopAndJoin()
        // Wait only for the prior state-changing operation. This transition
        // itself remains counted until the identity child has joined.
        if busy { await withCheckedContinuation { busyWaiters.append($0) } }
        guard !closing else { return }
        guard let state, profiles.contains(where: { $0.profileID == id && $0.created }) else {
            activeProfileID = nil
            message = "This profile requires explicit setup recovery. No bearer was forwarded."
            return
        }
        do {
            let (profile, _) = try state.retainedProfile(id)
            try await verifyBundledHelper(profile)
            guard !closing else { return }
            let opened = try await runCommand(.profileOpen, profile: profile, stdin: Data())
            try NativeLibraryWire.profileOpened(opened)
        } catch {
            activeProfileID = nil
            report(error, while: "Bundled helper identity")
            return
        }
        guard !closing else { return }
        activeProfileID = id
        message = "Profile selected. Copies below remain local; search needs its saved bearer."
    }
    func disconnect() async {
        guard !transitioning && !closing else { return }
        transitioning = true
        defer { transitioning = false; wakeWaitersIfIdle() }
        generation &+= 1; queryToken &+= 1
        page = nil; nextToken = nil; verifiedCopies = []
        await helper.stopAndJoin()
        if busy { await withCheckedContinuation { busyWaiters.append($0) } }
        activeProfileID = nil
        message = "Disconnected. Retained copy records and files are unchanged."
    }
    func stopForQuit() async {
        closing = true
        generation &+= 1; queryToken &+= 1
        page = nil; nextToken = nil; verifiedCopies = []
        await helper.stopAndJoin()
        await waitOperation() // includes all continuations between child launches
        await helper.stopAndJoin()
    }
    func resumeAfterCanceledQuit() { closing = false }

    func saveBearer(_ text: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        do {
            guard let id = activeProfileID else { throw NativeLibraryState.Failure.conflict }
            let p = try profile()
            try await verifyBundledHelper(p)
            guard !closing else { return }
            try credentials.save(Data(text.utf8), profileID: id)
            message = "Credential saved for this exact profile."
        } catch { report(error, while: "Credential save") }
    }
    func removeBearer() async {
        guard begin() else { return }
        defer { finishOperation() }
        do {
            guard let id = activeProfileID else { throw NativeLibraryState.Failure.conflict }
            let p = try profile()
            try await verifyBundledHelper(p)
            guard !closing else { return }
            try credentials.remove(profileID: id)
            message = "Credential removed. Local copy records and files remain."
        } catch { report(error, while: "Credential removal") }
    }

    func search(_ text: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        queryToken &+= 1
        let token = queryToken, current = generation
        page = nil; nextToken = nil
        do {
            let p = try profile()
            let input = try NativeLibraryWire.envelope(
                NativeLibraryWire.search(.init(text: text, contextAssetIDs: [], pageSize: 50), profile: p),
                bearer: bearer())
            let raw = try await runCommand(.search, profile: p, stdin: input)
            let result = try NativeLibraryWire.discoveryPage(raw)
            guard current == generation, token == queryToken else { return }
            page = result; nextToken = result.nextPageToken
            message = "Showing \(result.items.count) of \(result.matchedCount) search matches. Select a revision deliberately."
        } catch { if current == generation && token == queryToken { report(error, while: "Search") } }
    }
    func nextPage() async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation, token = queryToken
        do {
            guard let nextToken else { throw NativeLibraryWire.Failure.invalidRequest }
            let p = try profile()
            let input = try NativeLibraryWire.envelope(NativeLibraryWire.page(nextToken, profile: p), bearer: bearer())
            let raw = try await runCommand(.page, profile: p, stdin: input)
            let result = try NativeLibraryWire.discoveryPage(raw)
            guard current == generation, token == queryToken else { return }
            page = result; self.nextToken = result.nextPageToken
            message = "Next frozen search page. Select a revision deliberately."
        } catch { if current == generation && token == queryToken { report(error, while: "Page") } }
    }

    func pin(choice: Choice, basename: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard choice.profileID == activeProfileID, choice.generation == generation,
                  let revision = choice.asset.headRevisionID, let state, let id = activeProfileID else {
                throw NativeLibraryWire.Failure.invalidRequest
            }
            let p = try profile()
            let expiration = Int64(Date().timeIntervalSince1970) + 600
            let copy = try state.beginCopy(profileID: id, assetID: choice.asset.assetID,
                         revisionID: revision, basename: basename, expiresUnix: expiration,
                         maxBytes: 1 << 30)
            try refresh() // pending ID is now visible even if the network fails
            let input = try NativeLibraryWire.envelope(copy.selectBytes, bearer: bearer())
            guard current == generation && !closing else { return }
            let raw = try await runCommand(.select(copyID: copy.copyID, basename: copy.basename),
                                           profile: p, stdin: input)
            guard current == generation else { return }
            _ = try state.pinSelection(profileID: id, copyID: copy.copyID, receipt: raw)
            try refresh()
            message = "Exact revision pinned as \(copy.copyID). Receive it when ready."
        } catch { if current == generation { try? refresh(); report(error, while: "Select") } }
    }
    func retrySelect(copyID: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard let id = activeProfileID, let state,
                  let copy = copies.first(where: { $0.copyID == copyID }) else { throw NativeLibraryState.Failure.conflict }
            let p = try profile()
            let input = try NativeLibraryWire.envelope(copy.selectBytes, bearer: bearer())
            let raw = try await runCommand(.select(copyID: copy.copyID, basename: copy.basename),
                                           profile: p, stdin: input)
            guard current == generation else { return }
            _ = try state.pinSelection(profileID: id, copyID: copyID, receipt: raw)
            try refresh()
            message = "Original exact Select operation is pinned."
        } catch { if current == generation { report(error, while: "Exact retry") } }
    }
    func receive(copyID: String) async { await finish(copyID: copyID, recover: false) }
    func recoverPrepared(copyID: String) async { await finish(copyID: copyID, recover: true) }
    private func finish(copyID: String, recover: Bool) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard helperIdentityValidated, let id = activeProfileID, let state else { throw NativeLibraryState.Failure.conflict }
            let (p, _, _) = try state.selectedCopy(profileID: id, copyID: copyID)
            let input = recover ? Data() : try bearer()
            let command: NativeLibraryHelper.Command = recover ? .recover(copyID: copyID) : .receive(copyID: copyID)
            guard current == generation && !closing else { return }
            let raw = try await runCommand(command, profile: p, stdin: input)
            guard current == generation else { return }
            _ = try state.verifiedURL(profileID: id, copyID: copyID, statusData: raw)
            // A fresh credential-free status reopens and hashes the named
            // local object before the UI enables Finder actions.
            guard current == generation && !closing else { return }
            let status = try await runCommand(.copyStatus(copyID: copyID), profile: p, stdin: Data())
            guard current == generation else { return }
            _ = try state.verifiedURL(profileID: id, copyID: copyID, statusData: status)
            verifiedCopies.insert(copyID)
            message = "Previously downloaded local copy verified."
        } catch { if current == generation { report(error, while: recover ? "Recovery" : "Receive") } }
    }
    func checkLocal(copyID: String) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard helperIdentityValidated, let id = activeProfileID, let state else { throw NativeLibraryState.Failure.conflict }
            let (p, _, _) = try state.selectedCopy(profileID: id, copyID: copyID)
            let raw = try await runCommand(.copyStatus(copyID: copyID), profile: p, stdin: Data())
            guard current == generation else { return }
            _ = try state.verifiedURL(profileID: id, copyID: copyID, statusData: raw)
            verifiedCopies.insert(copyID)
            message = "Previously downloaded local copy verified offline."
        } catch {
            verifiedCopies.remove(copyID)
            if current == generation { report(error, while: "Offline status") }
        }
    }
    func reveal(copyID: String, openFile: Bool) async {
        guard begin() else { return }
        defer { finishOperation() }
        let current = generation
        do {
            guard helperIdentityValidated, let id = activeProfileID, let state else { throw NativeLibraryState.Failure.conflict }
            let (p, _, _) = try state.selectedCopy(profileID: id, copyID: copyID)
            let raw = try await runCommand(.copyStatus(copyID: copyID), profile: p, stdin: Data())
            guard current == generation else { return }
            let url = try state.verifiedURL(profileID: id, copyID: copyID, statusData: raw)
            verifiedCopies.insert(copyID)
            if openFile { _ = NSWorkspace.shared.open(url) }
            else { NSWorkspace.shared.activateFileViewerSelecting([url]) }
            message = "Finder was asked to open the verified previous local copy."
        } catch {
            verifiedCopies.remove(copyID)
            if current == generation { report(error, while: "Finder verification") }
        }
    }
}
