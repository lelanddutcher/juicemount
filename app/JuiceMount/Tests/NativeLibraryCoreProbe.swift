import Darwin
import Foundation

// Standalone source probe. The runner compiles this file together with
// NativeLibraryWire.swift and NativeLibraryHelper.swift into
// Probe.app/Contents/MacOS/NativeLibraryCoreProbe, copies that same binary to
// Probe.app/Contents/MacOS/juicemount-catalog, and runs the first path. The
// child never sees a bearer in argv or environment. This fixture does not
// exercise the production Go helper or claim packaged-app acceptance.
@main
enum NativeLibraryCoreProbe {
    static func main() async throws {
        if URL(fileURLWithPath: CommandLine.arguments[0]).lastPathComponent == "juicemount-catalog" {
            fakeChild()
            return
        }
        try wire()
        try await owner()
        print("native library core probe passed")
    }

    private static func check(_ value: @autoclosure () -> Bool, _ message: String) throws {
        if !value() { throw NSError(domain: "NativeLibraryCoreProbe", code: 1,
                                    userInfo: [NSLocalizedDescriptionKey: message]) }
    }
    private static func wire() throws {
        let p = try NativeLibraryWire.Profile(endpoint: "http://127.0.0.1:18080/manager",
                                               libraryID: "lib_" + String(repeating: "a", count: 32),
                                               nativeRoot: "/tmp/native-probe", allowLoopbackHTTP: true)
        let openWords = try NativeLibraryHelper.Command.profileOpen.arguments(profile: p)
        try check(Array(openWords.suffix(3)) == ["native", "profile", "open"] &&
                  !openWords.contains("--copy-id") && !openWords.contains("--basename"),
                  "credential-free exact native profile open command")
        try NativeLibraryWire.profileOpened(Data("{\"version\":1,\"opened\":true}\n".utf8))
        do { try NativeLibraryWire.profileOpened(Data("{\"version\":1,\"opened\":false}\n".utf8))
             throw NSError(domain: "probe", code: 13) }
        catch NativeLibraryWire.Failure.invalidReceipt { }
        try check(p.id.count == 64 && p.id.allSatisfy({ $0.isHexDigit }), "profile digest")
        let search = try NativeLibraryWire.search(.init(text: " A&<B\u{2028} ",
                                                         contextAssetIDs: [], pageSize: 1), profile: p)
        let expectedSearch = "{\"version\":2,\"library_id\":\"\(p.libraryID)\",\"text\":\"a\\u0026\\u003cb\\u2028\",\"context_asset_ids\":[],\"page_size\":1}"
        try check(search == Data(expectedSearch.utf8), "Go canonical discovery search bytes")
        let op = "op_fixed_1"
        let asset = "asset_" + String(repeating: "b", count: 32)
        let select = try NativeLibraryWire.select(.init(operationID: op, assetID: asset, mode: "latest",
                                                         revisionID: nil, expiresUnix: 9_007_199_254_740_993,
                                                         maxBytes: 1_073_741_824), profile: p)
        let expectedSelect = "{\"version\":1,\"library_id\":\"\(p.libraryID)\",\"operation_id\":\"\(op)\",\"asset_id\":\"\(asset)\",\"mode\":\"latest\",\"expires_unix\":9007199254740993,\"max_bytes\":1073741824}"
        try check(select == Data(expectedSelect.utf8), "Go canonical select integer/omission bytes")
        let bearer = Data(("jmc1_" + String(repeating: "c", count: 32) + "." + String(repeating: "d", count: 64)).utf8)
        let envelope = try NativeLibraryWire.envelope(select, bearer: bearer)
        try check(envelope == Data(("{\"version\":1,\"bearer\":\"\(String(decoding: bearer, as: UTF8.self))\",\"request\":\(expectedSelect)}").utf8), "Go canonical envelope bytes")
        let page = Data(("{\"version\":2,\"kind\":\"search\",\"items\":[],\"matched_count\":0,\"count_scope\":\"frozen_window\",\"window_complete\":true,\"next_page_token\":null,\"sort_version\":\"discovery2\"}\n").utf8)
        _ = try NativeLibraryWire.discoveryPage(page)
        var noncanonical = Data(page.dropLast())
        noncanonical.append(contentsOf: " \n".utf8)
        do { _ = try NativeLibraryWire.discoveryPage(noncanonical); throw NSError(domain: "probe", code: 2) }
        catch NativeLibraryWire.Failure.invalidReceipt { }
        do { _ = try NativeLibraryWire.Profile(endpoint: "http://example.com", libraryID: p.libraryID,
                                               nativeRoot: p.nativeRoot, allowLoopbackHTTP: true); throw NSError(domain: "probe", code: 3) }
        catch NativeLibraryWire.Failure.invalidProfile { }
        for invalidEndpoint in ["https://[::1]:", "https://127.00.0.1"] {
            do { _ = try NativeLibraryWire.Profile(endpoint: invalidEndpoint, libraryID: p.libraryID,
                                                   nativeRoot: p.nativeRoot, allowLoopbackHTTP: false)
                 throw NSError(domain: "probe", code: 6) }
            catch NativeLibraryWire.Failure.invalidProfile { }
        }
        do { _ = try NativeLibraryWire.Profile(endpoint: p.endpoint,
                                               libraryID: "lib_" + String(repeating: "0", count: 32),
                                               nativeRoot: p.nativeRoot, allowLoopbackHTTP: true)
             throw NSError(domain: "probe", code: 7) }
        catch NativeLibraryWire.Failure.invalidProfile { }
        do { _ = try NativeLibraryWire.select(.init(operationID: op,
                   assetID: "asset_" + String(repeating: "0", count: 32), mode: "latest",
                   revisionID: nil, expiresUnix: 123, maxBytes: 1), profile: p)
             throw NSError(domain: "probe", code: 8) }
        catch NativeLibraryWire.Failure.invalidRequest { }
        let impossiblePage = Data(("{\"version\":2,\"kind\":\"search\",\"items\":[{\"asset_id\":\"\(asset)\",\"label\":\"A\",\"head_revision_id\":null,\"reasons\":[\"stable_id\"]}],\"matched_count\":1,\"count_scope\":\"frozen_window\",\"window_complete\":false,\"next_page_token\":\"page1_\(String(repeating: "a", count: 64))\",\"sort_version\":\"discovery2\"}\n").utf8)
        do { _ = try NativeLibraryWire.discoveryPage(impossiblePage); throw NSError(domain: "probe", code: 9) }
        catch NativeLibraryWire.Failure.invalidReceipt { }
    }

    private static func owner() async throws {
        let probeRoot = NSTemporaryDirectory() + "native-library-core-" + UUID().uuidString
        try FileManager.default.createDirectory(atPath: probeRoot, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(atPath: probeRoot) }
        let p = try NativeLibraryWire.Profile(endpoint: "http://127.0.0.1:18080",
                                               libraryID: "lib_" + String(repeating: "a", count: 32),
                                               nativeRoot: probeRoot, allowLoopbackHTTP: true)
        let h = NativeLibraryHelper()
        let output = try await h.run(.version, profile: p, stdin: Data())
        try check(output == Data("{\"version\":1,\"product_version\":\"fixture\",\"commit\":\"fixture\"}\n".utf8), "joined child stdout")
        try check(!h.isBusy, "success joined original child")
        let full = try await h.run(.inspect, profile: p, stdin: Data())
        try check(full.count == (1 << 20) + 1 && full.last == 10, "1 MiB JSON plus LF is admitted")
        do { _ = try await h.run(.lookup, profile: p, stdin: Data()); throw NSError(domain: "probe", code: 10) }
        catch NativeLibraryHelper.Failure.malformedReceipt(uncertain: false) { }
        try check(!h.isBusy, "overflow joined original output")
        let preCanceled = Task { () throws -> Data in
            withUnsafeCurrentTask { $0?.cancel() }
            return try await h.run(.profileCreate, profile: p, stdin: Data())
        }
        do { _ = try await preCanceled.value; throw NSError(domain: "probe", code: 11) }
        catch NativeLibraryHelper.Failure.stopped { }
        try check(!FileManager.default.fileExists(atPath: probeRoot + "/child-ready"), "pre-canceled mutation never launched")
        do { _ = try await h.run(.copyStatus(copyID: "copy_" + String(repeating: "a", count: 32)),
                                 profile: p, stdin: Data()); throw NSError(domain: "probe", code: 12) }
        catch NativeLibraryHelper.Failure.malformedReceipt(uncertain: true) { }
        try check(!h.isBusy, "partial stdout and uncertain diagnostic joined")
        let pending = Task { try await h.run(.search, profile: p, stdin: Data(repeating: 97, count: 32_000)) }
        let marker = probeRoot + "/child-ready"
        do {
            for _ in 0..<100 where !FileManager.default.fileExists(atPath: marker) {
                try await Task.sleep(for: .milliseconds(20))
            }
            try check(FileManager.default.fileExists(atPath: marker), "original child reached ready fixture")
        } catch {
            pending.cancel()
            _ = try? await pending.value // consume the original, even on readiness failure
            await h.stopAndJoin()
            throw error
        }
        pending.cancel()
        do { _ = try await pending.value; throw NSError(domain: "probe", code: 4) }
        catch let failure as NativeLibraryHelper.Failure {
            switch failure {
            case .stopped, .originalCloseUncertain: break
            default: throw failure
            }
        }
        try check(!h.isBusy, "cancellation joined original child and pipes")
        let earlyClosed = Task { try await h.run(.page, profile: p, stdin: Data(repeating: 97, count: 33_000)) }
        do { _ = try await earlyClosed.value; throw NSError(domain: "probe", code: 5) }
        catch let failure as NativeLibraryHelper.Failure {
            switch failure {
            case .originalCloseUncertain, .childFailure(code: "invalid", httpStatus: nil, uncertain: false): break
            default: throw failure
            }
        }
        try check(!h.isBusy, "early child stdin closure joined original writer")
        await h.stopAndJoin()
        await h.stopAndJoin()
    }

    private static func fakeChild() {
        let words = CommandLine.arguments
        if Array(words.suffix(2)) == ["native", "version"] {
            _ = read(STDIN_FILENO, nil, 0)
            FileHandle.standardOutput.write(Data("{\"version\":1,\"product_version\":\"fixture\",\"commit\":\"fixture\"}\n".utf8))
            return
        }
        if Array(words.suffix(2)) == ["native", "page"] {
            _ = close(STDIN_FILENO)
            FileHandle.standardError.write(Data("{\"error\":\"fixture failure\",\"code\":\"invalid\"}\n".utf8))
            exit(3)
        }
        if Array(words.suffix(2)) == ["native", "inspect"] ||
           Array(words.suffix(2)) == ["native", "lookup"] {
            let extra = Array(words.suffix(2)) == ["native", "lookup"] ? 1 : 0
            let body = "{\"v\":\"" + String(repeating: "a", count: (1 << 20) - 8 + extra) + "\"}\n"
            FileHandle.standardOutput.write(Data(body.utf8))
            return
        }
        if Array(words.suffix(2)) == ["native", "copy-status"] {
            FileHandle.standardOutput.write(Data("partial".utf8))
            FileHandle.standardError.write(Data("{\"error\":\"native delivery outcome uncertain\",\"code\":\"uncertain\",\"outcome_uncertain\":true}\n".utf8))
            exit(3)
        }
        if let i = words.firstIndex(of: "--native-root"), i + 1 < words.count {
            FileHandle.standardOutput.write(Data())
            try? Data("ready".utf8).write(to: URL(fileURLWithPath: words[i + 1] + "/child-ready"))
        }
        // Do not read stdin. The parent writer may block. SIGTERM must end
        // this original child before cancellation can claim a joined owner.
        sleep(30)
    }
}
