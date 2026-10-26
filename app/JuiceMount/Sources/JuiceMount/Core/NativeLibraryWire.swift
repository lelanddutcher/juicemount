import CryptoKit
import Darwin
import Foundation

/// The finite JSON boundary shared with `juicemount-catalog native`.
/// Ordering and string escaping deliberately match Go's encoding/json;
/// JSONEncoder's sortedKeys and escaping do not.
enum NativeLibraryWire {
    enum Failure: Error {
        case invalidProfile, invalidRequest, invalidReceipt, invalidDiagnostic
    }

    struct Profile: Equatable {
        let endpoint: String
        let libraryID: String
        let nativeRoot: String
        let allowLoopbackHTTP: Bool
        let id: String

        init(endpoint: String, libraryID: String, nativeRoot: String, allowLoopbackHTTP: Bool) throws {
            guard Self.validEndpoint(endpoint, allowLoopbackHTTP: allowLoopbackHTTP),
                  nonzeroID(libraryID, prefix: "lib_", digits: 32),
                  nativeRoot.hasPrefix("/"), nativeRoot != "/",
                  URL(fileURLWithPath: nativeRoot).standardizedFileURL.path == nativeRoot else {
                throw Failure.invalidProfile
            }
            self.endpoint = endpoint
            self.libraryID = libraryID
            self.nativeRoot = nativeRoot
            self.allowLoopbackHTTP = allowLoopbackHTTP
            var material = Data("juicemount.native.profile.v1".utf8)
            material.append(contentsOf: [0])
            material.append(contentsOf: endpoint.utf8)
            material.append(contentsOf: [0])
            material.append(contentsOf: libraryID.utf8)
            id = SHA256.hash(data: material).map { String(format: "%02x", $0) }.joined()
        }

        private static func validEndpoint(_ raw: String, allowLoopbackHTTP: Bool) -> Bool {
            guard !raw.isEmpty, raw.utf8.count <= 275,
                  raw.utf8.allSatisfy({ $0 >= 0x21 && $0 <= 0x7e && $0 != 0x25 && $0 != 0x5c }) else { return false }
            let scheme: String
            let rest: String
            if raw.hasPrefix("https://") { scheme = "https"; rest = String(raw.dropFirst(8)) }
            else if raw.hasPrefix("http://") { scheme = "http"; rest = String(raw.dropFirst(7)) }
            else { return false }
            let slash = rest.firstIndex(of: "/") ?? rest.endIndex
            let authority = String(rest[..<slash])
            let path = String(rest[slash...])
            guard path.isEmpty || path == "/manager",
                  !authority.isEmpty, !authority.contains("@"), !authority.contains("?"), !authority.contains("#") else { return false }
            let host: String
            let portText: String
            let bracketed: Bool
            if authority.hasPrefix("[") {
                guard let end = authority.firstIndex(of: "]") else { return false }
                host = String(authority[authority.index(after: authority.startIndex)..<end])
                let tail = String(authority[authority.index(after: end)...])
                guard tail.isEmpty || tail.hasPrefix(":") && tail.count > 1 else { return false }
                portText = tail.isEmpty ? "" : String(tail.dropFirst())
                bracketed = true
            } else {
                let parts = authority.split(separator: ":", omittingEmptySubsequences: false)
                guard parts.count <= 2 else { return false }
                host = String(parts[0])
                portText = parts.count == 2 ? String(parts[1]) : ""
                if parts.count == 2 && portText.isEmpty { return false }
                bracketed = false
            }
            guard !host.isEmpty, !host.hasSuffix(".") else { return false }
            if !portText.isEmpty {
                guard portText.count <= 5, portText.first != "0", portText.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }),
                      let port = Int(portText), (1...65535).contains(port),
                      port != (scheme == "https" ? 443 : 80) else { return false }
            }
            if bracketed {
                var addr = in6_addr()
                guard host.withCString({ inet_pton(AF_INET6, $0, &addr) }) == 1 else { return false }
                var rendered = [CChar](repeating: 0, count: Int(INET6_ADDRSTRLEN))
                guard rendered.withUnsafeMutableBufferPointer({ inet_ntop(AF_INET6, &addr, $0.baseAddress, socklen_t($0.count)) }) != nil,
                      String(cString: rendered) == host else { return false }
                return scheme == "https" || allowLoopbackHTTP && host == "::1"
            }
            var address = in_addr()
            if host.withCString({ inet_pton(AF_INET, $0, &address) }) == 1 {
                var rendered = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
                guard rendered.withUnsafeMutableBufferPointer({ inet_ntop(AF_INET, &address, $0.baseAddress, socklen_t($0.count)) }) != nil,
                      String(cString: rendered) == host else { return false }
                return scheme == "https" || allowLoopbackHTTP && host.hasPrefix("127.")
            }
            guard scheme == "https", host.utf8.count <= 253, host == host.lowercased() else { return false }
            let labels = host.split(separator: ".", omittingEmptySubsequences: false)
            guard !labels.isEmpty, labels.allSatisfy({ label in
                label.utf8.count >= 1 && label.utf8.count <= 63 &&
                label.first.map(isDNSAlnum) == true && label.last.map(isDNSAlnum) == true &&
                label.utf8.allSatisfy { byte in isDNSAlnum(byte) || byte == 45 }
            }), host.utf8.contains(where: { (97...122).contains($0) || $0 == 45 }) else { return false }
            return true
        }
    }

    struct SearchRequest: Equatable {
        let text: String
        let contextAssetIDs: [String]
        let pageSize: Int
    }

    struct SelectRequest: Equatable {
        let operationID: String
        let assetID: String
        let mode: String
        let revisionID: String?
        let expiresUnix: Int64
        let maxBytes: Int64
    }

    struct DiscoveryAsset: Decodable, Equatable {
        let assetID: String
        let label: String
        let headRevisionID: String?
        let reasons: [String]
        enum CodingKeys: String, CodingKey {
            case assetID = "asset_id", label, headRevisionID = "head_revision_id", reasons
        }
    }
    struct DiscoveryPage: Decodable, Equatable {
        let version: Int
        let kind: String
        let items: [DiscoveryAsset]
        let matchedCount: Int
        let countScope: String
        let windowComplete: Bool
        let nextPageToken: String?
        let sortVersion: String
        enum CodingKeys: String, CodingKey {
            case version, kind, items, matchedCount = "matched_count", countScope = "count_scope"
            case windowComplete = "window_complete", nextPageToken = "next_page_token", sortVersion = "sort_version"
        }
    }
    struct Content: Decodable, Equatable {
        let sha256: String
        let size: Int64
        enum CodingKeys: String, CodingKey { case sha256 = "SHA256", size = "Size" }
    }
    struct Selection: Decodable, Equatable {
        let version: Int
        let libraryID: String
        let selectionID: String
        let operationID: String
        let assetID: String
        let revisionID: String
        let sourceID: String
        let sourceVersion: Int64
        let content: Content
        let expiresUnix: Int64
        let canceled: Bool
        enum CodingKeys: String, CodingKey {
            case version, libraryID = "library_id", selectionID = "selection_id", operationID = "operation_id"
            case assetID = "asset_id", revisionID = "revision_id", sourceID = "source_id"
            case sourceVersion = "source_version", content, expiresUnix = "expires_unix", canceled
        }
    }
    struct SelectReceipt: Decodable, Equatable {
        let version: Int
        let copyID: String
        let selection: Selection
        enum CodingKeys: String, CodingKey { case version, copyID = "copy_id", selection }
    }
    struct LocalStatus: Decodable, Equatable {
        let copyID: String
        let basename: String
        let size: Int64
        let sha256: String
        let path: String
        let previouslyDownloaded: Bool
        enum CodingKeys: String, CodingKey {
            case copyID = "copy_id", basename, size, sha256, path
            case previouslyDownloaded = "previously_downloaded"
        }
    }
    struct Diagnostic: Decodable, Equatable {
        let error: String
        let operationID: String?
        let selector: String?
        let outputCloseFailed: Bool?
        let partialOutput: Bool?
        let code: String
        let httpStatus: Int?
        let outcomeUncertain: Bool?
        enum CodingKeys: String, CodingKey {
            case error, operationID = "operation_id", selector
            case outputCloseFailed = "output_close_failed", partialOutput = "partial_output"
            case code, httpStatus = "http_status", outcomeUncertain = "outcome_uncertain"
        }
    }

    private static func isDNSAlnum(_ c: Character) -> Bool {
        c.utf8.count == 1 && c.utf8.first.map(isDNSAlnum) == true
    }
    private static func isDNSAlnum(_ c: UInt8) -> Bool { (97...122).contains(c) || (48...57).contains(c) }
    static func lowerHexID(_ value: String, prefix: String, digits: Int) -> Bool {
        value.hasPrefix(prefix) && value.utf8.count == prefix.utf8.count + digits &&
        value.utf8.dropFirst(prefix.utf8.count).allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
    }
    static func nonzeroID(_ value: String, prefix: String, digits: Int) -> Bool {
        lowerHexID(value, prefix: prefix, digits: digits) &&
        value.utf8.dropFirst(prefix.utf8.count).contains(where: { $0 != 48 })
    }
    static func validOperationID(_ value: String) -> Bool {
        (1...128).contains(value.utf8.count) && value.utf8.allSatisfy {
            (48...57).contains($0) || (65...90).contains($0) || (97...122).contains($0) || $0 == 95 || $0 == 45
        }
    }
    static func validBasename(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.count <= 255 && !value.hasPrefix(".") &&
        !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }) &&
        !value.contains("/") && !value.contains("\\") && !value.contains(":")
    }

    /// Go encoding/json string escaping, including HTML and line separators.
    static func quote(_ value: String) -> String {
        var out = "\""
        for scalar in value.unicodeScalars {
            switch scalar.value {
            case 34: out += "\\\""
            case 92: out += "\\\\"
            case 8: out += "\\b"
            case 12: out += "\\f"
            case 10: out += "\\n"
            case 13: out += "\\r"
            case 9: out += "\\t"
            case 0..<32, 38, 60, 62, 0x2028, 0x2029:
                out += String(format: "\\u%04x", scalar.value)
            default: out.unicodeScalars.append(scalar)
            }
        }
        return out + "\""
    }
    private static func object(_ fields: [(String, String)]) -> Data {
        Data(("{" + fields.map { quote($0.0) + ":" + $0.1 }.joined(separator: ",") + "}").utf8)
    }
    private static func array(_ values: [String]) -> String { "[" + values.map(quote).joined(separator: ",") + "]" }
    private static func text(_ value: String, max: Int) -> Bool {
        value.utf8.count <= max && !value.unicodeScalars.contains(where: { $0.value == 0 })
    }

    static func search(_ r: SearchRequest, profile: Profile) throws -> Data {
        guard text(r.text, max: 256), (1...100).contains(r.pageSize), r.contextAssetIDs.count <= 64,
              Set(r.contextAssetIDs).count == r.contextAssetIDs.count,
              r.contextAssetIDs.allSatisfy({ nonzeroID($0, prefix: "asset_", digits: 32) }) else {
            throw Failure.invalidRequest
        }
        let normalized = r.text.trimmingCharacters(in: CharacterSet(charactersIn: " \t\r\n\u{000B}\u{000C}"))
        var folded = ""
        for scalar in normalized.unicodeScalars {
            folded.unicodeScalars.append((65...90).contains(scalar.value) ? Unicode.Scalar(scalar.value + 32)! : scalar)
        }
        return object([("version", "2"), ("library_id", quote(profile.libraryID)), ("text", quote(folded)),
                       ("context_asset_ids", array(r.contextAssetIDs)), ("page_size", String(r.pageSize))])
    }
    static func page(_ token: String, profile: Profile) throws -> Data {
        guard lowerHexID(token, prefix: "page1_", digits: 64) else { throw Failure.invalidRequest }
        return object([("version", "2"), ("library_id", quote(profile.libraryID)), ("token", quote(token))])
    }
    static func select(_ r: SelectRequest, profile: Profile) throws -> Data {
        guard validOperationID(r.operationID), nonzeroID(r.assetID, prefix: "asset_", digits: 32),
              r.expiresUnix > 0, (0...(1 << 30)).contains(r.maxBytes),
              r.mode == "latest" && r.revisionID == nil ||
              r.mode == "exact" && r.revisionID.map({ lowerHexID($0, prefix: "rev1_", digits: 64) }) == true else {
            throw Failure.invalidRequest
        }
        var fields: [(String, String)] = [("version", "1"), ("library_id", quote(profile.libraryID)),
                                          ("operation_id", quote(r.operationID)), ("asset_id", quote(r.assetID)),
                                          ("mode", quote(r.mode))]
        if let revision = r.revisionID { fields.append(("revision_id", quote(revision))) }
        fields.append(("expires_unix", String(r.expiresUnix)))
        fields.append(("max_bytes", String(r.maxBytes)))
        return object(fields)
    }
    static func envelope(_ request: Data, bearer: Data) throws -> Data {
        guard validBearer(bearer), request.utf8Count <= 32768,
              let body = String(data: request, encoding: .utf8), body.hasPrefix("{"), body.hasSuffix("}") else {
            throw Failure.invalidRequest
        }
        return object([("version", "1"), ("bearer", quote(String(decoding: bearer, as: UTF8.self))), ("request", body)])
    }
    static func validBearer(_ bearer: Data) -> Bool {
        guard bearer.count == 102, bearer.prefix(5) == Data("jmc1_".utf8), Array(bearer)[37] == 46 else { return false }
        for (i, byte) in bearer.enumerated() where i >= 5 && i < 37 || i > 37 {
            if !((48...57).contains(byte) || (97...102).contains(byte)) { return false }
        }
        return true
    }

    private static func oneLine(_ data: Data, limit: Int) throws -> Data {
        guard data.count >= 3, data.count <= limit + 1, data.last == 10,
              !data.dropLast().contains(10) else { throw Failure.invalidReceipt }
        return Data(data.dropLast())
    }
    private static func decode<T: Decodable>(_ data: Data, limit: Int, as type: T.Type,
                                             canonical: (T) -> Data) throws -> T {
        let raw = try oneLine(data, limit: limit)
        guard let value = try? JSONDecoder().decode(type, from: raw), canonical(value) == raw else {
            throw Failure.invalidReceipt
        }
        return value
    }
    private static func canonicalAsset(_ a: DiscoveryAsset) -> String {
        let head = a.headRevisionID.map(quote) ?? "null"
        return String(decoding: object([("asset_id", quote(a.assetID)), ("label", quote(a.label)),
                                        ("head_revision_id", head), ("reasons", array(a.reasons))]), as: UTF8.self)
    }
    private static func canonicalPage(_ p: DiscoveryPage) -> Data {
        object([("version", String(p.version)), ("kind", quote(p.kind)),
                ("items", "[" + p.items.map(canonicalAsset).joined(separator: ",") + "]"),
                ("matched_count", String(p.matchedCount)), ("count_scope", quote(p.countScope)),
                ("window_complete", p.windowComplete ? "true" : "false"),
                ("next_page_token", p.nextPageToken.map(quote) ?? "null"),
                ("sort_version", quote(p.sortVersion))])
    }
    static func discoveryPage(_ data: Data) throws -> DiscoveryPage {
        let page = try decode(data, limit: 1 << 20, as: DiscoveryPage.self, canonical: canonicalPage)
        guard page.version == 2, page.kind == "search", page.countScope == "frozen_window", page.sortVersion == "discovery2",
              page.items.count <= 100, page.matchedCount >= page.items.count, page.matchedCount <= 4096,
              page.nextPageToken.map({ lowerHexID($0, prefix: "page1_", digits: 64) && !page.items.isEmpty && page.matchedCount > page.items.count }) ?? true,
              Set(page.items.map(\.assetID)).count == page.items.count,
              page.items.allSatisfy({ asset in
                  nonzeroID(asset.assetID, prefix: "asset_", digits: 32) && text(asset.label, max: 4096) &&
                  asset.headRevisionID.map({ lowerHexID($0, prefix: "rev1_", digits: 64) }) != false &&
                  (1...3).contains(asset.reasons.count)
              }) else { throw Failure.invalidReceipt }
        for asset in page.items {
            var reasons = asset.reasons
            if reasons.first == "context" { reasons.removeFirst() }
            if reasons.first == "declared_selection" { reasons.removeFirst() }
            guard reasons.count == 1, ["label_exact", "label_prefix", "label_contains", "stable_id"].contains(reasons[0]) else {
                throw Failure.invalidReceipt
            }
        }
        return page
    }
    private static func canonicalContent(_ c: Content) -> String {
        String(decoding: object([("SHA256", quote(c.sha256)), ("Size", String(c.size))]), as: UTF8.self)
    }
    private static func canonicalSelection(_ s: Selection) -> String {
        String(decoding: object([("version", String(s.version)), ("library_id", quote(s.libraryID)),
                                 ("selection_id", quote(s.selectionID)), ("operation_id", quote(s.operationID)),
                                 ("asset_id", quote(s.assetID)), ("revision_id", quote(s.revisionID)),
                                 ("source_id", quote(s.sourceID)), ("source_version", String(s.sourceVersion)),
                                 ("content", canonicalContent(s.content)), ("expires_unix", String(s.expiresUnix)),
                                 ("canceled", s.canceled ? "true" : "false")]), as: UTF8.self)
    }
    static func selectReceipt(_ data: Data, profile: Profile, copyID: String, request: SelectRequest) throws -> SelectReceipt {
        let result = try decode(data, limit: 1 << 20, as: SelectReceipt.self) { value in
            object([("version", String(value.version)), ("copy_id", quote(value.copyID)),
                    ("selection", canonicalSelection(value.selection))])
        }
        let s = result.selection
        guard result.version == 1, result.copyID == copyID, s.version == 1, s.libraryID == profile.libraryID,
              lowerHexID(s.selectionID, prefix: "dsel_", digits: 32), s.operationID == request.operationID,
              s.assetID == request.assetID, nonzeroID(s.assetID, prefix: "asset_", digits: 32),
              lowerHexID(s.revisionID, prefix: "rev1_", digits: 64),
              request.mode != "exact" || s.revisionID == request.revisionID,
              lowerHexID(s.sourceID, prefix: "dsrc_", digits: 32), s.sourceVersion > 0,
              lowerHexID(s.content.sha256, prefix: "", digits: 64), s.content.size >= 0,
              s.content.size <= request.maxBytes, s.expiresUnix == request.expiresUnix, !s.canceled else {
            throw Failure.invalidReceipt
        }
        return result
    }
    static func localStatus(_ data: Data, copyID: String, basename: String) throws -> LocalStatus {
        let result = try decode(data, limit: 1 << 20, as: LocalStatus.self) { value in
            object([("copy_id", quote(value.copyID)), ("basename", quote(value.basename)),
                    ("size", String(value.size)), ("sha256", quote(value.sha256)),
                    ("path", quote(value.path)),
                    ("previously_downloaded", value.previouslyDownloaded ? "true" : "false")])
        }
        guard result.copyID == copyID, result.basename == basename, result.size >= 0,
              lowerHexID(result.sha256, prefix: "", digits: 64), result.previouslyDownloaded else {
            throw Failure.invalidReceipt
        }
        return result
    }
    static func profileCreated(_ data: Data) throws {
        struct Created: Decodable { let version: Int; let created: Bool }
        let value = try decode(data, limit: 2048, as: Created.self) { created in
            object([("version", String(created.version)), ("created", created.created ? "true" : "false")])
        }
        guard value.version == 1, value.created else { throw Failure.invalidReceipt }
    }
    static func profileOpened(_ data: Data) throws {
        struct Opened: Decodable { let version: Int; let opened: Bool }
        let value = try decode(data, limit: 2048, as: Opened.self) { opened in
            object([("version", String(opened.version)), ("opened", opened.opened ? "true" : "false")])
        }
        guard value.version == 1, value.opened else { throw Failure.invalidReceipt }
    }
    static func diagnostics(_ data: Data) throws -> [Diagnostic] {
        guard !data.isEmpty, data.count <= 8192, data.last == 10 else { throw Failure.invalidDiagnostic }
        let lines = data.split(separator: 10, omittingEmptySubsequences: false).dropLast()
        guard (1...2).contains(lines.count) else { throw Failure.invalidDiagnostic }
        let allowed = Set(["invalid", "invalid_response", "unavailable", "unauthenticated", "not_found", "conflict",
                           "expired_request", "restart_query", "work_budget", "busy", "storage", "integrity", "deadline",
                           "canceled", "transport", "draining", "uncertain", "admission_limit", "denied", "reconciliation_required"])
        return try lines.map { line in
            guard line.count <= 4096, let value = try? JSONDecoder().decode(Diagnostic.self, from: Data(line)),
                  allowed.contains(value.code), !value.error.isEmpty, value.error.utf8.count <= 256,
                  value.httpStatus.map({ (400...599).contains($0) }) ?? true else { throw Failure.invalidDiagnostic }
            var fields: [(String, String)] = [("error", quote(value.error))]
            if let id = value.operationID { fields.append(("operation_id", quote(id))) }
            if let selector = value.selector { fields.append(("selector", quote(selector))) }
            if value.outputCloseFailed == true { fields.append(("output_close_failed", "true")) }
            if value.partialOutput == true { fields.append(("partial_output", "true")) }
            fields.append(("code", quote(value.code)))
            if let status = value.httpStatus, status != 0 { fields.append(("http_status", String(status))) }
            if value.outcomeUncertain == true { fields.append(("outcome_uncertain", "true")) }
            guard object(fields) == Data(line) else { throw Failure.invalidDiagnostic }
            return value
        }
    }
}

private extension Data {
    var utf8Count: Int { count }
}
