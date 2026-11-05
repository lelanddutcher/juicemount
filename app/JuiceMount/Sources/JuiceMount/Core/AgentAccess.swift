import Foundation

/// Agent access (docs/agent-mcp.md): the parts of Settings' Agents section
/// that need no bridge, so Tests/AgentAccessProbe.swift builds them alone.
/// The bridge (bridge/agenttokens.go) mints, lists and revokes this person's
/// agent tokens as this Mac and keeps the minted token in the Keychain item
/// the bundled juicemount-mcp reads. No value here is ever the token.
enum AgentAccess {
    /// The longest a token lives, and the default (the Manager's limit).
    static let maxDays = 90
    /// The bundled MCP server, in Contents/MacOS.
    static let binaryName = "juicemount-mcp"
    /// The MCP server's name in an agent's config.
    static let serverName = "juicemount"

    /// One token as the Manager describes it (agentmcp.TokenView).
    struct Token: Decodable, Equatable, Identifiable {
        var id: String
        var name: String
        var createdAtMS: Int64
        var expiresAtMS: Int64

        var created: Date { Date(timeIntervalSince1970: Double(createdAtMS) / 1000) }
        var expires: Date { Date(timeIntervalSince1970: Double(expiresAtMS) / 1000) }

        enum CodingKeys: String, CodingKey {
            case id, name
            case createdAtMS = "created_at_ms", expiresAtMS = "expires_at_ms"
        }

        init(id: String, name: String, createdAtMS: Int64, expiresAtMS: Int64) {
            self.id = id; self.name = name; self.createdAtMS = createdAtMS; self.expiresAtMS = expiresAtMS
        }

        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            id = try c.decode(String.self, forKey: .id)
            name = try c.decodeIfPresent(String.self, forKey: .name) ?? ""
            createdAtMS = try c.decodeIfPresent(Int64.self, forKey: .createdAtMS) ?? 0
            expiresAtMS = try c.decodeIfPresent(Int64.self, forKey: .expiresAtMS) ?? 0
        }
    }

    /// Output of NFSServerAgentToken{Create,List,Revoke} (agentTokenOutput).
    /// Holds no secret.
    struct Output: Decodable {
        var ok: Bool
        /// The Manager answered the agent-token routes (JM_CONTINUITY on).
        var continuity: Bool
        var tokens: [Token]
        var token: Token?
        /// The token stored on this Mac for juicemount-mcp ("" = none).
        var storedID: String
        var replacedID: String
        var gone: Bool
        var deleted: Bool
        var warning: String?
        var error: String?
        var detail: String?

        enum CodingKeys: String, CodingKey {
            case ok, continuity, tokens, token, gone, deleted, warning, error, detail
            case storedID = "stored_id", replacedID = "replaced_id"
        }

        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            ok = try c.decodeIfPresent(Bool.self, forKey: .ok) ?? false
            continuity = try c.decodeIfPresent(Bool.self, forKey: .continuity) ?? false
            tokens = try c.decodeIfPresent([Token].self, forKey: .tokens) ?? []
            token = try c.decodeIfPresent(Token.self, forKey: .token)
            storedID = try c.decodeIfPresent(String.self, forKey: .storedID) ?? ""
            replacedID = try c.decodeIfPresent(String.self, forKey: .replacedID) ?? ""
            gone = try c.decodeIfPresent(Bool.self, forKey: .gone) ?? false
            deleted = try c.decodeIfPresent(Bool.self, forKey: .deleted) ?? false
            warning = try c.decodeIfPresent(String.self, forKey: .warning)
            error = try c.decodeIfPresent(String.self, forKey: .error)
            detail = try c.decodeIfPresent(String.self, forKey: .detail)
        }

        static func decode(_ raw: String?) -> Output? {
            guard let raw, let data = raw.data(using: .utf8) else { return nil }
            return try? JSONDecoder().decode(Output.self, from: data)
        }
    }

    /// Where the token list stands.
    enum ListState: Equatable {
        case notLoaded
        case loading
        case loaded(continuity: Bool)
        case failed(code: String, detail: String)
    }

    /// What the Agents section shows.
    enum Visibility: Equatable {
        /// A short note and nothing else.
        case disabled(String)
        case loading
        /// The list could not be read: the message, and Retry.
        case failed(String)
        /// Tokens, New token…, Revoke and the copy buttons.
        case shown
    }

    static let notApprovedNote = "Agent access needs this Mac approved by the Manager (Server access, above)."
    static let continuityOffNote = "Agent access is off on this Manager. An admin turns it on with JM_CONTINUITY."

    /// The section is shown only when this Mac is approved and the Manager
    /// reports continuity on; otherwise a short disabled note.
    static func visibility(enrollmentApproved: Bool, list: ListState) -> Visibility {
        guard enrollmentApproved else { return .disabled(notApprovedNote) }
        switch list {
        case .notLoaded, .loading:
            return .loading
        case .loaded(let continuity):
            return continuity ? .shown : .disabled(continuityOffNote)
        case let .failed(code, detail):
            switch code {
            case "not_enrolled": return .disabled(notApprovedNote)
            case "continuity_off": return .disabled(continuityOffNote)
            case "refused": return .disabled(message(code: code, detail: detail))
            default: return .failed(message(code: code, detail: detail))
            }
        }
    }

    /// A short, plain message for a failed call (agentTokenOutput.Error).
    static func message(code: String?, detail: String?) -> String {
        let detail = (detail ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        switch code ?? "" {
        case "not_enrolled":
            return "This Mac is not approved by the Manager yet."
        case "refused":
            return "The Manager refused this Mac. An admin must approve it and assign it to you on the People page."
        case "continuity_off":
            return "Agent access is off on this Manager."
        case "unreachable":
            return "The Manager could not be reached."
        case "maybe_minted":
            return "The Manager did not answer, so a token may have been created. Check the list and revoke any you don't recognise."
        case "store_failed":
            return "The token could not be saved in the Keychain, so it was revoked."
        case "store_failed_live":
            return "The token could not be saved in the Keychain, and revoking it failed. Revoke it in the list."
        case "keychain":
            return "Revoked on the Manager, but the copy on this Mac could not be deleted."
        case "invalid":
            return detail.isEmpty ? "Check the name and the number of days." : sentence(detail)
        case "manager":
            return detail.isEmpty ? "The Manager refused the request." : "The Manager refused: \(detail)."
        default:
            return detail.isEmpty ? "Something went wrong." : sentence(detail)
        }
    }

    private static func sentence(_ s: String) -> String {
        let first = s.prefix(1).uppercased() + s.dropFirst()
        return first.hasSuffix(".") ? first : first + "."
    }

    /// What a successful create says. replacedID: the token stored here
    /// before, which stays valid on the Manager until revoked.
    static func createdMessage(name: String, replacedID: String) -> String {
        let base = "Created “\(name)”. Agents on this Mac use it now."
        return replacedID.isEmpty ? base : base + " The token it replaces here still works until you revoke it."
    }

    /// What a successful revoke says.
    static func revokedMessage(name: String, gone: Bool, deleted: Bool) -> String {
        var s = gone ? "“\(name)” was already revoked or expired." : "Revoked “\(name)”."
        if deleted { s += " Agents on this Mac no longer have a token." }
        return s
    }

    /// Days for a new token, kept in 1...maxDays.
    static func clampDays(_ days: Int) -> Int { min(max(days, 1), maxDays) }

    /// The bundled juicemount-mcp in an app bundle, when it is there.
    static func bundledBinary(bundleURL: URL,
                              isExecutable: (String) -> Bool = { FileManager.default.isExecutableFile(atPath: $0) }) -> String? {
        let path = bundleURL.appendingPathComponent("Contents/MacOS/\(binaryName)").path
        return isExecutable(path) ? path : nil
    }

    /// Claude Desktop's config (claude_desktop_config.json) running the
    /// bundled server for the mount point. Never holds the token: serve
    /// reads it from the Keychain.
    static func claudeDesktopConfig(binary: String, mount: String) -> String {
        struct Server: Encodable { let command: String; let args: [String] }
        struct Config: Encodable { let mcpServers: [String: Server] }
        let config = Config(mcpServers: [serverName: Server(command: binary, args: ["serve", "--mount", mount])])
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        guard let data = try? encoder.encode(config), let json = String(data: data, encoding: .utf8) else { return "" }
        return json
    }

    /// The Claude Code command that adds the bundled server.
    static func claudeCodeCommand(binary: String, mount: String) -> String {
        "claude mcp add \(serverName) -- \(shellQuote(binary)) serve --mount \(shellQuote(mount))"
    }

    /// A word for a POSIX shell: as is when it needs no quoting, else in
    /// single quotes.
    static func shellQuote(_ s: String) -> String {
        let plain = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./:@%+=,-")
        if !s.isEmpty, s.unicodeScalars.allSatisfy({ plain.contains($0) }) { return s }
        return "'" + s.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
}
