import Foundation
import JuiceMountCore
import Observation

/// Settings' Agents section: this person's agent tokens, minted, listed and
/// revoked through the bridge (bridge/agenttokens.go) as this Mac, with the
/// enrollment record the app keeps in its Keychain slot. The bridge stores a
/// minted token in the Keychain item the bundled juicemount-mcp reads; the
/// token never reaches Swift, so nothing here can show or copy it.
@Observable
final class AgentTokensModel {
    var list: AgentAccess.ListState = .notLoaded
    var tokens: [AgentAccess.Token] = []
    /// The token stored on this Mac for juicemount-mcp ("" = none).
    var storedID = ""
    /// A create or revoke is running.
    var busy = false
    /// The last create or revoke outcome (never a secret).
    var status: String?
    var statusIsError = false
    /// The Manager address is plain http to another host.
    var warning: String?
    /// A token a failed create removed from this Mac, still live on the
    /// Manager (its row says so, so it can be revoked).
    var removedHereID = ""
    /// A list call is running.
    private var listing = false

    /// The bridge's exports; a probe substitutes them.
    static var createCall: (String) -> String? = { json in AgentTokensModel.call(json) { NFSServerAgentTokenCreate($0) } }
    static var listCall: (String) -> String? = { json in AgentTokensModel.call(json) { NFSServerAgentTokenList($0) } }
    static var revokeCall: (String) -> String? = { json in AgentTokensModel.call(json) { NFSServerAgentTokenRevoke($0) } }
    /// This Mac's enrollment record (Keychain); a probe substitutes it.
    static var enrollmentRecord: () -> EnrollmentRecord? = {
        EnrollmentRecord.load(Preferences.enrollmentStore.password(for: EnrollmentRecord.account))
    }

    /// What the bridge needs of the enrollment record (never its secret).
    private static func macState(_ r: EnrollmentRecord) -> AgentAccess.MacState {
        AgentAccess.MacState(managerURL: r.managerURL, instanceID: r.instanceID, pollToken: r.pollToken, status: r.status)
    }

    /// Bridge calls make Manager requests: never on the main thread.
    private static let queue = DispatchQueue(label: "com.juicemount.agent-tokens")

    private static func call(_ json: String, _ export: (UnsafeMutablePointer<CChar>?) -> UnsafeMutablePointer<CChar>?) -> String? {
        var input = Array(json.utf8CString)
        return input.withUnsafeMutableBufferPointer { buf in
            guard let ptr = buf.baseAddress, let result = export(ptr) else { return nil }
            defer { NFSServerFreeString(result) }
            return String(cString: result)
        }
    }

    private struct Input: Encodable {
        let state: AgentAccess.MacState
        var name: String?
        var days: Int?
        var id: String?
    }

    /// Runs one bridge call on the queue and hands its output to the main
    /// thread. No enrollment record is a not_enrolled failure.
    private func run(_ export: @escaping (String) -> String?, name: String? = nil, days: Int? = nil, id: String? = nil,
                     then: @escaping (AgentAccess.Output?, String?) -> Void) {
        Self.queue.async {
            var out: AgentAccess.Output?
            var code: String?
            if let record = Self.enrollmentRecord(),
               let data = try? JSONEncoder().encode(Input(state: Self.macState(record), name: name, days: days, id: id)),
               let json = String(data: data, encoding: .utf8) {
                out = AgentAccess.Output.decode(export(json))
                if out == nil { code = "" }
            } else {
                code = "not_enrolled"
            }
            DispatchQueue.main.async { then(out, code) }
        }
    }

    /// Lists the tokens once, unless the list is loaded or loading.
    func refreshIfNeeded() {
        switch list {
        case .notLoaded, .failed: refresh()
        case .loading, .loaded: break
        }
    }

    /// Lists this person's tokens; tells whether the Manager has agent
    /// access on.
    func refresh() {
        guard !listing else { return }
        listing = true
        if case .loaded = list {} else { list = .loading }
        run(Self.listCall) { [weak self] out, code in
            guard let self else { return }
            self.listing = false
            guard let out else {
                self.list = .failed(code: code ?? "", detail: code == "" ? "the bridge returned no result" : "")
                return
            }
            guard out.ok else {
                self.list = .failed(code: out.error ?? "", detail: out.detail ?? "")
                return
            }
            self.list = .loaded(continuity: out.continuity)
            self.tokens = out.continuity ? out.tokens : []
            self.storedID = out.storedID
            self.warning = out.warning
        }
    }

    /// Mints a token and has the bridge store it for juicemount-mcp.
    func create(name: String, days: Int) {
        guard !busy else { return }
        let name = name.trimmingCharacters(in: .whitespacesAndNewlines)
        busy = true
        status = nil
        run(Self.createCall, name: name, days: AgentAccess.clampDays(days)) { [weak self] out, code in
            guard let self else { return }
            self.busy = false
            if let out, out.ok {
                self.removedHereID = ""
                self.finish(AgentAccess.createdMessage(name: out.token?.name ?? name, replacedID: out.replacedID), error: false)
            } else {
                let removed = out?.previousRemoved ?? false
                let replaced = out?.replacedID ?? ""
                if removed {
                    self.storedID = ""
                    self.removedHereID = replaced
                }
                let replacedName = replaced.isEmpty ? nil : self.tokens.first(where: { $0.id == replaced })?.name
                self.finish(AgentAccess.createFailedMessage(code: out?.error ?? code, detail: out?.detail,
                                                            previousRemoved: removed, replacedName: replacedName), error: true)
            }
            if let w = out?.warning { self.warning = w }
            self.refresh()
        }
    }

    /// Revokes a token on the Manager; the bridge deletes it here too when
    /// it is the one stored.
    func revoke(_ token: AgentAccess.Token) {
        guard !busy else { return }
        busy = true
        status = nil
        run(Self.revokeCall, id: token.id) { [weak self] out, code in
            guard let self else { return }
            self.busy = false
            if let out, out.ok {
                if token.id == self.removedHereID { self.removedHereID = "" }
                self.finish(AgentAccess.revokedMessage(name: token.name, gone: out.gone, deleted: out.deleted), error: false)
            } else {
                self.finish(AgentAccess.message(code: out?.error ?? code, detail: out?.detail), error: true)
            }
            self.refresh()
        }
    }

    private func finish(_ message: String, error: Bool) {
        status = message
        statusIsError = error
    }
}
