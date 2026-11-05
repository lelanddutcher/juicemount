import Foundation

// Standalone source probe for Settings' Agents section (Core/AgentAccess.swift).
// It needs no bridge, Keychain or Manager:
//
//   swiftc -parse-as-library -o <scratch>/agent-access-probe \
//     Tests/AgentAccessProbe.swift Sources/JuiceMount/Core/AgentAccess.swift
//   <scratch>/agent-access-probe
//
// CI's mac-app job runs it. Checks: the Claude Desktop config and Claude Code
// command run the bundled binary with serve --mount <mount point> and are
// quoted for any path; the section is shown only when this Mac is approved
// and the Manager reports continuity on (else a short disabled note); the
// bridge's failure codes map to short plain messages; the bridge's output
// decodes, and nothing the section shows or copies is the token.
@main
struct AgentAccessProbe {
    static var failures = 0

    static func check(_ ok: Bool, _ what: String) {
        if !ok {
            failures += 1
            FileHandle.standardError.write(Data("FAIL: \(what)\n".utf8))
        }
    }

    static func main() {
        configSnippets()
        visibilityRules()
        errorMapping()
        bridgeOutput()
        reviewFixes()
        guard failures == 0 else {
            FileHandle.standardError.write(Data("AgentAccessProbe: \(failures) failure(s)\n".utf8))
            exit(1)
        }
        print("AgentAccessProbe: ok")
    }

    static func configSnippets() {
        let binary = "/Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp"
        let json = AgentAccess.claudeDesktopConfig(binary: binary, mount: "/Volumes/zpool")
        check(!json.contains("\\/"), "desktop config escapes slashes: \(json)")
        let object = (try? JSONSerialization.jsonObject(with: Data(json.utf8))) as? [String: Any]
        let servers = object?["mcpServers"] as? [String: Any]
        let server = servers?["juicemount"] as? [String: Any]
        check(object?.count == 1 && servers?.count == 1, "desktop config holds one server: \(json)")
        check(server?["command"] as? String == binary, "desktop config command: \(json)")
        check(server?["args"] as? [String] == ["serve", "--mount", "/Volumes/zpool"], "desktop config args: \(json)")
        check(server?.count == 2, "desktop config server has only command and args (no env, no token): \(json)")
        check(AgentAccess.claudeDesktopConfig(binary: binary, mount: "/Volumes/zpool") == json, "desktop config is stable")

        // A mount point with a space and a quote survives JSON as is.
        let odd = "/Volumes/Shared Media's"
        let oddJSON = AgentAccess.claudeDesktopConfig(binary: binary, mount: odd)
        let oddArgs = (((try? JSONSerialization.jsonObject(with: Data(oddJSON.utf8))) as? [String: Any])?["mcpServers"] as? [String: Any])
            .flatMap { ($0["juicemount"] as? [String: Any])?["args"] as? [String] }
        check(oddArgs == ["serve", "--mount", odd], "desktop config with an odd mount: \(oddJSON)")

        check(AgentAccess.claudeCodeCommand(binary: binary, mount: "/Volumes/zpool") ==
              "claude mcp add juicemount -- /Applications/JuiceMount.app/Contents/MacOS/juicemount-mcp serve --mount /Volumes/zpool",
              "claude code command: \(AgentAccess.claudeCodeCommand(binary: binary, mount: "/Volumes/zpool"))")
        let quoted = AgentAccess.claudeCodeCommand(binary: "/Users/ann/Apps/Juice Mount.app/Contents/MacOS/juicemount-mcp", mount: odd)
        check(quoted == "claude mcp add juicemount -- '/Users/ann/Apps/Juice Mount.app/Contents/MacOS/juicemount-mcp' serve --mount '/Volumes/Shared Media'\\''s'",
              "claude code command quoting: \(quoted)")
        check(AgentAccess.shellQuote("") == "''", "empty word is quoted")
        check(AgentAccess.shellQuote("$(rm -rf ~)") == "'$(rm -rf ~)'", "shell syntax is quoted")

        check(AgentAccess.bundledBinary(bundleURL: URL(fileURLWithPath: "/Applications/JuiceMount.app"), isExecutable: { _ in true }) == binary,
              "bundled binary path")
        check(AgentAccess.bundledBinary(bundleURL: URL(fileURLWithPath: "/Applications/JuiceMount.app"), isExecutable: { _ in false }) == nil,
              "no bundled binary in a build without it")
        check(AgentAccess.clampDays(0) == 1 && AgentAccess.clampDays(91) == 90 && AgentAccess.clampDays(30) == 30, "days clamp to 1...90")
    }

    static func visibilityRules() {
        typealias V = AgentAccess.Visibility
        // Not approved: a disabled note, whatever the list says.
        for list: AgentAccess.ListState in [.notLoaded, .loaded(continuity: true), .failed(code: "unreachable", detail: "")] {
            check(AgentAccess.visibility(enrollmentApproved: false, list: list) == V.disabled(AgentAccess.notApprovedNote),
                  "not approved, \(list)")
        }
        check(AgentAccess.visibility(enrollmentApproved: true, list: .notLoaded) == .loading, "approved, not loaded")
        check(AgentAccess.visibility(enrollmentApproved: true, list: .loading) == .loading, "approved, loading")
        check(AgentAccess.visibility(enrollmentApproved: true, list: .loaded(continuity: true)) == .shown, "continuity on")
        check(AgentAccess.visibility(enrollmentApproved: true, list: .loaded(continuity: false)) == .disabled(AgentAccess.continuityOffNote),
              "continuity off")
        check(AgentAccess.visibility(enrollmentApproved: true, list: .failed(code: "continuity_off", detail: "")) == .disabled(AgentAccess.continuityOffNote),
              "continuity off as a failure")
        check(AgentAccess.visibility(enrollmentApproved: true, list: .failed(code: "not_enrolled", detail: "")) == .disabled(AgentAccess.notApprovedNote),
              "the bridge saw no approval")
        if case .disabled(let note) = AgentAccess.visibility(enrollmentApproved: true, list: .failed(code: "refused", detail: "this Mac belongs to no person")) {
            check(note.contains("People page"), "refused note: \(note)")
        } else {
            check(false, "a refused Mac is a disabled note")
        }
        check(AgentAccess.visibility(enrollmentApproved: true, list: .failed(code: "unreachable", detail: "dial tcp: refused")) ==
              .failed("The Manager could not be reached."), "unreachable is a failure with Retry")
    }

    static func errorMapping() {
        let codes = ["not_enrolled", "refused", "continuity_off", "unreachable", "maybe_minted", "store_failed",
                     "store_failed_live", "keychain", "invalid", "manager", "", "something-new"]
        for code in codes {
            let m = AgentAccess.message(code: code, detail: "x")
            check(!m.isEmpty && m.count < 160 && !m.contains("jma_"), "message for \(code): \(m)")
        }
        check(AgentAccess.message(code: "maybe_minted", detail: nil).contains("may have been created"), "maybe minted says so")
        check(AgentAccess.message(code: "store_failed", detail: "keychain locked").contains("revoked"), "store failure says revoked")
        check(AgentAccess.message(code: "manager", detail: "Ann already has 10 agent tokens") ==
              "The Manager refused: Ann already has 10 agent tokens.", "manager refusal quotes the Manager")
        check(AgentAccess.message(code: "invalid", detail: "a token lasts 1-90 days (default 90)") ==
              "A token lasts 1-90 days (default 90).", "invalid input reads as a sentence")
        check(AgentAccess.message(code: nil, detail: nil) == "Something went wrong.", "no code, no detail")
        check(AgentAccess.createdMessage(name: "Claude", replacedID: "").hasPrefix("Created “Claude”."), "created")
        check(AgentAccess.createdMessage(name: "Claude", replacedID: "0123456789abcdef").contains("still works until you revoke it"),
              "created, replacing one")
        check(AgentAccess.revokedMessage(name: "cli", gone: true, deleted: false) == "“cli” was already revoked or expired.", "revoked, gone")
        check(AgentAccess.revokedMessage(name: "cli", gone: false, deleted: true).contains("no longer have a token"), "revoked, deleted here")
    }

    static func bridgeOutput() {
        let list = """
        {"ok":true,"continuity":true,"stored_id":"fedcba9876543210","tokens":[
          {"id":"fedcba9876543210","name":"Claude Desktop","person":"p1","person_name":"Ann","mac":"mac-5a0000007b3d",
           "created_at_ms":1790000000000,"expires_at_ms":1797776000000},
          {"id":"aaaaaaaaaaaaaaaa","name":"cli"}]}
        """
        guard let out = AgentAccess.Output.decode(list) else {
            check(false, "list output does not decode")
            return
        }
        check(out.ok && out.continuity && out.storedID == "fedcba9876543210" && out.tokens.count == 2, "list output: \(out)")
        check(out.tokens[0].created == Date(timeIntervalSince1970: 1_790_000_000) && out.tokens[1].expiresAtMS == 0, "token dates")
        let off = AgentAccess.Output.decode(#"{"ok":true,"continuity":false}"#)
        check(off?.ok == true && off?.continuity == false && off?.tokens.isEmpty == true, "continuity off output")
        let failed = AgentAccess.Output.decode(#"{"ok":false,"continuity":false,"error":"refused","detail":"this Mac belongs to no person"}"#)
        check(failed?.error == "refused" && failed?.detail == "this Mac belongs to no person", "failure output")
        check(AgentAccess.Output.decode("not json") == nil && AgentAccess.Output.decode(nil) == nil, "garbage is no output")
    }

    static func reviewFixes() {
        // Only what minting needs of the enrollment record goes to the
        // bridge: never the enrollment secret.
        let state = AgentAccess.MacState(managerURL: "http://nas.example:30190", instanceID: "I", pollToken: "P", status: "approved")
        let encoded = (try? JSONEncoder().encode(state)).flatMap { try? JSONSerialization.jsonObject(with: $0) } as? [String: String]
        check(encoded == ["manager_url": "http://nas.example:30190", "instance_id": "I", "poll_token": "P", "status": "approved"],
              "mac state keys: \(String(describing: encoded))")

        // Manager text loses control and format characters before display.
        check(AgentAccess.cleanText("cl\u{202E}aude\u{200B}\u{2066}x\u{07}y") == "claudex y", "cleanText: \(AgentAccess.cleanText("cl\u{202E}aude\u{200B}\u{2066}x\u{07}y"))")
        let named = AgentAccess.Output.decode(#"{"ok":true,"continuity":true,"tokens":[{"id":"aaaaaaaaaaaaaaaa","name":"evil\u202eexe.txt"}],"detail":"a\u200bb"}"#)
        check(named?.tokens.first?.name == "evilexe.txt" && named?.detail == "ab", "decoded names and details are cleaned: \(String(describing: named?.tokens))")

        // A failed create that removed the token stored here says so, and
        // that the removed token is still active on the Manager.
        let removed = AgentAccess.Output.decode(#"{"ok":false,"continuity":false,"error":"store_failed","previous_removed":true,"replaced_id":"1111111111111111"}"#)
        check(removed?.previousRemoved == true && removed?.replacedID == "1111111111111111", "previous_removed decodes")
        let m1 = AgentAccess.createFailedMessage(code: "store_failed", detail: nil, previousRemoved: true, replacedName: "Claude")
        check(m1.contains("no longer have a token") && m1.contains("“Claude”") && m1.contains("still active on the Manager"), "removed, named: \(m1)")
        let m2 = AgentAccess.createFailedMessage(code: "store_failed", detail: nil, previousRemoved: true, replacedName: nil)
        check(m2.contains("no longer have a token") && m2.contains("The token that was stored here is still active"), "removed, unknown: \(m2)")
        check(AgentAccess.createFailedMessage(code: "manager", detail: "x", previousRemoved: false, replacedName: "Claude") ==
              AgentAccess.message(code: "manager", detail: "x"), "nothing removed, nothing added")

        // The copied config names a resolved, absolute mount point.
        check(AgentAccess.resolvedMount("~/Media", home: "/Users/ann") == "/Users/ann/Media", "tilde: \(String(describing: AgentAccess.resolvedMount("~/Media", home: "/Users/ann")))")
        check(AgentAccess.resolvedMount("~", home: "/Users/ann") == "/Users/ann", "bare tilde")
        check(AgentAccess.resolvedMount(" /Volumes/zpool/ ", home: "/Users/ann") == "/Volumes/zpool", "trailing slash and spaces")
        check(AgentAccess.resolvedMount("/Volumes/a/../zpool", home: "/Users/ann") == "/Volumes/zpool", "dot-dot")
        check(AgentAccess.resolvedMount("", home: "/Users/ann") == "/Volumes/zpool", "empty is the default")
        check(AgentAccess.resolvedMount("Volumes/zpool", home: "/Users/ann") == nil, "relative is refused")
        check(AgentAccess.resolvedMount("~bob/x", home: "/Users/ann") == nil, "another user's tilde is refused")

        // A translocated app is told to move to Applications first.
        check(AgentAccess.isTranslocated(bundlePath: "/private/var/folders/xy/T/AppTranslocation/0A1B-2C3D/d/JuiceMount.app"), "translocated")
        check(!AgentAccess.isTranslocated(bundlePath: "/Applications/JuiceMount.app"), "installed")
    }
}
