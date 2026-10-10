import Foundation

// Compile this file with the production Core/SpoolStatus.swift. NFSBridge's
// network/FFI implementation is intentionally replaced only by the minimal
// type shell needed to compile that exact production decoder and decision.
public enum NFSBridge {
    public enum SpoolSafetyState { case verifiedEmpty, pending, unknown }
}

@main
struct SpoolStatusDecisionProbe {
    static func main() throws {
        let complete = """
        {"enabled":true,"pending_files":0,"pending_symlinks":0,
         "pending_bytes":0,"in_progress":0,"stalled_files":0,
         "failed_files":0,"declined_files":0,"entries":[]}
        """
        let disabled = complete.replacingOccurrences(of: "\"enabled\":true", with: "\"enabled\":false")
            .replacingOccurrences(of: "\"entries\":[]", with: "\"entries\":[],\"error\":\"spool not enabled\"")
        let nullEnabled = disabled.replacingOccurrences(of: "\"enabled\":false", with: "\"enabled\":null")
        let missingEntries = """
        {"enabled":false,"pending_files":0,"pending_symlinks":0,
         "pending_bytes":0,"in_progress":0,"stalled_files":0,
         "failed_files":0,"declined_files":0,"error":"spool not enabled"}
        """
        let cases: [(String, String?, Int?, Bool?)] = [
            ("complete 200 enabled", complete, 200, true),
            ("complete 503 disabled with intentional error", disabled, 503, false),
            ("503 empty object", "{}", 503, nil),
            ("503 null enabled", nullEnabled, 503, nil),
            ("503 missing entries", missingEntries, 503, nil),
            ("503 undecodable", "{", 503, nil),
            ("503 enabled true", complete, 503, nil),
            ("200 disabled", disabled, 200, nil),
            ("500 complete disabled", disabled, 500, nil),
            ("unreachable", nil, nil, nil),
        ]
        for (name, body, code, expected) in cases {
            var status = body.flatMap { try? JSONDecoder().decode(NFSBridge.SpoolStatus.self, from: Data($0.utf8)) }
            status?.httpStatusCode = code
            let actual = status?.effectiveEnabledForCurrentResponse
            guard actual == expected else {
                fatalError("\(name): expected \(String(describing: expected)), got \(String(describing: actual))")
            }
            print("PASS \(name)")
        }
        let recoveryCases: [(String, String?, Int?, Bool?)] = [
            ("complete current recovery on", complete.replacingOccurrences(of: "\"entries\":[]", with: "\"recovery_enabled\":true,\"entries\":[]"), 200, true),
            ("complete current recovery off", complete.replacingOccurrences(of: "\"entries\":[]", with: "\"recovery_enabled\":false,\"entries\":[]"), 200, false),
            ("old provider omits recovery", complete, 200, nil),
            ("disabled spool reports off", disabled.replacingOccurrences(of: "\"entries\":[]", with: "\"recovery_enabled\":false,\"entries\":[]"), 503, false),
            ("failed recovery response", complete.replacingOccurrences(of: "\"entries\":[]", with: "\"recovery_enabled\":true,\"entries\":[]"), 500, nil),
            ("malformed recovery Boolean", complete.replacingOccurrences(of: "\"entries\":[]", with: "\"recovery_enabled\":1,\"entries\":[]"), 200, nil),
            ("unreachable recovery", nil, nil, nil),
        ]
        for (name, body, code, expected) in recoveryCases {
            var status = body.flatMap { try? JSONDecoder().decode(NFSBridge.SpoolStatus.self, from: Data($0.utf8)) }
            status?.httpStatusCode = code
            guard status?.effectiveRecoveryEnabledForCurrentResponse == expected else {
                fatalError("\(name): recovery state was not bounded to the current response")
            }
            print("PASS \(name)")
        }
        print("RESULT declared=17 executed=17 failures=0")
    }
}
