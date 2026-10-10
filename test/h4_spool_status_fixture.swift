import Foundation

// This fixture compiles the production SpoolStatus.swift without the c-archive.
public enum NFSBridge {
    public enum SpoolSafetyState {
        case verifiedEmpty
        case pending
        case unknown
    }

}

@main
struct H4SpoolStatusFixture {
    static func classify(_ json: String, httpStatus: Int) -> String {
        guard var status = try? JSONDecoder().decode(NFSBridge.SpoolStatus.self, from: Data(json.utf8)) else {
            return "unknown"
        }
        status.httpStatusCode = httpStatus
        switch status.safetyState {
        case .verifiedEmpty: return "empty"
        case .pending: return "pending"
        case .unknown: return "unknown"
        }
    }

    static func main() {
        let empty = """
        {"enabled":true,"pending_files":0,"pending_symlinks":0,"pending_bytes":0,
         "in_progress":0,"stalled_files":0,"failed_files":0,"declined_files":0,"entries":[]}
        """
        let pendingLink = empty.replacingOccurrences(of: "\"pending_symlinks\":0", with: "\"pending_symlinks\":1")
        let oldBridge = empty.replacingOccurrences(of: "\"pending_symlinks\":0,", with: "")
        let failure = empty.replacingOccurrences(of: "\"enabled\":true", with: "\"enabled\":true,\"error\":\"pending symlinks: SQL failure\"")
        let cases: [(String, String, Int, String)] = [
            ("complete empty", empty, 200, "empty"),
            ("pending-only link", pendingLink, 200, "pending"),
            ("old bridge missing key", oldBridge, 200, "unknown"),
            ("server error", failure, 500, "unknown"),
            ("disabled", empty, 503, "unknown"),
            ("malformed", "{", 200, "unknown"),
        ]
        for (name, json, httpStatus, want) in cases {
            let got = classify(json, httpStatus: httpStatus)
            precondition(got == want, "\(name): \(got), want \(want)")
        }
        print("H4 Swift spool status fixture: \(cases.count) cases passed")
    }
}
