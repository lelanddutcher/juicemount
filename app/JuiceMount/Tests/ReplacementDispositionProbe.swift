import Foundation

// Compile with the exact production Core/SpoolStatus.swift, including its
// real SpoolEntryView decoder. No network/FFI implementation is needed.
public enum NFSBridge {
    public enum SpoolSafetyState { case verifiedEmpty, pending, unknown }
}

@main
struct ReplacementDispositionProbe {
    static func decode(_ data: Data) throws -> NFSBridge.SpoolStatus {
        var status = try JSONDecoder().decode(NFSBridge.SpoolStatus.self, from: data)
        status.httpStatusCode = 200
        return status
    }

    static func check(_ status: NFSBridge.SpoolStatus, state: String) {
        let waiting = state == "waiting_writer"
        precondition(status.enabled && status.hasActivity)
        precondition(status.safetyState == .pending)
        precondition(status.needsAttention == !waiting)
        precondition(status.failedFiles == 0 && status.stalledFiles == 0)
        precondition(status.replacementWaiting == (waiting ? 1 : 0))
        precondition(status.replacementConflicts == (waiting ? 0 : 1))
        guard let entry = status.entries.first(where: { $0.replacementState == state }) else {
            fatalError("missing expected replacement state")
        }
        precondition(entry.drainState == "ready" && entry.size > 0 && !entry.stalled)
        precondition(entry.replacementIsWaiting == waiting)
        precondition(entry.replacementNeedsAttention == !waiting)
        precondition(entry.replacementStatusLabel == (waiting ? "Waiting for writer" : "Save needs review"))
        precondition(!(entry.lastError ?? "").isEmpty)
    }

    static func main() throws {
        let base: [String: Any] = ["enabled": true, "pending_files": 0,
            "pending_symlinks": 0, "pending_bytes": 0, "in_progress": 0,
            "stalled_files": 0, "failed_files": 0, "declined_files": 0, "entries": []]
        var cases = 0
        let legacy = try decode(JSONSerialization.data(withJSONObject: base))
        precondition(legacy.safetyState == .verifiedEmpty && !legacy.needsAttention)
        precondition(legacy.replacementConflicts == 0 && legacy.replacementWaiting == 0)
        cases += 1
        for state in ["waiting_writer", "writer_changed"] {
            var object = base
            object["pending_files"] = 1
            object["pending_bytes"] = 23
            object["replacement_waiting"] = state == "waiting_writer" ? 1 : 0
            object["replacement_conflicts"] = state == "writer_changed" ? 1 : 0
            object["entries"] = [["path": "media/project/save.mov", "size": 23,
                "drain_state": "ready", "replacement_state": state,
                "last_error": "Your save is kept on this Mac."]]
            check(try decode(JSONSerialization.data(withJSONObject: object)), state: state)
            cases += 1
        }
        var nulls = base
        nulls["replacement_conflicts"] = NSNull()
        nulls["replacement_waiting"] = NSNull()
        nulls["entries"] = [["replacement_state": NSNull()]]
        let absent = try decode(JSONSerialization.data(withJSONObject: nulls))
        precondition(absent.replacementConflicts == 0 && absent.replacementWaiting == 0)
        precondition(absent.entries[0].replacementState == nil)
        cases += 1
        for key in ["replacement_conflicts", "replacement_waiting"] {
            var partial = base
            partial[key] = 1
            let status = try decode(JSONSerialization.data(withJSONObject: partial))
            precondition(status.hasActivity && status.safetyState == .pending)
            cases += 1
        }
        // An additive future value must not make the entire queue undecodable.
        var future = base
        future["pending_files"] = 1
        future["entries"] = [["replacement_state": "future_state", "drain_state": "ready"]]
        let unknown = try decode(JSONSerialization.data(withJSONObject: future))
        precondition(unknown.entries[0].replacementState == "future_state")
        precondition(unknown.hasActivity && unknown.safetyState == .pending)
        cases += 1
        let availabilityLabels = [
            "recovery_disabled": "Saved locally · recovery off",
            "offline": "Saved locally · offline",
            "queued": "Saved locally · waiting to recover",
            "provider_unavailable": "Saved locally · provider unavailable",
            "finishing": "Finishing save",
        ]
        for (state, label) in availabilityLabels {
            var object = base
            object["pending_files"] = 1
            object["entries"] = [["path": "media/project/save.mov", "size": 23,
                "drain_state": "ready", "replacement_state": state]]
            let status = try decode(JSONSerialization.data(withJSONObject: object))
            precondition(status.entries[0].replacementStatusLabel == label)
            precondition(!status.entries[0].replacementNeedsAttention && !status.needsAttention)
            cases += 1
        }
        print("MODEL declared=12 executed=\(cases) failures=0")

        // Root acceptance supplies the exact JSON emitted by the actual Go
        // process-recovery tests, rather than a second hand-written wire model.
        let paths = Array(CommandLine.arguments.dropFirst())
        precondition(paths.count >= 2, "need actual waiting and conflict status JSON")
        for (file, state) in zip(paths.prefix(2), ["waiting_writer", "writer_changed"]) {
            check(try decode(Data(contentsOf: URL(fileURLWithPath: file))), state: state)
            print("GO_STATUS \(state) PASS")
        }
        for file in paths.dropFirst(2) {
            let status = try decode(Data(contentsOf: URL(fileURLWithPath: file)))
            if let writer = status.entries.first(where: { $0.replacementState == "waiting_writer" || $0.replacementState == "writer_changed" }),
               let state = writer.replacementState {
                check(status, state: state)
                print("GO_STATUS \(state) PASS")
                continue
            }
            guard let entry = status.entries.first(where: { availabilityLabels[$0.replacementState ?? ""] != nil }),
                  let expected = availabilityLabels[entry.replacementState ?? ""] else {
                fatalError("actual recovery availability status missing")
            }
            precondition(entry.replacementStatusLabel == expected && !entry.replacementNeedsAttention)
            print("GO_STATUS \(entry.replacementState ?? "") PASS")
        }
        print("RESULT model=12 go_status=\(paths.count) failures=0")
    }
}
