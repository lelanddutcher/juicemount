import Foundation

/// Early, registration-only command surface for the bundled eject helper.
/// Every recognized invocation writes one JSON line. Exit codes are stable:
/// 0 = enabled and authenticated ping succeeded; 2 = registration or user
/// approval still needed; 1 = invalid arguments, refusal, or failed operation.
/// These commands never open Settings, unregister, toggle a service, or mount.
enum UnmountHelperCommand {
    // Match the GUI peer identity before any global registration effect.
    static let appRequirement = UnmountHelperDecision.appDeveloperIDRequirement +
        #" and identifier "com.juicemount.app" and !(entitlement["com.apple.security.get-task-allow"] exists)"#

    struct Result: Encodable {
        let command: String
        let state: String
        let approval: String
        let trust: String
        let detail: String?
        let action: String?
    }

    private static let statusFlag = "--eject-helper-status"
    private static let prepareFlag = "--prepare-eject-helper"

    @discardableResult
    static func dispatch(
        arguments: [String],
        environment: [String: String],
        makeDaemon: () -> UnmountHelperDaemon,
        appTrust: () -> UnmountHelperDecision.AppTrust,
        ping: () -> String?,
        write: (String) -> Void,
        launchGUI: () -> Void
    ) -> Int? {
        let supplied = Array(arguments.dropFirst())
        guard supplied.contains(statusFlag) || supplied.contains(prepareFlag) else {
            launchGUI()
            return nil
        }
        func emit(_ command: String, _ state: String, _ approval: String = "unread",
                  _ trust: String = "unread", _ detail: String? = nil,
                  action: String? = nil, code: Int) -> Int {
            let result = Result(command: command, state: state, approval: approval,
                                trust: trust, detail: detail, action: action)
            // A plain String payload has no unencodable values. The fallback
            // still reports failure as JSON if a future field breaks encoding.
            if let data = try? JSONEncoder().encode(result), let line = String(data: data, encoding: .utf8) {
                write(line)
                return code
            }
            write(#"{"command":"invalid","state":"serialization_failed","approval":"unread","trust":"unread"}"#)
            return 1
        }
        guard supplied.count == 1, supplied[0] == statusFlag || supplied[0] == prepareFlag else {
            return emit("invalid", "invalid_arguments", code: 1)
        }
        let command = supplied[0] == statusFlag ? "status" : "prepare"
        if command == "prepare" && environment.keys.contains(where: { $0.hasPrefix("JM_APP_ISOLATION") }) {
            return emit(command, "isolation_refused", "unread", "unread", "The isolated app cannot register the production eject helper.", code: 1)
        }

        let trust = appTrust()
        let trustName: String
        switch trust {
        case .notarized: trustName = "notarized"
        case .developerIDNotNotarized: trustName = "developer_id"
        case .adhocOrForeign: trustName = "untrusted"
        case .unknown: trustName = "unknown"
        }
        if command == "prepare" && (trust == .adhocOrForeign || trust == .unknown) {
            return emit(command, "trust_refused", "unread", trustName,
                        "A Developer ID signature from JuiceMount is required.", code: 1)
        }

        let daemon = makeDaemon()
        func approvalName(_ value: UnmountHelperApproval) -> String {
            switch value {
            case .enabled: return "enabled"
            case .requiresApproval: return "requires_approval"
            case .notRegistered: return "not_registered"
            case .notFound: return "not_found"
            case .unknown: return "unknown"
            }
        }
        var approval = daemon.approval
        if command == "prepare" && (approval == .notRegistered || approval == .notFound) {
            do {
                try daemon.register() // one attempt only; never re-register an enabled helper
            } catch {
                approval = daemon.approval // truthfully report the state after a thrown attempt
                return emit(command, "registration_failed", approvalName(approval), trustName,
                            error.localizedDescription, code: 1)
            }
            approval = daemon.approval
            if approval == .notRegistered || approval == .notFound {
                return emit(command, "registration_failed", approvalName(approval), trustName,
                            "Registration returned without changing helper status.", code: 1)
            }
        }
        switch approval {
        case .enabled:
            if trust == .adhocOrForeign || trust == .unknown {
                return emit(command, "trust_refused", approvalName(approval), trustName,
                            "This app signature cannot authenticate to the helper.", code: 1)
            }
            if let problem = ping() {
                return emit(command, "not_responding", approvalName(approval), trustName, problem, code: 1)
            }
            return emit(command, "ready", approvalName(approval), trustName, code: 0)
        case .requiresApproval:
            return emit(command, "requires_approval", approvalName(approval), trustName,
                        "Turn on JuiceMount in System Settings → General → Login Items & Extensions.",
                        action: "open_login_items", code: 2)
        case .notRegistered, .notFound:
            return emit(command, "unregistered", approvalName(approval), trustName, code: 2)
        case .unknown:
            return emit(command, "unknown_status", approvalName(approval), trustName, code: 1)
        }
    }
}
