import Foundation
import Darwin

// Compile with the production UnmountHelperService.swift and
// UnmountHelperCommand.swift. All daemon, signature and ping I/O is injected.
private final class CommandFakeDaemon: UnmountHelperDaemon {
    var approval: UnmountHelperApproval
    var afterRegister: UnmountHelperApproval?
    var registerError: Error?
    var registrations = 0

    init(_ approval: UnmountHelperApproval, afterRegister: UnmountHelperApproval? = nil,
         registerError: Error? = nil) {
        self.approval = approval
        self.afterRegister = afterRegister
        self.registerError = registerError
    }

    func register() throws {
        registrations += 1
        if let afterRegister { approval = afterRegister }
        if let registerError { throw registerError }
    }
}

private struct RegisterBoom: LocalizedError {
    var errorDescription: String? { "synthetic register failure" }
}

@main
enum UnmountHelperCommandProbe {
    private struct Observation {
        let code: Int?
        let output: [String: Any]?
        let lines: Int
        let constructed: Int
        let trustCalls: Int
        let pings: Int
        let guiCalls: Int
        let registrations: Int

        func isState(_ state: String, approval: String? = nil) -> Bool {
            output?["state"] as? String == state &&
                (approval == nil || output?["approval"] as? String == approval)
        }
    }

    static func main() {
        var checks = 0
        var failures = 0
        func check(_ condition: Bool, _ description: String) {
            checks += 1
            if !condition { print("FAIL: \(description)"); failures += 1 }
        }
        func run(_ args: [String], status: UnmountHelperApproval = .notFound,
                 after: UnmountHelperApproval? = nil, error: Error? = nil,
                 trust: UnmountHelperDecision.AppTrust = .notarized,
                 pingProblem: String? = nil, env: [String: String] = [:]) -> Observation {
            let daemon = CommandFakeDaemon(status, afterRegister: after, registerError: error)
            var lines: [String] = []
            var constructed = 0
            var trustCalls = 0
            var pings = 0
            var guiCalls = 0
            let code = UnmountHelperCommand.dispatch(
                arguments: ["JuiceMount"] + args, environment: env,
                makeDaemon: { constructed += 1; return daemon },
                appTrust: { trustCalls += 1; return trust },
                ping: { pings += 1; return pingProblem },
                write: { lines.append($0) },
                launchGUI: { guiCalls += 1 })
            let output = lines.first.flatMap { try? JSONSerialization.jsonObject(with: Data($0.utf8)) as? [String: Any] }
            return Observation(code: code, output: output, lines: lines.count,
                               constructed: constructed, trustCalls: trustCalls,
                               pings: pings, guiCalls: guiCalls,
                               registrations: daemon.registrations)
        }

        var result = run([])
        check(result.code == nil && result.guiCalls == 1 && result.lines == 0 &&
              result.constructed == 0 && result.trustCalls == 0, "ordinary launch only enters GUI")
        for args in [["--eject-helper-status", "extra"], ["extra", "--prepare-eject-helper"],
                     ["--eject-helper-status", "--prepare-eject-helper"],
                     ["--prepare-eject-helper", "--prepare-eject-helper"]] {
            result = run(args)
            check(result.code == 1 && result.isState("invalid_arguments") && result.lines == 1 &&
                  result.guiCalls == 0 && result.constructed == 0 && result.trustCalls == 0,
                  "recognized flag with other arguments refused before GUI/I/O: \(args)")
        }

        result = run(["--eject-helper-status"])
        check(result.code == 2 && result.isState("unregistered", approval: "not_found") &&
              result.registrations == 0 && result.pings == 0 && result.guiCalls == 0,
              "status reports never-registered state without registering")
        result = run(["--eject-helper-status"], env: ["JM_APP_ISOLATION": "1"])
        check(result.code == 2 && result.isState("unregistered", approval: "not_found") &&
              result.registrations == 0 && result.constructed == 1 && result.guiCalls == 0,
              "isolated status remains truthful and never registers")
        result = run(["--eject-helper-status"], status: .requiresApproval)
        check(result.code == 2 && result.isState("requires_approval") && result.registrations == 0 &&
              result.output?["action"] as? String == "open_login_items",
              "status reports approval without registering")
        result = run(["--eject-helper-status"], status: .enabled)
        check(result.code == 0 && result.isState("ready", approval: "enabled") &&
              result.pings == 1 && result.registrations == 0,
              "status readiness requires an authenticated ping")
        result = run(["--eject-helper-status"], status: .enabled, pingProblem: "no reply")
        check(result.code == 1 && result.isState("not_responding", approval: "enabled") &&
              result.pings == 1 && result.registrations == 0,
              "enabled without ping is not ready")

        for env in [["JM_APP_ISOLATION": ""], ["JM_APP_ISOLATION_ROOT": "/owned"],
                    ["JM_APP_ISOLATION_FUSE_METRICS_ADDR": "127.0.0.1:1"]] {
            result = run(["--prepare-eject-helper"], env: env)
            check(result.code == 1 && result.isState("isolation_refused") &&
                  result.constructed == 0 && result.trustCalls == 0 && result.guiCalls == 0,
                  "any isolation configuration refuses prepare before I/O")
        }
        for trust in [UnmountHelperDecision.AppTrust.adhocOrForeign, .unknown] {
            result = run(["--prepare-eject-helper"], trust: trust)
            check(result.code == 1 && result.isState("trust_refused") &&
                  result.constructed == 0 && result.registrations == 0,
                  "untrusted prepare refuses before daemon construction")
        }
        result = run(["--prepare-eject-helper"], status: .enabled, trust: .developerIDNotNotarized)
        check(result.code == 0 && result.isState("ready") && result.pings == 1 &&
              result.registrations == 0, "Developer ID build may use enabled helper")
        result = run(["--prepare-eject-helper"], status: .requiresApproval)
        check(result.code == 2 && result.isState("requires_approval") && result.registrations == 0,
              "prepare does not register while approval is pending")
        result = run(["--prepare-eject-helper"], status: .notFound, after: .requiresApproval)
        check(result.code == 2 && result.isState("requires_approval") && result.registrations == 1 &&
              result.pings == 0, "absent helper registers once then rereads approval")
        result = run(["--prepare-eject-helper"], status: .notRegistered, after: .enabled)
        check(result.code == 0 && result.isState("ready") && result.registrations == 1 &&
              result.pings == 1, "registered and enabled helper must answer ping")
        result = run(["--prepare-eject-helper"], status: .notFound, after: .notFound)
        check(result.code == 1 && result.isState("registration_failed", approval: "not_found") &&
              result.registrations == 1, "unchanged status after one register is failure")
        result = run(["--prepare-eject-helper"], status: .notFound, after: .enabled,
                     error: RegisterBoom())
        check(result.code == 1 && result.isState("registration_failed", approval: "enabled") &&
              result.registrations == 1 && result.pings == 0,
              "thrown registration reports reread status without claiming readiness")
        result = run(["--prepare-eject-helper"], status: .enabled, pingProblem: "timeout")
        check(result.code == 1 && result.isState("not_responding", approval: "enabled") &&
              result.registrations == 0 && result.pings == 1,
              "enabled helper with failed ping is not re-registered")
        result = run(["--prepare-eject-helper"], status: .unknown)
        check(result.code == 1 && result.isState("unknown_status") && result.registrations == 0,
              "unknown status cannot register or claim ready")

        if failures > 0 { print("FAIL: \(failures) of \(checks) command checks"); exit(1) }
        print("PASS: \(checks) eject-helper command checks")
    }
}
