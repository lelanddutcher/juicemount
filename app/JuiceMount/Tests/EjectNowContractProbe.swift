import Foundation

// The GUI half of the busy-Quit contract test (bridge/eject_now_gui_contract_test.go,
// TestEjectNowGUIContract). It drives the REAL EjectNowRunner and EjectNowHTTP
// client against the address the Go test serves the REAL handleEjectNowHTTP
// on, and prints KEY=VALUE lines the Go test asserts. Build (repo root):
//
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/UnmountHelperService.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/EjectNowRunner.swift \
//     app/JuiceMount/Tests/EjectNowContractProbe.swift -o <scratch>/ejcontract
//   JM_GUI_EJECT_PROBE=<scratch>/ejcontract go test ./bridge/ -run TestEjectNowGUIContract
//
// Scenarios (argv[1]); argv[2] is host:port:
//   eject               start, then "Eject now" after the start reply; runs to the outcome
//   cancel-after-post   start, then Cancel Quit after the start reply
//   cancel-before-post  Cancel Quit before the runner starts
//   cancel-during-force start, "Eject now", then Cancel Quit once the service
//                       reports a forced unmount in the kernel (/test/forced)

func describe(_ o: EjectNowRequester.Outcome?) -> String {
    switch o {
    case nil: return "none"
    case .retired?: return "retired"
    case .forcePending(let why)?: return "force_pending:\(why)"
    case .outcomeUnknown(let why)?: return "outcome_unknown:\(why)"
    case .startUnanswered(let why)?: return "start_unanswered:\(why)"
    case .notAttempted(let why)?: return "not_attempted:\(why)"
    case .failed(let why)?: return "failed:\(why)"
    case .abandoned(nil)?: return "abandoned:none"
    case .abandoned(.withdrawn?)?: return "abandoned:withdrawn"
    case .abandoned(.forceInProgress?)?: return "abandoned:force_in_progress"
    case .abandoned(.alreadyEjected?)?: return "abandoned:already_ejected"
    case .abandoned(.failed(let why)?)?: return "abandoned:failed:\(why)"
    }
}

func forcedAtService(_ addr: String) -> Bool {
    guard let url = URL(string: "http://\(addr)/test/forced"),
          let data = try? Data(contentsOf: url) else { return false }
    return String(decoding: data, as: UTF8.self) == "true"
}

@main
struct EjectNowContractProbe {
    @MainActor static func main() {
        let args = CommandLine.arguments
        guard args.count == 3 else { print("usage: probe <scenario> <host:port>"); exit(2) }
        let scenario = args[1], addr = args[2]
        var posts = 0, withdraws = 0
        var phases: [String] = []
        var postsAtCancel = -1
        var outcome: EjectNowRequester.Outcome?
        var panel: String?
        var replies = 0
        var cancelled = false

        print("CAPABILITY=\(EjectNowHTTP.capability(metricsAddr: addr))")
        let runner = EjectNowRunner(
            post: { phase in
                posts += 1
                phases.append(phase.rawValue)
                let r = await Task.detached { EjectNowHTTP.post(metricsAddr: addr, phase: phase) }.value
                replies += 1
                return r
            },
            withdraw: {
                withdraws += 1
                return await Task.detached { EjectNowHTTP.withdraw(metricsAddr: addr) }.value
            },
            log: { print("LOG \($0)") },
            onChange: { r in if let m = r.ejectingMessage { panel = m } },
            onDone: { outcome = $0 })

        func cancel() {
            guard !cancelled else { return }
            cancelled = true
            postsAtCancel = posts
            runner.cancel()
        }

        if scenario == "cancel-before-post" {
            cancel()
        } else {
            runner.start()
        }
        var escalated = false
        let deadline = Date().addingTimeInterval(45)
        while outcome == nil && Date() < deadline {
            RunLoop.main.run(mode: .default, before: Date().addingTimeInterval(0.05))
            switch scenario {
            case "eject":
                if replies >= 1 && !escalated { escalated = true; runner.escalate(userPressed: true) }
            case "cancel-after-post":
                if replies >= 1 { cancel() }
            case "cancel-during-force":
                if replies >= 1 && !escalated { escalated = true; runner.escalate(userPressed: true) }
                if escalated && !cancelled && forcedAtService(addr) { cancel() }
            default:
                break
            }
        }
        // Let any stray request the runner might still send show up.
        let settle = Date().addingTimeInterval(2.5)
        while Date() < settle { RunLoop.main.run(mode: .default, before: Date().addingTimeInterval(0.05)) }

        print("OUTCOME=\(describe(outcome))")
        print("POSTS=\(posts)")
        print("PHASES=\(phases.joined(separator: ","))")
        print("WITHDRAWS=\(withdraws)")
        print("POSTS_AT_CANCEL=\(postsAtCancel)")
        print("PANEL=\(panel ?? "")")
        exit(outcome == nil ? 1 : 0)
    }
}
