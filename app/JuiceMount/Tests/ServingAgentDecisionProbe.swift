import ServiceManagement

// Compile with the production Core/ServingAgentDecision.swift:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/ServingAgentDecision.swift \
//     app/JuiceMount/Tests/ServingAgentDecisionProbe.swift -o <scratch>/probe && <scratch>/probe
@main
struct ServingAgentDecisionProbe {
    static func main() {
        var failures = 0
        func expect(_ got: ServingAgentStep, _ want: ServingAgentStep, _ what: String) {
            if got != want { print("FAIL: \(what): got \(got), want \(want)"); failures += 1 }
        }
        // First launch of an installed build: macOS reports .notFound before
        // the agent was ever registered. It must register, not refuse.
        expect(ServingAgentDecision.step(for: .notFound, attemptedRegister: false), .register, "never-registered agent (.notFound)")
        expect(ServingAgentDecision.step(for: .notRegistered, attemptedRegister: false), .register, "unregistered agent")
        expect(ServingAgentDecision.step(for: .enabled, attemptedRegister: false), .ready, "enabled agent")
        if case .fail = ServingAgentDecision.step(for: .notFound, attemptedRegister: true) {} else {
            print("FAIL: .notFound after a registration attempt must fail, not loop"); failures += 1
        }
        if case .fail = ServingAgentDecision.step(for: .notRegistered, attemptedRegister: true) {} else {
            print("FAIL: .notRegistered after a registration attempt must fail, not loop"); failures += 1
        }
        if case .fail = ServingAgentDecision.step(for: .requiresApproval, attemptedRegister: true) {} else {
            print("FAIL: requiresApproval must surface the Login Items instruction"); failures += 1
        }
        if failures > 0 { exit(1) }
        print("PASS: serving agent decision")
    }
}
