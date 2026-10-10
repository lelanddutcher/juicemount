import ServiceManagement

/// What ensureAvailable does next for a given LaunchAgent status. Pure, so the
/// decision is checked by Tests/ServingAgentDecisionProbe.swift without
/// touching launchd.
enum ServingAgentStep: Equatable {
    case register
    case ready
    case fail(String)
}

enum ServingAgentDecision {
    /// macOS reports an agent that has never been registered as `.notFound`,
    /// not only `.notRegistered`. Treating `.notFound` as a missing bundle
    /// meant the first launch of every installed build refused to start and
    /// told the user to reinstall. Both states get exactly one registration
    /// attempt; only a status that survives that attempt is a failure.
    static func step(for status: SMAppService.Status, attemptedRegister: Bool) -> ServingAgentStep {
        switch status {
        case .enabled:
            return .ready
        case .notRegistered, .notFound:
            if !attemptedRegister { return .register }
            return .fail(status == .notFound
                ? "macOS could not find the JuiceMount background service in this app bundle. Reinstall the complete app bundle."
                : "The JuiceMount background service could not be registered.")
        case .requiresApproval:
            return .fail("Allow JuiceMount in System Settings → General → Login Items & Extensions to start its background service.")
        @unknown default:
            return .fail("The JuiceMount background service has an unknown approval state.")
        }
    }
}
