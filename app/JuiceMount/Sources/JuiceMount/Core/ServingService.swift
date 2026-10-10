import Foundation
import ServiceManagement
import JuiceMountCore

/// The LaunchAgent owns storage independently of the menu bar process. Closing
/// the GUI never unregisters it. A future privileged broker owns kernel mount
/// custody; this per-user service owns the existing NFS/Link/FUSE runtime.
enum ServingService {
    private static let lock = NSLock()
    private static let agent = SMAppService.agent(plistName: "com.juicemount.serving.plist")

    enum Failure: LocalizedError {
        case unavailable(String)
        var errorDescription: String? {
            switch self { case .unavailable(let message): return message }
        }
    }

    /// Read-only LaunchAgent approval state for the Setup Assistant's
    /// "Background service" row. Reading status never registers anything.
    static var approvalStatus: SMAppService.Status { agent.status }

    /// Result of asking for a force-eject of the busy volume on Quit.
    typealias EjectRequestResult = UnmountHelperDecision.EjectResult

    /// Whether requestEjectNow() can actually force-eject, so the busy-Quit
    /// panel may count down to a force (founder decision, 0.5). True only
    /// when the privileged helper answers a status ping AND the serving
    /// service reports that its eject-now route can reach the helper. Until
    /// the service ships that route the panel keeps telling the truth: no
    /// countdown, "Quit anyway". Blocking (bounded); call off the main thread.
    static func forceEjectAvailable(metricsAddr: String) -> Bool {
        // Isolation normally refuses helper use. A private, validated marker
        // permits only this read-only check of an already registered helper.
        if let scope = AppIsolation.active, !scope.permitsExistingHelperForBusyQuit() {
            return false
        }
        // Reuse a good ping up to 10 s old (the popover refreshes it), else
        // one bounded attempt: the Quit panel waits on this.
        let helper = UnmountHelperService.shared.readiness(
            isolated: false, cacheMaxAge: 10, attempts: 1)
        guard helper == .ready else { return false }
        return UnmountHelperDecision.forceEjectAvailable(
            helper: helper,
            serviceSupportsEjectNow: EjectNowHTTP.capability(metricsAddr: metricsAddr))
    }

    /// Typed seam for the founder-approved busy-Quit force-eject. The GUI
    /// never calls the helper's service channel: only the serving service
    /// (signing identity juicemount-service, the helper's "service" role)
    /// may Retire. This asks the SERVICE, which forwards to the helper's
    /// identity-bound retire (by fsid, never by path) with eject-now. A
    /// service build without the route answers 404, reported as
    /// `.unavailable` with nothing attempted. Blocking: call off the main
    /// thread.
    /// `phase` .start never forces; only .escalate may (after the floor).
    static func requestEjectNow(metricsAddr: String, phase: EjectNowPhase) -> EjectRequestResult {
        EjectNowHTTP.post(metricsAddr: metricsAddr, phase: phase)
    }

    /// Busy Quit abandoned after a POST: DELETE /eject-now withdraws the
    /// ordinary retire that POST started (a force is never withdrawn: 409).
    /// Blocking (bounded); call off the main thread.
    static func withdrawEjectNow(metricsAddr: String) -> UnmountHelperDecision.WithdrawResult {
        EjectNowHTTP.withdraw(metricsAddr: metricsAddr)
    }

    /// Founder Q1: a NEW mount needs the privileged helper approved and
    /// answering. Throws the actionable refusal; RemediationAlert recognises
    /// it and offers "Open Login Items…". Never touches a running mount.
    /// Throws UnmountHelperGateError (its text starts with the gate prefix).
    static func ensureHelperAllowsMount() throws {
        try UnmountHelperService.shared.ensureAllowsMount(
            isolated: AppIsolation.active != nil,
            mountActive: NFSServerIsRunning() != 0)
    }

    /// Called on a background work queue before Start or Link pairing. Readiness
    /// probes are safe to repeat; the actual state-changing call is sent once.
    ///
    /// `forMount` (default true) also applies the helper gate
    /// (ensureHelperAllowsMount) after the service is ready. The Link test
    /// mounts nothing and passes false.
    static func ensureAvailable(forMount: Bool = true) throws {
        try UnmountHelperDecision.runAvailabilitySequence(
            forMount: forMount,
            service: ensureServiceAvailable,
            helper: ensureHelperAllowsMount)
    }

    private static func ensureServiceAvailable() throws {
        lock.lock()
        defer { lock.unlock() }
        guard NFSServerUsesServingService() != 0 else {
            throw Failure.unavailable("This app is missing its background-service control bridge.")
        }
        if AppIsolation.active != nil {
            // Acceptance service is started with the private HOME by its
            // harness. Registering the ordinary LaunchAgent would target the
            // production socket and production HOME.
            let deadline = ProcessInfo.processInfo.systemUptime + 5
            repeat {
                if NFSServerServiceReady() != 0 { return }
                Thread.sleep(forTimeInterval: 0.05)
            } while ProcessInfo.processInfo.systemUptime < deadline
            throw Failure.unavailable("The isolated background service is unavailable; Start was not sent.")
        }
        var attemptedRegister = false
        var registerError: Error?
        decide: while true {
            switch ServingAgentDecision.step(for: agent.status, attemptedRegister: attemptedRegister) {
            case .ready:
                break decide
            case .register:
                attemptedRegister = true
                // A simultaneous registration may have succeeded; the next
                // pass inspects the actual status instead of retrying blindly.
                do { try agent.register() } catch { registerError = error }
            case .fail(let message):
                if let registerError {
                    throw Failure.unavailable("\(message) (\(registerError.localizedDescription))")
                }
                throw Failure.unavailable(message)
            }
        }
        let deadline = ProcessInfo.processInfo.systemUptime + 5
        repeat {
            if NFSServerServiceReady() != 0 { return }
            Thread.sleep(forTimeInterval: 0.05)
        } while ProcessInfo.processInfo.systemUptime < deadline
        throw Failure.unavailable("The background service is unavailable or belongs to another app build. Existing storage has been retained; Start was not sent.")
    }
}
