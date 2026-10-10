import Foundation
import Security
import ServiceManagement
import XPC

// H-3 L3: the GUI side of the privileged unmount helper (juicemount-unmountd).
//
// The helper is a LaunchDaemon shipped inside the bundle
// (Contents/Library/LaunchDaemons/com.juicemount.unmountd.plist) and
// registered with SMAppService.daemon. It retires our own mounts by fsid,
// never by path (internal/unmountd). The GUI has exactly three powers here:
//
//   1. register the daemon (and re-register it once after an app update);
//   2. open System Settings → Login Items & Extensions for the approval;
//   3. ping the helper's STATUS mach service to prove it is running.
//
// The GUI never talks to the helper's service channel and never asks it to
// retire anything. Only the serving service (role "service") may Retire; a
// Quit force-eject is a request to the SERVICE (ServingService.requestEjectNow).
//
// This file has two halves. `UnmountHelperDecision` is pure and is pinned by
// Tests/UnmountHelperDecisionProbe.swift. `UnmountHelperService` does the I/O
// behind the `UnmountHelperDaemon` seam, so the probes never register or
// launch the real daemon. This file imports no other app source, so a probe
// compiles it alone.

// MARK: - Pure decisions

/// SMAppService.Status, reduced to the cases the decisions use. `unknown`
/// covers any status a future macOS adds.
enum UnmountHelperApproval: Equatable {
    case enabled
    case requiresApproval
    case notRegistered
    /// macOS reports a daemon that was NEVER registered as `.notFound`
    /// (H-11 lesson): it means "register me", not "missing from the bundle".
    case notFound
    case unknown

    init(_ status: SMAppService.Status) {
        switch status {
        case .enabled: self = .enabled
        case .requiresApproval: self = .requiresApproval
        case .notRegistered: self = .notRegistered
        case .notFound: self = .notFound
        @unknown default: self = .unknown
        }
    }
}

/// What the helper can do for this Mac right now.
enum UnmountHelperReadiness: Equatable {
    /// Isolated acceptance install: the harness owns everything; never
    /// register the production daemon from here.
    case notApplicable
    /// Never registered. A Start (or the Setup Assistant button) registers it.
    case notRegistered
    /// Registered; the user has not turned it on in Login Items yet.
    case requiresApproval
    /// Turned on, but the status ping failed (launchd has not started it
    /// yet, or the bundle's helper is broken). The detail is for diagnostics.
    case notResponding(String)
    /// Turned on and answered a ping as the GUI role for this user.
    case ready
    /// Registration failed in a way the user cannot fix in Settings.
    case unavailable(String)
}

enum UnmountHelperRegistrationStep: Equatable {
    case register
    /// The app's CFBundleVersion changed since the last registration (a
    /// Sparkle update). Register again so launchd picks up the new bundle.
    case reregisterAfterUpdate
    /// Enabled: ping next.
    case ready
    case needsApproval
    case fail(String)
}

enum UnmountHelperStartGate: Equatable {
    case allow
    /// Start is refused. `offersApproval` adds an "Open Login Items…" button.
    case refuse(message: String, offersApproval: Bool)
}

/// One row in the Setup Assistant and the menu popover.
struct UnmountHelperRow: Equatable {
    enum Action: Equatable {
        case none
        /// Register the daemon, then open Login Items.
        case setUp
        case openLoginItems
    }
    enum Tone: Equatable { case ok, attention, failed, neutral }

    let title: String
    let detail: String
    let action: Action
    let tone: Tone
    /// False for isolated installs: the row is hidden.
    let visible: Bool
}

enum UnmountHelperDecision {
    static let rowTitle = "Eject helper"

    /// Every Start refusal begins with this, so RemediationAlert can recognise
    /// the gate from the error text alone and offer the approval button.
    static let gatePrefix = "JuiceMount needs its background helper"

    static let approvalMessage = gatePrefix + " to safely eject its volume if JuiceMount stops responding. Turn on JuiceMount in System Settings → General → Login Items & Extensions, then Start again."
    static let notRegisteredMessage = gatePrefix + ", but it is not set up yet. Click Open Login Items…, turn on JuiceMount, then Start again."
    static let notFoundMessage = gatePrefix + ", but macOS could not find it in this copy of JuiceMount. Reinstall the complete app from a signed download."
    static let registerFailedMessage = gatePrefix + ", but macOS could not register it."
    static let unknownStateMessage = gatePrefix + ", but macOS reported an unknown approval state for it."

    static func notRespondingMessage(_ detail: String) -> String {
        gatePrefix + ". It is turned on but did not answer (\(detail)). In System Settings → General → Login Items & Extensions, turn JuiceMount off and on again, then Start again."
    }

    /// Next registration step. One register attempt per call chain; a status
    /// that survives the attempt is final. A re-register for an update is
    /// skipped while a mount is active: the gate never touches a live volume,
    /// and the helper already restarts itself when its binary is replaced.
    static func registrationStep(
        status: UnmountHelperApproval,
        attemptedRegister: Bool,
        registeredBuild: String?,
        currentBuild: String,
        mountActive: Bool
    ) -> UnmountHelperRegistrationStep {
        switch status {
        case .enabled:
            if !attemptedRegister && !mountActive && registeredBuild != currentBuild {
                return .reregisterAfterUpdate
            }
            return .ready
        case .requiresApproval:
            return .needsApproval
        case .notRegistered, .notFound:
            if !attemptedRegister { return .register }
            return .fail(status == .notFound ? notFoundMessage : registerFailedMessage)
        case .unknown:
            return .fail(unknownStateMessage)
        }
    }

    /// Whether the build should be recorded as registered after an attempt.
    /// Only a status that ended `.enabled` counts; macOS answers a repeated
    /// register() with kSMErrorAlreadyRegistered, which is success here.
    static func shouldRecordBuild(statusAfterAttempt: UnmountHelperApproval) -> Bool {
        statusAfterAttempt == .enabled
    }

    /// Founder Q1: no new mount until the helper is approved AND answers.
    static func startGate(_ readiness: UnmountHelperReadiness) -> UnmountHelperStartGate {
        switch readiness {
        case .ready, .notApplicable:
            return .allow
        case .requiresApproval:
            return .refuse(message: approvalMessage, offersApproval: true)
        case .notRegistered:
            return .refuse(message: notRegisteredMessage, offersApproval: true)
        case .notResponding(let detail):
            return .refuse(message: notRespondingMessage(detail), offersApproval: true)
        case .unavailable(let message):
            return .refuse(message: message, offersApproval: false)
        }
    }

    static func isStartGateRefusal(_ text: String) -> Bool {
        text.hasPrefix(gatePrefix)
    }

    /// The busy-Quit countdown may promise a force-eject only when the helper
    /// answers AND the serving service says it can forward the eject.
    static func forceEjectAvailable(helper: UnmountHelperReadiness, serviceSupportsEjectNow: Bool) -> Bool {
        helper == .ready && serviceSupportsEjectNow
    }

    static func row(for readiness: UnmountHelperReadiness) -> UnmountHelperRow {
        switch readiness {
        case .notApplicable:
            return UnmountHelperRow(title: rowTitle, detail: "Isolated test install: the helper is not used.", action: .none, tone: .neutral, visible: false)
        case .ready:
            return UnmountHelperRow(title: rowTitle, detail: "On. JuiceMount can safely eject its own volume.", action: .none, tone: .ok, visible: true)
        case .requiresApproval:
            return UnmountHelperRow(title: rowTitle, detail: "Off. Turn on JuiceMount in Login Items & Extensions. JuiceMount cannot start until you do.", action: .openLoginItems, tone: .attention, visible: true)
        case .notRegistered:
            return UnmountHelperRow(title: rowTitle, detail: "Not set up. JuiceMount needs this helper before it can start.", action: .setUp, tone: .attention, visible: true)
        case .notResponding(let detail):
            return UnmountHelperRow(title: rowTitle, detail: "On, but not responding (\(detail)). Turn JuiceMount off and on again in Login Items & Extensions.", action: .openLoginItems, tone: .failed, visible: true)
        case .unavailable(let message):
            return UnmountHelperRow(title: rowTitle, detail: message, action: .none, tone: .failed, visible: true)
        }
    }

    // MARK: This app's own signature

    /// How this app bundle is signed, read offline from its own signature.
    /// The release helper admits only notarized apps; any helper admits only
    /// Developer ID apps of our team.
    enum AppTrust: Equatable {
        /// Notarized with the ticket stapled (checked with network access off).
        case notarized
        /// Developer ID, but not notarized or the ticket is not stapled.
        case developerIDNotNotarized
        /// Ad-hoc, unsigned, or not our Developer ID.
        case adhocOrForeign
        /// The signature could not be read.
        case unknown
    }

    static let adhocMessage = gatePrefix + ", and this copy of JuiceMount is ad-hoc signed. The helper only accepts JuiceMount signed with its Developer ID. Build with a Developer ID identity (not JM_ADHOC), or install a signed release."
    static let unnotarizedMessage = gatePrefix + ", and this copy of JuiceMount is not notarized (or its ticket is not stapled), so a release helper refuses it. Install a notarized release, or rebuild with scripts/build-app.sh, which pairs unnotarized builds with the development helper."

    /// A helper that does not answer an app it can never admit is not fixed
    /// by toggling Login Items: say what is actually wrong. An ad-hoc app is
    /// refused by every helper; an unnotarized app only by a release helper,
    /// which is the likely cause once the helper is enabled but silent.
    static func refine(_ readiness: UnmountHelperReadiness, appTrust: AppTrust) -> UnmountHelperReadiness {
        switch (readiness, appTrust) {
        case (.ready, _), (.notApplicable, _):
            return readiness
        case (_, .adhocOrForeign):
            return .unavailable(adhocMessage)
        case (.notResponding, .developerIDNotNotarized):
            return .unavailable(unnotarizedMessage)
        default:
            return readiness
        }
    }

    // MARK: Ordering seams (pinned by the probe)

    /// ensureAvailable: the serving LaunchAgent first, then, for anything
    /// that will mount, the helper gate. The Link test passes forMount false.
    static func runAvailabilitySequence(forMount: Bool, service: () throws -> Void, helper: () throws -> Void) rethrows {
        try service()
        if forMount { try helper() }
    }

    /// Restart and recover-then-start: the gate runs BEFORE the stop. A
    /// refusal leaves the mounted volume exactly as it was.
    static func gateThenProceed(check: () throws -> Void, proceed: () -> Void, refused: (Error) -> Void) {
        do { try check() } catch { refused(error); return }
        proceed()
    }

    /// Launch auto-start: a helper that is enabled but slow to answer (just
    /// approved, launchd still spawning it) is retried before the Setup
    /// Assistant opens. Waits between the 3 attempts, ~20 s in all.
    static let launchRetryDelays: [TimeInterval] = [6, 8]

    static func isTransient(_ readiness: UnmountHelperReadiness) -> Bool {
        if case .notResponding = readiness { return true }
        return false
    }

    /// Runs `attempt`, retrying after each launchRetryDelays wait while it
    /// fails with a TRANSIENT helper refusal. Any other error, or the last
    /// transient one, is rethrown for the caller (Setup Assistant).
    static func withLaunchRetries(_ attempt: () throws -> Void, sleep: (TimeInterval) -> Void) throws {
        var delays = launchRetryDelays[...]
        while true {
            do {
                try attempt()
                return
            } catch let gate as UnmountHelperGateError where gate.isTransient && !delays.isEmpty {
                sleep(delays.removeFirst())
            }
        }
    }

    // MARK: Status-channel ping

    /// Wire constants, mirrored from internal/unmountd/client (protocol.go,
    /// requirement.go). The daemon rejects anything else.
    static let statusMachService = "com.juicemount.unmountd.status"
    static let protocolVersion: Int64 = 1
    static let keyVersion = "jm.v"
    static let keyBody = "jm.body"
    static let pingBody = Data(#"{"op":"ping"}"#.utf8)
    /// Our Developer ID team, without an identifier: what every helper
    /// channel requires of this app before the identifier check.
    static let appDeveloperIDRequirement =
        #"anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "PKUE74YS72""#
    /// client.DaemonRequirement(): our Developer ID team and the helper's
    /// explicit signing identifier. The GUI refuses any other peer.
    static let daemonRequirement =
        #"anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = "PKUE74YS72" and (identifier "com.juicemount.unmountd")"#

    private struct PingResponse: Decodable {
        struct Info: Decodable {
            let `protocol`: Int
            let role: String
            let uid: UInt32
        }
        let code: String
        let message: String?
        let ping: Info?
    }

    /// Verdict on one ping reply. nil means ready; otherwise the reason.
    /// The reply must be protocol 1, code "ok", admitted as the GUI role, and
    /// seen as this user's euid (the daemon reads it from the connection).
    static func pingProblem(version: Int64?, body: Data?, expectedUID: UInt32) -> String? {
        guard let version, let body else { return "no reply" }
        guard version == protocolVersion else { return "protocol \(version)" }
        guard let r = try? JSONDecoder().decode(PingResponse.self, from: body) else { return "unreadable reply" }
        guard r.code == "ok" else { return r.message.map { "\(r.code): \($0)" } ?? r.code }
        guard let p = r.ping else { return "reply without ping info" }
        guard p.protocol == Int(protocolVersion) else { return "protocol \(p.protocol)" }
        guard p.role == "gui" else { return "admitted as \(p.role)" }
        guard p.uid == expectedUID else { return "wrong user" }
        return nil
    }

    // MARK: Eject-now reply from the serving service

    /// Result of asking the serving service to force-eject (busy Quit).
    enum EjectResult: Equatable {
        /// The service had the helper retire the volume.
        case ejected
        /// The service is running an ORDINARY retire and nothing was forced:
        /// other apps still hold files. Ask again after `retryAfter` seconds.
        case inProgress(retryAfter: Int)
        /// Nothing was attempted (the service has no eject-now route yet,
        /// or no helper).
        case unavailable(String)
        /// The service sent the force and stopped waiting before the helper
        /// finished (HTTP 504). The force is under way and cannot be
        /// stopped; whether the volume is gone yet is unknown.
        case forcePending(String)
        /// The service or the helper refused or failed; the volume may still
        /// be mounted.
        case failed(String)
        /// No HTTP answer at all (the client timed out or the connection
        /// failed). The request may have reached the service, and an
        /// escalated one may have sent the force: what it did is unknown.
        case noAnswer(String)
    }

    static let ejectUnavailableText = "Force-eject needs the JuiceMount background service to support it, and this build does not yet."

    /// POST /eject-now reply → result (bridge/eject_now.go):
    ///   200 {"ok":true,"state":"RETIRED"}                     → ejected
    ///   202 {"ok":false,"state":"ordinary_in_progress","retry_after":N}
    ///                                                         → inProgress(N)
    ///   404/405 (route absent in this service build)          → unavailable
    ///   504 (the force was sent; the helper had not finished
    ///        when the service stopped waiting)                → forcePending
    ///   409 {"state":"withdrawn"} (a DELETE overtook this POST;
    ///        nothing was forced)                              → failed
    ///   anything else, with {"ok":false,"error":…}            → failed
    ///   no HTTP answer (client timeout, connection failure)   → noAnswer
    /// The service forces only on an ESCALATE POST, once its ordinary retire
    /// has been busy for 30 s since the START POST and is busy again on the
    /// current call; a 202 is never an error.
    static func ejectResult(httpStatus: Int?, body: Data?) -> EjectResult {
        guard let httpStatus else { return .noAnswer("The background service did not answer.") }
        if httpStatus == 404 || httpStatus == 405 { return .unavailable(ejectUnavailableText) }
        let obj = body.flatMap { try? JSONSerialization.jsonObject(with: $0) as? [String: Any] }
        if httpStatus == 200, obj?["ok"] as? Bool == true, obj?["state"] as? String == "RETIRED" {
            return .ejected
        }
        if httpStatus == 202, obj?["state"] as? String == "ordinary_in_progress" {
            let after = (obj?["retry_after"] as? NSNumber)?.intValue ?? 5
            return .inProgress(retryAfter: min(max(after, 1), 30))
        }
        if httpStatus == 504 {
            let error = (obj?["error"] as? String).flatMap { $0.isEmpty ? nil : $0 }
            return .forcePending(error ?? "the helper was still ejecting when the background service stopped waiting")
        }
        if let error = obj?["error"] as? String, !error.isEmpty { return .failed(error) }
        if let state = obj?["state"] as? String { return .failed("The helper reported \(state).") }
        return .failed("The background service answered HTTP \(httpStatus).")
    }

    /// Result of withdrawing the eject a POST started (busy Quit cancelled).
    enum WithdrawResult: Equatable {
        /// The service withdrew its ordinary retire: nothing further is
        /// issued for it, and the volume stays mounted.
        case withdrawn
        /// A force is already requested or issued and is never withdrawn.
        /// The Quit can no longer be cancelled.
        case forceInProgress(String)
        /// Nothing was left to withdraw: the eject a POST started had
        /// already finished and the volume is gone. The Quit continues.
        case alreadyEjected
        /// No answer, or a refusal other than a force. The ordinary retire
        /// the POST started may still finish once the apps let go.
        case failed(String)
    }

    static let withdrawUnsupportedText = "This background service cannot stop an eject it has started."

    /// DELETE /eject-now reply → result (bridge/eject_now.go,
    /// handleEjectNowWithdraw):
    ///   200 {"ok":true,"state":"RETIRED"}                      → alreadyEjected
    ///   200 {"ok":true,"state":…}                              → withdrawn
    ///   409 {"ok":false,"state":"force_in_progress","error":…} → forceInProgress
    ///   404/405 (service build without the DELETE route)       → failed
    ///   anything else, with {"ok":false,"error":…}             → failed
    static func withdrawResult(httpStatus: Int?, body: Data?) -> WithdrawResult {
        guard let httpStatus else { return .failed("The background service did not answer.") }
        if httpStatus == 404 || httpStatus == 405 { return .failed(withdrawUnsupportedText) }
        let obj = body.flatMap { try? JSONSerialization.jsonObject(with: $0) as? [String: Any] }
        if httpStatus == 200, obj?["ok"] as? Bool == true {
            return obj?["state"] as? String == "RETIRED" ? .alreadyEjected : .withdrawn
        }
        let error = (obj?["error"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        if httpStatus == 409, obj?["state"] as? String == "force_in_progress" {
            return .forceInProgress(error ?? "The forced eject is already under way.")
        }
        if let error { return .failed(error) }
        if let state = obj?["state"] as? String { return .failed("The helper reported \(state).") }
        return .failed("The background service answered HTTP \(httpStatus).")
    }

    /// The note the incomplete-shutdown alert leads with after a busy Quit's
    /// eject-now sequence, or nil when there is nothing to say. It never
    /// claims "not performed" for a force that may have been issued: a 504
    /// means the force is under way and its outcome is unknown, and a
    /// failure after escalation may follow an issued force that did not
    /// converge.
    static func shutdownEjectNote(for outcome: EjectNowRequester.Outcome) -> String? {
        switch outcome {
        case .forcePending(let why):
            return "A force-eject was started and had not finished when JuiceMount stopped waiting (\(why)). Its outcome is unknown: the volume may already be ejected."
        case .outcomeUnknown(let why):
            return "JuiceMount stopped waiting for an eject request that may have forced the volume (\(why)). Its outcome is unknown: the volume may already be ejected."
        case .startUnanswered(let why):
            return "JuiceMount stopped waiting for its eject request (\(why)). That request asked only for a normal eject, which never forces the volume. It may still eject once the other apps close their files."
        case .notAttempted(let why):
            return notPerformedEjectNote(why)
        case .failed(let why):
            return "The eject did not complete: \(why)"
        case .retired, .abandoned:
            return nil
        }
    }

    /// The note when nothing was attempted at all.
    static func notPerformedEjectNote(_ why: String) -> String {
        "The force-eject was not performed: \(why)"
    }

    /// GET /eject-now capability reply: 200 {"available":true} only.
    static func serviceSupportsEjectNow(httpStatus: Int?, body: Data?) -> Bool {
        guard httpStatus == 200, let body,
              let obj = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else { return false }
        return obj["available"] as? Bool == true
    }
}

/// A Start refused by the helper gate. Its text always begins with
/// UnmountHelperDecision.gatePrefix, so RemediationAlert recognises it from
/// error.localizedDescription alone.
struct UnmountHelperGateError: LocalizedError, Equatable {
    let readiness: UnmountHelperReadiness
    let message: String
    var errorDescription: String? { message }
    var isTransient: Bool { UnmountHelperDecision.isTransient(readiness) }
}

// MARK: - Busy-Quit eject-now sequencing (pure)

/// The phase every POST /eject-now carries (bridge/eject_now.go). The
/// service never forces on a START POST, and counts its 30 s force floor
/// from the most recent one; only an ESCALATE POST may force.
enum EjectNowPhase: String, Equatable {
    /// The countdown began.
    case start
    /// The countdown ended, or the user pressed "Eject now".
    case escalate
}

/// Sequences the busy-Quit requests to the serving service's /eject-now,
/// beside the 30 s QuitEjectCountdown (founder decision, 0.5). The service
/// contract is in bridge/eject_now.go (handleEjectNowHTTP):
///
///   - countdown STARTS → POST phase "start". That begins the service's
///     ORDINARY retire and restarts its 30 s force floor; on a volume nobody
///     holds it can finish at once (RETIRED: Quit proceeds). A start POST
///     never forces. A 202 here means "keep counting"; an error here is only
///     remembered.
///   - countdown ENDS, or the user presses "Eject now" → escalate: POST phase
///     "escalate" now, then again after each 202's retry_after until RETIRED
///     or an error. The service decides when a force is allowed (ordinary
///     retire busy for 30 s since the start POST and busy again); the GUI
///     never promises an instant force.
///   - a POST goes out as "escalate" only after a start POST was answered
///     202 (review #7, finding 1). Until then the service may never have seen
///     this panel's start: it can still hold a busy window an earlier panel
///     left (its DELETE got no answer), and an escalate would force on that
///     window at once, 1 s into the new countdown. So while no start was
///     answered 202, the POST that escalation sends is another "start"; its
///     202 restarts the service's floor, and the escalate POST after that
///     202's retry_after gets the full 30 s from a start the service saw.
///   - escalation is bounded (maxEscalatedSeconds); a timeout is a failure.
///   - the panel is abandoned ("Cancel Quit", or the files closed) → no more
///     POSTs, ever. If any POST was sent, DELETE exactly once: otherwise the
///     ordinary retire that POST started keeps retrying in the helper and
///     ejects the volume as soon as the apps let go. The DELETE waits for a
///     POST still in flight, so the panel reports what that POST actually
///     did (it may already have ejected the volume, or sent the force). The
///     service orders an overtaking DELETE correctly too
///     (nfscustody.HelperRetireGate), but only the POST's reply says which
///     happened. While an ESCALATE POST is in flight the panel says a force
///     may be under way and the wait can last up to about a minute; while
///     the start POST is in flight it does not (that POST cannot force). A 409
///     means a force is already under way: the Quit can no longer be
///     cancelled. A 200 "RETIRED" means the volume is already ejected: the
///     Quit continues.
///
/// The driver sends exactly one request per `.post` / `.withdraw` command and
/// feeds the reply back with `reply(_:)` / `withdrawReply(_:)`. At most one
/// request is ever in flight.
struct EjectNowRequester: Equatable {
    enum Command: Equatable { case none, post, withdraw }
    enum Outcome: Equatable {
        case retired
        /// The force was sent and is still running in the helper; the
        /// service stopped waiting (504). Not a failure, not a success:
        /// the outcome is unknown.
        case forcePending(String)
        /// An escalated POST got no HTTP answer: it may have reached the
        /// service and sent the force. Not a failure: the outcome is unknown.
        case outcomeUnknown(String)
        /// A START POST got no HTTP answer after escalation: the first one,
        /// with the user escalating while it was in flight (review #5,
        /// finding 2), or the start re-sent at escalation because no start
        /// had been answered 202 yet (review #7). A start POST never forces
        /// (round 7), so it is reported apart from `outcomeUnknown`, without
        /// claiming it may have forced.
        case startUnanswered(String)
        /// Nothing was attempted: the service has no eject-now route (an
        /// escalated 404/405).
        case notAttempted(String)
        case failed(String)
        /// The panel was abandoned. The DELETE result, or nil when no POST
        /// was ever sent (nothing to withdraw, nothing sent).
        case abandoned(UnmountHelperDecision.WithdrawResult?)
    }

    static let maxEscalatedSeconds = 120
    static let ejectingMessage = "Ejecting… other apps still have files open. JuiceMount is trying a normal eject first; it forces only after those apps have kept the volume busy for 30 s."
    static let cancellingMessage = "Cancelling… asking the background service to stop the eject it started."
    /// Cancel pressed while an ESCALATE POST is still with the service:
    /// that POST may be the force, which cannot be stopped, and the DELETE
    /// must wait for its answer (it can take up to about a minute).
    static let cancellingAfterForceRequestMessage = "Cancelling… an eject request that may force the volume is already with the background service. JuiceMount must wait for its answer (up to about a minute). If the force has started, it cannot be stopped and Quit will continue."
    static let forceInProgressMessage = "The volume is already being force-ejected, so Quit can no longer be cancelled. JuiceMount will finish quitting."

    private(set) var started = false
    private(set) var inFlight = false
    /// The POST in flight is an ESCALATE POST (sent after escalation, once a
    /// start was answered 202). Only such a POST may force; a start POST
    /// never does.
    private(set) var inFlightEscalated = false
    /// A start POST was answered 202: the service took this panel's start,
    /// ended any earlier busy window and restarted its 30 s floor. Until
    /// then every POST is a start, even after escalation (review #7).
    private(set) var startAccepted = false
    /// The phase of the POST in flight (meaningful while `inFlight` and not
    /// withdrawing). The driver sends it with the POST.
    var inFlightPhase: EjectNowPhase { inFlightEscalated ? .escalate : .start }
    private(set) var escalated = false
    private(set) var userPressedEjectNow = false
    /// Seconds until the next escalated POST (nil: none scheduled).
    private(set) var waitRemaining: Int?
    private(set) var escalatedSeconds = 0
    private(set) var outcome: Outcome?
    /// Why the start POST failed, kept for the shutdown note.
    private(set) var startNote: String?
    /// POSTs sent so far.
    private(set) var postsSent = 0
    /// The panel was abandoned: no further POST is ever sent.
    private(set) var cancelling = false
    /// The one DELETE has been sent.
    private(set) var withdrawSent = false

    var isDone: Bool { outcome != nil }

    /// Panel text while escalated, cancelling, or refused a cancel; nil while
    /// the countdown still runs. Non-nil hides the countdown text.
    var ejectingMessage: String? {
        if case .abandoned(.forceInProgress)? = outcome { return Self.forceInProgressMessage }
        guard outcome == nil else { return nil }
        if cancelling {
            // Only an escalate POST can be the force; a start POST in flight
            // (even after the user escalated) cannot.
            return inFlight && inFlightEscalated && !withdrawSent ? Self.cancellingAfterForceRequestMessage : Self.cancellingMessage
        }
        return escalated ? Self.ejectingMessage : nil
    }

    /// While escalated (and not yet cancelling) the panel still offers
    /// "Cancel Quit": the force waits for the service's 30 s floor.
    var offersCancel: Bool { outcome == nil && !cancelling }

    mutating func countdownStarted() -> Command {
        guard !started, outcome == nil, !cancelling else { return .none }
        started = true
        return post()
    }

    /// The countdown ended (`userPressed` false) or "Eject now" was pressed.
    mutating func escalate(userPressed: Bool) -> Command {
        guard outcome == nil, !cancelling else { return .none }
        escalated = true
        if userPressed { userPressedEjectNow = true }
        waitRemaining = nil
        return inFlight ? .none : post()
    }

    /// The panel was abandoned ("Cancel Quit", or the files closed).
    mutating func cancel() -> Command {
        guard outcome == nil, !cancelling else { return .none }
        cancelling = true
        waitRemaining = nil
        if postsSent == 0 {
            outcome = .abandoned(nil)
            return .none
        }
        // A POST still in flight: DELETE when its reply arrives.
        return inFlight ? .none : withdraw()
    }

    /// One second elapsed.
    mutating func tick() -> Command {
        guard outcome == nil, escalated, !cancelling else { return .none }
        escalatedSeconds += 1
        if escalatedSeconds >= Self.maxEscalatedSeconds && !inFlight {
            outcome = .failed("The volume was still busy after \(Self.maxEscalatedSeconds) s of ejecting.")
            return .none
        }
        guard !inFlight, let wait = waitRemaining else { return .none }
        if wait <= 1 {
            waitRemaining = nil
            return post()
        }
        waitRemaining = wait - 1
        return .none
    }

    mutating func reply(_ result: UnmountHelperDecision.EjectResult) -> Command {
        inFlight = false
        let sentEscalated = inFlightEscalated
        inFlightEscalated = false
        guard outcome == nil else { return .none }
        if cancelling {
            // The POST in flight at the cancel finished first. A volume it
            // already retired is gone either way; anything else may have
            // left a job running: withdraw it now.
            if result == .ejected {
                outcome = .retired
                return .none
            }
            return withdraw()
        }
        // Only a 202 proves the service took the start: no answer means it
        // may never have arrived, and an error answer may have come before
        // the service's gate (helper unavailable, bad request). Either way an
        // earlier panel's busy window may still stand, so the next POST is a
        // start again.
        if !sentEscalated, case .inProgress = result { startAccepted = true }
        switch result {
        case .ejected:
            outcome = .retired
        case .forcePending(let message):
            outcome = .forcePending(message)
        case .inProgress(let after):
            // Before escalation a 202 just means "keep counting"; the end of
            // the countdown POSTs again. After it, honour retry_after.
            if escalated {
                if escalatedSeconds >= Self.maxEscalatedSeconds {
                    outcome = .failed("The volume was still busy after \(Self.maxEscalatedSeconds) s of ejecting.")
                } else {
                    waitRemaining = max(1, after)
                }
            }
        case .unavailable(let message):
            if escalated { outcome = .notAttempted(message) } else { startNote = message }
        case .noAnswer(let message):
            if escalated {
                outcome = sentEscalated ? .outcomeUnknown(message) : .startUnanswered(message)
            } else {
                startNote = message
            }
        case .failed(let message):
            if escalated { outcome = .failed(message) } else { startNote = message }
        }
        return .none
    }

    mutating func withdrawReply(_ result: UnmountHelperDecision.WithdrawResult) -> Command {
        inFlight = false
        guard outcome == nil, withdrawSent else { return .none }
        outcome = .abandoned(result)
        return .none
    }

    private mutating func post() -> Command {
        inFlight = true
        inFlightEscalated = escalated && startAccepted
        postsSent += 1
        return .post
    }

    private mutating func withdraw() -> Command {
        guard !withdrawSent else { return .none }
        withdrawSent = true
        inFlight = true
        inFlightEscalated = false
        return .withdraw
    }
}

// MARK: - I/O

/// The SMAppService surface the helper needs. Probes inject a fake, so no
/// test ever registers or launches the real daemon.
protocol UnmountHelperDaemon: AnyObject {
    var approval: UnmountHelperApproval { get }
    func register() throws
}

final class SMAppServiceUnmountHelperDaemon: UnmountHelperDaemon {
    static let plistName = "com.juicemount.unmountd.plist"
    private let service = SMAppService.daemon(plistName: plistName)
    var approval: UnmountHelperApproval { UnmountHelperApproval(service.status) }
    func register() throws { try service.register() }
}

final class UnmountHelperService {
    static let shared = UnmountHelperService(
        daemon: SMAppServiceUnmountHelperDaemon(),
        currentBuild: Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "",
        defaults: .standard,
        appTrust: { UnmountHelperService.ownAppTrust },
        ping: { UnmountHelperService.xpcPing(timeout: 2) })

    static let registeredBuildKey = "JMUnmountHelperRegisteredBuild"

    private let daemon: UnmountHelperDaemon
    private let currentBuild: String
    private let defaults: UserDefaults
    private let appTrust: () -> UnmountHelperDecision.AppTrust
    /// nil = ready; otherwise why the ping failed.
    private let ping: () -> String?
    private let lock = NSLock()
    private let pingLock = NSLock()
    private var lastGoodPingAt: TimeInterval?

    init(daemon: UnmountHelperDaemon, currentBuild: String, defaults: UserDefaults,
         appTrust: @escaping () -> UnmountHelperDecision.AppTrust = { .notarized },
         ping: @escaping () -> String?) {
        self.daemon = daemon
        self.currentBuild = currentBuild
        self.defaults = defaults
        self.appTrust = appTrust
        self.ping = ping
    }

    /// Read-only readiness for status rows: never registers anything.
    /// Blocking (the ping is bounded); call off the main thread.
    /// - cacheMaxAge: reuse a successful ping at most this old (busy Quit).
    /// - attempts: ping attempts before "not responding".
    func readiness(isolated: Bool, cacheMaxAge: TimeInterval = 0, attempts: Int = 3) -> UnmountHelperReadiness {
        if isolated { return .notApplicable }
        let r: UnmountHelperReadiness
        switch daemon.approval {
        case .enabled: r = pingReadiness(cacheMaxAge: cacheMaxAge, attempts: attempts)
        case .requiresApproval: r = .requiresApproval
        case .notRegistered, .notFound: r = .notRegistered
        case .unknown: r = .unavailable(UnmountHelperDecision.unknownStateMessage)
        }
        return UnmountHelperDecision.refine(r, appTrust: appTrust())
    }

    /// Registers as needed (first use, or once after an app update), then
    /// pings. Used before a new Start and by the Setup button. Blocking.
    func prepare(isolated: Bool, mountActive: Bool) -> UnmountHelperReadiness {
        if isolated { return .notApplicable }
        return UnmountHelperDecision.refine(registerThenPing(mountActive: mountActive), appTrust: appTrust())
    }

    /// Founder Q1 gate for a NEW mount. Throws UnmountHelperGateError.
    func ensureAllowsMount(isolated: Bool, mountActive: Bool) throws {
        let readiness = prepare(isolated: isolated, mountActive: mountActive)
        if case .refuse(let message, _) = UnmountHelperDecision.startGate(readiness) {
            throw UnmountHelperGateError(readiness: readiness, message: message)
        }
    }

    private func registerThenPing(mountActive: Bool) -> UnmountHelperReadiness {
        lock.lock()
        defer { lock.unlock() }
        var attempted = false
        var registerError: Error?
        while true {
            let step = UnmountHelperDecision.registrationStep(
                status: daemon.approval, attemptedRegister: attempted,
                registeredBuild: defaults.string(forKey: Self.registeredBuildKey),
                currentBuild: currentBuild, mountActive: mountActive)
            switch step {
            case .register, .reregisterAfterUpdate:
                attempted = true
                do { try daemon.register() } catch { registerError = error }
                if UnmountHelperDecision.shouldRecordBuild(statusAfterAttempt: daemon.approval) {
                    defaults.set(currentBuild, forKey: Self.registeredBuildKey)
                }
            case .ready:
                return pingReadiness(cacheMaxAge: 0, attempts: 3)
            case .needsApproval:
                return .requiresApproval
            case .fail(let message):
                if let registerError {
                    return .unavailable("\(message) (\(registerError.localizedDescription))")
                }
                return .unavailable(message)
            }
        }
    }

    /// launchd starts the daemon on demand, so a ping right after approval
    /// can race its launch: retry briefly before calling it not responding.
    private func pingReadiness(cacheMaxAge: TimeInterval, attempts: Int) -> UnmountHelperReadiness {
        let now = ProcessInfo.processInfo.systemUptime
        if cacheMaxAge > 0 {
            pingLock.lock()
            let last = lastGoodPingAt
            pingLock.unlock()
            if let last, now - last <= cacheMaxAge { return .ready }
        }
        var problem = "no reply"
        for attempt in 0..<max(1, attempts) {
            guard let p = ping() else {
                pingLock.lock()
                lastGoodPingAt = ProcessInfo.processInfo.systemUptime
                pingLock.unlock()
                return .ready
            }
            problem = p
            if attempt < attempts - 1 { Thread.sleep(forTimeInterval: 0.3) }
        }
        pingLock.lock()
        lastGoodPingAt = nil
        pingLock.unlock()
        return .notResponding(problem)
    }

    /// This app's signature, read once, offline: kSecCSNoNetworkAccess makes
    /// an unstapled ticket count as not notarized instead of a network wait.
    static let ownAppTrust: UnmountHelperDecision.AppTrust = appTrust(of: Bundle.main.bundleURL)

    static func appTrust(of url: URL, requirement: String = UnmountHelperDecision.appDeveloperIDRequirement) -> UnmountHelperDecision.AppTrust {
        var code: SecStaticCode?
        guard SecStaticCodeCreateWithPath(url as CFURL, [], &code) == errSecSuccess, let code else { return .unknown }
        // kSecCSNoNetworkAccess (CSCommon.h: 1 << 29); not imported into Swift.
        let noNetworkAccess = SecCSFlags(rawValue: 1 << 29)
        func satisfies(_ text: String) -> Bool {
            var req: SecRequirement?
            guard SecRequirementCreateWithString(text as CFString, [], &req) == errSecSuccess, let req else { return false }
            return SecStaticCodeCheckValidity(code, noNetworkAccess, req) == errSecSuccess
        }
        let developerID = requirement
        guard satisfies(developerID) else { return .adhocOrForeign }
        return satisfies(developerID + " and notarized") ? .notarized : .developerIDNotNotarized
    }

    static func openLoginItems() {
        SMAppService.openSystemSettingsLoginItems()
    }

    // MARK: XPC status ping

    private final class ReplyBox: @unchecked Sendable {
        let lock = NSLock()
        var version: Int64?
        var body: Data?
        var transport: String?
    }

    /// One bounded ping on the helper's STATUS mach service (GUI role). The
    /// connection requires the daemon's code signature; a peer that fails it
    /// never gets the message. Returns nil when the helper is ready.
    static func xpcPing(timeout: TimeInterval) -> String? {
        let conn = xpc_connection_create_mach_service(
            UnmountHelperDecision.statusMachService, nil,
            UInt64(XPC_CONNECTION_MACH_SERVICE_PRIVILEGED))
        xpc_connection_set_event_handler(conn) { _ in }
        let rc = xpc_connection_set_peer_code_signing_requirement(conn, UnmountHelperDecision.daemonRequirement)
        guard rc == 0 else {
            xpc_connection_activate(conn)
            xpc_connection_cancel(conn)
            return "signing requirement rejected (\(rc))"
        }
        xpc_connection_activate(conn)
        defer { xpc_connection_cancel(conn) }

        let msg = xpc_dictionary_create(nil, nil, 0)
        xpc_dictionary_set_int64(msg, UnmountHelperDecision.keyVersion, UnmountHelperDecision.protocolVersion)
        UnmountHelperDecision.pingBody.withUnsafeBytes { raw in
            xpc_dictionary_set_data(msg, UnmountHelperDecision.keyBody, raw.baseAddress!, raw.count)
        }
        let box = ReplyBox()
        let done = DispatchSemaphore(value: 0)
        xpc_connection_send_message_with_reply(conn, msg, nil) { reply in
            box.lock.lock()
            if xpc_get_type(reply) == XPC_TYPE_DICTIONARY {
                box.version = xpc_dictionary_get_int64(reply, UnmountHelperDecision.keyVersion)
                var n = 0
                if let p = xpc_dictionary_get_data(reply, UnmountHelperDecision.keyBody, &n) {
                    box.body = Data(bytes: p, count: n)
                }
            } else if #available(macOS 15.0, *), reply === XPC_ERROR_PEER_CODE_SIGNING_REQUIREMENT {
                box.transport = "helper signature not trusted"
            } else if reply === XPC_ERROR_CONNECTION_INVALID {
                box.transport = "helper not running"
            } else {
                box.transport = "connection interrupted"
            }
            box.lock.unlock()
            done.signal()
        }
        if done.wait(timeout: .now() + timeout) == .timedOut {
            return "timed out"
        }
        box.lock.lock()
        defer { box.lock.unlock() }
        if let transport = box.transport { return transport }
        return UnmountHelperDecision.pingProblem(version: box.version, body: box.body, expectedUID: getuid())
    }
}
