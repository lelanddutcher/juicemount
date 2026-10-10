import Foundation
import ServiceManagement

// Compile with the production Core/UnmountHelperService.swift:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/UnmountHelperService.swift \
//     app/JuiceMount/Tests/UnmountHelperDecisionProbe.swift -o <scratch>/probe && <scratch>/probe
//
// Never touches launchd: the SMAppService seam is a fake and the XPC ping
// is injected. The real daemon is neither registered nor launched.
// Run from the repo root: the call-site checks read the production sources
// relative to this file's compile path.

final class FakeDaemon: UnmountHelperDaemon {
    var approval: UnmountHelperApproval
    var afterRegister: UnmountHelperApproval?
    var registerError: Error?
    var registerCalls = 0
    init(_ approval: UnmountHelperApproval, afterRegister: UnmountHelperApproval? = nil, registerError: Error? = nil) {
        self.approval = approval
        self.afterRegister = afterRegister
        self.registerError = registerError
    }
    func register() throws {
        registerCalls += 1
        if let afterRegister { approval = afterRegister }
        if let registerError { throw registerError }
    }
}

struct AlreadyRegistered: LocalizedError { var errorDescription: String? { "already registered" } }

@main
struct UnmountHelperDecisionProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        typealias D = UnmountHelperDecision

        // --- Registration step -------------------------------------------
        func step(_ s: UnmountHelperApproval, attempted: Bool = false, registered: String? = "7", current: String = "7", active: Bool = false) -> UnmountHelperRegistrationStep {
            D.registrationStep(status: s, attemptedRegister: attempted, registeredBuild: registered, currentBuild: current, mountActive: active)
        }
        // H-11 lesson: .notFound is "never registered", so register, don't fail.
        check(step(.notFound) == .register, "never-registered helper (.notFound) must register")
        check(step(.notRegistered) == .register, ".notRegistered must register")
        check(step(.notFound, attempted: true) == .fail(D.notFoundMessage), ".notFound surviving a register attempt fails, not loops")
        check(step(.notRegistered, attempted: true) == .fail(D.registerFailedMessage), ".notRegistered surviving a register attempt fails")
        check(step(.requiresApproval) == .needsApproval, "requiresApproval asks the user")
        check(step(.unknown) == .fail(D.unknownStateMessage), "unknown status fails")
        check(step(.enabled) == .ready, "enabled, same build: ready")
        // Sparkle update: CFBundleVersion changed since the last registration.
        check(step(.enabled, registered: "7", current: "8") == .reregisterAfterUpdate, "build changed: re-register once")
        check(step(.enabled, registered: nil, current: "8") == .reregisterAfterUpdate, "no recorded build: re-register once")
        check(step(.enabled, attempted: true, registered: "7", current: "8") == .ready, "re-register is attempted only once")
        check(step(.enabled, registered: "7", current: "8", active: true) == .ready, "never re-register under a live mount")
        check(D.shouldRecordBuild(statusAfterAttempt: .enabled), "record build when enabled")
        check(!D.shouldRecordBuild(statusAfterAttempt: .requiresApproval), "do not record build while approval is pending")

        // --- Start gate (founder Q1) -------------------------------------
        check(D.startGate(.ready) == .allow, "ready allows Start")
        check(D.startGate(.notApplicable) == .allow, "isolated install allows Start")
        check(D.startGate(.requiresApproval) == .refuse(message: D.approvalMessage, offersApproval: true), "approval pending refuses with the approval button")
        check(D.startGate(.notRegistered) == .refuse(message: D.notRegisteredMessage, offersApproval: true), "unregistered refuses")
        check(D.startGate(.notResponding("timed out")) == .refuse(message: D.notRespondingMessage("timed out"), offersApproval: true), "enabled but silent refuses: approval alone is not readiness")
        check(D.startGate(.unavailable(D.notFoundMessage)) == .refuse(message: D.notFoundMessage, offersApproval: false), "broken bundle refuses without a Settings button")
        for r: UnmountHelperReadiness in [.requiresApproval, .notRegistered, .notResponding("x"), .unavailable(D.notFoundMessage), .unavailable(D.registerFailedMessage)] {
            if case .refuse(let m, _) = D.startGate(r) {
                check(D.isStartGateRefusal(m), "refusal for \(r) is recognisable by RemediationAlert")
                check(!m.contains("umount") && !m.lowercased().contains("sudo"), "no path-unmount wording in \(r)")
            }
        }
        check(!D.isStartGateRefusal("Allow JuiceMount in System Settings → General → Login Items & Extensions to start its background service."), "the LaunchAgent message is not the helper gate")
        check(D.approvalMessage == "JuiceMount needs its background helper to safely eject its volume if JuiceMount stops responding. Turn on JuiceMount in System Settings → General → Login Items & Extensions, then Start again.", "approval wording: \(D.approvalMessage)")

        // --- Force-eject flag -------------------------------------------
        check(D.forceEjectAvailable(helper: .ready, serviceSupportsEjectNow: true), "helper ready + service route: countdown")
        check(!D.forceEjectAvailable(helper: .ready, serviceSupportsEjectNow: false), "no service route: never promise a force")
        check(!D.forceEjectAvailable(helper: .requiresApproval, serviceSupportsEjectNow: true), "helper off: no force")
        check(!D.forceEjectAvailable(helper: .notResponding("x"), serviceSupportsEjectNow: true), "helper silent: no force")
        check(!D.forceEjectAvailable(helper: .notApplicable, serviceSupportsEjectNow: true), "isolated: no force")

        // --- Rows --------------------------------------------------------
        check(D.row(for: .ready).tone == .ok && D.row(for: .ready).action == .none, "ready row")
        check(D.row(for: .requiresApproval).action == .openLoginItems, "approval row opens Login Items")
        check(D.row(for: .notRegistered).action == .setUp, "unregistered row offers Set Up")
        check(D.row(for: .notResponding("x")).action == .openLoginItems && D.row(for: .notResponding("x")).tone == .failed, "silent row")
        check(!D.row(for: .notApplicable).visible, "isolated row hidden")

        // --- Ping verdict ------------------------------------------------
        let uid: UInt32 = 501
        func body(_ s: String) -> Data { Data(s.utf8) }
        check(D.pingProblem(version: 1, body: body(#"{"code":"ok","ping":{"protocol":1,"channel":"release","role":"gui","uid":501}}"#), expectedUID: uid) == nil, "good ping is ready")
        check(D.pingProblem(version: 1, body: body(#"{"code":"ok","ping":{"protocol":1,"channel":"dev","role":"service","uid":501}}"#), expectedUID: uid) != nil, "wrong role refused")
        check(D.pingProblem(version: 1, body: body(#"{"code":"ok","ping":{"protocol":1,"channel":"dev","role":"gui","uid":0}}"#), expectedUID: uid) != nil, "wrong uid refused")
        check(D.pingProblem(version: 2, body: body(#"{"code":"ok","ping":{"protocol":1,"role":"gui","uid":501}}"#), expectedUID: uid) != nil, "wrong envelope version refused")
        check(D.pingProblem(version: 1, body: body(#"{"code":"busy","message":"daemon restarting"}"#), expectedUID: uid) == "busy: daemon restarting", "daemon error surfaced")
        check(D.pingProblem(version: 1, body: body("not json"), expectedUID: uid) != nil, "garbage refused")
        check(D.pingProblem(version: nil, body: nil, expectedUID: uid) == "no reply", "no reply")
        check(String(decoding: D.pingBody, as: UTF8.self) == #"{"op":"ping"}"#, "ping request body")

        // --- Eject-now reply (the service route the bridge must add) -----
        check(D.ejectResult(httpStatus: 404, body: Data("404 page not found".utf8)) == .unavailable(D.ejectUnavailableText), "no route: nothing attempted")
        check(D.ejectResult(httpStatus: 200, body: body(#"{"ok":true,"state":"RETIRED"}"#)) == .ejected, "retired")
        check(D.ejectResult(httpStatus: 200, body: body(#"{"ok":true,"state":"WAIT_NESTED"}"#)) == .failed("The helper reported WAIT_NESTED."), "non-terminal state is not success")
        check(D.ejectResult(httpStatus: 409, body: body(#"{"ok":false,"error":"identity refused"}"#)) == .failed("identity refused"), "refusal surfaced")
        check(D.ejectResult(httpStatus: nil, body: nil) == .noAnswer("The background service did not answer."), "no answer: outcome unknown, not a failure")
        check(D.serviceSupportsEjectNow(httpStatus: 200, body: body(#"{"available":true}"#)), "capability true")
        check(!D.serviceSupportsEjectNow(httpStatus: 200, body: body(#"{"available":false}"#)), "capability false")
        check(!D.serviceSupportsEjectNow(httpStatus: 404, body: nil), "no route: no capability")

        // --- Service behaviour behind the injected seam ------------------
        let suite = "jm.probe.unmountd.\(getpid())"
        guard let defaults = UserDefaults(suiteName: suite) else { print("FAIL: defaults"); exit(1) }
        defer { defaults.removePersistentDomain(forName: suite) }
        func service(_ d: FakeDaemon, build: String = "8", ping: @escaping () -> String? = { nil }) -> UnmountHelperService {
            UnmountHelperService(daemon: d, currentBuild: build, defaults: defaults, ping: ping)
        }
        func reset(_ build: String?) {
            if let build { defaults.set(build, forKey: UnmountHelperService.registeredBuildKey) }
            else { defaults.removeObject(forKey: UnmountHelperService.registeredBuildKey) }
        }

        reset(nil)
        var d = FakeDaemon(.notFound, afterRegister: .requiresApproval)
        check(service(d).prepare(isolated: false, mountActive: false) == .requiresApproval, "first Start registers, then needs approval")
        check(d.registerCalls == 1, "exactly one register, got \(d.registerCalls)")
        check(defaults.string(forKey: UnmountHelperService.registeredBuildKey) == nil, "build not recorded before approval")

        d = FakeDaemon(.notFound)
        check(service(d).readiness(isolated: false) == .notRegistered && d.registerCalls == 0, "status rows never register")

        d = FakeDaemon(.notFound)
        check(service(d).prepare(isolated: true, mountActive: false) == .notApplicable && d.registerCalls == 0, "isolated install never registers")

        reset("7")
        d = FakeDaemon(.enabled, afterRegister: .enabled, registerError: AlreadyRegistered())
        check(service(d, build: "8").prepare(isolated: false, mountActive: false) == .ready, "update re-register tolerates already-registered")
        check(d.registerCalls == 1, "update re-registers once, got \(d.registerCalls)")
        check(defaults.string(forKey: UnmountHelperService.registeredBuildKey) == "8", "new build recorded")
        d = FakeDaemon(.enabled)
        _ = service(d, build: "8").prepare(isolated: false, mountActive: false)
        check(d.registerCalls == 0, "same build: no re-register")

        reset("7")
        d = FakeDaemon(.enabled)
        _ = service(d, build: "8").prepare(isolated: false, mountActive: true)
        check(d.registerCalls == 0, "live mount: update re-register deferred")
        check(defaults.string(forKey: UnmountHelperService.registeredBuildKey) == "7", "deferred re-register leaves the old build recorded")

        reset("8")
        var pings = 0
        d = FakeDaemon(.enabled)
        let silent = service(d, ping: { pings += 1; return "timed out" }).prepare(isolated: false, mountActive: false)
        check(silent == .notResponding("timed out"), "enabled but silent is not ready: \(silent)")
        check(pings == 3, "bounded ping retries, got \(pings)")
        check(D.startGate(silent) != .allow, "silent helper gates Start")

        d = FakeDaemon(.notRegistered, afterRegister: .notRegistered, registerError: AlreadyRegistered())
        if case .unavailable(let m) = service(d).prepare(isolated: false, mountActive: false) {
            check(m.hasPrefix(D.registerFailedMessage) && m.contains("already registered"), "register error carried: \(m)")
        } else { check(false, "failed registration is unavailable") }

        // --- Inner ping protocol (review N5) -----------------------------
        check(D.pingProblem(version: 1, body: body(#"{"code":"ok","ping":{"protocol":2,"channel":"release","role":"gui","uid":501}}"#), expectedUID: uid) == "protocol 2", "inner ping protocol mismatch refused")

        // --- App signature refinement (review #2b) -----------------------
        check(D.refine(.notResponding("helper not running"), appTrust: .developerIDNotNotarized) == .unavailable(D.unnotarizedMessage), "unnotarized app + silent helper: specific message, not Login Items advice")
        check(D.refine(.notResponding("x"), appTrust: .notarized) == .notResponding("x"), "notarized app keeps the Login Items advice")
        check(D.refine(.requiresApproval, appTrust: .adhocOrForeign) == .unavailable(D.adhocMessage), "ad-hoc app: specific message")
        check(D.refine(.requiresApproval, appTrust: .developerIDNotNotarized) == .requiresApproval, "unnotarized app still needs approval first")
        check(D.refine(.ready, appTrust: .adhocOrForeign) == .ready, "a helper that answers is ready")
        check(D.refine(.notApplicable, appTrust: .adhocOrForeign) == .notApplicable, "isolated stays isolated")
        if case .refuse(_, let offers) = D.startGate(D.refine(.notResponding("x"), appTrust: .developerIDNotNotarized)) {
            check(!offers, "unnotarized refusal offers no Login Items button")
        } else { check(false, "unnotarized app is refused") }
        check(D.unnotarizedMessage.hasPrefix(D.gatePrefix) && D.adhocMessage.hasPrefix(D.gatePrefix), "trust messages are gate refusals")

        // --- Gate error round trip (Failure -> isStartGateRefusal) -------
        reset("8")
        d = FakeDaemon(.requiresApproval)
        do {
            try service(d).ensureAllowsMount(isolated: false, mountActive: false)
            check(false, "unapproved helper must throw")
        } catch {
            check(error is UnmountHelperGateError, "gate throws UnmountHelperGateError, got \(type(of: error))")
            check(D.isStartGateRefusal(error.localizedDescription), "localizedDescription round-trips to isStartGateRefusal: \(error.localizedDescription)")
        }
        d = FakeDaemon(.enabled)
        do { try service(d).ensureAllowsMount(isolated: false, mountActive: false) } catch { check(false, "ready helper must not throw: \(error)") }
        let adhoc = UnmountHelperService(daemon: FakeDaemon(.enabled), currentBuild: "8", defaults: defaults, appTrust: { .adhocOrForeign }, ping: { "helper not running" })
        do { try adhoc.ensureAllowsMount(isolated: false, mountActive: false); check(false, "ad-hoc must throw") }
        catch { check(error.localizedDescription == D.adhocMessage, "ad-hoc message: \(error.localizedDescription)") }

        // --- ensureAvailable gate wiring --------------------------------
        var calls: [String] = []
        try? D.runAvailabilitySequence(forMount: true, service: { calls.append("service") }, helper: { calls.append("helper") })
        check(calls == ["service", "helper"], "mount path runs service then helper gate, got \(calls)")
        calls = []
        try? D.runAvailabilitySequence(forMount: false, service: { calls.append("service") }, helper: { calls.append("helper") })
        check(calls == ["service"], "Link test skips the helper gate, got \(calls)")
        calls = []
        struct Boom: Error {}
        try? D.runAvailabilitySequence(forMount: true, service: { calls.append("service"); throw Boom() }, helper: { calls.append("helper") })
        check(calls == ["service"], "service failure stops before the helper, got \(calls)")

        // --- Restart ordering: gate BEFORE stop (review #1) --------------
        calls = []
        D.gateThenProceed(check: { calls.append("gate") }, proceed: { calls.append("stop"); calls.append("start") }, refused: { _ in calls.append("refused") })
        check(calls == ["gate", "stop", "start"], "allowed restart: gate, stop, start; got \(calls)")
        calls = []
        D.gateThenProceed(check: { calls.append("gate"); throw UnmountHelperGateError(readiness: .requiresApproval, message: D.approvalMessage) },
                          proceed: { calls.append("stop") }, refused: { _ in calls.append("refused") })
        check(calls == ["gate", "refused"], "refused restart never stops, got \(calls)")

        // --- Launch retry (review #5) -----------------------------------
        let transient = UnmountHelperGateError(readiness: .notResponding("timed out"), message: D.notRespondingMessage("timed out"))
        var tries = 0
        var slept: [TimeInterval] = []
        try? D.withLaunchRetries({ tries += 1; if tries < 3 { throw transient } }, sleep: { slept.append($0) })
        check(tries == 3 && slept == [6, 8], "slow helper retried, tries=\(tries) slept=\(slept)")
        tries = 0; slept = []
        do { try D.withLaunchRetries({ tries += 1; throw transient }, sleep: { slept.append($0) }); check(false, "exhausted retries rethrow") }
        catch { check(tries == 3 && slept.reduce(0, +) <= 20, "bounded: 3 attempts, ~20 s; tries=\(tries) slept=\(slept)") }
        tries = 0
        do { try D.withLaunchRetries({ tries += 1; throw UnmountHelperGateError(readiness: .requiresApproval, message: D.approvalMessage) }, sleep: { _ in }); check(false, "approval rethrows") }
        catch { check(tries == 1, "approval needed is not retried, tries=\(tries)") }

        // --- Ping cache for the Quit panel (review #5) -------------------
        pings = 0
        d = FakeDaemon(.enabled)
        let cached = service(d, ping: { pings += 1; return nil })
        check(cached.readiness(isolated: false) == .ready && pings == 1, "first readiness pings")
        check(cached.readiness(isolated: false, cacheMaxAge: 10, attempts: 1) == .ready && pings == 1, "Quit reuses a fresh good ping")
        check(cached.readiness(isolated: false) == .ready && pings == 2, "status rows ping again")

        // --- Own-signature reader (offline) -------------------------------
        // This probe binary is linker-signed ad-hoc.
        let selfTrust = UnmountHelperService.appTrust(of: URL(fileURLWithPath: CommandLine.arguments[0]))
        check(selfTrust == .adhocOrForeign, "ad-hoc binary reads as adhocOrForeign, got \(selfTrust)")
        // Optional, read-only: JM_PROBE_APP=<bundle> JM_PROBE_APP_TRUST=<notarized|developerIDNotNotarized>
        let env = ProcessInfo.processInfo.environment
        if let app = env["JM_PROBE_APP"], let want = env["JM_PROBE_APP_TRUST"] {
            let got = UnmountHelperService.appTrust(of: URL(fileURLWithPath: app))
            check("\(got)" == want, "\(app) reads as \(got), want \(want)")
            print("INFO: \(app) trust = \(got)")
        }

        // --- Production call sites (source structure) -------------------
        let core = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .deletingLastPathComponent().appendingPathComponent("Sources/JuiceMount/Core")
        func source(_ name: String) -> String { (try? String(contentsOf: core.appendingPathComponent(name), encoding: .utf8)) ?? "" }
        func body(of signature: String, in text: String) -> String {
            guard let r = text.range(of: signature) else { return "" }
            let rest = text[r.upperBound...]
            let end = rest.range(of: "\n    public func ") ?? rest.range(of: "\n    func ") ?? rest.range(of: "\n    private func ")
            return String(rest[..<(end?.lowerBound ?? rest.endIndex)])
        }
        func before(_ a: String, _ b: String, in text: String) -> Bool {
            guard let ra = text.range(of: a), let rb = text.range(of: b) else { return false }
            return ra.lowerBound < rb.lowerBound
        }
        let serving = source("ServingService.swift")
        // Validity gate: #filePath is as given to swiftc; a relative compile
        // path must be run from the repo root. Never pass on unread sources.
        check(!serving.isEmpty, "cannot read production sources at \(core.path); run from the repo root")
        let ensure = body(of: "static func ensureAvailable(forMount: Bool = true) throws", in: serving)
        check(ensure.contains("runAvailabilitySequence(") && ensure.contains("forMount: forMount") && ensure.contains("helper: ensureHelperAllowsMount"), "ensureAvailable wires the helper gate")
        check(body(of: "static func ensureHelperAllowsMount() throws", in: serving).contains(".ensureAllowsMount("), "ensureHelperAllowsMount throws the typed gate error")
        let bridge = source("NFSBridge.swift")
        check(body(of: "public static func testLink(config: ServerConfig)", in: bridge).contains("ensureAvailable(forMount: false)"), "Link test is not gated")
        check(body(of: "private static func start(config: ServerConfig, automatic: Bool)", in: bridge).contains("ServingService.ensureAvailable()"), "Start is gated")
        let controller = source("ServerController.swift")
        let restart = body(of: "public func restart() {", in: controller)
        check(before("helperGatedLifecycle", "stopMount", in: restart), "restart(): gate before stopMount")
        let restartFull = body(of: "public func restartFull() {", in: controller)
        check(before("helperGatedLifecycle", "stop {", in: restartFull), "restartFull(): gate before stop")
        check(body(of: "public func helperGatedLifecycle(", in: controller).contains("ServingService.ensureHelperAllowsMount()"), "helperGatedLifecycle checks the real gate")
        let onboarding = (try? String(contentsOf: core.deletingLastPathComponent().appendingPathComponent("UI/OnboardingWindowView.swift"), encoding: .utf8)) ?? ""
        check(onboarding.contains("server.helperGatedLifecycle { onContinue() }"), "Setup Assistant Continue-from-error is gated before its stop")

        if failures > 0 { exit(1) }
        print("PASS: unmount helper decisions")
    }
}
