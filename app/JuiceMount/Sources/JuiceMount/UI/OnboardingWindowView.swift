import SwiftUI
import AppKit
import Network
import Security

// MARK: - Preflight (LB-1)
//
// First-run / guided-setup checks. Each check mirrors where the truth
// already lives on the Go side so the two can't drift:
//
//   1. juicefs binary  — the pinned, patched JuiceFS the app ships at
//      Contents/MacOS/juicefs, the only one health/fuse.go findJuiceFSBin
//      will run (no PATH or Homebrew). It must report the version
//      build-app.sh stamped into Info.plist and carry the app's signer.
//   2. macFUSE         — /Library/Filesystems/macfuse.fs, the filesystem
//      bundle the macFUSE installer drops.
//   3. backend         — short TCP dial to the host:port parsed from the
//      Redis URL in Preferences (the metadata server; if this is down,
//      start() is guaranteed to fail).
//   4. mount permission — informational text only. The mount helper asks
//      for administrator approval when it must create the mount folder.
//      H-3 (2026-10-06): the former passwordless-privilege probe and its
//      copyable rule are gone. That rule also granted a path-based
//      force-unmount, which JuiceMount must never perform.

struct PreflightReport: Sendable, Equatable {
    var juicefsPath: String?
    /// Why the bundled JuiceFS failed its check; nil when it passed.
    var juicefsProblem: String?
    var macFUSEInstalled: Bool = false
    var backendReachable: Bool = false
    /// host:port we dialed, or a parse-failure explanation.
    var backendDetail: String = ""

    /// The checks a successful start() actually depends on. The mount
    /// permission row is informational and never critical.
    var criticalOK: Bool {
        juicefsPath != nil && macFUSEInstalled && backendReachable
    }
}

enum OnboardingPreflight {

    static let macFUSEBundlePath = "/Library/Filesystems/macfuse.fs"

    /// Run every check. The blocking piece (TCP dial) happens off the
    /// calling actor; total worst case ≈ the dial timeout (3 s).
    static func run(redisURL: String) async -> PreflightReport {
        var report = PreflightReport()
        let juicefs = await Task.detached(priority: .userInitiated) {
            BundledJuiceFS.check(
                executable: Bundle.main.executableURL,
                expectedVersion: Bundle.main.object(forInfoDictionaryKey: BundledJuiceFS.versionInfoKey) as? String)
        }.value
        switch juicefs {
        case .success(let path): report.juicefsPath = path
        case .failure(let problem): report.juicefsProblem = problem.message
        }
        report.macFUSEInstalled = FileManager.default.fileExists(atPath: macFUSEBundlePath)

        switch parseRedisHostPort(redisURL) {
        case .success(let (host, port)):
            report.backendDetail = "\(host):\(port)"
            report.backendReachable = await tcpReachable(host: host, port: port)
        case .failure(let message):
            report.backendDetail = message
            report.backendReachable = false
        }

        return report
    }

    /// Parses "redis://host:port/db" (or bare "host:port") into a dial
    /// target. Port defaults to 6379.
    enum ParseResult {
        case success((String, UInt16))
        case failure(String)
    }

    static func parseRedisHostPort(_ raw: String) -> ParseResult {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return .failure("Redis URL is empty — set it in Preferences") }
        // The Go client accepts redis://, rediss://, or a bare host:port.
        // Do not let an unrelated scheme look healthy
        // merely because its host happens to accept a TCP connection.
        if trimmed.contains("://") && !trimmed.hasPrefix("redis://") && !trimmed.hasPrefix("rediss://") {
            return .failure("Use redis://host:port/db or rediss://host:port/db")
        }
        // URLComponents handles the redis:// scheme; fall back to a bare
        // host:port for hand-typed values. Never echo the input in an error:
        // a Redis URL may contain credentials.
        let withScheme = trimmed.contains("://") ? trimmed : "redis://\(trimmed)"
        guard let comps = URLComponents(string: withScheme), let host = comps.host, !host.isEmpty else {
            return .failure("Redis URL needs a valid host and optional port (redis://host:port/db)")
        }
        let port = comps.port ?? 6379
        guard (1...65535).contains(port) else {
            return .failure("Redis port must be between 1 and 65535")
        }
        return .success((host, UInt16(port)))
    }

    /// Short TCP dial via Network.framework. True iff the connection
    /// reaches .ready within `timeout`. `.waiting` (refused / no route
    /// right now) counts as failure — NWConnection would otherwise sit
    /// there retrying.
    static func tcpReachable(host: String, port: UInt16, timeout: TimeInterval = 3.0) async -> Bool {
        await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
            let conn = NWConnection(
                host: NWEndpoint.Host(host),
                port: NWEndpoint.Port(rawValue: port) ?? 6379,
                using: .tcp
            )
            let queue = DispatchQueue(label: "com.juicemount.preflight.dial")
            // #87: schedule the watchdog on an INDEPENDENT queue, never the
            // connection's `queue`. If NWConnection setup wedges, work on
            // `queue` can back up — a timeout scheduled there would be stuck
            // behind it and never fire, exactly the 150 s-hang failure mode.
            // An independent queue guarantees the timeout always fires.
            let timeoutQueue = DispatchQueue(label: "com.juicemount.preflight.timeout")
            // finish() is now reachable from two unrelated queues (the
            // connection's stateUpdateHandler on `queue`, and the watchdog on
            // `timeoutQueue`), so guard the once-only flag with a lock instead
            // of relying on serialization by a single queue. The continuation
            // resumes exactly once no matter which fires first.
            let lock = NSLock()
            var finished = false
            func finish(_ ok: Bool) {
                lock.lock()
                if finished { lock.unlock(); return }
                finished = true
                lock.unlock()
                conn.cancel()
                cont.resume(returning: ok)
            }
            conn.stateUpdateHandler = { state in
                queue.async {
                    switch state {
                    case .ready:
                        finish(true)
                    case .failed, .waiting:
                        finish(false)
                    default:
                        break
                    }
                }
            }
            conn.start(queue: queue)
            timeoutQueue.asyncAfter(deadline: .now() + timeout) {
                finish(false)
            }
        }
    }
}

// MARK: - Welcome window (LB-1)

/// First-run / setup-assistant window. Shown automatically at launch when
/// onboarding was never completed or a critical preflight check fails
/// (see AppDelegate); reopenable any time via the popover's
/// "Setup Assistant…". Never blocks a healthy launch.
struct OnboardingWindowView: View {
    @Bindable var server: ServerController
    /// Called when the user hits Continue — the window owner closes the
    /// window; we mark onboarding complete and kick off a start if needed.
    let onContinue: () -> Void

    @State private var report: PreflightReport?
    @State private var permissions: PermissionReport?
    @State private var checking = false
    @State private var recheck = SingleFlight()
    /// H-3 L3: privileged eject helper. nil while the first check runs.
    @State private var helper: UnmountHelperReadiness?
    @State private var helperBusy = false

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            // Header
            VStack(alignment: .leading, spacing: 4) {
                Text("Welcome to JuiceMount")
                    .font(.title2.bold())
                Text("Let's make sure this Mac has everything the mount needs. JuiceMount streams your storage as a local volume — these three pieces make that work.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Divider()

            VStack(alignment: .leading, spacing: 5) {
                Toggle("Use this Mac's existing private network", isOn: Binding(
                    get: { server.preferences.useExistingPrivateNetwork },
                    set: { server.preferences.useExistingPrivateNetwork = $0 }
                ))
                if server.preferences.useExistingPrivateNetwork {
                    Text("Connect this Mac to your NAS through Tailscale or your trusted VPN. In Preferences → Connection, set Redis URL to the NAS's private address. Set the S3 endpoint override only if the stored object-store address is unreachable from this Mac. No JuiceMount Link pairing code is needed.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                } else {
                    Text("Configure JuiceMount Link in Preferences → Connection before starting.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            // Keep the checklist scrollable as repair hints expand so Re-check
            // and Continue stay visible in the fixed-size setup window.
            ScrollView {
            VStack(alignment: .leading, spacing: 12) {
                checkRow(
                    ok: report.map { $0.juicefsPath != nil },
                    title: "JuiceFS engine",
                    passDetail: report?.juicefsPath ?? "",
                    failDetail: report?.juicefsProblem ?? "The bundled JuiceFS engine failed its check.",
                    fixHint: "JuiceFS ships inside JuiceMount. Reinstall JuiceMount from a signed download, then re-check."
                ) {
                    EmptyView()
                }

                checkRow(
                    ok: report.map(\.macFUSEInstalled),
                    title: "macFUSE",
                    passDetail: "Installed at \(OnboardingPreflight.macFUSEBundlePath)",
                    failDetail: "macFUSE isn't installed — JuiceFS needs it to expose the cache filesystem.",
                    fixHint: "Download and run the installer, then re-check:"
                ) {
                    Link("Download macFUSE", destination: URL(string: "https://macfuse.github.io")!)
                        .font(.caption)
                }

                checkRow(
                    ok: report.map(\.backendReachable),
                    title: "Redis connection",
                    passDetail: "TCP connection succeeded at \(report?.backendDetail ?? ""). This does not verify TLS, Redis authentication, or access to stored files.",
                    failDetail: "Can't reach Redis (\(report?.backendDetail ?? "")).",
                    fixHint: server.preferences.useExistingPrivateNetwork
                        ? "Check that this Mac's private network is connected. In Preferences → Connection, enter the NAS's private Redis address, then Re-check. A JuiceMount Link pairing code is not required."
                        : "Check that the server is running and the Redis URL in Preferences → Connection points at it. Link pairing is checked when JuiceMount starts."
                ) {
                    Button("Open Preferences…") {
                        (NSApp.delegate as? AppDelegate)?.menuBarController?.openPreferencesWindow()
                    }
                    .controlSize(.small)
                }

                Divider()
                Text("macOS permissions")
                    .font(.subheadline.weight(.semibold))

                permissionRow(
                    status: permissions?.serviceApproval,
                    title: "Background service",
                    fixTitle: "Open Login Items…",
                    fix: { SystemSettingsLinks.openLoginItems() }
                )
                helperRow
                permissionRow(
                    status: permissions?.localNetwork,
                    title: "Local Network",
                    fixTitle: "Open Local Network Settings…",
                    fix: { SystemSettingsLinks.open(SystemSettingsLinks.localNetwork) }
                )
                permissionRow(
                    status: permissions?.networkVolumes,
                    title: "Network Volumes",
                    fixTitle: "Open Files & Folders Settings…",
                    fix: { SystemSettingsLinks.open(SystemSettingsLinks.filesAndFolders) }
                )

                // Mount permission — informational, never blocks Continue.
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    Image(systemName: "info.circle.fill")
                        .foregroundStyle(.blue)
                        .frame(width: 16)
                    VStack(alignment: .leading, spacing: 2) {
                        Text("Mount permission")
                            .font(.body.weight(.medium))
                        Text("macOS may ask for your administrator password when JuiceMount creates the mount folder, and separately ask JuiceMount for permission to access network volumes.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    Spacer()
                }
            }
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            // Footer
            HStack {
                Button {
                    runChecks()
                } label: {
                    if checking {
                        HStack(spacing: 6) {
                            ProgressView().controlSize(.small)
                            Text("Checking…")
                        }
                    } else {
                        Text("Re-check")
                    }
                }
                .disabled(checking)

                Spacer()

                Button(continueTitle) {
                    switch server.state {
                    case .error, .disconnected:
                        // Continue from a failed state stops everything and
                        // then starts (MenuBarController). Run the helper
                        // gate first so a refusal stops nothing.
                        server.helperGatedLifecycle { onContinue() }
                    default:
                        onContinue()
                    }
                }
                .keyboardShortcut(.defaultAction)
                .disabled(checking && report == nil)
            }
        }
        .padding(20)
        .frame(width: 480)
        .frame(minHeight: 430)
        .onAppear { runChecks() }
        // Re-check when the user comes back from System Settings. runChecks
        // is single-flight and every probe is bounded, so repeated focus
        // changes cannot stack work or hang the window.
        .onReceive(NotificationCenter.default.publisher(for: NSApplication.didBecomeActiveNotification)) { _ in
            runChecks()
        }
        .onReceive(NotificationCenter.default.publisher(for: NSWindow.didBecomeKeyNotification)) { note in
            guard (note.object as? NSWindow)?.identifier?.rawValue == "OnboardingWindow" else { return }
            runChecks()
        }
    }

    private var continueTitle: String {
        guard let r = report else { return "Continue" }
        return r.criticalOK ? "Continue" : "Continue Anyway"
    }

    private func runChecks() {
        let redisURL = server.preferences.redisURL
        let volumePath = Preferences.fuseMountPath()
        let serviceReportsLocalNetwork = server.localNetworkPermissionSuspected
        // Single-flight: a burst of focus/activation notifications (or a
        // Re-check click during a check) starts at most one check.
        recheck.run {
            checking = true
            async let preflight = OnboardingPreflight.run(redisURL: redisURL)
            async let perms = PermissionProbes.run(
                redisURL: redisURL, volumePath: volumePath,
                serviceReportsLocalNetwork: serviceReportsLocalNetwork)
            async let helperState = Task.detached(priority: .userInitiated) {
                UnmountHelperService.shared.readiness(isolated: AppIsolation.active != nil)
            }.value
            let (result, permissionResult, helperResult) = await (preflight, perms, helperState)
            report = result
            permissions = permissionResult
            helper = helperResult
            checking = false
        }
    }

    /// Founder Q1: JuiceMount does not start a mount until its privileged
    /// eject helper is turned on and answering. "Set Up Helper…" registers
    /// it (macOS then lists it in Login Items & Extensions) and opens that
    /// pane; reading the row never registers anything.
    @ViewBuilder
    private var helperRow: some View {
        let row = helper.map(UnmountHelperDecision.row(for:))
        if row?.visible ?? true {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                helperGlyph(row?.tone)
                VStack(alignment: .leading, spacing: 2) {
                    Text(UnmountHelperDecision.rowTitle)
                        .font(.body.weight(.medium))
                    if let row {
                        Text(row.detail)
                            .font(.caption)
                            .foregroundStyle(row.tone == .failed ? .red : (row.tone == .attention ? .orange : .secondary))
                            .fixedSize(horizontal: false, vertical: true)
                        switch row.action {
                        case .setUp:
                            Button(helperBusy ? "Setting Up…" : "Set Up Helper…") { setUpHelper() }
                                .controlSize(.small)
                                .disabled(helperBusy)
                        case .openLoginItems:
                            Button("Open Login Items…") { UnmountHelperService.openLoginItems() }
                                .controlSize(.small)
                        case .none:
                            EmptyView()
                        }
                    } else {
                        Text("Checking…")
                            .font(.caption)
                            .foregroundStyle(.tertiary)
                    }
                }
                Spacer()
            }
        }
    }

    private func setUpHelper() {
        helperBusy = true
        Task {
            let result = await Task.detached(priority: .userInitiated) {
                UnmountHelperService.shared.prepare(
                    isolated: AppIsolation.active != nil,
                    mountActive: NFSBridge.isRunning)
            }.value
            helper = result
            helperBusy = false
            if result == .requiresApproval || result == .notRegistered {
                UnmountHelperService.openLoginItems()
            }
        }
    }

    @ViewBuilder
    private func helperGlyph(_ tone: UnmountHelperRow.Tone?) -> some View {
        switch tone {
        case .none:
            ProgressView().controlSize(.small).frame(width: 16)
        case .some(.ok):
            Image(systemName: "checkmark.circle.fill").foregroundStyle(.green).frame(width: 16)
        case .some(.attention):
            Image(systemName: "clock.fill").foregroundStyle(.orange).frame(width: 16)
        case .some(.failed):
            Image(systemName: "xmark.circle.fill").foregroundStyle(.red).frame(width: 16)
        case .some(.neutral):
            Image(systemName: "minus.circle").foregroundStyle(.secondary).frame(width: 16)
        }
    }

    /// One permission row: glyph + title + status detail + a System Settings
    /// deep link shown whenever the user has something to do.
    @ViewBuilder
    private func permissionRow(status: PermissionStatus?, title: String, fixTitle: String, fix: @escaping () -> Void) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            permissionGlyph(status)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.body.weight(.medium))
                if let status {
                    Text(permissionDetail(status))
                        .font(.caption)
                        .foregroundStyle(permissionDetailColor(status))
                        .fixedSize(horizontal: false, vertical: true)
                    if status.needsUserAction {
                        Button(fixTitle, action: fix)
                            .controlSize(.small)
                    }
                } else {
                    Text("Checking…")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            Spacer()
        }
    }

    private func permissionDetail(_ status: PermissionStatus) -> String {
        switch status {
        case .granted: return "Allowed."
        case .denied(let s), .pending(let s), .notYetRequested(let s), .unknown(let s), .notApplicable(let s): return s
        }
    }

    private func permissionDetailColor(_ status: PermissionStatus) -> Color {
        switch status {
        case .denied: return .red
        case .pending: return .orange
        default: return .secondary
        }
    }

    @ViewBuilder
    private func permissionGlyph(_ status: PermissionStatus?) -> some View {
        switch status {
        case .none:
            ProgressView().controlSize(.small).frame(width: 16)
        case .some(.granted):
            Image(systemName: "checkmark.circle.fill").foregroundStyle(.green).frame(width: 16)
        case .some(.denied):
            Image(systemName: "xmark.circle.fill").foregroundStyle(.red).frame(width: 16)
        case .some(.pending):
            Image(systemName: "clock.fill").foregroundStyle(.orange).frame(width: 16)
        case .some(.notYetRequested), .some(.notApplicable):
            Image(systemName: "minus.circle").foregroundStyle(.secondary).frame(width: 16)
        case .some(.unknown):
            Image(systemName: "questionmark.circle").foregroundStyle(.secondary).frame(width: 16)
        }
    }

    /// One checklist row: status glyph + title + pass/fail detail + an
    /// optional fix view shown only on failure.
    @ViewBuilder
    private func checkRow<Fix: View>(
        ok: Bool?,
        title: String,
        passDetail: String,
        failDetail: String,
        fixHint: String,
        @ViewBuilder fix: () -> Fix
    ) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            statusGlyph(ok: ok)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.body.weight(.medium))
                switch ok {
                case .some(true):
                    Text(passDetail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                case .some(false):
                    Text(failDetail)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .fixedSize(horizontal: false, vertical: true)
                    Text(fixHint)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                    fix()
                case .none:
                    Text("Checking…")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            Spacer()
        }
    }

    @ViewBuilder
    private func statusGlyph(ok: Bool?) -> some View {
        switch ok {
        case .some(true):
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
        case .some(false):
            Image(systemName: "xmark.circle.fill")
                .foregroundStyle(.red)
        case .none:
            ProgressView()
                .controlSize(.small)
                .frame(width: 16)
        }
    }

    /// Monospaced command with a copy button — for "brew install juicefs"
    /// style hints.
    private func copyableCommand(_ cmd: String) -> some View {
        HStack(spacing: 6) {
            Text(cmd)
                .font(.caption.monospaced())
                .textSelection(.enabled)
                .padding(.horizontal, 6)
                .padding(.vertical, 3)
                .background(
                    RoundedRectangle(cornerRadius: 4)
                        .fill(Color.secondary.opacity(0.1))
                )
            Button {
                let pb = NSPasteboard.general
                pb.clearContents()
                pb.setString(cmd, forType: .string)
            } label: {
                Image(systemName: "doc.on.doc")
                    .font(.caption)
            }
            .buttonStyle(.borderless)
            .help("Copy command")
        }
    }
}

// BEGIN BundledJuiceFS — extracted verbatim by Tests/bundled_juicefs_preflight_probe.py
/// The JuiceFS engine the app ships beside its executable. Mirrors
/// health/fuse.go findJuiceFSBin (same location, no PATH or Homebrew) and the
/// scripts/verify-build.sh gate (pinned version, same signer as the app).
enum BundledJuiceFS {
    static let name = "juicefs"
    static let versionInfoKey = "JMBundledJuiceFSVersion"

    struct Problem: Error, Equatable { let message: String }

    static func check(executable: URL?, expectedVersion: String?) -> Result<String, Problem> {
        guard let executable else { return .failure(Problem(message: "The app's own executable path is unknown.")) }
        let path = executable.deletingLastPathComponent().appendingPathComponent(name).path
        guard FileManager.default.isExecutableFile(atPath: path) else {
            return .failure(Problem(message: "The bundled JuiceFS engine is missing from this copy of JuiceMount."))
        }
        guard let expectedVersion, !expectedVersion.isEmpty else {
            return .failure(Problem(message: "This copy of JuiceMount does not record its JuiceFS version."))
        }
        guard let appSigner = signer(of: executable.path), let engineSigner = signer(of: path),
              engineSigner == appSigner else {
            return .failure(Problem(message: "The bundled JuiceFS engine is not signed by JuiceMount's signer."))
        }
        guard let version = versionOutput(path), version == expectedVersion else {
            return .failure(Problem(message: "The bundled JuiceFS engine is not the version this JuiceMount expects."))
        }
        return .success(path)
    }

    /// A valid code signature's identity: team ID plus leaf certificate, or
    /// "adhoc" for an ad-hoc signature. nil when unsigned or invalid.
    static func signer(of path: String) -> String? {
        var code: SecStaticCode?
        guard SecStaticCodeCreateWithPath(URL(fileURLWithPath: path) as CFURL, [], &code) == errSecSuccess,
              let code,
              SecStaticCodeCheckValidity(code, SecCSFlags(rawValue: kSecCSStrictValidate), nil) == errSecSuccess
        else { return nil }
        var info: CFDictionary?
        guard SecCodeCopySigningInformation(code, SecCSFlags(rawValue: kSecCSSigningInformation), &info) == errSecSuccess,
              let dict = info as? [String: Any] else { return nil }
        guard let certs = dict[kSecCodeInfoCertificates as String] as? [SecCertificate], let leaf = certs.first else {
            return "adhoc"
        }
        let team = dict[kSecCodeInfoTeamIdentifier as String] as? String ?? ""
        return team + ":" + (SecCertificateCopyData(leaf) as Data).base64EncodedString()
    }

    /// `juicefs --version`, bounded so a wedged binary cannot hold preflight.
    static func versionOutput(_ path: String, timeout: TimeInterval = 5) -> String? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: path)
        process.arguments = ["--version"]
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        let done = DispatchSemaphore(value: 0)
        process.terminationHandler = { _ in done.signal() }
        do { try process.run() } catch { return nil }
        if done.wait(timeout: .now() + timeout) == .timedOut {
            process.terminate()
            return nil
        }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        guard process.terminationStatus == 0 else { return nil }
        return String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
// END BundledJuiceFS
