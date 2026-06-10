import SwiftUI
import AppKit
import Network

// MARK: - Preflight (LB-1)
//
// First-run / guided-setup checks. Each check mirrors where the truth
// already lives on the Go side so the two can't drift:
//
//   1. juicefs binary  — same candidate paths as health/fuse.go
//      findJuiceFSBin (plus $PATH as a last resort there; we check the
//      fixed paths, which is what a Homebrew or pkg install produces).
//   2. macFUSE         — /Library/Filesystems/macfuse.fs, the filesystem
//      bundle the macFUSE installer drops.
//   3. backend         — short TCP dial to the host:port parsed from the
//      Redis URL in Preferences (the metadata server; if this is down,
//      start() is guaranteed to fail).
//   4. mount privilege — informational: probes `sudo -n /sbin/mount_nfs`
//      exactly like bridge/cbridge.go runMountViaSudo. Pass = mounting is
//      prompt-free; fail = macOS will show ONE admin prompt per session
//      (expected for end users; the scoped sudoers snippet from
//      docs/dev-setup.md removes it).

struct PreflightReport: Sendable, Equatable {
    var juicefsPath: String?
    var macFUSEInstalled: Bool = false
    var backendReachable: Bool = false
    /// host:port we dialed, or a parse-failure explanation.
    var backendDetail: String = ""
    var passwordlessSudo: Bool = false

    /// The checks a successful start() actually depends on. The sudoers
    /// row is deliberately NOT critical — the admin-prompt path works.
    var criticalOK: Bool {
        juicefsPath != nil && macFUSEInstalled && backendReachable
    }
}

enum OnboardingPreflight {

    /// Mirror of health/fuse.go findJuiceFSBin's fixed candidates.
    static let juicefsCandidates = [
        "/opt/homebrew/bin/juicefs",
        "/usr/local/bin/juicefs",
        "/usr/bin/juicefs",
    ]

    static let macFUSEBundlePath = "/Library/Filesystems/macfuse.fs"

    /// Scoped sudoers snippet (docs/dev-setup.md) offered with a copy
    /// button so power users can skip the per-session admin prompt.
    static let sudoersSnippet = """
        sudo tee /etc/sudoers.d/juicemount-mount >/dev/null <<EOF
        # JuiceMount passwordless mount/unmount (see docs/dev-setup.md).
        %admin ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
        EOF
        sudo chmod 0440 /etc/sudoers.d/juicemount-mount
        sudo visudo -c -f /etc/sudoers.d/juicemount-mount
        """

    /// Run every check. Blocking pieces (TCP dial, sudo probe) happen off
    /// the calling actor; total worst case ≈ the dial timeout (3 s).
    static func run(redisURL: String) async -> PreflightReport {
        var report = PreflightReport()
        report.juicefsPath = juicefsCandidates.first {
            FileManager.default.isExecutableFile(atPath: $0)
        } ?? pathLookup("juicefs")
        report.macFUSEInstalled = FileManager.default.fileExists(atPath: macFUSEBundlePath)

        switch parseRedisHostPort(redisURL) {
        case .success(let (host, port)):
            report.backendDetail = "\(host):\(port)"
            report.backendReachable = await tcpReachable(host: host, port: port)
        case .failure(let message):
            report.backendDetail = message
            report.backendReachable = false
        }

        report.passwordlessSudo = await Task.detached(priority: .userInitiated) {
            passwordlessSudoAvailable()
        }.value
        return report
    }

    /// PATH fallback mirroring health/fuse.go findJuiceFSBin's
    /// exec.LookPath tier (Phase 3 review follow-up: without this, an
    /// unconventional install passes the Go side's check at start but
    /// fails the Swift preflight — the assistant blocks a launch that
    /// would have worked). Searches the app process's PATH, same
    /// semantics as Go's exec.LookPath, no subprocess needed.
    static func pathLookup(_ binary: String) -> String? {
        guard let pathEnv = ProcessInfo.processInfo.environment["PATH"],
              !pathEnv.isEmpty else { return nil }
        for dir in pathEnv.split(separator: ":") where !dir.isEmpty {
            let candidate = "\(dir)/\(binary)"
            if FileManager.default.isExecutableFile(atPath: candidate) {
                return candidate
            }
        }
        return nil
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
        // URLComponents handles the redis:// scheme fine; fall back to a
        // bare host:port for hand-typed values.
        let withScheme = trimmed.contains("://") ? trimmed : "redis://\(trimmed)"
        guard let comps = URLComponents(string: withScheme), let host = comps.host, !host.isEmpty else {
            return .failure("Can't parse Redis URL \"\(trimmed)\"")
        }
        let port = UInt16(clamping: comps.port ?? 6379)
        return .success((host, port))
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
            var finished = false
            // All finish() calls are funneled through `queue`, so the
            // flag is race-free and the continuation resumes exactly once.
            func finish(_ ok: Bool) {
                guard !finished else { return }
                finished = true
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
            queue.asyncAfter(deadline: .now() + timeout) {
                finish(false)
            }
        }
    }

    /// Same probe shape as bridge/cbridge.go runMountViaSudo: run one of
    /// the actually-allowed binaries under `sudo -n` and look for sudo's
    /// "password is required" gate. mount_nfs printing usage and exiting
    /// non-zero is SUCCESS for our purposes (sudo let it through).
    static func passwordlessSudoAvailable() -> Bool {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: "/usr/bin/sudo")
        p.arguments = ["-n", "/sbin/mount_nfs"]
        let pipe = Pipe()
        p.standardOutput = pipe
        p.standardError = pipe
        do {
            try p.run()
        } catch {
            return false
        }
        p.waitUntilExit()
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        let out = String(data: data, encoding: .utf8) ?? ""
        return !out.contains("password is required")
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
    @State private var checking = false
    @State private var showSudoersSnippet = false
    @State private var copiedSnippet = false

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

            // Checklist
            VStack(alignment: .leading, spacing: 12) {
                checkRow(
                    ok: report.map { $0.juicefsPath != nil },
                    title: "JuiceFS engine",
                    passDetail: report?.juicefsPath ?? "",
                    failDetail: "The juicefs binary wasn't found.",
                    fixHint: "Install it with Homebrew:"
                ) {
                    copyableCommand("brew install juicefs")
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
                    title: "Storage backend",
                    passDetail: "Reachable at \(report?.backendDetail ?? "")",
                    failDetail: "Can't reach the metadata server (\(report?.backendDetail ?? "")).",
                    fixHint: "Check that your server is running (server/juicemount-server compose stack) and that the Redis URL in Preferences points at it."
                ) {
                    Button("Open Preferences…") {
                        (NSApp.delegate as? AppDelegate)?.menuBarController?.openPreferencesWindow()
                    }
                    .controlSize(.small)
                }

                // Mount privilege — informational, never blocks Continue.
                VStack(alignment: .leading, spacing: 4) {
                    HStack(alignment: .firstTextBaseline, spacing: 8) {
                        statusGlyph(ok: report.map(\.passwordlessSudo), infoStyle: true)
                        VStack(alignment: .leading, spacing: 2) {
                            Text("Mount permission")
                                .font(.body.weight(.medium))
                            if let r = report {
                                if r.passwordlessSudo {
                                    Text("Passwordless mount is configured — no prompts.")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                } else {
                                    Text("macOS will ask for your administrator password once per session to mount the volume. That's normal. To make it prompt-free, add a scoped sudoers rule:")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                        .fixedSize(horizontal: false, vertical: true)
                                }
                            } else {
                                Text("Checking…")
                                    .font(.caption)
                                    .foregroundStyle(.tertiary)
                            }
                        }
                        Spacer()
                    }
                    if let r = report, !r.passwordlessSudo {
                        DisclosureGroup(isExpanded: $showSudoersSnippet) {
                            VStack(alignment: .leading, spacing: 6) {
                                Text(OnboardingPreflight.sudoersSnippet)
                                    .font(.caption2.monospaced())
                                    .textSelection(.enabled)
                                    .padding(8)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(
                                        RoundedRectangle(cornerRadius: 6)
                                            .fill(Color.secondary.opacity(0.1))
                                    )
                                Button(copiedSnippet ? "Copied" : "Copy to Clipboard") {
                                    let pb = NSPasteboard.general
                                    pb.clearContents()
                                    pb.setString(OnboardingPreflight.sudoersSnippet, forType: .string)
                                    copiedSnippet = true
                                }
                                .controlSize(.small)
                            }
                            .padding(.top, 4)
                        } label: {
                            Text("Show terminal snippet (optional)")
                                .font(.caption)
                        }
                        .padding(.leading, 24)
                    }
                }
            }

            Spacer(minLength: 4)

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
                    onContinue()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(checking && report == nil)
            }
        }
        .padding(20)
        .frame(width: 480)
        .frame(minHeight: 430)
        .onAppear { runChecks() }
    }

    private var continueTitle: String {
        guard let r = report else { return "Continue" }
        return r.criticalOK ? "Continue" : "Continue Anyway"
    }

    private func runChecks() {
        checking = true
        copiedSnippet = false
        let redisURL = server.preferences.redisURL
        Task {
            let result = await OnboardingPreflight.run(redisURL: redisURL)
            await MainActor.run {
                report = result
                checking = false
                // Surface the failure case in the disclosure by default so
                // the fix is visible without an extra click.
                if !result.passwordlessSudo && !result.criticalOK {
                    showSudoersSnippet = false
                }
            }
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
                        .lineLimit(1)
                        .truncationMode(.middle)
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
    private func statusGlyph(ok: Bool?, infoStyle: Bool = false) -> some View {
        switch ok {
        case .some(true):
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green)
        case .some(false):
            Image(systemName: infoStyle ? "info.circle.fill" : "xmark.circle.fill")
                .foregroundStyle(infoStyle ? Color.blue : Color.red)
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
