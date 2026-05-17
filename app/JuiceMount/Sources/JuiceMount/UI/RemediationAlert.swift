import AppKit

/// RemediationAlert (B.3, 2026-05-17) replaces raw
/// `showAlert(error.localizedDescription)` patterns with a structured
/// dialog that gives the user:
///
///   1. What failed (one short line)
///   2. Why it likely failed (best-guess cause from the category +
///      raw error string heuristics)
///   3. What to try (a concrete next step, sometimes a command)
///   4. A "Copy diagnostic" button that copies the raw error +
///      JuiceMount version + timestamp to the clipboard for bug
///      reports.
///
/// The motivation: half the QA findings in Loop A turned out to be
/// environmental (Redis unreachable, FUSE dead, auto-offline engaged)
/// — symptoms that looked like product bugs because the app surfaced
/// only the cryptic NSError. The user couldn't tell "I should restart
/// the mount" from "this is a JuiceMount bug to file."
///
/// Add a new category here for every new error path in the app.
/// Categories without an entry fall back to a generic "Try restart"
/// remediation; the dialog still renders cleanly, just less
/// helpfully.
public enum RemediationCategory {
    case pinFailed
    case unpinFailed
    case reclaimFailed
    case clearCacheFailed
    case forceEjectFailed
    case diagnosticsExportFailed
    case syncNowFailed
    case startFailed
    case stopFailed
    /// Generic fallback when the call site doesn't have a specific
    /// remediation script. Better than nothing; consider adding a
    /// dedicated case if a specific category becomes common.
    case generic(action: String)

    fileprivate var title: String {
        switch self {
        case .pinFailed: return "Pin failed"
        case .unpinFailed: return "Un-pin failed"
        case .reclaimFailed: return "Reclaim failed"
        case .clearCacheFailed: return "Clear cache failed"
        case .forceEjectFailed: return "Force eject failed"
        case .diagnosticsExportFailed: return "Diagnostics export failed"
        case .syncNowFailed: return "Sync Now failed"
        case .startFailed: return "Start failed"
        case .stopFailed: return "Stop failed"
        case .generic(let action): return "\(action) failed"
        }
    }

    /// Best-guess cause line based on the raw error string. Per-
    /// category heuristics layered on top of common patterns
    /// (network errors, permission denials, missing files). When
    /// nothing matches the heuristic, returns a category-specific
    /// generic explanation.
    fileprivate func cause(for rawError: String) -> String {
        let lower = rawError.lowercased()

        // Cross-cutting patterns checked first — they apply to any
        // category.
        if lower.contains("no route to host") {
            return "The backend server is unreachable from this Mac. " +
                   "Wi-Fi may have dropped, or the server's IP changed."
        }
        if lower.contains("connection refused") {
            return "The backend service is reachable but not " +
                   "accepting connections. The server-side container " +
                   "may be stopped or restarting."
        }
        if lower.contains("permission denied") || lower.contains("eacces") {
            return "Permission denied. Some operations need admin " +
                   "rights (mount, unmount) or specific sudoers entries."
        }

        // Category-specific.
        switch self {
        case .pinFailed:
            if lower.contains("not initialized") {
                return "The pin store isn't ready yet — the server " +
                       "may still be starting, or the pin database " +
                       "got into a bad state."
            }
            return "The pin store rejected the request. Common " +
                   "causes: server not fully started, or the path " +
                   "isn't under the JuiceMount mount."
        case .unpinFailed:
            return "The pin store couldn't remove the entry. The " +
                   "path may already be un-pinned, or the pin " +
                   "database is locked by another operation."
        case .reclaimFailed:
            return "macOS reports purgeable space but tmutil " +
                   "couldn't free any. The reclaimable space may be " +
                   "in iCloud Drive or system caches that the OS " +
                   "manages on its own under disk pressure."
        case .clearCacheFailed:
            return "Some chunk files couldn't be removed. The cache " +
                   "directory may have permission issues, or the " +
                   "JuiceFS daemon has file descriptors holding them."
        case .forceEjectFailed:
            return "The kernel didn't release the mount table " +
                   "entry. Another process may still hold an open " +
                   "file on the volume."
        case .diagnosticsExportFailed:
            return "Couldn't write the diagnostic zip. Check that " +
                   "the destination has write permission and free space."
        case .syncNowFailed:
            return "The /verify-pins endpoint didn't respond. The " +
                   "NFS server may be shutting down or already stopped."
        case .startFailed:
            return "The NFS server couldn't initialize. The most " +
                   "common cause is a configuration error: wrong " +
                   "Redis URL, wrong bucket, or wrong credentials."
        case .stopFailed:
            return "The teardown sequence hit an error. The mount " +
                   "may be in an inconsistent state — check " +
                   "Activity Monitor for stray juicefs processes."
        case .generic:
            return "An unexpected error occurred."
        }
    }

    /// Concrete next-step instructions for the user. Per-category
    /// scripts; intentionally specific rather than generic
    /// "try restarting" advice.
    fileprivate func tryThis() -> String {
        switch self {
        case .pinFailed:
            return "Open Preferences and confirm the server is " +
                   "showing the green dots in the popover. If not, " +
                   "click Stop everything, wait 5s, then Start."
        case .unpinFailed:
            return "Restart JuiceMount: Stop mount and finish sync → " +
                   "Start. If the pin still appears, file an issue " +
                   "with the diagnostic below."
        case .reclaimFailed:
            return "Wait an hour and try again — macOS purges " +
                   "snapshots on its own schedule. If the disk-free " +
                   "row still shows reclaimable space, file an issue."
        case .clearCacheFailed:
            return "Stop mount and finish sync, then run from terminal:\n" +
                   "  rm -rf ~/.juicefs/cache/*/raw/chunks/*\n" +
                   "Then Start again."
        case .forceEjectFailed:
            return "From terminal:\n" +
                   "  sudo umount -f -t nfs /Volumes/<your-mount>\n" +
                   "If that fails too, reboot the Mac."
        case .diagnosticsExportFailed:
            return "Try a different destination (Desktop usually " +
                   "works). Confirm at least 100 MB free disk space."
        case .syncNowFailed:
            return "Open the popover and check the 4 health dots. " +
                   "If any are red, fix the backend first " +
                   "(usually a network issue)."
        case .startFailed:
            return "Open Preferences, verify the active profile's " +
                   "Redis URL and bucket URL. Test them from " +
                   "terminal with:\n" +
                   "  redis-cli -u <your-redis-url> ping\n" +
                   "  curl <your-bucket-url>"
        case .stopFailed:
            return "Quit JuiceMount entirely (status bar → Quit), " +
                   "wait 5s, then reopen the app. Check Activity " +
                   "Monitor for stray 'juicefs' processes and kill " +
                   "any you find."
        case .generic:
            return "Try Stop everything → Start. If the error " +
                   "repeats, file an issue with the diagnostic " +
                   "below."
        }
    }
}

/// Presents the remediation alert on the main thread. Safe to call
/// from any queue — internally hops to main if needed.
///
/// Returns nothing; this is purely user-facing. The "Copy diagnostic"
/// button puts a plain-text snippet on the general pasteboard.
public func presentRemediation(
    _ category: RemediationCategory,
    rawError: String,
    extraContext: String? = nil
) {
    let work = {
        let alert = NSAlert()
        alert.messageText = category.title
        alert.informativeText = """
            \(category.cause(for: rawError))

            Try this:
            \(category.tryThis())
            """
        alert.alertStyle = .warning

        let okButton = alert.addButton(withTitle: "OK")
        okButton.keyEquivalent = "\r"
        let copyButton = alert.addButton(withTitle: "Copy diagnostic")
        // Make Copy non-default so accidental Enter dismisses without
        // overwriting whatever was on the clipboard.
        copyButton.keyEquivalent = ""

        let response = alert.runModal()
        if response == .alertSecondButtonReturn {
            let snippet = buildDiagnostic(
                category: category,
                rawError: rawError,
                extraContext: extraContext
            )
            let pb = NSPasteboard.general
            pb.clearContents()
            pb.setString(snippet, forType: .string)
        }
    }
    if Thread.isMainThread {
        work()
    } else {
        DispatchQueue.main.async(execute: work)
    }
}

/// Builds the plain-text snippet placed on the clipboard when the
/// user clicks "Copy diagnostic". One header line + the raw error +
/// JuiceMount version + timestamp + any per-call extra context.
/// Format is intentionally human-readable rather than JSON so it
/// pastes well into chat, email, or a GitHub issue body.
private func buildDiagnostic(
    category: RemediationCategory,
    rawError: String,
    extraContext: String?
) -> String {
    let ts = ISO8601DateFormatter().string(from: Date())
    let bundle = Bundle.main
    let version = bundle.infoDictionary?["CFBundleShortVersionString"] as? String ?? "?"
    let build = bundle.infoDictionary?["CFBundleVersion"] as? String ?? "?"
    var lines: [String] = [
        "JuiceMount remediation report",
        "  category   : \(category.title)",
        "  timestamp  : \(ts)",
        "  version    : \(version) (build \(build))",
        "  raw error  : \(rawError)",
    ]
    if let extra = extraContext, !extra.isEmpty {
        lines.append("  context    : \(extra)")
    }
    return lines.joined(separator: "\n")
}
