import SwiftUI
import AppKit

@main
struct JuiceMountApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        // We're a menu bar app — no Settings scene needed (we have a custom Preferences window)
        // Use a hidden Settings to satisfy SwiftUI's App protocol
        Settings {
            EmptyView()
        }
    }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    var menuBarController: MenuBarController!
    let finderService = FinderService()

    /// Kept for the quit-time stranded-writes guard (LB-3): the pending
    /// check needs the configured metrics address.
    private var server: ServerController?
    /// Panel shown while "Wait for uploads" polls the spool down to zero.
    private var spoolQuitWaitPanel: NSPanel?

    func applicationDidFinishLaunching(_ notification: Notification) {
        // Run as a status bar app (no dock icon, no main window)
        NSApp.setActivationPolicy(.accessory)

        let server = ServerController()
        self.server = server
        menuBarController = MenuBarController(server: server)

        // Register for hotkey if enabled. Use [weak self] to avoid pinning
        // the AppDelegate (and everything it owns) for the process lifetime.
        if server.preferences.showSearchHotkey {
            registerSearchHotkey()
        }

        // Register Finder right-click → Services → "JuiceMount: Pin for Offline".
        // After this, NSRegisterServicesProvider tells macOS where to dispatch
        // the service messages declared in Info.plist's NSServices array.
        // The user may need to enable the service once in System Settings →
        // Keyboard → Keyboard Shortcuts → Services on first run.
        NSApp.servicesProvider = finderService
        NSUpdateDynamicServices()

        // Auto-start the server on launch — saves the user a click.
        // If they want to manually control start/stop, they can quit and
        // start it from the popover instead. Most-common case is auto-start.
        //
        // LB-1 onboarding gate: instead of auto-starting into a dead-end
        // "Disconnected", first-run (hasCompletedOnboarding == false) or a
        // failing critical preflight (juicefs / macFUSE / backend) opens
        // the setup-assistant window, which guides the fix and starts the
        // server from its Continue button. When everything passes on an
        // already-onboarded machine, startup proceeds exactly as before —
        // the preflight file checks are instant and the TCP dial is bounded
        // at 3 s, well inside the existing start budget.
        if !NFSBridge.isRunning {
            if !server.preferences.hasCompletedOnboarding {
                menuBarController.openOnboardingWindow()
            } else {
                let redisURL = server.preferences.redisURL
                Task { @MainActor in
                    let report = await OnboardingPreflight.run(redisURL: redisURL)
                    if report.criticalOK {
                        server.start()
                    } else {
                        NFSBridge.appLog("launch preflight failed (juicefs=\(report.juicefsPath ?? "missing") macfuse=\(report.macFUSEInstalled) backend=\(report.backendReachable) \(report.backendDetail)) — opening setup assistant")
                        self.menuBarController.openOnboardingWindow()
                    }
                }
            }
        }
    }

    /// LB-3(b) stranded-writes guard. Quit's Go-side teardown
    /// (drainer.Stop with a 30 s deadline) waits only for IN-FLIGHT
    /// drains — entries still QUEUED in the spool silently wait for a
    /// next launch that may never come, even though Finder already told
    /// the user "copied". If uploads are pending, make the user decide
    /// before the teardown in applicationWillTerminate runs.
    ///
    /// The /spool fetch is blocking HTTP, so we answer `.terminateLater`,
    /// check off the main thread, and reply via
    /// `NSApp.reply(toApplicationShouldTerminate:)`.
    /// Re-entrancy guards (adversarial-review rec): a second Cmd+Q while the
    /// wait panel is up, or during the ~2s async /spool check, must not stack
    /// a second alert/panel or double-reply to a single terminateLater.
    private var quitCheckInFlight = false

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        if let panel = spoolQuitWaitPanel {
            // A wait panel is already deciding this quit — re-front it.
            panel.makeKeyAndOrderFront(nil)
            return .terminateCancel
        }
        if quitCheckInFlight {
            return .terminateCancel
        }
        quitCheckInFlight = true
        Task { @MainActor in
            let addr = self.server?.preferences.metricsAddr ?? "127.0.0.1:11050"
            let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: addr) }.value
            if let sp, sp.enabled, sp.hasActivity {
                self.presentQuitPendingAlert(sp, metricsAddr: addr)
            } else {
                // Spool off, queue empty, or metrics server unreachable
                // (server already stopped) — quit proceeds normally.
                self.quitCheckInFlight = false
                NSApp.reply(toApplicationShouldTerminate: true)
            }
        }
        return .terminateLater
    }

    private func presentQuitPendingAlert(_ sp: NFSBridge.SpoolStatus, metricsAddr: String) {
        let bytes = ByteCountFormatter.string(fromByteCount: sp.pendingBytes, countStyle: .file)
        let alert = NSAlert()
        alert.messageText = "\(sp.pendingFiles) file\(sp.pendingFiles == 1 ? "" : "s") (\(bytes)) still uploading to the NAS"
        alert.informativeText = "Finder already reported these files as copied, but they are still on this Mac's upload spool. If you quit now they upload only the next time JuiceMount runs."
        alert.alertStyle = .warning
        alert.addButton(withTitle: "Wait for uploads")
        alert.addButton(withTitle: "Quit anyway")
        alert.addButton(withTitle: "Cancel")
        NSApp.activate(ignoringOtherApps: true)
        switch alert.runModal() {
        case .alertFirstButtonReturn:
            // The wait panel takes over the re-entrancy guard from here
            // (spoolQuitWaitPanel != nil short-circuits a second Cmd+Q).
            quitCheckInFlight = false
            showQuitDrainWaitPanel(metricsAddr: metricsAddr)
        case .alertSecondButtonReturn:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: true)
        default:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        }
    }

    /// "Wait" path: a small floating panel with live progress that
    /// auto-quits when pending hits 0 (SpoolDrainWaitView calls
    /// onDrained). Offers "Quit anyway" and "Cancel" escape hatches.
    private func showQuitDrainWaitPanel(metricsAddr: String) {
        let view = SpoolDrainWaitView(
            metricsAddr: metricsAddr,
            skipTitle: "Quit anyway",
            onSkip: { [weak self] in self?.finishQuitWait(quit: true) },
            onDrained: { [weak self] in self?.finishQuitWait(quit: true) },
            onCancel: { [weak self] in self?.finishQuitWait(quit: false) }
        )
        let host = NSHostingController(rootView: view)
        let panel = NSPanel(contentViewController: host)
        panel.title = "Finishing uploads"
        panel.styleMask = [.titled] // no close button — decide via the buttons
        panel.isFloatingPanel = true
        panel.center()
        panel.makeKeyAndOrderFront(nil)
        spoolQuitWaitPanel = panel
    }

    private func finishQuitWait(quit: Bool) {
        spoolQuitWaitPanel?.orderOut(nil)
        spoolQuitWaitPanel = nil
        NSApp.reply(toApplicationShouldTerminate: quit)
    }

    func applicationWillTerminate(_ notification: Notification) {
        // Clean teardown — full unmount of FUSE + NFS (admin prompts).
        // Routine Stop from the popover stays soft (mounts persist) so
        // a Stop -> Start cycle is fast and prompt-free; only Quit does
        // the full teardown.
        HotkeyManager.shared.unregister()
        NFSBridge.shutdown()
    }

    /// Called by Preferences when the user toggles the hotkey on/off.
    /// Effect is immediate — no relaunch needed.
    func registerSearchHotkey() {
        HotkeyManager.shared.register { [weak self] in
            self?.menuBarController.openSearchWindow()
        }
    }

    func unregisterSearchHotkey() {
        HotkeyManager.shared.unregister()
    }
}
