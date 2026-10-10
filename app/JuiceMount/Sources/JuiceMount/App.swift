import SwiftUI
import AppKit
import Darwin

struct JuiceMountApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        // SwiftUI's App protocol requires a Scene, and a menu bar app has no
        // main window to give it. This empty Settings scene satisfies that —
        // but it also puts a real "Settings…" item in the app menu, and until
        // 2026-08-18 that item opened this EmptyView: a blank 900x450 window
        // with nothing in it. The app's actual settings live in the custom
        // Preferences window the popover opens.
        //
        // AppDelegate retargets the menu item at that window on launch (see
        // retargetSettingsMenuItem), so both routes reach the same place.
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
    /// Visible progress while the blocking Go teardown runs off the AppKit
    /// main thread. One terminateLater request owns this panel and reply.
    private var shutdownQuitPanel: NSPanel?
    private var quitShutdownInFlight = false
    /// Busy-Quit warning: other apps hold files open on the volume and
    /// JuiceMount will request a force-eject when the countdown ends.
    private var ejectCountdownPanel: NSPanel?
    private var ejectCountdownDriver: QuitCountdownDriver?
    private var ejectCountdownModel: EjectCountdownModel?
    /// Sequences the /eject-now POSTs beside the countdown (H-3).
    private var ejectNowRunner: EjectNowRunner?
    /// Why the busy-Quit panel was abandoned while the runner withdraws the
    /// eject a POST started (.cancelled or .holdersReleased).
    private var ejectAbandonedPhase: QuitEjectCountdown.Phase?

    func applicationDidFinishLaunching(_ notification: Notification) {
        do { try AppIsolation.requireValid() } catch {
            FileHandle.standardError.write(Data("JuiceMount isolation refused: \(error)\n".utf8))
            exit(78)
        }
        // Run as a status bar app (no dock icon, no main window)
        NSApp.setActivationPolicy(.accessory)

        let server = ServerController()
        self.server = server
        menuBarController = MenuBarController(server: server)

        // The app menu's "Settings…" item is created by SwiftUI's Settings
        // scene and points at an empty view. Point it at the real Preferences
        // window instead. Done after the menu exists, on the next runloop turn,
        // because SwiftUI installs the item during launch.
        DispatchQueue.main.async { [weak self] in
            self?.retargetSettingsMenuItem()
        }

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
        if AppIsolation.active == nil {
            NSApp.servicesProvider = finderService
            NSUpdateDynamicServices()
        }

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
                return
            }
            Task.detached { [weak self] in
                // A just-approved helper can take a few seconds to answer;
                // retry that (only that) before sending the user to Setup.
                do {
                    try UnmountHelperDecision.withLaunchRetries(
                        { try ServingService.ensureAvailable() },
                        sleep: { Thread.sleep(forTimeInterval: $0) })
                } catch {
                    await MainActor.run { self?.menuBarController.openOnboardingWindow() }
                    return
                }
                guard NFSBridge.autoStartEligible else { return }
                await MainActor.run {
                    guard let self, !NFSBridge.isRunning else { return }
                    self.startOnFirstLaunchIfEligible()
                }
            }
        }
    }

    private func startOnFirstLaunchIfEligible() {
        guard let server, server.preferences.hasCompletedOnboarding else { return }
                // #87 first-launch hang fix: the Go core needs NOTHING from
                // the preflight (start() reads only preferences.toServerConfig())
                // and already boots-while-offline (R-4) and self-recovers when
                // the backend returns. So START IMMEDIATELY — do NOT gate the
                // mount on a cold TCP dial to Redis, which on first-connect
                // could stall the whole launch ~150 s.
                //
                // The preflight still runs, but only as a NON-BLOCKING
                // diagnostic that can never hold start() back: it re-opens the
                // setup assistant ONLY for LOCAL hard-stops (juicefs binary or
                // macFUSE missing) that the Go core genuinely cannot work
                // around. A merely-unreachable backend is NOT a hard-stop — the
                // core is already serving cached navigation offline and will
                // reconnect on its own.
                NFSBridge.appLog("start() invoked (pre-preflight)")
                server.autoStart()

                let redisURL = server.preferences.redisURL
                Task.detached { @MainActor in
                    let report = await OnboardingPreflight.run(redisURL: redisURL)
                    let localHardStop = report.juicefsPath == nil || !report.macFUSEInstalled
                    if !report.criticalOK && localHardStop {
                        // A failed bundled-JuiceFS check means a damaged or
                        // tampered install; Homebrew is never consulted.
                        NFSBridge.appLog("launch preflight LOCAL hard-stop (bundled juicefs=\(report.juicefsPath ?? report.juicefsProblem ?? "failed") macfuse=\(report.macFUSEInstalled)) — opening setup assistant")
                        self.menuBarController.openOnboardingWindow()
                    } else if !report.backendReachable {
                        // Backend down at launch — the core boots offline (R-4)
                        // and recovers. Log only; do NOT reopen the assistant.
                        NFSBridge.appLog("launch preflight: backend unreachable (\(report.backendDetail)) — core started offline, will recover; not opening assistant")
                    }
                }

    }

    /// LB-3(b) stranded-writes guard. Quit's Go-side teardown
    /// (drainer.Stop with a 30 s deadline) waits only for IN-FLIGHT
    /// drains — entries still QUEUED in the spool silently wait for a
    /// next launch that may never come, even though Finder already told
    /// the user "copied". If uploads are pending, make the user decide
    /// before the asynchronous hard teardown runs.
    ///
    /// The /spool fetch is blocking HTTP, so we answer `.terminateLater`,
    /// check off the main thread, and reply via
    /// `NSApp.reply(toApplicationShouldTerminate:)`.
    /// Re-entrancy guards (adversarial-review rec): a second Cmd+Q while the
    /// wait panel is up, or during the ~2s async /spool check, must not stack
    /// a second alert/panel or double-reply to a single terminateLater.
    private var quitCheckInFlight = false

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        if quitShutdownInFlight {
            shutdownQuitPanel?.makeKeyAndOrderFront(nil)
            return .terminateCancel
        }
        if let panel = ejectCountdownPanel {
            panel.makeKeyAndOrderFront(nil)
            return .terminateCancel
        }
        if let panel = spoolQuitWaitPanel {
            // A wait panel is already deciding this quit — re-front it.
            panel.makeKeyAndOrderFront(nil)
            return .terminateCancel
        }
        if quitCheckInFlight {
            return .terminateCancel
        }
        quitCheckInFlight = true
        checkSpoolForQuit(metricsAddr: server?.preferences.metricsAddr ?? "127.0.0.1:11050")
        return .terminateLater
    }

    private func checkSpoolForQuit(metricsAddr: String) {
        Task { @MainActor in
            let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: metricsAddr) }.value
            switch sp?.safetyState ?? .unknown {
            case .verifiedEmpty:
                self.beginShutdownForQuit()
            case .pending:
                if let sp { self.presentQuitPendingAlert(sp, metricsAddr: metricsAddr) }
            case .unknown:
                self.presentQuitUnknownAlert(metricsAddr: metricsAddr)
            }
        }
    }

    private func presentQuitPendingAlert(_ sp: NFSBridge.SpoolStatus, metricsAddr: String) {
        let bytes = ByteCountFormatter.string(fromByteCount: sp.pendingBytes, countStyle: .file)
        let alert = NSAlert()
        alert.messageText = "Unfinished changes remain on this Mac"
        alert.informativeText = "\(sp.pendingFiles) files pending (\(bytes)); \(sp.pendingSymlinks) links pending; \(sp.failedFiles + sp.declinedFiles) failed or declined. Finder may already have reported them as copied. If you quit now, pending changes finish only the next time JuiceMount runs; failed files need attention."
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
            beginShutdownForQuit(allowPending: true)
        default:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        }
    }

    private func presentQuitUnknownAlert(metricsAddr: String) {
        let alert = NSAlert()
        alert.messageText = "Can't verify the upload queue"
        alert.informativeText = "JuiceMount couldn't confirm whether copied files remain on this Mac. Retry the check, cancel quitting, or explicitly quit anyway."
        alert.alertStyle = .warning
        alert.addButton(withTitle: "Retry")
        alert.addButton(withTitle: "Quit anyway")
        alert.addButton(withTitle: "Cancel")
        NSApp.activate(ignoringOtherApps: true)
        switch alert.runModal() {
        case .alertFirstButtonReturn:
            checkSpoolForQuit(metricsAddr: metricsAddr)
        case .alertSecondButtonReturn:
            beginShutdownForQuit(allowPending: true)
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
            onSkip: { [weak self] in self?.finishQuitWait(quit: true, allowPending: true) },
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

    private func finishQuitWait(quit: Bool, allowPending: Bool = false) {
        spoolQuitWaitPanel?.orderOut(nil)
        spoolQuitWaitPanel = nil
        if quit {
            beginShutdownForQuit(allowPending: allowPending)
        } else {
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        }
    }

    /// Entry point for every Quit path that has decided to disconnect. If
    /// other apps hold files open on the volume, warn first (and, once the
    /// privileged helper ships, count down to a force-eject — founder
    /// decision, 0.5); otherwise disconnect now. An unknown census (not
    /// mounted, listing timed out) never blocks Quit: the ordinary shutdown
    /// refuses a busy volume on its own.
    private func beginShutdownForQuit(allowPending: Bool = false) {
        guard !quitShutdownInFlight else {
            shutdownQuitPanel?.makeKeyAndOrderFront(nil)
            return
        }
        if let panel = ejectCountdownPanel {
            panel.makeKeyAndOrderFront(nil)
            return
        }
        // Keep the re-entrancy guard up while the holder listing runs.
        quitCheckInFlight = true
        let mountPoint = server?.preferences.mountPoint ?? ""
        Task { @MainActor in
            let census = mountPoint.isEmpty ? nil : await VolumeHolders.census(path: mountPoint)
            switch QuitHolderPlan.decide(fileApps: census?.fileApps, folderApps: census?.folderApps) {
            case .proceed:
                self.runShutdownForQuit(allowPending: allowPending)
            case .warnOpenFiles(let fileApps, let folderApps):
                NFSBridge.appLog("quit: \(fileApps.count) app(s) hold open files on the volume (\(fileApps.joined(separator: ", "))); folder-only: \(folderApps.joined(separator: ", "))")
                // The countdown promises a force only when the helper
                // answers and the service can forward eject-now (bounded).
                let metricsAddr = self.server?.preferences.metricsAddr ?? "127.0.0.1:11050"
                let forceAvailable = await Task.detached { ServingService.forceEjectAvailable(metricsAddr: metricsAddr) }.value
                NFSBridge.appLog("quit: force-eject available=\(forceAvailable)")
                guard let countdown = QuitEjectCountdown(holderApps: fileApps.count, forceAvailable: forceAvailable) else {
                    self.runShutdownForQuit(allowPending: allowPending)
                    return
                }
                self.showEjectCountdown(countdown, fileApps: fileApps, folderApps: folderApps, mountPoint: mountPoint, allowPending: allowPending)
            case .explainFoldersOnly(let folderApps):
                self.presentFolderHoldersAlert(folderApps, allowPending: allowPending)
            }
        }
    }

    /// Only working directories hold the volume. Nothing unsaved is at stake
    /// and JuiceMount never forces for this; the user moves the shell away.
    private func presentFolderHoldersAlert(_ folderApps: [String], allowPending: Bool) {
        let alert = NSAlert()
        alert.messageText = "A folder on the volume is in use"
        alert.informativeText = QuitHolderPlan.folderMessage(folderApps)
        alert.alertStyle = .informational
        alert.addButton(withTitle: "Retry")
        alert.addButton(withTitle: "Try to Disconnect Anyway")
        alert.addButton(withTitle: "Cancel Quit")
        NSApp.activate(ignoringOtherApps: true)
        switch alert.runModal() {
        case .alertFirstButtonReturn:
            beginShutdownForQuit(allowPending: allowPending)
        case .alertSecondButtonReturn:
            runShutdownForQuit(allowPending: allowPending)
        default:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        }
    }

    private func showEjectCountdown(_ countdown: QuitEjectCountdown, fileApps: [String], folderApps: [String], mountPoint: String, allowPending: Bool) {
        let model = EjectCountdownModel(countdown: countdown, holderNames: fileApps, folderNames: folderApps)
        ejectCountdownModel = model
        if countdown.forceAvailable {
            // The countdown START asks the service for an ordinary retire;
            // a volume nobody really holds may eject right away.
            let metricsAddr = server?.preferences.metricsAddr ?? "127.0.0.1:11050"
            let runner = EjectNowRunner(
                post: { phase in await Task.detached { ServingService.requestEjectNow(metricsAddr: metricsAddr, phase: phase) }.value },
                withdraw: { await Task.detached { ServingService.withdrawEjectNow(metricsAddr: metricsAddr) }.value },
                log: { NFSBridge.appLog($0) },
                onChange: { [weak model] r in
                    model?.ejectingMessage = r.ejectingMessage
                    model?.ejectingCancellable = r.offersCancel
                },
                onDone: { [weak self] outcome in self?.finishEjectNow(outcome, allowPending: allowPending) })
            ejectNowRunner = runner
        }
        let driver = QuitCountdownDriver(
            countdown: countdown,
            recount: { [weak model] in
                // Single-flight and bounded inside VolumeHolders.
                let census = await VolumeHolders.census(path: mountPoint)
                if let census, let model {
                    if !census.fileApps.isEmpty { model.holderNames = census.fileApps }
                    model.folderNames = census.folderApps
                }
                return census?.fileApps.count
            },
            onChange: { [weak model] in model?.countdown = $0 },
            onTerminal: { [weak self] phase in self?.finishEjectCountdown(phase, allowPending: allowPending) }
        )
        ejectCountdownDriver = driver
        let view = EjectCountdownView(
            model: model,
            onEjectNow: { [weak driver] in driver?.send(.ejectNow) },
            onQuitAnyway: { [weak driver] in driver?.send(.quitAnyway) },
            onCancel: { [weak self] in self?.cancelBusyQuit() }
        )
        // NSHostingView in a fixed-size panel, not NSHostingController: the
        // countdown text reflows every second, and a controller-sized window
        // re-enters SwiftUI sizing during AppKit's constraint pass (see
        // MenuBarController.openPreferencesWindow for that crash).
        let contentRect = NSRect(x: 0, y: 0, width: 420, height: 250)
        let hosting = NSHostingView(rootView: view)
        hosting.frame = contentRect
        let panel = NSPanel(contentRect: contentRect, styleMask: [.titled], backing: .buffered, defer: false)
        panel.contentView = hosting
        panel.title = "Files are open on the volume"
        panel.isReleasedWhenClosed = false
        panel.isFloatingPanel = true
        panel.center()
        NSApp.activate(ignoringOtherApps: true)
        panel.makeKeyAndOrderFront(nil)
        ejectCountdownPanel = panel
        // The driver's timer runs in .common modes: this whole flow lives
        // inside .terminateLater, where AppKit spins NSModalPanelRunLoopMode.
        driver.start()
        ejectNowRunner?.start()
    }

    private func closeEjectCountdownPanel() {
        ejectCountdownDriver?.stop()
        ejectCountdownDriver = nil
        ejectCountdownPanel?.orderOut(nil)
        ejectCountdownPanel = nil
        ejectCountdownModel = nil
    }

    /// The eject-now sequence finished: RETIRED (at the countdown start on a
    /// volume nobody holds, or after escalation), or a failure. Either way
    /// run the ordinary shutdown; a failure travels as the shutdown note.
    private func finishEjectNow(_ outcome: EjectNowRequester.Outcome, allowPending: Bool) {
        ejectNowRunner?.stop()
        ejectNowRunner = nil
        let abandoned = ejectAbandonedPhase
        ejectAbandonedPhase = nil
        if case .abandoned(let withdraw) = outcome {
            finishAbandonedBusyQuit(abandoned ?? .cancelled, withdraw: withdraw, allowPending: allowPending)
            return
        }
        closeEjectCountdownPanel()
        if abandoned != nil {
            NFSBridge.appLog("quit: a POST in flight at the cancel had already ejected the volume — ordinary shutdown")
        }
        switch outcome {
        case .retired:
            NFSBridge.appLog("quit: service retired the volume (eject-now) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending)
        case .forcePending(let why):
            NFSBridge.appLog("quit: eject-now force sent, outcome unknown (\(why)) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.shutdownEjectNote(for: outcome))
        case .outcomeUnknown(let why):
            NFSBridge.appLog("quit: eject-now got no answer, outcome unknown (\(why)) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.shutdownEjectNote(for: outcome))
        case .startUnanswered(let why):
            NFSBridge.appLog("quit: eject-now start request got no answer (\(why)) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.shutdownEjectNote(for: outcome))
        case .notAttempted(let why):
            NFSBridge.appLog("quit: eject-now not attempted (\(why)) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.shutdownEjectNote(for: outcome))
        case .failed(let why):
            NFSBridge.appLog("quit: eject-now failed: \(why) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.shutdownEjectNote(for: outcome))
        case .abandoned:
            break // handled above
        }
    }

    /// "Cancel Quit" on the busy-Quit panel. While the countdown runs it goes
    /// through the countdown; once escalated ("Ejecting…") the countdown is
    /// over and the runner withdraws directly.
    private func cancelBusyQuit() {
        if let driver = ejectCountdownDriver {
            driver.send(.cancelQuit)
        } else if let runner = ejectNowRunner, ejectAbandonedPhase == nil {
            NFSBridge.appLog("quit: Cancel Quit while ejecting — withdrawing the eject")
            ejectAbandonedPhase = .cancelled
            runner.cancel()
        }
    }

    /// The panel was abandoned and the runner has withdrawn what its POSTs
    /// started (or sent nothing: `withdraw` nil).
    private func finishAbandonedBusyQuit(_ phase: QuitEjectCountdown.Phase, withdraw: UnmountHelperDecision.WithdrawResult?, allowPending: Bool) {
        if case .forceInProgress(let why) = withdraw {
            // The force cannot be withdrawn: say so, then finish the Quit.
            // The panel already shows EjectNowRequester.forceInProgressMessage.
            NFSBridge.appLog("quit: cancel refused, force-eject already under way (\(why)) — continuing Quit")
            ejectCountdownModel?.ejectingCancellable = false
            DispatchQueue.main.asyncAfter(deadline: .now() + 4) { [weak self] in
                guard let self else { return }
                self.closeEjectCountdownPanel()
                self.runShutdownForQuit(allowPending: allowPending)
            }
            return
        }
        closeEjectCountdownPanel()
        if withdraw == .alreadyEjected {
            // The eject a POST started had already finished: there is no
            // volume left to keep, so cancelling the Quit would only strand
            // JuiceMount without its mount. Finish the Quit.
            NFSBridge.appLog("quit: cancel found the volume already ejected — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending)
            return
        }
        if phase == .holdersReleased {
            NFSBridge.appLog("quit: volume holders released their files — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending)
            return
        }
        quitCheckInFlight = false
        NSApp.reply(toApplicationShouldTerminate: false)
        if case .failed(let why) = withdraw {
            NFSBridge.appLog("quit: cancelled, but the eject withdraw failed: \(why)")
            let alert = NSAlert()
            alert.messageText = "Quit cancelled"
            alert.informativeText = "JuiceMount could not confirm that the background service stopped the eject it started (\(why)). The volume may still eject once the other apps close their files."
            alert.alertStyle = .warning
            alert.addButton(withTitle: "OK")
            NSApp.activate(ignoringOtherApps: true)
            alert.runModal()
        }
    }

    private func finishEjectCountdown(_ phase: QuitEjectCountdown.Phase, allowPending: Bool) {
        if case .ejectRequested(let trigger) = phase, let runner = ejectNowRunner {
            // Keep the panel: it now shows "Ejecting…" while the runner asks
            // the service again (honouring retry_after) until RETIRED or an
            // error. Never an instant force.
            NFSBridge.appLog("quit: eject requested (\(trigger)) — escalating eject-now")
            ejectCountdownDriver = nil
            runner.escalate(userPressed: trigger == .userChoseEjectNow)
            return
        }
        if let runner = ejectNowRunner, phase == .cancelled || phase == .holdersReleased {
            // Cancelled, or the files closed: stop POSTing and withdraw the
            // ordinary retire the start POST began, or the helper ejects the
            // volume later while JuiceMount keeps running. The panel stays
            // ("Cancelling…") until the DELETE answers; finishEjectNow then
            // completes this phase.
            ejectCountdownDriver?.stop()
            ejectCountdownDriver = nil
            ejectAbandonedPhase = phase
            runner.cancel()
            return
        }
        if ejectNowRunner != nil {
            ejectNowRunner?.stop()
            ejectNowRunner = nil
        }
        closeEjectCountdownPanel()
        switch phase {
        case .cancelled:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        case .holdersReleased:
            NFSBridge.appLog("quit: volume holders released their files — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending)
        case .quitAnywayRequested:
            NFSBridge.appLog("quit: user chose Quit anyway with files open (no force-eject available) — ordinary shutdown")
            runShutdownForQuit(allowPending: allowPending)
        case .ejectRequested(let trigger):
            // Only reachable without a runner (force unavailable), which the
            // countdown never produces; shut down ordinarily and say so.
            NFSBridge.appLog("quit: eject requested (\(trigger)) with no eject-now runner")
            runShutdownForQuit(allowPending: allowPending, ejectNote: UnmountHelperDecision.notPerformedEjectNote(UnmountHelperDecision.ejectUnavailableText))
        case .counting:
            break
        }
    }

    /// Completes the one outstanding terminateLater request only after the Go
    /// core has attempted its hard teardown. Running the cgo call in a detached
    /// task keeps AppKit responsive while macOS displays an administrator
    /// prompt or a sick filesystem exhausts its bounded cleanup deadlines.
    private func runShutdownForQuit(allowPending: Bool = false, ejectNote: String? = nil) {
        guard !quitShutdownInFlight else {
            shutdownQuitPanel?.makeKeyAndOrderFront(nil)
            return
        }
        quitCheckInFlight = false
        quitShutdownInFlight = true

        let host = NSHostingController(rootView: QuitShutdownView())
        let panel = NSPanel(contentViewController: host)
        panel.title = "Disconnecting JuiceMount"
        panel.styleMask = [.titled]
        panel.isFloatingPanel = true
        panel.center()
        NSApp.activate(ignoringOtherApps: true)
        panel.makeKeyAndOrderFront(nil)
        shutdownQuitPanel = panel

        Task { @MainActor in
            let outcome = await Task.detached { NFSBridge.shutdown(allowPending: allowPending) }.value
            self.shutdownQuitPanel?.orderOut(nil)
            self.shutdownQuitPanel = nil
            self.quitShutdownInFlight = false
            if outcome.complete {
                NSApp.reply(toApplicationShouldTerminate: true)
            } else {
                NFSBridge.appLog("quit: shutdown incomplete reason=\(outcome.reason) detail=\(outcome.detail)")
                self.presentIncompleteShutdownAlert(outcome, ejectNote: ejectNote)
            }
        }
    }

    private func presentIncompleteShutdownAlert(_ outcome: ShutdownOutcome, ejectNote: String? = nil) {
        let message = ShutdownMessage.forOutcome(outcome)
        let alert = NSAlert()
        alert.messageText = message.title
        var info = message.body
        if let ejectNote { info = ejectNote + "\n\n" + info }
        info += " Quit anyway may leave a stale volume in Finder that only a restart of the Mac clears, and unfinished uploads will wait until the next launch. Prefer Retry shutdown, or use \"Stop mount and finish sync\" and try again."
        alert.informativeText = info
        alert.alertStyle = .warning
        alert.addButton(withTitle: "Retry shutdown")
        alert.addButton(withTitle: "Quit anyway")
        alert.addButton(withTitle: "Cancel")
        if message.offersPrivacySettings {
            alert.addButton(withTitle: "Open Privacy Settings")
        }
        let openSettings = NSApplication.ModalResponse(rawValue: NSApplication.ModalResponse.alertThirdButtonReturn.rawValue + 1)
        NSApp.activate(ignoringOtherApps: true)
        switch alert.runModal() {
        case .alertFirstButtonReturn:
            quitCheckInFlight = true
            checkSpoolForQuit(metricsAddr: server?.preferences.metricsAddr ?? "127.0.0.1:11050")
        case .alertSecondButtonReturn:
            NSApp.reply(toApplicationShouldTerminate: true)
        case openSettings:
            SystemSettingsLinks.open(SystemSettingsLinks.filesAndFolders)
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        default:
            quitCheckInFlight = false
            NSApp.reply(toApplicationShouldTerminate: false)
        }
    }

    /// Repoints the app menu's "Settings…" item from SwiftUI's placeholder
    /// EmptyView scene to the real Preferences window.
    ///
    /// Matched by BOTH selector name and title, because neither alone is
    /// reliable: SwiftUI does not document the selector it installs (it is not
    /// showPreferences: on macOS 26), and the title is localized. Retried a few
    /// times because SwiftUI installs the item during launch, after
    /// applicationDidFinishLaunching runs.
    private func retargetSettingsMenuItem(attempt: Int = 0) {
        // Scan every top-level menu rather than indexing one. NSApp.mainMenu's
        // item 0 IS the app menu -- the Apple menu appears only in the
        // accessibility tree, not in mainMenu -- and indexing 1 quietly
        // searched Edit instead, which is why the first attempt at this silently
        // did nothing.
        guard let menus = NSApp.mainMenu?.items.compactMap({ $0.submenu }), !menus.isEmpty else {
            scheduleSettingsRetarget(attempt: attempt)
            return
        }
        for item in menus.flatMap(\.items) where item.target !== self {
            let selectorName = item.action.map(NSStringFromSelector) ?? ""
            let title = item.title
            let looksRight =
                selectorName.localizedCaseInsensitiveContains("settings")
                || selectorName.localizedCaseInsensitiveContains("preference")
                || title.hasPrefix("Settings")
                || title.hasPrefix("Preferences")
            guard looksRight else { continue }
            item.target = self
            item.action = #selector(openPreferencesFromMenu(_:))
            item.keyEquivalent = ","
            item.keyEquivalentModifierMask = [.command]
            return
        }
        scheduleSettingsRetarget(attempt: attempt)
    }

    private func scheduleSettingsRetarget(attempt: Int) {
        guard attempt < 10 else { return }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) { [weak self] in
            self?.retargetSettingsMenuItem(attempt: attempt + 1)
        }
    }

    @objc private func openPreferencesFromMenu(_ sender: Any?) {
        menuBarController.openPreferencesWindow()
    }

    func applicationWillTerminate(_ notification: Notification) {
        // The full FUSE + NFS teardown is completed asynchronously in
        // applicationShouldTerminate before AppKit accepts the quit. Never run
        // that blocking cgo bridge on the main thread during termination.
        HotkeyManager.shared.unregister()
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

/// Live state for the busy-Quit panel.
@MainActor @Observable
final class EjectCountdownModel {
    var countdown: QuitEjectCountdown
    var holderNames: [String]
    var folderNames: [String]
    /// Set while the eject-now sequence is escalated ("Ejecting…"),
    /// cancelling, or refused a cancel (force already under way).
    var ejectingMessage: String?
    /// While escalated, "Cancel Quit" stays offered until a cancel starts.
    var ejectingCancellable = true

    init(countdown: QuitEjectCountdown, holderNames: [String], folderNames: [String]) {
        self.countdown = countdown
        self.holderNames = holderNames
        self.folderNames = folderNames
    }
}

private struct EjectCountdownView: View {
    let model: EjectCountdownModel
    let onEjectNow: () -> Void
    let onQuitAnyway: () -> Void
    let onCancel: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 10) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.title2)
                    .foregroundStyle(.orange)
                Text("Files are still open on the volume")
                    .font(.headline)
            }
            if let ejecting = model.ejectingMessage {
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    ProgressView().controlSize(.small)
                    Text(ejecting)
                        .font(.callout)
                        .fixedSize(horizontal: false, vertical: true)
                }
            } else {
                Text(model.countdown.message)
                    .font(.callout)
                    .fixedSize(horizontal: false, vertical: true)
                    .monospacedDigit()
            }
            if !model.holderNames.isEmpty {
                Text("Open files: " + model.holderNames.joined(separator: ", "))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
            if !model.folderNames.isEmpty {
                Text(QuitHolderPlan.folderMessage(model.folderNames))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
            Spacer(minLength: 0)
            HStack {
                Spacer()
                if model.ejectingMessage == nil {
                Button("Cancel Quit", action: onCancel)
                    .keyboardShortcut(.cancelAction)
                if model.countdown.forceAvailable {
                    Button("Eject now", action: onEjectNow)
                } else {
                    Button("Quit anyway", action: onQuitAnyway)
                }
                } else if model.ejectingCancellable {
                    // Escalated: the force still waits for the service's
                    // 30 s floor, so the user may still back out.
                    Button("Cancel Quit", action: onCancel)
                        .keyboardShortcut(.cancelAction)
                }
            }
        }
        .padding(20)
        .frame(width: 420, height: 250, alignment: .topLeading)
    }
}

private struct QuitShutdownView: View {
    var body: some View {
        VStack(spacing: 14) {
            ProgressView()
                .controlSize(.large)
            Text("Disconnecting JuiceMount")
                .font(.headline)
            Text("Finishing filesystem cleanup. Finder remains available while this completes.")
                .font(.callout)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(24)
        .frame(width: 360)
    }
}
