import AppKit
import SwiftUI
import Combine
import Sparkle

/// Owns the NSStatusItem and the popover that appears when the user clicks the icon.
/// Built with AppKit because SwiftUI's MenuBarExtra has limitations around custom layouts
/// and polish — but the popover content itself is pure SwiftUI.
@MainActor
final class MenuBarController: NSObject {

    private let server: ServerController
    private let statusItem: NSStatusItem
    private let popover: NSPopover

    /// Sparkle updater. It is deliberately not started at launch: JuiceMount's
    /// privacy contract forbids silent update checks. The explicit "Check for
    /// Updates…" action starts it immediately before the user-requested check.
    /// Feed URL + EdDSA public key come from Info.plist. Retained for the
    /// process lifetime via this controller (which the AppDelegate owns).
    private let updaterController: SPUStandardUpdaterController
    private var updaterStarted = false

    private var searchWindow: NSWindow?
    private var preferencesWindow: NSWindow?
    private var onboardingWindow: NSWindow?
    private var stateTimer: Timer?

    init(server: ServerController) {
        self.server = server
        self.statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)

        let popover = NSPopover()
        popover.behavior = .transient
        popover.animates = true
        self.popover = popover

        // Construct the Sparkle updater before super.init so it's a fully
        // initialized stored property. No custom delegate/driver — Info.plist
        // carries the feed URL and public key. startingUpdater:false is both a
        // launch-performance guard and the enforcement point for manual-only
        // update checks.
        self.updaterController = SPUStandardUpdaterController(
            startingUpdater: false,
            updaterDelegate: nil,
            userDriverDelegate: nil
        )

        super.init()

        configureStatusItem()
        configurePopover()

        // Re-render the icon whenever server state changes
        startStateObservation()

    }

    deinit {
        stateTimer?.invalidate()
        stateTimer = nil
    }

    // MARK: - Status item

    private func configureStatusItem() {
        if let button = statusItem.button {
            button.action = #selector(togglePopover(_:))
            button.target = self
        }
        refreshIcon()
    }

    private func configurePopover() {
        let view = MenuPopoverView(
            server: server,
            onSearch: { [weak self] in self?.openSearchWindow() },
            onPreferences: { [weak self] in self?.openPreferencesWindow() },
            onSetupAssistant: { [weak self] in self?.openOnboardingWindow() },
            onCheckForUpdates: { [weak self] in self?.checkForUpdates() },
            onQuit: { NSApplication.shared.terminate(nil) }
        )
        popover.contentViewController = NSHostingController(rootView: view)
        popover.contentSize = NSSize(width: 320, height: 360)
    }

    /// User-initiated "Check for Updates…". As an LSUIElement (accessory)
    /// menu-bar app we have no regular activation, so Sparkle's update
    /// dialog would otherwise appear behind other apps with no way to
    /// focus it. Bring the app forward first, dismiss the popover, then
    /// drive the check so the progress/no-update/update sheets are visible
    /// and key.
    func checkForUpdates() {
        if popover.isShown {
            popover.performClose(nil)
        }
        NSApp.activate(ignoringOtherApps: true)
        if !updaterStarted {
            updaterController.startUpdater()
            updaterStarted = true
        }
        updaterController.checkForUpdates(nil)
    }

    // MARK: - State observation

    private func startStateObservation() {
        // Re-render the icon every time server.state mutates by polling at 1Hz.
        // We retain the timer so deinit can cancel it (otherwise it leaks forever).
        stateTimer?.invalidate()
        stateTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.refreshIcon()
            }
        }
    }

    // MARK: - Icon rendering (approved icon/state spec, 2026-06-10)

    /// The four state-tinted citrus-mark assets rendered by build-app.sh
    /// into Contents/Resources/menubar/. Raw value = PNG basename.
    private enum IconAsset: String {
        case healthy = "state-healthy"
        case degraded = "state-degraded"
        case offlineFiles = "state-offline-files"
        case fault = "state-fault"
    }

    /// Cache of the loaded logo images, one per state. Loaded once from the
    /// app bundle; nil entries are NOT cached so a missing asset re-probes
    /// (cheap) and the SF-Symbol fallback stays in effect.
    private var logoCache: [IconAsset: NSImage] = [:]

    /// Recompute the status-item image from current server state. The icon
    /// is the state-tinted logo when the rendered assets are bundled
    /// (normal .app build), or the legacy SF-Symbol composite when they're
    /// missing (bare `swift build` binary, or the build-script SVG render
    /// fell back).
    private func refreshIcon() {
        guard let button = statusItem.button else { return }
        let glance = server.glanceState
        let uploads = uploadsActive

        if let logo = logoImage(for: asset(for: glance)) {
            // Color IS the signal: never let macOS template-invert it.
            let alpha: CGFloat = (glance == .idle) ? 0.5 : 1.0
            let image = compositeLogoIcon(logo, alpha: alpha, uploadBadge: uploads)
            image.isTemplate = false
            image.accessibilityDescription = accessibilityLabel(for: glance, uploads: uploads)
            button.image = image
            button.toolTip = accessibilityLabel(for: glance, uploads: uploads)
        } else {
            let image = renderLegacySymbolIcon(
                for: server.state,
                selfTest: server.selfTest,
                offlineState: server.offlineState
            )
            image?.accessibilityDescription = accessibilityLabel(for: glance, uploads: uploads)
            button.image = image
            // F2: the tooltip must tell the truth in BOTH rendering paths —
            // the legacy SF-Symbol fallback previously had no tooltip at all.
            button.toolTip = accessibilityLabel(for: glance, uploads: uploads)
        }
    }

    /// Upload-activity badge trigger: the spool is on and has queued or
    /// in-flight drains, AND the server is in a state where draining can
    /// actually progress. (After Stop, pending rows persist but nothing is
    /// uploading — a badge then would lie.)
    private var uploadsActive: Bool {
        guard let sp = server.spoolStatus, sp.enabled else { return false }
        guard sp.inProgress > 0 || sp.pendingFiles > 0 else { return false }
        switch server.state {
        case .running, .syncing, .degraded: return true
        default: return false
        }
    }

    private func asset(for glance: GlanceState) -> IconAsset {
        switch glance {
        case .healthy:      return .healthy
        case .degraded:     return .degraded
        case .offlineFiles: return .offlineFiles
        case .fault:        return .fault
        case .idle:         return .healthy  // drawn at 0.5 alpha by caller
        }
    }

    private func accessibilityLabel(for glance: GlanceState, uploads: Bool) -> String {
        var label: String
        if glance == .degraded, case .degraded(let reason) = server.state {
            label = "JuiceMount: Degraded — \(reason)"
        } else if case .starting = server.state {
            // F2 honest tooltip: while starting we have NOT yet confirmed
            // bridge health — say exactly that instead of implying readiness.
            label = "JuiceMount: Starting — waiting for first health check"
        } else {
            label = "JuiceMount: \(server.glanceLabel)"
        }
        if uploads, let sp = server.spoolStatus {
            let n = sp.pendingFiles + Int(sp.inProgress)
            label += " · uploading \(n) file\(n == 1 ? "" : "s")"
        }
        return label
    }

    /// Load (and cache) one of the rendered logo PNGs from the app bundle.
    /// build-app.sh writes Contents/Resources/menubar/state-<x>.png (18px)
    /// and state-<x>@2x.png (36px); both reps are folded into one NSImage
    /// at 18pt so AppKit picks the right one per display scale.
    private func logoImage(for asset: IconAsset) -> NSImage? {
        if let cached = logoCache[asset] { return cached }
        guard let resURL = Bundle.main.resourceURL?.appendingPathComponent("menubar", isDirectory: true) else {
            return nil
        }
        let pointSize = NSSize(width: 18, height: 18)
        let image = NSImage(size: pointSize)
        for name in ["\(asset.rawValue).png", "\(asset.rawValue)@2x.png"] {
            let url = resURL.appendingPathComponent(name)
            guard let data = try? Data(contentsOf: url),
                  let rep = NSBitmapImageRep(data: data) else { continue }
            // Point size 18 over 18/36 pixels marks the reps as @1x/@2x.
            rep.size = pointSize
            image.addRepresentation(rep)
        }
        guard !image.representations.isEmpty else { return nil }
        logoCache[asset] = image
        return image
    }

    /// Draw the logo into a padded canvas (same technique as the legacy
    /// dot composite) applying the idle alpha and, when uploads are
    /// active, a small blue bottom-right badge with a tiny up-arrow.
    private func compositeLogoIcon(_ base: NSImage, alpha: CGFloat, uploadBadge: Bool) -> NSImage {
        // Fast path: untouched logo keeps its @1x/@2x reps intact.
        if alpha >= 1.0 && !uploadBadge { return base }

        let baseSize = base.size
        let canvas = NSImage(size: NSSize(width: baseSize.width + 2, height: baseSize.height + 2))
        canvas.lockFocus()
        defer { canvas.unlockFocus() }

        base.draw(in: NSRect(x: 1, y: 1, width: baseSize.width, height: baseSize.height),
                  from: .zero, operation: .sourceOver, fraction: alpha)

        if uploadBadge {
            // Blue circle + up-arrow, bottom-right — the corner the legacy
            // status dot used, so muscle memory carries over.
            let d: CGFloat = 8
            let badgeRect = NSRect(
                x: canvas.size.width - d - 0.5,
                y: 0.5,
                width: d,
                height: d
            )
            NSColor.systemBlue.setFill()
            NSBezierPath(ovalIn: badgeRect).fill()

            let cx = badgeRect.midX
            let cy = badgeRect.midY
            let arrow = NSBezierPath()
            arrow.lineWidth = 1.2
            arrow.lineCapStyle = .round
            arrow.lineJoinStyle = .round
            // Stem.
            arrow.move(to: NSPoint(x: cx, y: cy - 2.2))
            arrow.line(to: NSPoint(x: cx, y: cy + 2.2))
            // Chevron head.
            arrow.move(to: NSPoint(x: cx - 1.7, y: cy + 0.6))
            arrow.line(to: NSPoint(x: cx, y: cy + 2.3))
            arrow.line(to: NSPoint(x: cx + 1.7, y: cy + 0.6))
            NSColor.white.setStroke()
            arrow.stroke()
        }
        return canvas
    }

    /// LEGACY fallback: SF-Symbol icon + colored status dot, used only when
    /// the rendered logo assets aren't in the bundle. Template mode stays on
    /// unless a colored dot is drawn (the pre-Phase-3 behavior).
    private func renderLegacySymbolIcon(
        for state: ServerController.ServerState,
        selfTest: NFSBridge.SelfTestResult?,
        offlineState: NFSBridge.OfflineState
    ) -> NSImage? {
        // Compose: a base "drop" or "externaldrive" icon plus a small status badge color.
        // Apple's SF Symbol "externaldrive.connected.to.line.below" is perfect for "mounted volume"
        let baseSymbol: String = {
            switch state {
            case .idle, .error:                  return "externaldrive"
            case .starting, .syncing:            return "externaldrive.badge.timemachine"
            case .running:                       return "externaldrive.fill.badge.checkmark"
            case .degraded:                      return "externaldrive.fill.badge.exclamationmark"
            case .disconnected:                  return "externaldrive.fill.badge.xmark"
            }
        }()

        let config = NSImage.SymbolConfiguration(pointSize: 16, weight: .regular)
        guard let base = NSImage(systemSymbolName: baseSymbol, accessibilityDescription: state.displayLabel)?
            .withSymbolConfiguration(config) else {
            return nil
        }

        // Dot priority: offline (blue) > self-test attention (yellow/
        // red/orange) > none. Offline trumps self-test color because
        // when you're offline the self-test reading is necessarily
        // stale or measuring local-cache-only behavior.
        let dotColor: NSColor? = {
            if offlineState.offline {
                // Blue — distinct from the warning/error palette used
                // by self-test. The VISION doc specifies blue for
                // "offline, expected" so users learn it's not a fault.
                return NSColor.systemBlue
            }
            return selfTestDotColor(for: selfTest)
        }()
        guard let dotColor else {
            // Plain symbol: template mode ON so macOS inverts it for dark
            // menu bars as usual.
            base.isTemplate = true
            return base
        }

        // Composite a small dot in the lower-right corner. Pad the canvas a few
        // px so the dot doesn't get clipped by the menu-bar item bounds.
        let baseSize = base.size
        let canvas = NSImage(size: NSSize(width: baseSize.width + 2, height: baseSize.height + 2))
        canvas.lockFocus()
        defer { canvas.unlockFocus() }

        // Draw base centered.
        base.draw(in: NSRect(x: 1, y: 1, width: baseSize.width, height: baseSize.height),
                  from: .zero, operation: .sourceOver, fraction: 1.0)

        // Draw status dot — small enough to be subtle (5pt diameter) and offset
        // into the lower-right.
        let dotDiameter: CGFloat = 5
        let dotRect = NSRect(
            x: canvas.size.width - dotDiameter - 0.5,
            y: 0.5,
            width: dotDiameter,
            height: dotDiameter
        )
        dotColor.setFill()
        NSBezierPath(ovalIn: dotRect).fill()
        // A colored dot was drawn — template mode must be OFF or macOS
        // would flatten the color away.
        canvas.isTemplate = false
        return canvas
    }

    /// Returns the dot color to render for a given self-test result, or nil
    /// when no dot should be drawn (green or no result yet).
    private func selfTestDotColor(for result: NFSBridge.SelfTestResult?) -> NSColor? {
        guard let result, result.isAttentionWorthy else { return nil }
        switch result.status {
        case "yellow": return NSColor.systemYellow
        case "red":    return NSColor.systemRed
        case "error":  return NSColor.systemOrange
        default:       return nil
        }
    }

    // MARK: - Actions

    @objc private func togglePopover(_ sender: AnyObject?) {
        guard let button = statusItem.button else { return }
        if popover.isShown {
            popover.performClose(sender)
        } else {
            popover.show(relativeTo: button.bounds, of: button, preferredEdge: .minY)
            popover.contentViewController?.view.window?.makeKey()
        }
    }

    func openSearchWindow() {
        if let window = searchWindow {
            window.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        let view = SearchWindowView(server: server)
        let hosting = NSHostingController(rootView: view)
        let window = NSWindow(contentViewController: hosting)
        window.title = "JuiceMount Search"
        window.styleMask = [.titled, .closable, .miniaturizable, .resizable]
        window.setContentSize(NSSize(width: 800, height: 600))
        window.minSize = NSSize(width: 600, height: 400)
        window.center()
        window.isReleasedWhenClosed = false
        window.delegate = self
        window.identifier = NSUserInterfaceItemIdentifier("SearchWindow")
        window.titlebarAppearsTransparent = true
        searchWindow = window
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    /// LB-1 welcome / setup-assistant window. Opened automatically at
    /// launch by AppDelegate when onboarding was never completed or a
    /// critical preflight check fails; reopenable any time from the
    /// popover. Continue marks onboarding complete and starts the server
    /// if it isn't already running.
    func openOnboardingWindow() {
        if let window = onboardingWindow {
            window.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        let view = OnboardingWindowView(server: server) { [weak self] in
            guard let self else { return }
            self.server.preferences.hasCompletedOnboarding = true
            self.onboardingWindow?.close()
            // Straight to normal startup — but never stack a second start
            // on a server that's already up (e.g. assistant reopened later).
            switch self.server.state {
            case .idle:
                if !NFSBridge.isRunning {
                    self.server.start()
                }
            case .error, .disconnected:
                // Review P2-A: Continue from a failed state used to silently
                // no-op — the user fixed the backend, re-ran the checks
                // (green), clicked Continue, and nothing happened. Drive the
                // recovery the popover prescribes ("Stop everything, then
                // Start") for them.
                self.server.stop { [weak self] in
                    self?.server.start()
                }
            default:
                break // already starting/running — nothing to do
            }
        }
        // Same shape as the Preferences window, for the same reason: a window
        // whose size is derived from an NSHostingController re-enters SwiftUI's
        // preferredContentSize getter during AppKit's constraint pass, and any
        // content whose ideal height moves can then trip the 141-iteration
        // display-cycle limit and abort the process. This window's height DOES
        // move -- the preflight rows change as each check resolves. Converted
        // pre-emptively on 2026-08-18 alongside the Preferences fix rather than
        // waiting for it to crash in front of a first-run user, which is the
        // worst possible audience for it.
        let hosting = NSHostingView(rootView: view)
        let contentRect = NSRect(x: 0, y: 0, width: 620, height: 600)
        hosting.frame = contentRect
        let window = NSWindow(
            contentRect: contentRect,
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentView = hosting
        window.title = "JuiceMount Setup"
        window.minSize = NSSize(width: 560, height: 420)
        window.center()
        window.isReleasedWhenClosed = false
        window.delegate = self
        window.identifier = NSUserInterfaceItemIdentifier("OnboardingWindow")
        onboardingWindow = window
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    func openPreferencesWindow() {
        if let window = preferencesWindow {
            window.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        let view = PreferencesWindowView(preferences: server.preferences, server: server)

        // NSHostingView, NOT NSHostingController — and the window is built
        // from a contentRect rather than from a view controller. That is the
        // whole fix; it is not a style preference.
        //
        // AppKit's own -[NSViewController updateViewConstraints] reads the
        // controller's preferredContentSize while it is updating constraints.
        // On NSHostingController that getter is not a passive read: it runs
        // SwiftUI's sizeThatFits, which calls ViewGraph.setProposedSize, which
        // calls graphDidChange, which calls setNeedsUpdateConstraints: on the
        // hosting view -- back into the pass that is already running:
        //
        //   -[NSViewController updateViewConstraints]
        //     -> NSHostingController.preferredContentSize.getter
        //       -> ViewGraph.setProposedSize -> graphDidChange
        //         -> NSHostingView.setNeedsUpdateConstraints:
        //           -> -[NSWindow _postWindowNeedsUpdateConstraints] -> throw
        //
        // AppKit allows 141 of those per display cycle and then raises, which
        // +[NSApplication _crashOnException:] turns into SIGTRAP. It killed the
        // app again on 2026-08-18 at 10:36:56 in a build that ALREADY had the
        // earlier fix (ea1c473) -- because that fix removed one DRIVER (a
        // hand-maintained height that changed with the disclosure carets) while
        // leaving the reentrant path itself in place. Anything that still moves
        // the content's ideal height re-arms it, and two things do: the
        // "Advanced addresses" and "Use a custom location instead" disclosure
        // groups, and the polled spool counters whose footer text reflows.
        //
        // Dropping sizingOptions is NOT sufficient, because the getter is
        // called by AppKit whether or not we opted into content sizing. With no
        // view controller in the window there is no updateViewConstraints to
        // call it from, so the cycle cannot start. The window owns its size;
        // the content fills it. One authority, no feedback edge.
        //
        // Each tab is a .formStyle(.grouped) Form, which scrolls on macOS, so a
        // tab taller than the window scrolls instead of clipping -- which is
        // what made a fixed size unacceptable before.
        let hosting = NSHostingView(rootView: view)
        let contentRect = NSRect(x: 0, y: 0, width: 620, height: 640)
        hosting.frame = contentRect
        let window = NSWindow(
            contentRect: contentRect,
            styleMask: [.titled, .closable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.contentView = hosting
        window.title = "JuiceMount Preferences"
        // A floor, so the grouped forms never get squeezed to the point their
        // labels truncate; the ceiling is the screen.
        window.minSize = NSSize(width: 560, height: 420)
        window.center()
        window.isReleasedWhenClosed = false
        window.delegate = self
        window.identifier = NSUserInterfaceItemIdentifier("PreferencesWindow")
        preferencesWindow = window
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
}

extension MenuBarController: NSWindowDelegate {
    func windowWillClose(_ notification: Notification) {
        guard let window = notification.object as? NSWindow else { return }
        if window === searchWindow { searchWindow = nil }
        if window === preferencesWindow { preferencesWindow = nil }
        if window === onboardingWindow { onboardingWindow = nil }
    }
}
