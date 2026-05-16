import AppKit
import SwiftUI
import Combine

/// Owns the NSStatusItem and the popover that appears when the user clicks the icon.
/// Built with AppKit because SwiftUI's MenuBarExtra has limitations around custom layouts
/// and polish — but the popover content itself is pure SwiftUI.
@MainActor
final class MenuBarController: NSObject {

    private let server: ServerController
    private let statusItem: NSStatusItem
    private let popover: NSPopover

    private var searchWindow: NSWindow?
    private var preferencesWindow: NSWindow?
    private var stateTimer: Timer?

    init(server: ServerController) {
        self.server = server
        self.statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)

        let popover = NSPopover()
        popover.behavior = .transient
        popover.animates = true
        self.popover = popover

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
            button.image = renderIcon(for: server.state,
                                       selfTest: server.selfTest,
                                       offlineState: server.offlineState)
            // Initial state: no self-test result yet, so the icon is a plain
            // template. The state-observation timer will recompute this each
            // tick and toggle template mode off if a colored dot needs to show.
            button.image?.isTemplate = true
            button.action = #selector(togglePopover(_:))
            button.target = self
        }
    }

    private func configurePopover() {
        let view = MenuPopoverView(
            server: server,
            onSearch: { [weak self] in self?.openSearchWindow() },
            onPreferences: { [weak self] in self?.openPreferencesWindow() },
            onQuit: { NSApplication.shared.terminate(nil) }
        )
        popover.contentViewController = NSHostingController(rootView: view)
        popover.contentSize = NSSize(width: 320, height: 360)
    }

    // MARK: - State observation

    private func startStateObservation() {
        // Re-render the icon every time server.state mutates by polling at 1Hz.
        // We retain the timer so deinit can cancel it (otherwise it leaks forever).
        stateTimer?.invalidate()
        stateTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] _ in
            Task { @MainActor in
                guard let self else { return }
                self.statusItem.button?.image = self.renderIcon(
                    for: self.server.state,
                    selfTest: self.server.selfTest,
                    offlineState: self.server.offlineState
                )
                // Template-mode is OFF whenever ANY colored dot is
                // drawn — offline blue (iter 5), self-test yellow/red,
                // or self-test error orange. Otherwise template mode
                // stays on so macOS inverts the icon for dark menu
                // bars as usual.
                let offlineDot = self.server.offlineState.offline
                let attention = self.server.selfTest?.isAttentionWorthy ?? false
                self.statusItem.button?.image?.isTemplate = !(offlineDot || attention)
            }
        }
    }

    // MARK: - Icon rendering

    /// Build a small status icon using SF Symbols, optionally overlaying a
    /// colored dot in the lower-right when the self-test result is non-green.
    /// The base image is a template; when a dot is drawn we composite a
    /// non-template image and disable template mode on the result.
    private func renderIcon(
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

    func openPreferencesWindow() {
        if let window = preferencesWindow {
            window.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }
        let view = PreferencesWindowView(preferences: server.preferences, server: server)
        let hosting = NSHostingController(rootView: view)
        let window = NSWindow(contentViewController: hosting)
        window.title = "JuiceMount Preferences"
        window.styleMask = [.titled, .closable]
        window.setContentSize(NSSize(width: 520, height: 480))
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
    }
}
