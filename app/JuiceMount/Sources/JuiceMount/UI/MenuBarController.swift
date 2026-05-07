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
            button.image = renderIcon(for: server.state)
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
                self.statusItem.button?.image = self.renderIcon(for: self.server.state)
                self.statusItem.button?.image?.isTemplate = true
            }
        }
    }

    // MARK: - Icon rendering

    /// Build a small status icon using SF Symbols. We use a template image so macOS
    /// inverts it for dark menu bars automatically.
    private func renderIcon(for state: ServerController.ServerState) -> NSImage? {
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
        let image = NSImage(systemSymbolName: baseSymbol, accessibilityDescription: state.displayLabel)?
            .withSymbolConfiguration(config)
        return image
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
