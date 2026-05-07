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

    func applicationDidFinishLaunching(_ notification: Notification) {
        // Run as a status bar app (no dock icon, no main window)
        NSApp.setActivationPolicy(.accessory)

        let server = ServerController()
        menuBarController = MenuBarController(server: server)

        // Register for hotkey if enabled. Use [weak self] to avoid pinning
        // the AppDelegate (and everything it owns) for the process lifetime.
        if server.preferences.showSearchHotkey {
            registerSearchHotkey()
        }

        // Auto-start the server on launch — saves the user a click.
        // If they want to manually control start/stop, they can quit and
        // start it from the popover instead. Most-common case is auto-start.
        if !NFSBridge.isRunning {
            server.start()
        }
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
