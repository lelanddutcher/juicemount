import Foundation
import ServiceManagement

/// Manages the "Start at Login" preference using SMAppService (macOS 13+).
/// This registers the JuiceMount app to launch automatically when the user logs in.
enum LoginItemManager {

    static func setEnabled(_ enabled: Bool) {
        let service = SMAppService.mainApp
        do {
            if enabled {
                if service.status == .enabled { return }
                try service.register()
            } else {
                if service.status != .enabled { return }
                try service.unregister()
            }
        } catch {
            NSLog("LoginItemManager: failed to update login item status: \(error.localizedDescription)")
        }
    }

    static var isEnabled: Bool {
        SMAppService.mainApp.status == .enabled
    }
}
