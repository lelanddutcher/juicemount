import AppKit
import Foundation
import os.log

/// Exposes "Pin for Offline (JuiceMount)" as a macOS Service that appears
/// in Finder's right-click menu under Services. Receives file/folder URLs
/// from the selection and routes them through NFSBridge.pin().
///
/// macOS Services are registered via Info.plist (NSServices array) and
/// activated when the app calls NSApp.servicesProvider = (this instance)
/// during applicationDidFinishLaunching. The user may need to enable the
/// service once in System Settings → Keyboard → Keyboard Shortcuts →
/// Services on the first run; on macOS 13+ services from running apps
/// are usually auto-discovered.
@MainActor
final class FinderService: NSObject {

    private let log = Logger(subsystem: "com.juicemount.app", category: "FinderService")

    /// Pin handler. The selector is wired up in Info.plist via NSMessage.
    /// `pasteboard` carries the selected URLs from Finder.
    @objc func pinForOffline(
        _ pasteboard: NSPasteboard,
        userData: String?,
        error errorPtr: AutoreleasingUnsafeMutablePointer<NSString?>
    ) {
        guard let urls = pasteboard.readObjects(forClasses: [NSURL.self], options: nil) as? [URL] else {
            errorPtr.pointee = "JuiceMount: no folder selection on the pasteboard."
            log.error("FinderService got empty pasteboard")
            return
        }

        log.info("FinderService received \(urls.count) URL(s) for pinning")

        // Filter to folders (and also accept files — pin individual files too).
        // We do NOT prevalidate that the path is under /Volumes/zpool because
        // the user might have an unusual mount point in Preferences.
        let pinnable = urls.filter { url in
            (try? url.resourceValues(forKeys: [.isReadableKey]))?.isReadable ?? true
        }
        guard !pinnable.isEmpty else {
            errorPtr.pointee = "JuiceMount: nothing readable in the selection."
            return
        }

        // Run pinning on a background queue. The user already saw the menu
        // dismiss; we don't want to block the Finder process while we walk
        // a 1 TB tree.
        DispatchQueue.global(qos: .userInitiated).async {
            var totalFiles = 0
            var failures: [String] = []
            for url in pinnable {
                do {
                    let result = try NFSBridge.pin(url.path)
                    if let err = result.error, !err.isEmpty {
                        failures.append("\(url.lastPathComponent): \(err)")
                    } else {
                        totalFiles += result.files_pinned
                    }
                } catch {
                    failures.append("\(url.lastPathComponent): \(error.localizedDescription)")
                }
            }

            DispatchQueue.main.async {
                self.deliverPinNotification(totalFiles: totalFiles, failures: failures)
            }
        }
    }

    /// Quick "marked offline" toggle right-click action. Flips the global
    /// offline flag. Provided so the user can flip from the airport
    /// without opening the menu bar.
    @objc func toggleOfflineMode(
        _ pasteboard: NSPasteboard,
        userData: String?,
        error errorPtr: AutoreleasingUnsafeMutablePointer<NSString?>
    ) {
        let now = NFSBridge.isOffline
        NFSBridge.setOffline(!now)
        deliverOfflineNotification(now: !now)
    }

    private func deliverPinNotification(totalFiles: Int, failures: [String]) {
        let n = NSUserNotification()
        if failures.isEmpty {
            n.title = "JuiceMount"
            n.informativeText = "Pinned \(totalFiles) files for offline use. Watch progress in the menu bar."
        } else {
            n.title = "JuiceMount — partial pin"
            n.informativeText = "Pinned \(totalFiles) files; \(failures.count) failure(s). Check the menu bar."
        }
        NSUserNotificationCenter.default.deliver(n)
    }

    private func deliverOfflineNotification(now: Bool) {
        let n = NSUserNotification()
        n.title = "JuiceMount — Offline mode \(now ? "ON" : "OFF")"
        n.informativeText = now
            ? "Reads on un-cached files will fail fast."
            : "Reads will fall through to backend on cache miss."
        NSUserNotificationCenter.default.deliver(n)
    }
}
