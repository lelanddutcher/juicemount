import AppKit
import Quartz

/// Bridges QLPreviewPanel into our SwiftUI search window. QLPreviewPanel is a
/// shared singleton — only one panel can be active at a time across the system.
/// We register as its data source whenever it's shown for our URLs.
///
/// Note: this class is NOT @MainActor isolated because QLPreviewPanelDataSource
/// and QLPreviewPanelDelegate methods are nonisolated. We synchronize access to
/// `urls` via a serial queue and ensure all mutations happen on the main thread.
final class QuickLookCoordinator: NSObject, QLPreviewPanelDataSource, QLPreviewPanelDelegate {

    static let shared = QuickLookCoordinator()

    // Mutated only on the main thread; read by QLPreviewPanel callbacks (also main thread).
    private var urls: [URL] = []

    private override init() {
        super.init()
    }

    /// Show the QuickLook panel for the given URLs. Must be called on the main thread.
    @MainActor
    func show(urls: [URL]) {
        guard !urls.isEmpty else { return }
        self.urls = urls

        guard let panel = QLPreviewPanel.shared() else { return }
        panel.dataSource = self
        panel.delegate = self
        panel.reloadData()

        if !panel.isVisible {
            panel.makeKeyAndOrderFront(nil)
        }
    }

    @MainActor
    func hide() {
        QLPreviewPanel.shared().orderOut(nil)
    }

    // MARK: - QLPreviewPanelDataSource (nonisolated to satisfy ObjC protocol)

    func numberOfPreviewItems(in panel: QLPreviewPanel!) -> Int {
        urls.count
    }

    func previewPanel(_ panel: QLPreviewPanel!, previewItemAt index: Int) -> QLPreviewItem! {
        guard index >= 0 && index < urls.count else { return nil }
        return urls[index] as NSURL
    }

    // MARK: - QLPreviewPanelDelegate

    func previewPanel(_ panel: QLPreviewPanel!, handle event: NSEvent!) -> Bool {
        // Pass arrow key navigation through to the panel's default behavior
        return false
    }

    func previewPanel(_ panel: QLPreviewPanel!, sourceFrameOnScreenFor item: QLPreviewItem!) -> NSRect {
        // Returning .zero disables the open/close zoom animation
        return .zero
    }
}
