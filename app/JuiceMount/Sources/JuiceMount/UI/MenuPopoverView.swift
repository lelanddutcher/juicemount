import SwiftUI

/// The popover shown when the user clicks the menu bar icon.
/// Layout matches the design from the roadmap, with native macOS aesthetics.
struct MenuPopoverView: View {

    @Bindable var server: ServerController
    let onSearch: () -> Void
    let onPreferences: () -> Void
    let onQuit: () -> Void

    /// Computed mirror of `server.cacheStatus` — kept under the original
    /// name so the rest of this view's bindings stay untouched. The
    /// underlying cgo call now runs on `ServerController.workQueue`
    /// (see `ServerController.refreshCacheStatus`), never on MainActor.
    private var cacheStatus: NFSBridge.CacheStatus { server.cacheStatus }
    @State private var cacheTimer: Timer?
    @State private var offlineToggleBusy = false
    @State private var diskFreeGB: Double = 0
    @State private var diskImportantGB: Double = 0
    @State private var diskTotalGB: Double = 0
    @State private var reclaimBusy = false
    @State private var cacheClearBusy = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()
            volumeSection
            Divider()
            healthSection
            Divider()
            cacheSection
            Divider()
            actionsSection
        }
        .frame(width: 340)
        .padding(.vertical, 8)
        .background(.ultraThinMaterial)
        .onAppear {
            refreshCacheStatus()
            refreshDiskSpace()
            cacheTimer = Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { _ in
                Task { @MainActor in
                    refreshCacheStatus()
                    refreshDiskSpace()
                }
            }
        }
        .onDisappear {
            cacheTimer?.invalidate()
            cacheTimer = nil
        }
    }

    private func refreshCacheStatus() {
        // Dispatches the cgo call to ServerController.workQueue and hops
        // back to MainActor to publish. The view rerenders via the
        // @Bindable server reference.
        server.refreshCacheStatus()
    }

    /// Reads the system's view of disk space — both `volumeAvailableCapacityKey`
    /// (what statfs sees) and `volumeAvailableCapacityForImportantUsageKey`
    /// (what the system would free for an important request, including
    /// purgeable Time Machine snapshots and system caches). The gap between
    /// these is what the "Reclaim" button can recover.
    private func refreshDiskSpace() {
        let url = URL(fileURLWithPath: "/")
        let keys: Set<URLResourceKey> = [
            .volumeAvailableCapacityKey,
            .volumeAvailableCapacityForImportantUsageKey,
            .volumeTotalCapacityKey,
        ]
        guard let v = try? url.resourceValues(forKeys: keys) else { return }
        let toGB: (Int?) -> Double = { ($0.map { Double($0) / 1e9 }) ?? 0 }
        diskFreeGB = toGB(v.volumeAvailableCapacity)
        diskTotalGB = toGB(v.volumeTotalCapacity)
        diskImportantGB = (v.volumeAvailableCapacityForImportantUsage.map {
            Double($0) / 1e9
        }) ?? diskFreeGB
    }

    /// "Cache disk: 38 GB free · 283 GB reclaimable" with a Reclaim button.
    /// Plus an inline pressure banner when JuiceFS is about to stop caching
    /// or has already stopped — the actual operational thresholds, not a
    /// vague "pinned > free" signal.
    private var diskSpaceRow: some View {
        let purgeable = max(0, diskImportantGB - diskFreeGB)
        let pinnedGB = Double(cacheStatus.aggregate.TotalBytes) / 1e9

        // JuiceFS is launched with --free-space-ratio 0.01 — it skips cache
        // writes when free < 1% of total disk. Surface that operational
        // reality, not theoretical concerns.
        let freeRatio = diskTotalGB > 0 ? diskFreeGB / diskTotalGB : 1.0
        let cacheOff = freeRatio < 0.01     // hard cutoff: JuiceFS already refusing
        let cacheCutoffSoon = freeRatio < 0.03 && !cacheOff
        let pinnedExceedsTotal = diskTotalGB > 0 && pinnedGB > diskTotalGB

        return VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 6) {
                Image(systemName: "internaldrive")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                Text("\(String(format: "%.0f", diskFreeGB)) GB free")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                if purgeable >= 5 {
                    Text("· \(String(format: "%.0f", purgeable)) GB reclaimable")
                        .font(.caption2)
                        .foregroundStyle(.orange)
                    Spacer()
                    Button {
                        triggerReclaim()
                    } label: {
                        if reclaimBusy {
                            ProgressView().controlSize(.small)
                        } else {
                            Text("Reclaim").font(.caption2)
                        }
                    }
                    .controlSize(.mini)
                    .disabled(reclaimBusy)
                    .help("Thin Time Machine local snapshots and other purgeable space so JuiceFS can use it for cache.")
                    cacheClearButton
                } else {
                    Spacer()
                    cacheClearButton
                }
            }

            // Banner only for actionable, real pressure. Three specific
            // states; nothing else generates noise.
            if cacheOff {
                pressureBanner(
                    color: .red,
                    text: "Disk under 1% free — JuiceFS has stopped caching. Reads fall back to network until you free space (try Reclaim)."
                )
            } else if cacheCutoffSoon {
                pressureBanner(
                    color: .orange,
                    text: "Disk under 3% free — JuiceFS will stop caching at 1% free. Reclaim or unpin large folders to keep caching alive."
                )
            } else if pinnedExceedsTotal {
                pressureBanner(
                    color: .red,
                    text: String(format:
                        "Pinned set %.0f GB exceeds disk capacity %.0f GB. Some files will never fully cache.",
                        pinnedGB, diskTotalGB)
                )
            }
        }
    }

    private func pressureBanner(color: Color, text: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 4) {
            Image(systemName: "exclamationmark.triangle.fill")
                .font(.caption2)
                .foregroundStyle(color)
            Text(text)
                .font(.caption2)
                .foregroundStyle(color)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(.vertical, 2)
    }

    /// Calls /verify-pins on the local control plane. Fire-and-forget; the
    /// server returns immediately after enqueueing. Progress is observable
    /// via the live cache stats in the popover (pending counter rising,
    /// then ticking down as workers complete each file).
    private func triggerVerifyPins() {
        DispatchQueue.global(qos: .userInitiated).async {
            let url = URL(string: "http://127.0.0.1:11050/verify-pins")!
            var req = URLRequest(url: url)
            req.httpMethod = "POST"
            URLSession.shared.dataTask(with: req) { data, _, err in
                if let err = err {
                    NSLog("[JuiceMount] verify-pins failed: %@",
                          err.localizedDescription)
                    return
                }
                guard let data = data,
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                else { return }
                let n = obj["reenqueued"] as? Int ?? 0
                let total = obj["total_pinned"] as? Int ?? 0
                NSLog("[JuiceMount] verify-pins: re-enqueued %d / %d files", n, total)
                DispatchQueue.main.async { refreshCacheStatus() }
            }.resume()
        }
    }

    private func triggerReclaim() {
        reclaimBusy = true
        DispatchQueue.global(qos: .userInitiated).async {
            let url = URL(string: "http://127.0.0.1:11050/reclaim")!
            var req = URLRequest(url: url)
            req.httpMethod = "POST"
            let sem = DispatchSemaphore(value: 0)
            var freedGB: Double = 0
            var errMsg: String?
            URLSession.shared.dataTask(with: req) { data, _, err in
                defer { sem.signal() }
                if let err = err { errMsg = err.localizedDescription; return }
                guard let data = data,
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                else { return }
                if let f = obj["freed_gb"] as? Double { freedGB = f }
                if let e = obj["error"] as? String { errMsg = e }
            }.resume()
            sem.wait()
            DispatchQueue.main.async {
                reclaimBusy = false
                refreshDiskSpace()
                if let errMsg = errMsg {
                    showAlert(title: "Reclaim failed", message: errMsg)
                } else if freedGB < 0.1 {
                    showAlert(title: "Nothing to reclaim",
                              message: "macOS reports purgeable space, but tmutil couldn't free any. The reclaimable space may be in iCloud Drive or system caches that the system manages on its own under disk pressure.")
                } else {
                    NSLog("[JuiceMount] Reclaimed %.1f GB", freedGB)
                }
            }
        }
    }

    /// Calls /cache-clear on the local control plane with
    /// keep-pinned=true so pinned content immediately starts
    /// re-downloading rather than evicting along with everything else.
    /// Fire-and-forget on the prefetcher side; user sees progress in
    /// the cache stats row tick down to zero then back up.
    private func triggerClearCache() {
        cacheClearBusy = true
        DispatchQueue.global(qos: .userInitiated).async {
            let url = URL(string: "http://127.0.0.1:11050/cache-clear?keep-pinned=true")!
            var req = URLRequest(url: url)
            req.httpMethod = "POST"
            let sem = DispatchSemaphore(value: 0)
            var freedGB: Double = 0
            var filesRemoved: Int = 0
            var errMsg: String?
            URLSession.shared.dataTask(with: req) { data, _, err in
                defer { sem.signal() }
                if let err = err { errMsg = err.localizedDescription; return }
                guard let data = data,
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                else { return }
                if let f = obj["bytes_freed_gb"] as? Double { freedGB = f }
                if let n = obj["files_removed"] as? Int { filesRemoved = n }
                if let e = obj["error"] as? String { errMsg = e }
            }.resume()
            sem.wait()
            DispatchQueue.main.async {
                cacheClearBusy = false
                refreshDiskSpace()
                refreshCacheStatus()
                if let errMsg = errMsg {
                    showAlert(title: "Clear cache failed", message: errMsg)
                } else {
                    NSLog("[JuiceMount] Cleared %d chunks, freed %.1f GB; pinned content re-queueing",
                          filesRemoved, freedGB)
                }
            }
        }
    }

    // MARK: - Diagnostics export

    /// In-app rescue: force-unmount the NFS volume via the privileged
    /// `umount -f -t nfs` path. Used when the kernel mount table has a
    /// wedged entry (server died, kernel still has the mount registered,
    /// every stat() hangs). Confirms first so it can't be accidental,
    /// then POSTs /force-eject on the control plane which triggers the
    /// AppleScript-with-admin path.
    private func forceEjectMount() {
        let alert = NSAlert()
        alert.messageText = "Force Eject JuiceMount?"
        alert.informativeText = """
            This will unmount /Volumes/zpool using a privileged kernel-level \
            unmount. macOS will prompt for your administrator password.

            Use this only if Stop didn't work or Finder is hanging on the \
            mount. Any file currently open from the mount will see an \
            "input/output error."
            """
        alert.alertStyle = .warning
        alert.addButton(withTitle: "Force Eject")
        alert.addButton(withTitle: "Cancel")
        guard alert.runModal() == .alertFirstButtonReturn else { return }

        DispatchQueue.global(qos: .userInitiated).async {
            guard let url = URL(string: "http://127.0.0.1:11050/force-eject") else { return }
            var req = URLRequest(url: url)
            req.httpMethod = "POST"
            // Long timeout — the AppleScript admin prompt may sit waiting
            // for the user to enter their password before the underlying
            // umount even runs.
            req.timeoutInterval = 120

            let sem = DispatchSemaphore(value: 0)
            var ok = false
            var errMsg: String?
            URLSession.shared.dataTask(with: req) { data, _, err in
                defer { sem.signal() }
                if let err = err { errMsg = err.localizedDescription; return }
                guard let data = data,
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                else { errMsg = "Empty or invalid response from /force-eject"; return }
                ok = (obj["ok"] as? Bool) ?? false
                if !ok { errMsg = (obj["error"] as? String) ?? "Unknown error" }
            }.resume()
            sem.wait()

            DispatchQueue.main.async {
                if ok {
                    let done = NSAlert()
                    done.messageText = "Mount Ejected"
                    done.informativeText = "/Volumes/zpool is no longer in the kernel mount table. Finder should now be responsive."
                    done.runModal()
                } else {
                    showAlert(
                        title: "Force Eject Failed",
                        message: (errMsg ?? "Unknown error")
                            + "\n\nA reboot, or `sudo umount -f -t nfs /Volumes/zpool` from a fresh terminal, will clear the wedge."
                    )
                }
            }
        }
    }

    /// Shows an NSSavePanel suggesting a timestamped filename on the
    /// Desktop. On accept, runs DiagnosticsExporter off the main thread
    /// and reports success/failure via NSAlert. The save panel itself
    /// runs modal on the main thread (standard AppKit pattern); only
    /// the gathering work is dispatched away.
    private func exportDiagnostics() {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = DiagnosticsExporter.suggestedFilename()
        panel.title = "Export JuiceMount Diagnostics"
        panel.message = "Save a zipped bundle of logs, metrics, and system state for support."
        // Suggest Desktop. If unavailable (sandbox edge cases) the panel
        // falls back to its default location, which is fine.
        if let desktop = FileManager.default
            .urls(for: .desktopDirectory, in: .userDomainMask)
            .first
        {
            panel.directoryURL = desktop
        }
        panel.allowedContentTypes = [.zip]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let destination = panel.url else {
            return
        }

        // Off-main: shell-outs (pluginkit, fileproviderctl, ditto) plus
        // a couple of HTTP fetches can take several seconds. Don't freeze
        // the popover.
        Task.detached(priority: .userInitiated) {
            let exporter = DiagnosticsExporter()
            do {
                try await exporter.export(to: destination)
                await MainActor.run {
                    let alert = NSAlert()
                    alert.messageText = "Diagnostics exported"
                    alert.informativeText = "Saved to \(destination.path)"
                    alert.alertStyle = .informational
                    alert.addButton(withTitle: "Reveal in Finder")
                    alert.addButton(withTitle: "OK")
                    if alert.runModal() == .alertFirstButtonReturn {
                        NSWorkspace.shared.activateFileViewerSelecting([destination])
                    }
                }
            } catch {
                await MainActor.run {
                    let alert = NSAlert()
                    alert.messageText = "Export failed"
                    alert.informativeText = error.localizedDescription
                    alert.alertStyle = .warning
                    alert.runModal()
                }
            }
        }
    }

    // MARK: - Cache section (offline-pin prototype)

    private var cacheSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Image(systemName: "tray.full")
                    .foregroundStyle(.secondary)
                Text("Cache")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                offlineToggle
            }

            // Pin Folder button — always visible. Pops a native NSOpenPanel
            // rooted in the JuiceMount volume.
            HStack(spacing: 6) {
                Button {
                    pickFolderToPin()
                } label: {
                    Label("Pin Folder for Offline…", systemImage: "pin.circle.fill")
                        .font(.caption)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.small)
                .tint(.orange)
                .help("Pre-cache a folder for offline use. Or right-click a folder in Finder → Services → Pin for Offline (after enabling once in System Settings → Keyboard → Keyboard Shortcuts → Services).")
                Spacer()
            }

            diskSpaceRow

            selfTestRow

            if cacheStatus.aggregate.TotalFiles == 0 && cacheStatus.live.FilesPrefetched == 0 {
                Text("Nothing pinned yet. Pick a folder above, or right-click in Finder → Services → Pin for Offline.")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            } else {
                cacheCounts
                if !cacheStatus.live.CurrentFile.isEmpty {
                    livePrefetchRow
                }
                if !cacheStatus.roots.isEmpty {
                    rootsList
                }
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
    }

    /// Single-line self-test indicator (Phase A2). Shows e.g.
    /// "Self-test: 247 MB/s" with a status-colored dot. The button on the
    /// right re-runs the probe via POST /self-test. Hidden until the first
    /// result lands, so we don't show a stale "—" before the server's
    /// post-start probe completes.
    @ViewBuilder
    private var selfTestRow: some View {
        if let result = server.selfTest {
            HStack(spacing: 6) {
                Circle()
                    .fill(selfTestColor(result.status))
                    .frame(width: 8, height: 8)
                if result.status == "error" {
                    Text("Self-test: error")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                } else if result.mb_per_sec > 0 {
                    Text("Self-test: \(String(format: "%.0f", result.mb_per_sec)) MB/s")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                } else {
                    Text("Self-test: pending")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button {
                    server.refreshSelfTest(force: true, delayMs: 0)
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .font(.caption2)
                }
                .buttonStyle(.plain)
                .help(result.hint.isEmpty
                      ? "Re-run the 10 MB read self-test."
                      : "\(result.hint)\n\nClick to re-run.")
            }
        }
    }

    private func selfTestColor(_ status: String) -> Color {
        switch status {
        case "green":  return .green
        case "yellow": return .yellow
        case "red":    return .red
        case "error":  return .orange
        default:       return .gray
        }
    }

    /// Opens a folder picker rooted at the JuiceMount mount and pins the
    /// chosen directories. Pin work runs on a background queue so the
    /// popover doesn't freeze.
    ///
    /// QA-6 fix: NSStatusItem-based menu-bar apps run as accessory apps
    /// (LSUIElement). When a modal NSOpenPanel is spawned from inside
    /// the popover, macOS doesn't always promote the app to a regular
    /// foreground app — the panel appears but click events go to
    /// whatever full-app currently owns the foreground. Symptom: panel
    /// is visible but clicks into subdirectories register nothing.
    ///
    /// The fix has two parts: (a) explicitly activate the app
    /// (`NSApp.activate(ignoringOtherApps: true)`) before runModal so
    /// the panel becomes the keyWindow with focus, and (b) set
    /// `treatsFilePackagesAsDirectories = true` so .photoslibrary,
    /// .app, .bundle etc. behave like directories (some video assets
    /// arrive as packages from FCP / DaVinci).
    private func pickFolderToPin() {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = true
        panel.directoryURL = URL(fileURLWithPath: server.preferences.mountPoint)
        panel.message = "Select folder(s) to pre-cache for offline use."
        panel.prompt = "Pin"
        // treatsFilePackagesAsDirectories lets us descend into video-
        // production packages (.photoslibrary, .fcpbundle, .drp) which
        // is the intent. Side-effect: .app bundles also become
        // traversable — confusing but harmless; pinning the inside of
        // an .app is a no-op for the user's typical workflow.
        panel.treatsFilePackagesAsDirectories = true
        panel.canCreateDirectories = false  // we're pinning existing folders, not creating new ones
        panel.showsHiddenFiles = false

        // Promote the accessory app to foreground so the modal panel
        // becomes key — without this, panel clicks fall through to
        // whatever Foreground app the user was in. Non-parameterized
        // form is the macOS 14+ recommended API (parameterized form is
        // deprecated as of Sonoma).
        NSApp.activate()

        guard panel.runModal() == .OK else { return }
        let urls = panel.urls
        guard !urls.isEmpty else { return }

        DispatchQueue.global(qos: .userInitiated).async {
            for url in urls {
                do {
                    let result = try NFSBridge.pin(url.path)
                    DispatchQueue.main.async {
                        refreshCacheStatus()
                        if let err = result.error, !err.isEmpty {
                            showAlert(title: "Pin failed",
                                      message: "\(url.lastPathComponent): \(err)")
                        } else {
                            // Brief notification — don't be too noisy
                            NSLog("[JuiceMount] Pinned \(result.files_pinned) files under \(url.lastPathComponent)")
                        }
                    }
                } catch {
                    DispatchQueue.main.async {
                        showAlert(title: "Pin failed",
                                  message: error.localizedDescription)
                    }
                }
            }
        }
    }

    private func showAlert(title: String, message: String) {
        let alert = NSAlert()
        alert.messageText = title
        alert.informativeText = message
        alert.alertStyle = .informational
        alert.runModal()
    }

    @ViewBuilder
    private var cacheClearButton: some View {
        Button {
            triggerClearCache()
        } label: {
            if cacheClearBusy {
                ProgressView().controlSize(.small)
            } else {
                Text("Clear Cache").font(.caption2)
            }
        }
        .controlSize(.mini)
        .disabled(cacheClearBusy)
        .help("Empty the JuiceFS chunk cache. Pinned folders re-download immediately; other content downloads on next access.")
    }

    private var offlineToggle: some View {
        Toggle(isOn: Binding(
            get: { cacheStatus.offline_mode },
            set: { newValue in
                // ServerController.setOffline dispatches both the
                // NFSServerSetOffline cgo call and the follow-up
                // cacheStatus refresh to workQueue. The toggle's
                // visual state updates when cacheStatus republishes
                // on MainActor.
                offlineToggleBusy = true
                server.setOffline(newValue)
                offlineToggleBusy = false
            }
        )) {
            Text("Offline")
                .font(.caption2)
                .foregroundStyle(cacheStatus.offline_mode ? .orange : .secondary)
        }
        .toggleStyle(.switch)
        .controlSize(.mini)
        .help(cacheStatus.offline_mode
            ? "Reads on un-cached files fail fast (good for cellular)"
            : "Reads fall through to backend on cache miss")
    }

    private var cacheCounts: some View {
        VStack(alignment: .leading, spacing: 2) {
            HStack {
                Text("\(cacheStatus.aggregate.TotalFiles) pinned")
                    .font(.caption.monospaced())
                Spacer()
                Text("\(formatBytes(cacheStatus.aggregate.CachedBytes)) / \(formatBytes(cacheStatus.aggregate.TotalBytes))")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
            }
            if cacheStatus.aggregate.TotalBytes > 0 {
                ProgressView(value: Double(cacheStatus.aggregate.CachedBytes),
                             total: Double(cacheStatus.aggregate.TotalBytes))
                    .progressViewStyle(.linear)
                    .controlSize(.mini)
                    .tint(cacheStatus.aggregate.FailedFiles > 0 ? .red : .accentColor)
            }
            HStack(spacing: 8) {
                if cacheStatus.aggregate.ReadyFiles > 0 {
                    Text("✓ \(cacheStatus.aggregate.ReadyFiles)")
                        .font(.caption2.monospaced())
                        .foregroundStyle(.green)
                }
                if cacheStatus.aggregate.PendingFiles > 0 {
                    Text("⋯ \(cacheStatus.aggregate.PendingFiles)")
                        .font(.caption2.monospaced())
                        .foregroundStyle(.blue)
                }
                if cacheStatus.aggregate.FailedFiles > 0 {
                    Text("✗ \(cacheStatus.aggregate.FailedFiles)")
                        .font(.caption2.monospaced())
                        .foregroundStyle(.red)
                }
            }
        }
    }

    private var livePrefetchRow: some View {
        HStack {
            Image(systemName: "arrow.down.circle.fill")
                .foregroundStyle(.blue)
                .frame(width: 12)
            Text(URL(fileURLWithPath: cacheStatus.live.CurrentFile).lastPathComponent)
                .font(.caption2.monospaced())
                .lineLimit(1)
                .truncationMode(.middle)
            Spacer()
            Text("\(cacheStatus.live.FilesPrefetched) done")
                .font(.caption2.monospaced())
                .foregroundStyle(.secondary)
        }
    }

    private var rootsList: some View {
        VStack(alignment: .leading, spacing: 2) {
            ForEach(cacheStatus.roots.prefix(3)) { root in
                HStack {
                    Image(systemName: "pin.fill")
                        .foregroundStyle(.orange)
                        .font(.caption2)
                    Text(URL(fileURLWithPath: root.Root).lastPathComponent)
                        .font(.caption2.monospaced())
                        .lineLimit(1)
                    Spacer()
                    Text("\(root.ReadyFiles)/\(root.TotalFiles)")
                        .font(.caption2.monospaced())
                        .foregroundStyle(root.ReadyFiles == root.TotalFiles ? .green : .secondary)
                    // Un-pin button — calls NFSServerUnpin via cgo,
                    // refreshes cache-status afterward so the row vanishes.
                    // No confirm dialog: un-pin is non-destructive (data
                    // stays in cache until cache eviction or /cache-clear);
                    // adding one would make the action friction-heavy.
                    Button(action: { unpinRoot(root.Root) }) {
                        Image(systemName: "minus.circle")
                            .foregroundStyle(.secondary)
                            .font(.caption2)
                    }
                    // .borderless preserves keyboard focus traversal so
                    // tab-navigators can reach this control (vs .plain
                    // which suppresses the focus ring entirely).
                    .buttonStyle(.borderless)
                    .help("Un-pin \(URL(fileURLWithPath: root.Root).lastPathComponent)")
                }
            }
            if cacheStatus.roots.count > 3 {
                Text("+ \(cacheStatus.roots.count - 3) more pin roots")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
        }
    }

    /// Calls NFSBridge.unpin off the main thread to avoid blocking the
    /// popover during the cgo round-trip, then refreshes cache-status
    /// so the row vanishes from the list. Errors are logged but not
    /// surfaced via dialog — un-pin not-found is non-fatal (the row
    /// was already gone), and other errors are rare enough that the
    /// NSLog trail is sufficient until a notifications system exists.
    private func unpinRoot(_ path: String) {
        DispatchQueue.global(qos: .userInitiated).async {
            do {
                let result = try NFSBridge.unpin(path)
                NSLog("[JuiceMount] Unpinned %d files under %@",
                      result.files_pinned, path)
            } catch {
                NSLog("[JuiceMount] unpin failed for %@: %@",
                      path, String(describing: error))
            }
            DispatchQueue.main.async { refreshCacheStatus() }
        }
    }

    private func formatBytes(_ b: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: b, countStyle: .file)
    }

    // MARK: - Header

    private var header: some View {
        HStack(spacing: 10) {
            statusDot
            VStack(alignment: .leading, spacing: 2) {
                Text("JuiceMount").font(.headline)
                Text(headerSubtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
    }

    /// Header subtitle. When offline-engaged, replaces the generic
    /// state label with a clear "Offline · N pinned · disconnected
    /// M:SS" string. The user-toggle and auto-engage cases use the
    /// same shape but different reason text.
    private var headerSubtitle: String {
        let off = server.offlineState
        if !off.offline {
            return server.state.displayLabel
        }
        let pinned = server.cacheStatus.aggregate.ReadyFiles
        if off.user_offline && !off.auto_offline {
            return "Offline (you toggled it) · \(pinned) pinned available"
        }
        // Auto-engaged: show how long
        let elapsed = formatOfflineElapsed(seconds: off.since_sec)
        if elapsed.isEmpty {
            return "Offline · \(pinned) pinned available"
        }
        return "Offline · \(pinned) pinned · disconnected \(elapsed)"
    }

    /// Formats N seconds as "M:SS" up to an hour, "H:MM:SS" beyond.
    /// Empty when N==0 so callers can omit the suffix.
    private func formatOfflineElapsed(seconds: Int64) -> String {
        if seconds <= 0 { return "" }
        let s = Int(seconds % 60)
        let m = Int((seconds / 60) % 60)
        let h = Int(seconds / 3600)
        if h > 0 {
            return String(format: "%d:%02d:%02d", h, m, s)
        }
        return String(format: "%d:%02d", m, s)
    }

    private var statusDot: some View {
        Circle()
            .fill(statusColor)
            .frame(width: 12, height: 12)
            .overlay(Circle().stroke(.black.opacity(0.15), lineWidth: 0.5))
            .shadow(color: statusColor.opacity(0.5), radius: 4)
    }

    private var statusColor: Color {
        // Offline trumps the state-driven color so the dot in the
        // popover header agrees with the menu-bar icon. Otherwise the
        // user sees a green dot in the popover and a blue dot in the
        // menu bar at the same time, which is confusing.
        if server.offlineState.offline {
            return .blue
        }
        switch server.state {
        case .idle:           return .gray
        case .starting:       return .blue
        case .running:        return .green
        case .syncing:        return .blue
        case .degraded:       return .yellow
        case .disconnected:   return .red
        case .error:          return .red
        }
    }

    // MARK: - Volume section

    private var volumeSection: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Image(systemName: "externaldrive.fill")
                    .foregroundStyle(.secondary)
                    .frame(width: 16)
                Text("Volume")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(server.preferences.mountPoint)
                    .font(.caption.monospaced())
                    .lineLimit(1)
                    .truncationMode(.head)
            }

            HStack {
                Image(systemName: "doc.text")
                    .foregroundStyle(.secondary)
                    .frame(width: 16)
                Text("Entries")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(server.stats.entryCount.formatted(.number))
                    .font(.caption.monospaced())
            }

            if server.stats.lastSyncMs > 0 {
                HStack {
                    Image(systemName: "clock")
                        .foregroundStyle(.secondary)
                        .frame(width: 16)
                    Text("Last sync")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                    Text(formatSyncTime(server.stats.lastSyncTime))
                        .font(.caption.monospaced())
                }
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
    }

    // MARK: - Health section

    private var healthSection: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text("Health")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.bottom, 2)

            healthRow(label: "Redis", healthy: server.stats.healthRedis)
            healthRow(label: "MinIO", healthy: server.stats.healthMinIO)
            healthRow(label: "FUSE",  healthy: server.stats.healthFUSE)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
    }

    private func healthRow(label: String, healthy: Bool) -> some View {
        HStack {
            Image(systemName: healthy ? "checkmark.circle.fill" : "xmark.circle.fill")
                .foregroundStyle(healthy ? .green : .red)
                .frame(width: 16)
            Text(label)
                .font(.caption)
            Spacer()
            Text(healthy ? "OK" : "down")
                .font(.caption.monospaced())
                .foregroundStyle(healthy ? Color.gray : Color.red)
        }
    }

    // MARK: - Actions

    private var actionsSection: some View {
        VStack(spacing: 4) {
            // Primary: start/stop
            primaryActionButton

            // Search (always available, even before start so users can configure first)
            ActionButton(
                title: "Search Files…",
                systemImage: "magnifyingglass",
                shortcut: "⌘⇧F",
                disabled: !isRunningLike,
                action: onSearch
            )

            // Sync now (only when running). Triggers BOTH a metadata
            // reconciliation (Redis → SQLite) AND a pin-coverage verify
            // (re-enqueue every pinned-Ready file; the prefetcher reads
            // each through FUSE, which JuiceFS serves from local cache
            // when present and re-fetches from backend when missing).
            // The latter is the answer to "is my cache actually holding
            // what I pinned?"
            ActionButton(
                title: "Sync Now",
                systemImage: "arrow.triangle.2.circlepath",
                disabled: !isRunningLike,
                action: {
                    server.syncNow()
                    triggerVerifyPins()
                }
            )

            ActionButton(
                title: "Preferences…",
                systemImage: "gear",
                shortcut: "⌘,",
                action: onPreferences
            )

            // Phase B observability: bundle logs, control-plane snapshots,
            // and system state into a zip the user can attach to a bug
            // report. Save dialog runs modal; export runs off-main.
            ActionButton(
                title: "Export Diagnostics…",
                systemImage: "tray.and.arrow.up",
                action: { exportDiagnostics() }
            )

            // In-app rescue when the kernel mount table is wedged.
            // Runs `umount -f -t nfs <path>` via AppleScript-with-admin,
            // which is the only thing that can dislodge an NFS mount whose
            // server has died. Hidden behind a clear confirmation so the
            // user doesn't accidentally trigger it.
            ActionButton(
                title: "Force Eject Mount",
                systemImage: "eject.circle",
                tint: .orange,
                action: { forceEjectMount() }
            )

            ActionButton(
                title: "Quit JuiceMount",
                systemImage: "power",
                shortcut: "⌘Q",
                action: onQuit
            )
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 6)
    }

    @ViewBuilder
    private var primaryActionButton: some View {
        switch server.state {
        case .idle, .error, .disconnected:
            ActionButton(
                title: "Start JuiceMount",
                systemImage: "play.fill",
                tint: .accentColor,
                action: { server.start() }
            )
        case .starting:
            ActionButton(
                title: "Starting…",
                systemImage: "ellipsis.circle",
                disabled: true,
                action: {}
            )
        case .running, .syncing, .degraded:
            ActionButton(
                title: "Stop JuiceMount",
                systemImage: "stop.fill",
                tint: .red,
                action: { server.stop() }
            )
        }
    }

    private var isRunningLike: Bool {
        switch server.state {
        case .running, .syncing, .degraded: return true
        default: return false
        }
    }

    // MARK: - Helpers

    // Hoisted out of formatSyncTime so we don't allocate formatters every render pass.
    private static let isoFormatterFractional: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
    private static let isoFormatterPlain: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f
    }()
    private static let timeFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateStyle = .none
        f.timeStyle = .short
        return f
    }()

    private func formatSyncTime(_ iso: String) -> String {
        guard !iso.isEmpty else { return "—" }
        let date = Self.isoFormatterFractional.date(from: iso)
            ?? Self.isoFormatterPlain.date(from: iso)
        guard let date else { return "—" }

        let interval = Date().timeIntervalSince(date)
        if interval < 60 {
            return "\(Int(interval))s ago"
        } else if interval < 3600 {
            return "\(Int(interval/60))m ago"
        } else {
            return Self.timeFormatter.string(from: date)
        }
    }
}

/// A polished button row — matches macOS popover/menu aesthetics.
struct ActionButton: View {
    let title: String
    let systemImage: String
    var shortcut: String? = nil
    var tint: Color? = nil
    var disabled: Bool = false
    let action: () -> Void

    @State private var isHovering = false

    var body: some View {
        Button(action: action) {
            HStack(spacing: 8) {
                Image(systemName: systemImage)
                    .frame(width: 18)
                    .foregroundStyle(tint ?? .primary)
                Text(title)
                    .foregroundStyle(.primary)
                Spacer()
                if let shortcut {
                    Text(shortcut)
                        .font(.caption.monospaced())
                        .foregroundStyle(.secondary)
                }
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 6)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(
                RoundedRectangle(cornerRadius: 6, style: .continuous)
                    .fill(isHovering && !disabled ? Color.accentColor.opacity(0.15) : .clear)
            )
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .onHover { isHovering = $0 }
        .disabled(disabled)
        .opacity(disabled ? 0.5 : 1.0)
    }
}
