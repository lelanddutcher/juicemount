import SwiftUI

/// The popover shown when the user clicks the menu bar icon.
/// Layout matches the design from the roadmap, with native macOS aesthetics.
struct MenuPopoverView: View {

    @Bindable var server: ServerController
    let onSearch: () -> Void
    let onPreferences: () -> Void
    let onSetupAssistant: () -> Void
    let onQuit: () -> Void

    /// Computed mirror of `server.cacheStatus` — kept under the original
    /// name so the rest of this view's bindings stay untouched. The
    /// underlying cgo call now runs on `ServerController.workQueue`
    /// (see `ServerController.refreshCacheStatus`), never on MainActor.
    private var cacheStatus: NFSBridge.CacheStatus { server.cacheStatus }
    @State private var cacheTimer: Timer?
    /// Local source of truth for the offline switch. Deliberately NOT a
    /// derived binding over cacheStatus.offline_mode — see offlineToggle.
    @State private var offlineSwitch = false
    @State private var diskFreeGB: Double = 0
    @State private var diskImportantGB: Double = 0
    @State private var diskTotalGB: Double = 0
    @State private var reclaimBusy = false
    @State private var cacheClearBusy = false
    // Pin-progress rate readout (QA-1 perception fix). Tracks
    // CachedBytes between cache-status refresh ticks so we can show
    // "downloading at X MB/s" instead of a flat counter that looks
    // stuck during a multi-GB pin. Reset whenever PendingFiles drops
    // to zero so a stale rate doesn't linger.
    @State private var prevCachedBytes: Int64 = 0
    @State private var prevCachedAt: Date = .distantPast
    @State private var pinRateMBps: Double = 0
    @State private var showStopEverythingConfirm = false
    /// True while a /spool-recover action (LB-5) is round-tripping.
    @State private var spoolRecoverInFlight = false
    // Self-test dashboard (B.2). Health is fetched from /health on the
    // same 2s tick as cache-status. Each component is "ok" or a reason
    // string ("ping failed: …"); render as colored dots, full reason
    // visible in tooltip + click-to-copy diagnostic.
    @State private var healthRedis: String = ""
    @State private var healthMinIO: String = ""
    @State private var healthFUSE: String = ""
    @State private var healthNFS: String = ""
    @State private var healthFetchedAt: Date = .distantPast
    // Throughput (B.2): rolling MB/s computed from /metrics bytes_read
    // delta between cache-status polls. Distinct from pinRateMBps,
    // which is pin-specific.
    @State private var prevBytesRead: Int64 = 0
    @State private var prevBytesReadAt: Date = .distantPast
    @State private var readRateMBps: Double = 0
    @State private var lastMetricsFetchedAt: Date = .distantPast

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()
            volumeSection
            Divider()
            healthSection
            Divider()
            cacheSection
            if let sp = server.spoolStatus, sp.enabled {
                Divider()
                pendingUploadsSection
            }
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
        updatePinRate()
        refreshSelfTest()
    }

    /// B.2: pulls /health + /metrics on the same 2s cadence the
    /// popover already uses for cache-status. Fire-and-forget; the
    /// @State updates trigger view re-render via SwiftUI's normal
    /// path. URLSession runs on its own queue so we don't block the
    /// main thread or the cgo workQueue. Failures leave the previous
    /// values intact rather than wiping to empty — same pattern the
    /// juicemount-watch observer uses.
    private func refreshSelfTest() {
        DispatchQueue.global(qos: .utility).async {
            // /health — 4 components
            if let url = URL(string: "http://127.0.0.1:11050/health"),
               let data = try? Data(contentsOf: url),
               let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let comps = obj["components"] as? [String: String] {
                let r = comps["redis"] ?? ""
                let m = comps["minio"] ?? ""
                let f = comps["fuse"] ?? ""
                let n = comps["nfs"] ?? ""
                DispatchQueue.main.async {
                    healthRedis = r
                    healthMinIO = m
                    healthFUSE = f
                    healthNFS = n
                    healthFetchedAt = Date()
                }
            }
            // /metrics — bytes_read delta → MB/s
            if let url = URL(string: "http://127.0.0.1:11050/metrics"),
               let data = try? Data(contentsOf: url),
               let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
               let br = (obj["bytes_read"] as? Int64) ?? (obj["bytes_read"] as? Double).map({ Int64($0) }) {
                let now = Date()
                DispatchQueue.main.async {
                    if prevBytesReadAt != .distantPast {
                        let elapsed = now.timeIntervalSince(prevBytesReadAt)
                        if elapsed > 0.1 {
                            let delta = max(0, br - prevBytesRead)
                            readRateMBps = Double(delta) / elapsed / 1_048_576
                        }
                    }
                    prevBytesRead = br
                    prevBytesReadAt = now
                    lastMetricsFetchedAt = now
                }
            }
        }
    }

    /// Maps a component's health-string ("ok" or a reason) to a dot
    /// color. Anything starting with "ok" is green; explicit known
    /// errors get red; anything else (e.g. "starting", "unknown") is
    /// yellow.
    private func healthDotColor(_ s: String) -> Color {
        let trimmed = s.trimmingCharacters(in: .whitespaces).lowercased()
        if trimmed.isEmpty { return .gray }
        if trimmed.hasPrefix("ok") { return .green }
        if trimmed.contains("failed") || trimmed.contains("error")
            || trimmed.contains("no route") || trimmed.contains("refused")
            || trimmed.contains("not mounted") {
            return .red
        }
        return .yellow
    }

    /// Builds a single plain-text diagnostic snippet for the click-to-
    /// copy action. One line per component plus the read rate so a
    /// user filing an issue can paste it without screenshotting.
    private func selfTestDiagnostic() -> String {
        let ts = ISO8601DateFormatter().string(from: healthFetchedAt)
        return """
        JuiceMount self-test @ \(ts)
          redis : \(healthRedis.isEmpty ? "(unknown)" : healthRedis)
          minio : \(healthMinIO.isEmpty ? "(unknown)" : healthMinIO)
          fuse  : \(healthFUSE.isEmpty ? "(unknown)" : healthFUSE)
          nfs   : \(healthNFS.isEmpty ? "(unknown)" : healthNFS)
          read  : \(String(format: "%.1f", readRateMBps)) MB/s (rolling)
        """
    }

    /// Computes a rolling MB/s rate from successive CachedBytes
    /// samples. Drops the rate to 0 when nothing is pending (no work
    /// in flight) so we don't display a misleading "downloading"
    /// number after a pin completes.
    ///
    /// Sampling happens at the cache-status refresh cadence (2s) —
    /// faster polling would just produce noisier numbers. The user-
    /// facing problem (popover looks stuck at 0 KB during a pin)
    /// is solved by SHOWING the rate, not by sampling more often.
    private func updatePinRate() {
        let now = Date()
        let cur = cacheStatus.aggregate.CachedBytes
        let pending = cacheStatus.aggregate.PendingFiles

        // If nothing pending and nothing changing, surface a clean
        // zero so the row hides the rate readout.
        if pending == 0 {
            pinRateMBps = 0
            prevCachedBytes = cur
            prevCachedAt = now
            return
        }

        // First sample after a pin starts: seed and wait for the
        // next tick before computing a delta.
        if prevCachedAt == .distantPast || cur < prevCachedBytes {
            prevCachedBytes = cur
            prevCachedAt = now
            return
        }

        let elapsed = now.timeIntervalSince(prevCachedAt)
        guard elapsed > 0.1 else { return }  // avoid divide-by-tiny

        let deltaBytes = max(0, cur - prevCachedBytes)
        let rateBps = Double(deltaBytes) / elapsed
        pinRateMBps = rateBps / 1_048_576  // 1 MiB

        prevCachedBytes = cur
        prevCachedAt = now
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
            var snapshots: Int = 0
            var source: String = "Time Machine local snapshots"
            var errMsg: String?
            URLSession.shared.dataTask(with: req) { data, _, err in
                defer { sem.signal() }
                if let err = err { errMsg = err.localizedDescription; return }
                guard let data = data,
                      let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
                else { return }
                if let f = obj["freed_gb"] as? Double { freedGB = f }
                if let n = obj["snapshots_thinned"] as? Int { snapshots = n }
                if let s = obj["source"] as? String, !s.isEmpty { source = s }
                if let e = obj["error"] as? String { errMsg = e }
            }.resume()
            sem.wait()
            DispatchQueue.main.async {
                reclaimBusy = false
                refreshDiskSpace()
                if let errMsg = errMsg {
                    presentRemediation(.reclaimFailed, rawError: errMsg)
                } else if freedGB < 0.1 {
                    // Informational, not an error — keep the lighter
                    // showAlert for this case so we don't surface a
                    // "Copy diagnostic" button on a no-op outcome.
                    showAlert(title: "Nothing to reclaim",
                              message: "Reclaim only thins \(source). macOS may report purgeable space elsewhere (iCloud Drive, system caches) that's managed automatically under disk pressure — those aren't safe to clean from here.")
                } else {
                    // Report WHAT was reclaimed, not just how much. The user
                    // should know we touched Time Machine snapshots and
                    // nothing else (no app caches, no system files).
                    let detail = snapshots > 0
                        ? "Thinned \(snapshots) \(source.lowercased()), freed \(String(format: "%.1f", freedGB)) GB."
                        : "Freed \(String(format: "%.1f", freedGB)) GB from \(source.lowercased())."
                    NSLog("[JuiceMount] Reclaim: %@", detail)
                    showAlert(title: "Reclaimed \(String(format: "%.1f", freedGB)) GB",
                              message: detail + "\n\nReclaim only touches Time Machine snapshots; app caches and other purgeable space are managed by macOS automatically.")
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
                    presentRemediation(.clearCacheFailed, rawError: errMsg)
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

            // B.2: 4-dot health glance + rolling read throughput.
            // Click anywhere on the row → copies a plain-text
            // diagnostic snippet to the clipboard. Each dot's color
            // is driven by /health.components; tooltip carries the
            // full reason string when degraded.
            healthDotsRow

            // Always show cache counts. With 0 pins the JuiceFS chunk
            // cache still holds every-ever-read chunk and offline mode
            // is still useful for working with whatever's been touched
            // recently — hiding the cache UI hid that fact. (User QA
            // 2026-05-17.)
            cacheCounts
            if !cacheStatus.live.CurrentFile.isEmpty {
                livePrefetchRow
            }
            if !cacheStatus.roots.isEmpty {
                rootsList
            }
            if cacheStatus.aggregate.TotalFiles == 0 && cacheStatus.live.FilesPrefetched == 0 {
                Text("Tip: pin a folder above (or in Finder → Services → Pin for Offline) to guarantee it stays cached. Even without pins, the chunk cache holds recently-read files for offline use.")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
    }

    /// "Pending uploads" surface for the write spool (Option 2). The body
    /// gates this on `server.spoolStatus?.enabled`, so it only appears when
    /// the spool is on. Shows queue depth + bytes, in-flight count, the first
    /// few in-flight files, spool disk fill, and cumulative drained / failed /
    /// quarantined counters. Reads `server.spoolStatus`, refreshed on the same
    /// cadence as cache status by `ServerController.refreshCacheStatus()`.
    @ViewBuilder
    private var pendingUploadsSection: some View {
        if let s = server.spoolStatus, s.enabled {
            VStack(alignment: .leading, spacing: 6) {
                HStack {
                    Image(systemName: "arrow.up.circle")
                        .foregroundStyle(.secondary)
                    Text("Pending Uploads")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                    if !s.hasActivity && !s.needsAttention {
                        Text("idle")
                            .font(.caption2)
                            .foregroundStyle(.tertiary)
                    }
                }

                // LB-5: failed entries are not "pending", so this section must
                // also render when there's nothing uploading but something
                // needs the user (stalled/failed rows + the recover actions).
                if s.hasActivity || s.needsAttention {
                    HStack {
                        Text("\(s.pendingFiles) waiting · \(formatBytes(s.pendingBytes))\(s.oldestPendingAgeSec >= 120 ? " · oldest \(formatAge(s.oldestPendingAgeSec))" : "")")
                            .font(.caption)
                        Spacer()
                        if s.inProgress > 0 {
                            HStack(spacing: 3) {
                                Image(systemName: "arrow.up.circle.fill")
                                Text("\(s.inProgress) uploading")
                            }
                            .font(.caption2)
                            .foregroundStyle(.blue)
                        }
                    }

                    // Stuck-spool signals (LB-5): surfaced ABOVE the entry
                    // list so "43 waiting · 0 KB" can never again be the
                    // whole story.
                    if s.needsAttention {
                        HStack(spacing: 10) {
                            if s.stalledFiles > 0 {
                                Label("\(s.stalledFiles) stalled", systemImage: "exclamationmark.circle.fill")
                                    .foregroundStyle(.orange)
                            }
                            if s.failedFiles > 0 {
                                Label("\(s.failedFiles) failed", systemImage: "exclamationmark.triangle.fill")
                                    .foregroundStyle(.red)
                            }
                        }
                        .font(.caption2)
                    }

                    // First few entries (server returns active rows newest-
                    // first, then a short recently-done tail).
                    ForEach(s.entries.prefix(4)) { e in
                        VStack(alignment: .leading, spacing: 1) {
                            HStack(spacing: 6) {
                                Image(systemName: entryIcon(e))
                                    .foregroundStyle(entryColor(e))
                                    .font(.caption2)
                                Text(URL(fileURLWithPath: e.path).lastPathComponent)
                                    .font(.caption2)
                                    .lineLimit(1)
                                    .truncationMode(.middle)
                                Spacer()
                                Text(formatBytes(e.size))
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                            // Status detail line for problem entries only:
                            // "writing · 2h — stalled" / "failed · 5m".
                            if e.stalled || e.drainState == "failed" {
                                Text(entryStatusLine(e))
                                    .font(.caption2)
                                    .foregroundStyle(e.drainState == "failed" ? .red : .orange)
                                    .lineLimit(1)
                                    .truncationMode(.tail)
                                    .padding(.leading, 18)
                            }
                        }
                        .help(entryTooltip(e))
                    }
                    if s.entries.count > 4 {
                        Text("+ \(s.entries.count - 4) more")
                            .font(.caption2)
                            .foregroundStyle(.tertiary)
                    }

                    // Last error, truncated; hover for the full text. The
                    // decoded last_error was previously never rendered (LB-5).
                    if let err = firstSpoolError(s) {
                        Text(err)
                            .font(.caption2)
                            .foregroundStyle(.red)
                            .lineLimit(1)
                            .truncationMode(.tail)
                            .help(err)
                    }

                    // Recovery actions → /spool-recover (LB-5).
                    if s.needsAttention {
                        HStack(spacing: 8) {
                            if s.failedFiles > 0 {
                                Button("Retry failed") { runSpoolRecover("retry-failed") }
                                    .help("Re-queue failed uploads whose data is still on this Mac's spool.")
                            }
                            if s.stalledFiles > 0 {
                                Button("Recover stalled") { runSpoolRecover("clear-stalled") }
                                    .help("Finalize stuck entries and hand their bytes to the uploader. No data is deleted.")
                            }
                            if spoolRecoverInFlight {
                                ProgressView().controlSize(.small)
                            }
                        }
                        .buttonStyle(.bordered)
                        .controlSize(.small)
                        .disabled(spoolRecoverInFlight)
                    }
                } else {
                    Text("All uploads drained — writes are caught up with your storage.")
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }

                // Spool disk fill.
                if s.capacityTotal > 0 {
                    ProgressView(value: Double(min(s.capacityUsed, s.capacityTotal)),
                                 total: Double(s.capacityTotal))
                        .progressViewStyle(.linear)
                    Text("Spool \(formatBytes(s.capacityUsed)) / \(formatBytes(s.capacityTotal))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }

                // Cumulative counters (only the non-zero ones).
                if s.succeeded > 0 || s.failed > 0 || s.quarantined > 0 {
                    HStack(spacing: 10) {
                        if s.succeeded > 0 {
                            Text("✓ \(s.succeeded) uploaded").foregroundStyle(.green)
                        }
                        if s.failed > 0 {
                            Text("✗ \(s.failed) failed").foregroundStyle(.red)
                        }
                        if s.quarantined > 0 {
                            Text("⚠ \(s.quarantined) quarantined").foregroundStyle(.orange)
                        }
                    }
                    .font(.caption2)
                }
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
        }
    }

    private func drainStateIcon(_ state: String) -> String {
        switch state {
        case "writing":  return "pencil.circle"
        case "ready":    return "clock"
        case "draining": return "arrow.up.circle.fill"
        case "failed":   return "exclamationmark.triangle.fill"
        case "done":     return "checkmark.circle"
        default:         return "circle"
        }
    }

    private func drainStateColor(_ state: String) -> Color {
        switch state {
        case "draining": return .blue
        case "failed":   return .red
        case "done":     return .green
        default:         return .gray
        }
    }

    // MARK: - Spool entry presentation (LB-5)

    /// Entry-aware icon: stalled overrides the per-state icon so a stuck
    /// `writing` row stops masquerading as routine activity.
    private func entryIcon(_ e: NFSBridge.SpoolEntryView) -> String {
        e.stalled ? "exclamationmark.circle.fill" : drainStateIcon(e.drainState)
    }

    private func entryColor(_ e: NFSBridge.SpoolEntryView) -> Color {
        e.stalled ? .orange : drainStateColor(e.drainState)
    }

    /// "writing · 2h — stalled" / "failed · 5m" status detail.
    private func entryStatusLine(_ e: NFSBridge.SpoolEntryView) -> String {
        var line = "\(e.drainState) · \(formatAge(e.ageSec))"
        if e.stalled { line += " — stalled" }
        if e.drainState == "failed", let err = e.lastError, !err.isEmpty {
            line += " — \(err)"
        }
        return line
    }

    /// Full detail for the hover tooltip (the row truncates).
    private func entryTooltip(_ e: NFSBridge.SpoolEntryView) -> String {
        var parts = ["\(e.path)", "state: \(e.drainState)\(e.stalled ? " (stalled)" : "")", "age: \(formatAge(e.ageSec))"]
        if e.drainAttempts > 0 { parts.append("attempts: \(e.drainAttempts)") }
        if let err = e.lastError, !err.isEmpty { parts.append("last error: \(err)") }
        return parts.joined(separator: "\n")
    }

    /// First error worth surfacing under the entry list: a problem entry's
    /// last_error, else the endpoint-level error string.
    private func firstSpoolError(_ s: NFSBridge.SpoolStatus) -> String? {
        if let e = s.entries.first(where: { ($0.stalled || $0.drainState == "failed") && !($0.lastError ?? "").isEmpty }) {
            return e.lastError
        }
        if let err = s.error, !err.isEmpty { return err }
        return nil
    }

    /// Compact age: 45s / 12m / 2h / 3d.
    private func formatAge(_ sec: Int64) -> String {
        switch sec {
        case ..<60: return "\(max(sec, 0))s"
        case ..<3600: return "\(sec / 60)m"
        case ..<86400: return "\(sec / 3600)h"
        default: return "\(sec / 86400)d"
        }
    }

    /// Fire a /spool-recover action off the main thread, then refresh the
    /// spool status so the section reflects the result promptly.
    private func runSpoolRecover(_ action: String) {
        spoolRecoverInFlight = true
        Task {
            let addr = server.preferences.metricsAddr
            let result = await Task.detached { NFSBridge.spoolRecover(action: action, metricsAddr: addr) }.value
            await MainActor.run {
                spoolRecoverInFlight = false
                if let result, !result.ok, let err = result.error {
                    NFSBridge.appLog("spool-recover \(action) failed: \(err)")
                }
                server.refreshCacheStatus()
            }
        }
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
                    // B.6: show first-byte RTT alongside MB/s when
                    // available. RTT is a distinct signal (round-trip
                    // latency) from throughput (sustained transfer).
                    if result.first_byte_ms > 0 {
                        Text("Self-test: \(String(format: "%.0f", result.mb_per_sec)) MB/s · \(result.first_byte_ms)ms RTT")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("Self-test: \(String(format: "%.0f", result.mb_per_sec)) MB/s")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
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
                            presentRemediation(.pinFailed,
                                               rawError: err,
                                               extraContext: "folder: \(url.lastPathComponent)")
                        } else {
                            // Brief notification — don't be too noisy
                            NSLog("[JuiceMount] Pinned \(result.files_pinned) files under \(url.lastPathComponent)")
                        }
                    }
                } catch {
                    DispatchQueue.main.async {
                        presentRemediation(.pinFailed,
                                           rawError: error.localizedDescription)
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

    /// B.2: 4-dot health row + throughput readout. Hidden until the
    /// first /health response lands so the user doesn't see stale
    /// gray dots at app launch. Click anywhere on the row to copy
    /// a plain-text diagnostic snippet to the clipboard — useful
    /// for bug reports.
    @ViewBuilder
    private var healthDotsRow: some View {
        if healthFetchedAt != .distantPast {
            HStack(spacing: 8) {
                healthDot(label: "R", reason: healthRedis, full: "Redis")
                healthDot(label: "M", reason: healthMinIO, full: "MinIO")
                healthDot(label: "F", reason: healthFUSE, full: "FUSE")
                healthDot(label: "N", reason: healthNFS, full: "NFS")
                Spacer()
                if readRateMBps >= 0.1 {
                    Text(String(format: "%.0f MB/s", readRateMBps))
                        .font(.caption2.monospaced())
                        .foregroundStyle(.secondary)
                }
            }
            .contentShape(Rectangle())
            .onTapGesture {
                let pasteboard = NSPasteboard.general
                pasteboard.clearContents()
                pasteboard.setString(selfTestDiagnostic(), forType: .string)
            }
            .help("Click to copy diagnostic snippet")
        }
    }

    /// Single labeled health dot. Label is a 1-char glyph for compact
    /// row layout; `full` is the component name surfaced in the
    /// hover tooltip alongside the reason string.
    @ViewBuilder
    private func healthDot(label: String, reason: String, full: String) -> some View {
        HStack(spacing: 2) {
            Circle()
                .fill(healthDotColor(reason))
                .frame(width: 8, height: 8)
            Text(label)
                .font(.caption2.monospaced())
                .foregroundStyle(.secondary)
        }
        .help("\(full): \(reason.isEmpty ? "(unknown)" : reason)")
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
        // The switch binds to a LOCAL @State (offlineSwitch), NOT to a derived
        // Binding(get:set:) over cacheStatus.offline_mode. The derived binding
        // proved unfixable: under the poll loop's constant re-render churn,
        // SwiftUI kept re-reading a stale `get` immediately after a tap, snapped
        // the switch back, and re-fired `set` — the [swift] log showed it firing
        // `setOffline(true)` ×21 and `setOffline(false)` ×0 across a test
        // session (write-only-ON: offline could never be cleared from the UI,
        // even with an optimistic synchronous update in setOffline).
        //
        // Binding to a local Bool makes each tap flip the switch
        // DETERMINISTICALLY in both directions. `onChange(offlineSwitch)` writes
        // the user's intent to the backend; the second `onChange` mirrors
        // backend-initiated changes (auto-offline, an external curl) back into
        // the switch. The `!= offline_mode` guards stop the two onChange
        // handlers from echoing each other into a loop.
        Toggle(isOn: $offlineSwitch) {
            Text("Offline")
                .font(.caption2)
                .foregroundStyle(offlineSwitch ? .orange : .secondary)
        }
        .toggleStyle(.switch)
        .controlSize(.mini)
        .onChange(of: offlineSwitch) { _, newValue in
            // Only write when the user actually diverged from the backend.
            // (When we sync the switch FROM the backend below, offline_mode
            //  already equals newValue, so this no-ops — no echo write.)
            guard newValue != server.cacheStatus.offline_mode else { return }
            server.setOffline(newValue)
        }
        .onChange(of: server.cacheStatus.offline_mode) { _, backend in
            // Reflect backend-driven changes without triggering a write.
            if offlineSwitch != backend { offlineSwitch = backend }
        }
        .onAppear { offlineSwitch = server.cacheStatus.offline_mode }
        .help(offlineSwitch
            ? "Offline: reads are served from local cache; un-cached requests fail fast instead of stalling the network."
            : "Online: cache misses transparently fall through to the backend. Toggle to use only what's already cached.")
    }

    private var cacheCounts: some View {
        VStack(alignment: .leading, spacing: 2) {
            HStack {
                Text("\(cacheStatus.aggregate.TotalFiles) pinned")
                    .font(.caption.monospaced())
                Spacer()
                // Show the live download rate next to the byte counter
                // while a pin is draining. Only when PendingFiles > 0
                // AND we've measured a non-trivial rate (>= 0.5 MB/s)
                // — avoids flicker on tiny tail-end transfers.
                if cacheStatus.aggregate.PendingFiles > 0 && pinRateMBps >= 0.5 {
                    Text(String(format: "%.0f MB/s", pinRateMBps))
                        .font(.caption2.monospaced())
                        .foregroundStyle(.blue)
                }
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

    // MARK: - Header (at-a-glance, Phase 3)
    //
    // Three glanceable elements — what the user ultimately needs to know:
    //   (a) mount health: color dot + word from the SAME 4-state mapping
    //       the menu-bar icon uses (ServerController.glanceState);
    //   (b) cache: used vs free with a thin proportional bar;
    //   (c) uploads: "N uploading · M queued", hidden when idle.
    // Detail sections stay below.

    /// Exact palette from the approved icon/state spec.
    static let glanceAmber = Color(red: 0xEF / 255.0, green: 0x9F / 255.0, blue: 0x27 / 255.0)
    static let glanceBlue  = Color(red: 0x37 / 255.0, green: 0x8A / 255.0, blue: 0xDD / 255.0)
    static let glanceRed   = Color(red: 0xE2 / 255.0, green: 0x4B / 255.0, blue: 0x4A / 255.0)

    private var header: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 10) {
                statusDot
                VStack(alignment: .leading, spacing: 2) {
                    Text("JuiceMount").font(.headline)
                    Text(headerSubtitle)
                        .font(.caption)
                        .foregroundStyle(statusColor)
                }
                Spacer()
            }
            cacheGlanceRow
            uploadsGlanceRow
            mountRemedyRow
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
    }

    /// True for the states where a missing volume is a REMEDY situation
    /// (server alive, Finder has nothing). Excludes .starting — the mount is
    /// legitimately in flight then — and all stopped/fault states, where
    /// Start (not Mount Now) is the correct action.
    private var isRunningLikeForMountRemedy: Bool {
        switch server.state {
        case .running, .syncing, .degraded: return true
        default: return false
        }
    }

    /// LB-2 remedy row (review P1-A: the plumbing existed with no UI). Shown
    /// only when the server runs but /Volumes/<name> is gone — the exact
    /// state where every other indicator used to read "Connected".
    @ViewBuilder
    private var mountRemedyRow: some View {
        if !server.volumeMounted, isRunningLikeForMountRemedy {
            HStack(spacing: 6) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .font(.caption)
                    .foregroundStyle(Self.glanceAmber)
                Text("Volume not mounted — Finder can't see your files")
                    .font(.caption)
                    .foregroundStyle(Self.glanceAmber)
                Spacer()
                Button {
                    server.mountNow()
                } label: {
                    if server.mountNowInFlight {
                        ProgressView().controlSize(.small)
                    } else {
                        Text("Mount Now").font(.caption)
                    }
                }
                .disabled(server.mountNowInFlight)
                .help("Re-mounts the volume via mount_nfs. May ask for your password once unless the scoped sudoers rule is installed.")
            }
        }
    }

    /// (b) "X cached · Y GB free" + a thin proportional bar of cached vs
    /// free disk. Cached = pinned bytes actually resident (the number the
    /// cache section details below); free = statfs free on the cache disk.
    private var cacheGlanceRow: some View {
        let cachedBytes = max(0, cacheStatus.aggregate.CachedBytes)
        let freeBytes = Int64(max(0, diskFreeGB) * 1e9)
        let total = Double(cachedBytes + freeBytes)
        let fraction = total > 0 ? Double(cachedBytes) / total : 0
        return VStack(alignment: .leading, spacing: 3) {
            HStack(spacing: 4) {
                Image(systemName: "internaldrive")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                Text("\(formatBytes(cachedBytes)) cached · \(String(format: "%.0f", diskFreeGB)) GB free")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
            }
            GeometryReader { geo in
                ZStack(alignment: .leading) {
                    Capsule().fill(Color.secondary.opacity(0.18))
                    Capsule()
                        .fill(Color.accentColor.opacity(0.85))
                        .frame(width: max(0, min(1, fraction)) * geo.size.width)
                }
            }
            .frame(height: 3)
        }
    }

    /// (c) Upload glance — "N uploading · M queued" plus compact failed/
    /// stalled chips. Hidden entirely when the spool is off or idle with
    /// nothing needing attention. `pending_files` counts writing+ready+
    /// draining rows, so queued = pending − in-flight.
    @ViewBuilder
    private var uploadsGlanceRow: some View {
        if let sp = server.spoolStatus, sp.enabled, sp.hasActivity || sp.needsAttention {
            HStack(spacing: 6) {
                Image(systemName: "arrow.up.circle.fill")
                    .font(.caption)
                    .foregroundStyle(Self.glanceBlue)
                Text("\(sp.inProgress) uploading · \(max(0, sp.pendingFiles - Int(sp.inProgress))) queued")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if sp.failedFiles > 0 {
                    Text("\(sp.failedFiles) failed")
                        .font(.caption2)
                        .foregroundStyle(Self.glanceRed)
                }
                if sp.stalledFiles > 0 {
                    Text("\(sp.stalledFiles) stalled")
                        .font(.caption2)
                        .foregroundStyle(Self.glanceAmber)
                }
                Spacer()
            }
        }
    }

    /// Header subtitle — the glance "word" from the shared 4-state mapping.
    /// Offline-files mode keeps the rich "Offline · N pinned · disconnected
    /// M:SS" string; the other states show the mapped word plus the reason
    /// when there is one.
    private var headerSubtitle: String {
        switch server.glanceState {
        case .offlineFiles:
            let off = server.offlineState
            let pinned = server.cacheStatus.aggregate.ReadyFiles
            if off.user_offline && !off.auto_offline {
                return "Offline (you toggled it) · \(pinned) pinned available"
            }
            let elapsed = formatOfflineElapsed(seconds: off.since_sec)
            if elapsed.isEmpty {
                return "Offline · \(pinned) pinned available"
            }
            return "Offline · \(pinned) pinned · disconnected \(elapsed)"
        case .healthy:
            return "Healthy"
        case .degraded:
            // Running-but-unmounted is its own honest message (review B-gap:
            // this used to fall through to .running's "Connected" — amber dot
            // with a green word). The Mount Now remedy row sits right below.
            if !server.volumeMounted, isRunningLikeForMountRemedy {
                return "Volume not mounted"
            }
            // Keep the reason visible ("Degraded — Redis unreachable …",
            // "Starting…").
            return server.state.displayLabel
        case .fault:
            return server.state.displayLabel
        case .idle:
            return "Not started"
        }
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
        // Same 4-state mapping the menu-bar icon uses (and the exact spec
        // palette) so the popover dot/word and the icon can never disagree.
        switch server.glanceState {
        case .healthy:      return .green
        case .degraded:     return Self.glanceAmber
        case .offlineFiles: return Self.glanceBlue
        case .fault:        return Self.glanceRed
        case .idle:         return .gray
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
    //
    // Phase 3: the per-component rows fold into a "Details" disclosure —
    // the at-a-glance header already answers "is it healthy"; this is for
    // the diagnosis case. Auto-expanded when something is actually down so
    // the cause is one glance away.

    @State private var healthDetailsExpanded = false

    private var healthSection: some View {
        DisclosureGroup(isExpanded: $healthDetailsExpanded) {
            VStack(alignment: .leading, spacing: 4) {
                healthRow(label: "Redis", healthy: server.stats.healthRedis)
                healthRow(label: "MinIO", healthy: server.stats.healthMinIO)
                healthRow(label: "FUSE",  healthy: server.stats.healthFUSE)
            }
            .padding(.top, 4)
        } label: {
            HStack(spacing: 6) {
                Text("Health details")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                if !allComponentsHealthy {
                    Image(systemName: "exclamationmark.triangle.fill")
                        .font(.caption2)
                        .foregroundStyle(Self.glanceAmber)
                }
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .onChange(of: allComponentsHealthy) { _, healthy in
            if !healthy { healthDetailsExpanded = true }
        }
    }

    private var allComponentsHealthy: Bool {
        server.stats.healthRedis && server.stats.healthMinIO && server.stats.healthFUSE
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

            // Open the mount in Finder. Most-common need is "drag a file in"
            // and the user otherwise has to navigate Finder → Go → Connect or
            // type the path. Disabled when the mount isn't ready.
            ActionButton(
                title: "Open Mount in Finder",
                systemImage: "folder",
                disabled: !isRunningLike,
                action: {
                    let url = URL(fileURLWithPath: server.preferences.mountPoint)
                    NSWorkspace.shared.open(url)
                }
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

            // LB-1: re-open the welcome/preflight checklist any time —
            // the same window first-run shows. Useful after installing
            // juicefs/macFUSE or fixing the backend.
            ActionButton(
                title: "Setup Assistant…",
                systemImage: "checklist",
                action: onSetupAssistant
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
        case .idle:
            ActionButton(
                title: "Start JuiceMount",
                systemImage: "play.fill",
                tint: .accentColor,
                action: { server.start() }
            )
        case .error, .disconnected:
            // QA-11 fix (Loop C.3, 2026-05-17): in .error/.disconnected,
            // ServerController.start() silently returns (guard case .idle).
            // Previously the popover offered a Start button anyway → user
            // clicked, nothing happened, no visible recovery path. Now we
            // disable Start, surface the reason, and offer Stop everything
            // as the explicit recovery action below.
            ActionButton(
                title: "Start JuiceMount",
                systemImage: "play.fill",
                tint: .accentColor,
                disabled: true,
                action: {}
            )
            Text("Server is \(server.state == .disconnected ? "disconnected" : "in error") — use \"Stop everything\" to fully reset, then Start.")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.horizontal, 8)
                .padding(.bottom, 4)
            ActionButton(
                title: "Stop everything",
                systemImage: "stop.fill",
                tint: .red,
                action: { showStopEverythingConfirm = true }
            )
            .confirmationDialog(
                "Stop everything?",
                isPresented: $showStopEverythingConfirm,
                titleVisibility: .visible
            ) {
                Button("Stop everything", role: .destructive) {
                    server.stop()
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("Tears down FUSE and any partial state so the next Start can begin from a clean slate." + stopEverythingSpoolWarning)
            }
        case .starting:
            ActionButton(
                title: "Starting…",
                systemImage: "ellipsis.circle",
                disabled: true,
                action: {}
            )
        case .running, .syncing, .degraded:
            // Two-tier Stop (QA-7, 2026-05-17):
            //   - "Stop mount and finish sync" → ServerController.stopMount()
            //     Unmounts /Volumes/<name> + drains in-flight sync, but
            //     keeps FUSE + JuiceFS alive so next Start is fast.
            //   - "Stop everything" → ServerController.stop()
            //     Full teardown — also unmounts FUSE + kills JuiceFS
            //     daemons. Next Start re-mounts (admin password if
            //     passwordless-sudo not configured).
            ActionButton(
                title: "Stop mount and finish sync",
                systemImage: "pause.fill",
                tint: .orange,
                action: { server.stopMount() }
            )
            ActionButton(
                title: "Stop everything",
                systemImage: "stop.fill",
                tint: .red,
                action: { showStopEverythingConfirm = true }
            )
            .confirmationDialog(
                "Stop everything?",
                isPresented: $showStopEverythingConfirm,
                titleVisibility: .visible
            ) {
                Button("Stop everything", role: .destructive) {
                    server.stop()
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("Unmounts the volume and kills the JuiceFS daemon. Next Start may prompt for your password to re-mount. Use \"Stop mount and finish sync\" if you'll restart soon." + stopEverythingSpoolWarning)
            }
        }
    }

    /// LB-3 consistency: the quit guard and the spool-disable guard both
    /// surface pending-upload numbers, so "Stop everything" shows the SAME
    /// numbers in its confirmation. (Stopping does not strand entries —
    /// the spool stays enabled and boot recovery requeues them on the next
    /// Start — but the user should know uploads are pausing.)
    private var stopEverythingSpoolWarning: String {
        guard let s = server.spoolStatus, s.enabled, s.hasActivity else { return "" }
        return "\n\n\(s.pendingFiles) file\(s.pendingFiles == 1 ? "" : "s") (\(formatBytes(s.pendingBytes))) are still uploading to the NAS — they stay queued on this Mac and resume on the next Start (keep the write spool enabled)."
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
