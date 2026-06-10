import SwiftUI
import UserNotifications

/// Preferences window (Phase 3b redesign).
///
/// Layout principles:
///   - One grouped Form per tab; Sections carry clear headers and a footer
///     that states exactly WHEN each group takes effect (immediate vs next
///     start vs full Stop → Start) — accurate to the actual plumbing:
///     `Preferences.toServerConfig()` is read once in
///     `ServerController.start()`, and Restart (soft-stop) keeps the
///     JuiceFS daemon + Finder mount alive, so daemon-level settings only
///     apply after a full Stop everything → Start.
///   - Fixed 600 pt width; each tab declares its own content height so the
///     window hugs the form (no scroll-within-scroll, no dead space). The
///     hosting controller tracks the SwiftUI ideal size
///     (see MenuBarController.openPreferencesWindow).
///   - Numeric fields clamp to sane ranges on commit; URL/address fields
///     strip ALL whitespace on change (un-trimmed URLs once cost a day —
///     `juicefs mount: exit status 1` six layers down).
struct PreferencesWindowView: View {
    @Bindable var preferences: Preferences
    @Bindable var server: ServerController

    @State private var selectedTab: Tab = .general
    @State private var advancedAddressesExpanded = false
    /// Last non-empty sanitized volume name — the comparison anchor for the
    /// name→mount-point derivation (see deriveMountPoint).
    @State private var derivationAnchor = ""

    enum Tab: String, CaseIterable, Identifiable {
        case general
        case connection
        case cacheStorage
        case maintenance
        var id: Self { self }
    }

    var body: some View {
        TabView(selection: $selectedTab) {
            generalTab
                .tabItem { Label("General", systemImage: "gearshape") }
                .tag(Tab.general)

            connectionTab
                .tabItem { Label("Connection", systemImage: "network") }
                .tag(Tab.connection)

            cacheStorageTab
                .tabItem { Label("Cache & Storage", systemImage: "internaldrive") }
                .tag(Tab.cacheStorage)

            maintenanceTab
                .tabItem { Label("Maintenance", systemImage: "wrench.and.screwdriver") }
                .tag(Tab.maintenance)
        }
        .frame(width: 600, height: idealHeight)
    }

    /// Per-tab content height so the window fits each form without dead
    /// space. The grouped Form scrolls gracefully if a localized build
    /// wraps a footer onto an extra line.
    private var idealHeight: CGFloat {
        switch selectedTab {
        case .general:      return 360
        case .connection:   return advancedAddressesExpanded ? 660 : 545
        case .cacheStorage: return 610
        case .maintenance:  return 500
        }
    }

    // MARK: - General

    private var generalTab: some View {
        Form {
            Section {
                Toggle("Start at login", isOn: $preferences.startAtLogin)
                    .onChange(of: preferences.startAtLogin) { _, newValue in
                        LoginItemManager.setEnabled(newValue)
                    }
                Toggle("Global search hotkey (⌘⇧F)", isOn: $preferences.showSearchHotkey)
                    .onChange(of: preferences.showSearchHotkey) { _, newValue in
                        // Apply immediately so the user sees the effect right away.
                        if let appDelegate = NSApp.delegate as? AppDelegate {
                            if newValue {
                                appDelegate.registerSearchHotkey()
                            } else {
                                appDelegate.unregisterSearchHotkey()
                            }
                        }
                    }
                // C.4 (QA-10, 2026-05-17): opt-in notifications on
                // auto-offline transitions. ServerController watches the
                // auto_offline edge in /offline polling and calls
                // UNUserNotificationCenter when this is enabled. We
                // request authorization on the false→true edge — no
                // point asking the system for permission until the user
                // actually wants the notifications.
                Toggle("Notify on auto-offline and recovery", isOn: $preferences.offlineNotificationsEnabled)
                    .onChange(of: preferences.offlineNotificationsEnabled) { _, newValue in
                        guard newValue else { return }
                        UNUserNotificationCenter.current().requestAuthorization(
                            options: [.alert, .sound]
                        ) { granted, _ in
                            if !granted {
                                Task { @MainActor in
                                    preferences.offlineNotificationsEnabled = false
                                }
                            }
                        }
                    }
            } header: {
                Text("Behavior")
            } footer: {
                footnote("These settings apply immediately.")
            }

            Section {
                LabeledContent("First-run checks and guided start") {
                    Button("Open Setup Assistant…") {
                        if let appDelegate = NSApp.delegate as? AppDelegate,
                           let menuBar = appDelegate.menuBarController {
                            menuBar.openOnboardingWindow()
                        }
                    }
                }
            } header: {
                Text("Setup")
            } footer: {
                footnote("Re-runs the preflight checks (juicefs, macFUSE, backend reachability) and walks through any fixes.")
            }
        }
        .formStyle(.grouped)
    }

    // MARK: - Connection

    private var connectionTab: some View {
        Form {
            Section {
                LabeledContent("Volume name") {
                    TextField("zpool", text: $preferences.volumeName)
                        .frame(width: 200)
                        .multilineTextAlignment(.trailing)
                        .onChange(of: preferences.volumeName) { oldName, newName in
                            deriveMountPoint(oldName: oldName, newName: newName)
                        }
                }
                LabeledContent("Mounts at") {
                    Text(preferences.mountPoint)
                        .font(.body.monospaced())
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }
            } header: {
                Text("Volume")
            } footer: {
                footnote("The volume appears in Finder at /Volumes/<name>. Renaming applies after Stop everything → Start — Restart keeps the current Finder mount in place. A custom mount point can be set under Advanced addresses.")
            }

            Section {
                LabeledContent("Redis URL") {
                    TextField("redis://127.0.0.1:6379/1", text: $preferences.redisURL)
                        .font(.body.monospaced())
                        .frame(width: 300)
                        .multilineTextAlignment(.trailing)
                        .onChange(of: preferences.redisURL) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.redisURL = clean }
                        }
                }
                LabeledContent("S3 endpoint override") {
                    TextField("http://<server-ip>:9000/<bucket>", text: $preferences.s3EndpointOverride)
                        .font(.body.monospaced())
                        .frame(width: 300)
                        .multilineTextAlignment(.trailing)
                        .onChange(of: preferences.s3EndpointOverride) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.s3EndpointOverride = clean }
                        }
                }
            } header: {
                Text("Server")
            } footer: {
                footnote("Redis stores the JuiceFS metadata this Mac syncs from. Leave the S3 override empty for direct-LAN setups; set it when the server formatted JuiceFS with a hostname this Mac can't resolve (typical for docker-internal names) — e.g. http://<server-ip>:9000/<bucket>. Applies on the next start; the background JuiceFS daemon re-reads these only after Stop everything → Start.")
            }

            Section {
                DisclosureGroup("Advanced addresses", isExpanded: $advancedAddressesExpanded) {
                    LabeledContent("NFS listen address") {
                        TextField("127.0.0.1:11049", text: $preferences.nfsListenAddr)
                            .font(.body.monospaced())
                            .frame(width: 200)
                            .multilineTextAlignment(.trailing)
                            .onChange(of: preferences.nfsListenAddr) { _, newValue in
                                let clean = stripWhitespace(newValue)
                                if clean != newValue { preferences.nfsListenAddr = clean }
                            }
                    }
                    LabeledContent("Metrics address") {
                        TextField("127.0.0.1:11050", text: $preferences.metricsAddr)
                            .font(.body.monospaced())
                            .frame(width: 200)
                            .multilineTextAlignment(.trailing)
                            .onChange(of: preferences.metricsAddr) { _, newValue in
                                let clean = stripWhitespace(newValue)
                                if clean != newValue { preferences.metricsAddr = clean }
                            }
                    }
                    LabeledContent("Custom mount point") {
                        HStack(spacing: 6) {
                            TextField("/Volumes/zpool", text: $preferences.mountPoint)
                                .font(.body.monospaced())
                                .frame(width: 220)
                                .multilineTextAlignment(.trailing)
                            Button("Choose…") { chooseMountPoint() }
                        }
                    }
                }
            } footer: {
                footnote("Local loopback endpoints for the built-in NFS server and its control plane. The NFS listen address and mount point take effect after Stop everything → Start (the Finder mount must re-target them); the metrics address applies on the next start — Restart Server is enough. After editing the metrics address, health readouts in the popover pause until that restart.")
            }
        }
        .formStyle(.grouped)
        .onAppear { seedDerivationAnchor() }
    }

    // MARK: - Cache & Storage

    private var cacheStorageTab: some View {
        Form {
            Section {
                numericRow("SSD cache size", value: $preferences.ssdCacheGB,
                           unit: "GB", range: 10...2000, fallback: 100)
                numericRow("Memory buffer budget", value: $preferences.memoryBufferMB,
                           unit: "MB", range: 128...16384, fallback: 2048)
                numericRow("Buffer files smaller than", value: $preferences.memBufFileLimitMB,
                           unit: "MB", range: 1...1024, fallback: 128)
            } header: {
                Text("Cache layers")
            } footer: {
                footnote("The SSD cache stores file blocks via JuiceFS (it may grow automatically to keep pinned files resident). The memory buffer serves small files (project files, LUTs) under the size threshold from RAM with zero syscalls. Memory-buffer changes apply on the next start — Restart Server is enough; the SSD cache size is read by the JuiceFS daemon, so Stop everything → Start to apply it.")
            }

            Section {
                Toggle("Enable write spool", isOn: $preferences.spoolEnabled)
                    .onChange(of: preferences.spoolEnabled) { _, newValue in
                        // The Go core reads the spool flag only at start, so
                        // a running server must restart to pick up the change.
                        guard !suppressSpoolToggleSideEffect else { return }
                        guard isRunningLike else { return }
                        if newValue {
                            server.restart()
                        } else {
                            // LB-3 stranded-writes guard: restarting with the
                            // spool DISABLED skips ALL spool wiring including
                            // boot recovery, so any not-yet-uploaded entries
                            // would be orphaned forever — Finder already told
                            // the user "copied". Check pending before applying.
                            checkPendingThenDisableSpool()
                        }
                    }
                numericRow("Spool capacity", value: $preferences.spoolCapacityGB,
                           unit: "GB", range: 10...500, fallback: 50)
                    .disabled(!preferences.spoolEnabled)
            } header: {
                Text("Write spool (background uploads)")
            } footer: {
                footnote("Writes land on the local SSD and are acknowledged immediately, then upload in the background — large copies feel local even over a slow link. Pending uploads show in the menu-bar popover. The toggle restarts the server when needed; capacity changes apply on the next start.")
            }

            Section {
                numericRow("Reconcile interval", value: $preferences.reconcileSeconds,
                           unit: "seconds", range: 5...3600, fallback: 30)
            } header: {
                Text("Metadata sync")
            } footer: {
                footnote("How often the local metadata cache reconciles with Redis (real-time events arrive separately). Applies on the next start — Restart Server is enough.")
            }
        }
        .formStyle(.grouped)
        // LB-3 stranded-writes guard dialogs. confirmationDialog matches the
        // destructive-action idiom used elsewhere (Stop everything, Reset DB).
        .confirmationDialog(
            "\(spoolDisablePendingFiles) file\(spoolDisablePendingFiles == 1 ? "" : "s") (\(formatBytesPrefs(spoolDisablePendingBytes))) not yet uploaded",
            isPresented: $showSpoolDisableDialog,
            titleVisibility: .visible
        ) {
            Button("Upload now, then turn off") {
                spoolDrainWaitActive = true
            }
            Button("Turn off anyway", role: .destructive) {
                // The bytes stay on the local SSD and the rows stay in the
                // index, but with the spool off nothing will ever upload
                // them (the Go side logs a loud warning at start).
                server.restart()
            }
            Button("Cancel", role: .cancel) {
                revertSpoolToggleToEnabled()
            }
        } message: {
            Text("Turning the write spool off skips its recovery at start, so files still waiting to upload would stay stranded on this Mac — even though Finder already reported them as copied. Upload them first, or keep the spool on.")
        }
        .sheet(isPresented: $spoolDrainWaitActive) {
            SpoolDrainWaitView(
                metricsAddr: preferences.metricsAddr,
                onDrained: {
                    spoolDrainWaitActive = false
                    server.restart()
                },
                onCancel: {
                    spoolDrainWaitActive = false
                    revertSpoolToggleToEnabled()
                }
            )
        }
    }

    // MARK: - Maintenance

    private var maintenanceTab: some View {
        Form {
            Section {
                LabeledContent("Database file") {
                    HStack(spacing: 6) {
                        TextField("", text: $preferences.dbPath)
                            .font(.caption.monospaced())
                            .frame(width: 260)
                            .multilineTextAlignment(.trailing)
                        Button("Choose…") { chooseDBPath() }
                    }
                }
                LabeledContent("Force a clean re-sync from Redis") {
                    Button("Reset local metadata cache…", role: .destructive) {
                        resetDatabase()
                    }
                    .disabled(!canResetDatabase)
                    .help(resetDatabaseHelp)
                }
            } header: {
                Text("Local database")
            } footer: {
                footnote("The metadata cache is a local SQLite mirror of Redis — resetting it is safe and rebuilds on the next start. Offline pins (pin.db) are never touched. Database-file changes apply on the next start — Restart Server is enough.")
            }

            Section {
                LabeledContent("Apply settings that need a restart") {
                    Button("Restart Server") {
                        server.restart()
                    }
                    .disabled(!isRunningLike)
                }
                LabeledContent("Backend health") {
                    HStack(spacing: 14) {
                        healthDot("Redis", healthy: server.stats.healthRedis)
                        healthDot("MinIO", healthy: server.stats.healthMinIO)
                        healthDot("FUSE",  healthy: server.stats.healthFUSE)
                    }
                }
            } header: {
                Text("Server")
            } footer: {
                footnote("Restart soft-stops and starts the server; the JuiceFS daemon and the Finder mount stay up throughout.")
            }

            Section {
                LabeledContent("Logs, metrics, and system state for support") {
                    Button("Export Diagnostics…") {
                        exportDiagnostics()
                    }
                }
            } header: {
                Text("Diagnostics")
            } footer: {
                footnote("Saves a zipped bundle — nothing leaves this Mac unless you share the file.")
            }
        }
        .formStyle(.grouped)
    }

    // MARK: - Shared bits

    /// Section footnote in the standard secondary-caption style.
    private func footnote(_ text: String) -> some View {
        Text(text)
            .font(.caption)
            .foregroundStyle(.secondary)
    }

    /// A labeled numeric field with a trailing unit, clamped to `range` on
    /// commit. Typing an out-of-range value snaps to the nearest bound.
    /// `fallback` is only the placeholder text — a legacy out-of-range value
    /// already stored in defaults renders as-is until this field is edited
    /// (the clamp is write-through, not read-through); the Go side maps
    /// nonsensical ≤0 tuning values to its own defaults, so such a value is
    /// cosmetic here, never harmful downstream.
    private func numericRow(_ label: String, value: Binding<Int>,
                            unit: String, range: ClosedRange<Int>,
                            fallback: Int) -> some View {
        LabeledContent(label) {
            HStack(spacing: 5) {
                TextField("\(fallback)", value: clamped(value, range), format: .number)
                    .frame(width: 70)
                    .multilineTextAlignment(.trailing)
                Text(unit)
                    .foregroundStyle(.secondary)
            }
        }
    }

    /// Clamping write-through binding: reads pass straight through, writes
    /// snap into `range`. TextField(value:format:) commits on Enter/focus
    /// loss, so this never fights the user mid-keystroke.
    private func clamped(_ binding: Binding<Int>, _ range: ClosedRange<Int>) -> Binding<Int> {
        Binding(
            get: { binding.wrappedValue },
            set: { binding.wrappedValue = min(max($0, range.lowerBound), range.upperBound) }
        )
    }

    private var isRunningLike: Bool {
        switch server.state {
        case .running, .syncing, .degraded: return true
        default: return false
        }
    }

    /// Strip ALL whitespace (spaces, tabs, newlines) from URL / address
    /// fields. URLs and host:port pairs never contain whitespace, so this
    /// is always safe. Catches paste-with-stray-space accidents
    /// (e.g. `redis://192.168.0.197: 30179/1`) that would otherwise
    /// produce an unhelpful `juicefs mount: exit status 1` six layers down.
    private func stripWhitespace(_ s: String) -> String {
        return s.replacingOccurrences(
            of: "\\s+",
            with: "",
            options: .regularExpression
        )
    }

    private func healthDot(_ label: String, healthy: Bool) -> some View {
        HStack(spacing: 4) {
            Circle()
                .fill(healthy ? Color.green : Color.red)
                .frame(width: 8, height: 8)
            Text(label).font(.caption)
        }
    }

    // MARK: - LB-4: volume name → mount point derivation

    /// Volume name is real now, the cheap way: editing it derives the
    /// mount point "/Volumes/<name>" — but ONLY while the mount point is
    /// still the derived value of the previous name, so a custom override
    /// (Advanced addresses) is never clobbered. `mountPoint` stays the
    /// single source of truth passed to the Go side.
    ///
    /// `derivationAnchor` (review 3b BUG 1): comparing against the
    /// derivation of the IMMEDIATELY-previous field value broke the chain on
    /// clear-then-retype — old="" derived "/Volumes/" which never matches,
    /// permanently de-linking name from mount point. The anchor remembers
    /// the last NON-EMPTY name (seeded in onAppear, from the mount point
    /// itself when the stored name is empty), so the link survives an empty
    /// intermediate state while a custom override still never matches.
    private func deriveMountPoint(oldName: String, newName: String) {
        let newClean = sanitizeVolumeName(newName)
        let anchor = derivationAnchor.isEmpty ? sanitizeVolumeName(oldName) : derivationAnchor
        guard !newClean.isEmpty else { return } // keep anchor for the retype
        let oldDerived = "/Volumes/\(anchor)"
        if preferences.mountPoint == oldDerived || preferences.mountPoint.isEmpty {
            preferences.mountPoint = "/Volumes/\(newClean)"
        }
        derivationAnchor = newClean
    }

    /// Seed the derivation anchor when the Connection tab appears. When the
    /// stored name is empty (e.g. cleared in a previous session), recover the
    /// anchor from a "/Volumes/<x>"-shaped mount point so retyping re-links.
    private func seedDerivationAnchor() {
        let name = sanitizeVolumeName(preferences.volumeName)
        if !name.isEmpty {
            derivationAnchor = name
        } else if preferences.mountPoint.hasPrefix("/Volumes/") {
            derivationAnchor = String(preferences.mountPoint.dropFirst("/Volumes/".count))
        }
    }

    /// Volume names become a path segment — strip path separators and
    /// surrounding whitespace (inner spaces are legal: "/Volumes/My Pool").
    /// "." and ".." are rejected outright (review 3b BUG 4: they would
    /// derive /Volumes or / as the target of a privileged mount).
    private func sanitizeVolumeName(_ s: String) -> String {
        let cleaned = s.replacingOccurrences(of: "/", with: "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if cleaned == "." || cleaned == ".." { return "" }
        return cleaned
    }

    private func chooseMountPoint() {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.canCreateDirectories = true
        panel.directoryURL = URL(fileURLWithPath: "/Volumes")
        if panel.runModal() == .OK, let url = panel.url {
            preferences.mountPoint = url.path
        }
    }

    private func chooseDBPath() {
        let panel = NSSavePanel()
        panel.allowedContentTypes = [.database]
        panel.nameFieldStringValue = "metadata.db"
        if panel.runModal() == .OK, let url = panel.url {
            preferences.dbPath = url.path
        }
    }

    // MARK: - S-6: reset local metadata cache

    /// True while the stop → delete → offer-start sequence is running, so
    /// the button can't double-fire and the user gets a why in the tooltip.
    @State private var resetDBInFlight = false

    private var canResetDatabase: Bool {
        if resetDBInFlight { return false }
        if case .starting = server.state { return false }
        return true
    }

    private var resetDatabaseHelp: String {
        if resetDBInFlight {
            return "Reset in progress — waiting for the server to stop and the cache files to be deleted."
        }
        if case .starting = server.state {
            return "The server is starting — wait for it to finish before resetting the cache."
        }
        return "Stops the server if it's running, deletes the local metadata cache, and offers to start again. Offline pins are kept."
    }

    /// S-6 (Phase 3b): the old flow unlinked the SQLite files under a
    /// RUNNING server — the open file handle kept the data live, so it was
    /// a silent no-op until some future restart the dialog never mentioned.
    /// New flow: explain exactly what will happen → stop the server first
    /// (soft-stop: the Go side closes the store; FUSE + the Finder mount
    /// stay up, so no admin re-prompt) → delete metadata.db/-wal/-shm
    /// (NOT pin.db — pins are a user contract for offline availability) →
    /// offer "Start Now" / "Later".
    private func resetDatabase() {
        let dbPath = preferences.dbPath
        // Trust the Go-side truth, not the UI state machine: a degraded /
        // disconnected UI can still have a live server holding the SQLite
        // files open — exactly the silent no-op this flow exists to fix.
        let serverIsRunning = NFSBridge.isRunning

        let alert = NSAlert()
        alert.messageText = "Reset local metadata cache?"
        var info = "The local metadata cache will be deleted:\n\(dbPath)\n\nIt is a mirror of the metadata in Redis and rebuilds on the next start. Files on the volume are not affected. Offline pins (pin.db) are NOT touched — pinned files stay available offline."
        if serverIsRunning {
            info += "\n\nThe server will stop first so the delete is real (deleting under a running server silently does nothing). The volume stays mounted but won't respond until the server starts again."
        }
        alert.informativeText = info
        alert.alertStyle = .warning
        alert.addButton(withTitle: serverIsRunning ? "Stop and Reset" : "Reset")
        alert.addButton(withTitle: "Cancel")
        guard alert.runModal() == .alertFirstButtonReturn else { return }

        resetDBInFlight = true
        if serverIsRunning {
            server.softStopForMaintenance { [self] in
                deleteMetadataDBFiles(at: dbPath)
                resetDBInFlight = false
                offerStartAfterReset()
            }
        } else {
            deleteMetadataDBFiles(at: dbPath)
            resetDBInFlight = false
            offerStartAfterReset()
        }
    }

    private func deleteMetadataDBFiles(at path: String) {
        // metadata.db plus SQLite WAL sidecars. pin.db is deliberately
        // NOT in this list.
        for suffix in ["", "-wal", "-shm"] {
            try? FileManager.default.removeItem(atPath: path + suffix)
        }
    }

    private func offerStartAfterReset() {
        let done = NSAlert()
        done.messageText = "Metadata cache reset"
        if case .idle = server.state {
            done.informativeText = "The local cache was deleted. Start the server now to rebuild it from Redis, or start later from the menu-bar popover."
            done.addButton(withTitle: "Start Now")
            done.addButton(withTitle: "Later")
            if done.runModal() == .alertFirstButtonReturn {
                server.start()
            }
        } else {
            done.informativeText = "The local cache was deleted. It rebuilds from Redis the next time the server starts."
            done.addButton(withTitle: "OK")
            done.runModal()
        }
    }

    // MARK: - LB-3: spool-disable stranded-writes guard

    @State private var showSpoolDisableDialog = false
    @State private var spoolDisablePendingFiles = 0
    @State private var spoolDisablePendingBytes: Int64 = 0
    @State private var spoolDrainWaitActive = false
    /// Set while we programmatically revert the toggle so onChange doesn't
    /// treat the revert as a user-initiated enable (and restart again).
    @State private var suppressSpoolToggleSideEffect = false

    /// Fetch a FRESH pending snapshot off the main thread, then either
    /// apply the disable directly (nothing pending) or raise the dialog.
    private func checkPendingThenDisableSpool() {
        // Snapshot on MainActor; the detached task must not touch
        // actor-isolated state (Swift 6 error-to-be).
        let metricsAddr = preferences.metricsAddr
        Task {
            // NFSBridge.spoolStatus is blocking HTTP — keep it off MainActor.
            let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: metricsAddr) }.value
            await MainActor.run {
                if let sp, sp.enabled, sp.hasActivity {
                    spoolDisablePendingFiles = sp.pendingFiles
                    spoolDisablePendingBytes = sp.pendingBytes
                    showSpoolDisableDialog = true
                } else {
                    // Nothing pending (or spool already off server-side):
                    // safe to restart with the spool disabled.
                    server.restart()
                }
            }
        }
    }

    private func revertSpoolToggleToEnabled() {
        suppressSpoolToggleSideEffect = true
        preferences.spoolEnabled = true
        // Clear on the next runloop tick, after onChange has observed the
        // revert with the suppression flag still up.
        DispatchQueue.main.async { suppressSpoolToggleSideEffect = false }
    }

    private func formatBytesPrefs(_ bytes: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }

    // MARK: - Diagnostics export

    /// Same flow as the popover's exporter: NSSavePanel suggesting a
    /// timestamped name on the Desktop, gathering work off the main thread.
    private func exportDiagnostics() {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = DiagnosticsExporter.suggestedFilename()
        panel.title = "Export JuiceMount Diagnostics"
        panel.message = "Save a zipped bundle of logs, metrics, and system state for support."
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

        // Capture on MainActor before the detached hop (preferences is
        // MainActor-isolated).
        let metricsAddr = server.preferences.metricsAddr
        Task.detached(priority: .userInitiated) {
            let exporter = DiagnosticsExporter(metricsAddr: metricsAddr)
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
}

// MARK: - Spool drain-wait (LB-3)

/// Small progress surface shown while we wait for the write spool to finish
/// uploading. Polls `/spool` once a second and calls `onDrained` when the
/// queue empties. Used by two stranded-write guards:
///   - Preferences → "Upload now, then turn off" (spool-disable guard)
///   - Quit → "Wait for uploads" (applicationShouldTerminate guard)
struct SpoolDrainWaitView: View {
    let metricsAddr: String
    /// Optional extra escape hatch (the quit flow offers "Quit anyway").
    var skipTitle: String?
    var onSkip: (() -> Void)?
    let onDrained: () -> Void
    let onCancel: () -> Void

    @State private var pendingFiles = 0
    @State private var pendingBytes: Int64 = 0
    @State private var inProgress: Int64 = 0
    @State private var stalledFiles = 0
    @State private var failedFiles = 0
    @State private var hasFetched = false
    /// Terminal states: the wait STOPPED without success. Auto-proceed must
    /// never fire from these — "all the files failed" is not "all the files
    /// uploaded" (adversarial-review BUG 2: with the NAS down, rows exhaust
    /// retries and pending hits 0 BECAUSE everything failed; auto-proceeding
    /// here quits/disables right on top of the stranding LB-3 exists to
    /// prevent). The user decides explicitly.
    @State private var endedWithFailures = false
    @State private var serverUnreachable = false
    @State private var nilStreak = 0

    private var proceedTitle: String { skipTitle ?? "Continue anyway" }
    private func proceedAnyway() { (onSkip ?? onDrained)() }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                if endedWithFailures || serverUnreachable {
                    Image(systemName: "exclamationmark.triangle.fill")
                        .foregroundStyle(.orange)
                } else {
                    ProgressView()
                        .controlSize(.small)
                }
                Text(endedWithFailures ? "Uploads finished with failures"
                     : serverUnreachable ? "Can't reach the upload service"
                     : "Uploading pending files…")
                    .font(.headline)
            }
            if endedWithFailures {
                Text("\(failedFiles) file\(failedFiles == 1 ? "" : "s") failed to upload — they remain only on this Mac. Use \"Retry failed\" in the menu-bar popover once the server is reachable, or continue and they stay parked in the local spool.")
                    .font(.caption)
                    .foregroundStyle(.red)
            } else if serverUnreachable {
                Text("The control plane stopped answering. The queue state is unknown — continuing may leave files un-uploaded.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else if hasFetched {
                Text("\(pendingFiles) file\(pendingFiles == 1 ? "" : "s") (\(ByteCountFormatter.string(fromByteCount: pendingBytes, countStyle: .file))) remaining\(inProgress > 0 ? " · \(inProgress) uploading now" : "")")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Checking the upload queue…")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if !endedWithFailures && !serverUnreachable && (stalledFiles + failedFiles) > 0 {
                Label("\(stalledFiles + failedFiles) entr\(stalledFiles + failedFiles == 1 ? "y" : "ies") look stuck — this wait may not finish. Use \"Retry failed\" / \"Recover stalled\" in the menu-bar popover.", systemImage: "exclamationmark.triangle")
                    .font(.caption2)
                    .foregroundStyle(.orange)
            }
            HStack {
                Spacer()
                if endedWithFailures || serverUnreachable {
                    Button(proceedTitle, role: .destructive) { proceedAnyway() }
                } else if let skipTitle, let onSkip {
                    Button(skipTitle, role: .destructive) { onSkip() }
                }
                Button("Cancel") { onCancel() }
                    .keyboardShortcut(.cancelAction)
            }
        }
        .padding(20)
        .frame(width: 380)
        .task {
            // 1 Hz poll until drained or the view goes away. The fetch is
            // blocking HTTP, so it runs detached off MainActor.
            while !Task.isCancelled {
                let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: metricsAddr) }.value
                if Task.isCancelled { return }
                if let sp {
                    nilStreak = 0
                    serverUnreachable = false
                    pendingFiles = sp.pendingFiles
                    pendingBytes = sp.pendingBytes
                    inProgress = sp.inProgress
                    stalledFiles = sp.stalledFiles
                    failedFiles = sp.failedFiles
                    hasFetched = true
                    if sp.enabled && !sp.hasActivity {
                        if sp.failedFiles > 0 {
                            // Queue emptied by FAILURE, not success: stop and
                            // make the user decide (BUG 2). No auto-proceed.
                            endedWithFailures = true
                            return
                        }
                        onDrained()
                        return
                    }
                    if !sp.enabled {
                        // Spool vanished server-side (e.g. server stopped):
                        // nothing more to wait for here.
                        onDrained()
                        return
                    }
                } else {
                    nilStreak += 1
                    if nilStreak >= 5 {
                        // ~5s of no answers: stop spinning forever on a dead
                        // control plane; surface it and let the user decide.
                        serverUnreachable = true
                        return
                    }
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }
}
