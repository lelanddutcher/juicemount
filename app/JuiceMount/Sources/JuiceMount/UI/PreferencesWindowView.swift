import SwiftUI
import UserNotifications

struct PreferencesWindowView: View {
    @Bindable var preferences: Preferences
    @Bindable var server: ServerController

    @State private var selectedTab: Tab = .general

    enum Tab: String, CaseIterable, Identifiable {
        case general = "General"
        case server = "Server"
        case cache = "Cache"
        case advanced = "Advanced"
        var id: Self { self }

        var icon: String {
            switch self {
            case .general:  return "gearshape"
            case .server:   return "network"
            case .cache:    return "memorychip"
            case .advanced: return "slider.horizontal.3"
            }
        }
    }

    var body: some View {
        TabView(selection: $selectedTab) {
            generalTab
                .tabItem { Label("General", systemImage: "gearshape") }
                .tag(Tab.general)

            serverTab
                .tabItem { Label("Server", systemImage: "network") }
                .tag(Tab.server)

            cacheTab
                .tabItem { Label("Cache", systemImage: "memorychip") }
                .tag(Tab.cache)

            advancedTab
                .tabItem { Label("Advanced", systemImage: "slider.horizontal.3") }
                .tag(Tab.advanced)
        }
        .padding(20)
        .frame(width: 520, height: 480)
    }

    // MARK: - General

    private var generalTab: some View {
        Form {
            Section {
                LabeledContent("Volume Name") {
                    TextField("zpool", text: $preferences.volumeName)
                        .textFieldStyle(.roundedBorder)
                }
                LabeledContent("Mount Point") {
                    HStack {
                        TextField("/Volumes/zpool", text: $preferences.mountPoint)
                            .textFieldStyle(.roundedBorder)
                        Button("Choose…") { chooseMountPoint() }
                    }
                }
            } header: {
                Text("Volume").font(.headline)
            }

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
                Toggle("Notify on auto-offline / recovery", isOn: $preferences.offlineNotificationsEnabled)
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
                Text("Behavior").font(.headline)
            }

            Spacer()
            restartHint
        }
        .formStyle(.grouped)
    }

    // MARK: - Server

    private var serverTab: some View {
        Form {
            Section {
                LabeledContent("Redis URL") {
                    TextField("redis://127.0.0.1:6379/1", text: $preferences.redisURL)
                        .textFieldStyle(.roundedBorder)
                        .font(.system(.body, design: .monospaced))
                        .onChange(of: preferences.redisURL) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.redisURL = clean }
                        }
                }
                LabeledContent("S3 Endpoint Override") {
                    TextField("http://<truenas-ip>:30151/zpool", text: $preferences.s3EndpointOverride)
                        .textFieldStyle(.roundedBorder)
                        .font(.system(.body, design: .monospaced))
                        .onChange(of: preferences.s3EndpointOverride) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.s3EndpointOverride = clean }
                        }
                }
                LabeledContent("NFS Listen Address") {
                    TextField("127.0.0.1:11049", text: $preferences.nfsListenAddr)
                        .textFieldStyle(.roundedBorder)
                        .font(.system(.body, design: .monospaced))
                        .onChange(of: preferences.nfsListenAddr) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.nfsListenAddr = clean }
                        }
                }
            } header: {
                Text("Connection").font(.headline)
            } footer: {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Redis stores JuiceFS metadata. The NFS listen address is the local NFS server endpoint that macOS mounts.")
                    Text("S3 Endpoint Override: leave empty for direct-LAN setups. Set this when the server formatted JuiceFS with a docker-internal hostname your Mac can't resolve (typical for the TrueNAS app install). Example: http://192.168.0.197:30151/zpool")
                }
                .font(.caption)
                .foregroundStyle(.secondary)
            }

            Section {
                LabeledContent("Health") {
                    HStack(spacing: 14) {
                        healthDot("Redis", healthy: server.stats.healthRedis)
                        healthDot("MinIO", healthy: server.stats.healthMinIO)
                        healthDot("FUSE",  healthy: server.stats.healthFUSE)
                    }
                }
            } header: {
                Text("Status").font(.headline)
            }

            Spacer()
            restartHint
        }
        .formStyle(.grouped)
    }

    // MARK: - Cache

    private var cacheTab: some View {
        Form {
            Section {
                LabeledContent("SSD Cache Size") {
                    HStack {
                        TextField("100", value: $preferences.ssdCacheGB, format: .number)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 80)
                        Text("GB")
                        Slider(value: Binding(
                            get: { Double(preferences.ssdCacheGB) },
                            set: { preferences.ssdCacheGB = Int($0) }
                        ), in: 10...2000, step: 10)
                    }
                }

                LabeledContent("Memory Buffer Budget") {
                    HStack {
                        TextField("2048", value: $preferences.memoryBufferMB, format: .number)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 80)
                        Text("MB")
                        Slider(value: Binding(
                            get: { Double(preferences.memoryBufferMB) },
                            set: { preferences.memoryBufferMB = Int($0) }
                        ), in: 128...16384, step: 128)
                    }
                }

                LabeledContent("Buffer Files Smaller Than") {
                    HStack {
                        TextField("128", value: $preferences.memBufFileLimitMB, format: .number)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 80)
                        Text("MB")
                    }
                }
            } header: {
                Text("Cache Layers").font(.headline)
            } footer: {
                Text("SSD cache stores file blocks via JuiceFS. Memory buffer is for small files (project files, LUTs) under the size threshold — eliminates syscalls.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
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
                LabeledContent("Spool Capacity") {
                    HStack {
                        TextField("50", value: $preferences.spoolCapacityGB, format: .number)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 80)
                        Text("GB")
                        Slider(value: Binding(
                            get: { Double(preferences.spoolCapacityGB) },
                            set: { preferences.spoolCapacityGB = Int($0) }
                        ), in: 10...500, step: 10)
                    }
                    .disabled(!preferences.spoolEnabled)
                }
            } header: {
                Text("Write Spool (Background Uploads)").font(.headline)
            } footer: {
                Text("Writes land on local SSD and are acknowledged immediately, then upload to your storage in the background — large copies feel local even over a slow link. Toggling restarts the server; pending uploads show in the menu-bar popover.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()
            restartHint
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
        Task {
            // NFSBridge.spoolStatus is blocking HTTP — keep it off MainActor.
            let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: preferences.metricsAddr) }.value
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

    // MARK: - Advanced

    private var advancedTab: some View {
        Form {
            Section {
                LabeledContent("Reconcile Interval") {
                    HStack {
                        TextField("30", value: $preferences.reconcileSeconds, format: .number)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 80)
                        Text("seconds")
                    }
                }
                LabeledContent("Database Path") {
                    HStack {
                        TextField("", text: $preferences.dbPath)
                            .textFieldStyle(.roundedBorder)
                            .font(.system(.caption, design: .monospaced))
                        Button("Choose…") { chooseDBPath() }
                    }
                }
            } header: {
                Text("Tuning").font(.headline)
            }

            Section {
                Button(role: .destructive) {
                    resetDatabase()
                } label: {
                    Label("Reset Local Metadata Cache", systemImage: "trash")
                }

                Button {
                    server.restart()
                } label: {
                    Label("Restart Server", systemImage: "arrow.triangle.2.circlepath")
                }
            } header: {
                Text("Maintenance").font(.headline)
            } footer: {
                Text("Resetting the local cache forces a full re-sync from Redis on next start. Useful if the cache has gotten out of sync with the server.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()
            restartHint
        }
        .formStyle(.grouped)
    }

    // MARK: - Shared bits

    private var restartHint: some View {
        Group {
            if isRunningLike {
                HStack(spacing: 6) {
                    Image(systemName: "info.circle")
                    Text("Most changes apply on next server restart.")
                }
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.top, 8)
            }
        }
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

    private func resetDatabase() {
        let alert = NSAlert()
        alert.messageText = "Reset metadata cache?"
        alert.informativeText = "The local SQLite cache at \(preferences.dbPath) will be deleted. The server will resync everything from Redis on next start. Files on the volume will not be affected."
        alert.alertStyle = .warning
        alert.addButton(withTitle: "Reset")
        alert.addButton(withTitle: "Cancel")
        if alert.runModal() == .alertFirstButtonReturn {
            try? FileManager.default.removeItem(atPath: preferences.dbPath)
            // Also clean up WAL files
            try? FileManager.default.removeItem(atPath: preferences.dbPath + "-wal")
            try? FileManager.default.removeItem(atPath: preferences.dbPath + "-shm")
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
