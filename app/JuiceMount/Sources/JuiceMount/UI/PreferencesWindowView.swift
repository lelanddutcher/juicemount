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
                    .onChange(of: preferences.spoolEnabled) { _, _ in
                        // The Go core reads JM_SPOOL_ENABLE only at start, so
                        // a running server must restart to pick up the change.
                        if isRunningLike { server.restart() }
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
