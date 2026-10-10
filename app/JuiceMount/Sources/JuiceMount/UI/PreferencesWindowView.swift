import SwiftUI
import UserNotifications

/// Preferences window (Phase 3b redesign).
///
/// Layout principles:
///   - One grouped Form per tab; Sections carry clear headers and a footer
///     that states exactly WHEN each group takes effect (immediate vs next
///     start vs full Stop → Start) — accurate to the actual plumbing:
///     `Preferences.toServerConfig()` is read once in
///     `ServerController.start()`, and Restart keeps the JuiceFS daemon warm
///     while briefly remounting Finder/NFS, so daemon-level settings only
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
    @State private var customMountExpanded = false
    // Connection tab: Redis password entry. The draft never leaves this view
    // except through Preferences.setRedisPassword (Keychain).
    @State private var redisPasswordDraft = ""
    @State private var redisPasswordSaveFailed = false
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
        // NO fixed size here, in EITHER axis, and do not add one back.
        //
        // The window owns its size now (see openPreferencesWindow). The content
        // fills whatever it is given and each tab's grouped Form scrolls if it
        // needs more room. That is deliberate: while this view declared its own
        // size AND the window derived its size from this view, the two chased
        // each other through SwiftUI's preferredContentSize getter until AppKit
        // hit its 141-iteration display-cycle limit and aborted the process.
        //
        //   Marking window as needing Update Constraints in Window
        //   for identifier 2702 (limit: 141, count: 143)
        //
        // Clicking either disclosure caret below did it every time (four
        // crashes in eleven minutes on 2026-08-16), and it recurred on
        // 2026-08-18 through the same getter after only the changing HEIGHT had
        // been removed. Each crash costs minutes of unusable mount, because the
        // app relaunches and re-runs a full metadata sync before the NFS server
        // starts listening.
        //
        // A size here would restore the second authority and with it the loop.
        .frame(minWidth: 560, minHeight: 420)
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
                // Composed "mounts at" row: a fixed /Volumes/ prefix followed by
                // the EDITABLE, bordered volume name — so the name field is
                // unmistakably a field, and the name↔mount-path link is visible
                // (editing the tail derives mountPoint via the unchanged
                // deriveMountPoint machinery). R-9 design sprint, 2026-06-17.
                LabeledContent("Mounts at") {
                    HStack(spacing: 2) {
                        Text("/Volumes/")
                            .font(.body.monospaced())
                            .foregroundStyle(.secondary)
                        TextField("zpool", text: $preferences.volumeName)
                            .textFieldStyle(.roundedBorder)
                            .font(.body.monospaced())
                            .frame(width: 180, alignment: .leading)
                            .onChange(of: preferences.volumeName) { oldName, newName in
                                deriveMountPoint(oldName: oldName, newName: newName)
                            }
                    }
                }
                footnote("This name is what Finder shows for the volume — type to rename it. The mount path updates as you type.")

                // Power-user escape hatch, grouped WITH the volume (a custom mount
                // location is a volume-identity decision, not a network address).
                DisclosureGroup("Use a custom location instead", isExpanded: $customMountExpanded) {
                    LabeledContent("Mount point") {
                        HStack(spacing: 6) {
                            TextField("/Volumes/zpool", text: $preferences.mountPoint)
                                .textFieldStyle(.roundedBorder)
                                .font(.body.monospaced())
                                .frame(width: 240, alignment: .leading)
                            Button("Choose…") { chooseMountPoint() }
                        }
                    }
                    footnote("Overrides the derived /Volumes/<name> path. Use this only if you need the volume somewhere other than /Volumes.")
                }
            } header: {
                Text("Volume")
            } footer: {
                footnote("Renaming or moving the volume applies after Stop everything → Start — Restart keeps the current Finder mount in place.")
            }

            Section {
                LabeledContent("Redis URL") {
                    TextField("redis://127.0.0.1:6379/1", text: $preferences.redisURL)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospaced())
                        .frame(width: 300, alignment: .leading)
                        .onChange(of: preferences.redisURL) { _, newValue in
                            let clean = stripWhitespace(newValue)
                            if clean != newValue { preferences.redisURL = clean }
                        }
                }
                LabeledContent("Redis password") {
                    HStack(spacing: 6) {
                        SecureField("Password for this Mac", text: $redisPasswordDraft)
                            .textFieldStyle(.roundedBorder)
                            .frame(width: 220, alignment: .leading)
                            .onSubmit(saveRedisPassword)
                        Button("Save") { saveRedisPassword() }
                            .disabled(redisPasswordDraft.isEmpty)
                    }
                }
                redisPasswordStatusLine
                LabeledContent("S3 endpoint override") {
                    TextField("http://<server-ip>:9000/<bucket>", text: $preferences.s3EndpointOverride)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospaced())
                        .frame(width: 300, alignment: .leading)
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

            serverAccessSection

            Section {
                DisclosureGroup("Advanced addresses", isExpanded: $advancedAddressesExpanded) {
                    LabeledContent("NFS listen address") {
                        TextField("127.0.0.1:11049", text: $preferences.nfsListenAddr)
                            .textFieldStyle(.roundedBorder)
                            .font(.body.monospaced())
                            .frame(width: 200, alignment: .leading)
                            .onChange(of: preferences.nfsListenAddr) { _, newValue in
                                let clean = stripWhitespace(newValue)
                                if clean != newValue { preferences.nfsListenAddr = clean }
                            }
                    }
                    LabeledContent("Metrics address") {
                        TextField("127.0.0.1:11050", text: $preferences.metricsAddr)
                            .textFieldStyle(.roundedBorder)
                            .font(.body.monospaced())
                            .frame(width: 200, alignment: .leading)
                            .onChange(of: preferences.metricsAddr) { _, newValue in
                                let clean = stripWhitespace(newValue)
                                if clean != newValue { preferences.metricsAddr = clean }
                            }
                    }
                }
            } footer: {
                footnote("Local loopback endpoints for the built-in NFS server and its control plane. Changed addresses are saved while the mounted server keeps its current configuration. They remain pending until the volume can be safely restarted.")
            }

            // MARK: - Remote Access (JuiceMount Link)

            Section {
                Toggle("Use existing private network", isOn: $preferences.useExistingPrivateNetwork)
                if preferences.useExistingPrivateNetwork {
                    footnote("Uses the Redis and object-store addresses above through macOS routing. Choose this when Tailscale, NetBird, Firezone, ZeroTier, or another trusted VPN already makes the NAS reachable. JuiceMount does not receive or store that provider's credentials. A mode change is saved; the mounted server keeps its previous route until the volume can be safely restarted.")
                }
                if let status = server.restartConfigurationStatus {
                    Label(status, systemImage: "clock.arrow.circlepath")
                        .font(.caption)
                        .foregroundStyle(MenuPopoverView.glanceAmber)
                }
                LabeledContent("Server URL") {
                    TextField("http://<nas-ip>:30193", text: $preferences.linkServerURL)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospaced())
                        .onChange(of: preferences.linkServerURL) { _, newValue in
                            let clean = newValue.trimmingCharacters(in: .whitespaces)
                            if clean != newValue { preferences.linkServerURL = clean }
                        }
                        .disabled(preferences.useExistingPrivateNetwork)
                }
                LabeledContent("Pairing Code") {
                    SecureField("hskey-auth-…", text: $preferences.linkAuthKey)
                        .textFieldStyle(.roundedBorder)
                        .font(.body.monospaced())
                        .disabled(preferences.useExistingPrivateNetwork)
                }
                LabeledContent("Hostname") {
                    TextField("juicemount-mac", text: $preferences.linkHostname)
                        .textFieldStyle(.roundedBorder)
                        .disabled(preferences.useExistingPrivateNetwork)
                }
                HStack(spacing: 10) {
                    Button {
                        server.testLink()
                    } label: {
                        if server.linkTestInFlight {
                            HStack(spacing: 6) {
                                ProgressView().controlSize(.small)
                                Text("Pairing & Testing…")
                            }
                        } else {
                            Label("Apply & Test", systemImage: "network.badge.shield.half.filled")
                        }
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(preferences.useExistingPrivateNetwork || server.linkTestInFlight || preferences.linkServerURL.isEmpty || preferences.linkAuthKey.isEmpty)

                    if let result = server.linkTestResult {
                        Label(linkResultText(result), systemImage: result.ok ? "checkmark.circle.fill" : (result.online ? "exclamationmark.triangle.fill" : "xmark.circle.fill"))
                            .font(.caption)
                            .foregroundStyle(result.ok ? .green : (result.online ? .orange : .red))
                            .lineLimit(2)
                            .help(result.error ?? linkResultText(result))
                    }
                }
                footnote("Apply & Test is available before a volume is mounted. It confirms pairing, authorization, object storage, and the encrypted data route. Pairing changes made while a volume is mounted remain saved until the volume can be safely restarted.")
            } header: {
                Text("Remote Access")
            } footer: {
                if preferences.useExistingPrivateNetwork {
                    footnote("Your VPN owns peer discovery, encryption, relays, and access policy. JuiceMount uses only the private NAS addresses you configured; no inbound public port or JuiceMount-hosted coordination service is required.")
                } else {
                    footnote("When configured, JuiceMount routes backend traffic through an encrypted tailnet tunnel. Get a code from your NAS manager's Link tab. Requires JM_NET_HEADSCALE=on on the server.")
                }
            }
        }
        .formStyle(.grouped)
        .onAppear { seedDerivationAnchor() }
    }

    // MARK: - Server access (H-8B enrollment)

    /// This Mac's own access to the server: it asks the Manager for a
    /// principal, an admin approves it there, and the password and object key
    /// arrive in the Keychain. The clone notice lives here too, so a migrated
    /// or cloned Mac sees why it must be approved again.
    private var serverAccessSection: some View {
        Section {
            LabeledContent("Manager URL") {
                TextField(preferences.effectiveManagerURL ?? "http://<nas-ip>:30190", text: $preferences.managerURL)
                    .textFieldStyle(.roundedBorder)
                    .font(.body.monospaced())
                    .frame(width: 300, alignment: .leading)
                    .onChange(of: preferences.managerURL) { _, newValue in
                        let clean = stripWhitespace(newValue)
                        if clean != newValue { preferences.managerURL = clean }
                    }
            }
            if let notice = preferences.deviceMigrationNotice {
                Label(notice, systemImage: "exclamationmark.triangle.fill")
                    .font(.caption)
                    .foregroundStyle(MenuPopoverView.glanceAmber)
            }
            LabeledContent("Access") {
                HStack(spacing: 8) {
                    Label(preferences.enrollment.summary, systemImage: enrollmentSymbol)
                        .font(.caption)
                        .foregroundStyle(enrollmentTint)
                        .textSelection(.enabled)
                        .lineLimit(4)
                        .frame(maxWidth: 320, alignment: .leading)
                    Button("Request access") { preferences.requestAccess() }
                        .disabled(!enrollmentCanRequest)
                }
            }
            if let note = preferences.enrollment.storageKeyNote {
                LabeledContent("Storage key") {
                    Label(note, systemImage: storageKeySymbol)
                        .font(.caption)
                        .foregroundStyle(storageKeyTint)
                        .textSelection(.enabled)
                        .lineLimit(6)
                        .frame(maxWidth: 400, alignment: .leading)
                }
            }
            if preferences.canRestorePreviousLogin,
               [.approved, .approvedPendingCutover, .approvedLoginRefused, .approvedNotInUse].contains(preferences.enrollment.phase) {
                LabeledContent("Previous login") {
                    Button("Use previous login") { preferences.restorePreviousLogin() }
                        .help("Go back to the Redis login this Mac used before it switched to its approved principal. Request access switches to the principal again.")
                }
            }
            if preferences.enrollment.phase == .waiting, !preferences.enrollment.fingerprint.isEmpty {
                LabeledContent("Code") {
                    Text(preferences.enrollment.fingerprint)
                        .font(.body.monospaced().weight(.semibold))
                        .textSelection(.enabled)
                }
            }
        } header: {
            Text("Server access")
        } footer: {
            footnote("Each Mac signs in to the server as itself. Request access sends this Mac's request to the Manager; an admin approves it on the Principals page after checking that the code matches. The approved password and object-store key are saved in the Keychain only once the server accepts them (after its proxy cutover); until then this Mac keeps its current login. They apply after Stop everything → Start. Leave the Manager URL empty to use the Redis host on port 30190.")
        }
    }

    private var storageKeySymbol: String {
        switch preferences.enrollment.storageKey {
        case .refused: return "xmark.octagon.fill"
        case .pending: return "hourglass"
        case .restartNeeded: return "arrow.clockwise.circle"
        case .normal: return "key"
        }
    }

    private var storageKeyTint: Color {
        switch preferences.enrollment.storageKey {
        case .refused: return .red
        case .pending, .restartNeeded: return MenuPopoverView.glanceAmber
        case .normal: return .secondary
        }
    }

    private var enrollmentCanRequest: Bool {
        switch preferences.enrollment.phase {
        case .notEnrolled, .failed, .conflict, .revoked, .approvedPendingCutover, .approvedLoginRefused, .approvedNotInUse: return true
        case .requesting, .waiting, .approved: return false
        }
    }

    private var enrollmentSymbol: String {
        switch preferences.enrollment.phase {
        case .approved: return "checkmark.seal.fill"
        case .waiting, .requesting, .approvedPendingCutover: return "hourglass"
        case .revoked, .conflict: return "xmark.octagon.fill"
        case .failed, .approvedLoginRefused: return "exclamationmark.triangle.fill"
        case .approvedNotInUse: return "arrow.uturn.backward.circle"
        case .notEnrolled: return "person.crop.circle.badge.questionmark"
        }
    }

    private var enrollmentTint: Color {
        switch preferences.enrollment.phase {
        case .approved: return .green
        case .waiting, .requesting, .failed, .approvedPendingCutover, .approvedLoginRefused, .approvedNotInUse: return MenuPopoverView.glanceAmber
        case .revoked, .conflict: return .red
        case .notEnrolled: return .secondary
        }
    }

    private func linkResultText(_ result: NFSBridge.LinkTestResult) -> String {
        if result.ok {
            let latency = result.rttMS.map { " · \($0) ms" } ?? ""
            let transport = result.transportMode.map { " · \($0 == "direct" ? "direct" : $0)" } ?? ""
            let throughput: String
            if let up = result.uploadMbps, let down = result.downloadMbps {
                throughput = String(format: " · %.0f/%.0f Mb/s", up, down)
            } else {
                throughput = ""
            }
            return "Paired · data plane verified\(transport)\(throughput)\(latency)"
        }
        if result.online {
            if result.redisReachable && result.objectStoreReachable {
                let transport = result.transportMode.map { " (\($0))" } ?? ""
                return "Paired; endpoints ready, data-plane test failed\(transport)"
            }
            if result.redisReachable {
                return "Paired; Redis ready, object storage unavailable"
            }
            return "Paired; Redis unavailable through Link"
        }
        return result.error ?? "Pairing failed"
    }

    // MARK: - Cache & Storage

    /// Which server the saved password belongs to, from memory only (no
    /// Keychain read in a render). A changed host, port or user is called out
    /// rather than silently leaving the new server without a password.
    @ViewBuilder
    private var redisPasswordStatusLine: some View {
        switch preferences.redisPasswordStatus {
        case .saved(let account):
            footnote("Saved in the Keychain for \(account).")
        case .missing(let account):
            Label("No password saved for \(account). Enter it above if this server requires one.",
                  systemImage: "key")
                .font(.caption)
                .foregroundStyle(MenuPopoverView.glanceAmber)
        case .invalidURL:
            footnote("Enter a valid Redis URL to save a password for it.")
        }
        if redisPasswordSaveFailed {
            Label("The Keychain did not accept the password. It is used until JuiceMount quits but is not saved.",
                  systemImage: "exclamationmark.triangle")
                .font(.caption)
                .foregroundStyle(.red)
        }
    }

    private func saveRedisPassword() {
        guard !redisPasswordDraft.isEmpty else { return }
        let draft = redisPasswordDraft
        redisPasswordDraft = ""
        preferences.setRedisPassword(draft) { ok in
            DispatchQueue.main.async { redisPasswordSaveFailed = !ok }
        }
    }

    private var cacheStorageTab: some View {
        Form {
            Section {
                numericRow("SSD cache size", value: $preferences.ssdCacheGB,
                           unit: "GB", range: 10...2000, fallback: 100)
                // The --cache-size flag is minted when the JuiceFS daemon
                // launches, so a committed change can't reach the running
                // volume. The commit raises the Restart Now / Later alert
                // (see .onChange below); "Later" leaves this persistent
                // badge so the saved-but-not-live state stays visible.
                if server.cacheSizePendingRestart {
                    cacheSizePendingRow
                }
                numericRow("Memory buffer budget", value: $preferences.memoryBufferMB,
                           unit: "MB", range: 128...16384, fallback: 2048)
                numericRow("Buffer files smaller than", value: $preferences.memBufFileLimitMB,
                           unit: "MB", range: 1...1024, fallback: 128)
            } header: {
                Text("Cache layers")
            } footer: {
                footnote("The SSD cache stores file blocks via JuiceFS (it may grow automatically to keep pinned files resident). The memory buffer serves small files (project files, LUTs) under the size threshold from RAM with zero syscalls. Changes to either setting remain saved while the mounted server uses its current configuration; they remain pending until the volume can be safely restarted.")
            }

            Section {
                Toggle("Prepare video previews in background", isOn: $preferences.nativePreviewWarm)
            } header: {
                Text("Video previews")
            } footer: {
                footnote("Downloads parts of videos in the background on healthy connections. This can use additional data. The saved choice applies after Stop everything, then Start; the mounted server keeps its current setting until then.")
            }

            Section {
                HStack {
                    Toggle("Enable write spool", isOn: Binding(
                        get: { preferences.spoolEnabled },
                        set: { enabled in
                            if enabled {
                                preferences.spoolEnabled = true
                            } else {
                                // Do not persist false before the asynchronous
                                // queue check, including while the server is stopped.
                                checkPendingThenDisableSpool()
                            }
                        }
                    ))
                    .disabled(spoolDisableCheckInFlight)
                    if spoolDisableCheckInFlight {
                        ProgressView().controlSize(.small)
                    }
                }
                if let status = server.writeSpoolConfigurationStatus {
                    Label(status, systemImage: "clock.arrow.circlepath")
                        .font(.caption)
                        .foregroundStyle(MenuPopoverView.glanceAmber)
                }
                Toggle("Automatically recover saved files", isOn: $preferences.replacementRecovery)
                    .disabled(!preferences.spoolEnabled)
                footnote("An accepted save stays readable on this Mac while it waits. Automatic recovery needs support from the storage provider. This saved choice applies after Stop everything, then Start.")
                if let status = server.recoveryConfigurationStatus {
                    Label(status, systemImage: "clock.arrow.circlepath")
                        .font(.caption)
                        .foregroundStyle(MenuPopoverView.glanceAmber)
                }
                Toggle("Auto-size spool to free disk", isOn: Binding(
                    get: { preferences.spoolCapacityGB == 0 },
                    set: { preferences.spoolCapacityGB = $0 ? 0 : 200 }
                ))
                .disabled(!preferences.spoolEnabled)
                if preferences.spoolCapacityGB != 0 {
                    numericRow("Spool capacity", value: $preferences.spoolCapacityGB,
                               unit: "GB", range: 10...2000, fallback: 200)
                        .disabled(!preferences.spoolEnabled)
                }
            } header: {
                Text("Write spool (background uploads)")
            } footer: {
                footnote("When active, writes land on the local SSD and upload in the background. Auto-size normally keeps a 20 GB safety floor. If the Mac is below it, a small working window remains without crossing the 10 GB OS floor. When the buffer fills, copies pause until space frees or you reconnect. Pending uploads show in the menu-bar popover. Spool and capacity changes are saved while the mounted server keeps its current configuration; they remain pending until the volume can be safely restarted.")
            }

            Section {
                footnote("Metadata syncs in real time via Redis change notifications — there is no interval to configure. A rare background reconcile, automatically paced by link speed, guarantees convergence as a backstop.")
            } header: {
                Text("Metadata sync")
            }
        }
        .formStyle(.grouped)
        // Cache-size restart prompt. TextField(value:format:) commits on
        // Enter/focus loss, so this fires once per committed edit — never
        // mid-keystroke. Only prompt while a change is actually PENDING
        // (a live daemon holds a different minted size); with no daemon up,
        // the next start applies the value and no ceremony is needed.
        // Editing back to the applied value clears the pending state, so
        // no alert fires for a no-op round trip.
        .onChange(of: preferences.ssdCacheGB) { _, _ in
            if server.cacheSizePendingRestart {
                showCacheSizeRestartAlert = true
            }
        }
        .alert("Restart to apply the new cache size?",
               isPresented: $showCacheSizeRestartAlert) {
            Button("Restart Now") { server.restartFull() }
            Button("Later", role: .cancel) {}
        } message: {
            Text("The new cache size takes effect after JuiceMount restarts the volume. \"Later\" keeps the setting saved — it applies on the next restart.")
        }
        // LB-3 stranded-writes guard dialogs. confirmationDialog matches the
        // destructive-action idiom used elsewhere (Stop everything, Reset DB).
        .confirmationDialog(
            spoolDisableStatusUnknown ? "Can't verify the upload queue"
                : spoolDisableVerifiedEmpty ? "No unfinished changes in the latest check"
                : "Unfinished changes remain on this Mac",
            isPresented: $showSpoolDisableDialog,
            titleVisibility: .visible
        ) {
            if !spoolDisableVerifiedEmpty {
                Button(spoolDisableStatusUnknown ? "Check again" : "Upload now, then turn off") {
                    if spoolDisableStatusUnknown {
                        checkPendingThenDisableSpool()
                    } else {
                        spoolDrainWaitActive = true
                    }
                }
            }
            Button(spoolDisableVerifiedEmpty ? "Save write spool off" : "Save off anyway", role: .destructive) {
                // The live mount keeps its current spool wiring. Pending rows
                // remain on SSD; applying the saved disable later can strand them.
                commitSpoolDisable()
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text(spoolDisableStatusUnknown
                 ? "JuiceMount couldn't confirm whether copied files remain on this Mac. If the server is stopped, start it with the spool enabled before checking again. Otherwise, keep the setting on or explicitly save it off for a later full restart."
                 : spoolDisableVerifiedEmpty
                 ? "The latest check found no unfinished changes, but another write can arrive before the setting is applied. Save the spool off only when copying to this volume has stopped. The current mount keeps its live setting."
                 : "\(spoolDisablePendingFiles) files pending (\(formatBytesPrefs(spoolDisablePendingBytes))); \(spoolDisablePendingSymlinks) links pending; \(spoolDisableFailedFiles) failed or declined. Saving the write spool off leaves the current mount unchanged, but applying that setting at a later full restart may strand copied changes on this Mac.")
        }
        .sheet(isPresented: $spoolDrainWaitActive) {
            SpoolDrainWaitView(
                metricsAddr: preferences.metricsAddr,
                onDrained: {
                    spoolDrainWaitActive = false
                    // A zero poll from the wait sheet can be stale by the
                    // time it closes; inspect again before changing the flag.
                    checkPendingThenDisableSpool()
                },
                onCancel: {
                    spoolDrainWaitActive = false
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
                footnote("The metadata cache is a local SQLite mirror of Redis — resetting it is safe, but rebuilding a large index transfers metadata and can take much longer on cellular or a VPN. Prefer LAN when possible. Ordinary restarts do not need a rebuild. Offline pins (pin.db) are never touched. Database-file changes stay saved while the mounted server uses its current configuration; they remain pending until the volume can be safely restarted.")
            }

            Section {
                LabeledContent("Apply settings that need a restart") {
                    Button("Restart Server") {
                        server.restart()
                    }
                    .disabled(!isRunningLike || server.restartConfigurationStatus != nil)
                }
                if let status = server.restartConfigurationStatus {
                    Label(status, systemImage: "clock.arrow.circlepath")
                        .font(.caption)
                        .foregroundStyle(MenuPopoverView.glanceAmber)
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
                footnote("Changed settings are saved and remain pending until the volume can be safely restarted.")
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

    // MARK: - Cache-size pending-restart flow

    /// Raised when a committed SSD cache-size edit diverges from the size
    /// the live JuiceFS daemon was minted with (see
    /// `ServerController.cacheSizePendingRestart`).
    @State private var showCacheSizeRestartAlert = false

    /// Persistent "pending restart" badge under the SSD cache size field.
    /// Amber remedy-row conventions (same palette as the popover's remedy
    /// rows) — informative, not alarming. The inline Restart Now mirrors
    /// the alert action for users who picked "Later" and changed their
    /// mind; it is the full Stop → Start (restartFull), because a soft
    /// Restart Server keeps the daemon — and its old --cache-size — alive.
    private var cacheSizePendingRow: some View {
        HStack(alignment: .firstTextBaseline, spacing: 6) {
            Image(systemName: "clock.arrow.circlepath")
                .font(.caption)
                .foregroundStyle(MenuPopoverView.glanceAmber)
            VStack(alignment: .leading, spacing: 1) {
                Text("Pending restart")
                    .font(.caption)
                    .foregroundStyle(MenuPopoverView.glanceAmber)
                Text(cacheSizePendingDetail)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            Button("Restart Now") { server.restartFull() }
                .controlSize(.small)
                .disabled(isStartingState)
                .help("Stops everything and starts again so the JuiceFS daemon relaunches with the new cache size. The volume disappears from Finder for a moment; re-mounting may ask for your password once unless the scoped sudoers rule is installed.")
        }
    }

    private var cacheSizePendingDetail: String {
        if let applied = server.appliedSsdCacheGB {
            return "The volume is running with \(applied) GB — the new \(preferences.ssdCacheGB) GB size applies when it restarts."
        }
        return "The new cache size applies when the volume restarts."
    }

    private var isStartingState: Bool {
        if case .starting = server.state { return true }
        return false
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
                    .textFieldStyle(.roundedBorder) // R-9 field style — was missed on the shared numericRow (Cache & Storage pane)
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
    /// (e.g. `redis://10.0.0.5: 30179/1`) that would otherwise
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
        if NFSBridge.usesServingService { return false }
        if resetDBInFlight { return false }
        if case .starting = server.state { return false }
        return true
    }

    private var resetDatabaseHelp: String {
        if NFSBridge.usesServingService { return ServingControlSafety.resetUnavailable }
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
    /// (the Go side closes the store; FUSE stays warm while the Finder mount
    /// briefly disappears) → delete metadata.db/-wal/-shm
    /// (NOT pin.db — pins are a user contract for offline availability) →
    /// offer "Start Now" / "Later".
    private func resetDatabase() {
        guard !NFSBridge.usesServingService else {
            let alert = NSAlert()
            alert.messageText = "Cache kept"
            alert.informativeText = ServingControlSafety.resetUnavailable
            alert.runModal()
            return
        }
        let dbPath = preferences.dbPath
        // Trust the Go-side truth, not the UI state machine: a degraded /
        // disconnected UI can still have a live server holding the SQLite
        // files open — exactly the silent no-op this flow exists to fix.
        let serverIsRunning = NFSBridge.isRunning

        let alert = NSAlert()
        alert.messageText = "Reset local metadata cache?"
        var info = "The local metadata cache will be deleted:\n\(dbPath)\n\nIt is a mirror of the metadata in Redis and rebuilds on the next start. Rebuilding a large index transfers metadata and can take much longer on cellular or a VPN; prefer LAN when possible. Ordinary restarts do not need a rebuild. Files on the volume are not affected. Offline pins (pin.db) are NOT touched — pinned files stay available offline."
        if serverIsRunning {
            info += "\n\nThe server will stop first so the delete is real (deleting under a running server silently does nothing). The Finder mount briefly disappears; the warm JuiceFS backend is retained for a fast restart."
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
        try? ServingControlSafety.deleteMetadataFiles(at: path, serviceManaged: NFSBridge.usesServingService) {
            try FileManager.default.removeItem(atPath: $0)
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
    @State private var spoolDisablePendingSymlinks = 0
    @State private var spoolDisablePendingBytes: Int64 = 0
    @State private var spoolDisableFailedFiles = 0
    @State private var spoolDisableStatusUnknown = false
    @State private var spoolDisableVerifiedEmpty = false
    @State private var spoolDrainWaitActive = false
    @State private var spoolDisableCheckInFlight = false

    /// Fetch a fresh pending snapshot off the main thread, then ask for an
    /// explicit decision. A live snapshot cannot quiesce new Finder writes.
    private func checkPendingThenDisableSpool() {
        guard !spoolDisableCheckInFlight else { return }
        spoolDisableCheckInFlight = true
        // Snapshot on MainActor; the detached task must not touch
        // actor-isolated state (Swift 6 error-to-be).
        let metricsAddr = preferences.metricsAddr
        Task {
            // NFSBridge.spoolStatus is blocking HTTP — keep it off MainActor.
            let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: metricsAddr) }.value
            await MainActor.run {
                spoolDisableCheckInFlight = false
                switch sp?.safetyState ?? .unknown {
                case .pending:
                    guard let sp else { return }
                    spoolDisablePendingFiles = sp.pendingFiles
                    spoolDisablePendingSymlinks = sp.pendingSymlinks
                    spoolDisablePendingBytes = sp.pendingBytes
                    spoolDisableFailedFiles = sp.failedFiles + sp.declinedFiles
                    spoolDisableStatusUnknown = false
                    spoolDisableVerifiedEmpty = false
                    showSpoolDisableDialog = true
                case .verifiedEmpty:
                    spoolDisableStatusUnknown = false
                    spoolDisableVerifiedEmpty = true
                    showSpoolDisableDialog = true
                case .unknown:
                    spoolDisableStatusUnknown = true
                    spoolDisableVerifiedEmpty = false
                    showSpoolDisableDialog = true
                }
            }
        }
    }

    private func commitSpoolDisable() {
        preferences.spoolEnabled = false
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
    @State private var pendingSymlinks = 0
    @State private var pendingBytes: Int64 = 0
    @State private var inProgress: Int64 = 0
    @State private var stalledFiles = 0
    @State private var failedFiles = 0
    @State private var declinedFiles = 0
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
    @State private var retryToken = 0

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
                Text(endedWithFailures ? "Uploads need attention"
                     : serverUnreachable ? "Can't verify the upload queue"
                     : "Uploading pending files…")
                    .font(.headline)
            }
            if endedWithFailures {
                Text("\(failedFiles + declinedFiles) failed or declined, and \(stalledFiles) stalled. These entries need attention before they can be considered uploaded.")
                    .font(.caption)
                    .foregroundStyle(.red)
            } else if serverUnreachable {
                Text("The queue state is unknown. The service may be unreachable or returned an incomplete status; continuing may leave files un-uploaded.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else if hasFetched {
                Text("\(pendingFiles) files (\(ByteCountFormatter.string(fromByteCount: pendingBytes, countStyle: .file))) and \(pendingSymlinks) links remaining\(inProgress > 0 ? " · \(inProgress) uploading now" : "")")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Checking the upload queue…")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            if !endedWithFailures && !serverUnreachable && (stalledFiles + failedFiles + declinedFiles) > 0 {
                Label("\(stalledFiles + failedFiles + declinedFiles) entr\(stalledFiles + failedFiles + declinedFiles == 1 ? "y" : "ies") need attention. Use \"Retry failed\" / \"Recover stalled\" in the menu-bar popover.", systemImage: "exclamationmark.triangle")
                    .font(.caption2)
                    .foregroundStyle(.orange)
            }
            HStack {
                Spacer()
                if serverUnreachable {
                    Button("Retry") {
                        serverUnreachable = false
                        nilStreak = 0
                        retryToken += 1
                    }
                }
                if let skipTitle, let onSkip {
                    Button(skipTitle, role: .destructive) { onSkip() }
                }
                Button("Cancel") { onCancel() }
                    .keyboardShortcut(.cancelAction)
            }
        }
        .padding(20)
        .frame(width: 380)
        .task(id: retryToken) {
            // 1 Hz poll until drained or the view goes away. The fetch is
            // blocking HTTP, so it runs detached off MainActor.
            while !Task.isCancelled {
                let sp = await Task.detached { NFSBridge.spoolStatus(metricsAddr: metricsAddr) }.value
                if Task.isCancelled { return }
                switch sp?.safetyState ?? .unknown {
                case .pending:
                    guard let sp else { return }
                    nilStreak = 0
                    serverUnreachable = false
                    pendingFiles = sp.pendingFiles
                    pendingSymlinks = sp.pendingSymlinks
                    pendingBytes = sp.pendingBytes
                    inProgress = sp.inProgress
                    stalledFiles = sp.stalledFiles
                    failedFiles = sp.failedFiles
                    declinedFiles = sp.declinedFiles
                    hasFetched = true
                    if sp.pendingFiles == 0 && sp.pendingSymlinks == 0 && sp.inProgress == 0 {
                        endedWithFailures = true
                        return
                    }
                case .verifiedEmpty:
                    onDrained()
                    return
                case .unknown:
                    nilStreak += 1
                    if nilStreak >= 5 {
                        serverUnreachable = true
                        return
                    }
                }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }
}
