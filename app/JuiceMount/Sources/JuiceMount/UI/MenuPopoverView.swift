import SwiftUI

/// The popover shown when the user clicks the menu bar icon.
/// Layout matches the design from the roadmap, with native macOS aesthetics.
struct MenuPopoverView: View {

    @Bindable var server: ServerController
    let onSearch: () -> Void
    let onPreferences: () -> Void
    let onQuit: () -> Void

    @State private var cacheStatus = NFSBridge.CacheStatus()
    @State private var cacheTimer: Timer?
    @State private var offlineToggleBusy = false

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
            cacheTimer = Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { _ in
                Task { @MainActor in refreshCacheStatus() }
            }
        }
        .onDisappear {
            cacheTimer?.invalidate()
            cacheTimer = nil
        }
    }

    private func refreshCacheStatus() {
        cacheStatus = NFSBridge.cacheStatus()
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

            if cacheStatus.aggregate.TotalFiles == 0 && cacheStatus.live.FilesPrefetched == 0 {
                Text("No pinned files. Use the CLI: `juicemount pin <path>`")
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

    private var offlineToggle: some View {
        Toggle(isOn: Binding(
            get: { cacheStatus.offline_mode },
            set: { newValue in
                offlineToggleBusy = true
                NFSBridge.setOffline(newValue)
                refreshCacheStatus()
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
                }
            }
            if cacheStatus.roots.count > 3 {
                Text("+ \(cacheStatus.roots.count - 3) more pin roots")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
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
                Text(server.state.displayLabel)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
    }

    private var statusDot: some View {
        Circle()
            .fill(statusColor)
            .frame(width: 12, height: 12)
            .overlay(Circle().stroke(.black.opacity(0.15), lineWidth: 0.5))
            .shadow(color: statusColor.opacity(0.5), radius: 4)
    }

    private var statusColor: Color {
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

            // Sync now (only when running)
            ActionButton(
                title: "Sync Now",
                systemImage: "arrow.triangle.2.circlepath",
                disabled: !isRunningLike,
                action: { server.syncNow() }
            )

            ActionButton(
                title: "Preferences…",
                systemImage: "gear",
                shortcut: "⌘,",
                action: onPreferences
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
