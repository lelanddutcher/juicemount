import SwiftUI
import AppKit
import Quartz

/// Asset search window — the killer feature. Type to search across the entire
/// JuiceMount filesystem (FTS5 trigram index). Spacebar preview via QuickLook.
/// Drag results into Premiere/Resolve/FCPX timelines. Enter reveals in Finder.
struct SearchWindowView: View {

    @Bindable var server: ServerController

    @State private var query: String = ""
    @State private var results: [NFSBridge.SearchResult] = []
    @State private var selection: Set<NFSBridge.SearchResult.ID> = []
    @State private var isSearching = false
    @State private var lastQuery: String = ""
    @State private var searchTask: Task<Void, Never>?
    @State private var resultLimit: Int = 100
    @State private var scopePath: String = ""

    var body: some View {
        VStack(spacing: 0) {
            searchBar
            Divider()
            scopeBar
            Divider()
            resultsList
            Divider()
            statusBar
        }
        .frame(minWidth: 600, minHeight: 400)
        .background(VisualEffectBackground())
        .onAppear {
            // Kick off an initial query if there's a saved one
        }
        .onDisappear {
            // Cancel any in-flight search so it doesn't keep working after close
            searchTask?.cancel()
            searchTask = nil
        }
    }

    // MARK: - Search bar

    private var searchBar: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
                .font(.title3)

            TextField("Search filenames…", text: $query)
                .textFieldStyle(.plain)
                .font(.title3)
                .onChange(of: query) { _, newValue in
                    scheduleSearch(query: newValue)
                }
                .onSubmit {
                    revealSelectionInFinder()
                }

            if !query.isEmpty {
                Button(action: { query = "" }) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.plain)
            }

            if isSearching {
                ProgressView()
                    .scaleEffect(0.6)
                    .frame(width: 18, height: 18)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
    }

    // MARK: - Scope

    private var scopeBar: some View {
        HStack(spacing: 6) {
            Text("Scope:")
                .font(.caption)
                .foregroundStyle(.secondary)
            Picker("", selection: $scopePath) {
                Text("Whole library").tag("")
                Text("SFX").tag("Video Editing Assets/SFX")
                Text("LUTs").tag("Video Editing Assets/LUTS")
                Text("Footage").tag("Footage")
                Text("Film Projects").tag("Film Projects")
                Text("Music").tag("Video Editing Assets/MUSIC")
            }
            .pickerStyle(.menu)
            .labelsHidden()
            .frame(width: 200)
            .onChange(of: scopePath) { _, _ in
                scheduleSearch(query: query)
            }

            Spacer()

            Picker("", selection: $resultLimit) {
                Text("50 results").tag(50)
                Text("100 results").tag(100)
                Text("250 results").tag(250)
                Text("1000 results").tag(1000)
            }
            .pickerStyle(.menu)
            .labelsHidden()
            .frame(width: 120)
            .onChange(of: resultLimit) { _, _ in
                scheduleSearch(query: query)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 6)
    }

    // MARK: - Results

    private var resultsList: some View {
        Table(results, selection: $selection) {
            TableColumn("Name") { result in
                HStack(spacing: 8) {
                    Image(systemName: iconName(for: result))
                        .foregroundStyle(iconColor(for: result))
                        .frame(width: 18)
                    Text(result.name)
                        .lineLimit(1)
                        .truncationMode(.middle)
                }
            }
            .width(min: 200, ideal: 280)

            TableColumn("Path") { result in
                Text(result.path)
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.head)
            }
            .width(min: 200, ideal: 320)

            TableColumn("Size") { result in
                Text(result.isDir ? "—" : formatSize(result.size))
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
            }
            .width(80)
        }
        .tableStyle(.inset)
        .contextMenu(forSelectionType: NFSBridge.SearchResult.ID.self) { ids in
            if !ids.isEmpty {
                Button("Open in Finder") { revealInFinder(ids: ids) }
                Button("Quick Look") { quickLook(ids: ids) }
                Button("Copy Path") { copyPath(ids: ids) }
            }
        } primaryAction: { ids in
            // Double-click — reveal in Finder
            revealInFinder(ids: ids)
        }
        .onKeyPress(.space) {
            quickLook(ids: selection)
            return .handled
        }
    }

    // MARK: - Status bar

    private var statusBar: some View {
        HStack(spacing: 12) {
            if isSearching {
                Text("Searching…")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else if !lastQuery.isEmpty {
                Text("\(results.count) result\(results.count == 1 ? "" : "s") for \"\(lastQuery)\"")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Type to search across \(server.stats.entryCount.formatted()) files")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            Text("Spacebar: preview · ↩: open in Finder · drag to NLE")
                .font(.caption2)
                .foregroundStyle(.tertiary)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
    }

    // MARK: - Search debouncing

    private func scheduleSearch(query: String) {
        searchTask?.cancel()
        let q = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !q.isEmpty else {
            results = []
            lastQuery = ""
            isSearching = false
            return
        }
        isSearching = true
        searchTask = Task {
            // Debounce 150ms — small enough to feel instant, large enough to skip mid-typing
            try? await Task.sleep(for: .milliseconds(150))
            if Task.isCancelled { return }
            let r = await server.search(q, limit: resultLimit, parentPath: scopePath)
            if Task.isCancelled { return }
            await MainActor.run {
                self.results = r
                self.lastQuery = q
                self.isSearching = false
                // Auto-select first result so spacebar preview works immediately
                if let first = r.first {
                    self.selection = [first.id]
                }
            }
        }
    }

    // MARK: - Actions

    private func selectedResults(_ ids: Set<NFSBridge.SearchResult.ID>) -> [NFSBridge.SearchResult] {
        results.filter { ids.contains($0.id) }
    }

    private func revealInFinder(ids: Set<NFSBridge.SearchResult.ID>) {
        let urls = selectedResults(ids).compactMap { fileURL(for: $0) }
        if urls.isEmpty { return }
        if urls.count == 1 {
            NSWorkspace.shared.activateFileViewerSelecting(urls)
        } else {
            // For multiple selections, open Finder windows for each unique parent dir
            // (better UX than 10 windows is to just reveal them)
            NSWorkspace.shared.activateFileViewerSelecting(urls)
        }
    }

    private func revealSelectionInFinder() {
        revealInFinder(ids: selection)
    }

    private func quickLook(ids: Set<NFSBridge.SearchResult.ID>) {
        let urls = selectedResults(ids).compactMap { fileURL(for: $0) }
        QuickLookCoordinator.shared.show(urls: urls)
    }

    private func copyPath(ids: Set<NFSBridge.SearchResult.ID>) {
        let paths = selectedResults(ids).map { fileURL(for: $0)?.path ?? $0.path }
        let pb = NSPasteboard.general
        pb.clearContents()
        pb.setString(paths.joined(separator: "\n"), forType: .string)
    }

    private func fileURL(for result: NFSBridge.SearchResult) -> URL? {
        let mountPoint = server.preferences.mountPoint
        let fullPath = mountPoint.hasSuffix("/") ? mountPoint + result.path : mountPoint + "/" + result.path
        return URL(fileURLWithPath: fullPath)
    }

    // MARK: - Display helpers

    private func iconName(for result: NFSBridge.SearchResult) -> String {
        if result.isDir { return "folder.fill" }
        let ext = (result.name as NSString).pathExtension.lowercased()
        switch ext {
        case "mov", "mp4", "m4v", "avi", "mkv", "r3d", "braw":
            return "film.fill"
        case "wav", "mp3", "aif", "aiff", "m4a", "flac":
            return "waveform"
        case "jpg", "jpeg", "png", "tif", "tiff", "raw", "cr2", "nef":
            return "photo.fill"
        case "psd", "ai":
            return "paintbrush.fill"
        case "prproj", "drp", "fcpxml", "aaf":
            return "film.stack.fill"
        case "cube", "lut", "look":
            return "swatchpalette.fill"
        case "pdf":
            return "doc.fill"
        case "txt", "md", "rtf":
            return "doc.text.fill"
        default:
            return "doc.fill"
        }
    }

    private func iconColor(for result: NFSBridge.SearchResult) -> Color {
        if result.isDir { return .blue }
        let ext = (result.name as NSString).pathExtension.lowercased()
        switch ext {
        case "mov", "mp4", "m4v", "avi", "mkv", "r3d", "braw":  return .purple
        case "wav", "mp3", "aif", "aiff", "m4a", "flac":         return .orange
        case "jpg", "jpeg", "png", "tif", "tiff", "raw":         return .pink
        case "cube", "lut", "look":                              return .green
        case "prproj", "drp", "fcpxml", "aaf":                   return .indigo
        default:                                                 return .secondary
        }
    }

    private func formatSize(_ bytes: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }
}

// MARK: - Visual effect background

struct VisualEffectBackground: NSViewRepresentable {
    func makeNSView(context: Context) -> NSVisualEffectView {
        let view = NSVisualEffectView()
        view.material = .underWindowBackground
        view.blendingMode = .behindWindow
        view.state = .active
        return view
    }
    func updateNSView(_ nsView: NSVisualEffectView, context: Context) {}
}
