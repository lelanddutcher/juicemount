import SwiftUI

/// Separate from mounted Search Files. Discovery labels are metadata only;
/// every Receive action works from a deliberately chosen exact revision.
struct NativeLibraryWindowView: View {
    @ObservedObject var model: NativeLibraryModel
    @State private var endpoint = ""
    @State private var libraryID = ""
    @State private var bearer = ""
    @State private var allowLoopbackHTTP = false
    @State private var searchText = ""
    @State private var basename = ""
    @State private var chosen: NativeLibraryModel.Choice?

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Mac Library").font(.largeTitle.bold())
            Text("Choose a remote revision and keep a verified, named copy on this Mac.")
                .foregroundStyle(.secondary)
            profileControls
            if model.activeProfileID != nil {
                Divider()
                discoveryControls
                Divider()
                copyControls
            }
            Spacer(minLength: 0)
            HStack {
                if model.busy { ProgressView().controlSize(.small) }
                Text(model.message).font(.caption).foregroundStyle(.secondary)
                    .accessibilityIdentifier("nativeLibraryStatus")
                Spacer()
            }
        }
        .padding(22)
        .frame(minWidth: 740, minHeight: 600)
        .onChange(of: model.activeProfileID) { _, _ in chosen = nil }
    }

    private var profileControls: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Connection").font(.headline)
            HStack {
                TextField("https://manager.example or explicit loopback URL", text: $endpoint)
                    .textFieldStyle(.roundedBorder)
                    .accessibilityIdentifier("nativeEndpoint")
                TextField("lib_…", text: $libraryID)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 285)
                    .accessibilityIdentifier("nativeLibraryID")
            }
            HStack {
                SecureField("Delivery bearer", text: $bearer)
                    .textFieldStyle(.roundedBorder)
                    .accessibilityIdentifier("nativeBearer")
                Button("Create profile") {
                    let secret = bearer
                    bearer = ""
                    Task { await model.createProfile(endpoint: endpoint, libraryID: libraryID,
                             allowLoopbackHTTP: allowLoopbackHTTP, bearer: secret) }
                }
                .disabled(model.busy || endpoint.isEmpty || libraryID.isEmpty || bearer.isEmpty)
                .accessibilityIdentifier("nativeCreateProfile")
                if model.activeProfileID != nil {
                    Button("Save credential") {
                        let secret = bearer; bearer = ""
                        Task { await model.saveBearer(secret) }
                    }
                        .disabled(model.busy || bearer.isEmpty)
                    Button("Remove credential") { Task { await model.removeBearer() } }.disabled(model.busy)
                }
            }
            Toggle("Allow numeric loopback HTTP (development only)", isOn: $allowLoopbackHTTP)
                .font(.caption)
            HStack {
                Picker("Profile", selection: Binding(
                    get: { model.activeProfileID ?? "" },
                    set: { id in Task { await model.selectProfile(id) } })) {
                    Text("Disconnected").tag("")
                    ForEach(model.profiles) { entry in
                        Text(entry.created ? "\(entry.libraryID) · \(entry.endpoint)" : "Setup pending · \(entry.libraryID)")
                            .tag(entry.profileID)
                    }
                }
                .frame(maxWidth: .infinity)
                Button("Disconnect") { Task { await model.disconnect() } }
                    .disabled(model.activeProfileID == nil)
            }
            ForEach(model.profiles.filter { !$0.created }) { entry in
                HStack {
                    Text("Saved setup pending: \(entry.libraryID) · \(entry.endpoint)")
                        .font(.caption)
                    Spacer()
                    Button("Recover exact profile") {
                        let secret = bearer; bearer = ""
                        Task { await model.recoverProfile(entry.profileID, bearer: secret) }
                    }
                    .disabled(model.busy)
                }
            }
        }
    }

    private var discoveryControls: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Discover remote files").font(.headline)
            HStack {
                TextField("Name or stable ID", text: $searchText)
                    .textFieldStyle(.roundedBorder)
                    .onSubmit { Task { await model.search(searchText) } }
                    .accessibilityIdentifier("nativeSearchText")
                Button("Search") { Task { await model.search(searchText) } }
                    .disabled(model.busy)
                    .accessibilityIdentifier("nativeSearch")
                if model.page?.nextPageToken != nil {
                    Button("Next page") { Task { await model.nextPage() } }
                        .disabled(model.busy)
                        .accessibilityIdentifier("nativeNextPage")
                }
            }
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 6) {
                    ForEach(model.page?.items ?? [], id: \.assetID) { asset in
                        HStack {
                            VStack(alignment: .leading) {
                                Text(asset.label.isEmpty ? "Untitled asset" : asset.label)
                                Text("\(asset.assetID) · \(asset.headRevisionID ?? "No readable head")")
                                    .font(.caption.monospaced()).foregroundStyle(.secondary)
                            }
                            Spacer()
                            Button("Choose exact revision") {
                                chosen = model.choice(for: asset)
                                basename = NativeLibraryWire.validBasename(asset.label) ? asset.label : ""
                            }
                            .disabled(asset.headRevisionID == nil || model.busy)
                        }
                        .padding(.vertical, 3)
                    }
                }
            }
            .frame(maxHeight: 180)
            if let chosen {
                HStack {
                    Text("Exact pin: \(chosen.asset.assetID) / \(chosen.asset.headRevisionID ?? "")")
                        .font(.caption.monospaced())
                    TextField("Approved output basename", text: $basename)
                        .textFieldStyle(.roundedBorder)
                        .accessibilityIdentifier("nativeBasename")
                    Button("Pin this revision") {
                        Task { await model.pin(choice: chosen, basename: basename) }
                    }
                    .disabled(model.busy || !NativeLibraryWire.validBasename(basename))
                    .accessibilityIdentifier("nativePin")
                }
            }
        }
    }

    private var copyControls: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Retained local copies").font(.headline)
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 8) {
                    ForEach(model.copies) { copy in
                        VStack(alignment: .leading, spacing: 4) {
                            Text("\(copy.basename) · \(copy.copyID)")
                                .font(.body.monospaced())
                            Text("Exact revision \(copy.revisionID) · operation \(copy.operationID)")
                                .font(.caption.monospaced()).foregroundStyle(.secondary)
                            HStack {
                                if copy.selectReceipt == nil {
                                    Button("Retry original Select") { Task { await model.retrySelect(copyID: copy.copyID) } }
                                        .disabled(model.busy)
                                } else {
                                    Button("Receive") { Task { await model.receive(copyID: copy.copyID) } }
                                        .disabled(model.busy)
                                    Button("Recover prepared") { Task { await model.recoverPrepared(copyID: copy.copyID) } }
                                        .disabled(model.busy)
                                    Button("Check local copy") { Task { await model.checkLocal(copyID: copy.copyID) } }
                                        .disabled(model.busy)
                                    Button("Reveal in Finder") { Task { await model.reveal(copyID: copy.copyID, openFile: false) } }
                                        .disabled(model.busy || !model.verifiedCopies.contains(copy.copyID))
                                    Button("Open") { Task { await model.reveal(copyID: copy.copyID, openFile: true) } }
                                        .disabled(model.busy || !model.verifiedCopies.contains(copy.copyID))
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
