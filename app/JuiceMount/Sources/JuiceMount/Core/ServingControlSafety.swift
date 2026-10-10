import Foundation

/// Shared by the UI and the standalone failure fixtures. Transport loss never
/// grants permission to delete the service's database or claim offline success.
enum ServingControlSafety {
    static let resetUnavailable = "Cache reset is unavailable until the background service can verify that all writes are safe. Your cache has been kept."

    enum Failure: LocalizedError {
        case rejected(String)
        var errorDescription: String? { switch self { case .rejected(let reason): return reason } }
    }

    static func offlineAcknowledgement(_ raw: String) throws -> Bool {
        struct Reply: Decodable { let ok: Bool; let offline_mode: Bool }
        guard !raw.hasPrefix("error:") else { throw Failure.rejected(raw) }
        let reply = try JSONDecoder().decode(Reply.self, from: Data(raw.utf8))
        guard reply.ok else { throw Failure.rejected("The service did not confirm the offline change.") }
        return reply.offline_mode
    }

    static func deleteMetadataFiles(at path: String, serviceManaged: Bool, remove: (String) throws -> Void) throws {
        // The database contains durable write/spool records as well as a mirror.
        // A disconnected GUI, or even an idle service, is not deletion authority.
        guard !serviceManaged else { throw Failure.rejected(resetUnavailable) }
        for suffix in ["", "-wal", "-shm"] { try remove(path + suffix) }
    }
}
