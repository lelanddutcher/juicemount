import Foundation

/// Typed reason behind a shutdown that the engine reported as incomplete.
/// Raw values are the Go constants in bridge/cbridge.go (shutdownReason*),
/// forwarded by NFSServerLastShutdownOutcome. Pure, so the mapping is checked
/// by Tests/ShutdownOutcomeProbe.swift without a running engine.
enum ShutdownReason: Equatable {
    case complete
    case incomplete
    case fuseOrLinkRetained
    case fuseAdmissionPending
    case networkVolumesPermissionDenied
    /// The engine sent a reason this build does not know, or none at all.
    case unknown(String)

    init(raw: String) {
        switch raw {
        case "complete": self = .complete
        case "incomplete": self = .incomplete
        case "fuse_or_link_retained": self = .fuseOrLinkRetained
        case "fuse_admission_pending": self = .fuseAdmissionPending
        case "network_volumes_permission_denied": self = .networkVolumesPermissionDenied
        default: self = .unknown(raw)
        }
    }
}

struct ShutdownOutcome: Equatable {
    /// True only when the engine's C call returned nonzero. Never taken from
    /// the fetched JSON: a stale or racing outcome must not turn a failed
    /// shutdown into a successful quit.
    let complete: Bool
    let reason: ShutdownReason
    let detail: String

    static let completed = ShutdownOutcome(complete: true, reason: .complete, detail: "")

    /// Combine the authoritative C result with the fetched explanation.
    /// - cResult != 0: complete, whatever the JSON says.
    /// - cResult == 0: incomplete. The JSON supplies the reason only when it
    ///   also says incomplete; a fetched "complete" (or an unreadable reply)
    ///   contradicts the C result and degrades to the generic reason.
    static func resolve(cResult: Int32, fetchedJSON: String?) -> ShutdownOutcome {
        if cResult != 0 { return .completed }
        struct Wire: Decodable { let complete: Bool; let reason: String; let detail: String? }
        guard let raw = fetchedJSON, !raw.hasPrefix("error:"),
              let wire = try? JSONDecoder().decode(Wire.self, from: Data(raw.utf8)),
              !wire.complete else {
            return ShutdownOutcome(complete: false, reason: .incomplete, detail: "")
        }
        let reason = ShutdownReason(raw: wire.reason)
        if reason == .complete {
            return ShutdownOutcome(complete: false, reason: .incomplete, detail: wire.detail ?? "")
        }
        return ShutdownOutcome(complete: false, reason: reason, detail: wire.detail ?? "")
    }
}

/// User-visible wording for an incomplete shutdown.
struct ShutdownMessage: Equatable {
    let title: String
    let body: String
    /// True when the remedy is a macOS privacy setting the user can open.
    let offersPrivacySettings: Bool

    static let networkVolumesDenied = "macOS Network Volumes access was not granted"
    static let admissionPending = "Waiting for macOS to open the volume — a pending Network Volumes prompt or an unresponsive filesystem"

    static func forOutcome(_ outcome: ShutdownOutcome) -> ShutdownMessage {
        switch outcome.reason {
        case .networkVolumesPermissionDenied:
            return ShutdownMessage(
                title: networkVolumesDenied,
                body: "JuiceMount cannot release its cache volume without that access, so it kept the volume connected for a safe retry. Allow Network Volumes for JuiceMount in System Settings → Privacy & Security → Files & Folders, then retry.",
                offersPrivacySettings: true)
        case .fuseAdmissionPending:
            return ShutdownMessage(
                title: admissionPending,
                body: "If macOS is showing a permission prompt, answer it, then retry. If no prompt is visible, the filesystem is not responding yet; JuiceMount kept its resources open for a safe retry.",
                offersPrivacySettings: true)
        case .fuseOrLinkRetained:
            return ShutdownMessage(
                title: "JuiceMount could not finish disconnecting",
                body: "The cache filesystem or the JuiceMount Link connection is still in use. JuiceMount kept them open for a safe retry.",
                offersPrivacySettings: false)
        case .complete, .incomplete, .unknown:
            return ShutdownMessage(
                title: "JuiceMount could not finish disconnecting",
                body: "An upload, mount, or filesystem operation is still active. JuiceMount kept its resources open for a safe retry.",
                offersPrivacySettings: false)
        }
    }

    /// Short single-line status for the menu (ServerController.lastError).
    static func statusLine(_ outcome: ShutdownOutcome) -> String {
        switch outcome.reason {
        case .networkVolumesPermissionDenied:
            return "Shutdown is incomplete: \(networkVolumesDenied). Retry Stop after allowing it."
        case .fuseAdmissionPending:
            return "Shutdown is incomplete: waiting for macOS to open the volume (a pending Network Volumes prompt or an unresponsive filesystem). Retry Stop."
        case .fuseOrLinkRetained:
            return "Shutdown is incomplete: the cache filesystem or Link is still in use. Retry Stop before starting again."
        case .complete, .incomplete, .unknown:
            return "Shutdown is incomplete. Retry Stop before starting again."
        }
    }
}
