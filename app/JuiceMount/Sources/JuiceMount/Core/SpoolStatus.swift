import Foundation

public extension NFSBridge {
    public struct SpoolEntryView: Codable, Equatable, Identifiable {
        public var path: String = ""
        public var size: Int64 = 0
        public var drainState: String = ""
        public var drainAttempts: Int = 0
        public var lastError: String?
        /// Scheduling disposition; the accepted save remains local and readable.
        public var replacementState: String?

        public var replacementNeedsAttention: Bool { replacementState == "writer_changed" }
        public var replacementIsWaiting: Bool { replacementState == "waiting_writer" }
        public var replacementStatusLabel: String? {
            switch replacementState {
            case "waiting_writer": return "Waiting for writer"
            case "writer_changed": return "Save needs review"
            case "recovery_disabled": return "Saved locally · recovery off"
            case "offline": return "Saved locally · offline"
            case "provider_unavailable": return "Saved locally · provider unavailable"
            case "queued": return "Saved locally · waiting to recover"
            case "finishing": return "Finishing save"
            default: return nil
            }
        }
        public var updatedAtUnix: Int64 = 0
        /// Seconds since the row last made progress (now - updated_at).
        public var ageSec: Int64 = 0
        /// True when the entry is making no progress and won't without
        /// intervention (LB-5): a leaked-handle `writing` row quiescent
        /// beyond the escalation window, or a row whose drain retries are
        /// exhausted. The popover offers "Recover stalled" for these.
        public var stalled: Bool = false

        // Identity for ForEach: path + last-update so a re-queued path
        // (same name, new generation) gets a fresh row rather than animating
        // a stale one.
        public var id: String { "\(path)|\(updatedAtUnix)" }

        enum CodingKeys: String, CodingKey {
            case path, size
            case drainState = "drain_state"
            case drainAttempts = "drain_attempts"
            case lastError = "last_error"
            case replacementState = "replacement_state"
            case updatedAtUnix = "updated_at_unix"
            case ageSec = "age_sec"
            case stalled
        }

        public init() {}

        /// Null/absence-tolerant decode — same discipline as SpoolStatus
        /// and CacheStatus below. Synthesized Codable would `decode` the
        /// non-optionals and abort the WHOLE /spool decode on a missing
        /// key (e.g. polling an older Go core that doesn't emit age_sec/
        /// stalled yet), silently blanking the spool UI.
        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.path = try c.decodeIfPresent(String.self, forKey: .path) ?? ""
            self.size = try c.decodeIfPresent(Int64.self, forKey: .size) ?? 0
            self.drainState = try c.decodeIfPresent(String.self, forKey: .drainState) ?? ""
            self.drainAttempts = try c.decodeIfPresent(Int.self, forKey: .drainAttempts) ?? 0
            self.lastError = try c.decodeIfPresent(String.self, forKey: .lastError)
            self.replacementState = try c.decodeIfPresent(String.self, forKey: .replacementState)
            self.updatedAtUnix = try c.decodeIfPresent(Int64.self, forKey: .updatedAtUnix) ?? 0
            self.ageSec = try c.decodeIfPresent(Int64.self, forKey: .ageSec) ?? 0
            self.stalled = try c.decodeIfPresent(Bool.self, forKey: .stalled) ?? false
        }
    }

    struct SpoolStatus: Codable, Equatable {
        public var enabled: Bool = false
        public var error: String?
        public var pendingFiles: Int = 0
        public var pendingSymlinks: Int = 0
        public var pendingBytes: Int64 = 0
        public var inProgress: Int64 = 0
        public var succeeded: Int64 = 0
        public var failed: Int64 = 0
        public var quarantined: Int64 = 0
        public var capacityUsed: Int64 = 0
        public var capacityTotal: Int64 = 0
        public var stalledFiles: Int = 0
        public var failedFiles: Int = 0
        public var declinedFiles: Int = 0
        public var replacementConflicts: Int = 0
        public var replacementWaiting: Int = 0
        public var recoveryEnabled: Bool?
        public var oldestPendingAgeSec: Int64 = 0
        public var entries: [SpoolEntryView] = []
        var requiredFieldsPresent = false
        var httpStatusCode: Int?

        public var hasActivity: Bool {
            pendingFiles > 0 || pendingSymlinks > 0 || inProgress > 0 || stalledFiles > 0
                || failedFiles > 0 || declinedFiles > 0
                || replacementConflicts > 0 || replacementWaiting > 0
        }

        public var safetyState: SpoolSafetyState {
            guard requiredFieldsPresent, httpStatusCode == 200, enabled,
                  error?.isEmpty ?? true else { return .unknown }
            return hasActivity ? .pending : .verifiedEmpty
        }

        /// Only a complete response from the expected /spool status branch
        /// establishes the live switch state. The disabled branch carries an
        /// intentional error string, so its error is not a decode failure.
        public var effectiveEnabledForCurrentResponse: Bool? {
            guard requiredFieldsPresent else { return nil }
            switch httpStatusCode {
            case 200 where enabled && (error?.isEmpty ?? true): return true
            case 503 where !enabled: return false
            default: return nil
            }
        }

        public var effectiveRecoveryEnabledForCurrentResponse: Bool? {
            guard requiredFieldsPresent, let recoveryEnabled else { return nil }
            switch httpStatusCode {
            case 200 where enabled && (error?.isEmpty ?? true): return recoveryEnabled
            case 503 where !enabled && !recoveryEnabled: return false
            default: return nil
            }
        }

        public var needsAttention: Bool {
            stalledFiles > 0 || failedFiles > 0 || declinedFiles > 0 || replacementConflicts > 0
        }

        enum CodingKeys: String, CodingKey {
            case enabled, error
            case pendingFiles = "pending_files"
            case pendingSymlinks = "pending_symlinks"
            case pendingBytes = "pending_bytes"
            case inProgress = "in_progress"
            case succeeded, failed, quarantined
            case capacityUsed = "capacity_used"
            case capacityTotal = "capacity_total"
            case stalledFiles = "stalled_files"
            case failedFiles = "failed_files"
            case declinedFiles = "declined_files"
            case replacementConflicts = "replacement_conflicts"
            case replacementWaiting = "replacement_waiting"
            case recoveryEnabled = "recovery_enabled"
            case oldestPendingAgeSec = "oldest_pending_age_sec"
            case entries
        }

        public init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            self.requiredFieldsPresent = try [CodingKeys.enabled, .pendingFiles, .pendingSymlinks,
                .pendingBytes, .inProgress, .stalledFiles, .failedFiles,
                .declinedFiles, .entries].allSatisfy { key in
                guard c.contains(key) else { return false }
                return try !c.decodeNil(forKey: key)
            }
            self.enabled = try c.decodeIfPresent(Bool.self, forKey: .enabled) ?? false
            self.error = try c.decodeIfPresent(String.self, forKey: .error)
            self.pendingFiles = try c.decodeIfPresent(Int.self, forKey: .pendingFiles) ?? 0
            self.pendingSymlinks = try c.decodeIfPresent(Int.self, forKey: .pendingSymlinks) ?? 0
            self.pendingBytes = try c.decodeIfPresent(Int64.self, forKey: .pendingBytes) ?? 0
            self.inProgress = try c.decodeIfPresent(Int64.self, forKey: .inProgress) ?? 0
            self.succeeded = try c.decodeIfPresent(Int64.self, forKey: .succeeded) ?? 0
            self.failed = try c.decodeIfPresent(Int64.self, forKey: .failed) ?? 0
            self.quarantined = try c.decodeIfPresent(Int64.self, forKey: .quarantined) ?? 0
            self.capacityUsed = try c.decodeIfPresent(Int64.self, forKey: .capacityUsed) ?? 0
            self.capacityTotal = try c.decodeIfPresent(Int64.self, forKey: .capacityTotal) ?? 0
            self.stalledFiles = try c.decodeIfPresent(Int.self, forKey: .stalledFiles) ?? 0
            self.failedFiles = try c.decodeIfPresent(Int.self, forKey: .failedFiles) ?? 0
            self.declinedFiles = try c.decodeIfPresent(Int.self, forKey: .declinedFiles) ?? 0
            self.replacementConflicts = try c.decodeIfPresent(Int.self, forKey: .replacementConflicts) ?? 0
            self.replacementWaiting = try c.decodeIfPresent(Int.self, forKey: .replacementWaiting) ?? 0
            self.recoveryEnabled = try c.decodeIfPresent(Bool.self, forKey: .recoveryEnabled)
            self.oldestPendingAgeSec = try c.decodeIfPresent(Int64.self, forKey: .oldestPendingAgeSec) ?? 0
            self.entries = try c.decodeIfPresent([SpoolEntryView].self, forKey: .entries) ?? []
        }
    }
}
