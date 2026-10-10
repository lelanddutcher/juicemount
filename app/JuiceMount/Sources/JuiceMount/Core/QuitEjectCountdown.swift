import Foundation

/// How one process relates to the JuiceMount volume. Only open files are
/// unsaved work that a force-eject could destroy; a shell whose working
/// directory is on the volume blocks the eject but loses nothing, so it never
/// counts toward the force.
enum VolumeHoldKind: Equatable {
    /// At least one open file descriptor on the volume, or a mapped file
    /// (an app reading media through mmap). Counts toward the force.
    case openFiles
    /// Only the working (or root) directory is on the volume.
    case workingDirectoryOnly

    /// - fileOnVolume: a vnode descriptor of the process is on the volume.
    /// - cwdOnVolume: its current or root directory is on the volume.
    /// A process libproc listed with neither is holding the volume another
    /// way (a mapped file); treat it as open files, never as harmless.
    static func classify(fileOnVolume: Bool, cwdOnVolume: Bool) -> VolumeHoldKind {
        if !fileOnVolume && cwdOnVolume { return .workingDirectoryOnly }
        return .openFiles
    }
}

/// What Quit does with the holder list it found.
enum QuitHolderPlan: Equatable {
    /// Nobody holds the volume (or the count is unknown): ordinary shutdown.
    case proceed
    /// Apps hold open files: show the warning/countdown panel.
    case warnOpenFiles(fileApps: [String], folderApps: [String])
    /// Only working directories: explain, never force.
    case explainFoldersOnly(folderApps: [String])

    static func decide(fileApps: [String]?, folderApps: [String]?) -> QuitHolderPlan {
        guard let fileApps, let folderApps else { return .proceed }
        if !fileApps.isEmpty { return .warnOpenFiles(fileApps: fileApps, folderApps: folderApps) }
        if !folderApps.isEmpty { return .explainFoldersOnly(folderApps: folderApps) }
        return .proceed
    }

    static func folderMessage(_ apps: [String]) -> String {
        let names = apps.joined(separator: ", ")
        let verb = apps.count == 1 ? "is" : "are"
        return "\(names) \(verb) in a folder on the volume. JuiceMount will not force-eject for that; change to a folder outside the volume (for example `cd ~`) and retry."
    }
}

/// Founder decision (0.5): Quit while other apps hold files open on the volume
/// warns first, then force-ejects after 30 s unless the user cancels. Until
/// the privileged helper exists (`forceAvailable == false`) the same panel
/// tells the truth instead: no countdown, no promise of a force, and the user
/// either closes the files (detected by recount) or chooses Quit anyway.
///
/// Pure decision logic, checked by Tests/QuitEjectCountdownProbe.swift.
/// QuitCountdownDriver supplies ticks and recounts.
struct QuitEjectCountdown: Equatable {
    static let seconds = 30

    enum Trigger: Equatable {
        case userChoseEjectNow
        case timerExpired
    }

    enum Phase: Equatable {
        case counting(remaining: Int)
        /// Terminal: ask the helper to force-eject, then run the shutdown.
        case ejectRequested(Trigger)
        /// Terminal: every holder closed its files; an ordinary shutdown
        /// needs no force.
        case holdersReleased
        /// Terminal (force unavailable only): the user chose to try the
        /// ordinary shutdown anyway. Nothing is forced.
        case quitAnywayRequested
        /// Terminal: the user kept JuiceMount running.
        case cancelled
    }

    enum Event: Equatable {
        case tick
        case ejectNow
        case quitAnyway
        case cancelQuit
        /// A fresh count of apps with OPEN FILES. Pass nil when the recount
        /// failed or timed out: an unknown count never releases the panel.
        case holdersCounted(Int?)
    }

    let forceAvailable: Bool
    private(set) var phase: Phase
    private(set) var holderApps: Int

    /// Nil when nobody holds files: Quit needs no warning at all.
    init?(holderApps: Int, forceAvailable: Bool) {
        guard holderApps > 0 else { return nil }
        self.holderApps = holderApps
        self.forceAvailable = forceAvailable
        self.phase = .counting(remaining: Self.seconds)
    }

    var isTerminal: Bool {
        if case .counting = phase { return false }
        return true
    }

    /// Apply one event. Terminal phases ignore every later event, so a late
    /// timer tick or a double click can never request a second eject or turn
    /// a cancelled quit back into a force.
    mutating func handle(_ event: Event) {
        guard case .counting(let remaining) = phase else { return }
        switch event {
        case .tick:
            // Without a helper there is nothing to count down to.
            guard forceAvailable else { return }
            let next = remaining - 1
            phase = next <= 0 ? .ejectRequested(.timerExpired) : .counting(remaining: next)
        case .ejectNow:
            guard forceAvailable else { return }
            phase = .ejectRequested(.userChoseEjectNow)
        case .quitAnyway:
            guard !forceAvailable else { return }
            phase = .quitAnywayRequested
        case .cancelQuit:
            phase = .cancelled
        case .holdersCounted(let count):
            guard let count else { return }
            if count <= 0 {
                phase = .holdersReleased
            } else {
                holderApps = count
            }
        }
    }

    /// The warning text, with the live count and remaining seconds.
    var message: String {
        let remaining: Int
        if case .counting(let r) = phase { remaining = r } else { remaining = 0 }
        return Self.message(holderApps: holderApps, remaining: remaining, forceAvailable: forceAvailable)
    }

    static func message(holderApps: Int, remaining: Int, forceAvailable: Bool) -> String {
        let subject = holderApps == 1 ? "1 app still has" : "\(holderApps) apps still have"
        if forceAvailable {
            return "\(subject) files open on the volume; JuiceMount will force-eject in \(remaining) s; unsaved work in those apps may be lost."
        }
        return "\(subject) files open on the volume. JuiceMount can't force-eject yet; close the files in these apps, or choose Quit anyway."
    }
}
