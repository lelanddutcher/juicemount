import Foundation

/// Runs at most one piece of async work at a time; extra requests while one
/// is running are dropped, not queued. The Setup Assistant re-checks on every
/// focus change, so this keeps a burst of activations to one check.
/// Checked by Tests/RecheckSingleFlightProbe.swift.
@MainActor
final class SingleFlight {
    private(set) var running = false
    private(set) var started = 0

    @discardableResult
    func run(_ work: @escaping @MainActor () async -> Void) -> Bool {
        guard !running else { return false }
        running = true
        started += 1
        Task { @MainActor in
            await work()
            self.running = false
        }
        return true
    }
}
