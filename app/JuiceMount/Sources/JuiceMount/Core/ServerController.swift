import Foundation
import Observation
import os.log

/// Owns the JuiceMount server lifecycle and exposes its state to SwiftUI.
/// All NFSBridge calls are dispatched to a background queue so the UI never blocks.
@MainActor
@Observable
public final class ServerController {

    public enum ServerState: Equatable {
        case idle               // not started
        case starting           // start in progress
        case running            // healthy
        case syncing            // sync in progress (shown briefly in icon)
        case degraded(String)   // running but a backend is unhealthy
        case disconnected       // FUSE down or NFS unmounted
        case error(String)      // start failed
    }

    public private(set) var state: ServerState = .idle
    public private(set) var stats: NFSBridge.Stats = .zero
    public private(set) var lastError: String?

    /// Latest result of the post-mount read self-test (Phase A2). Nil before
    /// the server runs its first probe; refreshed automatically after
    /// `start()` completes and again whenever the user invokes Sync Now.
    public private(set) var selfTest: NFSBridge.SelfTestResult?

    public var preferences: Preferences

    private let log = Logger(subsystem: "com.juicemount.app", category: "ServerController")
    private let workQueue = DispatchQueue(label: "com.juicemount.work", qos: .userInitiated)
    private var pollTask: Task<Void, Never>?

    public init(preferences: Preferences = Preferences.load()) {
        self.preferences = preferences
        // Reflect the actual Go-side state on launch
        if NFSBridge.isRunning {
            self.state = .running
        }
    }

    // MARK: - Lifecycle

    public func start() {
        guard case .idle = state else { return }
        state = .starting
        let cfg = preferences.toServerConfig()
        log.info("Starting server with mount \(cfg.mountPoint, privacy: .public)")
        workQueue.async { [weak self] in
            guard let self else { return }
            do {
                let addr = try NFSBridge.start(config: cfg)
                Task { @MainActor in
                    self.log.info("Server started at \(addr, privacy: .public)")
                    self.state = .running
                    self.lastError = nil
                    self.startPolling()
                    // Phase A2: pull the post-mount self-test result. The Go
                    // side runs the probe in a background goroutine at start,
                    // so a short delay lets the first run land in the cache
                    // before we fetch. If it's not ready yet, GET /self-test
                    // will run it synchronously on demand anyway.
                    self.refreshSelfTest(force: false, delayMs: 1500)
                }
            } catch {
                Task { @MainActor in
                    self.log.error("Start failed: \(error.localizedDescription, privacy: .public)")
                    self.state = .error(error.localizedDescription)
                    self.lastError = error.localizedDescription
                }
            }
        }
    }

    public func stop(completion: (@MainActor () -> Void)? = nil) {
        log.info("Stopping server (full unmount)")
        pollTask?.cancel()
        pollTask = nil
        workQueue.async { [weak self] in
            NFSBridge.stop()
            Task { @MainActor in
                self?.state = .idle
                self?.stats = .zero
                completion?()
            }
        }
    }

    /// Internal soft-stop used by `restart()`. Leaves FUSE + NFS mounted so
    /// the subsequent start is fast and prompt-free. Not exposed in the UI
    /// — the user-facing Stop button uses the hard `stop()` above.
    private func softStop(completion: (@MainActor () -> Void)? = nil) {
        log.info("Soft-stopping server (mounts preserved)")
        pollTask?.cancel()
        pollTask = nil
        workQueue.async { [weak self] in
            NFSBridge.softStop()
            Task { @MainActor in
                self?.state = .idle
                self?.stats = .zero
                completion?()
            }
        }
    }

    /// Restart with a proper completion handoff — waits for soft-stop to
    /// fully complete before kicking off start. Restart uses softStop so
    /// FUSE/NFS stay up between the teardown and the new start, avoiding
    /// a password prompt the user didn't ask for.
    public func restart() {
        softStop { [weak self] in
            self?.start()
        }
    }

    public func syncNow() {
        // Allow sync from any "running-like" state, not just .running.
        guard isRunningLike else { return }
        state = .syncing
        workQueue.async { [weak self] in
            do {
                _ = try NFSBridge.syncNow()
                Task { @MainActor in
                    // Don't restore the previous state — let the polling loop
                    // reconcile from fresh stats. This avoids suppressing real
                    // state transitions (e.g. degraded → disconnected) that may
                    // have happened during the sync.
                    self?.state = .running
                    self?.refreshStats()
                    // Intentionally NOT triggering refreshSelfTest here. Sync
                    // Now is supposed to be a lightweight metadata refresh.
                    // Auto-running a 10 MB read on the localhost NFS mount
                    // serializes with other mount ops and made the mount
                    // appear frozen during sync. Self-test runs once at
                    // launch (Go-side background goroutine) and rerunnable
                    // only via explicit user action (planned future button)
                    // or `curl -X POST http://127.0.0.1:11050/self-test`.
                }
            } catch {
                Task { @MainActor in
                    self?.lastError = error.localizedDescription
                    // Same idea — let the next poll tick determine real state
                    self?.state = .running
                }
            }
        }
    }

    /// Pulls the self-test result from the Go metrics endpoint and publishes
    /// it on the main actor. `force` issues a POST (rerun) instead of GET.
    /// `delayMs` lets the caller wait for an asynchronous first-run to land
    /// in the Go cache before fetching.
    public func refreshSelfTest(force: Bool = false, delayMs: Int = 0) {
        let addr = preferences.metricsAddr
        workQueue.async { [weak self] in
            if delayMs > 0 {
                Thread.sleep(forTimeInterval: Double(delayMs) / 1000.0)
            }
            let result = NFSBridge.selfTest(force: force, metricsAddr: addr)
            Task { @MainActor in
                self?.selfTest = result
            }
        }
    }

    private var isRunningLike: Bool {
        switch state {
        case .running, .syncing, .degraded: return true
        default: return false
        }
    }

    // MARK: - Search

    public func search(_ query: String, limit: Int = 50, parentPath: String = "") async -> [NFSBridge.SearchResult] {
        await withCheckedContinuation { continuation in
            workQueue.async {
                do {
                    let results = try NFSBridge.search(query, limit: limit, parentPath: parentPath)
                    continuation.resume(returning: results)
                } catch {
                    Task { @MainActor in
                        self.lastError = error.localizedDescription
                    }
                    continuation.resume(returning: [])
                }
            }
        }
    }

    // MARK: - Polling

    private func startPolling() {
        pollTask?.cancel()
        pollTask = Task { [weak self] in
            while !Task.isCancelled {
                self?.refreshStats()
                try? await Task.sleep(for: .seconds(2))
            }
        }
    }

    private func refreshStats() {
        workQueue.async { [weak self] in
            guard let self else { return }
            do {
                let s = try NFSBridge.stats()
                Task { @MainActor in
                    self.stats = s
                    self.updateStateFromStats(s)
                }
            } catch {
                // Bridge may be torn down — ignore
            }
        }
    }

    private func updateStateFromStats(_ s: NFSBridge.Stats) {
        guard s.running else {
            // Server stopped from underneath us. Only transition to .disconnected
            // when we previously believed we were .running — this preserves real
            // .error / .idle states. Future maintainers: be careful adding cases
            // here; sticky terminal states (.error, .idle) should NOT be silently
            // overwritten by a stale stats response.
            if case .running = state { state = .disconnected }
            return
        }
        if !s.healthFUSE {
            state = .disconnected
        } else if !s.healthRedis {
            state = .degraded("Redis unreachable — serving from cache")
        } else if !s.healthMinIO {
            state = .degraded("MinIO unreachable — reads may fail")
        } else {
            if case .syncing = state { return } // don't override transient sync state
            state = .running
        }
    }
}

// MARK: - State helpers for views

public extension ServerController.ServerState {
    var displayLabel: String {
        switch self {
        case .idle:                return "Idle"
        case .starting:            return "Starting…"
        case .running:             return "Connected"
        case .syncing:             return "Syncing…"
        case .degraded(let reason): return "Degraded — \(reason)"
        case .disconnected:        return "Disconnected"
        case .error(let msg):      return "Error — \(msg)"
        }
    }

    var iconName: String {
        switch self {
        case .idle:           return "circle"
        case .starting:       return "arrow.triangle.2.circlepath"
        case .running:        return "circle.fill"
        case .syncing:        return "arrow.triangle.2.circlepath.circle"
        case .degraded:       return "exclamationmark.circle.fill"
        case .disconnected:   return "xmark.circle.fill"
        case .error:          return "xmark.octagon.fill"
        }
    }
}
