import Foundation
import Observation
import os.log
import UserNotifications

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

    /// Latest cache status (pin coverage + offline flag). Refreshed off
    /// MainActor from `refreshCacheStatus()` so the popover never calls
    /// the cgo `NFSServerCacheStatus()` symbol from the UI thread —
    /// every 2 s blocking-on-cgo from MainActor was a freeze waiting to
    /// happen if any future regression parks a Go-side lock.
    public private(set) var cacheStatus: NFSBridge.CacheStatus = NFSBridge.CacheStatus()

    /// Latest result of the post-mount read self-test (Phase A2). Nil before
    /// the server runs its first probe; refreshed automatically after
    /// `start()` completes and again whenever the user invokes Sync Now.
    public private(set) var selfTest: NFSBridge.SelfTestResult?

    /// Latest offline state (iter 5 of the offline-resilience plan).
    /// Refreshed off MainActor by `refreshCacheStatus()` because the
    /// HTTP GET /offline call is blocking. Two consumers care:
    ///   - MenuBarController renders a distinct BLUE icon dot when
    ///     auto-offline is engaged (not the yellow/red used for
    ///     self-test attention-worthy states).
    ///   - MenuPopoverView header surfaces "Offline · N pinned ·
    ///     disconnected for M:SS" so the user understands WHY their
    ///     un-pinned files are refused.
    public private(set) var offlineState: NFSBridge.OfflineState = NFSBridge.OfflineState()

    /// Latest write-spool status (Option 2). Nil until the first fetch, or
    /// when the metrics server is unreachable. The popover's "Pending
    /// uploads" section reads this and renders only when `.enabled`.
    public private(set) var spoolStatus: NFSBridge.SpoolStatus?

    /// LB-2 state honesty: false when the server is running but the
    /// user-visible NFS volume is NOT in the kernel mount table (the
    /// health monitor's "nfs" component reports "not mounted" — see
    /// health/monitor.go checkNFS). The UI surfaces "Volume not mounted"
    /// + a Mount Now button off this instead of pretending all is well.
    /// Defaults true and keeps last-known on probe failure so a flaky
    /// metrics fetch can't flash a false "not mounted".
    public private(set) var volumeMounted: Bool = true

    /// True while a /mount-now round-trip is in flight (it can sit on the
    /// macOS admin-password prompt). Drives the Mount Now button spinner
    /// and double-click guard.
    public private(set) var mountNowInFlight = false

    public var preferences: Preferences

    private let log = Logger(subsystem: "com.juicemount.app", category: "ServerController")
    private let workQueue = DispatchQueue(label: "com.juicemount.work", qos: .userInitiated)
    private var pollTask: Task<Void, Never>?

    /// Heartbeat: updated at the top of every poll-loop iteration. The recovery
    /// watchdog uses it to detect a stalled/dead poll loop.
    private var lastPollTickAt = Date()

    /// Independent recovery path (2026-06-01). The main `workQueue` is SERIAL,
    /// so a single hung cgo call — e.g. a parked Go-side lock under disk-full /
    /// FUSE-wedge pressure — wedges it, which freezes the stats path, the
    /// in-loop `runStuckStateBackstop`, AND offline refresh all at once. That
    /// leaves the menu bar stuck on "Disconnected"/offline even though the Go
    /// HTTP `/health` endpoint is still healthy and responding. (The cure used
    /// to be: quit and relaunch.) This Timer + dedicated queue run the recovery
    /// probe over HTTP, independent of `workQueue`, so the UI can always climb
    /// back out. A Timer (not a Task) is used deliberately: it can't be
    /// silently cancelled/suspended along with the poll loop.
    private let recoveryQueue = DispatchQueue(label: "com.juicemount.recovery", qos: .utility)
    private var recoveryTimer: Timer?

    /// Set when the user explicitly stops the server (Stop / Stop Mount) or a
    /// restart soft-stops it; cleared on start(). The recovery watchdog respects
    /// it so it never resurrects a server that's intentionally down.
    private var userStopRequested = false

    /// True once the first offline-state fetch has completed. Used to
    /// suppress the transition notification on the very first refresh
    /// — without this, an app launch into a wake-from-sleep-offline
    /// state would fire a banner the user didn't trigger.
    private var hasCompletedInitialOfflineFetch = false

    public init(preferences: Preferences = Preferences.load()) {
        self.preferences = preferences
        // Reflect the actual Go-side state on launch
        if NFSBridge.isRunning {
            self.state = .running
            // QA-24 fix (2026-05-19): when the app launches while the Go
            // daemon is ALREADY running (re-launch with daemon still alive),
            // App.swift skips server.start() — which used to be the only
            // call site for startPolling(). The polling task never started,
            // so the state machine couldn't transition back from
            // .disconnected to .running on network recovery. The header
            // would say "Disconnected" forever while the four health dots
            // all read green (stats was being read on UI events, but
            // updateStateFromStats was never called to consume them).
            //
            // Move polling kickoff to wherever we believe the server is
            // running, not just to the start() path.
            startPolling()
        }
        // Always run the recovery watchdog from launch — even if the server
        // isn't started yet, or a later start() fails — so a stuck .idle/.error
        // can never become permanent. It no-ops unless a fresh /health probe
        // proves the backend is actually up.
        startRecoveryWatchdog()
    }

    // MARK: - Lifecycle

    public func start() {
        guard case .idle = state else { return }
        state = .starting
        userStopRequested = false
        let cfg = preferences.toServerConfig()
        // Spool (Option 2) settings travel in the config JSON (cfg.spoolEnable
        // / spoolSizeGB), NOT via env: Go snapshots os.Environ at c-archive
        // init, so a host-side setenv() after that is invisible to os.Getenv.
        log.info("Starting server with mount \(cfg.mountPoint, privacy: .public) (spool=\(self.preferences.spoolEnabled, privacy: .public))")
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
                    // LB-2/S-1: a failed start must be ACTIONABLE, not a
                    // silent slide into idle/error. The menu-bar fault
                    // icon makes it visible; this makes it explainable
                    // (cause heuristics + concrete next step + copyable
                    // diagnostic).
                    presentRemediation(.startFailed, rawError: error.localizedDescription)
                }
            }
        }
    }

    public func stop(completion: (@MainActor () -> Void)? = nil) {
        log.info("Stopping server (full unmount)")
        userStopRequested = true
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

    /// Middle-ground stop (QA-7, 2026-05-17): unmount NFS + tear down
    /// the server, but leave FUSE/JuiceFS alive so the subsequent
    /// Start avoids the admin-password re-prompt. /Volumes/<name>
    /// disappears from Finder; the backend stays warm.
    public func stopMount(completion: (@MainActor () -> Void)? = nil) {
        log.info("Stopping mount + draining sync (FUSE stays alive)")
        userStopRequested = true
        pollTask?.cancel()
        pollTask = nil
        workQueue.async { [weak self] in
            NFSBridge.stopMount()
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
        userStopRequested = true
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

    /// Fetch cache status from Go and publish on MainActor. The cgo call
    /// happens on `workQueue`, never on the UI thread. Safe to call at any
    /// cadence (e.g. the popover's 2 s timer).
    ///
    /// Also refreshes the offline state in the same dispatch — that's
    /// what the UI consumes for the menu-bar blue dot and the popover
    /// "Offline · …" header. Doing both in one workQueue tick keeps the
    /// two values consistent on every render frame.
    public func refreshCacheStatus() {
        let metricsAddr = preferences.metricsAddr
        workQueue.async { [weak self] in
            let s = NFSBridge.cacheStatus()
            // Fetch offline state — if the metrics server is
            // unreachable, the helper returns nil. Critically, we do
            // NOT substitute the zero value here: that would make the
            // UI report "online" exactly when the metrics path is
            // broken — the opposite of the truth in many failure
            // modes. Pass the optional through and let the MainActor
            // side decide whether to apply or keep last-known state.
            let o: NFSBridge.OfflineState? = NFSBridge.offlineState(metricsAddr: metricsAddr)
            let sp: NFSBridge.SpoolStatus? = NFSBridge.spoolStatus(metricsAddr: metricsAddr)
            // LB-2: piggyback the NFS-mounted signal on the same tick —
            // /health is a cached-snapshot read on the Go side (the
            // monitor's 10 s loop does the actual probing), so this adds
            // one loopback GET, no new polling loop and nothing on an NFS
            // hot path.
            let hp: NFSBridge.HealthProbe? = NFSBridge.healthProbe(metricsAddr: metricsAddr)
            Task { @MainActor in
                guard let self else { return }
                // Observe Swift-side offline_mode transitions. This is the exact
                // value the offline toggle reads; logging its transitions makes
                // the cgo→Swift cache-status read path visible (the roots:null
                // decode bug silently pinned it to false whenever nothing was
                // pinned). Low noise — fires only on an actual change.
                let prevOfflineMode = self.cacheStatus.offline_mode
                self.cacheStatus = s
                if s.offline_mode != prevOfflineMode {
                    NFSBridge.appLog("offline_mode (swift decoded) \(prevOfflineMode) -> \(s.offline_mode), roots=\(s.roots.count)")
                }
                // Spool status: keep last-known on a nil (unreachable) fetch,
                // same rationale as offlineState below.
                if let sp { self.spoolStatus = sp }

                // NFS-mounted (LB-2). Only the monitor's explicit
                // "not mounted" verdict flips this off — "stale"/"
                // unresponsive" are a different condition (wedged mount,
                // remedied by Force Eject, not Mount Now). Keep last-known
                // when the probe is unreachable.
                if let hp {
                    let prevMounted = self.volumeMounted
                    let nfsLabel = hp.components["nfs"] ?? ""
                    self.volumeMounted = !nfsLabel.contains("not mounted")
                    if prevMounted != self.volumeMounted {
                        NFSBridge.appLog("volumeMounted \(prevMounted) -> \(self.volumeMounted) (nfs=\(nfsLabel))")
                    }
                }

                if let o {
                    let prevAuto = self.offlineState.auto_offline
                    self.offlineState = o
                    // Notification edges. Suppressed on the very first
                    // fetch so an app launch into wake-from-sleep-
                    // offline doesn't fire an unexpected banner.
                    // Opt-in via preferences (VISION non-negotiable:
                    // no notifications without opt-in).
                    if self.hasCompletedInitialOfflineFetch &&
                       prevAuto != o.auto_offline {
                        self.notifyOfflineTransition(autoEngaged: o.auto_offline, reason: o.reason)
                    }
                    self.hasCompletedInitialOfflineFetch = true
                }
                // If o is nil: leave offlineState as-is (stale rather
                // than misleadingly reset to "online"). Next successful
                // fetch will reconverge.
            }
        }
    }

    /// Toggle the Go-side offline flag, then refresh cache status. Both
    /// cgo calls run on `workQueue` so the UI toggle doesn't freeze the
    /// popover under contention.
    public func setOffline(_ on: Bool) {
        // OPTIMISTIC, synchronous UI update (2026-06-02 — fixes the
        // write-only-ON bug). The popover's offline Toggle binds its `get` to
        // cacheStatus.offline_mode. If we only republish that after the async
        // cgo round-trip, SwiftUI re-reads the STALE (pre-tap) value the instant
        // after the user's tap, snaps the switch back ON, and re-fires the
        // binding — so an "off" tap never reaches NFSServerSetOffline (the log
        // showed `on:true` ×N, `on:false` ×0; user_offline latched on forever).
        // Reflecting the user's intent here, synchronously on MainActor, makes
        // the control AND every offline indicator update immediately and breaks
        // the feedback loop. The async block reconciles against the real flag.
        cacheStatus.offline_mode = on
        offlineState.offline = on
        offlineState.user_offline = on
        let metricsAddr = preferences.metricsAddr
        workQueue.async { [weak self] in
            NFSBridge.appLog("user setOffline(\(on))")
            NFSBridge.setOffline(on)
            let s = NFSBridge.cacheStatus()
            // See refreshCacheStatus for the nil-state rationale: keep
            // stale rather than reset to "online" on metrics failure.
            let o: NFSBridge.OfflineState? = NFSBridge.offlineState(metricsAddr: metricsAddr)
            Task { @MainActor in
                self?.cacheStatus = s
                if let o {
                    self?.offlineState = o
                }
            }
        }
    }

    /// LB-2 "Mount Now": ask the control plane to re-run the NFS mount
    /// for the configured mount point. Runs on a global queue — NEVER the
    /// serial workQueue — because the Go side may block inside the macOS
    /// admin-password prompt for as long as the user leaves it up, and
    /// parking workQueue would freeze stats/cache polling (the exact
    /// failure mode the recovery watchdog exists for).
    public func mountNow() {
        guard !mountNowInFlight else { return }
        mountNowInFlight = true
        let addr = preferences.metricsAddr
        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let result = NFSBridge.mountNow(metricsAddr: addr)
            Task { @MainActor in
                guard let self else { return }
                self.mountNowInFlight = false
                if let result, result.ok {
                    NFSBridge.appLog("mount-now ok (already_mounted=\(result.alreadyMounted))")
                    self.volumeMounted = true
                    self.refreshCacheStatus()
                } else {
                    let raw = result?.error ?? "control plane unreachable — is the server running?"
                    NFSBridge.appLog("mount-now failed: \(raw)")
                    presentRemediation(.mountFailed, rawError: raw)
                }
            }
        }
    }

    /// Posts a user notification when auto-offline mode transitions.
    /// Opt-in via `preferences.offlineNotificationsEnabled`; defaults
    /// to false to honor the VISION "no telemetry without opt-in" rule
    /// (notifications aren't telemetry but the user-attention budget
    /// is precious — surface only when the user asked for it).
    @MainActor
    private func notifyOfflineTransition(autoEngaged: Bool, reason: String) {
        guard preferences.offlineNotificationsEnabled else { return }
        let content = UNMutableNotificationContent()
        if autoEngaged {
            content.title = "JuiceMount: offline mode engaged"
            content.body = reason.isEmpty
                ? "Network path to backend lost — pinned files still available."
                : "\(reason) — pinned files still available."
        } else {
            content.title = "JuiceMount: back online"
            content.body = "Network path to backend restored."
        }
        content.sound = nil // silent; this isn't an alarm
        let req = UNNotificationRequest(
            identifier: "com.juicemount.offline.transition",
            content: content,
            trigger: nil
        )
        UNUserNotificationCenter.current().add(req) { err in
            if let err {
                Logger(subsystem: "com.juicemount.app", category: "Notify")
                    .debug("offline-transition notification failed: \(err.localizedDescription)")
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
        lastPollTickAt = Date()
        startRecoveryWatchdog()
        pollTask = Task { [weak self] in
            var tick = 0
            while !Task.isCancelled {
                self?.lastPollTickAt = Date()   // heartbeat for the recovery watchdog
                self?.refreshStats()
                // QA-23 fix (2026-05-18): the offline/cache state was only
                // refreshing on UI events (popover open, toggle click). After
                // auto-offline recovered the header banner "Offline · Xm"
                // stayed stuck because offlineState was never re-read.
                //
                // QA-26 (2026-05-21): bumped cadence from every-5th (10s) to
                // every-2nd (4s). The semi-stale window after a second wifi-
                // recovery was on the order of minutes; cutting the worst-case
                // re-converge time matters more than the extra HTTP calls
                // (loopback, ~ms each).
                tick &+= 1
                if tick % 2 == 0 {
                    self?.refreshCacheStatus()
                }
                // QA-26 backstop (2026-05-21): if the state machine has been
                // stuck in .disconnected/.degraded for ≥3 consecutive ticks
                // (~6s) AND the Go-side /health endpoint reports healthy,
                // force-transition to .running. This is independent of the
                // cgo Stats path and protects against any future stuck-state
                // failure mode where Stats keeps reporting unhealthy while
                // the Go monitor disagrees. Costs one extra HTTP call per
                // tick only while stuck — zero overhead in the healthy case.
                if let self {
                    self.runStuckStateBackstop()
                }
                try? await Task.sleep(for: .seconds(2))
            }
        }
    }

    /// QA-26: counter for consecutive ticks observed in a non-healthy state.
    /// Reset whenever updateStateFromStats transitions to .running/.syncing.
    private var stuckTicks: Int = 0

    /// QA-26 backstop. Runs every poll tick while polling is active.
    /// When stuckTicks ≥3 and state is .disconnected/.degraded, fires an
    /// HTTP /health probe (ephemeral session — independent of cgo Stats).
    /// If the probe says healthy, force-transition the state machine and
    /// log it loudly so future debugging can find this path.
    @MainActor
    private func runStuckStateBackstop() {
        let isStuck: Bool
        switch state {
        case .disconnected, .degraded:
            isStuck = true
        default:
            isStuck = false
        }
        if !isStuck {
            stuckTicks = 0
            return
        }
        stuckTicks &+= 1
        guard stuckTicks >= 3 else { return }

        let metricsAddr = preferences.metricsAddr
        workQueue.async { [weak self] in
            guard let probe = NFSBridge.healthProbe(metricsAddr: metricsAddr) else { return }
            guard probe.healthy else { return }
            Task { @MainActor in
                guard let self else { return }
                switch self.state {
                case .disconnected, .degraded:
                    self.log.warning("backstop: /health probe says healthy while state=\(self.state.displayLabel, privacy: .public) for \(self.stuckTicks) ticks — forcing transition to .running")
                    self.state = .running
                    self.stuckTicks = 0
                    // Refresh ancillary state too so the popover doesn't
                    // keep showing "Offline · disconnected M:SS" against
                    // an offline_state that's also stale.
                    self.refreshCacheStatus()
                default:
                    return
                }
            }
        }
    }

    /// Reliable, workQueue-independent recovery watchdog (2026-06-01). Fires on
    /// a Timer (not the poll Task) and probes `/health` over HTTP on a dedicated
    /// queue. If the UI is stuck in .disconnected/.degraded while the Go server
    /// reports healthy, it forces the state back to .running — even when the
    /// serial workQueue (and thus the in-loop backstop + stats path) is wedged.
    /// It also revives the poll loop if its heartbeat has gone stale. This is
    /// the backstop the QA-26 backstop needed: that one runs *inside* the poll
    /// loop, so if the loop wedges it dies with it.
    private func startRecoveryWatchdog() {
        guard recoveryTimer == nil else { return }
        recoveryTimer = Timer.scheduledTimer(withTimeInterval: 5.0, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.recoveryTick() }
        }
    }

    @MainActor
    private func recoveryTick() {
        // Respect an explicit user stop — never resurrect a server the user
        // deliberately stopped. Cleared on start().
        guard !userStopRequested else { return }

        let staleHeartbeat = Int(Date().timeIntervalSince(lastPollTickAt)) > 15
        let uiStuck: Bool
        switch state {
        case .running, .syncing: uiStuck = false   // good/transient — leave it
        default:                 uiStuck = true     // .idle/.starting/.error/.disconnected/.degraded
        }
        // Nothing to do if the loop is ticking AND the UI already looks running.
        guard staleHeartbeat || uiStuck else { return }

        // Probe /health on an INDEPENDENT queue (never the serial workQueue,
        // which may be wedged). If it doesn't come back healthy, the server is
        // genuinely down/stopped — do nothing, so a real .idle/.error is left
        // alone. If it IS healthy while the UI isn't running, the UI has
        // diverged from reality: a sticky .idle/.error that updateStateFromStats
        // refuses to overwrite, or a .disconnected the poll loop stopped
        // touching. Reconverge it — this is the case the previous watchdog
        // missed (it only covered .disconnected/.degraded).
        let metricsAddr = preferences.metricsAddr
        recoveryQueue.async { [weak self] in
            guard let probe = NFSBridge.healthProbe(metricsAddr: metricsAddr), probe.healthy else { return }
            Task { @MainActor in
                guard let self, !self.userStopRequested else { return }
                if Int(Date().timeIntervalSince(self.lastPollTickAt)) > 15 {
                    NFSBridge.appLog("recovery watchdog: poll loop heartbeat stale — restarting it")
                    self.startPolling()
                }
                switch self.state {
                case .running, .syncing:
                    break
                default:
                    NFSBridge.appLog("recovery watchdog: /health healthy but UI=\(self.state.displayLabel) — forcing .running")
                    self.log.warning("recovery watchdog: /health healthy but UI=\(self.state.displayLabel, privacy: .public) — forcing .running")
                    self.state = .running
                    self.stuckTicks = 0
                }
                self.refreshCacheStatus()
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

        // QA-24 (2026-05-19): explicit recovery path. If a previous poll
        // observed degraded/disconnected health and now the snapshot is
        // fully healthy, transition to .running. Previously this case was
        // implicit in the else branch below, but it was hard to reason
        // about because a single non-healthy intermediate poll would
        // ping-pong the state. Making the recovery explicit ALSO logs the
        // transition so future diagnosis has evidence.
        let fullyHealthy = s.healthFUSE && s.healthRedis && s.healthMinIO
        if fullyHealthy {
            switch state {
            case .running, .syncing:
                // Already in a good state; preserve .syncing (transient).
                if case .syncing = state { return }
                state = .running
                return
            case .disconnected, .degraded:
                // Recovery edge — log it. Future-me trying to debug a
                // "stuck disconnected" report will check os_log for this.
                log.info("recovery: backend healthy, transitioning \(self.state.displayLabel, privacy: .public) -> running")
                state = .running
                stuckTicks = 0
                return
            default:
                // .idle / .starting / .error are sticky; don't overwrite.
                return
            }
        }

        // Not fully healthy — pick the most-severe component as the cause.
        if !s.healthFUSE {
            state = .disconnected
        } else if !s.healthRedis {
            state = .degraded("Redis unreachable — serving from cache")
        } else if !s.healthMinIO {
            state = .degraded("MinIO unreachable — reads may fail")
        }
    }
}

// MARK: - Glance state (approved icon/state spec, 2026-06-10)

/// The four user-approved at-a-glance states (LAUNCH_PLAN "Approved
/// icon/state spec") plus the quiet not-started case. Both the menu-bar
/// icon (MenuBarController) and the popover header (MenuPopoverView)
/// derive their color/word from THIS mapping so they can never disagree.
///
///   healthy      — green (original logo palette)
///   degraded     — amber #EF9F27 (degraded / starting / recovering)
///   offlineFiles — blue  #378ADD (offline-files-only mode, user or auto)
///   fault        — red   #E24B4A (unreachable / start failed / FUSE down)
///   idle         — healthy mark at reduced alpha ("not started" reads
///                  quiet, not alarming)
public enum GlanceState: Equatable {
    case healthy
    case degraded
    case offlineFiles
    case fault
    case idle
}

public extension ServerController {
    /// Single source of truth for the 4-state UI mapping. Priority:
    ///
    ///  1. fault  — .error/.disconnected. A real fault trumps everything,
    ///     including offline mode: when FUSE/the server is down, even the
    ///     offline-pinned files aren't being served.
    ///  2. idle   — server deliberately not started. Checked before offline
    ///     because any offlineState held while idle is stale (the metrics
    ///     server that reports it is down).
    ///  3. offlineFiles — offline engaged (user toggle or auto). Trumps
    ///     degraded: an unreachable backend is the EXPECTED condition while
    ///     offline, and the VISION spec colors it blue so users learn it's
    ///     not a fault. (Same precedence the popover status dot has always
    ///     used.)
    ///  4. degraded — .degraded/.starting, or running with the volume NOT
    ///     mounted (LB-2: the server is fine but Finder has nothing — amber,
    ///     with "Mount Now" as the popover remedy).
    ///  5. healthy  — .running/.syncing with the volume mounted.
    var glanceState: GlanceState {
        switch state {
        case .error, .disconnected:
            return .fault
        case .idle:
            return .idle
        case .starting:
            return offlineState.offline ? .offlineFiles : .degraded
        case .degraded:
            if !volumeMounted { return .degraded }
            return offlineState.offline ? .offlineFiles : .degraded
        case .running, .syncing:
            if !volumeMounted { return .degraded }
            return offlineState.offline ? .offlineFiles : .healthy
        }
    }

    /// Short human word for the glance state — popover header + icon
    /// accessibility label share it.
    var glanceLabel: String {
        switch glanceState {
        case .healthy:      return "Healthy"
        case .degraded:
            if case .starting = state { return "Starting…" }
            if !volumeMounted { return "Volume not mounted" }
            return "Degraded"
        case .offlineFiles: return "Offline files mode"
        case .fault:        return state == .disconnected ? "Disconnected" : "Fault"
        case .idle:         return "Not started"
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
