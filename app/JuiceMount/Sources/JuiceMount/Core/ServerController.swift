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

    /// Live JuiceMount Link pairing/route verification shown in Preferences.
    public private(set) var linkTestResult: NFSBridge.LinkTestResult?
    public private(set) var linkTestInFlight = false

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

    /// R-4 (start-while-offline): set to the reason string when the server
    /// booted with the backend unreachable (Go returned "started_offline:").
    /// Drives the "Started offline — showing cached state" banner. Cleared
    /// automatically once auto-offline lifts (the backend came back), in
    /// refreshCacheStatus — so the banner is self-dismissing on recovery.
    public private(set) var offlineStartupReason: String?

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

    /// #106: true when the Go health monitor reports a backend component as
    /// "local-network-permission" — the conservative signature of macOS
    /// having silently reset this app's Local Network privacy permission
    /// (it happens on every rebuild/re-sign/update: dials to the NAS fail
    /// EHOSTUNREACH while the network is otherwise fine). Drives the
    /// popover's "Local Network permission needed" remedy row. Keeps
    /// last-known on a failed probe, same policy as volumeMounted.
    public private(set) var localNetworkPermissionSuspected = false

    /// SSD cache size (GB) the live JuiceFS daemon is believed to have been
    /// launched with. `--cache-size` is minted when the daemon is spawned,
    /// so a Settings change does NOT reach a running mount. nil = no daemon
    /// launched this app session, or a full Stop killed it (the next Start
    /// mints fresh flags from preferences). Deliberately NOT reset by
    /// stopMount/restart() — those keep the JuiceFS daemon (and its original
    /// flags) alive, which is exactly why the Settings pane needs
    /// `cacheSizePendingRestart` below.
    public private(set) var appliedSsdCacheGB: Int?

    public var preferences: Preferences

    private let log = Logger(subsystem: "com.juicemount.app", category: "ServerController")

    /// Mutations and user operations (start/stop/mounts/sync/setOffline/search).
    /// A slow operation may legitimately park this queue for seconds — which
    /// is exactly why status reads must NEVER share it.
    private let workQueue = DispatchQueue(label: "com.juicemount.work", qos: .userInitiated)

    /// F2 (menu-bar truthfulness): dedicated SERIAL queue for status-critical
    /// reads — stats, cache/offline/spool status, /health probes. Because it
    /// never accepts mutations, a long mount or search can no longer delay
    /// the state machine's view of reality (the old single serial queue let
    /// one slow cgo op freeze every poll behind it → stuck "Starting…").
    private let pollQueue = DispatchQueue(label: "com.juicemount.poll", qos: .userInitiated)

    private var pollTask: Task<Void, Never>?

    /// F2 drop-if-pending/single-flight flags (all touched on MainActor
    /// only). A poll that finds its predecessor still executing is DROPPED
    /// instead of stacking onto the queue: a wedged cgo read then costs one
    /// slot, not an unbounded backlog of stale reads landing late.
    private var statsRefreshInFlight = false
    private var cacheRefreshInFlight = false
    private var selfTestRefreshInFlight = false
    private var backstopProbeInFlight = false

    /// F2 no-optimistic-flip: true from a successful NFSBridge.start() until
    /// the FIRST real health evidence lands (cgo Stats snapshot or /health
    /// probe). While true the machine holds `.starting` — it does not claim
    /// `.running` until something actual confirms the bridge is up.
    private var awaitingFirstHealth = false

    /// True while a Sync Now round-trip is in flight. Lets updateStateFromStats
    /// keep showing `.syncing` during the operation WITHOUT hardcoding
    /// `.running` afterwards — the post-sync state comes from fresh stats.
    private var syncInFlight = false

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
        // Cache size the daemon will be minted with IF this start spawns a
        // fresh JuiceFS daemon (see appliedSsdCacheGB). Captured on MainActor
        // before the hop — preferences is MainActor-isolated.
        let startCacheGB = preferences.ssdCacheGB
        // Spool (Option 2) settings travel in the config JSON (cfg.spoolEnable
        // / spoolSizeGB), NOT via env: Go snapshots os.Environ at c-archive
        // init, so a host-side setenv() after that is invisible to os.Getenv.
        log.info("Starting server with mount \(cfg.mountPoint, privacy: .public) (spool=\(self.preferences.spoolEnabled, privacy: .public))")
        workQueue.async { [weak self] in
            guard let self else { return }
            do {
                let addr = try NFSBridge.start(config: cfg)
                // R-4: the Go core hands back "started_offline: <reason>" when it
                // booted with the backend unreachable — the NFS share is mounted
                // and serving cached navigation, so this is a SUCCESS (.running),
                // not a start failure. Auto-offline is already engaged on the Go
                // side; the reason drives the "showing cached state" banner and
                // auto-clears when the backend returns.
                let offlinePrefix = "started_offline:"
                let startedOffline = addr.hasPrefix(offlinePrefix)
                Task { @MainActor in
                    // F2: a Stop (or restart's soft-stop) may have been
                    // requested while the start round-trip was in flight —
                    // never surface a state the user just cancelled.
                    guard !self.userStopRequested else {
                        self.log.info("start completed but a stop was requested meanwhile — ignoring")
                        return
                    }
                    if startedOffline {
                        let reason = String(addr.dropFirst(offlinePrefix.count)).trimmingCharacters(in: .whitespaces)
                        self.log.warning("Server started OFFLINE — \(reason, privacy: .public)")
                        self.offlineStartupReason = reason
                    } else {
                        self.log.info("Server started at \(addr, privacy: .public)")
                        self.offlineStartupReason = nil
                    }
                    // F2 no-optimistic-flip: stay in `.starting` until the
                    // poll loop's first REAL health sample confirms the
                    // bridge (updateStateFromStats / recovery watchdog own
                    // the transition to .running/.degraded/.disconnected).
                    self.state = .starting
                    self.awaitingFirstHealth = true
                    self.lastError = nil
                    // Record the daemon's minted cache size only when this
                    // start could have spawned it (nil = no live daemon).
                    // After a soft-stop/restart the daemon SURVIVED with its
                    // original flags, so the old value must stay — that's the
                    // signal Settings uses to show "pending restart".
                    if self.appliedSsdCacheGB == nil {
                        self.appliedSsdCacheGB = startCacheGB
                    }
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
                self?.awaitingFirstHealth = false
                self?.syncInFlight = false
                // Full stop kills the JuiceFS daemon — its minted flags die
                // with it, so the next start() records fresh ones.
                self?.appliedSsdCacheGB = nil
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
                self?.awaitingFirstHealth = false
                self?.syncInFlight = false
                completion?()
            }
        }
    }

    /// Restart with a proper completion handoff. NFS is briefly unmounted so
    /// the kernel cannot retain file handles whose handler owns the spool and
    /// SQLite stores we are about to close. JuiceFS/FUSE stays warm, so this is
    /// still the inexpensive restart path and does not rebuild the data cache.
    public func restart() {
        stopMount { [weak self] in
            self?.start()
        }
    }

    /// FULL restart — Stop everything → Start, with the same completion
    /// handoff as `restart()`. Unlike the soft restart above, this tears
    /// down the JuiceFS daemon too, so flags minted at daemon launch
    /// (--cache-size) are re-read from preferences on the way back up.
    /// This is the Settings "Restart Now" action for a pending SSD
    /// cache-size change. Never called silently — only from an explicit
    /// user confirmation.
    public func restartFull() {
        // Don't yank an in-flight start out from under itself.
        if case .starting = state { return }
        stop { [weak self] in
            self?.start()
        }
    }

    /// Apply the current pairing fields, join/refresh the embedded tailnet
    /// identity, and verify a real Redis dial through the advertised NAS route.
    /// If the bridge is already serving, a successful test performs a warm
    /// restart so the live mount begins using the retained Link node now.
    public func testLink() {
        guard !linkTestInFlight else { return }
        let serverURL = preferences.linkServerURL.trimmingCharacters(in: .whitespacesAndNewlines)
        let authKey = preferences.linkAuthKey.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !serverURL.isEmpty, !authKey.isEmpty else {
            linkTestResult = NFSBridge.LinkTestResult(
                ok: false, authorized: false, online: false, backendReachable: false,
                redisReachable: false, objectStoreReachable: false,
                hostname: nil, addresses: [], rttMS: nil,
                error: "Enter the server URL and pairing code first."
            )
            return
        }
        if preferences.linkHostname.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            let source = Host.current().localizedName ?? "juicemount-mac"
            let clean = source.lowercased().map { ch -> Character in
                (ch.isLetter || ch.isNumber || ch == "-") ? ch : "-"
            }
            preferences.linkHostname = String(clean).trimmingCharacters(in: CharacterSet(charactersIn: "-"))
        }
        let cfg = preferences.toServerConfig()
        let shouldRestart = isRunningLike
        linkTestInFlight = true
        linkTestResult = nil
        workQueue.async { [weak self] in
            let result: NFSBridge.LinkTestResult
            do {
                result = try NFSBridge.testLink(config: cfg)
            } catch {
                result = NFSBridge.LinkTestResult(
                    ok: false, authorized: false, online: false, backendReachable: false,
                    redisReachable: false, objectStoreReachable: false,
                    hostname: cfg.netHostname, addresses: [], rttMS: nil,
                    error: error.localizedDescription
                )
            }
            Task { @MainActor in
                guard let self else { return }
                self.linkTestInFlight = false
                self.linkTestResult = result
                if result.ok && shouldRestart {
                    self.restart()
                }
            }
        }
    }

    /// True when the user's saved SSD cache size differs from the size the
    /// live JuiceFS daemon was launched with. The setting is safely saved,
    /// but it cannot take effect until the volume fully restarts (the
    /// daemon re-mints --cache-size at launch). Drives the Settings prompt
    /// and its persistent "pending restart" badge. False when no daemon is
    /// up (nil applied) — the next start applies the new value anyway.
    public var cacheSizePendingRestart: Bool {
        guard let applied = appliedSsdCacheGB else { return false }
        return applied != preferences.ssdCacheGB
    }

    /// S-6 Reset-DB flow: close the stores and drop only the NFS mount. Keeping
    /// NFS mounted across this boundary leaves kernel file handles attached to
    /// the just-closed spool, so the next start appears healthy but all writes
    /// fail. FUSE remains warm and `completion` runs on MainActor.
    public func softStopForMaintenance(completion: (@MainActor () -> Void)? = nil) {
        stopMount(completion: completion)
    }

    public func syncNow() {
        // Allow sync from any "running-like" state, not just .running.
        guard isRunningLike else { return }
        state = .syncing
        syncInFlight = true
        workQueue.async { [weak self] in
            do {
                _ = try NFSBridge.syncNow()
                Task { @MainActor in
                    self?.syncInFlight = false
                    // F2: don't hardcode `.running` — reconcile from a fresh
                    // stats sample so a genuinely degraded/disconnected bridge
                    // says so the moment the sync ends. updateStateFromStats
                    // keeps showing .syncing only while syncInFlight is true,
                    // then transitions on the strength of real health.
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
                    self?.syncInFlight = false
                    self?.lastError = error.localizedDescription
                    // Same idea — let the next stats sample determine real state.
                    self?.refreshStats()
                }
            }
        }
    }

    /// Fetch cache status from Go and publish on MainActor. The cgo call
    /// happens on `pollQueue`, never on the UI thread. Safe to call at any
    /// cadence (e.g. the popover's 2 s timer).
    ///
    /// F2: runs on the dedicated status queue (never behind mutations) and
    /// is single-flight — an overlapping call while one is executing is
    /// dropped, so a slow cgo/HTTP read can't stack a backlog of stale
    /// publishes.
    ///
    /// Also refreshes the offline state in the same dispatch — that's
    /// what the UI consumes for the menu-bar blue dot and the popover
    /// "Offline · …" header. Doing both in one pollQueue tick keeps the
    /// two values consistent on every render frame.
    public func refreshCacheStatus() {
        guard !cacheRefreshInFlight else { return }
        cacheRefreshInFlight = true
        let metricsAddr = preferences.metricsAddr
        pollQueue.async { [weak self] in
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
                self.cacheRefreshInFlight = false
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

                    // #106: the Go monitor reports the distinct
                    // "local-network-permission" component status when a
                    // backend dial failure matches the macOS Local Network
                    // permission-denial signature (health/localnet.go — it
                    // never fires while genuinely offline). Piggybacks on
                    // the same /health snapshot; no new polling.
                    let prevPerm = self.localNetworkPermissionSuspected
                    self.localNetworkPermissionSuspected =
                        hp.components.values.contains { $0.contains("local-network-permission") }
                    if prevPerm != self.localNetworkPermissionSuspected {
                        NFSBridge.appLog("localNetworkPermissionSuspected \(prevPerm) -> \(self.localNetworkPermissionSuspected)")
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
                    // R-4: the backend came back (auto-offline lifted) — drop the
                    // "started offline" banner so it's self-dismissing on recovery.
                    if !o.auto_offline && self.offlineStartupReason != nil {
                        self.offlineStartupReason = nil
                    }
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
                    // F2: no optimistic volumeMounted=true here — the /health
                    // piggyback in refreshCacheStatus publishes the monitor's
                    // ACTUAL verdict (and keeps last-known if the probe fails).
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
    ///
    /// F2: this is a status query with a caller-supplied sleep (the post-start
    /// 1.5 s warmup), so it runs on a global utility queue — never parked on
    /// pollQueue where it would stall every health read behind its sleep — and
    /// is single-flight so overlapping callers can't stack sleeps.
    public func refreshSelfTest(force: Bool = false, delayMs: Int = 0) {
        guard !selfTestRefreshInFlight else { return }
        selfTestRefreshInFlight = true
        let addr = preferences.metricsAddr
        DispatchQueue.global(qos: .utility).async { [weak self] in
            if delayMs > 0 {
                Thread.sleep(forTimeInterval: Double(delayMs) / 1000.0)
            }
            let result = NFSBridge.selfTest(force: force, metricsAddr: addr)
            Task { @MainActor in
                self?.selfTestRefreshInFlight = false
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
        // F2: one probe in flight at a time — a slow /health answer drops
        // subsequent tick-probes instead of queueing them.
        guard !backstopProbeInFlight else { return }
        backstopProbeInFlight = true

        let metricsAddr = preferences.metricsAddr
        pollQueue.async { [weak self] in
            guard let probe = NFSBridge.healthProbe(metricsAddr: metricsAddr) else {
                Task { @MainActor in self?.backstopProbeInFlight = false }
                return
            }
            // R-8: gate on coreHealthy (FUSE/Redis/MinIO), NOT probe.healthy
            // (= /health Overall, which ANDs in NFS). The loopback NFS mount
            // recovers on a slower timeline (deferred remount up to ~180s) than
            // the backend, so gating on Overall left the red "Disconnected"
            // latched while reads already worked. NFS-mount health drives
            // volumeMounted separately, not the connected/disconnected verdict.
            guard probe.coreHealthy else {
                Task { @MainActor in self?.backstopProbeInFlight = false }
                return
            }
            Task { @MainActor in
                guard let self else { return }
                self.backstopProbeInFlight = false
                switch self.state {
                case .disconnected, .degraded:
                    self.log.warning("backstop: /health probe says core-healthy while state=\(self.state.displayLabel, privacy: .public) for \(self.stuckTicks) ticks — forcing transition to .running")
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
            // R-8: coreHealthy (FUSE/Redis/MinIO), NOT probe.healthy (NFS-
            // inclusive Overall). This watchdog is the ONLY recovery path that
            // survives a wedged workQueue — but while it gated on Overall it
            // no-op'd for the entire NFS-recovery lag, so a flap left the menu
            // bar stuck "Disconnected" even though the backend was reachable.
            guard let probe = NFSBridge.healthProbe(metricsAddr: metricsAddr), probe.coreHealthy else { return }
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
                    // This probe IS the first real health evidence — a start
                    // still awaiting confirmation is confirmed by it too.
                    self.awaitingFirstHealth = false
                    self.state = .running
                    self.stuckTicks = 0
                }
                self.refreshCacheStatus()
            }
        }
    }

    /// F2: the primary health feed for the state machine. Runs on pollQueue
    /// (never behind a mutation) and is single-flight — while one cgo stats
    /// read is executing, overlapping ticks are dropped rather than queued,
    /// so a wedged read can't build a backlog of stale samples.
    private func refreshStats() {
        guard !statsRefreshInFlight else { return }
        statsRefreshInFlight = true
        pollQueue.async { [weak self] in
            guard let self else { return }
            do {
                let s = try NFSBridge.stats()
                Task { @MainActor in
                    self.statsRefreshInFlight = false
                    self.stats = s
                    self.updateStateFromStats(s)
                }
            } catch {
                // Bridge may be torn down — release the slot so a later
                // tick can retry.
                Task { @MainActor in
                    self.statsRefreshInFlight = false
                }
            }
        }
    }

    private func updateStateFromStats(_ s: NFSBridge.Stats) {
        guard s.running else {
            // Server stopped from underneath us. From .running this is a
            // disconnect. F2: while a start is still awaiting its first
            // health sample, an immediate death is reported honestly too —
            // no parking in .starting (the recovery watchdog can revive if
            // this was a fluke). Sticky .error/.idle stay untouched.
            if case .running = state {
                state = .disconnected
                return
            }
            if awaitingFirstHealth, case .starting = state {
                awaitingFirstHealth = false
                log.warning("bridge exited immediately after start — reporting disconnected")
                state = .disconnected
            }
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
            case .running:
                return
            case .syncing:
                // Keep the sync spinner ONLY while the sync round-trip is
                // actually in flight. Once it completes (syncInFlight false),
                // THIS healthy sample is what earns .running — no hardcoded
                // optimistic flip at the end of syncNow.
                if syncInFlight { return }
                state = .running
                return
            case .starting:
                // F2: the first REAL health confirmation is what ends the
                // start. Until this sample (or a /health probe) arrived we
                // stayed honestly in .starting.
                guard awaitingFirstHealth else { return }
                awaitingFirstHealth = false
                log.info("first health sample healthy — starting -> running")
                state = .running
                stuckTicks = 0
                return
            case .disconnected, .degraded:
                // Recovery edge — log it. Future-me trying to debug a
                // "stuck disconnected" report will check os_log for this.
                log.info("recovery: backend healthy, transitioning \(self.state.displayLabel, privacy: .public) -> running")
                state = .running
                stuckTicks = 0
                return
            default:
                // .idle / .error are sticky; don't overwrite.
                return
            }
        }

        // Not fully healthy — pick the most-severe component as the cause.
        // F2: only running-like states may be remapped (plus a start still
        // awaiting its first sample). A stale in-flight stats read landing
        // after Stop must never stomp a deliberate .idle or a real .error,
        // and the first post-start sample answers "how did it come up?"
        // rather than leaving the question open.
        let remappable: Bool
        switch state {
        case .running, .syncing, .degraded, .disconnected:
            remappable = true
        default:
            remappable = awaitingFirstHealth
        }
        guard remappable else { return }
        if awaitingFirstHealth {
            awaitingFirstHealth = false
            log.warning("first health sample UNHEALTHY after start — reporting degradation honestly")
        }

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
        // R-8: trust cacheStatus.offline_mode (read via the in-process cgo
        // NFSServerCacheStatus on every refresh — it CANNOT go stale the way the
        // separate HTTP /offline fetch can, which is kept last-known on a nil
        // response). offlineState.offline lagged up to a full poll after a
        // reconnect, leaving the blue "Offline files" icon up while the core had
        // already cleared auto-offline. This also matches the offline TOGGLE,
        // which already binds to cacheStatus.offline_mode — so icon and toggle
        // never disagree. (offlineState is still used for auto-offline
        // notification edges, which want the reason/since metadata.)
        let effectivelyOffline = cacheStatus.offline_mode
        switch state {
        case .error, .disconnected:
            return .fault
        case .idle:
            return .idle
        case .starting:
            return effectivelyOffline ? .offlineFiles : .degraded
        case .degraded:
            if !volumeMounted { return .degraded }
            return effectivelyOffline ? .offlineFiles : .degraded
        case .running, .syncing:
            if !volumeMounted { return .degraded }
            return effectivelyOffline ? .offlineFiles : .healthy
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
