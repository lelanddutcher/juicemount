import Foundation

/// Drives a QuitEjectCountdown with real time: one tick per `tickInterval`,
/// a holder recount every `recountEveryTicks` ticks, and exactly one terminal
/// callback.
///
/// It runs while applicationShouldTerminate has answered `.terminateLater`.
/// In that state AppKit spins the main run loop in NSModalPanelRunLoopMode, so
/// a timer in the default mode (Timer.scheduledTimer) never fires: the panel
/// would sit at "30 s" forever and never recount. The timer is therefore added
/// to RunLoop.main in `.common` mode, which includes the modal-panel mode.
/// Tests/QuitCountdownTerminateLaterHarness.swift runs this exact driver
/// inside a real terminateLater state.
@MainActor
final class QuitCountdownDriver {
    private(set) var countdown: QuitEjectCountdown
    private let tickInterval: TimeInterval
    private let recountEveryTicks: Int
    private let recount: @MainActor () async -> Int?
    private let onChange: @MainActor (QuitEjectCountdown) -> Void
    private let onTerminal: @MainActor (QuitEjectCountdown.Phase) -> Void
    private var timer: Timer?
    private var ticks = 0
    private var recountInFlight = false
    private var finished = false

    init(countdown: QuitEjectCountdown,
         tickInterval: TimeInterval = 1,
         recountEveryTicks: Int = 3,
         recount: @escaping @MainActor () async -> Int?,
         onChange: @escaping @MainActor (QuitEjectCountdown) -> Void,
         onTerminal: @escaping @MainActor (QuitEjectCountdown.Phase) -> Void) {
        self.countdown = countdown
        self.tickInterval = tickInterval
        self.recountEveryTicks = max(1, recountEveryTicks)
        self.recount = recount
        self.onChange = onChange
        self.onTerminal = onTerminal
    }

    func start() {
        guard timer == nil, !finished else { return }
        let t = Timer(timeInterval: tickInterval, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.fire() }
        }
        RunLoop.main.add(t, forMode: .common)
        timer = t
    }

    /// Button presses and recount results enter here.
    func send(_ event: QuitEjectCountdown.Event) {
        guard !finished else { return }
        countdown.handle(event)
        onChange(countdown)
        if countdown.isTerminal { finish() }
    }

    func stop() {
        timer?.invalidate()
        timer = nil
    }

    private func fire() {
        guard !finished else { return }
        ticks += 1
        send(.tick)
        guard !finished, ticks % recountEveryTicks == 0, !recountInFlight else { return }
        recountInFlight = true
        Task { @MainActor [weak self] in
            guard let self else { return }
            let count = await self.recount()
            self.recountInFlight = false
            self.send(.holdersCounted(count))
        }
    }

    private func finish() {
        finished = true
        stop()
        onTerminal(countdown.phase)
    }
}
