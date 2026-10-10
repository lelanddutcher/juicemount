import AppKit

// AppKit harness: runs the REAL QuitCountdownDriver inside a real
// applicationShouldTerminate -> .terminateLater state, where AppKit spins the
// main run loop in NSModalPanelRunLoopMode. A default-mode timer never fires
// there; this harness fails if the countdown does not reach its terminal
// phase, or if no holder recount ran.
//
// Not the JuiceMount app: a bare NSApplication with no windows and the
// .prohibited activation policy. Build and run:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/QuitEjectCountdown.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/QuitCountdownDriver.swift \
//     app/JuiceMount/Tests/QuitCountdownTerminateLaterHarness.swift -o <scratch>/h && <scratch>/h
@main
struct QuitCountdownTerminateLaterHarness {
    static func main() {
        let app = NSApplication.shared
        app.setActivationPolicy(.prohibited)
        let delegate = HarnessDelegate()
        app.delegate = delegate
        // Watchdog off the main thread: a driver that never ticks would
        // otherwise hang here forever.
        Thread.detachNewThread {
            Thread.sleep(forTimeInterval: 8)
            print("FAIL: countdown did not finish inside terminateLater (ticks=\(delegate.ticksSeen), recounts=\(delegate.recounts), modes=\(delegate.modes))")
            exit(1)
        }
        app.run()
    }
}

@MainActor
final class HarnessDelegate: NSObject, NSApplicationDelegate {
    nonisolated(unsafe) var ticksSeen = 0
    nonisolated(unsafe) var recounts = 0
    nonisolated(unsafe) var modes = Set<String>()
    var driver: QuitCountdownDriver?
    var terminal: QuitEjectCountdown.Phase?

    func applicationDidFinishLaunching(_ notification: Notification) {
        // Trigger Quit from a run-loop source, as a menu click does. Calling
        // terminate from a DispatchQueue.main block would nest the modal loop
        // inside that block, and the serial main queue (and so every
        // @MainActor Task) would stall for reasons that never occur in the app.
        RunLoop.main.add(Timer(timeInterval: 0.05, repeats: false) { _ in NSApp.terminate(nil) }, forMode: .default)
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        let countdown = QuitEjectCountdown(holderApps: 2, forceAvailable: true)!
        let d = QuitCountdownDriver(
            countdown: countdown,
            tickInterval: 0.02,
            recountEveryTicks: 3,
            recount: { [weak self] in
                self?.recounts += 1
                return 2 // still held: the clock must keep running
            },
            onChange: { [weak self] _ in
                guard let self else { return }
                self.ticksSeen += 1
                if let mode = RunLoop.current.currentMode { self.modes.insert(mode.rawValue) }
            },
            onTerminal: { [weak self] phase in
                guard let self else { return }
                self.terminal = phase
                NSApp.reply(toApplicationShouldTerminate: true)
            })
        driver = d
        d.start()
        return .terminateLater
    }

    func applicationWillTerminate(_ notification: Notification) {
        var failures = 0
        if terminal != .ejectRequested(.timerExpired) {
            print("FAIL: terminal phase \(String(describing: terminal)), want ejectRequested(timerExpired)"); failures += 1
        }
        if recounts == 0 { print("FAIL: no holder recount ran inside terminateLater"); failures += 1 }
        if !modes.contains(RunLoop.Mode.modalPanel.rawValue) {
            print("FAIL: never observed NSModalPanelRunLoopMode (modes=\(modes)); the harness did not exercise terminateLater"); failures += 1
        }
        if failures > 0 { exit(1) }
        print("PASS: countdown ticks and recounts inside terminateLater (modes=\(modes.sorted()), recounts=\(recounts))")
        exit(0)
    }
}
