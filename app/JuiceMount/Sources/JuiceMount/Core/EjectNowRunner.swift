import Foundation

// H-3: the busy-Quit panel's I/O with the serving service's /eject-now route
// (bridge/eject_now.go). This file imports only Foundation and uses only
// UnmountHelperDecision / EjectNowRequester from UnmountHelperService.swift,
// so a probe compiles the two alone and drives the real runner and the real
// HTTP client against a real bridge handler (Tests/EjectNowContractProbe.swift).

/// One bounded request per call to the service's local control route
/// http://<metricsAddr>/eject-now. Blocking: call off the main thread.
enum EjectNowHTTP {
    /// Bound for the capability read at Quit; the countdown decision waits
    /// on it, so it stays short.
    static let capabilityTimeout: TimeInterval = 2
    /// The service may hold POST /eject-now for the helper's full retire wait
    /// (client.MaxRetireWait = 60 s) plus its own bookkeeping.
    static let postTimeout: TimeInterval = 75
    /// The service bounds its helper Cancel at 5 s inside a 10 s context.
    static let withdrawTimeout: TimeInterval = 15

    /// GET: does the service's eject-now route reach the helper?
    static func capability(metricsAddr: String) -> Bool {
        let (status, body) = request(metricsAddr: metricsAddr, method: "GET", timeout: capabilityTimeout)
        return UnmountHelperDecision.serviceSupportsEjectNow(httpStatus: status, body: body)
    }

    /// POST {"phase":…}: ordinary retire first. A start POST never forces;
    /// an escalate POST forces only after the service's floor.
    static func post(metricsAddr: String, phase: EjectNowPhase) -> UnmountHelperDecision.EjectResult {
        let (status, body) = request(metricsAddr: metricsAddr, method: "POST", timeout: postTimeout,
                                     body: postBody(phase))
        return UnmountHelperDecision.ejectResult(httpStatus: status, body: body)
    }

    /// The POST body: {"phase":"start"} or {"phase":"escalate"}.
    static func postBody(_ phase: EjectNowPhase) -> Data {
        Data("{\"phase\":\"\(phase.rawValue)\"}".utf8)
    }

    /// DELETE: withdraw the ordinary retire a POST started.
    static func withdraw(metricsAddr: String) -> UnmountHelperDecision.WithdrawResult {
        let (status, body) = request(metricsAddr: metricsAddr, method: "DELETE", timeout: withdrawTimeout)
        return UnmountHelperDecision.withdrawResult(httpStatus: status, body: body)
    }

    static func request(metricsAddr: String, method: String, timeout: TimeInterval, body: Data? = nil) -> (Int?, Data?) {
        guard let url = URL(string: "http://\(metricsAddr)/eject-now") else { return (nil, nil) }
        var req = URLRequest(url: url, timeoutInterval: timeout)
        req.httpMethod = method
        if let body {
            req.httpBody = body
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        // Both mutations need the local-control header (the service answers
        // 403 without it).
        if method != "GET" { req.setValue("1", forHTTPHeaderField: "X-JM-Control") }
        let done = DispatchSemaphore(value: 0)
        let box = Reply()
        URLSession.shared.dataTask(with: req) { data, response, _ in
            box.lock.lock()
            box.status = (response as? HTTPURLResponse)?.statusCode
            box.body = data
            box.lock.unlock()
            done.signal()
        }.resume()
        if done.wait(timeout: .now() + timeout + 1) == .timedOut { return (nil, nil) }
        box.lock.lock()
        defer { box.lock.unlock() }
        return (box.status, box.body)
    }

    private final class Reply: @unchecked Sendable {
        let lock = NSLock()
        var status: Int?
        var body: Data?
    }
}

/// Real-time driver for EjectNowRequester: one tick per second on
/// RunLoop.main in .common modes (the busy-Quit flow lives inside
/// .terminateLater, see QuitCountdownDriver), one request per `.post` /
/// `.withdraw` command, and exactly one onDone. Each POST carries the phase
/// the requester recorded for it (EjectNowRequester.inFlightPhase).
@MainActor
final class EjectNowRunner {
    private(set) var requester = EjectNowRequester()
    private let post: @MainActor (EjectNowPhase) async -> UnmountHelperDecision.EjectResult
    private let withdraw: @MainActor () async -> UnmountHelperDecision.WithdrawResult
    private let log: @MainActor (String) -> Void
    private let onChange: @MainActor (EjectNowRequester) -> Void
    private let onDone: @MainActor (EjectNowRequester.Outcome) -> Void
    private var timer: Timer?
    private var finished = false

    init(post: @escaping @MainActor (EjectNowPhase) async -> UnmountHelperDecision.EjectResult,
         withdraw: @escaping @MainActor () async -> UnmountHelperDecision.WithdrawResult,
         log: @escaping @MainActor (String) -> Void,
         onChange: @escaping @MainActor (EjectNowRequester) -> Void,
         onDone: @escaping @MainActor (EjectNowRequester.Outcome) -> Void) {
        self.post = post
        self.withdraw = withdraw
        self.log = log
        self.onChange = onChange
        self.onDone = onDone
    }

    func start() {
        guard timer == nil, !finished else { return }
        let t = Timer(timeInterval: 1, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.apply { $0.tick() } }
        }
        RunLoop.main.add(t, forMode: .common)
        timer = t
        apply { $0.countdownStarted() }
    }

    func escalate(userPressed: Bool) {
        apply { $0.escalate(userPressed: userPressed) }
    }

    /// The panel was abandoned: stop POSTing (the clock stops), and withdraw
    /// whatever a POST started. onDone follows with `.abandoned`, or
    /// `.retired` when a POST in flight had already ejected the volume.
    func cancel() {
        timer?.invalidate()
        timer = nil
        apply { $0.cancel() }
    }

    func stop() {
        finished = true
        timer?.invalidate()
        timer = nil
    }

    private func apply(_ step: (inout EjectNowRequester) -> EjectNowRequester.Command) {
        guard !finished else { return }
        let command = step(&requester)
        onChange(requester)
        if let outcome = requester.outcome {
            stop()
            onDone(outcome)
            return
        }
        switch command {
        case .none:
            break
        case .post:
            let phase = requester.inFlightPhase
            Task { @MainActor [weak self] in
                guard let self else { return }
                let result = await self.post(phase)
                self.log("quit: eject-now \(phase.rawValue) reply \(result)")
                self.apply { $0.reply(result) }
            }
        case .withdraw:
            log("quit: busy Quit abandoned after \(requester.postsSent) eject-now POST(s) — DELETE /eject-now")
            Task { @MainActor [weak self] in
                guard let self else { return }
                let result = await self.withdraw()
                self.log("quit: eject-now withdraw reply \(result)")
                self.apply { $0.withdrawReply(result) }
            }
        }
    }
}
