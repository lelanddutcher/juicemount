import Darwin
import Foundation

/// One retained child and its original three pipes. A timeout or cancellation
/// sends SIGTERM, then remains stopping until the process and every pipe worker
/// actually join. No timeout is ever reported as a successful join.
final class NativeLibraryHelper: @unchecked Sendable {
    enum Command {
        case profileCreate, profileOpen, search, page, select(copyID: String, basename: String)
        case receive(copyID: String), recover(copyID: String), copyStatus(copyID: String)
        case inspect, lookup, cancel, reconcile, version

        var words: [String] {
            switch self {
            case .profileCreate: return ["native", "profile", "create"]
            case .profileOpen: return ["native", "profile", "open"]
            case .search: return ["native", "search"]
            case .page: return ["native", "page"]
            case .select: return ["native", "select"]
            case .receive: return ["native", "receive"]
            case .recover: return ["native", "recover"]
            case .copyStatus: return ["native", "copy-status"]
            case .inspect: return ["native", "inspect"]
            case .lookup: return ["native", "lookup"]
            case .cancel: return ["native", "cancel"]
            case .reconcile: return ["native", "reconcile"]
            case .version: return ["native", "version"]
            }
        }
        var mayMutate: Bool {
            switch self {
            case .profileCreate, .select, .receive, .recover, .cancel, .reconcile: return true
            default: return false
            }
        }
        var longRunning: Bool {
            switch self { case .receive, .recover, .copyStatus: return true; default: return false }
        }
        var stdoutLimit: Int {
            (1 << 20) + 1 // one 1 MiB JSON receipt plus its final LF
        }
        func arguments(profile: NativeLibraryWire.Profile) throws -> [String] {
            if case .version = self { return words }
            var args = ["--native-root", profile.nativeRoot, "--endpoint", profile.endpoint,
                        "--library", profile.libraryID]
            if profile.allowLoopbackHTTP { args.append("--allow-loopback-http") }
            switch self {
            case .select(let id, let basename):
                guard NativeLibraryWire.lowerHexID(id, prefix: "copy_", digits: 32),
                      NativeLibraryWire.validBasename(basename) else { throw Failure.invalidCommand }
                args += ["--copy-id", id, "--basename", basename]
            case .receive(let id), .recover(let id), .copyStatus(let id):
                guard NativeLibraryWire.lowerHexID(id, prefix: "copy_", digits: 32) else { throw Failure.invalidCommand }
                args += ["--copy-id", id]
            default: break
            }
            if longRunning { args += ["--timeout", "900s"] }
            return args + words
        }
    }

    enum Failure: Error, Equatable {
        case invalidCommand, helperUnavailable, busy, stopped, timedOut
        case childFailure(code: String, httpStatus: Int?, uncertain: Bool)
        case malformedReceipt(uncertain: Bool)
        case originalCloseUncertain

        var outcomeUncertain: Bool {
            switch self {
            case .childFailure(_, _, let uncertain), .malformedReceipt(let uncertain): return uncertain
            case .originalCloseUncertain, .stopped, .timedOut: return true
            default: return false
            }
        }
    }

    private let lock = NSLock()
    private var active: Invocation?
    private let queue = DispatchQueue(label: "com.juicemount.native-library.helper", qos: .userInitiated)

    private func reserve(_ invocation: Invocation) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard active == nil else { return false }
        active = invocation
        return true
    }
    private func clear(_ invocation: Invocation) {
        lock.lock(); defer { lock.unlock() }
        if active === invocation { active = nil }
    }
    private func current() -> Invocation? {
        lock.lock(); defer { lock.unlock() }
        return active
    }

    /// The caller passes only canonical envelope bytes or a raw bearer; no
    /// secret is ever copied to command arguments or the child's environment.
    func run(_ command: Command, profile: NativeLibraryWire.Profile, stdin: Data) async throws -> Data {
        guard stdin.count <= 33_280 else { throw Failure.invalidCommand }
        let args = try command.arguments(profile: profile)
        let executable = try Self.bundledExecutable()
        let invocation = Invocation(executable: executable, arguments: args, input: stdin,
                                    stdoutLimit: command.stdoutLimit, mayMutate: command.mayMutate,
                                    timeout: command.longRunning ? 900 : 30)
        guard reserve(invocation) else { throw Failure.busy }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                queue.async { [self] in
                    let outcome = invocation.execute()
                    clear(invocation)
                    switch outcome {
                    case .success(let bytes): continuation.resume(returning: bytes)
                    case .failure(let error): continuation.resume(throwing: error)
                    }
                }
            }
        } onCancel: {
            invocation.requestStop(.canceled)
        }
    }

    /// Quit and profile disconnect await the same original invocation. Several
    /// callers may wait concurrently; none can clear a busy owner by timeout.
    func stopAndJoin() async {
        let original = current()
        guard let original else { return }
        original.requestStop(.canceled)
        await withCheckedContinuation { continuation in
            DispatchQueue.global(qos: .utility).async {
                original.joined.wait()
                continuation.resume()
            }
        }
    }

    var isBusy: Bool {
        lock.lock(); defer { lock.unlock() }
        return active != nil
    }

    private static func bundledExecutable() throws -> URL {
        let bundle = Bundle.main.bundleURL.standardizedFileURL
        guard bundle.pathExtension == "app" else { throw Failure.helperUnavailable }
        let url = bundle.appendingPathComponent("Contents/MacOS/juicemount-catalog")
        guard url.standardizedFileURL.path == url.path,
              url.resolvingSymlinksInPath().path == url.path,
              FileManager.default.isExecutableFile(atPath: url.path) else { throw Failure.helperUnavailable }
        return url
    }

    private final class Invocation: @unchecked Sendable {
        enum StopReason: Equatable { case canceled, deadline, overflow, pipeFault }
        let joined = DispatchGroup()
        private let process = Process()
        private let inputPipe = Pipe()
        private let outputPipe = Pipe()
        private let errorPipe = Pipe()
        private let input: Data
        private let stdoutLimit: Int
        private let mayMutate: Bool
        private let timeout: TimeInterval
        private let lock = NSLock()
        private var launched = false
        private var finished = false
        private var signalSent = false
        private var stopReason: StopReason?
        private var ownershipFault = false
        private var stdoutBytes = Data()
        private var stderrBytes = Data()
        private var stdoutOverflow = false
        private var stderrOverflow = false

        init(executable: URL, arguments: [String], input: Data, stdoutLimit: Int,
             mayMutate: Bool, timeout: TimeInterval) {
            self.input = input
            self.stdoutLimit = stdoutLimit
            self.mayMutate = mayMutate
            self.timeout = timeout
            process.executableURL = executable
            process.arguments = arguments
            process.environment = ["PATH": "/usr/bin:/bin", "LANG": "C"]
            process.standardInput = inputPipe
            process.standardOutput = outputPipe
            process.standardError = errorPipe
            joined.enter()
        }

        func requestStop(_ reason: StopReason) {
            lock.lock()
            if stopReason == nil { stopReason = reason }
            let shouldSignal = launched && !finished && !signalSent && process.isRunning
            if shouldSignal { signalSent = true }
            lock.unlock()
            if shouldSignal { process.terminate() }
        }

        private func markFault() {
            lock.lock(); ownershipFault = true; lock.unlock()
            requestStop(.pipeFault)
        }
        private func close(_ handle: FileHandle) {
            do { try handle.close() } catch { markFault() }
        }
        private func drain(_ handle: FileHandle, stdout: Bool) {
            defer { close(handle) }
            while true {
                do {
                    guard let next = try handle.read(upToCount: 32 * 1024), !next.isEmpty else { return }
                    lock.lock()
                    if stdout {
                        let available = max(0, stdoutLimit + 1 - stdoutBytes.count)
                        stdoutBytes.append(next.prefix(available))
                        if next.count > available || stdoutBytes.count > stdoutLimit { stdoutOverflow = true }
                    } else {
                        let available = max(0, 8192 + 1 - stderrBytes.count)
                        stderrBytes.append(next.prefix(available))
                        if next.count > available || stderrBytes.count > 8192 { stderrOverflow = true }
                    }
                    let overflow = stdoutOverflow || stderrOverflow
                    lock.unlock()
                    if overflow { requestStop(.overflow) }
                } catch {
                    markFault()
                    return
                }
            }
        }
        private func writeInput() {
            defer { close(inputPipe.fileHandleForWriting) }
            do { try inputPipe.fileHandleForWriting.write(contentsOf: input) }
            catch { markFault() }
        }

        func execute() -> Result<Data, Failure> {
            defer {
                lock.lock(); finished = true; lock.unlock()
                joined.leave()
            }
            let termination = DispatchSemaphore(value: 0)
            process.terminationHandler = { _ in termination.signal() } // installed before launch
            // A child may close stdin as soon as it validates a request.
            // Broken-pipe must reach the original writer as an error, never
            // deliver SIGPIPE to the GUI process.
            if fcntl(inputPipe.fileHandleForWriting.fileDescriptor, F_SETNOSIGPIPE, 1) != 0 {
                close(inputPipe.fileHandleForWriting)
                close(inputPipe.fileHandleForReading)
                close(outputPipe.fileHandleForReading)
                close(outputPipe.fileHandleForWriting)
                close(errorPipe.fileHandleForReading)
                close(errorPipe.fileHandleForWriting)
                return .failure(.originalCloseUncertain)
            }
            lock.lock()
            let canceledBeforeLaunch = stopReason != nil
            if !canceledBeforeLaunch {
                do { try process.run(); launched = true }
                catch {
                    lock.unlock()
                    close(inputPipe.fileHandleForWriting)
                    close(inputPipe.fileHandleForReading)
                    close(outputPipe.fileHandleForReading)
                    close(outputPipe.fileHandleForWriting)
                    close(errorPipe.fileHandleForReading)
                    close(errorPipe.fileHandleForWriting)
                    lock.lock(); let fault = ownershipFault; lock.unlock()
                    return .failure(fault ? .originalCloseUncertain : .helperUnavailable)
                }
            }
            lock.unlock()
            if canceledBeforeLaunch {
                close(inputPipe.fileHandleForWriting)
                close(inputPipe.fileHandleForReading)
                close(outputPipe.fileHandleForReading)
                close(outputPipe.fileHandleForWriting)
                close(errorPipe.fileHandleForReading)
                close(errorPipe.fileHandleForWriting)
                return .failure(.stopped)
            }
            // Foundation's Process closes its child-side pipe descriptors in
            // the parent at launch. The three parent-side workers remain
            // owned until actual EOF/close and Process termination.
            let workers = DispatchGroup()
            for (handle, isStdout) in [(outputPipe.fileHandleForReading, true),
                                       (errorPipe.fileHandleForReading, false)] {
                workers.enter()
                DispatchQueue.global(qos: .utility).async {
                    self.drain(handle, stdout: isStdout)
                    workers.leave()
                }
            }
            workers.enter()
            DispatchQueue.global(qos: .utility).async {
                self.writeInput()
                workers.leave()
            }
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout) { [weak self] in
                self?.requestStop(.deadline)
            }
            termination.wait()
            process.waitUntilExit()
            workers.wait() // no bounded-return shortcut; original pipes joined
            lock.lock()
            finished = true
            let reason = stopReason
            let fault = ownershipFault
            let overflow = stdoutOverflow || stderrOverflow
            let output = stdoutBytes
            let diagnostic = stderrBytes
            let exit = process.terminationStatus
            lock.unlock()
            if fault { return .failure(.originalCloseUncertain) }
            if overflow { return .failure(.malformedReceipt(uncertain: mayMutate)) }
            if reason == .deadline { return .failure(.timedOut) }
            if reason != nil { return .failure(.stopped) }
            if exit == 0 {
                guard diagnostic.isEmpty, !output.isEmpty else { return .failure(.malformedReceipt(uncertain: mayMutate)) }
                return .success(output)
            }
            guard exit == 3,
                  let values = try? NativeLibraryWire.diagnostics(diagnostic), let last = values.last else {
                return .failure(.malformedReceipt(uncertain: mayMutate))
            }
            let uncertain = mayMutate || values.contains { $0.outcomeUncertain == true || $0.code == "uncertain" }
            if !output.isEmpty { return .failure(.malformedReceipt(uncertain: uncertain)) }
            return .failure(.childFailure(code: last.code, httpStatus: last.httpStatus,
                                          uncertain: uncertain))
        }
    }
}
