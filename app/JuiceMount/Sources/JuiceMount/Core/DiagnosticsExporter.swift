import Foundation
import AppKit
import os.log

/// Bundles the artifacts a support engineer needs to triage a JuiceMount
/// install into a single zip: app + JuiceFS logs, control-plane snapshots,
/// FileProvider/pluginkit state, mount/df state, and a small system
/// summary. Best-effort: every step is wrapped so one failing tool doesn't
/// torpedo the whole export. Failures are recorded in `errors.txt` inside
/// the zip.
///
/// Usage:
///     let exporter = DiagnosticsExporter()
///     try await exporter.export(to: destinationURL)
///
/// The destination URL is the user-chosen save location (e.g. a `.zip`
/// path on Desktop). All work runs off the main actor.
public final class DiagnosticsExporter {

    private let log = Logger(subsystem: "com.juicemount.app",
                             category: "DiagnosticsExporter")

    /// Control-plane address for the HTTP snapshots (/health, /spool, …).
    /// Review 3b BUG 2: this was hardcoded to 127.0.0.1:11050, so a custom
    /// metrics address silently produced diagnostics bundles MISSING every
    /// control-plane snapshot — exactly the configs where support needs them.
    /// Callers pass preferences.metricsAddr (captured on MainActor).
    private let metricsAddr: String

    public init(metricsAddr: String = "127.0.0.1:11050") {
        self.metricsAddr = metricsAddr
    }

    public enum ExportError: Error {
        case stagingFailed(String)
        case zipFailed(String)
    }

    /// Gathers diagnostics into a temp staging directory, zips with `ditto`,
    /// then moves the zip to `destination`. Runs entirely off the main
    /// actor. Tools/files that are missing are recorded in `errors.txt`
    /// inside the bundle but do not stop the export.
    public func export(to destination: URL) async throws {
        try await Task.detached(priority: .userInitiated) { [self] in
            try performExport(to: destination)
        }.value
    }

    // MARK: - Implementation

    private func performExport(to destination: URL) throws {
        let fm = FileManager.default
        var errors: [String] = []

        // 1. Create staging dir
        let stageName = "JuiceMount-diagnostics-\(Self.timestamp())"
        let scratch = AppIsolation.active.map { URL(fileURLWithPath: $0.child("tmp"), isDirectory: true) }
            ?? fm.temporaryDirectory
        let stageRoot = scratch.appendingPathComponent(stageName)
        do {
            try fm.createDirectory(at: stageRoot,
                                   withIntermediateDirectories: true)
        } catch {
            throw ExportError.stagingFailed(error.localizedDescription)
        }
        defer {
            try? fm.removeItem(at: stageRoot)
        }

        // 2. Collect each artifact. Each helper records its own error
        //    into `errors` rather than throwing.
        collectAppLog(into: stageRoot, errors: &errors)
        collectJuiceFSLog(into: stageRoot, errors: &errors)
        collectHTTPSnapshot(path: "/metrics",
                            file: "metrics.json",
                            into: stageRoot, errors: &errors)
        collectHTTPSnapshot(path: "/cache-status",
                            file: "cache-status.json",
                            into: stageRoot, errors: &errors)
        // Spool state (pending/in-progress/drain queue) — the write-spool + NFS
        // in/out-node work's live snapshot, so a diagnostics bundle shows the
        // drain backlog alongside the spool: COMMIT/JUKEBOX/drain log lines.
        collectHTTPSnapshot(path: "/spool",
                            file: "spool.json",
                            into: stageRoot, errors: &errors)
        collectCommand(["/usr/bin/pluginkit", "-m"],
                       file: "pluginkit.txt",
                       into: stageRoot, errors: &errors)
        collectFileProviderDump(into: stageRoot, errors: &errors)
        collectCommand(["/bin/df", "-h", "/"],
                       file: "df.txt",
                       into: stageRoot, errors: &errors)
        collectMount(into: stageRoot, errors: &errors)
        collectCommand(["/usr/sbin/nfsstat", "-m"],
                       file: "nfsstat.txt",
                       into: stageRoot, errors: &errors)
        collectSystemInfo(into: stageRoot, errors: &errors)

        // Write any errors.txt last so it captures everything above.
        if !errors.isEmpty {
            let errorsURL = stageRoot.appendingPathComponent("errors.txt")
            let body = errors.joined(separator: "\n")
            try? writeRedacted(body, to: errorsURL, maxBytes: 64 * 1024)
        }

        // 3. Zip via ditto. -c create, -k keep PKZip format,
        //    --keepParent preserves the top-level folder inside the zip
        //    so unzipping yields one tidy directory.
        let tmpZip = scratch
            .appendingPathComponent("\(stageName).zip")
        try? fm.removeItem(at: tmpZip)

        let dittoResult = runProcess(launchPath: "/usr/bin/ditto",
                                     args: ["-c", "-k", "--keepParent",
                                            stageRoot.path, tmpZip.path],
                                     timeout: 60)
        if dittoResult.exitCode != 0 {
            throw ExportError.zipFailed(
                "ditto exit=\(dittoResult.exitCode): \(dittoResult.stderr)"
            )
        }

        // 4. Move zip to destination. If a file already exists there,
        //    replace it so the user's "Save" intent wins.
        if fm.fileExists(atPath: destination.path) {
            try? fm.removeItem(at: destination)
        }
        do {
            try fm.moveItem(at: tmpZip, to: destination)
        } catch {
            // Fallback: copy + remove (handles cross-volume moves).
            try fm.copyItem(at: tmpZip, to: destination)
            try? fm.removeItem(at: tmpZip)
        }

        log.info("Diagnostics exported to \(destination.path, privacy: .private)")
    }

    // MARK: - Collectors

    private func collectAppLog(into stage: URL, errors: inout [String]) {
        let logDir = AppIsolation.active.map { URL(fileURLWithPath: $0.child("logs")) }
            ?? FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent("Library/Logs/JuiceMount")
        let logURL = logDir.appendingPathComponent("juicemount.log")
        let dest = stage.appendingPathComponent("juicemount.log")
        do {
            try copyRedactedLog(of: logURL, to: dest, maxBytes: 4 * 1024 * 1024)
        } catch {
            errors.append("juicemount.log: \(error.localizedDescription)")
        }
        // Keep bounded complete segments. A partial tail can lose the label
        // belonging to a multiline credential value, so oversized logs omit.
        for i in 1...5 {
            let backup = logDir.appendingPathComponent("juicemount.log.\(i)")
            guard FileManager.default.fileExists(atPath: backup.path) else { continue }
            let backupDest = stage.appendingPathComponent("juicemount.log.\(i)")
            do {
                try copyRedactedLog(of: backup, to: backupDest,
                                     maxBytes: 1 * 1024 * 1024)
            } catch {
                errors.append("juicemount.log.\(i): \(error.localizedDescription)")
            }
        }
    }

    private func collectJuiceFSLog(into stage: URL, errors: inout [String]) {
        let logURL = AppIsolation.active.map { URL(fileURLWithPath: $0.child(".juicefs/juicefs.log")) }
            ?? FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent(".juicefs/juicefs.log")
        let dest = stage.appendingPathComponent("juicefs.log")
        guard FileManager.default.fileExists(atPath: logURL.path) else {
            // Not an error — JuiceFS log just doesn't exist on this host
            // (e.g. never started). Record a sentinel for clarity.
            try? "(juicefs.log not present at \(logURL.path))\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        do {
            try copyRedactedLog(of: logURL, to: dest, maxBytes: 1 * 1024 * 1024)
        } catch {
            errors.append("juicefs.log: \(error.localizedDescription)")
        }
    }

    private func collectHTTPSnapshot(path: String,
                                     file: String,
                                     into stage: URL,
                                     errors: inout [String]) {
        let dest = stage.appendingPathComponent(file)
        guard let url = URL(string: "http://\(metricsAddr)\(path)") else {
            errors.append("\(file): bad URL")
            return
        }
        var req = URLRequest(url: url)
        req.timeoutInterval = 5
        let capture = RequestCapture()
        let sem = DispatchSemaphore(value: 0)
        let task = URLSession.shared.dataTask(with: req) { d, _, e in
            capture.set(data: d, error: e)
            sem.signal()
        }
        task.resume()
        // Wait up to 6s for the request to complete (URLSession honors the
        // 5s timeout above; this is a safety net for the semaphore wait).
        let completed = sem.wait(timeout: .now() + 6) == .success
        if !completed { task.cancel() }
        guard completed else {
            errors.append("\(file): timed out")
            try? "(no response within 6s)\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        let (data, err) = capture.snapshot()
        if let err {
            errors.append("\(file): \(err.localizedDescription)")
            try? writeRedacted("(metrics endpoint unreachable: \(err.localizedDescription))\n",
                               to: dest, maxBytes: 1 * 1024 * 1024)
            return
        }
        guard let data else {
            errors.append("\(file): no data and no error")
            try? "(no response within 6s)\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        guard data.count <= 1 * 1024 * 1024 else {
            errors.append("\(file): response exceeded 1 MiB limit")
            try? "(response omitted: exceeded 1 MiB limit)\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        do {
            let body = String(decoding: data, as: UTF8.self)
            try writeRedacted(body, to: dest, maxBytes: 1 * 1024 * 1024)
        } catch {
            errors.append("\(file): write failed: \(error.localizedDescription)")
        }
    }

    private func collectFileProviderDump(into stage: URL,
                                         errors: inout [String]) {
        // fileproviderctl dump can be very long; cap to first 200 lines.
        // The binary lives in /usr/bin on modern macOS.
        let dest = stage.appendingPathComponent("fileproviderctl-dump.txt")
        let result = runProcess(launchPath: "/usr/bin/fileproviderctl",
                                args: ["dump"],
                                timeout: 15)
        if result.exitCode != 0 && result.stdout.isEmpty {
            errors.append("fileproviderctl: exit=\(result.exitCode) \(result.stderr)")
        }
        let lines = result.stdout.split(separator: "\n",
                                        omittingEmptySubsequences: false)
        let head = lines.prefix(200).joined(separator: "\n")
        try? writeRedacted(head, to: dest, maxBytes: 1 * 1024 * 1024)
    }

    private func collectMount(into stage: URL, errors: inout [String]) {
        let dest = stage.appendingPathComponent("mount.txt")
        let result = runProcess(launchPath: "/sbin/mount",
                                args: [],
                                timeout: 5)
        if result.exitCode != 0 {
            errors.append("mount: exit=\(result.exitCode) \(result.stderr)")
        }
        // Filter to lines mentioning our mounts. Case-insensitive match.
        let needles = ["juicemount", "juicefs", "zpool"]
        let filtered = result.stdout
            .split(separator: "\n", omittingEmptySubsequences: false)
            .filter { line in
                let l = line.lowercased()
                return needles.contains { l.contains($0) }
            }
            .joined(separator: "\n")
        let body = filtered.isEmpty
            ? "(no juicemount/juicefs/zpool mounts found)\n"
            : filtered + "\n"
        try? writeRedacted(body, to: dest, maxBytes: 1 * 1024 * 1024)
    }

    private func collectCommand(_ argv: [String],
                                file: String,
                                into stage: URL,
                                errors: inout [String]) {
        precondition(!argv.isEmpty)
        let dest = stage.appendingPathComponent(file)
        let result = runProcess(launchPath: argv[0],
                                args: Array(argv.dropFirst()),
                                timeout: 15)
        if result.exitCode != 0 && result.stdout.isEmpty {
            errors.append("\(file): exit=\(result.exitCode) \(result.stderr)")
        }
        try? writeRedacted(result.stdout, to: dest, maxBytes: 1 * 1024 * 1024)
    }

    private func collectSystemInfo(into stage: URL, errors: inout [String]) {
        let dest = stage.appendingPathComponent("system.txt")
        var lines: [String] = []

        // macOS version
        let sw = runProcess(launchPath: "/usr/bin/sw_vers",
                            args: [],
                            timeout: 5)
        lines.append("=== sw_vers ===")
        lines.append(sw.stdout.trimmingCharacters(in: .whitespacesAndNewlines))

        // App version from bundle
        let info = Bundle.main.infoDictionary
        let appVersion = info?["CFBundleShortVersionString"] as? String ?? "(unknown)"
        let appBuild = info?["CFBundleVersion"] as? String ?? "(unknown)"
        lines.append("")
        lines.append("=== app ===")
        lines.append("CFBundleShortVersionString: \(appVersion)")
        lines.append("CFBundleVersion: \(appBuild)")
        lines.append("CFBundleIdentifier: \(Bundle.main.bundleIdentifier ?? "(none)")")

        // Date
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        lines.append("")
        lines.append("=== date ===")
        lines.append(iso.string(from: Date()))

        // Anonymized hostname — show only the suffix length, hash the value.
        let host = ProcessInfo.processInfo.hostName
        let hash = String(format: "%08x", abs(host.hashValue))
        lines.append("")
        lines.append("=== host (anonymized) ===")
        lines.append("hostname_hash: \(hash)")
        lines.append("hostname_length: \(host.count)")

        // PIDs
        lines.append("")
        lines.append("=== processes ===")
        let jmPids = pgrep("JuiceMount")
        let jfsPids = pgrep("juicefs")
        lines.append("JuiceMount pids: \(jmPids.joined(separator: ", "))")
        lines.append("juicefs pid count: \(jfsPids.count)")
        lines.append("juicefs pids: \(jfsPids.joined(separator: ", "))")

        // ProcessInfo basics
        lines.append("")
        lines.append("=== ProcessInfo ===")
        lines.append("os: \(ProcessInfo.processInfo.operatingSystemVersionString)")
        lines.append("activeProcessorCount: \(ProcessInfo.processInfo.activeProcessorCount)")
        lines.append("physicalMemory: \(ProcessInfo.processInfo.physicalMemory) bytes")

        let body = lines.joined(separator: "\n") + "\n"
        do {
            try writeRedacted(body, to: dest, maxBytes: 1 * 1024 * 1024)
        } catch {
            errors.append("system.txt: \(error.localizedDescription)")
        }
    }

    // MARK: - Helpers

    private func pgrep(_ name: String) -> [String] {
        let result = runProcess(launchPath: "/usr/bin/pgrep",
                                args: ["-x", name],
                                timeout: 3)
        return result.stdout
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { String($0) }
    }

    /// Redact a bounded complete log before staging. Omit oversized logs:
    /// a credential label can occur arbitrarily far before a multiline value.
    func copyRedactedLog(of src: URL,
                                  to dest: URL,
                                  maxBytes: UInt64) throws {
        let fm = FileManager.default
        guard fm.fileExists(atPath: src.path) else {
            throw NSError(domain: "DiagnosticsExporter",
                          code: 1,
                          userInfo: [NSLocalizedDescriptionKey:
                                        "file not found: \(src.path)"])
        }
        let handle = try FileHandle(forReadingFrom: src)
        defer { try? handle.close() }

        let size = try handle.seekToEnd()
        guard size <= maxBytes else {
            try writeRedacted("(content omitted: oversized log)\n", to: dest, maxBytes: Int(maxBytes))
            return
        }
        try handle.seek(toOffset: 0)
        // One extra byte detects a file that grew past the cap after seek.
        let data = handle.readData(ofLength: Int(maxBytes) + 1)
        guard data.count <= Int(maxBytes) else {
            try writeRedacted("(content omitted: oversized log)\n", to: dest, maxBytes: Int(maxBytes))
            return
        }
        try writeRedacted(String(decoding: data, as: UTF8.self),
                          to: dest, maxBytes: Int(maxBytes))
    }

    func writeRedacted(_ text: String, to destination: URL,
                       maxBytes: Int) throws {
        let redacted = Self.redact(text)
        let output = String(decoding: redacted.utf8.prefix(maxBytes), as: UTF8.self)
        try output.write(to: destination, atomically: true, encoding: .utf8)
    }

    // Omit the entire artifact when a marker appears: values in JSON or logs
    // can be on later lines and cannot be safely redacted line by line.
    // "fingerprint": the enrollment fingerprint is a function of this Mac's
    // secret, and the Mac accepts it as proof that a server knows that
    // secret, so it must not reach a diagnostics zip attached to an issue.
    private static let credentialMarkers =
        ["password", "passwd", "secret", "token", "credential",
         "authorization", "cookie", "api_key", "apikey",
         "access_key", "private_key", "signature", "bearer",
         "x-amz", "redis_url", "fingerprint", "://"]

    private static func hasCredentialMarker(_ text: String) -> Bool {
        let lowered = text.lowercased()
        return credentialMarkers.contains(where: lowered.contains)
    }

    static func redact(_ text: String) -> String {
        return hasCredentialMarker(text)
            ? "(content omitted: credential-bearing content)\n" : text
    }

    private final class RequestCapture {
        private let lock = NSLock()
        private var data: Data?
        private var error: Error?

        func set(data: Data?, error: Error?) {
            lock.lock()
            self.data = data
            self.error = error
            lock.unlock()
        }

        func snapshot() -> (Data?, Error?) {
            lock.lock()
            defer { lock.unlock() }
            return (data, error)
        }
    }

    private final class BoundedPipeCapture {
        private let lock = NSLock()
        private var data = Data()

        func append(_ chunk: Data) {
            lock.lock()
            let remaining = max(0, 1 * 1024 * 1024 - data.count)
            data.append(chunk.prefix(remaining))
            lock.unlock()
        }

        func snapshot() -> Data {
            lock.lock()
            defer { lock.unlock() }
            return data
        }
    }

    struct ProcessResult {
        let stdout: String
        let stderr: String
        let exitCode: Int32
    }

    /// Runs a child process and returns its stdout/stderr/exit. On timeout
    /// the process is killed and bounded partial output is returned with
    /// exitCode = -1; pipe readers get a further two seconds each to finish.
    func runProcess(launchPath: String,
                    args: [String],
                    timeout: TimeInterval,
                    captureCompletion: DispatchGroup? = nil) -> ProcessResult {
        guard FileManager.default.isExecutableFile(atPath: launchPath) else {
            return ProcessResult(stdout: "",
                                 stderr: "executable not found: \(launchPath)",
                                 exitCode: -1)
        }
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: launchPath)
        proc.arguments = args
        let outPipe = Pipe()
        let errPipe = Pipe()
        proc.standardOutput = outPipe
        proc.standardError = errPipe

        do {
            try proc.run()
        } catch {
            return ProcessResult(stdout: "",
                                 stderr: "launch failed: \(error.localizedDescription)",
                                 exitCode: -1)
        }

        // Bounded wait. We read pipes after termination to avoid the
        // classic pipe-buffer-fills deadlock on big outputs by draining
        // concurrently below.
        let outQueue = DispatchQueue(label: "diag.out.\(launchPath)")
        let errQueue = DispatchQueue(label: "diag.err.\(launchPath)")
        let outData = BoundedPipeCapture()
        let errData = BoundedPipeCapture()
        let outDone = DispatchSemaphore(value: 0)
        let errDone = DispatchSemaphore(value: 0)
        // Optional observation lets a canary join late reader completion after
        // the bounded result has returned; it does not extend production waits.
        captureCompletion?.enter()
        captureCompletion?.enter()
        outQueue.async {
            defer { captureCompletion?.leave() }
            while true {
                let chunk = outPipe.fileHandleForReading.readData(ofLength: 64 * 1024)
                if chunk.isEmpty { break }
                outData.append(chunk)
            }
            outDone.signal()
        }
        errQueue.async {
            defer { captureCompletion?.leave() }
            while true {
                let chunk = errPipe.fileHandleForReading.readData(ofLength: 64 * 1024)
                if chunk.isEmpty { break }
                errData.append(chunk)
            }
            errDone.signal()
        }

        let deadline = DispatchTime.now() + timeout
        let waitSem = DispatchSemaphore(value: 0)
        let waitQ = DispatchQueue(label: "diag.wait.\(launchPath)")
        waitQ.async {
            proc.waitUntilExit()
            waitSem.signal()
        }
        var exited = waitSem.wait(timeout: deadline) == .success
        let timedOut = !exited
        if !exited {
            if proc.isRunning { proc.terminate() }
            exited = waitSem.wait(timeout: .now() + 1) == .success
            if !exited && proc.isRunning {
                // Last resort: SIGKILL
                kill(proc.processIdentifier, SIGKILL)
            }
            if !exited {
                exited = waitSem.wait(timeout: .now() + 1) == .success
            }
        }
        _ = outDone.wait(timeout: .now() + 2)
        _ = errDone.wait(timeout: .now() + 2)

        let stdout = String(decoding: outData.snapshot(), as: UTF8.self)
        let stderr = String(decoding: errData.snapshot(), as: UTF8.self)
        return ProcessResult(stdout: stdout,
                             stderr: stderr,
                             exitCode: exited && !timedOut ? proc.terminationStatus : -1)
    }

    private static func timestamp() -> String {
        let f = DateFormatter()
        f.dateFormat = "yyyyMMdd-HHmmss"
        f.locale = Locale(identifier: "en_US_POSIX")
        return f.string(from: Date())
    }

    /// Suggested filename for the save panel — exposed so the UI doesn't
    /// have to duplicate the format string.
    public static func suggestedFilename() -> String {
        return "JuiceMount-diagnostics-\(timestamp()).zip"
    }
}
