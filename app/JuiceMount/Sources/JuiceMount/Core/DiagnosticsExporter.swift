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
        let stageRoot = fm.temporaryDirectory.appendingPathComponent(stageName)
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
            try? body.write(to: errorsURL, atomically: true, encoding: .utf8)
        }

        // 3. Zip via ditto. -c create, -k keep PKZip format,
        //    --keepParent preserves the top-level folder inside the zip
        //    so unzipping yields one tidy directory.
        let tmpZip = fm.temporaryDirectory
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

        log.info("Diagnostics exported to \(destination.path, privacy: .public)")
    }

    // MARK: - Collectors

    private func collectAppLog(into stage: URL, errors: inout [String]) {
        let logURL = FileManager.default
            .homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Logs/JuiceMount/juicemount.log")
        let dest = stage.appendingPathComponent("juicemount.log")
        do {
            try copyLastBytes(of: logURL, to: dest, maxBytes: 5 * 1024 * 1024)
        } catch {
            errors.append("juicemount.log: \(error.localizedDescription)")
        }
    }

    private func collectJuiceFSLog(into stage: URL, errors: inout [String]) {
        let logURL = FileManager.default
            .homeDirectoryForCurrentUser
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
            try copyLastBytes(of: logURL, to: dest, maxBytes: 1 * 1024 * 1024)
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
        let sem = DispatchSemaphore(value: 0)
        var data: Data?
        var err: Error?
        URLSession.shared.dataTask(with: req) { d, _, e in
            data = d
            err = e
            sem.signal()
        }.resume()
        // Wait up to 6s for the request to complete (URLSession honors the
        // 5s timeout above; this is a safety net for the semaphore wait).
        _ = sem.wait(timeout: .now() + 6)
        if let err {
            errors.append("\(file): \(err.localizedDescription)")
            try? "(metrics endpoint unreachable: \(err.localizedDescription))\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        guard let data else {
            errors.append("\(file): no data and no error (timeout?)")
            try? "(no response within 6s)\n"
                .write(to: dest, atomically: true, encoding: .utf8)
            return
        }
        do {
            try data.write(to: dest)
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
        try? head.write(to: dest, atomically: true, encoding: .utf8)
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
        try? body.write(to: dest, atomically: true, encoding: .utf8)
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
        try? result.stdout.write(to: dest, atomically: true, encoding: .utf8)
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
            try body.write(to: dest, atomically: true, encoding: .utf8)
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

    /// Copies the last `maxBytes` of `src` to `dest`. If the source is
    /// smaller than the cap, the whole file is copied. Throws if the file
    /// is missing or unreadable.
    private func copyLastBytes(of src: URL,
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

        let size = (try? handle.seekToEnd()) ?? 0
        let start: UInt64 = size > maxBytes ? size - maxBytes : 0
        try handle.seek(toOffset: start)
        let data = handle.readDataToEndOfFile()
        try data.write(to: dest)
    }

    private struct ProcessResult {
        let stdout: String
        let stderr: String
        let exitCode: Int32
    }

    /// Runs a child process and returns its stdout/stderr/exit. Bounded
    /// by `timeout` seconds — on overrun the process is killed and the
    /// partial output is returned with exitCode = -1.
    private func runProcess(launchPath: String,
                            args: [String],
                            timeout: TimeInterval) -> ProcessResult {
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
        var outData = Data()
        var errData = Data()
        let outDone = DispatchSemaphore(value: 0)
        let errDone = DispatchSemaphore(value: 0)
        outQueue.async {
            outData = outPipe.fileHandleForReading.readDataToEndOfFile()
            outDone.signal()
        }
        errQueue.async {
            errData = errPipe.fileHandleForReading.readDataToEndOfFile()
            errDone.signal()
        }

        let deadline = DispatchTime.now() + timeout
        let waitSem = DispatchSemaphore(value: 0)
        let waitQ = DispatchQueue(label: "diag.wait.\(launchPath)")
        waitQ.async {
            proc.waitUntilExit()
            waitSem.signal()
        }
        if waitSem.wait(timeout: deadline) == .timedOut {
            if proc.isRunning { proc.terminate() }
            _ = waitSem.wait(timeout: .now() + 1)
            if proc.isRunning {
                // Last resort: SIGKILL
                kill(proc.processIdentifier, SIGKILL)
            }
        }
        _ = outDone.wait(timeout: .now() + 2)
        _ = errDone.wait(timeout: .now() + 2)

        let stdout = String(data: outData, encoding: .utf8) ?? ""
        let stderr = String(data: errData, encoding: .utf8) ?? ""
        return ProcessResult(stdout: stdout,
                             stderr: stderr,
                             exitCode: proc.terminationStatus)
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
