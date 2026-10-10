import Foundation

@main
struct DiagnosticsExporterCanary {
    static func main() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let stage = root.appendingPathComponent("JuiceMount-diagnostics-canary")
        let archive = root.appendingPathComponent("diagnostics.zip")
        let extracted = root.appendingPathComponent("extracted")
        try fm.createDirectory(at: stage, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        let exporter = DiagnosticsExporter()
        let canaries = [
            "canary-split-value-48291",
            "canary-pretty-value-03752",
            "canary-url-value-60934",
            "canary-truncated-value-57413",
            "C4AF-7E19"
        ]
        let splitLog = "event: config\napi_key:\n  \(canaries[0])\n"
        let prettyJSON = "{\n  \"authorization\":\n    \"\(canaries[1])\"\n}\n"
        let urlLog = "redis_url:\n  redis://\(canaries[2])@localhost:6379\n"
        // The bridge's enrollment line carries the fingerprint, which the Mac
        // accepts as proof that a server knows its secret. No other marker.
        let enrollLog = "level=INFO msg=\"enrollment step\" phase=waiting principal=jm-mac-0a1b fingerprint=\(canaries[4]) retry_ms=5000\n"
        try exporter.writeRedacted(splitLog,
                                   to: stage.appendingPathComponent("juicemount.log"),
                                   maxBytes: 1_024)
        try exporter.writeRedacted(prettyJSON,
                                   to: stage.appendingPathComponent("spool.json"),
                                   maxBytes: 1_024)
        try exporter.writeRedacted(urlLog,
                                   to: stage.appendingPathComponent("errors.txt"),
                                   maxBytes: 1_024)
        try exporter.writeRedacted(enrollLog,
                                   to: stage.appendingPathComponent("enroll.log"),
                                   maxBytes: 1_024)
        // Cut inside the label line. The following plain value has no marker
        // of its own, so dropping only the partial first line is unsafe.
        let boundaryLog = root.appendingPathComponent("boundary.log")
        let labeled = "api_key:\n  \(canaries[3])\nsafe status\n"
        try ("prefix\n" + labeled).write(to: boundaryLog, atomically: true, encoding: .utf8)
        try exporter.copyRedactedLog(of: boundaryLog,
                                      to: stage.appendingPathComponent("boundary.log"),
                                      maxBytes: UInt64(labeled.utf8.count - 4))
        try exporter.writeRedacted("safe status: ok\n",
                                   to: stage.appendingPathComponent("status.txt"),
                                   maxBytes: 1_024)

        try runDitto(["-c", "-k", "--keepParent", stage.path, archive.path])
        try fm.createDirectory(at: extracted, withIntermediateDirectories: true)
        try runDitto(["-x", "-k", archive.path, extracted.path])

        let unpacked = extracted.appendingPathComponent(stage.lastPathComponent)
        let files = ["juicemount.log", "spool.json", "errors.txt", "boundary.log", "enroll.log", "status.txt"]
        let contents = try files.map {
            try String(contentsOf: unpacked.appendingPathComponent($0), encoding: .utf8)
        }
        guard !canaries.contains(where: { contents.joined().contains($0) }),
              contents.prefix(5).allSatisfy({ $0.contains("content omitted") }),
              contents[5] == "safe status: ok\n" else {
            print("FAIL: zipped diagnostics canary escaped or safe content changed")
            try? fm.removeItem(at: root)
            exit(1)
        }

        // Exercise timeout while both pipe readers continue draining output.
        for _ in 0..<10 {
            let result = exporter.runProcess(launchPath: "/usr/bin/yes",
                                             args: ["flood"], timeout: 0.01)
            guard result.stdout.utf8.count <= 1 * 1_024 * 1_024,
                  result.stderr.utf8.count <= 1 * 1_024 * 1_024 else {
                fatalError("process output exceeded its cap")
            }
        }
        // The shell exits while its child keeps stdout open past the reader
        // deadline. Late output must not mutate storage owned by the caller.
        let marker = root.appendingPathComponent("late-writer-complete")
        let readers = DispatchGroup()
        let late = exporter.runProcess(launchPath: "/bin/sh",
                                       args: ["-c", "(sleep 6; printf late-output; printf done > \"$1\") & exec /usr/bin/true", "diagnostics-canary", marker.path],
                                       timeout: 0.5, captureCompletion: readers)
        guard !late.stdout.contains("late-output") else {
            fatalError("process output changed after the reader deadline")
        }
        guard readers.wait(timeout: .now() + 10) == .success,
              (try String(contentsOf: marker, encoding: .utf8)) == "done",
              !late.stdout.contains("late-output") else {
            fatalError("late writer or capture readers did not complete independently of returned output")
        }
        print("diagnostics zipped canaries and bounded process capture: PASS")
    }

    private static func runDitto(_ arguments: [String]) throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/ditto")
        process.arguments = arguments
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            fatalError("ditto failed: \(arguments)")
        }
    }
}
