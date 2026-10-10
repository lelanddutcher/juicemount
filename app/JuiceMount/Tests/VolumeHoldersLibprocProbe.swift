import Foundation
import Darwin

// Real-libproc probe for the holder classification. Uses the local Data
// volume (no network mount, nothing mounted or detached): one child whose
// only hold is its working directory, one with an open file and cwd "/".
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/QuitEjectCountdown.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/MountTable.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/VolumeHolders.swift \
//     app/JuiceMount/Tests/VolumeHoldersLibprocProbe.swift -o <scratch>/p && <scratch>/p <scratch-dir-on-Data>
@main
struct VolumeHoldersLibprocProbe {
    static func main() throws {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        guard CommandLine.arguments.count == 2 else { print("usage: probe <dir on /System/Volumes/Data>"); exit(2) }
        let dir = CommandLine.arguments[1]
        guard let data = MountTable.fsid(of: "/System/Volumes/Data") else { print("FAIL: no Data volume in mount table"); exit(1) }
        let file = dir + "/holder-probe.txt"
        FileManager.default.createFile(atPath: file, contents: Data("x".utf8))
        let null = FileHandle(forUpdatingAtPath: "/dev/null")!

        let cwdOnly = Process()
        cwdOnly.executableURL = URL(fileURLWithPath: "/bin/sleep")
        cwdOnly.arguments = ["20"]
        cwdOnly.currentDirectoryURL = URL(fileURLWithPath: dir)
        cwdOnly.standardInput = null; cwdOnly.standardOutput = null; cwdOnly.standardError = null

        let fileHolder = Process()
        fileHolder.executableURL = URL(fileURLWithPath: "/bin/sleep")
        fileHolder.arguments = ["20"]
        fileHolder.currentDirectoryURL = URL(fileURLWithPath: "/")
        fileHolder.standardInput = FileHandle(forReadingAtPath: file)!
        fileHolder.standardOutput = null; fileHolder.standardError = null

        try cwdOnly.run()
        try fileHolder.run()
        defer { cwdOnly.terminate(); fileHolder.terminate(); try? FileManager.default.removeItem(atPath: file) }
        Thread.sleep(forTimeInterval: 0.3)

        check(VolumeHolders.hold(pid: cwdOnly.processIdentifier, volume: data) == .workingDirectoryOnly,
              "shell-like process with only its cwd on the volume must be workingDirectoryOnly")
        check(VolumeHolders.hold(pid: fileHolder.processIdentifier, volume: data) == .openFiles,
              "process with an open file on the volume must be openFiles")

        let census = VolumeHolders.group([
            .init(pid: 1, name: "Terminal", kind: .workingDirectoryOnly),
            .init(pid: 2, name: "Premiere Pro", kind: .openFiles),
            .init(pid: 3, name: "Premiere Pro", kind: .workingDirectoryOnly),
        ])
        check(census == .init(fileApps: ["Premiere Pro"], folderApps: ["Terminal"]), "grouping: \(census)")

        if failures > 0 { exit(1) }
        print("PASS: libproc holder classification")
    }
}
