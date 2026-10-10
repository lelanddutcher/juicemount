import Foundation
import Darwin

/// Network Volumes check: a bounded open(2) of the mounted cache-volume root
/// by this app process. Single-flight across calls: a parked open cannot be
/// cancelled, so focus-driven re-checks must never start a second one while
/// the first is still stuck in the kernel (they answer `.timedOut` instead).
/// Seams (`isMounted`, `openRoot`) let Tests/RecheckSingleFlightProbe.swift
/// park an open without any filesystem.
enum VolumeAccessProbe {
    private static let lock = NSLock()
    private static var parked = false
    /// Opens actually started; read by the probe.
    private(set) static var opensStarted = 0

    private static func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if parked { return false }
        parked = true
        opensStarted += 1
        return true
    }

    private static func release() {
        lock.lock()
        parked = false
        lock.unlock()
    }

    /// Returns 0 on success or the errno of the failed open.
    static func openAndClose(_ path: String) -> Int32 {
        let fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC)
        let code = errno
        if fd >= 0 { close(fd); return 0 }
        return code
    }

    static func probe(path: String,
                      timeout: TimeInterval,
                      isMounted: (String) -> Bool = MountTable.isMounted,
                      openRoot: @escaping @Sendable (String) -> Int32 = VolumeAccessProbe.openAndClose) async -> PermissionClassifier.VolumeOpenOutcome {
        guard isMounted(path) else { return .notMounted }
        guard claim() else { return .timedOut }
        return await withCheckedContinuation { (cont: CheckedContinuation<PermissionClassifier.VolumeOpenOutcome, Never>) in
            let once = ResumeOnce(cont)
            // A dedicated thread, not GCD: a parked open must not occupy a
            // shared worker.
            Thread.detachNewThread {
                let code = openRoot(path)
                release()
                once.resume(code == 0 ? .opened : .failed(errno: code))
            }
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout) {
                once.resume(.timedOut)
            }
        }
    }
}
