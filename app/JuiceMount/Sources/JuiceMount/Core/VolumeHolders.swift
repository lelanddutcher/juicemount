import Foundation
import AppKit
import Darwin

/// Which other apps hold the JuiceMount volume, and how. Feeds the busy Quit
/// warning (QuitHolderPlan / QuitEjectCountdown). Read-only: it lists
/// processes and never signals, closes or detaches anything.
///
/// proc_listpidspath(PATH_IS_VOLUME) also returns a process whose working
/// directory is on the volume (a Terminal shell cd'd into it). Such a process
/// has no unsaved work, so each listed pid is re-examined: its vnode file
/// descriptors and its current/root directory are compared by fsid with the
/// volume, and the result is classified by VolumeHoldKind.
enum VolumeHolders {
    struct Holder: Equatable {
        let pid: pid_t
        let name: String
        let kind: VolumeHoldKind
    }

    struct Census: Equatable {
        /// Distinct app names with open files on the volume.
        var fileApps: [String]
        /// Distinct app names whose only hold is a working directory.
        var folderApps: [String]
    }

    private static let lock = NSLock()
    private static var parked = false

    /// Holders of the volume mounted at `path`, or nil when the answer is
    /// unknown: not mounted, the listing failed, or it did not return within
    /// `timeout` (libproc resolves vnodes, which a hung network mount can
    /// block). Processes from this app bundle (the GUI, the serving service,
    /// its helpers) are excluded: JuiceMount's own handles are its
    /// shutdown's business, not another app's unsaved work.
    static func census(path: String, timeout: TimeInterval = 2) async -> Census? {
        guard let volume = MountTable.fsid(of: path) else { return nil }
        guard claim() else { return nil }
        let bundlePath = Bundle.main.bundleURL.resolvingSymlinksInPath().path + "/"
        let ownPid = getpid()
        return await withCheckedContinuation { (cont: CheckedContinuation<Census?, Never>) in
            let gate = NSLock()
            var done = false
            func finish(_ value: Census?) {
                gate.lock()
                if done { gate.unlock(); return }
                done = true
                gate.unlock()
                cont.resume(returning: value)
            }
            Thread.detachNewThread {
                let pids = listPids(onVolume: path)
                let holders = pids.map { examine(pids: $0, volume: volume, ownPid: ownPid, bundlePath: bundlePath) }
                release()
                guard let holders else { finish(nil); return }
                finish(group(holders))
            }
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + timeout) { finish(nil) }
        }
    }

    /// Single-flight: a listing parked on a hung mount cannot be cancelled,
    /// so a recount must not start a second one.
    private static func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if parked { return false }
        parked = true
        return true
    }

    private static func release() {
        lock.lock()
        parked = false
        lock.unlock()
    }

    private static func listPids(onVolume path: String) -> [pid_t]? {
        let flags = UInt32(PROC_LISTPIDSPATH_PATH_IS_VOLUME | PROC_LISTPIDSPATH_EXCLUDE_EVTONLY)
        let needed = proc_listpidspath(UInt32(PROC_ALL_PIDS), 0, path, flags, nil, 0)
        guard needed > 0 else { return needed == 0 ? [] : nil }
        let slots = Int(needed) / MemoryLayout<pid_t>.stride + 32
        var pids = [pid_t](repeating: 0, count: slots)
        let got = pids.withUnsafeMutableBytes { raw in
            proc_listpidspath(UInt32(PROC_ALL_PIDS), 0, path, flags, raw.baseAddress, Int32(raw.count))
        }
        guard got >= 0 else { return nil }
        return Array(pids.prefix(Int(got) / MemoryLayout<pid_t>.stride)).filter { $0 > 0 }
    }

    private static func examine(pids: [pid_t], volume: fsid_t, ownPid: pid_t, bundlePath: String) -> [Holder] {
        var out: [Holder] = []
        for pid in pids where pid != ownPid {
            let exe = executablePath(pid)
            if !exe.isEmpty && exe.hasPrefix(bundlePath) { continue }
            let kind = hold(pid: pid, volume: volume)
            let name = NSRunningApplication(processIdentifier: pid)?.localizedName
                ?? (exe.isEmpty ? "process \(pid)" : (exe as NSString).lastPathComponent)
            out.append(Holder(pid: pid, name: name, kind: kind))
        }
        return out
    }

    /// Distinct names; an app with open files in any process is a file app.
    static func group(_ holders: [Holder]) -> Census {
        var files: [String] = []
        var folders: [String] = []
        for h in holders where h.kind == .openFiles && !files.contains(h.name) { files.append(h.name) }
        for h in holders where h.kind == .workingDirectoryOnly && !files.contains(h.name) && !folders.contains(h.name) {
            folders.append(h.name)
        }
        return Census(fileApps: files, folderApps: folders)
    }

    /// How `pid` holds the volume identified by `volume`.
    static func hold(pid: pid_t, volume: fsid_t) -> VolumeHoldKind {
        VolumeHoldKind.classify(
            fileOnVolume: hasVnodeDescriptor(pid: pid, on: volume),
            cwdOnVolume: directoryOnVolume(pid: pid, on: volume))
    }

    private static func same(_ a: fsid_t, _ b: fsid_t) -> Bool {
        a.val.0 == b.val.0 && a.val.1 == b.val.1
    }

    private static func hasVnodeDescriptor(pid: pid_t, on volume: fsid_t) -> Bool {
        let bytes = proc_pidinfo(pid, PROC_PIDLISTFDS, 0, nil, 0)
        guard bytes > 0 else { return false }
        let stride = MemoryLayout<proc_fdinfo>.stride
        var fds = [proc_fdinfo](repeating: proc_fdinfo(), count: Int(bytes) / stride + 16)
        let got = fds.withUnsafeMutableBytes { raw in
            proc_pidinfo(pid, PROC_PIDLISTFDS, 0, raw.baseAddress, Int32(raw.count))
        }
        guard got > 0 else { return false }
        for fd in fds.prefix(Int(got) / stride) where fd.proc_fdtype == UInt32(PROX_FDTYPE_VNODE) {
            var info = vnode_fdinfowithpath()
            let size = Int32(MemoryLayout<vnode_fdinfowithpath>.size)
            if proc_pidfdinfo(pid, fd.proc_fd, PROC_PIDFDVNODEPATHINFO, &info, size) == size,
               same(info.pvip.vip_vi.vi_fsid, volume) {
                return true
            }
        }
        return false
    }

    private static func directoryOnVolume(pid: pid_t, on volume: fsid_t) -> Bool {
        var info = proc_vnodepathinfo()
        let size = Int32(MemoryLayout<proc_vnodepathinfo>.size)
        guard proc_pidinfo(pid, PROC_PIDVNODEPATHINFO, 0, &info, size) == size else { return false }
        return same(info.pvi_cdir.vip_vi.vi_fsid, volume) || same(info.pvi_rdir.vip_vi.vi_fsid, volume)
    }

    private static func executablePath(_ pid: pid_t) -> String {
        var buf = [CChar](repeating: 0, count: Int(MAXPATHLEN) * 4)
        return proc_pidpath(pid, &buf, UInt32(buf.count)) > 0 ? String(cString: buf) : ""
    }
}
