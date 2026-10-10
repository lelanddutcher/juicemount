import Darwin

/// Kernel mount table, read with getfsstat(MNT_NOWAIT): the kernel's cached
/// mount list, without calling into any mounted filesystem, so a wedged
/// mount cannot block it.
enum MountTable {
    static func isMounted(_ path: String) -> Bool {
        fsid(of: path) != nil
    }

    /// The fsid of the mount whose mount-on path is exactly `path`.
    static func fsid(of path: String) -> fsid_t? {
        let want = path.count > 1 && path.hasSuffix("/") ? String(path.dropLast()) : path
        let count = getfsstat(nil, 0, MNT_NOWAIT)
        guard count > 0 else { return nil }
        let capacity = Int(count) + 8 // mounts may appear between the calls
        let buf = UnsafeMutablePointer<statfs>.allocate(capacity: capacity)
        defer { buf.deallocate() }
        let got = getfsstat(buf, Int32(MemoryLayout<statfs>.stride * capacity), MNT_NOWAIT)
        guard got > 0 else { return nil }
        for i in 0..<Int(got) {
            let onName = withUnsafeBytes(of: buf[i].f_mntonname) { raw in
                String(cString: raw.bindMemory(to: CChar.self).baseAddress!)
            }
            if onName == want { return buf[i].f_fsid }
        }
        return nil
    }
}
