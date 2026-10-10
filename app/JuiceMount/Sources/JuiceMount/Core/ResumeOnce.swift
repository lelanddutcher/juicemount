import Foundation

/// Resumes a continuation exactly once from any thread.
final class ResumeOnce<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var cont: CheckedContinuation<T, Never>?
    private let onFirst: (() -> Void)?

    init(_ cont: CheckedContinuation<T, Never>, onFirst: (() -> Void)? = nil) {
        self.cont = cont
        self.onFirst = onFirst
    }

    func resume(_ value: T) {
        lock.lock()
        guard let c = cont else { lock.unlock(); return }
        cont = nil
        lock.unlock()
        onFirst?()
        c.resume(returning: value)
    }
}
