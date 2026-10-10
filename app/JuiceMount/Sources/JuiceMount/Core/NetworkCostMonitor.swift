import Foundation
import Network

/// Process-lifetime bridge from Network.framework's physical-path cost signal
/// to JuiceMount's link policy. RTT cannot answer whether bytes are billed: a
/// Tailscale hop over a phone hotspot can be 5 ms and still be cellular.
///
/// The monitor starts with ServerController construction. start() waits off the
/// main thread for at most 500 ms for the first path so the Go mount normally
/// receives the class before it chooses immutable JuiceFS launch flags. Live
/// updates keep policy correct across Wi-Fi/cellular transitions.
final class NetworkCostMonitor: @unchecked Sendable {
    static let shared = NetworkCostMonitor()

    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "com.juicemount.network-cost", qos: .utility)
    private let lock = NSLock()
    private let initialPath = DispatchGroup()
    private var started = false
    private var deliveredInitialPath = false
    private var latest: (expensive: Bool, constrained: Bool)?

    private init() {
        initialPath.enter()
    }

    func begin() {
        lock.lock()
        guard !started else {
            lock.unlock()
            return
        }
        started = true
        lock.unlock()

        monitor.pathUpdateHandler = { [weak self] path in
            guard let self else { return }
            let value = (expensive: path.isExpensive, constrained: path.isConstrained)

            self.lock.lock()
            self.latest = value
            let isFirst = !self.deliveredInitialPath
            self.deliveredInitialPath = true
            self.lock.unlock()

            NFSBridge.setNetworkCost(expensive: value.expensive, constrained: value.constrained)
            if isFirst {
                self.initialPath.leave()
            }
        }
        monitor.start(queue: queue)
    }

    /// Called only from ServerController's background work queue.
    func waitForInitialPathAndApply() {
        begin()
        _ = initialPath.wait(timeout: .now() + .milliseconds(500))
        lock.lock()
        let value = latest
        lock.unlock()
        if let value {
            NFSBridge.setNetworkCost(expensive: value.expensive, constrained: value.constrained)
        }
    }
}
