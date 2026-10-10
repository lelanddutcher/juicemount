import Foundation
import AppKit
import Darwin
import Network
import ServiceManagement

// Setup Assistant permission checks: the I/O half. Every probe is bounded so
// the assistant can never hang, and is safe to re-run whenever the window
// regains focus (the user returning from System Settings).
//
// Honesty limits, stated once:
//  - macOS offers no API to read another app's TCC grant. The Network Volumes
//    probe measures what THIS app process gets when it opens the mounted
//    cache-volume root. The authoritative answer for the service's own FUSE
//    custodian is the typed shutdown reason (ShutdownOutcome).
//  - An in-app ping cannot prove the network is fine, because Local Network
//    privacy blocks the app's ICMP too. The Local Network row therefore uses
//    Network.framework's own `.localNetworkDenied` path verdict, a policy
//    errno, or the serving engine's attached-subnet classifier
//    (health/localnet.go); a bare "no route to host" stays "unknown".

struct PermissionReport: Equatable {
    var networkVolumes: PermissionStatus
    var localNetwork: PermissionStatus
    var serviceApproval: PermissionStatus
}

enum PermissionProbes {
    static let probeTimeout: TimeInterval = 3

    /// Run all three checks concurrently. Worst case ≈ `probeTimeout`.
    static func run(redisURL: String, volumePath: String, serviceReportsLocalNetwork: Bool) async -> PermissionReport {
        async let volume = VolumeAccessProbe.probe(path: volumePath, timeout: probeTimeout)
        async let dial = probeLocalNetwork(redisURL: redisURL)
        let approval = PermissionClassifier.serviceApproval(
            ServingService.approvalStatus, isolated: AppIsolation.active != nil)
        let (dialOutcome, privateTarget) = await dial
        return PermissionReport(
            networkVolumes: PermissionClassifier.networkVolumes(await volume),
            localNetwork: PermissionClassifier.localNetwork(
                dialOutcome, privateTarget: privateTarget,
                serviceReportsPermission: serviceReportsLocalNetwork),
            serviceApproval: approval)
    }

    // MARK: Local Network

    static func probeLocalNetwork(redisURL: String, timeout: TimeInterval = probeTimeout) async -> (PermissionClassifier.DialOutcome, Bool) {
        let host: String
        let port: UInt16
        switch OnboardingPreflight.parseRedisHostPort(redisURL) {
        case .success(let target):
            (host, port) = target
        case .failure(let why):
            return (.noTarget(why), false)
        }
        // Hostnames are not resolved here (resolution could block); only an
        // IP literal or an mDNS .local name is known to be on the LAN.
        let privateTarget = PermissionClassifier.isLocalNetworkAddress(host)
            || host.lowercased().hasSuffix(".local")
        let outcome = await dial(host: host, port: port, timeout: timeout)
        return (outcome, privateTarget)
    }

    private static func dial(host: String, port: UInt16, timeout: TimeInterval) async -> PermissionClassifier.DialOutcome {
        await withCheckedContinuation { (cont: CheckedContinuation<PermissionClassifier.DialOutcome, Never>) in
            let conn = NWConnection(
                host: NWEndpoint.Host(host),
                port: NWEndpoint.Port(rawValue: port) ?? 6379,
                using: .tcp)
            let queue = DispatchQueue(label: "com.juicemount.permission.dial")
            // Independent timer queue, same reason as tcpReachable (#87).
            let timerQueue = DispatchQueue(label: "com.juicemount.permission.timeout")
            let once = ResumeOnce(cont) { conn.cancel() }
            conn.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    once.resume(.connected)
                case .waiting(let error), .failed(let error):
                    if conn.currentPath?.unsatisfiedReason == .localNetworkDenied {
                        once.resume(.pathLocalNetworkDenied)
                        return
                    }
                    if case .posix(let code) = error {
                        once.resume(code == .ECONNREFUSED ? .refused : .failed(errno: code.rawValue))
                    } else {
                        once.resume(.failed(errno: 0))
                    }
                default:
                    break
                }
            }
            conn.start(queue: queue)
            timerQueue.asyncAfter(deadline: .now() + timeout) { once.resume(.timedOut) }
        }
    }
}

/// System Settings deep links. Opening a pane is the only action taken; the
/// app never toggles a privacy setting itself.
enum SystemSettingsLinks {
    /// Network Volumes is listed per app under Files & Folders.
    static let filesAndFolders = URL(string: "x-apple.systempreferences:com.apple.preference.security?Privacy_FilesAndFolders")!
    static let localNetwork = URL(string: "x-apple.systempreferences:com.apple.preference.security?Privacy_LocalNetwork")!

    static func open(_ url: URL) { NSWorkspace.shared.open(url) }
    static func openLoginItems() { SMAppService.openSystemSettingsLoginItems() }
}
