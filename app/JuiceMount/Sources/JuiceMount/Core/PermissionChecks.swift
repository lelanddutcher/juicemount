import Foundation
import Darwin
import ServiceManagement

// Setup Assistant permission checks: the PURE classification half. The probes
// that produce these inputs live in PermissionProbes.swift. Keeping the
// decisions free of I/O lets Tests/PermissionCheckProbe.swift pin every
// verdict without touching TCC, launchd or the network.
//
// Why there is no Full Disk Access row: nothing in the app, the serving
// service, or the bundled helpers reads a TCC-protected user location (Mail,
// Safari, other apps' containers, TCC.db). The old "needs FDA to read its own
// FUSE mount" note predates the macOS split of Network Volumes out of FDA; that
// access is now the Network Volumes row. Asking for FDA would grant far more
// than JuiceMount uses.

/// Verdict for one permission row.
enum PermissionStatus: Equatable {
    case granted
    /// The user must act; `detail` says what macOS refused.
    case denied(String)
    /// macOS has not answered yet (prompt showing, or filesystem not
    /// responding). Neutral: the probe cannot tell those apart.
    case pending(String)
    /// Nothing to check yet; macOS will ask at the named moment.
    case notYetRequested(String)
    /// The probe could not decide. Never shown as a failure.
    case unknown(String)
    /// The permission does not govern this configuration.
    case notApplicable(String)

    /// Rows show a fix button only when the user can act on something.
    var needsUserAction: Bool {
        switch self {
        case .denied, .pending: return true
        default: return false
        }
    }
}

enum PermissionClassifier {

    // MARK: Network Volumes (TCC)

    /// Outcome of a bounded open(2) of the mounted cache-volume root by the
    /// app process.
    enum VolumeOpenOutcome: Equatable {
        case notMounted
        case opened
        case failed(errno: Int32)
        /// The open did not return within the bound (or a previous probe is
        /// still parked in the kernel).
        case timedOut
    }

    static func networkVolumes(_ outcome: VolumeOpenOutcome) -> PermissionStatus {
        switch outcome {
        case .notMounted:
            return .notYetRequested("macOS will ask for Network Volumes access the first time JuiceMount mounts its volume.")
        case .opened:
            return .granted
        case .failed(let code) where code == EPERM || code == EACCES:
            return .denied("macOS Network Volumes access was not granted to JuiceMount.")
        case .failed(let code):
            // ENOENT/ENOTCONN/ENXIO etc. describe the mount, not the grant.
            return .unknown("The volume could not be opened (\(String(cString: strerror(code)))); this is not a permission answer.")
        case .timedOut:
            return .pending("Waiting for macOS to open the volume — a pending Network Volumes prompt or an unresponsive filesystem.")
        }
    }

    // MARK: Local Network

    /// Outcome of a bounded TCP dial to the configured Redis host.
    enum DialOutcome: Equatable {
        case connected
        /// The host answered with a reset: packets left this Mac, so the
        /// Local Network permission is not blocking it.
        case refused
        /// Network.framework reported the path unsatisfied for
        /// `.localNetworkDenied` — the OS's own answer.
        case pathLocalNetworkDenied
        case failed(errno: Int32)
        case timedOut
        case noTarget(String)
    }

    /// - privateTarget: the dial target is an RFC1918 / link-local address,
    ///   the only kind of address Local Network privacy governs.
    /// - serviceReportsPermission: the serving engine's health monitor
    ///   reports "local-network-permission" (health/localnet.go applies the
    ///   attached-subnet and network-up tests that rule out a plain outage).
    static func localNetwork(_ outcome: DialOutcome, privateTarget: Bool, serviceReportsPermission: Bool) -> PermissionStatus {
        if serviceReportsPermission {
            return .denied("The background service cannot reach your NAS because macOS Local Network access is off for JuiceMount.")
        }
        switch outcome {
        case .noTarget(let why):
            return .notYetRequested(why)
        case .connected, .refused:
            return .granted
        case .pathLocalNetworkDenied:
            return .denied("macOS Local Network access is off for JuiceMount.")
        case .failed(let code) where code == EPERM || code == EACCES:
            // A policy refusal is never an ordinary outage.
            return privateTarget
                ? .denied("macOS refused the connection to your NAS (Local Network access is off for JuiceMount).")
                : .unknown("The connection was refused by a system policy (\(String(cString: strerror(code)))).")
        case .failed(let code) where code == EHOSTUNREACH || code == ENETUNREACH:
            // Same errno as a powered-off NAS on the attached subnet. Do not
            // claim a denial without the engine's stronger evidence.
            return privateTarget
                ? .unknown("No route to your NAS. If the NAS is on and other devices reach it, turn on Local Network access for JuiceMount.")
                : .notApplicable("The backend is not on the local network; macOS Local Network access does not apply.")
        case .failed(let code):
            return .unknown("The backend dial failed (\(String(cString: strerror(code)))); this is not a permission answer.")
        case .timedOut:
            return .unknown("The backend did not answer in time; this is not a permission answer.")
        }
    }

    /// True for the address families Local Network privacy governs.
    static func isLocalNetworkAddress(_ host: String) -> Bool {
        var v4 = in_addr()
        if inet_pton(AF_INET, host, &v4) == 1 {
            let b = withUnsafeBytes(of: v4.s_addr) { Array($0) } // network order
            switch (b[0], b[1]) {
            case (10, _): return true
            case (172, 16...31): return true
            case (192, 168): return true
            case (169, 254): return true
            default: return false
            }
        }
        var v6 = in6_addr()
        if inet_pton(AF_INET6, host, &v6) == 1 {
            let b = withUnsafeBytes(of: v6) { Array($0) }
            if b[0] == 0xfe && (b[1] & 0xc0) == 0x80 { return true } // fe80::/10
            if (b[0] & 0xfe) == 0xfc { return true }                  // fc00::/7
        }
        return false
    }

    // MARK: Background service approval (Login Items)

    static func serviceApproval(_ status: SMAppService.Status, isolated: Bool) -> PermissionStatus {
        if isolated {
            return .notApplicable("Isolated test install: the background service is started by its harness.")
        }
        switch status {
        case .enabled:
            return .granted
        case .requiresApproval:
            return .denied("JuiceMount's background service is waiting for approval in System Settings → General → Login Items & Extensions.")
        case .notRegistered, .notFound:
            return .notYetRequested("JuiceMount registers its background service the first time it starts; macOS may then ask you to allow it.")
        @unknown default:
            return .unknown("macOS reported an unknown approval state for the background service.")
        }
    }
}
