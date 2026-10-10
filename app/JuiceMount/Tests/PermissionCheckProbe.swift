import Darwin
import ServiceManagement

// Compile with the production Core/PermissionChecks.swift:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/PermissionChecks.swift \
//     app/JuiceMount/Tests/PermissionCheckProbe.swift -o <scratch>/probe && <scratch>/probe
@main
struct PermissionCheckProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        func kind(_ s: PermissionStatus) -> String {
            switch s {
            case .granted: return "granted"
            case .denied: return "denied"
            case .pending: return "pending"
            case .notYetRequested: return "notYetRequested"
            case .unknown: return "unknown"
            case .notApplicable: return "notApplicable"
            }
        }
        typealias C = PermissionClassifier

        // Network Volumes: only EPERM/EACCES is a denial; a timeout is a
        // neutral pending; other errnos describe the mount, not the grant.
        check(kind(C.networkVolumes(.notMounted)) == "notYetRequested", "unmounted volume must say 'requested at first mount'")
        check(kind(C.networkVolumes(.opened)) == "granted", "opened root is granted")
        check(kind(C.networkVolumes(.failed(errno: EPERM))) == "denied", "EPERM is denied")
        check(kind(C.networkVolumes(.failed(errno: EACCES))) == "denied", "EACCES is denied")
        check(kind(C.networkVolumes(.failed(errno: ENOTCONN))) == "unknown", "ENOTCONN is not a permission answer")
        check(kind(C.networkVolumes(.timedOut)) == "pending", "timeout is pending")
        if case .pending(let s) = C.networkVolumes(.timedOut) {
            check(s.contains("pending Network Volumes prompt or an unresponsive filesystem"), "pending wording must stay neutral")
            check(!s.contains("not granted"), "pending must never claim a denial")
        }

        // Local Network: the OS verdict and policy errnos deny; a refusal
        // proves egress; no-route alone is ambiguous unless the engine's
        // attached-subnet classifier agrees.
        check(kind(C.localNetwork(.connected, privateTarget: true, serviceReportsPermission: false)) == "granted", "connected is granted")
        check(kind(C.localNetwork(.refused, privateTarget: true, serviceReportsPermission: false)) == "granted", "ECONNREFUSED proves LAN egress")
        check(kind(C.localNetwork(.pathLocalNetworkDenied, privateTarget: true, serviceReportsPermission: false)) == "denied", "localNetworkDenied path is denied")
        check(kind(C.localNetwork(.failed(errno: EPERM), privateTarget: true, serviceReportsPermission: false)) == "denied", "EPERM to a LAN target is denied")
        check(kind(C.localNetwork(.failed(errno: EHOSTUNREACH), privateTarget: true, serviceReportsPermission: false)) == "unknown", "bare EHOSTUNREACH (NAS may be off) must not claim denial")
        check(kind(C.localNetwork(.failed(errno: EHOSTUNREACH), privateTarget: true, serviceReportsPermission: true)) == "denied", "engine permission verdict upgrades to denied")
        check(kind(C.localNetwork(.failed(errno: EHOSTUNREACH), privateTarget: false, serviceReportsPermission: false)) == "notApplicable", "public target is not a Local Network matter")
        check(kind(C.localNetwork(.timedOut, privateTarget: true, serviceReportsPermission: false)) == "unknown", "timeout is unknown")
        check(kind(C.localNetwork(.noTarget("x"), privateTarget: false, serviceReportsPermission: false)) == "notYetRequested", "no Redis URL yet")

        check(C.isLocalNetworkAddress("192.168.0.197"), "192.168/16 private")
        check(C.isLocalNetworkAddress("10.1.2.3"), "10/8 private")
        check(C.isLocalNetworkAddress("172.20.0.1"), "172.16/12 private")
        check(!C.isLocalNetworkAddress("172.32.0.1"), "172.32 is public")
        check(C.isLocalNetworkAddress("169.254.10.2"), "IPv4 link-local")
        check(!C.isLocalNetworkAddress("8.8.8.8"), "public IPv4")
        check(!C.isLocalNetworkAddress("127.0.0.1"), "loopback is exempt")
        check(C.isLocalNetworkAddress("fe80::1"), "IPv6 link-local")
        check(C.isLocalNetworkAddress("fd00::5"), "IPv6 ULA")
        check(!C.isLocalNetworkAddress("nas.example.com"), "hostname is not assumed private")

        // Background service approval.
        check(kind(C.serviceApproval(.enabled, isolated: false)) == "granted", "enabled agent")
        check(kind(C.serviceApproval(.requiresApproval, isolated: false)) == "denied", "requiresApproval needs Login Items")
        check(C.serviceApproval(.requiresApproval, isolated: false).needsUserAction, "requiresApproval shows a fix button")
        check(kind(C.serviceApproval(.notRegistered, isolated: false)) == "notYetRequested", "unregistered before first start")
        check(kind(C.serviceApproval(.notFound, isolated: false)) == "notYetRequested", "never-registered (.notFound) before first start")
        check(kind(C.serviceApproval(.requiresApproval, isolated: true)) == "notApplicable", "isolated install never asks for approval")

        if failures > 0 { exit(1) }
        print("PASS: permission classification")
    }
}
