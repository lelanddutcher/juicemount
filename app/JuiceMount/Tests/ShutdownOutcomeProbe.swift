import Foundation

// Compile with the production Core/ShutdownOutcome.swift:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/ShutdownOutcome.swift \
//     app/JuiceMount/Tests/ShutdownOutcomeProbe.swift -o <scratch>/probe && <scratch>/probe
@main
struct ShutdownOutcomeProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        func outcome(_ reason: String) -> ShutdownOutcome {
            ShutdownOutcome.resolve(cResult: 0, fetchedJSON: #"{"complete":false,"reason":"\#(reason)","detail":"d"}"#)
        }

        // The C result is authoritative in both directions.
        check(ShutdownOutcome.resolve(cResult: 1, fetchedJSON: #"{"complete":false,"reason":"incomplete"}"#).complete, "nonzero C result is complete")
        let contradicted = ShutdownOutcome.resolve(cResult: 0, fetchedJSON: #"{"complete":true,"reason":"complete"}"#)
        check(!contradicted.complete && contradicted.reason == .incomplete, "stale 'complete' JSON must not turn a failed shutdown into success")
        let lost = ShutdownOutcome.resolve(cResult: 0, fetchedJSON: "error: serving service unavailable")
        check(!lost.complete && lost.reason == .incomplete, "lost reply degrades to generic incomplete")
        check(ShutdownOutcome.resolve(cResult: 0, fetchedJSON: nil).reason == .incomplete, "nil reply degrades to generic incomplete")
        check(ShutdownOutcome.resolve(cResult: 0, fetchedJSON: "{").reason == .incomplete, "garbage reply degrades to generic incomplete")
        check(outcome("network_volumes_permission_denied").reason == .networkVolumesPermissionDenied, "decode NV denied")
        check(outcome("fuse_admission_pending").reason == .fuseAdmissionPending, "decode admission pending")
        check(outcome("fuse_or_link_retained").reason == .fuseOrLinkRetained, "decode retained")
        check(outcome("brand_new").reason == .unknown("brand_new"), "unknown reason kept as unknown")
        check(outcome("network_volumes_permission_denied").detail == "d", "detail carried")

        // The "not granted" wording appears ONLY for the denied reason.
        let denied = "macOS Network Volumes access was not granted"
        for reason in ["fuse_admission_pending", "fuse_or_link_retained", "incomplete", "", "brand_new"] {
            let m = ShutdownMessage.forOutcome(outcome(reason))
            check(!m.title.contains("not granted") && !m.body.contains("not granted"), "\(reason): must not claim Network Volumes was not granted")
            check(!ShutdownMessage.statusLine(outcome(reason)).contains("not granted"), "\(reason): status line must not claim denial")
        }
        let nv = ShutdownMessage.forOutcome(outcome("network_volumes_permission_denied"))
        check(nv.title == denied, "NV denied title")
        check(nv.offersPrivacySettings, "NV denied offers the settings link")
        check(ShutdownMessage.statusLine(outcome("network_volumes_permission_denied")).contains(denied), "NV denied status line")
        let pending = ShutdownMessage.forOutcome(outcome("fuse_admission_pending"))
        check(pending.title.contains("waiting for macOS to open the volume") || pending.title.contains("Waiting for macOS to open the volume"), "pending names the wait")
        check(pending.title.contains("a pending Network Volumes prompt or an unresponsive filesystem"), "pending wording stays neutral")
        check(!ShutdownMessage.forOutcome(outcome("incomplete")).offersPrivacySettings, "generic incomplete does not send the user to privacy settings")

        if failures > 0 { exit(1) }
        print("PASS: shutdown outcome mapping")
    }
}
