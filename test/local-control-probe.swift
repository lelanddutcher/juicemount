import Foundation

// Compiled with the production NFSBridge sources and service-client archive.
// Calls only HTTP methods against the acceptance owner's private listener.
@main
struct LocalControlProbe {
    static func main() {
        precondition(CommandLine.arguments.count == 2, "expected private host:port")
        let address = CommandLine.arguments[1]
        for action in ["retry-failed", "clear-stalled"] {
            let result = NFSBridge.spoolRecover(action: action, metricsAddr: address)
            precondition(result?.ok == true && result?.recovered == 0, "recovery action rejected")
        }
        let preview = NFSBridge.spoolClearFailed(confirm: false, metricsAddr: address)
        precondition(preview?.ok == true && preview?.preview == true && preview?.wouldClear == 0)
        let confirm = NFSBridge.spoolClearFailed(confirm: true, metricsAddr: address)
        precondition(confirm?.ok == true && confirm?.preview == false && confirm?.cleared == 0)
        let mount = NFSBridge.mountNow(metricsAddr: address)
        precondition(mount?.ok == false && mount?.error == "injected mount refusal", "mount control did not reach the injected refusal")
        let pending = NFSBridge.selfTest(metricsAddr: address)
        precondition(pending?.status == "pending" && pending?.write_hint == "" && pending?.first_byte_ms == 0,
                     "cached self-test rejected")
        let forced = NFSBridge.selfTest(force: true, metricsAddr: address)
        precondition(forced?.status == "deferred", "explicit self-test did not reach guarded handler")
        let populated = Data(#"{"elapsed_ms":25,"bytes_read":1024,"mb_per_sec":12.5,"status":"green","hint":"ready","ran_at":"2026-01-01T00:00:00Z","target":"fixture","write_ok":true,"write_ms":7,"write_hint":"verified","first_byte_ms":3}"#.utf8)
        let decoded = try! JSONDecoder().decode(NFSBridge.SelfTestResult.self, from: populated)
        precondition(decoded.write_hint == "verified" && decoded.first_byte_ms == 3 && decoded.write_ok)
        for malformed in [#"{}"#, #"{"status":"green","write_ok":true}"#] {
            precondition((try? JSONDecoder().decode(NFSBridge.SelfTestResult.self, from: Data(malformed.utf8))) == nil,
                         "incomplete health response accepted")
        }
        print("PASS native Swift controls: 7 production HTTP calls")
    }
}
