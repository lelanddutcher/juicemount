import Foundation

// Focus re-check wiring: a burst of focus/activation events starts ONE check,
// and a Network Volumes open parked in the kernel is never joined by a second.
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/SingleFlight.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/ResumeOnce.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/PermissionChecks.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/MountTable.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/VolumeAccessProbe.swift \
//     app/JuiceMount/Tests/RecheckSingleFlightProbe.swift -o <scratch>/p && <scratch>/p
@main
struct RecheckSingleFlightProbe {
    @MainActor
    static func main() async {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }

        // 1. SingleFlight drops overlapping runs.
        let flight = SingleFlight()
        var release: CheckedContinuation<Void, Never>?
        var ran = 0
        let work: @MainActor () async -> Void = {
            ran += 1
            await withCheckedContinuation { release = $0 }
        }
        check(flight.run(work), "first focus event starts a check")
        for _ in 0..<5 { check(!flight.run(work), "overlapping focus event must not start another check") }
        await Task.yield()
        while release == nil { await Task.yield() }
        check(flight.started == 1 && ran == 1, "exactly one check ran (started=\(flight.started) ran=\(ran))")
        release?.resume()
        while flight.running { await Task.yield() }
        check(flight.run { }, "after it finishes, the next focus event re-checks")

        // 2. A parked open is never joined by a second open.
        let gate = DispatchSemaphore(value: 0)
        let before = VolumeAccessProbe.opensStarted
        let parked = await VolumeAccessProbe.probe(path: "/fixture", timeout: 0.1, isMounted: { _ in true }, openRoot: { _ in gate.wait(); return 0 })
        check(parked == .timedOut, "parked open reports pending (timedOut)")
        for _ in 0..<3 {
            let again = await VolumeAccessProbe.probe(path: "/fixture", timeout: 0.1, isMounted: { _ in true }, openRoot: { _ in 0 })
            check(again == .timedOut, "re-check while parked answers pending")
        }
        check(VolumeAccessProbe.opensStarted == before + 1, "re-checks while parked must not start more opens (started \(VolumeAccessProbe.opensStarted - before))")
        gate.signal()
        try? await Task.sleep(nanoseconds: 100_000_000)
        let fresh = await VolumeAccessProbe.probe(path: "/fixture", timeout: 1, isMounted: { _ in true }, openRoot: { _ in EPERM })
        check(fresh == .failed(errno: EPERM), "after the parked open returns, a re-check runs again")
        let unmounted = await VolumeAccessProbe.probe(path: "/fixture", timeout: 1, isMounted: { _ in false }, openRoot: { _ in 0 })
        check(unmounted == .notMounted, "not mounted never opens")

        if failures > 0 { exit(1) }
        print("PASS: re-check single-flight")
    }
}
