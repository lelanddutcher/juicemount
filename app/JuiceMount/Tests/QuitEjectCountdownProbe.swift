import Foundation

// Compile with the production Core/QuitEjectCountdown.swift:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/QuitEjectCountdown.swift \
//     app/JuiceMount/Tests/QuitEjectCountdownProbe.swift -o <scratch>/probe && <scratch>/probe
@main
struct QuitEjectCountdownProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }

        // Countdown entry: only OPEN FILES start the warning; a shell in a
        // folder on the volume is explained, never forced; unknown proceeds.
        check(QuitHolderPlan.decide(fileApps: nil, folderApps: nil) == .proceed, "unknown census proceeds")
        check(QuitHolderPlan.decide(fileApps: [], folderApps: []) == .proceed, "no holders proceed")
        check(QuitHolderPlan.decide(fileApps: [], folderApps: ["Terminal"]) == .explainFoldersOnly(folderApps: ["Terminal"]), "cwd-only holder must not start a force countdown")
        check(QuitHolderPlan.decide(fileApps: ["Premiere Pro"], folderApps: ["Terminal"]) == .warnOpenFiles(fileApps: ["Premiere Pro"], folderApps: ["Terminal"]), "open files warn")
        check(QuitHolderPlan.folderMessage(["Terminal"]).hasPrefix("Terminal is in a folder on the volume."), "folder wording")
        check(VolumeHoldKind.classify(fileOnVolume: false, cwdOnVolume: true) == .workingDirectoryOnly, "cwd only")
        check(VolumeHoldKind.classify(fileOnVolume: true, cwdOnVolume: true) == .openFiles, "file + cwd is open files")
        check(VolumeHoldKind.classify(fileOnVolume: false, cwdOnVolume: false) == .openFiles, "unexplained hold (mapped file) is never harmless")

        check(QuitEjectCountdown(holderApps: 0, forceAvailable: true) == nil, "no holders: no warning")

        // Founder flow (helper available).
        guard var c = QuitEjectCountdown(holderApps: 2, forceAvailable: true) else { print("FAIL: holders must warn"); exit(1) }
        check(c.phase == .counting(remaining: 30), "starts at 30 s")
        check(c.message == "2 apps still have files open on the volume; JuiceMount will force-eject in 30 s; unsaved work in those apps may be lost.", "warning text: \(c.message)")
        check(QuitEjectCountdown.message(holderApps: 1, remaining: 5, forceAvailable: true).hasPrefix("1 app still has files open"), "singular wording")
        for _ in 0..<29 { c.handle(.tick) }
        check(c.phase == .counting(remaining: 1), "29 ticks leave 1 s, got \(c.phase)")
        c.handle(.tick)
        check(c.phase == .ejectRequested(.timerExpired), "30th tick requests eject, got \(c.phase)")
        c.handle(.cancelQuit); c.handle(.ejectNow); c.handle(.holdersCounted(0))
        check(c.phase == .ejectRequested(.timerExpired), "terminal phase ignores later events")

        var now = QuitEjectCountdown(holderApps: 1, forceAvailable: true)!
        now.handle(.quitAnyway)
        check(!now.isTerminal, "with a helper there is no Quit-anyway shortcut")
        now.handle(.ejectNow)
        check(now.phase == .ejectRequested(.userChoseEjectNow), "Eject now")

        var cancel = QuitEjectCountdown(holderApps: 1, forceAvailable: true)!
        cancel.handle(.cancelQuit)
        for _ in 0..<40 { cancel.handle(.tick) }
        check(cancel.phase == .cancelled, "late ticks never turn a cancelled quit into a force")

        var release = QuitEjectCountdown(holderApps: 3, forceAvailable: true)!
        release.handle(.holdersCounted(1))
        check(release.holderApps == 1 && release.phase == .counting(remaining: 30), "recount updates N without resetting the clock")
        release.handle(.holdersCounted(nil))
        check(release.holderApps == 1 && !release.isTerminal, "unknown recount changes nothing")
        release.handle(.holdersCounted(0))
        check(release.phase == .holdersReleased, "all files closed: ordinary shutdown, no force")

        // This build: no helper. No countdown, no force promise.
        var honest = QuitEjectCountdown(holderApps: 2, forceAvailable: false)!
        check(!honest.message.contains("will force-eject"), "must not promise a force-eject this build cannot do")
        check(honest.message.contains("can't force-eject yet; close the files in these apps, or choose Quit anyway"), "honest wording: \(honest.message)")
        for _ in 0..<100 { honest.handle(.tick) }
        honest.handle(.ejectNow)
        check(honest.phase == .counting(remaining: 30), "without a helper, time and Eject now never request a force")
        honest.handle(.quitAnyway)
        check(honest.phase == .quitAnywayRequested, "Quit anyway")
        var honestRelease = QuitEjectCountdown(holderApps: 1, forceAvailable: false)!
        honestRelease.handle(.holdersCounted(0))
        check(honestRelease.phase == .holdersReleased, "closing files releases the honest panel too")

        if failures > 0 { exit(1) }
        print("PASS: quit eject countdown")
    }
}
