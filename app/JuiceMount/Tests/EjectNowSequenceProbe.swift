import Foundation

// Busy-Quit countdown + eject-now sequencing against a fake serving service.
// Compile with the production sources:
//   swiftc -parse-as-library app/JuiceMount/Sources/JuiceMount/Core/QuitEjectCountdown.swift \
//     app/JuiceMount/Sources/JuiceMount/Core/UnmountHelperService.swift \
//     app/JuiceMount/Tests/EjectNowSequenceProbe.swift -o <scratch>/p && <scratch>/p
//
// The simulation mirrors App.swift: the countdown and the requester share
// one 1 s clock; the countdown START posts; the countdown's ejectRequested
// (timer or "Eject now") escalates; each `.post` is answered by the fake
// service with the next scripted reply (synchronously, so the POST lands in
// the second it was sent).

typealias R = UnmountHelperDecision.EjectResult

struct Run {
    var posts: [Int] = []          // second of each POST
    var phases: [EjectNowPhase] = [] // phase of each POST
    var withdraws: [Int] = []      // second of each DELETE
    var outcome: EjectNowRequester.Outcome?
    var outcomeAt: Int?
    var countdown: QuitEjectCountdown.Phase
    var sawEjecting = false
    var ejectingBeforeOutcome: String?
}

func simulate(replies: [R], pressEjectNowAt: Int? = nil, cancelAt: Int? = nil,
              withdrawReply: UnmountHelperDecision.WithdrawResult = .withdrawn, seconds: Int = 200) -> Run {
    var script = replies[...]
    var countdown = QuitEjectCountdown(holderApps: 2, forceAvailable: true)!
    var req = EjectNowRequester()
    var run = Run(countdown: countdown.phase)
    var t = 0
    func service() -> R { script.isEmpty ? .failed("unexpected extra POST") : script.removeFirst() }
    func perform(_ c: EjectNowRequester.Command) {
        var c = c
        while c != .none {
            if c == .post {
                run.posts.append(t)
                run.phases.append(req.inFlightPhase)
                c = req.reply(service())
            } else {
                run.withdraws.append(t)
                c = req.withdrawReply(withdrawReply)
            }
        }
    }
    func escalateIfNeeded(_ before: QuitEjectCountdown.Phase) {
        if case .counting = before, case .ejectRequested(let trig) = countdown.phase {
            perform(req.escalate(userPressed: trig == .userChoseEjectNow))
        }
    }
    perform(req.countdownStarted())
    while t < seconds && req.outcome == nil {
        t += 1
        // The requester's second elapses first; an escalation in this second
        // then schedules from a full retry_after.
        perform(req.tick())
        if req.outcome != nil { break }
        let before = countdown.phase
        if t == cancelAt {
            // App.swift: "Cancel Quit" goes through the countdown while it
            // runs, straight to the runner once escalated.
            if case .counting = countdown.phase { countdown.handle(.cancelQuit) }
            perform(req.cancel())
            if req.outcome != nil { break }
            continue
        }
        if req.cancelling { continue }
        if t == pressEjectNowAt { countdown.handle(.ejectNow) } else { countdown.handle(.tick) }
        escalateIfNeeded(before)
        if let m = req.ejectingMessage { run.sawEjecting = true; run.ejectingBeforeOutcome = m }
    }
    run.outcome = req.outcome
    run.outcomeAt = req.outcome == nil ? nil : t
    run.countdown = countdown.phase
    return run
}

@main
struct EjectNowSequenceProbe {
    static func main() {
        var failures = 0
        func check(_ ok: Bool, _ what: String) {
            if !ok { print("FAIL: \(what)"); failures += 1 }
        }
        let D = UnmountHelperDecision.self

        // Reply parser (review: 202 is not a failure).
        func body(_ s: String) -> Data { Data(s.utf8) }
        check(D.ejectResult(httpStatus: 202, body: body(#"{"ok":false,"state":"ordinary_in_progress","error":"ejecting normally; apps still have files open","retry_after":27}"#)) == .inProgress(retryAfter: 27), "202 ordinary_in_progress is in progress, not failure")
        check(D.ejectResult(httpStatus: 202, body: body(#"{"ok":false,"state":"ordinary_in_progress"}"#)) == .inProgress(retryAfter: 5), "202 without retry_after defaults to 5 s")
        check(D.ejectResult(httpStatus: 202, body: body(#"{"ok":false,"state":"ordinary_in_progress","retry_after":900}"#)) == .inProgress(retryAfter: 30), "retry_after is clamped")
        check(D.ejectResult(httpStatus: 200, body: body(#"{"ok":true,"state":"RETIRED","op_id":"nfs-1"}"#)) == .ejected, "200 RETIRED is success")
        check(D.ejectResult(httpStatus: 409, body: body(#"{"ok":false,"error":"this service owns no mount registered with the unmount helper","state":"NO_RECEIPT"}"#)) == .failed("this service owns no mount registered with the unmount helper"), "409 is a failure")
        check(D.ejectResult(httpStatus: 504, body: body(#"{"ok":false,"error":"retire pending","state":"FORCE"}"#)) == .forcePending("retire pending"), "504 is a force under way, not a failure")
        check(D.ejectResult(httpStatus: 409, body: body(#"{"ok":false,"state":"withdrawn","error":"the eject was withdrawn"}"#)) == .failed("the eject was withdrawn"), "409 withdrawn (a DELETE overtook the POST) is not success")
        // 504 during escalation: the outcome is "force under way, result
        // unknown", and the shutdown note never says it was not performed.
        var p504 = EjectNowRequester()
        _ = p504.countdownStarted(); _ = p504.reply(.inProgress(retryAfter: 30)); _ = p504.escalate(userPressed: true)
        check(p504.reply(.forcePending("retire pending")) == .none && p504.outcome == .forcePending("retire pending"), "504: forcePending outcome, got \(String(describing: p504.outcome))")
        let note504 = D.shutdownEjectNote(for: .forcePending("retire pending")) ?? ""
        check(!note504.lowercased().contains("not performed") && note504.lowercased().contains("unknown"), "504 note is truthful: \(note504)")
        let noteFail = D.shutdownEjectNote(for: .failed("identity refused")) ?? ""
        check(!noteFail.lowercased().contains("not performed") && noteFail.contains("identity refused"), "failure note does not claim nothing happened: \(noteFail)")
        check(D.shutdownEjectNote(for: .retired) == nil, "no note after a clean eject")
        // Review #4: an escalated 404 attempted nothing; say so.
        var p404 = EjectNowRequester()
        _ = p404.countdownStarted(); _ = p404.reply(.inProgress(retryAfter: 30)); _ = p404.escalate(userPressed: true)
        _ = p404.reply(D.ejectResult(httpStatus: 404, body: nil))
        check(p404.outcome == .notAttempted(D.ejectUnavailableText), "escalated 404: not attempted, got \(String(describing: p404.outcome))")
        let note404 = D.shutdownEjectNote(for: p404.outcome ?? .retired) ?? ""
        check(note404.lowercased().contains("not performed") && !note404.lowercased().contains("did not complete"), "escalated 404 note says not attempted: \(note404)")
        // Review #4: no answer to an escalated POST (it may have forced): the
        // outcome is unknown, never "did not complete".
        var pNA = EjectNowRequester()
        _ = pNA.countdownStarted(); _ = pNA.reply(.inProgress(retryAfter: 30)); _ = pNA.escalate(userPressed: true)
        _ = pNA.reply(D.ejectResult(httpStatus: nil, body: nil))
        check(pNA.outcome == .outcomeUnknown("The background service did not answer."), "escalated no-answer: outcome unknown, got \(String(describing: pNA.outcome))")
        let noteNA = D.shutdownEjectNote(for: pNA.outcome ?? .retired) ?? ""
        check(noteNA.lowercased().contains("unknown") && !noteNA.lowercased().contains("did not complete") && !noteNA.lowercased().contains("not performed"), "no-answer note is truthful: \(noteNA)")
        // Review #5, finding 2: "Eject now" pressed while the START POST is
        // in flight, and that POST then gets no answer. It asked for an
        // ordinary eject, so the note never says it may have forced.
        var sEsc = EjectNowRequester()
        _ = sEsc.countdownStarted()
        check(sEsc.escalate(userPressed: true) == .none, "Eject now with the start POST in flight sends nothing yet")
        _ = sEsc.reply(D.ejectResult(httpStatus: nil, body: nil))
        check(sEsc.outcome == .startUnanswered("The background service did not answer."), "unanswered start POST after escalation: startUnanswered, got \(String(describing: sEsc.outcome))")
        let noteSE = D.shutdownEjectNote(for: sEsc.outcome ?? .retired) ?? ""
        check(!noteSE.lowercased().contains("may have forced") && noteSE.lowercased().contains("normal eject") && !noteSE.lowercased().contains("did not complete"), "unanswered start POST note does not claim a possible force: \(noteSE)")
        // The escalated POST that follows a 202 start reply is escalated
        // again: no answer to it stays outcomeUnknown.
        var sEsc2 = EjectNowRequester()
        _ = sEsc2.countdownStarted(); _ = sEsc2.escalate(userPressed: true)
        _ = sEsc2.reply(.inProgress(retryAfter: 1))
        check(sEsc2.tick() == .post, "escalated re-POST after the start 202")
        _ = sEsc2.reply(D.ejectResult(httpStatus: nil, body: nil))
        check(sEsc2.outcome == .outcomeUnknown("The background service did not answer."), "unanswered escalated re-POST: outcomeUnknown, got \(String(describing: sEsc2.outcome))")
        // A start POST with no answer is only remembered, as before.
        var sNA = EjectNowRequester()
        _ = sNA.countdownStarted(); _ = sNA.reply(D.ejectResult(httpStatus: nil, body: nil))
        check(sNA.outcome == nil && sNA.startNote == "The background service did not answer.", "start no-answer: remembered, no outcome")
        // A POST with no answer after Cancel still withdraws.
        var cNA = EjectNowRequester()
        _ = cNA.countdownStarted(); _ = cNA.reply(.inProgress(retryAfter: 30)); _ = cNA.escalate(userPressed: true); _ = cNA.cancel()
        check(cNA.reply(D.ejectResult(httpStatus: nil, body: nil)) == .withdraw, "no-answer POST after Cancel: DELETE follows")
        // Review #5: an internal withdrawal's 202 keeps the GUI asking.
        check(D.ejectResult(httpStatus: 202, body: body(#"{"ok":false,"state":"ordinary_in_progress","retry_after":1,"error":"JuiceMount stopped its own eject attempt while this request was in flight; nothing was forced. Ejecting normally again"}"#)) == .inProgress(retryAfter: 1), "interrupted 202 is in progress")

        // 1. Volume nobody holds: the START post retires it; Quit proceeds
        //    without waiting for the countdown.
        let free = simulate(replies: [.ejected])
        check(free.posts == [0], "non-busy: one POST at the start, got \(free.posts)")
        check(free.outcome == .retired && free.outcomeAt == 0, "non-busy: retired at once, got \(String(describing: free.outcome)) at \(String(describing: free.outcomeAt))")
        check(free.countdown == .counting(remaining: 30), "non-busy: the countdown never had to run")
        check(!free.sawEjecting, "non-busy: no Ejecting… state")

        // 2. Busy volume: START gets 202 (keep counting, no re-POST), the END
        //    POST at 30 s returns RETIRED.
        let busy = simulate(replies: [.inProgress(retryAfter: 30), .ejected])
        check(busy.posts == [0, 30], "busy: POST at start and at the end, got \(busy.posts)")
        check(busy.outcome == .retired && busy.outcomeAt == 30, "busy: retired at 30 s, got \(String(describing: busy.outcome)) at \(String(describing: busy.outcomeAt))")
        check(busy.countdown == .ejectRequested(.timerExpired), "busy: countdown ran out")

        // 2b. The end POST lands just inside the service floor: honour
        //     retry_after, then RETIRED.
        let floor = simulate(replies: [.inProgress(retryAfter: 30), .inProgress(retryAfter: 2), .ejected])
        check(floor.posts == [0, 30, 32], "busy: end 202 honours retry_after, got \(floor.posts)")
        check(floor.outcome == .retired, "busy: retired after retry")
        check(floor.sawEjecting && floor.ejectingBeforeOutcome == EjectNowRequester.ejectingMessage, "busy: shows Ejecting… while waiting")
        let m = EjectNowRequester.ejectingMessage.lowercased()
        check(m.hasPrefix("ejecting… other apps still have files open") && m.contains("only after") && !m.contains("immediately") && !m.contains("instant"), "never promises an instant force: \(m)")

        // 3. Early "Eject now" at 5 s: POST now, keep polling with
        //    retry_after until RETIRED; Ejecting… meanwhile.
        let early = simulate(replies: [.inProgress(retryAfter: 30), .inProgress(retryAfter: 25), .ejected], pressEjectNowAt: 5)
        check(early.posts == [0, 5, 30], "early Eject now: POST at 0, 5, then after retry_after, got \(early.posts)")
        check(early.outcome == .retired && early.outcomeAt == 30, "early Eject now: retired when the service allows, got \(String(describing: early.outcomeAt))")
        check(early.countdown == .ejectRequested(.userChoseEjectNow), "early Eject now ends the countdown")
        check(early.sawEjecting, "early Eject now shows Ejecting…")

        // 4. Service error. At the start it is remembered and the countdown
        //    keeps running; at the end it fails the eject (Quit then runs the
        //    ordinary shutdown with the reason as its note).
        let err = simulate(replies: [.failed("unmount helper unavailable"), .failed("identity refused")])
        check(err.posts == [0, 30], "error: start error does not stop the countdown, got \(err.posts)")
        check(err.outcome == .failed("identity refused") && err.outcomeAt == 30, "error: end failure is the outcome, got \(String(describing: err.outcome))")
        let errEarly = simulate(replies: [.inProgress(retryAfter: 30), .failed("helper refused")], pressEjectNowAt: 3)
        check(errEarly.outcome == .failed("helper refused") && errEarly.posts == [0, 3], "error after Eject now fails at once, got \(errEarly.posts)")

        // Bound: a service that answers 202 forever fails after the cap.
        let stuck = simulate(replies: Array(repeating: .inProgress(retryAfter: 5), count: 100))
        if case .failed = stuck.outcome {} else { check(false, "endless 202 must time out") }
        check((stuck.outcomeAt ?? 999) <= 30 + EjectNowRequester.maxEscalatedSeconds + 1, "timeout bounded, at \(String(describing: stuck.outcomeAt))")

        // Single flight: escalation while the start POST is still in flight
        // sends nothing until its reply arrives.
        var r = EjectNowRequester()
        check(r.countdownStarted() == .post, "start posts")
        check(r.escalate(userPressed: true) == .none, "no second POST while one is in flight")
        check(r.reply(.inProgress(retryAfter: 2)) == .none && r.waitRemaining == 2, "reply schedules the retry")
        check(r.tick() == .none && r.tick() == .post, "posts again after retry_after")
        check(r.countdownStarted() == .none, "start posts only once")

        // --- Cancel / withdraw (DELETE /eject-now) ---------------------------
        // Withdraw reply parser, against the bytes bridge/eject_now.go writes.
        check(D.withdrawResult(httpStatus: 200, body: body(#"{"ok":true,"state":"CANCELLED","op_id":"nfs-1"}"#)) == .withdrawn, "200 ok is withdrawn")
        check(D.withdrawResult(httpStatus: 200, body: body(#"{"ok":true,"state":"RETIRED","op_id":"nfs-1"}"#)) == .alreadyEjected, "200 RETIRED: nothing to withdraw, already ejected")
        check(D.withdrawResult(httpStatus: 409, body: body(#"{"ok":false,"state":"force_in_progress","error":"the forced eject is already under way and cannot be withdrawn","op_id":"nfs-1"}"#)) == .forceInProgress("the forced eject is already under way and cannot be withdrawn"), "409 force_in_progress is the force state")
        check(D.withdrawResult(httpStatus: 409, body: body(#"{"ok":false,"error":"this service owns no mount registered with the unmount helper","state":"NO_RECEIPT"}"#)) == .failed("this service owns no mount registered with the unmount helper"), "409 NO_RECEIPT is not a force")
        check(D.withdrawResult(httpStatus: 405, body: nil) == .failed(D.withdrawUnsupportedText), "405 (old service) is a failure")
        check(D.withdrawResult(httpStatus: nil, body: nil) == .failed("The background service did not answer."), "no answer is a failure")

        // 5. Cancel Quit at 10 s after the start POST: exactly one DELETE,
        //    then nothing more.
        let cancelled = simulate(replies: [.inProgress(retryAfter: 30)], cancelAt: 10)
        check(cancelled.posts == [0], "cancel: only the start POST, got \(cancelled.posts)")
        check(cancelled.withdraws == [10], "cancel after a POST: exactly one DELETE at the cancel, got \(cancelled.withdraws)")
        check(cancelled.outcome == .abandoned(.withdrawn), "cancel: withdrawn, got \(String(describing: cancelled.outcome))")
        check(cancelled.countdown == .cancelled, "cancel: the countdown is cancelled")

        // 5b. Cancel while escalated (Ejecting…, waiting out retry_after):
        //     one DELETE, no further POST.
        let escCancel = simulate(replies: [.inProgress(retryAfter: 30), .inProgress(retryAfter: 25)], pressEjectNowAt: 3, cancelAt: 8)
        check(escCancel.posts == [0, 3], "escalated cancel: no POST after the cancel, got \(escCancel.posts)")
        check(escCancel.withdraws == [8], "escalated cancel: one DELETE, got \(escCancel.withdraws)")
        check(escCancel.outcome == .abandoned(.withdrawn), "escalated cancel: withdrawn")

        // 6. 409: a force is already under way. The outcome says so and the
        //    panel shows the honest text, not "cancelled".
        let refused = simulate(replies: [.inProgress(retryAfter: 30), .inProgress(retryAfter: 2)], pressEjectNowAt: 3, cancelAt: 4,
                               withdrawReply: .forceInProgress("the forced eject is already under way and cannot be withdrawn"))
        check(refused.withdraws == [4], "409: one DELETE, got \(refused.withdraws)")
        check(refused.outcome == .abandoned(.forceInProgress("the forced eject is already under way and cannot be withdrawn")), "409: force-in-progress outcome, got \(String(describing: refused.outcome))")
        var r409 = EjectNowRequester()
        _ = r409.countdownStarted(); _ = r409.reply(.inProgress(retryAfter: 30))
        check(r409.cancel() == .withdraw, "409 setup: cancel withdraws")
        check(r409.ejectingMessage == EjectNowRequester.cancellingMessage && !r409.offersCancel, "while the DELETE runs: Cancelling…, no button")
        _ = r409.withdrawReply(.forceInProgress("x"))
        check(r409.ejectingMessage == EjectNowRequester.forceInProgressMessage, "409: panel shows the force-in-progress text, got \(String(describing: r409.ejectingMessage))")
        let fm = EjectNowRequester.forceInProgressMessage.lowercased()
        check(fm.contains("can no longer be cancelled") && fm.contains("force-ejected"), "force text is honest: \(fm)")
        var rOK = EjectNowRequester()
        _ = rOK.countdownStarted(); _ = rOK.reply(.inProgress(retryAfter: 30)); _ = rOK.cancel(); _ = rOK.withdrawReply(.withdrawn)
        check(rOK.ejectingMessage == nil, "withdrawn: no Ejecting/force text left on the panel")

        // 7. Cancel before any POST: nothing to withdraw, nothing sent.
        var none = EjectNowRequester()
        check(none.cancel() == .none && none.outcome == .abandoned(nil) && !none.withdrawSent && none.postsSent == 0, "cancel before any POST sends no DELETE")
        check(none.countdownStarted() == .none && none.escalate(userPressed: true) == .none && none.tick() == .none, "nothing is sent after a cancel")

        // 8. Cancel while a POST is in flight: the DELETE waits for its reply
        //    (sent first it could overtake the POST at the service), then
        //    goes exactly once; no POST after the cancel.
        var fl = EjectNowRequester()
        check(fl.countdownStarted() == .post, "start posts")
        check(fl.cancel() == .none && !fl.withdrawSent, "no DELETE while the POST is in flight")
        check(fl.cancel() == .none, "a second cancel sends nothing")
        for _ in 0..<200 { if fl.tick() != .none { check(false, "tick sent a request after cancel"); break } }
        check(fl.outcome == nil, "the escalation timeout never fires while cancelling")
        check(fl.reply(.inProgress(retryAfter: 30)) == .withdraw, "the POST's reply releases the DELETE")
        check(fl.reply(.inProgress(retryAfter: 1)) == .none && fl.escalate(userPressed: true) == .none, "nothing more after the DELETE is sent")
        check(fl.withdrawReply(.withdrawn) == .none && fl.outcome == .abandoned(.withdrawn) && fl.postsSent == 1, "withdrawn after one POST")
        // ... and a POST in flight that had already ejected: nothing to withdraw.
        var gone = EjectNowRequester()
        _ = gone.countdownStarted(); _ = gone.cancel()
        check(gone.reply(.ejected) == .none && gone.outcome == .retired && !gone.withdrawSent, "already ejected: no DELETE, outcome retired")
        // ... and a failed force POST (504: force still pending) then DELETE 409.
        var f504 = EjectNowRequester()
        _ = f504.countdownStarted(); _ = f504.reply(.inProgress(retryAfter: 30)); _ = f504.escalate(userPressed: true)
        check(f504.cancel() == .none, "escalated POST in flight: DELETE deferred")
        check(f504.ejectingMessage == EjectNowRequester.cancellingAfterForceRequestMessage, "waiting on an escalated POST: says a force may be under way, got \(String(describing: f504.ejectingMessage))")
        check(f504.reply(.forcePending("retire pending")) == .withdraw, "a 504 POST after the cancel still withdraws")
        check(f504.outcome == nil, "the 504 POST is not the outcome once cancelled")
        check(f504.ejectingMessage == EjectNowRequester.cancellingMessage, "once the DELETE is sent: plain Cancelling…")
        _ = f504.withdrawReply(.forceInProgress("x"))
        check(f504.outcome == .abandoned(.forceInProgress("x")), "504 then DELETE 409: force in progress")
        let cfm = EjectNowRequester.cancellingAfterForceRequestMessage.lowercased()
        check(cfm.contains("may force") && cfm.contains("wait") && cfm.contains("cannot be stopped"), "in-flight force text is honest: \(cfm)")
        // The start POST in flight (not escalated) keeps the plain text.
        var sfl = EjectNowRequester()
        _ = sfl.countdownStarted(); _ = sfl.cancel()
        check(sfl.ejectingMessage == EjectNowRequester.cancellingMessage, "start POST in flight: plain Cancelling…")
        // Round 7: the start POST still in flight after "Eject now" cannot
        // force (the service never forces on a start), so Cancel keeps the
        // plain text too.
        var sfe = EjectNowRequester()
        _ = sfe.countdownStarted()
        check(sfe.escalate(userPressed: true) == .none && sfe.inFlightPhase == .start, "Eject now with the start POST in flight: that POST stays a start")
        _ = sfe.cancel()
        check(sfe.ejectingMessage == EjectNowRequester.cancellingMessage, "escalated, but only the start POST in flight: plain Cancelling…, got \(String(describing: sfe.ejectingMessage))")
        // Round 7: every POST carries its phase.
        var ph = EjectNowRequester()
        check(ph.countdownStarted() == .post && ph.inFlightPhase == .start, "the countdown's POST is a start")
        _ = ph.reply(.inProgress(retryAfter: 30))
        check(ph.escalate(userPressed: false) == .post && ph.inFlightPhase == .escalate, "the countdown end POSTs escalate")
        _ = ph.reply(.inProgress(retryAfter: 1))
        check(ph.tick() == .post && ph.inFlightPhase == .escalate, "re-POSTs after escalation stay escalate")
        // (The bodies on the wire are checked by TestEjectNowGUIContract.)
        // Review #7, finding 1 (lost start): the start POST got no HTTP
        // answer, so the service may never have seen this panel's start and
        // may still hold an earlier panel's busy window. "Eject now" 1 s
        // later must not send an escalate (the service could force on that
        // old window at once): it sends the start again, and escalates only
        // after that start's 202, 30 s later.
        let lost = simulate(replies: [.noAnswer("The background service did not answer."), .inProgress(retryAfter: 30), .ejected], pressEjectNowAt: 1)
        check(lost.phases == [.start, .start, .escalate], "lost start: escalation re-sends start before any escalate, got \(lost.phases)")
        check(lost.posts == [0, 1, 31], "lost start: start at 0, start again at Eject now, escalate after its 30 s retry_after, got \(lost.posts)")
        check(lost.outcome == .retired, "lost start: retired after the full floor, got \(String(describing: lost.outcome))")
        var ls = EjectNowRequester()
        _ = ls.countdownStarted(); _ = ls.reply(.noAnswer("x"))
        check(ls.escalate(userPressed: true) == .post && ls.inFlightPhase == .start && !ls.startAccepted, "lost start: the Eject-now POST is a start, got \(ls.inFlightPhase)")
        _ = ls.cancel()
        check(ls.ejectingMessage == EjectNowRequester.cancellingMessage, "lost start: a re-sent start in flight cannot force, plain Cancelling…, got \(String(describing: ls.ejectingMessage))")
        var ls2 = EjectNowRequester()
        _ = ls2.countdownStarted(); _ = ls2.reply(.noAnswer("x")); _ = ls2.escalate(userPressed: false)
        _ = ls2.reply(.noAnswer("y"))
        check(ls2.outcome == .startUnanswered("y"), "re-sent start unanswered: startUnanswered, never outcomeUnknown, got \(String(describing: ls2.outcome))")
        // An error answer to the start may have come before the service's
        // gate: the countdown's end POST is a start too, and only after its
        // 202 does an escalate follow.
        let failedStart = simulate(replies: [.failed("unmount helper unavailable"), .inProgress(retryAfter: 30), .ejected])
        check(failedStart.phases == [.start, .start, .escalate] && failedStart.posts == [0, 30, 60], "failed start: countdown end re-sends start, got \(failedStart.phases) at \(failedStart.posts)")
        // A start answered 202 is enough: the next POST is an escalate.
        var acc = EjectNowRequester()
        _ = acc.countdownStarted(); _ = acc.reply(.inProgress(retryAfter: 30))
        check(acc.startAccepted && acc.escalate(userPressed: true) == .post && acc.inFlightPhase == .escalate, "accepted start: Eject now escalates")
        // DELETE answers RETIRED: already ejected.
        var ae = EjectNowRequester()
        _ = ae.countdownStarted(); _ = ae.reply(.inProgress(retryAfter: 30)); _ = ae.cancel()
        _ = ae.withdrawReply(.alreadyEjected)
        check(ae.outcome == .abandoned(.alreadyEjected), "already ejected outcome")

        if failures > 0 { exit(1) }
        print("PASS: eject-now sequence")
    }
}
